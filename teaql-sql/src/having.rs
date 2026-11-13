//! Bind result names to their source expressions before compiling HAVING.
//! Projection aliases are not physical columns (PostgreSQL does not resolve
//! SELECT aliases in HAVING). Source columns inside aggregates remain source
//! columns, even when an output alias happens to have the same spelling.
use teaql_core::{Aggregate, AggregateFunction as A, Expr, ExprFunction as F, SelectQuery};

use crate::SqlCompileError;

/// The ordinary plan cache recollects operands from the unexpanded AST. An
/// alias may duplicate operands in HAVING, including inside a nested SELECT.
/// Until that cache stores a lowering-aware binding plan, compile these shapes
/// fresh instead of reusing a parameter layout with missing/stale values.
pub(crate) fn requires_fresh_compilation(query: &SelectQuery) -> Result<bool, SqlCompileError> {
    if let Some(having) = &query.having
        && bind(query, having)? != *having
    {
        return Ok(true);
    }
    if !query.group_by.is_empty() || !query.aggregates.is_empty() {
        for order in &query.order_by {
            let expression = order
                .expr
                .clone()
                .unwrap_or_else(|| Expr::column(&order.field));
            if bind_order(query, &expression)? != expression {
                return Ok(true);
            }
        }
    }
    for expr in query
        .filter
        .iter()
        .chain(query.having.iter())
        .chain(
            query
                .expr_projection
                .iter()
                .map(|projection| &projection.expr),
        )
        .chain(
            query
                .order_by
                .iter()
                .filter_map(|order| order.expr.as_ref()),
        )
    {
        if nested_requires_fresh_compilation(expr)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Ordinary statistics sort completed selected cells, never arbitrary source
/// observations. Keep this binding separate from HAVING: HAVING's source-field
/// fallback is not authorization to use an unselected ORDER BY operand.
pub(crate) fn bind_order(query: &SelectQuery, expression: &Expr) -> Result<Expr, SqlCompileError> {
    fn identity_operand(mut expression: &Expr) -> Result<&Expr, SqlCompileError> {
        for _ in 0..64 {
            if let Expr::Function {
                function: F::Identity,
                args,
            } = expression
            {
                let [argument] = args.as_slice() else {
                    return Err(SqlCompileError::InvalidFunctionArguments(
                        "IDENTITY expects exactly one argument".into(),
                    ));
                };
                expression = argument;
            } else {
                return Ok(expression);
            }
        }
        Err(SqlCompileError::InvalidFunctionArguments(
            "statistical order expression exceeds depth limit".into(),
        ))
    }
    let key = identity_operand(expression)?;
    // GBK is an existing ordering modifier, not permission to read another
    // source field. Preserve its real provider transform over a selected cell;
    // do not add a binary projection or substitute an identity fallback.
    let gbk_key = if let Expr::Function {
        function: F::Gbk,
        args,
    } = key
    {
        let [argument] = args.as_slice() else {
            return Err(SqlCompileError::InvalidFunctionArguments(
                "GBK expects exactly one argument".into(),
            ));
        };
        Some(identity_operand(argument)?)
    } else {
        None
    };
    let mut selected = None;
    let mut consider = |alias: &str, candidate: &Expr| -> Result<(), SqlCompileError> {
        let candidate_key = identity_operand(candidate)?;
        let direct = matches!(key, Expr::Column(name) if name == alias) || key == candidate_key;
        let modifier = gbk_key.is_some_and(|operand| {
            matches!(operand, Expr::Column(name) if name == alias) || operand == candidate_key
        });
        if direct || modifier {
            if selected.is_some() {
                return Err(SqlCompileError::InvalidFunctionArguments(
                    "ambiguous statistical order expression".into(),
                ));
            }
            selected = Some(if direct {
                candidate.clone()
            } else {
                Expr::gbk(candidate.clone())
            });
        }
        Ok(())
    };
    for field in &query.group_by {
        if !query
            .expr_projection
            .iter()
            .any(|projection| projection.alias == *field)
        {
            consider(field, &Expr::column(field))?;
        }
    }
    for projection in &query.expr_projection {
        consider(&projection.alias, &projection.expr)?;
    }
    for aggregate in &query.aggregates {
        consider(&aggregate.alias, &aggregate_expr(aggregate))?;
    }
    selected.ok_or_else(|| {
        SqlCompileError::InvalidFunctionArguments(
            "order expression is not a selected statistical result".into(),
        )
    })
}

fn nested_requires_fresh_compilation(expr: &Expr) -> Result<bool, SqlCompileError> {
    match expr {
        Expr::SubQuery { left, query, .. } => {
            Ok(requires_fresh_compilation(query)? || nested_requires_fresh_compilation(left)?)
        }
        Expr::Function { args, .. } | Expr::And(args) | Expr::Or(args) => {
            for arg in args {
                if nested_requires_fresh_compilation(arg)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::Binary { left, right, .. } => {
            Ok(nested_requires_fresh_compilation(left)?
                || nested_requires_fresh_compilation(right)?)
        }
        Expr::Between { expr, lower, upper } => Ok(nested_requires_fresh_compilation(expr)?
            || nested_requires_fresh_compilation(lower)?
            || nested_requires_fresh_compilation(upper)?),
        Expr::IsNull(value) | Expr::IsNotNull(value) | Expr::Not(value) => {
            nested_requires_fresh_compilation(value)
        }
        Expr::Column(_) | Expr::Value(_) | Expr::LikePattern { .. } => Ok(false),
    }
}

pub(crate) fn bind(query: &SelectQuery, expr: &Expr) -> Result<Expr, SqlCompileError> {
    Ok(match expr {
        Expr::Column(name) => {
            let measures = query
                .aggregates
                .iter()
                .filter(|item| item.alias == *name)
                .collect::<Vec<_>>();
            let projections = query
                .expr_projection
                .iter()
                .filter(|item| item.alias == *name)
                .collect::<Vec<_>>();
            if measures.len() + projections.len() > 1 {
                return Err(SqlCompileError::InvalidFunctionArguments(format!(
                    "ambiguous HAVING result alias: {name}"
                )));
            }
            if let Some(measure) = measures.first() {
                aggregate_expr(measure)
            } else if let Some(projection) = projections.first() {
                // A projection expression is already expressed in source fields,
                // not in the namespace of its sibling result aliases.
                projection.expr.clone()
            } else {
                expr.clone()
            }
        }
        Expr::Function { function, args } => {
            if matches!(function, F::Identity | F::Soundex | F::Gbk) {
                Expr::function(
                    *function,
                    args.iter()
                        .map(|arg| bind(query, arg))
                        .collect::<Result<Vec<_>, _>>()?,
                )
            } else {
                // Never turn SUM(result_alias) into SUM(SUM(source_column)).
                // Explicit aggregate operands bind only to source metadata.
                expr.clone()
            }
        }
        Expr::Binary { left, op, right } => Expr::Binary {
            left: Box::new(bind(query, left)?),
            op: *op,
            right: Box::new(bind(query, right)?),
        },
        Expr::Between { expr, lower, upper } => Expr::Between {
            expr: Box::new(bind(query, expr)?),
            lower: Box::new(bind(query, lower)?),
            upper: Box::new(bind(query, upper)?),
        },
        Expr::IsNull(value) => Expr::IsNull(Box::new(bind(query, value)?)),
        Expr::IsNotNull(value) => Expr::IsNotNull(Box::new(bind(query, value)?)),
        Expr::Not(value) => Expr::Not(Box::new(bind(query, value)?)),
        Expr::And(parts) => Expr::And(
            parts
                .iter()
                .map(|part| bind(query, part))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Expr::Or(parts) => Expr::Or(
            parts
                .iter()
                .map(|part| bind(query, part))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Expr::SubQuery {
            left,
            op,
            entity,
            query: nested,
        } => Expr::SubQuery {
            left: Box::new(bind(query, left)?),
            op: *op,
            entity: entity.clone(),
            query: nested.clone(),
        },
        Expr::Value(_) | Expr::LikePattern { .. } => expr.clone(),
    })
}

pub(crate) fn aggregate_expr(aggregate: &Aggregate) -> Expr {
    let function = match aggregate.function {
        A::Count => F::Count,
        A::CountDistinct => F::CountDistinct,
        A::Sum => F::Sum,
        A::Avg => F::Avg,
        A::Min => F::Min,
        A::Max => F::Max,
        A::Stddev => F::Stddev,
        A::StddevPop => F::StddevPop,
        A::VarSamp => F::VarSamp,
        A::VarPop => F::VarPop,
        A::BitAnd => F::BitAnd,
        A::BitOr => F::BitOr,
        A::BitXor => F::BitXor,
    };
    let args = if aggregate.function == A::Count && aggregate.field == "*" {
        Vec::new()
    } else {
        vec![Expr::column(&aggregate.field)]
    };
    Expr::function(function, args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatabaseKind, SqlDialect};
    use teaql_core::{BinaryOp, DataType, EntityDescriptor, PropertyDescriptor, Value};
    use teaql_data_service::SqlParameterLogPolicy;

    struct Dialect;
    impl SqlDialect for Dialect {
        fn kind(&self) -> DatabaseKind {
            DatabaseKind::PostgreSql
        }
        fn quote_ident(&self, name: &str) -> String {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
        fn placeholder(&self, index: usize) -> String {
            format!("${index}")
        }
    }
    fn entity() -> EntityDescriptor {
        EntityDescriptor::new("School")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(
                PropertyDescriptor::new("capacity", DataType::I64).column_name("Legacy Capacity"),
            )
            .audit_mask_fields(vec!["capacity".into()])
    }

    #[test]
    fn measure_alias_binds_source_column_and_threshold_policy() {
        let query = SelectQuery::new("School")
            .sum("capacity", "total")
            .having(Expr::gt("total", 3000));
        let original = query.clone();
        let compiled = Dialect.compile_select(&entity(), &query).unwrap();
        assert!(
            compiled
                .sql
                .ends_with("HAVING (SUM(\"Legacy Capacity\") > $1)")
        );
        assert_eq!(compiled.params, [Value::I64(3000)]);
        assert_eq!(
            compiled.log_context.parameter_policies,
            [SqlParameterLogPolicy::Masked]
        );
        assert_eq!(query, original);
    }

    #[test]
    fn alias_identical_to_source_field_does_not_make_nested_aggregate() {
        let query = SelectQuery::new("School")
            .sum("capacity", "capacity")
            .having(Expr::gt("capacity", 3000));
        let bound = bind(&query, query.having.as_ref().unwrap()).unwrap();
        assert_eq!(bound, bind(&query, &bound).unwrap());
        assert!(
            Dialect
                .compile_select(&entity(), &query)
                .unwrap()
                .sql
                .ends_with("HAVING (SUM(\"Legacy Capacity\") > $1)")
        );
    }

    #[test]
    fn scalar_functions_null_checks_and_disjunction_bind_aliases() {
        let query = SelectQuery::new("School")
            .count("schools")
            .sum("capacity", "total")
            .having(Expr::or([
                Expr::IsNull(Box::new(Expr::column("total"))),
                Expr::Between {
                    expr: Box::new(Expr::column("schools")),
                    lower: Box::new(Expr::value(2)),
                    upper: Box::new(Expr::value(4)),
                },
            ]));
        let compiled = Dialect.compile_select(&entity(), &query).unwrap();
        assert!(compiled.sql.contains(
            "HAVING ((SUM(\"Legacy Capacity\") IS NULL) OR (COUNT(*) BETWEEN $1 AND $2))"
        ));
    }

    #[test]
    fn identity_unwraps_measure_alias_without_losing_bindings_or_mask_policy() {
        let query = SelectQuery::new("School")
            .sum("capacity", "total")
            .having(Expr::binary(
                Expr::identity(Expr::column("total")),
                BinaryOp::Gt,
                Expr::value(9_007_199_254_740_993_i64),
            ));
        let compiled = Dialect.compile_select(&entity(), &query).unwrap();
        assert!(
            compiled
                .sql
                .ends_with("HAVING (SUM(\"Legacy Capacity\") > $1)")
        );
        assert!(!compiled.sql.contains("IDENTITY("));
        assert_eq!(compiled.params, [Value::I64(9_007_199_254_740_993)]);
        assert_eq!(
            compiled.log_context.parameter_policies,
            [SqlParameterLogPolicy::Masked]
        );
    }

    #[test]
    fn identity_rejects_wrong_arity_without_disclosing_operands() {
        for args in [
            vec![],
            vec![Expr::value("secret-a"), Expr::value("secret-b")],
        ] {
            let query = SelectQuery::new("School").project_expr(
                "invalid",
                Expr::function(teaql_core::ExprFunction::Identity, args),
            );
            let error = Dialect
                .compile_select(&entity(), &query)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("IDENTITY expects exactly one argument"),
                "{error}"
            );
            assert!(!error.contains("secret"));
        }
    }

    #[test]
    fn explicit_aggregate_operands_use_source_not_result_namespace() {
        let query = SelectQuery::new("School")
            .sum("capacity", "capacity")
            .having(Expr::binary(
                Expr::sum_expr(Expr::column("capacity")),
                BinaryOp::Gt,
                Expr::value(4),
            ));
        assert_eq!(
            bind(&query, query.having.as_ref().unwrap()).unwrap(),
            query.having.clone().unwrap()
        );
        assert!(!requires_fresh_compilation(&query).unwrap());
    }

    #[test]
    fn ambiguous_alias_and_unselected_name_are_rejected() {
        let ambiguous = SelectQuery::new("School")
            .count("total")
            .sum("capacity", "total")
            .having(Expr::gt("total", 1));
        assert!(
            Dialect
                .compile_select(&entity(), &ambiguous)
                .unwrap_err()
                .to_string()
                .contains("ambiguous")
        );
        let unknown = SelectQuery::new("School")
            .count("schools")
            .having(Expr::gt("missing", 1));
        assert!(Dialect.compile_select(&entity(), &unknown).is_err());
        let nested = SelectQuery::new("School")
            .sum("capacity", "total")
            .having(Expr::binary(
                Expr::sum_expr(Expr::column("total")),
                BinaryOp::Gt,
                Expr::value(1),
            ));
        assert!(Dialect.compile_select(&entity(), &nested).is_err());
    }

    #[test]
    fn parameterized_alias_inside_subquery_requires_fresh_outer_compilation() {
        let nested = SelectQuery::new("School")
            .project_expr("total", Expr::sum_expr(Expr::value(7)))
            .having(Expr::gt("total", 2));
        let outer = SelectQuery::new("School").filter(Expr::SubQuery {
            left: Box::new(Expr::column("id")),
            op: BinaryOp::In,
            entity: entity(),
            query: Box::new(nested),
        });
        assert!(requires_fresh_compilation(&outer).unwrap());
        let compiled = Dialect.compile_select(&entity(), &outer).unwrap();
        assert_eq!(
            compiled.params,
            [Value::I64(7), Value::I64(7), Value::I64(2)]
        );
        assert!(compiled.sql.contains("HAVING (SUM($2) > $3)"));
    }

    #[test]
    fn statistical_order_binds_alias_and_recompiles_parameterized_projection() {
        let query = SelectQuery::new("School")
            .project_expr("total", Expr::sum_expr(Expr::value(7)))
            .count("schools")
            .order_desc("total");
        let compiled = Dialect.compile_select(&entity(), &query).unwrap();
        assert!(compiled.sql.ends_with("ORDER BY SUM($2) DESC"));
        assert_eq!(compiled.params, [Value::I64(7), Value::I64(7)]);
        assert!(requires_fresh_compilation(&query).unwrap());
    }

    #[test]
    fn ordinary_order_rejects_unselected_fields_and_ambiguous_results() {
        let base = SelectQuery::new("School")
            .group_by("id")
            .sum("capacity", "total");
        for query in [
            base.clone().order_desc("capacity"),
            base.clone().order_desc("missing"),
            base.clone()
                .order_expr_desc(Expr::sum_expr(Expr::column("id"))),
            base.order_expr_desc(Expr::value("PRIVATE-ORDER-OPERAND")),
        ] {
            let error = Dialect
                .compile_select(&entity(), &query)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("not a selected statistical result"),
                "{error}"
            );
            assert!(!error.contains("PRIVATE-ORDER-OPERAND"));
        }
        let query = SelectQuery::new("School")
            .group_by_expr("a", Expr::column("capacity"))
            .group_by_expr("b", Expr::identity(Expr::column("capacity")))
            .count("schools")
            .order_desc("capacity");
        let error = Dialect
            .compile_select(&entity(), &query)
            .unwrap_err()
            .to_string();
        assert!(error.contains("ambiguous statistical order"), "{error}");
    }

    #[test]
    fn ordinary_order_retains_selected_identity_alias_and_trusted_parameters() {
        let query = SelectQuery::new("School")
            .group_by_expr("bucket", Expr::identity(Expr::column("capacity")))
            .count("schools")
            .filter(Expr::gt("capacity", 7))
            .order_desc("capacity");
        let original = query.clone();
        let compiled = Dialect.compile_select(&entity(), &query).unwrap();
        assert!(compiled.sql.ends_with("ORDER BY \"Legacy Capacity\" DESC"));
        assert_eq!(compiled.params, [Value::I64(7)]);
        assert_eq!(
            compiled.log_context.parameter_policies,
            [SqlParameterLogPolicy::Masked]
        );
        assert_eq!(query, original);
    }

    #[test]
    fn partitioned_statistics_cannot_bypass_selected_order_validation() {
        let query = SelectQuery::new("School")
            .group_by("id")
            .count("schools")
            .order_desc("capacity")
            .partition_by("id")
            .limit(1);
        let error = Dialect
            .compile_select(&entity(), &query)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("not a selected statistical result"),
            "{error}"
        );
    }

    #[test]
    fn gbk_order_is_preserved_only_for_a_selected_result() {
        let query = SelectQuery::new("School").group_by("id").count("schools");
        let error = Dialect
            .compile_select(&entity(), &query.order_gbk_asc("capacity"))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("not a selected statistical result"),
            "{error}"
        );
        let query = SelectQuery::new("School")
            .group_by("id")
            .count("schools")
            .order_by(teaql_core::OrderBy::asc_expr(Expr::function(
                F::Gbk,
                Vec::new(),
            )));
        assert!(
            Dialect
                .compile_select(&entity(), &query)
                .unwrap_err()
                .to_string()
                .contains("GBK expects exactly one argument")
        );
    }

    #[test]
    fn default_dialect_cannot_claim_gbk_by_returning_the_same_field() {
        let entity = entity().property(PropertyDescriptor::new("name", DataType::Text));
        let query = SelectQuery::new("School")
            .group_by("name")
            .count("schools")
            .order_gbk_asc("name");
        let error = Dialect
            .compile_select(&entity, &query)
            .unwrap_err()
            .to_string();
        assert!(error.contains("GBK is unsupported"), "{error}");
    }

    #[test]
    fn duplicate_grouping_alias_fails_without_having_or_ordering() {
        let query = SelectQuery::new("School")
            .group_by_expr("band", Expr::column("capacity"))
            .project_expr("band", Expr::column("id"))
            .count("schools");
        assert!(
            Dialect
                .compile_select(&entity(), &query)
                .unwrap_err()
                .to_string()
                .contains("ambiguous grouping")
        );
    }
}
