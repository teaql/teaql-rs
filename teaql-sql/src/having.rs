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
            if matches!(function, F::Soundex | F::Gbk) {
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

fn aggregate_expr(aggregate: &Aggregate) -> Expr {
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
}
