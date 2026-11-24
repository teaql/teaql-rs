//! Pure binding of aggregate projections. Provider SQL and Context stay outside
//! this layer; a typed scalar input is evaluated before owner deduplication.
use super::{MeasureBinding, StatisticalPlanError};
use crate::{
    Aggregate, AggregateFunction as A, DataType as T, EntityDescriptor, Expr, ExprFunction as F,
    Value,
};

pub(super) fn bind(
    descriptor: &EntityDescriptor,
    owner: usize,
    path: Vec<String>,
    alias: &str,
    mut expression: &Expr,
) -> Result<Option<MeasureBinding>, StatisticalPlanError> {
    let mut depth = 0;
    while let Expr::Function {
        function: F::Identity,
        args,
    } = expression
    {
        if depth >= 64 || args.len() != 1 {
            return Err(invalid(
                "aggregate projection has malformed or oversized Identity wrappers",
            ));
        }
        expression = &args[0];
        depth += 1;
    }
    let Expr::Function { function, args } = expression else {
        return Ok(None);
    };
    let function = match function {
        F::Count => A::Count,
        F::CountDistinct => A::CountDistinct,
        F::Sum => A::Sum,
        F::Avg => A::Avg,
        F::Min => A::Min,
        F::Max => A::Max,
        F::Stddev => A::Stddev,
        F::StddevPop => A::StddevPop,
        F::VarSamp => A::VarSamp,
        F::VarPop => A::VarPop,
        F::BitAnd => A::BitAnd,
        F::BitOr => A::BitOr,
        F::BitXor => A::BitXor,
        F::Identity | F::Soundex | F::Gbk => return Ok(None),
    };
    if alias.trim().is_empty() {
        return Err(invalid("statistical measure alias must not be empty"));
    }
    if function == A::Count && args.is_empty() {
        return Ok(Some(MeasureBinding {
            owner_layer: owner,
            aggregate: Aggregate::count(alias),
            output_path: path,
            operand: None,
            input_field: None,
            input_type: T::I64,
        }));
    }
    let [operand] = args.as_slice() else {
        return Err(invalid(
            "aggregate projection expects exactly one scalar operand",
        ));
    };
    let (input_type, input_field) = scalar(descriptor, operand, depth + 1)?;
    match function {
        A::Sum | A::Avg | A::Stddev | A::StddevPop | A::VarSamp | A::VarPop
            if !matches!(input_type, T::I64 | T::U64 | T::F64 | T::Decimal) =>
        {
            return Err(invalid(
                "numeric aggregate requires a numeric scalar operand",
            ));
        }
        A::BitAnd | A::BitOr | A::BitXor if !matches!(input_type, T::I64 | T::U64) => {
            return Err(invalid(
                "bitwise aggregate requires an integer field or scalar operand",
            ));
        }
        A::Min | A::Max if matches!(input_type, T::Bool | T::Json) => {
            return Err(invalid(
                "ordered aggregate requires an ordered scalar operand",
            ));
        }
        _ => (),
    }
    Ok(Some(MeasureBinding {
        owner_layer: owner,
        aggregate: Aggregate::new(function, input_field.as_deref().unwrap_or("*"), alias),
        output_path: path,
        operand: Some(operand.clone()),
        input_field,
        input_type,
    }))
}

fn scalar(
    descriptor: &EntityDescriptor,
    expression: &Expr,
    depth: usize,
) -> Result<(T, Option<String>), StatisticalPlanError> {
    if depth > 64 {
        return Err(invalid("statistical measure expression exceeds 64 levels"));
    }
    match expression {
        Expr::Column(field) => descriptor
            .property_by_name(field)
            .map(|property| (property.data_type, Some(field.clone())))
            .ok_or_else(|| invalid("unknown logical statistical operand field")),
        Expr::Function {
            function: F::Identity,
            args,
        } if args.len() == 1 => scalar(descriptor, &args[0], depth + 1),
        Expr::Function {
            function: F::Soundex,
            args,
        } if args.len() == 1 => {
            let (kind, field) = scalar(descriptor, &args[0], depth + 1)?;
            if !matches!(kind, T::Text | T::LargeText) {
                return Err(invalid("SOUNDEX measure operand requires text"));
            }
            Ok((T::Text, field))
        }
        Expr::Value(value) => Ok((
            match value {
                Value::Null => T::I64,
                Value::TypedNull(kind) => *kind,
                Value::I64(_) => T::I64,
                Value::U64(value) if *value <= i64::MAX as u64 => T::U64,
                Value::F64(value) if value.is_finite() => T::F64,
                Value::Decimal(_) => T::Decimal,
                Value::Text(_) => T::Text,
                Value::Bool(_) => T::Bool,
                Value::Date(_) => T::Date,
                Value::Timestamp(_) => T::Timestamp,
                Value::Json(_) => T::Json,
                _ => {
                    return Err(invalid(
                        "statistical measure requires a finite scalar parameter within the provider domain",
                    ));
                }
            },
            None,
        )),
        _ => Err(invalid(
            "statistical measure requires a scalar operand, not a nested aggregate or subquery",
        )),
    }
}
fn invalid(message: &str) -> StatisticalPlanError {
    StatisticalPlanError(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Decimal, PropertyDescriptor as P};
    fn descriptor() -> EntityDescriptor {
        EntityDescriptor::new("Owner")
            .property(P::new("id", T::I64).id())
            .property(P::new("amount", T::Decimal).column_name("Legacy Amount"))
            .property(P::new("name", T::Text).column_name("Legacy Name"))
    }
    fn measure(expression: &Expr) -> Result<MeasureBinding, StatisticalPlanError> {
        bind(
            &descriptor(),
            1,
            vec!["owner".into(), "result".into()],
            "result",
            expression,
        )?
        .ok_or_else(|| invalid("not an aggregate projection"))
    }
    #[test]
    fn every_aggregate_preserves_scalar_field_and_owner_binding() {
        for function in [
            F::Count,
            F::CountDistinct,
            F::Sum,
            F::Avg,
            F::Min,
            F::Max,
            F::Stddev,
            F::StddevPop,
            F::VarSamp,
            F::VarPop,
            F::BitAnd,
            F::BitOr,
            F::BitXor,
        ] {
            let operand = Expr::identity(Expr::column("id"));
            let expression = Expr::function(function, [operand.clone()]);
            let before = expression.clone();
            let bound = measure(&expression).unwrap();
            assert_eq!(bound.owner_layer, 1);
            assert_eq!(bound.input_field.as_deref(), Some("id"));
            assert_eq!(bound.operand, Some(operand));
            assert_eq!(bound.input_type, T::I64);
            assert!(!bound.all_rows());
            assert_eq!(bound.output_path, ["owner", "result"]);
            assert_eq!(expression, before);
        }
        assert!(measure(&Expr::count_all()).unwrap().all_rows());
    }
    #[test]
    fn transformed_text_and_exact_decimal_inputs_retain_their_domains() {
        let bound = measure(&Expr::count_distinct(Expr::soundex(Expr::identity(
            Expr::column("name"),
        ))))
        .unwrap();
        assert_eq!(bound.input_type, T::Text);
        assert_eq!(bound.input_field.as_deref(), Some("name"));
        for operand in [
            Expr::identity(Expr::column("amount")),
            Expr::value(Decimal::new(1, 9)),
        ] {
            let bound = measure(&Expr::sum_expr(operand.clone())).unwrap();
            assert_eq!(bound.input_type, T::Decimal);
            assert_eq!(bound.operand, Some(operand));
        }
    }
    #[test]
    fn scalar_parameters_keep_values_types_and_real_nulls() {
        for (value, kind) in [
            (Value::I64(i64::MIN), T::I64),
            (Value::I64(i64::MAX), T::I64),
            (Value::F64(1.25), T::F64),
            (Value::Null, T::I64),
            (Value::TypedNull(T::Decimal), T::Decimal),
            (Value::Text("private literal".into()), T::Text),
            (Value::Bool(true), T::Bool),
        ] {
            let expression = Expr::count_expr(Expr::identity(Expr::value(value.clone())));
            let bound = measure(&expression).unwrap();
            assert_eq!(bound.input_type, kind);
            assert_eq!(bound.operand, Some(Expr::identity(Expr::value(value))));
            assert!(bound.input_field.is_none());
        }
    }
    #[test]
    fn nested_aggregates_malformed_arguments_and_coercions_are_rejected() {
        for expression in [
            Expr::sum_expr(Expr::count_all()),
            Expr::sum_expr(Expr::value("SECRET-CANARY")),
            Expr::avg_expr(Expr::value(true)),
            Expr::min_expr(Expr::value(false)),
            Expr::bit_xor_expr(Expr::value(1.0)),
            Expr::sum_expr(Expr::gbk(Expr::column("name"))),
            Expr::count_expr(Expr::value(Value::List(vec![Value::I64(1)]))),
            Expr::count_expr(Expr::value(Value::U64(u64::MAX))),
            Expr::sum_expr(Expr::value(f64::NAN)),
            Expr::function(F::Avg, []),
            Expr::function(F::Sum, [Expr::value(1), Expr::value(2)]),
        ] {
            let error = measure(&expression).unwrap_err().to_string();
            assert!(!error.contains("CANARY"));
        }
    }
    #[test]
    fn expression_depth_aliases_and_source_fields_are_checked_without_io() {
        let mut deep = Expr::column("id");
        for _ in 0..66 {
            deep = Expr::identity(deep);
        }
        assert!(
            measure(&Expr::sum_expr(deep))
                .unwrap_err()
                .to_string()
                .contains("64 levels")
        );
        assert!(measure(&Expr::sum_expr(Expr::column("Legacy Amount"))).is_err());
        assert!(bind(&descriptor(), 0, vec![], " ", &Expr::count_all()).is_err());
        assert!(
            bind(
                &descriptor(),
                0,
                vec![],
                "name",
                &Expr::identity(Expr::column("name"))
            )
            .unwrap()
            .is_none()
        );
    }
}
