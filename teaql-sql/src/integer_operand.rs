//! Shared bitwise input typing, before SQL rendering or parameter binding.
use crate::SqlCompileError;
use teaql_core::{DataType, EntityDescriptor, Expr, ExprFunction, Value};

pub(crate) fn field(entity: &EntityDescriptor, name: &str) -> Result<(), SqlCompileError> {
    if entity
        .property_by_name(name)
        .is_some_and(|property| matches!(property.data_type, DataType::I64 | DataType::U64))
    {
        Ok(())
    } else {
        Err(invalid("bitwise aggregate requires an integer field"))
    }
}

/// True means the operand is a scalar parameter. SELF does not change its type.
pub(crate) fn expression(
    entity: &EntityDescriptor,
    mut operand: &Expr,
) -> Result<bool, SqlCompileError> {
    for _ in 0..=64 {
        match operand {
            Expr::Function {
                function: ExprFunction::Identity,
                args,
            } => {
                let [argument] = args.as_slice() else {
                    return Err(invalid("bitwise SELF expects one operand"));
                };
                operand = argument;
            }
            Expr::Column(name) => {
                field(entity, name)?;
                return Ok(false);
            }
            Expr::Value(
                Value::I64(_) | Value::Null | Value::TypedNull(DataType::I64 | DataType::U64),
            ) => return Ok(true),
            Expr::Value(Value::U64(value)) if *value <= i64::MAX as u64 => return Ok(true),
            _ => {
                return Err(invalid(
                    "bitwise expression requires a signed 64-bit integer operand",
                ));
            }
        }
    }
    Err(invalid("bitwise operand exceeds 64 wrappers"))
}
fn invalid(message: &str) -> SqlCompileError {
    SqlCompileError::InvalidFunctionArguments(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use teaql_core::{Decimal, PropertyDescriptor};

    #[test]
    fn field_and_self_keep_the_modeled_type() {
        for data_type in [
            DataType::I64,
            DataType::U64,
            DataType::Bool,
            DataType::F64,
            DataType::Decimal,
            DataType::Text,
            DataType::LargeText,
            DataType::Json,
            DataType::Date,
            DataType::Timestamp,
        ] {
            let entity = EntityDescriptor::new("Typed")
                .property(PropertyDescriptor::new("value", data_type));
            for depth in [0, 1, 2, 64] {
                let mut operand = Expr::column("value");
                for _ in 0..depth {
                    operand = Expr::identity(operand);
                }
                let original = operand.clone();
                let result = expression(&entity, &operand);
                if matches!(data_type, DataType::I64 | DataType::U64) {
                    assert!(!result.unwrap());
                } else {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .contains("requires an integer field")
                    );
                }
                assert_eq!(operand, original);
            }
        }
    }
    #[test]
    fn scalar_parameters_preserve_signed_bounds_and_null_types() {
        let entity = EntityDescriptor::new("Typed");
        for value in [
            Value::I64(i64::MIN),
            Value::I64(i64::MAX),
            Value::U64(i64::MAX as u64),
            Value::Null,
            Value::TypedNull(DataType::I64),
            Value::TypedNull(DataType::U64),
        ] {
            assert!(expression(&entity, &Expr::identity(Expr::value(value))).unwrap());
        }
        for value in [
            Value::U64(i64::MAX as u64 + 1),
            Value::F64(1.0),
            Value::Decimal(Decimal::ONE),
            Value::Bool(true),
            Value::Text("PRIVATE-BITWISE-CANARY".into()),
            Value::TypedNull(DataType::Bool),
        ] {
            let error = expression(&entity, &Expr::value(value))
                .unwrap_err()
                .to_string();
            assert!(error.contains("signed 64-bit integer operand"));
            assert!(!error.contains("CANARY"));
        }
    }
    #[test]
    fn malformed_and_deep_wrappers_fail_without_recursive_inspection() {
        let entity = EntityDescriptor::new("Typed");
        for arguments in [vec![], vec![Expr::value(1), Expr::value(2)]] {
            assert!(
                expression(&entity, &Expr::function(ExprFunction::Identity, arguments))
                    .unwrap_err()
                    .to_string()
                    .contains("one operand")
            );
        }
        let mut deep = Expr::value(1);
        for _ in 0..66 {
            deep = Expr::identity(deep);
        }
        assert!(
            expression(&entity, &deep)
                .unwrap_err()
                .to_string()
                .contains("64 wrappers")
        );
    }
    #[test]
    fn aggregate_and_boolean_expressions_are_not_integer_scalar_operands() {
        let entity = EntityDescriptor::new("Typed");
        for operand in [
            Expr::count_all(),
            Expr::soundex(Expr::value("PRIVATE-BITWISE-CANARY")),
            Expr::eq("value", 1),
        ] {
            let error = expression(&entity, &operand).unwrap_err().to_string();
            assert!(error.contains("integer operand"));
            assert!(!error.contains("CANARY"));
        }
    }
}
