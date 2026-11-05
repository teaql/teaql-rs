use std::collections::BTreeMap;
use std::str::FromStr;

use rust_decimal::Decimal;
use teaql_core::{
    Aggregate, CompactRow, DataType, EntityDescriptor, PropertyDescriptor, SelectQuery, Value,
};
use teaql_runtime::{InMemoryMetadataStore, memory::MemoryDataService};

fn sum(values: Vec<Value>) -> Result<Value, String> {
    let metadata = InMemoryMetadataStore::new().with_entity(
        EntityDescriptor::new("SumProbe")
            .property(PropertyDescriptor::new("amount", DataType::Decimal)),
    );
    let rows: Vec<_> = values
        .into_iter()
        .map(|value| CompactRow::from_map(BTreeMap::from([("amount".into(), value)])))
        .collect();
    let service = MemoryDataService::new(metadata).with_rows("SumProbe", rows);
    let result = service
        .fetch_all(&SelectQuery::new("SumProbe").aggregate(Aggregate::sum("amount", "total")))
        .map_err(|error| error.to_string())?;
    assert_eq!(result.len(), 1);
    Ok(result[0]
        .get("total")
        .expect("selected SUM must be present")
        .clone())
}

fn decimal(value: &str) -> Value {
    Value::Decimal(Decimal::from_str(value).unwrap())
}

#[test]
fn memory_sum_rejects_unsigned_result_overflow_instead_of_wrapping() {
    assert!(sum(vec![Value::U64(u64::MAX), Value::U64(1)]).is_err());
}

#[test]
fn memory_sum_rejects_signed_result_underflow_instead_of_wrapping() {
    assert!(sum(vec![Value::I64(i64::MIN), Value::I64(-1)]).is_err());
}

#[test]
fn memory_sum_rejects_decimal_range_overflow_without_panicking() {
    assert!(sum(vec![Value::Decimal(Decimal::MAX), decimal("1")]).is_err());
}

#[test]
fn memory_sum_rejects_decimal_precision_loss_instead_of_rounding() {
    assert!(sum(vec![Value::Decimal(Decimal::MAX), decimal("0.1")]).is_err());
}

#[test]
fn memory_sum_nonfinite_and_unrepresentable_float_never_become_zero() {
    for value in [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MAX,
        -f64::MAX,
    ] {
        assert!(
            sum(vec![Value::F64(value)]).is_err(),
            "invalid floating SUM was accepted"
        );
    }
}

#[test]
fn memory_sum_invalid_input_errors_identify_field_without_disclosing_value() {
    let error = sum(vec![Value::from("private-sum-payload")]).unwrap_err();
    assert!(error.contains("amount"));
    assert!(!error.contains("private-sum-payload"));
}

#[test]
fn memory_sum_exact_decimal_cancellation_is_order_independent() {
    for values in [
        vec![
            Value::Decimal(Decimal::MAX),
            decimal("1"),
            Value::Decimal(Decimal::MIN),
        ],
        vec![
            Value::Decimal(Decimal::MAX),
            Value::Decimal(Decimal::MIN),
            decimal("1"),
        ],
    ] {
        assert_eq!(sum(values).unwrap(), decimal("1"));
    }
}

#[test]
fn memory_sum_exact_decimal_scale_cancellation_is_order_independent() {
    for values in [
        vec![
            Value::Decimal(Decimal::MAX),
            decimal("0.0000000000000000000000000001"),
            Value::Decimal(Decimal::MIN),
        ],
        vec![
            Value::Decimal(Decimal::MAX),
            Value::Decimal(Decimal::MIN),
            decimal("0.0000000000000000000000000001"),
        ],
    ] {
        assert_eq!(
            sum(values).unwrap(),
            decimal("0.0000000000000000000000000001")
        );
    }
}

#[test]
fn memory_sum_null_is_distinct_from_numeric_zero() {
    assert_eq!(sum(vec![]).unwrap(), Value::Null);
    assert_eq!(sum(vec![Value::Null, Value::Null]).unwrap(), Value::Null);
    assert_eq!(
        sum(vec![Value::I64(4), Value::Null, Value::I64(-4)]).unwrap(),
        Value::U64(0)
    );
    assert_eq!(
        sum(vec![decimal("0.1"), decimal("-0.1")]).unwrap(),
        decimal("0")
    );
}

#[test]
fn memory_sum_legal_integer_boundaries_remain_exact() {
    for (values, expected) in [
        (vec![Value::U64(u64::MAX)], Value::U64(u64::MAX)),
        (vec![Value::I64(i64::MIN)], Value::I64(i64::MIN)),
        (
            vec![Value::U64(u64::MAX), Value::I64(i64::MIN)],
            Value::U64(i64::MAX as u64),
        ),
    ] {
        assert_eq!(sum(values).unwrap(), expected);
    }
}

#[test]
fn memory_sum_mixed_decimal_integer_and_finite_float_controls() {
    assert_eq!(
        sum(vec![decimal("0.1"), decimal("0.2"), Value::I64(2)]).unwrap(),
        decimal("2.3")
    );
    assert_eq!(
        sum(vec![Value::F64(0.5), Value::U64(2)]).unwrap(),
        decimal("2.5")
    );
    assert_eq!(
        sum(vec![Value::Decimal(Decimal::MAX), decimal("0")]).unwrap(),
        Value::Decimal(Decimal::MAX)
    );
}
