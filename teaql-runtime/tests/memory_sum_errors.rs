use std::{collections::BTreeMap, str::FromStr};

use teaql_core::{Aggregate, CompactRow, Decimal, SelectQuery, Value};
use teaql_runtime::{InMemoryQueryEngine, InMemoryQueryErrorKind as Kind};

fn decimal(text: &str) -> Value {
    Value::Decimal(Decimal::from_str(text).unwrap())
}

fn query() -> SelectQuery {
    SelectQuery::new("NumericProbe")
        .aggregate(Aggregate::sum("amount", "total"))
        .limit(10)
        .comment("inspect exact numeric aggregate boundaries")
        .purpose("prevent silent loss or coercion in memory SUM")
}

fn rows(values: Vec<Value>) -> Vec<CompactRow> {
    values
        .into_iter()
        .map(|value| CompactRow::from_map(BTreeMap::from([("amount".into(), value)])))
        .collect()
}

fn total(values: Vec<Value>) -> Value {
    InMemoryQueryEngine::execute(&query(), rows(values))
        .unwrap()
        .rows[0]
        .get("total")
        .unwrap()
        .clone()
}

fn reject(values: Vec<Value>, kind: Kind) {
    let query = query();
    let snapshot = format!("{query:?}");
    let error = InMemoryQueryEngine::execute(&query, rows(values)).unwrap_err();
    assert_eq!(error.kind, kind);
    assert_eq!(error.code(), kind.code());
    assert_eq!(error.field, "amount");
    assert_eq!(error.alias, "total");
    assert_eq!(format!("{query:?}"), snapshot);
    // A failed invocation cannot poison a later independent execution.
    assert_eq!(total(vec![Value::I64(1), Value::I64(2)]), Value::I64(3));
}

#[test]
fn signed_sum_reports_final_overflow() {
    reject(
        vec![Value::I64(i64::MAX), Value::I64(1)],
        Kind::NumericOverflow,
    );
    reject(
        vec![Value::I64(i64::MIN), Value::I64(-1)],
        Kind::NumericOverflow,
    );
}

#[test]
fn unsigned_sum_reports_final_overflow() {
    reject(
        vec![Value::U64(u64::MAX), Value::U64(1)],
        Kind::NumericOverflow,
    );
}

#[test]
fn intermediate_signed_overflow_can_cancel_before_final_carrier_check() {
    assert_eq!(
        total(vec![Value::I64(i64::MAX), Value::I64(1), Value::I64(-1)]),
        Value::I64(i64::MAX)
    );
}

#[test]
fn exact_decimal_accumulation_is_not_order_dependent_under_cancellation() {
    let max = Value::Decimal(Decimal::MAX);
    let negative = Value::Decimal(-Decimal::MAX);
    for values in [
        vec![max.clone(), max.clone(), negative.clone()],
        vec![negative.clone(), max.clone(), max.clone()],
        vec![max.clone(), negative, max.clone()],
    ] {
        assert_eq!(total(values), max);
    }
}

#[test]
fn decimal_overflow_is_an_error_not_rounding_or_infinity() {
    reject(
        vec![Value::Decimal(Decimal::MAX), Value::Decimal(Decimal::ONE)],
        Kind::NumericOverflow,
    );
}

#[test]
fn decimal_checked_add_rounding_is_not_mistaken_for_exact_arithmetic() {
    let large = Decimal::from_str("10000000000000000000000000000").unwrap();
    let tiny = Decimal::from_str("0.0000000000000000000000000001").unwrap();
    assert_eq!(large.checked_add(tiny), Some(large));
    reject(
        vec![Value::Decimal(large), Value::Decimal(tiny)],
        Kind::DecimalPrecisionLoss,
    );
}

#[test]
fn unrepresentable_intermediate_decimal_can_cancel_exactly() {
    assert_eq!(
        total(vec![
            decimal("10000000000000000000000000000"),
            decimal("0.0000000000000000000000000001"),
            decimal("-10000000000000000000000000000")
        ]),
        decimal("0.0000000000000000000000000001")
    );
}

#[test]
fn decimal_trailing_zero_reduction_is_exact_when_required() {
    assert_eq!(
        total(vec![
            decimal("3961408125713216879677197516.0"),
            decimal("3961408125713216879677197516.0")
        ]),
        decimal("7922816251426433759354395032")
    );
}

#[test]
fn mixed_exact_domains_preserve_all_unsigned_digits_and_fractional_scale() {
    assert_eq!(
        total(vec![Value::U64(u64::MAX), Value::I64(-1), decimal("0.5")]),
        decimal("18446744073709551614.5")
    );
    assert_eq!(
        total(vec![Value::I64(-1), Value::U64(u64::MAX)]),
        decimal("18446744073709551614")
    );
}

#[test]
fn decimal_sum_retains_existing_scale_and_typed_zero() {
    assert_eq!(
        total(vec![decimal("1.20"), Value::I64(2), decimal("0.003")]),
        decimal("3.203")
    );
    let Value::Decimal(zero) = total(vec![decimal("0.00"), Value::Null]) else {
        panic!("Decimal domain lost")
    };
    assert_eq!(zero.scale(), 2);
    assert_eq!(zero.to_string(), "0.00");
}

#[test]
fn finite_floating_domain_is_preserved_but_its_overflow_is_rejected() {
    assert_eq!(
        total(vec![Value::F64(1.5), Value::Null, Value::F64(-0.25)]),
        Value::F64(1.25)
    );
    reject(
        vec![Value::F64(f64::MAX), Value::F64(f64::MAX)],
        Kind::NumericOverflow,
    );
}

#[test]
fn non_finite_floating_inputs_are_not_silently_skipped() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        reject(vec![Value::F64(value)], Kind::NonFiniteValue);
    }
}

#[test]
fn mixing_floating_and_exact_domains_requires_explicit_caller_conversion() {
    for exact in [Value::I64(0), Value::U64(u64::MAX), decimal("0.1")] {
        reject(
            vec![exact.clone(), Value::F64(1.0)],
            Kind::MixedFloatingAndExact,
        );
        reject(vec![Value::F64(1.0), exact], Kind::MixedFloatingAndExact);
    }
}

#[test]
fn text_and_boolean_are_not_coerced_or_dropped_and_errors_do_not_echo_values() {
    for value in [
        Value::Text("PRIVATE-SUM-CANARY-12345".into()),
        Value::Text("123.45".into()),
        Value::Bool(true),
    ] {
        let error = InMemoryQueryEngine::execute(&query(), rows(vec![value])).unwrap_err();
        assert_eq!(error.kind, Kind::NonNumericValue);
        assert!(!format!("{error:?} {error}").contains("CANARY"));
        assert!(!format!("{error:?} {error}").contains("123.45"));
    }
}

#[test]
fn absent_and_null_inputs_remain_a_present_null_aggregate() {
    let result =
        InMemoryQueryEngine::execute(&query(), vec![CompactRow::from_map(BTreeMap::new())])
            .unwrap();
    assert_eq!(result.rows[0].get("total"), Some(&Value::Null));
    assert_eq!(result.metadata.result_count, Some(1));
}

#[test]
fn grouped_failure_rejects_the_entire_result_instead_of_returning_partial_groups() {
    let mut values = rows(vec![Value::I64(1), Value::I64(i64::MAX), Value::I64(1)]);
    for (i, row) in values.iter_mut().enumerate() {
        row.insert(
            "bucket".into(),
            Value::from(if i == 0 { "valid" } else { "overflow" }),
        );
    }
    let error = InMemoryQueryEngine::execute(&query().group_by("bucket"), values).unwrap_err();
    assert_eq!(error.kind, Kind::NumericOverflow);
}
