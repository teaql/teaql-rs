//! Public production-engine probe, not the cfg(test)-only MemoryDataService.
use std::{collections::BTreeMap, str::FromStr};

use teaql_core::{Aggregate, CompactRow, Decimal, SelectQuery, Value};
use teaql_runtime::InMemoryQueryEngine;

fn decimal(text: &str) -> Value {
    Value::Decimal(Decimal::from_str(text).unwrap())
}

fn rows(values: Vec<Value>) -> Vec<CompactRow> {
    values
        .into_iter()
        .map(|value| CompactRow::from_map(BTreeMap::from([("amount".to_owned(), value)])))
        .collect()
}

fn query() -> SelectQuery {
    SelectQuery::new("NumericProbe")
        .aggregate(Aggregate::sum("amount", "total"))
        .limit(10)
        .comment("measure exact memory SUM results")
        .purpose("verify the existing numeric precision contract")
}

fn execute(query: &SelectQuery, rows: Vec<CompactRow>) -> teaql_data_service::QueryResult {
    InMemoryQueryEngine::execute(query, rows).expect("exact SUM is representable")
}

fn sum(values: Vec<Value>) -> Value {
    let result = execute(&query(), rows(values));
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.metadata.result_count, Some(1));
    result.rows[0].get("total").unwrap().clone()
}

#[test]
fn memory_sum_decimal_fraction_stays_exact() {
    assert_eq!(sum(vec![decimal("0.1"), decimal("0.2")]), decimal("0.3"));
}

#[test]
fn memory_sum_large_decimal_keeps_cents() {
    assert_eq!(
        sum(vec![decimal("9007199254740992.01"), decimal("0.01")]),
        decimal("9007199254740992.02")
    );
}

#[test]
fn memory_sum_signed_integer_above_float_precision() {
    let actual = sum(vec![Value::I64(9007199254740993), Value::I64(2)]);
    assert_eq!(actual.try_i64(), Some(9007199254740995), "{actual:?}");
}

#[test]
fn memory_sum_unsigned_integer_near_maximum() {
    let actual = sum(vec![Value::U64(u64::MAX - 1), Value::U64(1)]);
    assert_eq!(actual.try_u64(), Some(u64::MAX), "{actual:?}");
}

#[test]
fn memory_sum_grouped_decimal_uses_same_exact_domain() {
    let mut values = rows(vec![decimal("0.1"), decimal("0.2")]);
    for row in &mut values {
        row.insert("bucket".to_owned(), Value::from("A"));
    }
    let result = execute(&query().group_by("bucket"), values);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("total"), Some(&decimal("0.3")));
}

#[test]
fn memory_sum_empty_scalar_is_present_null() {
    assert_eq!(sum(vec![]), Value::Null);
}

#[test]
fn memory_sum_all_null_is_present_null() {
    assert_eq!(sum(vec![Value::Null, Value::Null]), Value::Null);
}

#[test]
fn memory_sum_real_zero_is_not_null() {
    assert_eq!(
        sum(vec![Value::I64(5), Value::Null, Value::I64(-5)]).try_decimal(),
        Some(Decimal::ZERO)
    );
}

#[test]
fn memory_sum_does_not_change_count_semantics() {
    let query = query()
        .aggregate(Aggregate::count("rows"))
        .aggregate(Aggregate::count_field("amount", "values"));
    let result = execute(&query, rows(vec![Value::Null, Value::I64(1)]));
    assert_eq!(result.rows[0].get("rows"), Some(&Value::I64(2)));
    assert_eq!(result.rows[0].get("values"), Some(&Value::I64(1)));
}
