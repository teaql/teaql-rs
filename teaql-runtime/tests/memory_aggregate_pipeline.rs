use std::collections::BTreeMap;

use teaql_core::{Aggregate, CompactRow, Expr, OrderBy, SelectQuery, Value};
use teaql_runtime::InMemoryQueryEngine;

fn rows() -> Vec<CompactRow> {
    [("A", 4), ("B", 2), ("A", 6), ("C", 3), ("C", 5)]
        .into_iter()
        .map(|(bucket, amount)| {
            CompactRow::from_map(BTreeMap::from([
                ("bucket".to_owned(), Value::from(bucket)),
                ("amount".to_owned(), Value::I64(amount)),
            ]))
        })
        .collect()
}

fn grouped() -> SelectQuery {
    SelectQuery::new("PipelineProbe")
        .group_by("bucket")
        .aggregate(Aggregate::sum("amount", "total"))
}

fn names(rows: &[CompactRow]) -> Vec<&str> {
    rows.iter()
        .map(|row| row.get("bucket").unwrap().try_text().unwrap())
        .collect()
}

#[test]
fn memory_aggregate_pipeline_having_filters_aggregate_aliases() {
    let query = grouped().having(Expr::gt("total", 5_i64));
    let result = InMemoryQueryEngine::execute(&query, rows());
    assert_eq!(names(&result.rows), ["A", "C"]);
    assert_eq!(result.metadata.result_count, Some(2));
}

#[test]
fn memory_aggregate_pipeline_orders_before_limit_and_offset() {
    let query = grouped()
        .order_by(OrderBy::desc("total"))
        .offset(1)
        .limit(1);
    let result = InMemoryQueryEngine::execute(&query, rows());
    assert_eq!(names(&result.rows), ["C"]);
    assert_eq!(result.metadata.result_count, Some(1));
}

#[test]
fn memory_aggregate_pipeline_having_precedes_sort_and_slice() {
    let query = grouped()
        .having(Expr::gt("total", 5_i64))
        .order_by(OrderBy::asc("total"))
        .limit(1);
    let result = InMemoryQueryEngine::execute(&query, rows());
    assert_eq!(names(&result.rows), ["C"]);
    assert_eq!(result.metadata.result_count, Some(1));
}

#[test]
fn memory_aggregate_pipeline_having_may_remove_a_scalar_aggregate_row() {
    let query = SelectQuery::new("PipelineProbe")
        .aggregate(Aggregate::count("n"))
        .having(Expr::gt("n", 50_i64));
    let result = InMemoryQueryEngine::execute(&query, rows());
    assert!(result.rows.is_empty());
    assert_eq!(result.metadata.result_count, Some(0));
}

#[test]
fn memory_aggregate_pipeline_limit_zero_removes_scalar_result_without_changing_count() {
    let query = SelectQuery::new("PipelineProbe")
        .aggregate(Aggregate::count("n"))
        .limit(0);
    let result = InMemoryQueryEngine::execute(&query, rows());
    assert!(result.rows.is_empty());
    assert_eq!(result.metadata.result_count, Some(0));
}

#[test]
fn memory_aggregate_pipeline_where_still_runs_before_grouping() {
    let query = grouped().and_filter(Expr::gt("amount", 3_i64));
    let result = InMemoryQueryEngine::execute(&query, rows());
    assert_eq!(names(&result.rows), ["A", "C"]);
    assert_eq!(
        result.rows[0].get("total").and_then(Value::try_f64),
        Some(10.0)
    );
    assert_eq!(
        result.rows[1].get("total").and_then(Value::try_f64),
        Some(5.0)
    );
}

#[test]
fn memory_aggregate_pipeline_empty_grouped_input_stays_empty() {
    let result =
        InMemoryQueryEngine::execute(&grouped().order_by(OrderBy::desc("total")).limit(1), vec![]);
    assert!(result.rows.is_empty());
    assert_eq!(result.metadata.result_count, Some(0));
}
