use std::collections::BTreeMap;

use teaql_core::{EntityDescriptor, Record, RelationAggregate, SelectQuery, Value};
use teaql_sql::CompiledQuery;

use crate::{GraphNode, RepositoryError, RuntimeError};

use super::{AggregationCacheBackend, RelationLoadPlan};

pub(super) fn relation_bucket_key(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(v) => format!("b:{v}"),
        Value::I64(v) => format!("i:{v}"),
        Value::U64(v) => format!("u:{v}"),
        Value::F64(v) => format!("f:{v}"),
        Value::Decimal(v) => format!("d:{v}"),
        Value::Text(v) => format!("t:{v}"),
        Value::Json(v) => format!("j:{v}"),
        Value::Date(v) => format!("d:{v}"),
        Value::Timestamp(v) => format!("ts:{}", v.to_rfc3339()),
        Value::Object(_) => "o".to_owned(),
        Value::List(_) => "l".to_owned(),
    }
}

pub(super) fn aggregation_cache_namespace(entity: &str) -> String {
    format!("entity:{entity}")
}

pub(super) fn invalidate_aggregation_cache_namespace(
    cache: &dyn AggregationCacheBackend,
    entity: &str,
) {
    let namespace = format!(
        "{}::{}",
        cache.namespace(),
        aggregation_cache_namespace(entity)
    );
    cache.invalidate_namespace(&namespace);
}

pub(super) fn aggregation_cache_key(
    cache_namespace: &str,
    query_namespace: &str,
    query: &CompiledQuery,
) -> String {
    format!(
        "{cache_namespace}::{query_namespace}::{}::{:?}",
        query.sql, query.params
    )
}

pub(super) fn ensure_projection(query: &mut SelectQuery, field: &str) {
    if !query.projection.is_empty()
        && !query
            .projection
            .iter()
            .any(|projection| projection == field)
    {
        query.projection.push(field.to_owned());
    }
}

pub(super) fn attach_empty_relation_aggregate(
    parent_rows: &mut [Record],
    alias: &str,
    single_result: bool,
) {
    let value = if single_result {
        Value::U64(0)
    } else {
        Value::List(Vec::new())
    };
    for parent in parent_rows {
        parent.insert(alias.to_owned(), value.clone());
    }
}

pub(super) fn attach_relation_aggregate_rows(
    parent_rows: &mut [Record],
    plan: &RelationLoadPlan,
    aggregate: &RelationAggregate,
    aggregate_rows: Vec<Record>,
) {
    let mut buckets: BTreeMap<String, Vec<Record>> = BTreeMap::new();
    for mut row in aggregate_rows {
        if let Some(key) = row.remove(&plan.foreign_key) {
            buckets
                .entry(relation_bucket_key(&key))
                .or_default()
                .push(row);
        }
    }

    for parent in parent_rows {
        let value = parent
            .get(&plan.local_key)
            .and_then(|local_value| buckets.get(&relation_bucket_key(local_value)))
            .map(|rows| relation_aggregate_value(rows, aggregate.single_result))
            .unwrap_or_else(|| {
                if aggregate.single_result {
                    Value::U64(0)
                } else {
                    Value::List(Vec::new())
                }
            });
        parent.insert(aggregate.alias.clone(), value);
    }
}

pub(super) fn relation_aggregate_value(rows: &[Record], single_result: bool) -> Value {
    if single_result {
        rows.first()
            .map(single_relation_aggregate_value)
            .unwrap_or(Value::U64(0))
    } else {
        Value::List(rows.iter().cloned().map(Value::object).collect())
    }
}

pub(super) fn single_relation_aggregate_value(row: &Record) -> Value {
    if row.len() == 1 {
        row.values().next().cloned().unwrap_or(Value::Null)
    } else {
        Value::object(row.clone())
    }
}

pub(super) fn graph_record_version(record: &Record, descriptor: &EntityDescriptor) -> Option<i64> {
    descriptor
        .version_property()
        .and_then(|property| match record.get(&property.name) {
            Some(Value::I64(version)) => Some(*version),
            _ => None,
        })
}

pub(super) fn graph_identity_key(value: &Value) -> String {
    match value {
        Value::I64(value) if *value >= 0 => format!("u:{}", *value as u64),
        Value::U64(value) => format!("u:{value}"),
        _ => relation_bucket_key(value),
    }
}

pub(super) fn ensure_relation_target<ExecError>(
    parent_entity: &str,
    relation_name: &str,
    expected_entity: &str,
    child: &GraphNode,
) -> Result<(), RepositoryError<ExecError>> {
    if child.entity == expected_entity {
        return Ok(());
    }
    Err(RepositoryError::Runtime(RuntimeError::Graph(format!(
        "relation {parent_entity}.{relation_name} expects {expected_entity}, got {}",
        child.entity
    ))))
}
