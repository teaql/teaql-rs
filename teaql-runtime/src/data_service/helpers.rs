use std::collections::BTreeMap;

use teaql_core::{
    BinaryOp, CompactRow, EntityDescriptor, Expr, RelationAggregate, SelectQuery, Value,
};

use crate::{DataServiceError, GraphNode, RuntimeError};

use super::{AggregationCacheBackend, RelationLoadPlan};

pub(super) fn default_aggregate_value(single_result: bool) -> Value {
    match single_result {
        true => Value::U64(0),
        false => Value::List(Vec::new()),
    }
}

pub(super) fn aggregate_alias(single_result: bool, alias: &str) -> String {
    match single_result {
        true => alias.to_owned(),
        false => "count".to_owned(),
    }
}

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
        Value::Timestamp(v) => format!("ts:{}", v.0),
        Value::Object(_) => "o".to_owned(),
        Value::List(_) => "l".to_owned(),
        Value::TypedNull(_) => "null".to_owned(),
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
    query: &SelectQuery,
) -> String {
    let query_str = format!("{:?}", canonical_aggregation_query(query));
    format!("{cache_namespace}::{query_namespace}::{query_str}")
}

fn canonical_aggregation_query(query: &SelectQuery) -> SelectQuery {
    let mut query = query.clone();
    query.filter = query.filter.take().map(canonical_expr);
    query.having = query.having.take().map(canonical_expr);
    for projection in &mut query.expr_projection {
        projection.expr = canonical_expr(projection.expr.clone());
    }
    for order in &mut query.order_by {
        order.expr = order.expr.take().map(canonical_expr);
    }
    for relation in &mut query.relations {
        if let Some(child) = relation.query.take() {
            relation.query = Some(Box::new(canonical_aggregation_query(&child)));
        }
    }
    for group in &mut query.object_group_bys {
        group.query = canonical_aggregation_query(&group.query);
    }
    for child in &mut query.child_enhancements {
        *child = canonical_aggregation_query(child);
    }

    // These fields control cache/runtime behavior or observability, not the
    // returned value. Including them fragments one semantic query into keys
    // based on TTL, comments, tracing, or streaming configuration.
    query.aggregation_cache = None;
    query.comment = None;
    query.trace_chain.clear();
    query.stream_config = None;
    query.continuous_page_fetch = None;
    query
}

fn canonical_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Function { function, args } => Expr::Function {
            function,
            args: args.into_iter().map(canonical_expr).collect(),
        },
        Expr::Binary { left, op, right } => {
            let left = canonical_expr(*left);
            let mut right = canonical_expr(*right);
            if matches!(
                op,
                BinaryOp::In | BinaryOp::NotIn | BinaryOp::InLarge | BinaryOp::NotInLarge
            ) {
                if let Expr::Value(Value::List(values)) = &mut right {
                    values.sort_by_cached_key(|value| format!("{value:?}"));
                    values.dedup();
                }
            }
            Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }
        }
        Expr::SubQuery {
            left,
            op,
            entity,
            query,
        } => Expr::SubQuery {
            left: Box::new(canonical_expr(*left)),
            op,
            entity,
            query: Box::new(canonical_aggregation_query(&query)),
        },
        Expr::Between { expr, lower, upper } => Expr::Between {
            expr: Box::new(canonical_expr(*expr)),
            lower: Box::new(canonical_expr(*lower)),
            upper: Box::new(canonical_expr(*upper)),
        },
        Expr::IsNull(expr) => Expr::IsNull(Box::new(canonical_expr(*expr))),
        Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(canonical_expr(*expr))),
        Expr::And(parts) => canonical_commutative(parts, true),
        Expr::Or(parts) => canonical_commutative(parts, false),
        Expr::Not(expr) => Expr::Not(Box::new(canonical_expr(*expr))),
        leaf => leaf,
    }
}

fn canonical_commutative(parts: Vec<Expr>, and: bool) -> Expr {
    let mut flattened = Vec::new();
    for part in parts.into_iter().map(canonical_expr) {
        match (and, part) {
            (true, Expr::And(nested)) | (false, Expr::Or(nested)) => flattened.extend(nested),
            (_, part) => flattened.push(part),
        }
    }
    flattened.sort_by_cached_key(|part| format!("{part:?}"));
    flattened.dedup();
    if and {
        Expr::And(flattened)
    } else {
        Expr::Or(flattened)
    }
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
    parent_rows: &mut [CompactRow],
    alias: &str,
    single_result: bool,
) {
    let value = default_aggregate_value(single_result);
    for parent in parent_rows {
        parent.insert(alias.to_owned(), value.clone());
    }
}

#[cfg(test)]
mod aggregation_cache_key_tests {
    use super::*;

    #[test]
    fn key_canonicalizes_predicate_order_and_runtime_metadata() {
        let first = SelectQuery::new("Trip")
            .and_filter(Expr::gt("version", 0_i64))
            .and_filter(Expr::eq("vendor_id", 1_u64))
            .and_filter(Expr::eq("payment_type", 1_i64))
            .comment("first explanation")
            .enable_aggregation_cache_for(1_000);
        let second = SelectQuery::new("Trip")
            .and_filter(Expr::eq("payment_type", 1_i64))
            .and_filter(Expr::gt("version", 0_i64))
            .and_filter(Expr::eq("vendor_id", 1_u64))
            .comment("different explanation")
            .enable_aggregation_cache_for(60_000);

        assert_eq!(
            aggregation_cache_key("tenant", "entity:Trip", &first),
            aggregation_cache_key("tenant", "entity:Trip", &second)
        );
    }

    #[test]
    fn key_retains_result_affecting_values() {
        let first = SelectQuery::new("Trip").filter(Expr::eq("vendor_id", 1_u64));
        let second = SelectQuery::new("Trip").filter(Expr::eq("vendor_id", 2_u64));

        assert_ne!(
            aggregation_cache_key("tenant", "entity:Trip", &first),
            aggregation_cache_key("tenant", "entity:Trip", &second)
        );
    }
}

pub(super) fn attach_relation_aggregate_rows(
    parent_rows: &mut [CompactRow],
    plan: &RelationLoadPlan,
    aggregate: &RelationAggregate,
    aggregate_rows: Vec<CompactRow>,
) {
    let mut buckets: BTreeMap<String, Vec<CompactRow>> = BTreeMap::new();
    for mut row in aggregate_rows {
        if let Some(key) = row.remove(&plan.foreign_key) {
            buckets
                .entry(graph_identity_key(&key))
                .or_default()
                .push(row);
        }
    }

    for parent in parent_rows {
        let value = parent
            .get(&plan.local_key)
            .and_then(|local_value| buckets.get(&graph_identity_key(local_value)))
            .map(|rows| relation_aggregate_value(rows, aggregate.single_result))
            .unwrap_or_else(|| default_aggregate_value(aggregate.single_result));
        parent.insert(aggregate.alias.clone(), value);
    }
}

pub(super) fn relation_aggregate_value(rows: &[CompactRow], single_result: bool) -> Value {
    match single_result {
        true => rows
            .first()
            .map(single_relation_aggregate_value)
            .unwrap_or(Value::U64(0)),
        false => Value::List(
            rows.iter()
                .cloned()
                .map(|row| Value::object(row.into_map()))
                .collect(),
        ),
    }
}

pub(super) fn single_relation_aggregate_value(row: &CompactRow) -> Value {
    match row.len() {
        1 => row.values().next().cloned().unwrap_or(Value::Null),
        _ => Value::object(row.clone().into_map()),
    }
}

pub(super) fn ensure_initial_version(
    values: &mut BTreeMap<String, Value>,
    descriptor: &EntityDescriptor,
) {
    if let Some(version_property) = descriptor.version_property() {
        let needs_version = match values.get(&version_property.name) {
            None | Some(Value::Null) | Some(Value::I64(0)) | Some(Value::U64(0)) => true,
            _ => false,
        };
        if needs_version {
            values.insert(version_property.name.clone(), Value::I64(1));
        }
    }
}

pub(super) fn ensure_timestamps(
    values: &mut BTreeMap<String, Value>,
    descriptor: &EntityDescriptor,
    is_new: bool,
) {
    let now = Value::Timestamp(teaql_core::time::Timestamp::now());
    let has_property =
        |name: &str| -> bool { descriptor.properties.iter().any(|p| p.name == name) };

    if is_new && has_property("create_time") {
        let needs_time = match values.get("create_time") {
            None | Some(Value::Null) => true,
            Some(Value::I64(0)) | Some(Value::U64(0)) => true,
            _ => false,
        };
        if needs_time {
            values.insert("create_time".to_owned(), now.clone());
        }
    }

    if has_property("update_time") {
        let needs_time = match values.get("update_time") {
            None | Some(Value::Null) => true,
            Some(Value::I64(0)) | Some(Value::U64(0)) => true,
            _ => false,
        };
        if needs_time {
            values.insert("update_time".to_owned(), now);
        }
    }
}

pub(super) fn is_unassigned_id(value: Option<&Value>) -> bool {
    matches!(
        value,
        None | Some(Value::Null) | Some(Value::U64(0)) | Some(Value::I64(0))
    )
}

pub(super) fn is_unassigned_id_value(value: &Value) -> bool {
    matches!(value, Value::Null | Value::U64(0) | Value::I64(0))
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
) -> Result<(), DataServiceError<ExecError>> {
    if child.entity == expected_entity {
        return Ok(());
    }
    Err(DataServiceError::Runtime(RuntimeError::Graph(format!(
        "relation {parent_entity}.{relation_name} expects {expected_entity}, got {}",
        child.entity
    ))))
}

pub(crate) fn increment_version(
    values: &mut BTreeMap<String, Value>,
    descriptor: &EntityDescriptor,
    original_version: Option<i64>,
) {
    if let Some(prop) = descriptor.version_property() {
        if !values.contains_key(&prop.name) {
            let next_version = original_version.map(|v| v + 1).unwrap_or(2);
            values.insert(prop.name.clone(), teaql_core::Value::I64(next_version));
        }
    }
}
