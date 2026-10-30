#![allow(clippy::items_after_test_module)] // Helper tests remain beside the group they specify.

use std::collections::BTreeMap;

use teaql_core::{
    BinaryOp, CompactRow, EntityDescriptor, Expr, RelationAggregate, SelectQuery, Value,
};

use crate::{DataServiceError, GraphNode, RuntimeError};

use super::{AggregationCacheBackend, RelationLoadPlan};

pub(super) fn default_aggregate_value(aggregate: &RelationAggregate) -> Value {
    match aggregate.single_result {
        true if matches!(aggregate.query.aggregates.as_slice(), [value] if value.function == teaql_core::AggregateFunction::Sum) => {
            Value::Null
        }
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
    query.clear_request_scope();
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
            ) && let Expr::Value(Value::List(values)) = &mut right
            {
                values.sort_by_cached_key(|value| format!("{value:?}"));
                values.dedup();
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
    aggregate: &RelationAggregate,
) {
    let value = default_aggregate_value(aggregate);
    for parent in parent_rows {
        parent.insert(aggregate.alias.clone(), value.clone());
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
    parent_keys: &[CompactRow],
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

    for (parent, keys) in parent_rows.iter_mut().zip(parent_keys) {
        let value = keys
            .get(&plan.local_key)
            .and_then(|local_value| buckets.get(&graph_identity_key(local_value)))
            .map(|rows| relation_aggregate_value(rows, aggregate))
            .unwrap_or_else(|| default_aggregate_value(aggregate));
        parent.insert(aggregate.alias.clone(), value);
    }
}

pub(super) fn relation_aggregate_value(
    rows: &[CompactRow],
    aggregate: &RelationAggregate,
) -> Value {
    match aggregate.single_result {
        true => rows
            .first()
            .map(single_relation_aggregate_value)
            .unwrap_or_else(|| default_aggregate_value(aggregate)),
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

#[cfg(test)]
mod empty_aggregate_tests {
    use super::*;
    use teaql_core::Aggregate;

    #[test]
    fn empty_aggregate_sql_bucket_does_not_invent_sum_zero() {
        let plan = RelationLoadPlan {
            parent_entity: "Owner".into(),
            relation_name: "lines".into(),
            path: "lines".into(),
            target_entity: "Line".into(),
            local_key: "id".into(),
            foreign_key: "owner".into(),
            many: true,
            query: None,
            children: vec![],
            operation: None,
        };
        for (query, expected) in [
            (SelectQuery::new("Line"), Value::U64(0)),
            (
                SelectQuery::new("Line").aggregate(Aggregate::count("value")),
                Value::U64(0),
            ),
            (
                SelectQuery::new("Line").aggregate(Aggregate::sum("amount", "value")),
                Value::Null,
            ),
        ] {
            let keys = vec![CompactRow::from_map(BTreeMap::from([(
                "id".into(),
                Value::U64(1),
            )]))];
            let mut parents = keys.clone();
            let aggregate = RelationAggregate::new("lines", "total", query, true);
            attach_relation_aggregate_rows(&mut parents, &keys, &plan, &aggregate, vec![]);
            assert_eq!(parents[0].get("total"), Some(&expected));
            assert!(
                Value::object(parents[0].clone().into_map())
                    .to_json_value()
                    .as_object()
                    .unwrap()
                    .contains_key("total")
            );
        }
    }
}

pub(super) fn ensure_initial_version(
    values: &mut BTreeMap<String, Value>,
    descriptor: &EntityDescriptor,
) {
    if let Some(version_property) = descriptor.version_property() {
        let needs_version = matches!(
            values.get(&version_property.name),
            None | Some(Value::Null) | Some(Value::I64(0)) | Some(Value::U64(0))
        );
        if needs_version {
            values.insert(version_property.name.clone(), Value::I64(1));
        }
    }
}

pub(super) fn ensure_timestamps(
    values: &mut BTreeMap<String, Value>,
    descriptor: &EntityDescriptor,
    is_new: bool,
    context: &crate::UserContext,
) {
    let has_property =
        |name: &str| -> bool { descriptor.properties.iter().any(|p| p.name == name) };
    let missing_time = |name: &str| {
        matches!(
            values.get(name),
            None | Some(Value::Null) | Some(Value::I64(0)) | Some(Value::U64(0))
        )
    };
    let create = is_new && has_property("create_time") && missing_time("create_time");
    let update = has_property("update_time") && missing_time("update_time");
    if !create && !update {
        return;
    }
    // A frozen clock can produce the same value as the loaded row, so typed
    // Fix need not put this managed field into the sparse mutation delta. The
    // persistence fallback must use that same invocation's Business Clock,
    // never introduce an independent wall-clock instant after validation.
    let now = Value::Timestamp(context.fix_time());
    if create {
        values.insert("create_time".to_owned(), now.clone());
    }
    if update {
        values.insert("update_time".to_owned(), now);
    }
}

#[cfg(test)]
mod timestamp_fallback_tests {
    use super::*;
    use crate::{BusinessClock, FixedBusinessClock, UserContext};
    use chrono::{DateTime, TimeZone, Utc};
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };
    use teaql_core::{DataType, PropertyDescriptor, time::Timestamp};

    const FIXED: i64 = 1_961_662_530_000;
    fn descriptor() -> EntityDescriptor {
        EntityDescriptor::new("TimestampFallback")
            .property(PropertyDescriptor::new("create_time", DataType::Timestamp))
            .property(PropertyDescriptor::new("update_time", DataType::Timestamp))
    }
    fn context() -> UserContext {
        UserContext::new().with_business_clock(FixedBusinessClock::new(
            Utc.timestamp_millis_opt(FIXED).unwrap(),
        ))
    }
    struct AdvancingClock(Arc<AtomicI64>);
    impl BusinessClock for AdvancingClock {
        fn business_time(&self) -> DateTime<Utc> {
            Utc.timestamp_millis_opt(self.0.fetch_add(1, Ordering::SeqCst))
                .unwrap()
        }
    }
    struct NoClock;
    impl BusinessClock for NoClock {
        fn business_time(&self) -> DateTime<Utc> {
            panic!("complete or unrelated payloads must not consult the clock")
        }
    }
    #[test]
    fn missing_create_and_update_timestamps_use_the_business_clock() {
        let mut values = BTreeMap::new();
        ensure_timestamps(&mut values, &descriptor(), true, &context());
        assert_eq!(
            values.get("create_time"),
            Some(&Value::Timestamp(Timestamp(FIXED)))
        );
        assert_eq!(
            values.get("update_time"),
            Some(&Value::Timestamp(Timestamp(FIXED)))
        );
    }
    #[test]
    fn sparse_update_fallback_never_introduces_a_create_time() {
        let mut values = BTreeMap::from([("name".into(), Value::from("updated"))]);
        ensure_timestamps(&mut values, &descriptor(), false, &context());
        assert!(!values.contains_key("create_time"));
        assert_eq!(values.get("name"), Some(&Value::from("updated")));
        assert_eq!(
            values.get("update_time"),
            Some(&Value::Timestamp(Timestamp(FIXED)))
        );
    }
    #[test]
    fn explicit_times_and_the_epoch_are_preserved_without_clock_access() {
        let context = UserContext::new().with_business_clock(NoClock);
        let mut values = BTreeMap::from([
            ("create_time".into(), Value::Timestamp(Timestamp(0))),
            ("update_time".into(), Value::Timestamp(Timestamp(123))),
        ]);
        let before = values.clone();
        ensure_timestamps(&mut values, &descriptor(), true, &context);
        assert_eq!(values, before);
        ensure_timestamps(
            &mut BTreeMap::new(),
            &EntityDescriptor::new("Untimed"),
            true,
            &context,
        );
    }
    #[test]
    fn null_and_legacy_zero_placeholders_share_one_standalone_instant() {
        for placeholder in [Value::Null, Value::I64(0), Value::U64(0)] {
            let ticks = Arc::new(AtomicI64::new(FIXED));
            let context = UserContext::new().with_business_clock(AdvancingClock(ticks.clone()));
            let mut values = BTreeMap::from([
                ("create_time".into(), placeholder.clone()),
                ("update_time".into(), placeholder),
            ]);
            ensure_timestamps(&mut values, &descriptor(), true, &context);
            assert_eq!(values.get("create_time"), values.get("update_time"));
            assert_eq!(
                values.get("update_time"),
                Some(&Value::Timestamp(Timestamp(FIXED)))
            );
            assert_eq!(ticks.load(Ordering::SeqCst), FIXED + 1);
        }
    }
    #[test]
    fn multiple_entities_reuse_the_enclosing_fix_snapshot() {
        let ticks = Arc::new(AtomicI64::new(FIXED));
        let context = UserContext::new().with_business_clock(AdvancingClock(ticks.clone()));
        crate::entity_save::with_fix_time(&context, || {
            for create in [true, false, true] {
                let mut values = BTreeMap::new();
                ensure_timestamps(&mut values, &descriptor(), create, &context);
                assert_eq!(
                    values.get("update_time"),
                    Some(&Value::Timestamp(Timestamp(FIXED)))
                );
            }
        });
        assert_eq!(ticks.load(Ordering::SeqCst), FIXED + 1);
    }
    #[test]
    fn fallback_contexts_and_later_invocations_do_not_share_time() {
        let ticks = Arc::new(AtomicI64::new(FIXED));
        let first = UserContext::new().with_business_clock(AdvancingClock(ticks.clone()));
        let mut second = context();
        second.set_business_clock(FixedBusinessClock::new(
            Utc.timestamp_millis_opt(9000).unwrap(),
        ));
        crate::entity_save::with_fix_time(&first, || {
            let mut values = BTreeMap::new();
            ensure_timestamps(&mut values, &descriptor(), false, &second);
            assert_eq!(
                values.get("update_time"),
                Some(&Value::Timestamp(Timestamp(9000)))
            );
        });
        let mut values = BTreeMap::new();
        ensure_timestamps(&mut values, &descriptor(), false, &first);
        assert_eq!(
            values.get("update_time"),
            Some(&Value::Timestamp(Timestamp(FIXED + 1)))
        );
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
) -> Result<(), RuntimeError> {
    if let Some(prop) = descriptor.version_property() {
        let next_version =
            crate::mutation_version::updated(&descriptor.name, original_version)?.unwrap_or(2);
        values.insert(prop.name.clone(), teaql_core::Value::I64(next_version));
    }
    Ok(())
}
