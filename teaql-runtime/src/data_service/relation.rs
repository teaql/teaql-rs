use std::collections::BTreeMap;
use std::slice;

use teaql_core::{
    Aggregate, CompactRow, Expr, ObjectGroupBy, RelationAggregate, RelationLoad, SelectQuery, Value,
};

use crate::{DataServiceError, RuntimeError};

use super::{EntityDataService, RelationLoadPlan, helpers::*};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum FlatIdentityKey {
    U64(u64),
    Other(String),
}

impl FlatIdentityKey {
    fn from_value(value: &Value) -> Self {
        match value {
            Value::U64(value) => Self::U64(*value),
            Value::I64(value) if *value >= 0 => Self::U64(*value as u64),
            _ => Self::Other(graph_identity_key(value)),
        }
    }
}

fn unique_relation_values(rows: &[CompactRow], field: &str) -> Vec<Value> {
    let mut values = rows
        .iter()
        .filter_map(|row| row.get(field).cloned())
        .map(|value| (FlatIdentityKey::from_value(&value), value))
        .collect::<Vec<_>>();
    values.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    values.dedup_by(|left, right| left.0 == right.0);
    values.into_iter().map(|(_, value)| value).collect()
}

const SMALL_PARENT_RELATION_PROBE_LIMIT: usize = 16;

fn should_use_small_parent_relation_probes(
    capabilities: &teaql_data_service::DataServiceCapabilities,
    plan: &RelationLoadPlan,
    parent_count: usize,
) -> bool {
    capabilities.small_parent_relation_probes
        && plan.many
        && parent_count <= SMALL_PARENT_RELATION_PROBE_LIMIT
        && plan
            .query
            .as_ref()
            .is_some_and(|query| query.slice.is_some())
}

impl<'a, E> EntityDataService<'a, E>
where
    E: teaql_data_service::QueryExecutor
        + teaql_data_service::MutationExecutor
        + Send
        + Sync
        + 'static,
{
    pub fn relation_loads(&self) -> Vec<String> {
        self.behavior()
            .map(|behavior| behavior.relation_loads(self.data_service.metadata.context))
            .unwrap_or_default()
    }

    pub fn relation_plans(&self) -> Result<Vec<RelationLoadPlan>, RuntimeError> {
        self.build_relation_plans(&self.entity, &self.relation_loads())
    }

    pub fn relation_query(
        &self,
        relation_name: &str,
        parent_rows: &[CompactRow],
    ) -> Result<SelectQuery, RuntimeError> {
        let plan = self
            .relation_plans()?
            .into_iter()
            .find(|plan| plan.relation_name == relation_name)
            .ok_or_else(|| RuntimeError::MissingRelation {
                entity: self.entity.clone(),
                relation: relation_name.to_owned(),
            })?;
        Ok(self.query_for_plan(&plan, parent_rows))
    }

    pub(crate) async fn enhance_relations_internal(
        &self,
        parent_rows: &mut [CompactRow],
    ) -> Result<(), DataServiceError<E::Error>> {
        let plans = self.relation_plans().map_err(DataServiceError::Runtime)?;
        for plan in plans {
            self.enhance_plan(parent_rows, &plan).await?;
        }
        Ok(())
    }

    pub(crate) async fn enhance_query_relations_internal(
        &self,
        parent_rows: &mut [CompactRow],
        query: &SelectQuery,
    ) -> Result<(), DataServiceError<E::Error>> {
        let plans = self
            .build_relation_plans_from_loads(&query.entity, &query.relations)
            .map_err(DataServiceError::Runtime)?;
        for plan in plans {
            self.enhance_plan(parent_rows, &plan).await?;
        }
        Ok(())
    }

    pub(crate) async fn hydrate_flat_plans_internal(
        &self,
        parent_rows: &mut [CompactRow],
        plans: &[RelationLoadPlan],
        root: &crate::EntityRoot,
        graph: &mut crate::EntityGraphBuilder,
    ) -> Result<(), DataServiceError<E::Error>> {
        for plan in plans {
            self.hydrate_flat_plan(parent_rows, plan, root, graph)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn hydrate_compact_flat_plans_internal(
        &self,
        parent_rows: &[CompactRow],
        plans: &[RelationLoadPlan],
        root: &crate::EntityRoot,
        graph: &mut crate::EntityGraphBuilder,
    ) -> Result<(), DataServiceError<E::Error>> {
        for plan in plans {
            self.hydrate_compact_flat_plan(parent_rows, plan, root, graph)
                .await?;
        }
        Ok(())
    }

    pub(crate) fn flat_relation_plans(
        &self,
        query: &SelectQuery,
    ) -> Result<Option<(Vec<RelationLoadPlan>, Vec<RelationLoadPlan>)>, RuntimeError> {
        let context = self.data_service.metadata.context;
        let query_plans = self.build_relation_plans_from_loads(&query.entity, &query.relations)?;
        let behavior_plans = self.relation_plans()?;

        fn supported(context: &crate::UserContext, plan: &RelationLoadPlan) -> bool {
            context.has_entity_graph_decoder(&plan.target_entity)
                && plan.children.iter().all(|child| supported(context, child))
        }

        Ok(query_plans
            .iter()
            .chain(behavior_plans.iter())
            .all(|plan| supported(context, plan))
            .then_some((query_plans, behavior_plans)))
    }

    pub(crate) fn enhance_relation_aggregates_internal<'b>(
        &'b self,
        parent_rows: &'b mut [CompactRow],
        relation_aggregates: &'b [RelationAggregate],
        parent_cache_options: Option<teaql_core::AggregationCacheOptions>,
        parent_trace_chain: &'b [teaql_core::TraceNode],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            for aggregate in relation_aggregates {
                self.enhance_relation_aggregate(
                    parent_rows,
                    aggregate,
                    parent_cache_options,
                    parent_trace_chain,
                )
                .await?;
            }
            Ok(())
        })
    }

    pub(crate) fn enhance_object_group_bys_internal<'b>(
        &'b self,
        rows: &'b mut [CompactRow],
        object_group_bys: &'b [ObjectGroupBy],
        parent_trace_chain: &'b [teaql_core::TraceNode],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            for group_by in object_group_bys {
                let ids = rows
                    .iter()
                    .filter_map(|row| row.get(&group_by.storage_field).cloned())
                    .collect::<Vec<_>>();
                if ids.is_empty() {
                    continue;
                }
                let mut query = group_by.query.clone();
                ensure_projection(&mut query, "id");
                query = query.and_filter(Expr::in_list("id", ids));
                let object_rows = self
                    .scoped_data_service_internal(query.entity.clone())
                    .with_trace_context(parent_trace_chain.to_vec())
                    .fetch_compact_all_internal(query)
                    .await?
                    .into_iter()
                    .filter_map(|row| {
                        row.get("id")
                            .cloned()
                            .map(|id| (graph_identity_key(&id), row))
                    })
                    .collect::<BTreeMap<_, _>>();
                for row in rows.iter_mut() {
                    if let Some(key) = row.get(&group_by.storage_field).map(graph_identity_key) {
                        let value = object_rows
                            .get(&key)
                            .cloned()
                            .map(|row| Value::object(row.into_map()))
                            .unwrap_or(Value::Null);
                        row.insert(group_by.property_name.clone(), value);
                    }
                }
            }
            Ok(())
        })
    }

    pub(crate) fn enhance_child_queries_internal<'b>(
        &'b self,
        rows: &'b mut [CompactRow],
        child_queries: &'b [SelectQuery],
        parent_trace_chain: &'b [teaql_core::TraceNode],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            for child_query in child_queries {
                let ids = rows
                    .iter()
                    .filter_map(|row| row.get("id").cloned())
                    .collect::<Vec<_>>();
                if ids.is_empty() {
                    continue;
                }
                let mut query = child_query.clone();
                ensure_projection(&mut query, "id");
                query = query.and_filter(Expr::in_list("id", ids));
                let child_rows = self
                    .scoped_data_service_internal(query.entity.clone())
                    .with_trace_context(parent_trace_chain.to_vec())
                    .fetch_compact_all_internal(query)
                    .await?
                    .into_iter()
                    .filter_map(|row| {
                        row.get("id")
                            .cloned()
                            .map(|id| (graph_identity_key(&id), row))
                    })
                    .collect::<BTreeMap<_, _>>();
                for row in rows.iter_mut() {
                    if let Some(key) = row.get("id").map(graph_identity_key) {
                        if let Some(child) = child_rows.get(&key) {
                            row.extend(child.clone());
                        }
                    }
                }
            }
            Ok(())
        })
    }

    async fn enhance_relation_aggregate(
        &self,
        parent_rows: &mut [CompactRow],
        aggregate: &RelationAggregate,
        parent_cache_options: Option<teaql_core::AggregationCacheOptions>,
        parent_trace_chain: &[teaql_core::TraceNode],
    ) -> Result<(), DataServiceError<E::Error>> {
        let plan = self
            .build_relation_plans_from_loads(
                &self.entity,
                &[RelationLoad::with_query(
                    aggregate.relation_name.clone(),
                    aggregate.query.clone(),
                )],
            )
            .map_err(DataServiceError::Runtime)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                DataServiceError::Runtime(RuntimeError::MissingRelation {
                    entity: self.entity.clone(),
                    relation: aggregate.relation_name.clone(),
                })
            })?;

        let ids = parent_rows
            .iter()
            .filter_map(|row| row.get(&plan.local_key).cloned())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            attach_empty_relation_aggregate(parent_rows, &aggregate.alias, aggregate.single_result);
            return Ok(());
        }

        let child_repo = self.scoped_data_service_internal(plan.target_entity.clone());
        let mut query = aggregate.query.clone();
        query.entity = plan.target_entity.clone();
        if query.aggregation_cache.is_none() {
            if let Some(options) = parent_cache_options.filter(|options| options.propagate) {
                query.aggregation_cache = Some(teaql_core::AggregationCacheOptions::enabled(
                    options.propagate_cache_expired_millis,
                ));
            }
        }
        query.projection.clear();
        query.expr_projection.clear();
        query.order_by.clear();
        query.slice = None;
        query.relations.clear();
        if query.aggregates.is_empty() {
            let alias = aggregate_alias(aggregate.single_result, &aggregate.alias);
            query = query.aggregate(Aggregate::count(alias));
        }
        if !query
            .group_by
            .iter()
            .any(|field| field == &plan.foreign_key)
        {
            query = query.group_by(plan.foreign_key.clone());
        }
        query = query.and_filter(Expr::in_list(plan.foreign_key.clone(), ids));

        let mut chain = parent_trace_chain.to_vec();
        chain.push(teaql_core::TraceNode {
            entity_type: query.entity.clone(),
            entity_id: None,
            comment: aggregate.alias.clone(),
        });

        let mut aggregate_rows = child_repo
            .with_trace_context(chain)
            .fetch_compact_all_internal(query)
            .await?;
        let foreign_key_column = self
            .data_service
            .metadata
            .context
            .entity(&plan.target_entity)
            .and_then(|descriptor| {
                descriptor
                    .properties
                    .iter()
                    .find(|property| property.name == plan.foreign_key)
                    .map(|property| property.column_name.clone())
            });
        if let Some(foreign_key_column) =
            foreign_key_column.filter(|column| column != &plan.foreign_key)
        {
            for row in &mut aggregate_rows {
                if !row.contains_key(&plan.foreign_key) {
                    if let Some(value) = row.remove(&foreign_key_column) {
                        row.insert(plan.foreign_key.clone(), value);
                    }
                }
            }
        }
        attach_relation_aggregate_rows(parent_rows, &plan, aggregate, aggregate_rows);
        Ok(())
    }

    fn build_relation_plans(
        &self,
        entity: &str,
        loads: &[String],
    ) -> Result<Vec<RelationLoadPlan>, RuntimeError> {
        let descriptor = self.data_service.metadata.context.require_entity(entity)?;
        let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for load in loads {
            match load.split_once('.') {
                Some((head, tail)) => {
                    grouped
                        .entry(head.to_owned())
                        .or_default()
                        .push(tail.to_owned());
                }
                None => {
                    grouped.entry(load.clone()).or_default();
                }
            }
        }

        grouped
            .into_iter()
            .map(|(name, child_loads)| {
                let relation = descriptor.relation_by_name(&name).ok_or_else(|| {
                    RuntimeError::MissingRelation {
                        entity: entity.to_owned(),
                        relation: name.clone(),
                    }
                })?;
                let child_repo = self.scoped_data_service_internal(relation.target_entity.clone());
                let children =
                    child_repo.build_relation_plans(&relation.target_entity, &child_loads)?;
                Ok(RelationLoadPlan {
                    parent_entity: entity.to_owned(),
                    relation_name: relation.name.clone(),
                    path: relation.name.clone(),
                    target_entity: relation.target_entity.clone(),
                    local_key: relation.local_key.clone(),
                    foreign_key: relation.foreign_key.clone(),
                    many: relation.many,
                    query: None,
                    children,
                })
            })
            .collect()
    }

    fn build_relation_plans_from_loads(
        &self,
        entity: &str,
        loads: &[RelationLoad],
    ) -> Result<Vec<RelationLoadPlan>, RuntimeError> {
        let descriptor = self.data_service.metadata.context.require_entity(entity)?;
        loads
            .iter()
            .map(|load| {
                let relation = descriptor.relation_by_name(&load.name).ok_or_else(|| {
                    RuntimeError::MissingRelation {
                        entity: entity.to_owned(),
                        relation: load.name.clone(),
                    }
                })?;
                let relation_query = load.query.as_deref().cloned();
                let child_loads = relation_query
                    .as_ref()
                    .map(|query| query.relations.as_slice())
                    .unwrap_or_default();
                let child_repo = self.scoped_data_service_internal(relation.target_entity.clone());
                let children = child_repo
                    .build_relation_plans_from_loads(&relation.target_entity, child_loads)?;
                Ok(RelationLoadPlan {
                    parent_entity: entity.to_owned(),
                    relation_name: relation.name.clone(),
                    path: relation.name.clone(),
                    target_entity: relation.target_entity.clone(),
                    local_key: relation.local_key.clone(),
                    foreign_key: relation.foreign_key.clone(),
                    many: relation.many,
                    query: relation_query,
                    children,
                })
            })
            .collect()
    }
    fn enhance_plan<'b>(
        &'b self,
        parent_rows: &'b mut [CompactRow],
        plan: &'b RelationLoadPlan,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            let scope = self.data_service.metadata.context.start_runtime_operation(
                crate::RuntimeOperation::new(
                    "relation_load",
                    format!("{}.{}", plan.parent_entity, plan.path),
                )
                .attribute("teaql.entity.type", plan.parent_entity.clone())
                .attribute("teaql.relation.name", plan.path.clone()),
            );
            let result = scope
                .run(async {
                    let child_repo = self.scoped_data_service_internal(plan.target_entity.clone());
                    let mut child_rows = self
                        .fetch_relation_rows(&child_repo, plan, parent_rows, false)
                        .await?;
                    for child in &mut child_rows {
                        child.remove(teaql_core::PARTITION_RANK_PROPERTY);
                    }
                    self.attach_relation_rows(parent_rows, plan, child_rows);

                    if !plan.children.is_empty() {
                        for parent in parent_rows.iter_mut() {
                            match parent.get_mut(&plan.relation_name) {
                                Some(Value::Object(child)) => {
                                    child_repo
                                        .enhance_child_record(child, &plan.children)
                                        .await?;
                                }
                                Some(Value::List(values)) => {
                                    for value in values.iter_mut() {
                                        if let Value::Object(child) = value {
                                            child_repo
                                                .enhance_child_record(child, &plan.children)
                                                .await?;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    Ok(())
                })
                .await;
            match &result {
                Ok(_) => scope.success(BTreeMap::from([(
                    "teaql.result.cardinality".to_owned(),
                    crate::RuntimeAttributeValue::Integer(parent_rows.len() as i64),
                )])),
                Err(_) => scope.failure("relation_load_error"),
            }
            result
        })
    }

    fn hydrate_flat_plan<'b>(
        &'b self,
        parent_rows: &'b mut [CompactRow],
        plan: &'b RelationLoadPlan,
        root: &'b crate::EntityRoot,
        graph: &'b mut crate::EntityGraphBuilder,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            let child_repo = self.scoped_data_service_internal(plan.target_entity.clone());
            let mut child_rows = self
                .fetch_relation_rows(&child_repo, plan, parent_rows, false)
                .await?;
            for child in &mut child_rows {
                child.remove(teaql_core::PARTITION_RANK_PROPERTY);
            }

            // Hydrate descendants while the rows are still owned by this level. Nothing is
            // embedded into a parent row: every relation is published directly into the
            // shared, immutable identity graph.
            for child_plan in &plan.children {
                child_repo
                    .hydrate_flat_plan(&mut child_rows, child_plan, root, graph)
                    .await?;
            }

            let inverse_relation = self
                .data_service
                .metadata
                .context
                .entity(&plan.target_entity)
                .and_then(|descriptor| {
                    descriptor.relations.iter().find(|relation| {
                        relation.target_entity == plan.parent_entity
                            && relation.local_key == plan.foreign_key
                            && relation.foreign_key == plan.local_key
                    })
                })
                .map(|relation| (relation.name.clone(), relation.many));

            let mut buckets: BTreeMap<FlatIdentityKey, Vec<CompactRow>> = BTreeMap::new();
            for child in child_rows {
                if let Some(key) = child.get(&plan.foreign_key) {
                    buckets
                        .entry(FlatIdentityKey::from_value(key))
                        .or_default()
                        .push(child);
                }
            }

            let context = self.data_service.metadata.context;
            for parent in parent_rows {
                let Some(local_value) = parent.get(&plan.local_key) else {
                    continue;
                };
                let related = buckets
                    .remove(&FlatIdentityKey::from_value(local_value))
                    .unwrap_or_default();

                if let Some((inverse_name, inverse_many)) = &inverse_relation {
                    let parent_record = parent.clone();
                    for child in &related {
                        let Some(child_id) = child.get("id").and_then(Value::try_u64) else {
                            continue;
                        };
                        if *inverse_many {
                            context
                                .decode_compact_entity_list_into_graph(
                                    &plan.parent_entity,
                                    vec![parent_record.clone()],
                                    root,
                                    graph,
                                    &plan.target_entity,
                                    child_id,
                                    inverse_name,
                                )
                                .map_err(DataServiceError::Entity)?;
                        } else {
                            context
                                .decode_compact_entity_option_into_graph(
                                    &plan.parent_entity,
                                    vec![parent_record.clone()],
                                    root,
                                    graph,
                                    &plan.target_entity,
                                    child_id,
                                    inverse_name,
                                )
                                .map_err(DataServiceError::Entity)?;
                        }
                    }
                }

                if plan.many || plan.local_key == "id" {
                    let owner_id = parent.get("id").and_then(Value::try_u64).ok_or_else(|| {
                        DataServiceError::Entity(teaql_core::EntityError::new(
                            &plan.parent_entity,
                            "loaded reverse relation owner is missing its u64 id",
                        ))
                    })?;
                    if plan.many {
                        context
                            .decode_compact_entity_list_into_graph(
                                &plan.target_entity,
                                related,
                                root,
                                graph,
                                &plan.parent_entity,
                                owner_id,
                                &plan.relation_name,
                            )
                            .map_err(DataServiceError::Entity)?;
                    } else {
                        context
                            .decode_compact_entity_option_into_graph(
                                &plan.target_entity,
                                related,
                                root,
                                graph,
                                &plan.parent_entity,
                                owner_id,
                                &plan.relation_name,
                            )
                            .map_err(DataServiceError::Entity)?;
                    }
                } else if related.is_empty() {
                    // Forward optional relations use the scalar loaded marker to distinguish a
                    // loaded null from a relation that was never requested.
                    parent.insert(plan.relation_name.clone(), Value::Null);
                } else {
                    for child in related {
                        context
                            .decode_compact_entity_into_graph(
                                &plan.target_entity,
                                child,
                                root,
                                graph,
                            )
                            .map_err(DataServiceError::Entity)?;
                    }
                }
            }
            Ok(())
        })
    }

    fn hydrate_compact_flat_plan<'b>(
        &'b self,
        parent_rows: &'b [CompactRow],
        plan: &'b RelationLoadPlan,
        root: &'b crate::EntityRoot,
        graph: &'b mut crate::EntityGraphBuilder,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            let child_repo = self.scoped_data_service_internal(plan.target_entity.clone());
            let child_rows = self
                .fetch_relation_rows(&child_repo, plan, parent_rows, true)
                .await?;

            for child_plan in &plan.children {
                child_repo
                    .hydrate_compact_flat_plan(&child_rows, child_plan, root, graph)
                    .await?;
            }

            // A forward to-one relation only needs its fetched targets installed in the shared
            // identity table. Building owner buckets and then removing them one parent at a time
            // creates a map and one Vec per distinct target without adding information.
            if !plan.many && plan.local_key != "id" {
                return self
                    .data_service
                    .metadata
                    .context
                    .decode_compact_entity_batch_into_graph(
                        &plan.target_entity,
                        child_rows,
                        root,
                        graph,
                    )
                    .map_err(DataServiceError::Entity);
            }

            let mut buckets: BTreeMap<FlatIdentityKey, Vec<CompactRow>> = BTreeMap::new();
            for child in child_rows {
                if let Some(key) = child.get(&plan.foreign_key) {
                    buckets
                        .entry(FlatIdentityKey::from_value(key))
                        .or_default()
                        .push(child);
                }
            }

            let context = self.data_service.metadata.context;
            for parent in parent_rows {
                let Some(local_value) = parent.get(&plan.local_key) else {
                    continue;
                };
                let related = buckets
                    .remove(&FlatIdentityKey::from_value(local_value))
                    .unwrap_or_default();

                if plan.many || plan.local_key == "id" {
                    let owner_id = parent.get("id").and_then(Value::try_u64).ok_or_else(|| {
                        DataServiceError::Entity(teaql_core::EntityError::new(
                            &plan.parent_entity,
                            "loaded reverse relation owner is missing its u64 id",
                        ))
                    })?;
                    if plan.many {
                        context
                            .decode_compact_entity_list_into_graph(
                                &plan.target_entity,
                                related,
                                root,
                                graph,
                                &plan.parent_entity,
                                owner_id,
                                &plan.relation_name,
                            )
                            .map_err(DataServiceError::Entity)?;
                    } else {
                        context
                            .decode_compact_entity_option_into_graph(
                                &plan.target_entity,
                                related,
                                root,
                                graph,
                                &plan.parent_entity,
                                owner_id,
                                &plan.relation_name,
                            )
                            .map_err(DataServiceError::Entity)?;
                    }
                } else {
                    for child in related {
                        context
                            .decode_compact_entity_into_graph(
                                &plan.target_entity,
                                child,
                                root,
                                graph,
                            )
                            .map_err(DataServiceError::Entity)?;
                    }
                }
            }
            Ok(())
        })
    }

    fn enhance_child_record<'b>(
        &'b self,
        child: &'b mut std::collections::BTreeMap<String, Value>,
        plans: &'b [RelationLoadPlan],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            for plan in plans {
                let mut row = CompactRow::from_map(std::mem::take(child));
                self.enhance_plan(slice::from_mut(&mut row), plan).await?;
                *child = row.into_map();
            }
            Ok(())
        })
    }

    fn query_for_plan(&self, plan: &RelationLoadPlan, parent_rows: &[CompactRow]) -> SelectQuery {
        // Relation identities are a set. Keeping one value per normalized identity avoids
        // compiling and binding the same foreign key once for every parent row (a common shape
        // for pages containing many rows that share a small reference table).
        let ids = unique_relation_values(parent_rows, &plan.local_key);

        let mut query = plan
            .query
            .clone()
            .unwrap_or_else(|| SelectQuery::new(plan.target_entity.clone()));
        query.entity = plan.target_entity.clone();
        ensure_projection(&mut query, &plan.foreign_key);
        for child in &plan.children {
            ensure_projection(&mut query, &child.local_key);
        }
        if !ids.is_empty() {
            query = query.and_filter(Expr::in_list(plan.foreign_key.clone(), ids));
        }
        if query.slice.is_some() {
            query.partition_by = Some(plan.foreign_key.clone());
        }
        query
    }

    async fn fetch_relation_rows(
        &self,
        child_repo: &EntityDataService<'a, E>,
        plan: &RelationLoadPlan,
        parent_rows: &[CompactRow],
        compact: bool,
    ) -> Result<Vec<CompactRow>, DataServiceError<E::Error>> {
        let ids = unique_relation_values(parent_rows, &plan.local_key);
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let capabilities =
            teaql_data_service::DataServiceExecutor::capabilities(self.data_service.executor);
        let probe = should_use_small_parent_relation_probes(&capabilities, plan, ids.len());

        if !probe {
            let query = if compact {
                self.query_for_compact_plan(plan, parent_rows)
            } else {
                self.query_for_plan(plan, parent_rows)
            };
            return child_repo.fetch_compact_all_internal(query).await;
        }

        let mut rows = Vec::new();
        for id in ids {
            let mut query = self.base_relation_query(plan);
            query = query.and_filter(Expr::eq(plan.foreign_key.clone(), id));
            // The slice now belongs to one parent, so no window partition is needed.
            query.partition_by = None;
            rows.extend(child_repo.fetch_compact_all_internal(query).await?);
        }
        Ok(rows)
    }

    fn base_relation_query(&self, plan: &RelationLoadPlan) -> SelectQuery {
        let mut query = plan
            .query
            .clone()
            .unwrap_or_else(|| SelectQuery::new(plan.target_entity.clone()));
        query.entity = plan.target_entity.clone();
        ensure_projection(&mut query, &plan.foreign_key);
        for child in &plan.children {
            ensure_projection(&mut query, &child.local_key);
        }
        query
    }

    fn query_for_compact_plan(
        &self,
        plan: &RelationLoadPlan,
        parent_rows: &[CompactRow],
    ) -> SelectQuery {
        let ids = unique_relation_values(parent_rows, &plan.local_key);
        let mut query = plan
            .query
            .clone()
            .unwrap_or_else(|| SelectQuery::new(plan.target_entity.clone()));
        query.entity = plan.target_entity.clone();
        ensure_projection(&mut query, &plan.foreign_key);
        for child in &plan.children {
            ensure_projection(&mut query, &child.local_key);
        }
        if !ids.is_empty() {
            query = query.and_filter(Expr::in_list(plan.foreign_key.clone(), ids));
        }
        if query.slice.is_some() {
            query.partition_by = Some(plan.foreign_key.clone());
        }
        query
    }

    fn attach_relation_rows(
        &self,
        parent_rows: &mut [CompactRow],
        plan: &RelationLoadPlan,
        child_rows: Vec<CompactRow>,
    ) {
        let inverse_relation = self
            .data_service
            .metadata
            .context
            .entity(&plan.target_entity)
            .and_then(|descriptor| {
                descriptor.relations.iter().find(|relation| {
                    relation.target_entity == plan.parent_entity
                        && relation.local_key == plan.foreign_key
                        && relation.foreign_key == plan.local_key
                })
            })
            .map(|relation| (relation.name.clone(), relation.many));

        let mut buckets: BTreeMap<String, Vec<CompactRow>> = BTreeMap::new();
        for child in child_rows.clone() {
            if let Some(key) = child.get(&plan.foreign_key) {
                buckets
                    .entry(graph_identity_key(key))
                    .or_default()
                    .push(child);
            }
        }

        for parent in parent_rows.iter_mut() {
            let Some(local_value) = parent.get(&plan.local_key) else {
                continue;
            };
            let bucket_key = graph_identity_key(local_value);
            let related = buckets.get(&bucket_key).cloned().unwrap_or_default();
            let related = match &inverse_relation {
                Some((inverse_relation, inverse_many)) => {
                    let mut parent_object = parent.clone();
                    parent_object.remove(&plan.relation_name);
                    related
                        .into_iter()
                        .map(|mut child| {
                            match *inverse_many {
                                true => {
                                    if !child.contains_key(inverse_relation) {
                                        child.insert(
                                            inverse_relation.clone(),
                                            Value::List(Vec::new()),
                                        );
                                    }
                                    let entry = child
                                        .get_mut(inverse_relation)
                                        .expect("inverse relation was inserted immediately above");
                                    if let Value::List(list) = entry {
                                        list.push(Value::object(parent_object.clone().into_map()));
                                    }
                                }
                                false => {
                                    child.insert(
                                        inverse_relation.clone(),
                                        Value::object(parent_object.clone().into_map()),
                                    );
                                }
                            }
                            child
                        })
                        .collect::<Vec<_>>()
                }
                None => related,
            };
            match plan.many {
                true => {
                    parent.insert(
                        plan.relation_name.clone(),
                        Value::List(
                            related
                                .into_iter()
                                .map(|row| Value::object(row.into_map()))
                                .collect(),
                        ),
                    );
                }
                false => {
                    let value = related
                        .into_iter()
                        .next()
                        .map(|row| Value::object(row.into_map()))
                        .unwrap_or(Value::Null);
                    parent.insert(plan.relation_name.clone(), value);
                }
            }
        }
    }
}

#[cfg(test)]
mod planner_tests {
    use super::*;

    fn limited_many_plan() -> RelationLoadPlan {
        RelationLoadPlan {
            parent_entity: "Vendor".to_owned(),
            relation_name: "trips".to_owned(),
            path: "trips".to_owned(),
            target_entity: "Trip".to_owned(),
            local_key: "id".to_owned(),
            foreign_key: "vendor_id".to_owned(),
            many: true,
            query: Some(SelectQuery::new("Trip").order_desc("id").limit(10)),
            children: Vec::new(),
        }
    }

    #[test]
    fn small_parent_probe_requires_provider_opt_in_and_bounded_parent_set() {
        let plan = limited_many_plan();
        let mut capabilities = teaql_data_service::DataServiceCapabilities::default();
        assert!(!should_use_small_parent_relation_probes(
            &capabilities,
            &plan,
            6
        ));

        capabilities.small_parent_relation_probes = true;
        assert!(should_use_small_parent_relation_probes(
            &capabilities,
            &plan,
            6
        ));
        assert!(!should_use_small_parent_relation_probes(
            &capabilities,
            &plan,
            SMALL_PARENT_RELATION_PROBE_LIMIT + 1
        ));
    }
}
