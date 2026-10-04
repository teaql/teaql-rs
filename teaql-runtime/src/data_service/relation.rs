// Named aliases will replace these recursive relation-future signatures in a later API cycle.
#![allow(clippy::type_complexity)]

use std::collections::BTreeMap;

use teaql_core::{
    Aggregate, CompactRow, Expr, ObjectGroupBy, OrderBy, RelationAggregate, RelationLoad,
    SelectQuery, SmartList, Value,
};

use crate::{DataServiceError, MetadataStore, RuntimeError};

use super::{EntityDataService, RelationLoadPlan, helpers::*};

// Native relation Facets reuse the request-local scope, not a new Context root.
impl<E> crate::TeaqlRuntime for EntityDataService<'_, E>
where
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync,
{
    fn user_context(&self) -> &crate::UserContext {
        self.data_service.metadata.context
    }

    async fn fetch_facet_smart_list(
        &self,
        entity: &str,
        query: &crate::PurposedSelectQuery,
        aggregates: &[RelationAggregate],
        trace_context: Vec<teaql_core::TraceNode>,
    ) -> Result<SmartList<CompactRow>, RuntimeError> {
        self.scoped_data_service_internal(entity.to_owned())
            .with_trace_context(trace_context)
            .fetch_smart_list_with_relation_aggregates(query, aggregates)
            .await
            .map_err(|error| RuntimeError::Graph(error.to_string()))
    }
}

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
        .filter(|value| !matches!(value, Value::Null | Value::TypedNull(_)))
        .map(|value| (FlatIdentityKey::from_value(&value), value))
        .collect::<Vec<_>>();
    values.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    values.dedup_by(|left, right| left.0 == right.0);
    values.into_iter().map(|(_, value)| value).collect()
}

// Capture only assembly keys, not entire records. Relation names may be the
// same as scalar FK fields; a previous sibling can replace that field with an
// object or null. These private rows never enter the returned graph or ledger.
fn relation_key_rows(rows: &[CompactRow], plans: &[RelationLoadPlan]) -> Vec<CompactRow> {
    if plans.is_empty() {
        return Vec::new();
    }
    rows.iter()
        .map(|row| {
            CompactRow::from_map(
                plans
                    .iter()
                    .filter_map(|plan| {
                        row.get(&plan.local_key)
                            .cloned()
                            .map(|value| (plan.local_key.clone(), value))
                    })
                    .collect(),
            )
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelationTopNExecutionPlan {
    Window,
    BoundedProbes,
}

fn relation_top_n_operation(
    plan: &RelationLoadPlan,
    selected_plan: RelationTopNExecutionPlan,
    parent_count: usize,
) -> crate::RuntimeOperation {
    let configured_threshold = plan
        .query
        .as_ref()
        .and_then(|query| query.top_n_probe_parent_threshold)
        .unwrap_or(0);
    let per_parent_limit = plan
        .query
        .as_ref()
        .and_then(|query| query.slice)
        .and_then(|slice| slice.limit)
        .unwrap_or(0);
    crate::RuntimeOperation::new(
        "relation_load",
        format!("{}.{}", plan.parent_entity, plan.path),
    )
    .attribute("teaql.entity.type", plan.parent_entity.clone())
    .attribute("teaql.relation.name", plan.path.clone())
    .attribute("teaql.relation.parent_count", parent_count)
    .attribute("teaql.relation.per_parent_limit", per_parent_limit as usize)
    .attribute(
        "teaql.relation.configured_probe_threshold",
        configured_threshold,
    )
    .attribute(
        "teaql.relation.selected_plan",
        match selected_plan {
            RelationTopNExecutionPlan::Window => "WINDOW",
            RelationTopNExecutionPlan::BoundedProbes => "BOUNDED_PROBES",
        },
    )
    .attribute(
        "teaql.relation.probe_count",
        if selected_plan == RelationTopNExecutionPlan::BoundedProbes {
            parent_count
        } else {
            0
        },
    )
}

fn relation_top_n_execution_plan(
    capabilities: &teaql_data_service::DataServiceCapabilities,
    plan: &RelationLoadPlan,
    parent_count: usize,
) -> RelationTopNExecutionPlan {
    let Some(query) = plan.query.as_ref().filter(|_| plan.many) else {
        return RelationTopNExecutionPlan::Window;
    };
    if query.slice.is_none() {
        return RelationTopNExecutionPlan::Window;
    }
    let use_probes = match query.top_n_probe_parent_threshold {
        Some(0) => false,
        Some(threshold) => parent_count <= threshold,
        None => capabilities.small_parent_relation_probes,
    };
    if use_probes {
        RelationTopNExecutionPlan::BoundedProbes
    } else {
        RelationTopNExecutionPlan::Window
    }
}

impl<'a, E> EntityDataService<'a, E>
where
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync,
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
        let keys = relation_key_rows(parent_rows, &plans);
        for plan in plans {
            self.enhance_plan(parent_rows, &keys, &plan).await?;
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
        // Prepared queries already contain the inherited repository prefix.
        let parent_trace = query.trace_chain.clone();
        let traced = self
            .scoped_data_service_internal(query.entity.clone())
            .with_trace_context(parent_trace);
        let keys = relation_key_rows(parent_rows, &plans);
        for plan in plans {
            traced.enhance_plan(parent_rows, &keys, &plan).await?;
        }
        Ok(())
    }

    pub(crate) async fn hydrate_flat_plans_internal(
        &self,
        parent_rows: &mut [CompactRow],
        plans: &[RelationLoadPlan],
        root: &crate::EntityRuntimeState,
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
        root: &crate::EntityRuntimeState,
        graph: &mut crate::EntityGraphBuilder,
    ) -> Result<(), DataServiceError<E::Error>> {
        for plan in plans {
            if plan.children.is_empty() {
                self.hydrate_compact_flat_leaf(parent_rows, plan, root, graph)
                    .await?;
            } else {
                self.hydrate_compact_flat_plan(parent_rows, plan, root, graph)
                    .await?;
            }
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

        let all_supported = query_plans
            .iter()
            .chain(behavior_plans.iter())
            .all(|plan| supported(context, plan));
        Ok(all_supported.then_some((query_plans, behavior_plans)))
    }

    pub(crate) fn relation_aggregate_key_rows(
        &self,
        rows: &[CompactRow],
        aggregates: &[RelationAggregate],
    ) -> Result<Vec<CompactRow>, RuntimeError> {
        if aggregates.is_empty() {
            return Ok(Vec::new());
        }
        let descriptor = self
            .data_service
            .metadata
            .context
            .require_entity(&self.entity)?;
        let fields = aggregates
            .iter()
            .map(|aggregate| {
                descriptor
                    .relation_by_name(&aggregate.relation_name)
                    .map(|relation| relation.local_key.as_str())
                    .ok_or_else(|| RuntimeError::MissingRelation {
                        entity: self.entity.clone(),
                        relation: aggregate.relation_name.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows
            .iter()
            .map(|row| {
                CompactRow::from_map(
                    fields
                        .iter()
                        .filter_map(|field| {
                            row.get(field)
                                .cloned()
                                .map(|value| ((*field).to_owned(), value))
                        })
                        .collect(),
                )
            })
            .collect())
    }

    pub(crate) fn enhance_relation_aggregates_internal<'b>(
        &'b self,
        parent_rows: &'b mut [CompactRow],
        parent_keys: &'b [CompactRow],
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
                    parent_keys,
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
                    if let Some(key) = row.get("id").map(graph_identity_key)
                        && let Some(child) = child_rows.get(&key)
                    {
                        row.extend(child.clone());
                    }
                }
            }
            Ok(())
        })
    }

    async fn enhance_relation_aggregate(
        &self,
        parent_rows: &mut [CompactRow],
        parent_keys: &[CompactRow],
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

        let ids = parent_keys
            .iter()
            .filter_map(|row| row.get(&plan.local_key).cloned())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            attach_empty_relation_aggregate(parent_rows, &aggregate.alias, aggregate.single_result);
            return Ok(());
        }

        let chain = parent_trace_chain.to_vec();
        let parent_repo = self
            .scoped_data_service_internal(self.entity.clone())
            .with_trace_context(chain);
        let child_repo = parent_repo.relation_child_repo(&plan);
        let mut query = aggregate.query.clone();
        query.entity = plan.target_entity.clone();
        if query.aggregation_cache.is_none()
            && let Some(options) = parent_cache_options.filter(|options| options.propagate)
        {
            query.aggregation_cache = Some(teaql_core::AggregationCacheOptions::enabled(
                options.propagate_cache_expired_millis,
            ));
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

        let mut aggregate_rows = child_repo.fetch_compact_all_internal(query).await?;
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
                if !row.contains_key(&plan.foreign_key)
                    && let Some(value) = row.remove(&foreign_key_column)
                {
                    row.insert(plan.foreign_key.clone(), value);
                }
            }
        }
        attach_relation_aggregate_rows(parent_rows, parent_keys, &plan, aggregate, aggregate_rows);
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
        parent_keys: &'b [CompactRow],
        plan: &'b RelationLoadPlan,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            let capabilities =
                teaql_data_service::DataServiceExecutor::capabilities(self.data_service.executor);
            let parent_count = unique_relation_values(parent_keys, &plan.local_key).len();
            let selected_plan = relation_top_n_execution_plan(&capabilities, plan, parent_count);
            let scope = self.data_service.metadata.context.start_runtime_operation(
                relation_top_n_operation(plan, selected_plan, parent_count),
            );
            let result = scope
                .run(async {
                    let child_repo = self.relation_child_repo(plan);
                    let mut child_rows = self
                        .fetch_relation_rows(&child_repo, plan, parent_keys, false)
                        .await?;
                    for child in &mut child_rows {
                        child.remove(teaql_core::PARTITION_RANK_PROPERTY);
                    }
                    // This planner owns the subtree. Load each nested relation once for
                    // the whole child batch, before publishing it into the parent graph.
                    let attachment_keys = child_rows
                        .iter()
                        .map(|row| row.get(&plan.foreign_key).cloned())
                        .collect::<Vec<_>>();
                    let child_keys = relation_key_rows(&child_rows, &plan.children);
                    for child_plan in &plan.children {
                        child_repo
                            .enhance_plan(&mut child_rows, &child_keys, child_plan)
                            .await?;
                    }
                    self.attach_relation_rows(
                        parent_rows,
                        parent_keys,
                        plan,
                        child_rows,
                        attachment_keys,
                    );
                    if plan
                        .query
                        .as_ref()
                        .is_some_and(|query| !query.facets.is_empty())
                    {
                        for (parent, keys) in parent_rows.iter_mut().zip(parent_keys) {
                            let facets = self
                                .relation_facets(plan, keys.get(&plan.local_key))
                                .await?;
                            if let Some(mut list) = parent.take_loaded_relation(&plan.relation_name)
                            {
                                list.facets = facets;
                                parent.set_loaded_relation(plan.relation_name.clone(), list);
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
        root: &'b crate::EntityRuntimeState,
        graph: &'b mut crate::EntityGraphBuilder,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            let child_repo = self.relation_child_repo(plan);
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
                let local_value = parent.get(&plan.local_key);
                let related = local_value
                    .and_then(|value| {
                        let key = FlatIdentityKey::from_value(value);
                        if plan.local_key == "id" {
                            buckets.remove(&key)
                        } else {
                            buckets.get(&key).cloned()
                        }
                    })
                    .unwrap_or_default();

                let facets = self.relation_facets(plan, local_value).await?;
                if !plan.many && !facets.is_empty() {
                    let owner_id = parent.get("id").and_then(Value::try_u64).ok_or_else(|| {
                        DataServiceError::Entity(teaql_core::EntityError::new(
                            &plan.parent_entity,
                            "Facet owner is missing its u64 id",
                        ))
                    })?;
                    graph.install_relation_facets(
                        &plan.parent_entity,
                        owner_id,
                        &plan.relation_name,
                        facets.clone(),
                    );
                }

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
                            .decode_compact_smart_list_into_graph(
                                &plan.target_entity,
                                SmartList {
                                    facets,
                                    ..SmartList::new(related)
                                },
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
        root: &'b crate::EntityRuntimeState,
        graph: &'b mut crate::EntityGraphBuilder,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            let child_repo = self.relation_child_repo(plan);
            let child_rows = self
                .fetch_relation_rows(&child_repo, plan, parent_rows, true)
                .await?;

            for child_plan in &plan.children {
                if child_plan.children.is_empty() {
                    child_repo
                        .hydrate_compact_flat_leaf(&child_rows, child_plan, root, graph)
                        .await?;
                } else {
                    child_repo
                        .hydrate_compact_flat_plan(&child_rows, child_plan, root, graph)
                        .await?;
                }
            }

            self.install_compact_flat_relation(parent_rows, plan, child_rows, root, graph)
                .await
        })
    }

    fn hydrate_compact_flat_leaf<'b>(
        &'b self,
        parent_rows: &'b [CompactRow],
        plan: &'b RelationLoadPlan,
        root: &'b crate::EntityRuntimeState,
        graph: &'b mut crate::EntityGraphBuilder,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            let child_repo = self.relation_child_repo(plan);
            let child_rows = self
                .fetch_relation_rows(&child_repo, plan, parent_rows, true)
                .await?;
            self.install_compact_flat_relation(parent_rows, plan, child_rows, root, graph)
                .await
        })
    }

    async fn install_compact_flat_relation(
        &self,
        parent_rows: &[CompactRow],
        plan: &RelationLoadPlan,
        child_rows: Vec<CompactRow>,
        root: &crate::EntityRuntimeState,
        graph: &mut crate::EntityGraphBuilder,
    ) -> Result<(), DataServiceError<E::Error>> {
        // A forward to-one relation only needs its fetched targets installed in the shared
        // identity table. Building owner buckets and then removing them one parent at a time
        // creates a map and one Vec per distinct target without adding information.
        if !plan.many && plan.local_key != "id" {
            if plan
                .query
                .as_ref()
                .is_some_and(|query| !query.facets.is_empty())
            {
                for parent in parent_rows {
                    let owner_id = parent.get("id").and_then(Value::try_u64).ok_or_else(|| {
                        DataServiceError::Entity(teaql_core::EntityError::new(
                            &plan.parent_entity,
                            "Facet owner is missing its u64 id",
                        ))
                    })?;
                    let facets = self
                        .relation_facets(plan, parent.get(&plan.local_key))
                        .await?;
                    graph.install_relation_facets(
                        &plan.parent_entity,
                        owner_id,
                        &plan.relation_name,
                        facets,
                    );
                }
            }
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
            let local_value = parent.get(&plan.local_key);
            let related = local_value
                .and_then(|value| {
                    let key = FlatIdentityKey::from_value(value);
                    if plan.local_key == "id" {
                        buckets.remove(&key)
                    } else {
                        buckets.get(&key).cloned()
                    }
                })
                .unwrap_or_default();

            let facets = self.relation_facets(plan, local_value).await?;
            if !plan.many && !facets.is_empty() {
                let owner_id = parent.get("id").and_then(Value::try_u64).ok_or_else(|| {
                    DataServiceError::Entity(teaql_core::EntityError::new(
                        &plan.parent_entity,
                        "Facet owner is missing its u64 id",
                    ))
                })?;
                graph.install_relation_facets(
                    &plan.parent_entity,
                    owner_id,
                    &plan.relation_name,
                    facets.clone(),
                );
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
                        .decode_compact_smart_list_into_graph(
                            &plan.target_entity,
                            SmartList {
                                facets,
                                ..SmartList::new(related)
                            },
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
                        .decode_compact_entity_into_graph(&plan.target_entity, child, root, graph)
                        .map_err(DataServiceError::Entity)?;
                }
            }
        }
        Ok(())
    }

    fn relation_child_repo(&self, plan: &RelationLoadPlan) -> EntityDataService<'a, E> {
        let mut trace = self.trace_context.clone();
        trace.push(teaql_core::TraceNode::typed(
            teaql_core::TraceKind::Relation,
            plan.relation_name.clone(),
            None,
            format!("{}.{}", plan.parent_entity, plan.relation_name),
        ));
        self.scoped_data_service_internal(plan.target_entity.clone())
            .with_trace_context(trace)
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
        // The relation planner, not fetch_prepared_all, executes nested loads.
        query.relations.clear();
        query.facets.clear();
        ensure_projection(&mut query, &plan.foreign_key);
        for child in &plan.children {
            ensure_projection(&mut query, &child.local_key);
        }
        self.ensure_stable_top_n_order(plan, &mut query);
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
        let probe = relation_top_n_execution_plan(&capabilities, plan, ids.len())
            == RelationTopNExecutionPlan::BoundedProbes;

        if !probe {
            let query = if compact {
                self.query_for_compact_plan(plan, parent_rows)
            } else {
                self.query_for_plan(plan, parent_rows)
            };
            return child_repo.fetch_compact_all_internal(query).await;
        }

        // With execution metadata disabled, retain one semantic partition query and let an
        // embedded provider execute its indexed parent probes behind one executor boundary.
        // When metadata is enabled we intentionally keep one observable result per SQL probe.
        let provider_owns_probe_batch = capabilities.small_parent_relation_probes
            && plan
                .query
                .as_ref()
                .is_none_or(|query| query.top_n_probe_parent_threshold.is_none());
        if provider_owns_probe_batch && !self.data_service.metadata.capture_execution_metadata() {
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

    async fn relation_facets(
        &self,
        plan: &RelationLoadPlan,
        local_value: Option<&Value>,
    ) -> Result<BTreeMap<String, SmartList<CompactRow>>, DataServiceError<E::Error>> {
        let Some(source) = plan.query.as_ref().filter(|query| !query.facets.is_empty()) else {
            return Ok(BTreeMap::new());
        };
        let child_repo = self.relation_child_repo(plan);
        let mut outer = source.clone();
        let options = teaql_core::request::QueryOptions {
            facets: std::mem::take(&mut outer.facets),
            ..Default::default()
        };
        // Facet membership belongs to the complete filtered relation for this
        // owner, never its limited materialized page or another owner's rows.
        outer.slice = None;
        outer.partition_by = None;
        outer.order_by.clear();
        outer = outer.and_filter(match local_value {
            Some(value) if !matches!(value, Value::Null | Value::TypedNull(_)) => {
                Expr::eq(plan.foreign_key.clone(), value.clone())
            }
            _ => Expr::Value(Value::Bool(false)),
        });
        let intent = child_repo
            .request_intent_for(&outer)
            .map_err(DataServiceError::Runtime)?;
        outer.comment = Some(intent.comment().to_owned());
        outer.purpose = Some(intent.purpose().to_owned());
        let mut trace = child_repo.trace_context.clone();
        trace.extend(outer.trace_chain);
        outer.trace_chain = trace;
        crate::execute_facets(&child_repo, &outer, &options)
            .await
            .map_err(DataServiceError::Runtime)
    }

    fn base_relation_query(&self, plan: &RelationLoadPlan) -> SelectQuery {
        let mut query = plan
            .query
            .clone()
            .unwrap_or_else(|| SelectQuery::new(plan.target_entity.clone()));
        query.entity = plan.target_entity.clone();
        query.relations.clear();
        query.facets.clear();
        ensure_projection(&mut query, &plan.foreign_key);
        for child in &plan.children {
            ensure_projection(&mut query, &child.local_key);
        }
        self.ensure_stable_top_n_order(plan, &mut query);
        query
    }

    fn ensure_stable_top_n_order(&self, plan: &RelationLoadPlan, query: &mut SelectQuery) {
        if query.slice.is_none() {
            return;
        }
        if !query.group_by.is_empty() || !query.aggregates.is_empty() {
            // A grouped result has group identity, not a source-row identity.
            // Adding the entity id is invalid on strict SQL databases unless it
            // is itself grouped, and does not make aggregate pagination stable.
            for field in &query.group_by {
                if !query
                    .order_by
                    .iter()
                    .any(|order| order.expr.is_none() && order.field == *field)
                {
                    query.order_by.push(OrderBy::asc(field.clone()));
                }
            }
            return;
        }
        let Some(id_property) = self
            .data_service
            .metadata
            .entity(&plan.target_entity)
            .and_then(|entity| entity.id_property())
        else {
            return;
        };
        if !query
            .order_by
            .iter()
            .any(|order| order.expr.is_none() && order.field == id_property.name)
        {
            query.order_by.push(OrderBy::asc(id_property.name.clone()));
        }
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
        // The flat hydrator owns the relation tree and recursively loads each child plan into
        // one shared identity graph. Leaving nested relations on this single-layer query makes
        // fetch_compact_all_internal hydrate the same subtree first; the flat hydrator then
        // hydrates it again, causing an exponential number of duplicate relation queries.
        query.relations.clear();
        query.facets.clear();
        ensure_projection(&mut query, &plan.foreign_key);
        for child in &plan.children {
            ensure_projection(&mut query, &child.local_key);
        }
        self.ensure_stable_top_n_order(plan, &mut query);
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
        parent_keys: &[CompactRow],
        plan: &RelationLoadPlan,
        child_rows: Vec<CompactRow>,
        attachment_keys: Vec<Option<Value>>,
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
            // An explicit nested request owns its result, including filtered null.
            // Inverse convenience wiring must not override that predicate.
            .filter(|relation| {
                !plan
                    .children
                    .iter()
                    .any(|child| child.relation_name == relation.name)
            })
            .map(|relation| (relation.name.clone(), relation.many));

        let mut buckets: BTreeMap<String, Vec<CompactRow>> = BTreeMap::new();
        for (child, key) in child_rows.into_iter().zip(attachment_keys) {
            if let Some(key) = key {
                buckets
                    .entry(graph_identity_key(&key))
                    .or_default()
                    .push(child);
            }
        }

        for (parent, keys) in parent_rows.iter_mut().zip(parent_keys) {
            let related = keys
                .get(&plan.local_key)
                .and_then(|value| buckets.get(&graph_identity_key(value)))
                .cloned()
                .unwrap_or_default();
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
            if plan
                .query
                .as_ref()
                .is_some_and(|query| !query.facets.is_empty())
                || related.iter().any(CompactRow::has_loaded_relations)
            {
                let mut result = SmartList::new(related.clone());
                if !plan.many {
                    result.data.truncate(1);
                }
                parent.set_loaded_relation(plan.relation_name.clone(), result);
            }
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
    fn topn_001_002_003_004_plan_respects_default_threshold_and_sqlite_policy() {
        let plan = limited_many_plan();
        let mut capabilities = teaql_data_service::DataServiceCapabilities::default();
        assert_eq!(
            relation_top_n_execution_plan(&capabilities, &plan, 6),
            RelationTopNExecutionPlan::Window
        );

        capabilities.small_parent_relation_probes = true;
        assert_eq!(
            relation_top_n_execution_plan(&capabilities, &plan, 10_000),
            RelationTopNExecutionPlan::BoundedProbes
        );

        let mut threshold_plan = plan.clone();
        threshold_plan
            .query
            .as_mut()
            .unwrap()
            .top_n_probe_parent_threshold = Some(4);
        capabilities.small_parent_relation_probes = false;
        assert_eq!(
            relation_top_n_execution_plan(&capabilities, &threshold_plan, 4),
            RelationTopNExecutionPlan::BoundedProbes
        );
        assert_eq!(
            relation_top_n_execution_plan(&capabilities, &threshold_plan, 5),
            RelationTopNExecutionPlan::Window
        );

        threshold_plan
            .query
            .as_mut()
            .unwrap()
            .top_n_probe_parent_threshold = Some(0);
        capabilities.small_parent_relation_probes = true;
        assert_eq!(
            relation_top_n_execution_plan(&capabilities, &threshold_plan, 1),
            RelationTopNExecutionPlan::Window
        );
    }

    #[test]
    fn topn_010_plan_telemetry_is_safe_and_complete() {
        let mut plan = limited_many_plan();
        plan.query.as_mut().unwrap().top_n_probe_parent_threshold = Some(32);
        let operation =
            relation_top_n_operation(&plan, RelationTopNExecutionPlan::BoundedProbes, 6);

        assert_eq!(operation.family, "relation_load");
        assert_eq!(
            operation.attributes.get("teaql.relation.selected_plan"),
            Some(&crate::RuntimeAttributeValue::String(
                "BOUNDED_PROBES".to_owned()
            ))
        );
        assert_eq!(
            operation.attributes.get("teaql.relation.parent_count"),
            Some(&crate::RuntimeAttributeValue::Integer(6))
        );
        assert_eq!(
            operation.attributes.get("teaql.relation.per_parent_limit"),
            Some(&crate::RuntimeAttributeValue::Integer(10))
        );
        assert_eq!(
            operation
                .attributes
                .get("teaql.relation.configured_probe_threshold"),
            Some(&crate::RuntimeAttributeValue::Integer(32))
        );
        assert_eq!(
            operation.attributes.get("teaql.relation.probe_count"),
            Some(&crate::RuntimeAttributeValue::Integer(6))
        );
        assert!(!operation.attributes.contains_key("teaql.entity.id"));
    }
}
