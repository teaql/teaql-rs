use std::collections::BTreeMap;
use std::slice;

use teaql_core::{
    Aggregate, Expr, ObjectGroupBy, Record, RelationAggregate, RelationLoad, SelectQuery, Value,
};

use crate::{DataServiceError, RuntimeError};

use super::{EntityDataService, RelationLoadPlan, helpers::*};

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
        parent_rows: &[Record],
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

    pub async fn enhance_relations(
        &self,
        parent_rows: &mut [Record],
    ) -> Result<(), DataServiceError<E::Error>> {
        let plans = self.relation_plans().map_err(DataServiceError::Runtime)?;
        for plan in plans {
            self.enhance_plan(parent_rows, &plan).await?;
        }
        Ok(())
    }

    pub async fn enhance_query_relations(
        &self,
        parent_rows: &mut [Record],
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

    pub fn enhance_relation_aggregates<'b>(
        &'b self,
        parent_rows: &'b mut [Record],
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

    pub fn enhance_object_group_bys<'b>(
        &'b self,
        rows: &'b mut [Record],
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
                    .scoped_data_service(query.entity.clone())
                    .with_trace_context(parent_trace_chain.to_vec())
                    .fetch_all(&query)
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
                            .map(Value::object)
                            .unwrap_or(Value::Null);
                        row.insert(group_by.property_name.clone(), value);
                    }
                }
            }
            Ok(())
        })
    }

    pub fn enhance_child_queries<'b>(
        &'b self,
        rows: &'b mut [Record],
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
                    .scoped_data_service(query.entity.clone())
                    .with_trace_context(parent_trace_chain.to_vec())
                    .fetch_all(&query)
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
        parent_rows: &mut [Record],
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

        let child_repo = self.scoped_data_service(plan.target_entity.clone());
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
            .fetch_all(&query)
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
                let child_repo = self.scoped_data_service(relation.target_entity.clone());
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
                let child_repo = self.scoped_data_service(relation.target_entity.clone());
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
        parent_rows: &'b mut [Record],
        plan: &'b RelationLoadPlan,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            let child_repo = self.scoped_data_service(plan.target_entity.clone());
            let query = self.query_for_plan(plan, parent_rows);
            let child_rows = child_repo.fetch_all(&query).await?;
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
    }

    fn enhance_child_record<'b>(
        &'b self,
        child: &'b mut Record,
        plans: &'b [RelationLoadPlan],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            for plan in plans {
                self.enhance_plan(slice::from_mut(child), plan).await?;
            }
            Ok(())
        })
    }

    fn query_for_plan(&self, plan: &RelationLoadPlan, parent_rows: &[Record]) -> SelectQuery {
        let ids = parent_rows
            .iter()
            .filter_map(|row| row.get(&plan.local_key).cloned())
            .collect::<Vec<_>>();

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
        query
    }

    fn attach_relation_rows(
        &self,
        parent_rows: &mut [Record],
        plan: &RelationLoadPlan,
        child_rows: Vec<Record>,
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

        let mut buckets: BTreeMap<String, Vec<Record>> = BTreeMap::new();
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
                                    let entry = child
                                        .entry(inverse_relation.clone())
                                        .or_insert_with(|| Value::List(Vec::new()));
                                    if let Value::List(list) = entry {
                                        list.push(Value::object(parent_object.clone()));
                                    }
                                }
                                false => {
                                    child.insert(
                                        inverse_relation.clone(),
                                        Value::object(parent_object.clone()),
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
                        Value::List(related.into_iter().map(Value::object).collect()),
                    );
                }
                false => {
                    let value = related
                        .into_iter()
                        .next()
                        .map(Value::object)
                        .unwrap_or(Value::Null);
                    parent.insert(plan.relation_name.clone(), value);
                }
            }
        }
    }
}
