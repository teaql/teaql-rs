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

    pub(crate) async fn enhance_relations_internal(
        &self,
        parent_rows: &mut [Record],
    ) -> Result<(), DataServiceError<E::Error>> {
        let plans = self.relation_plans().map_err(DataServiceError::Runtime)?;
        for plan in plans {
            self.enhance_plan(parent_rows, &plan).await?;
        }
        Ok(())
    }

    pub(crate) async fn enhance_query_relations_internal(
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

    pub(crate) fn enhance_relation_aggregates_internal<'b>(
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

    pub(crate) fn enhance_object_group_bys_internal<'b>(
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
                    .scoped_data_service_internal(query.entity.clone())
                    .with_trace_context(parent_trace_chain.to_vec())
                    .fetch_all_internal(&query)
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

    pub(crate) fn enhance_child_queries_internal<'b>(
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
                    .scoped_data_service_internal(query.entity.clone())
                    .with_trace_context(parent_trace_chain.to_vec())
                    .fetch_all_internal(&query)
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
            .fetch_all_internal(&query)
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
        parent_rows: &'b mut [Record],
        plan: &'b RelationLoadPlan,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), DataServiceError<E::Error>>> + Send + 'b>,
    > {
        Box::pin(async move {
            let child_repo = self.scoped_data_service_internal(plan.target_entity.clone());
            let query = self.query_for_plan(plan, parent_rows);
            let child_rows = child_repo.fetch_all_internal(&query).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_service::{ContextDataService, UserContextMetadata};
    use crate::{InMemoryMetadataStore, UserContext};
    use teaql_core::{
        Aggregate, AggregationCacheOptions, DataType, EntityDescriptor, Expr, ObjectGroupBy,
        PropertyDescriptor, Record, RelationAggregate, RelationDescriptor, RelationLoad,
        SelectQuery, Value,
    };
    use teaql_data_service::{
        DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
        MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest,
        QueryResult,
    };

    #[derive(Debug)]
    struct MyError;
    impl std::fmt::Display for MyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MyError")
        }
    }
    impl std::error::Error for MyError {}

    struct MyExecutor {
        rows: Vec<Record>,
    }

    impl DataServiceExecutor for MyExecutor {
        type Error = MyError;
        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for MyExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            let mut rows = self.rows.clone();
            if request.query.entity == "Profile" {
                rows = vec![Record::from([
                    ("id".to_string(), Value::U64(1)),
                    ("user_id".to_string(), Value::U64(1)),
                ])];
            } else if request.query.entity == "Post" {
                rows = vec![Record::from([
                    ("id".to_string(), Value::U64(100)),
                    ("author_id".to_string(), Value::U64(1)),
                ])];
            }
            Ok(QueryResult {
                rows,
                metadata: ExecutionMetadata {
                    debug_query: None,
                    backend: "stub".to_owned(),
                    operation: DataServiceOperation::Query,
                    started_at: std::time::SystemTime::now(),
                    ended_at: std::time::SystemTime::now(),
                    affected_rows: None,
                    result_count: Some(1),
                    trace_chain: Vec::new(),
                    comment: None,
                    backend_request_id: None,
                },
            })
        }
    }

    impl MutationExecutor for MyExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            Ok(MutationResult {
                affected_rows: 1,
                generated_values: Record::new(),
                metadata: ExecutionMetadata {
                    debug_query: None,
                    backend: "stub".to_owned(),
                    operation: DataServiceOperation::Update,
                    started_at: std::time::SystemTime::now(),
                    ended_at: std::time::SystemTime::now(),
                    affected_rows: Some(1),
                    result_count: None,
                    trace_chain: Vec::new(),
                    comment: None,
                    backend_request_id: None,
                },
            })
        }
    }

    fn setup_context() -> UserContext {
        let mut user = EntityDescriptor::new("User");
        user.properties
            .push(PropertyDescriptor::new("id", DataType::U64).id());
        user.properties
            .push(PropertyDescriptor::new("profile_id", DataType::U64));
        user.relations.push(RelationDescriptor {
            name: "Profile".to_string(),
            target_entity: "Profile".to_string(),
            local_key: "id".to_string(),
            foreign_key: "user_id".to_string(),
            many: false,
            attach: false,
            delete_missing: false,
        });
        user.relations.push(RelationDescriptor {
            name: "Posts".to_string(),
            target_entity: "Post".to_string(),
            local_key: "id".to_string(),
            foreign_key: "author_id".to_string(),
            many: true,
            attach: false,
            delete_missing: false,
        });
        user.relations.push(RelationDescriptor {
            name: "Child".to_string(),
            target_entity: "Child".to_string(),
            local_key: "id".to_string(),
            foreign_key: "parent_id".to_string(),
            many: false,
            attach: false,
            delete_missing: false,
        });

        let mut profile = EntityDescriptor::new("Profile");
        profile
            .properties
            .push(PropertyDescriptor::new("id", DataType::U64).id());
        profile
            .properties
            .push(PropertyDescriptor::new("user_id", DataType::U64));
        // Inverse relation mapping
        profile.relations.push(RelationDescriptor {
            name: "User".to_string(),
            target_entity: "User".to_string(),
            local_key: "user_id".to_string(),
            foreign_key: "id".to_string(),
            many: false,
            attach: false,
            delete_missing: false,
        });

        let mut post = EntityDescriptor::new("Post");
        post.properties
            .push(PropertyDescriptor::new("id", DataType::U64).id());
        post.properties
            .push(PropertyDescriptor::new("author_id", DataType::U64));
        post.relations.push(RelationDescriptor {
            name: "Author".to_string(),
            target_entity: "User".to_string(),
            local_key: "author_id".to_string(),
            foreign_key: "id".to_string(),
            many: true,
            attach: false,
            delete_missing: false,
        });

        let store = InMemoryMetadataStore::new()
            .with_entity(user)
            .with_entity(profile)
            .with_entity(post);

        let mut ctx = UserContext::new().with_metadata(store);
        ctx.insert_resource(MyExecutor {
            rows: vec![Record::from([
                ("id".to_string(), Value::U64(1)),
                ("profile_id".to_string(), Value::U64(10)),
            ])],
        });
        ctx
    }

    #[tokio::test]
    async fn test_relation_query_missing() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };

        let err = ds.relation_query("UnknownRel", &[]).unwrap_err();
        assert!(matches!(err, RuntimeError::MissingRelation { .. }));
    }

    #[tokio::test]
    async fn test_enhance_object_group_bys_internal() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };

        let mut rows = vec![Record::from([("profile_id".to_string(), Value::U64(1))])];

        let group_by = ObjectGroupBy {
            property_name: "profile_obj".to_string(),
            storage_field: "profile_id".to_string(),
            query: SelectQuery::new("Profile"),
        };

        ds.enhance_object_group_bys_internal(&mut rows, &[group_by], &[])
            .await
            .unwrap();

        // Assert Profile was joined
        let obj = rows[0].get("profile_obj").unwrap();
        assert!(matches!(obj, Value::Object(_)));
    }

    #[tokio::test]
    async fn test_enhance_child_queries_internal() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };

        let mut rows = vec![Record::from([("id".to_string(), Value::U64(1))])];

        let child_query = SelectQuery::new("Profile"); // We query profile to merge back
        ds.enhance_child_queries_internal(&mut rows, &[child_query], &[])
            .await
            .unwrap();

        // Assert profile properties merged into rows
        assert!(rows[0].contains_key("user_id"));
    }

    #[tokio::test]
    async fn test_enhance_relation_aggregate() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };

        // 1. empty ids
        let mut rows = vec![];
        let agg = RelationAggregate {
            alias: "count".to_string(),
            relation_name: "Posts".to_string(),
            query: SelectQuery::new("Post"),
            single_result: true,
        };
        ds.enhance_relation_aggregates_internal(&mut rows, &[agg.clone()], None, &[])
            .await
            .unwrap();

        // 2. missing relation
        let mut rows2 = vec![Record::from([("id".to_string(), Value::U64(1))])];
        let agg2 = RelationAggregate {
            alias: "count".to_string(),
            relation_name: "MissingRel".to_string(),
            query: SelectQuery::new("Post"),
            single_result: true,
        };
        assert!(
            ds.enhance_relation_aggregates_internal(&mut rows2, &[agg2], None, &[])
                .await
                .is_err()
        );

        // 3. valid propagation
        let cache_opt = AggregationCacheOptions {
            propagate: true,
            propagate_cache_expired_millis: 100,
            enabled: true,
            cache_expired_millis: 0,
        };
        ds.enhance_relation_aggregates_internal(&mut rows2, &[agg], Some(cache_opt), &[])
            .await
            .unwrap();
        assert!(rows2[0].contains_key("count"));
    }

    #[tokio::test]
    async fn test_build_relation_plans() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };

        let plans = ds.build_relation_plans(
            "User",
            &[
                "Profile".to_string(),
                "Posts".to_string(),
                "Unknown".to_string(),
            ],
        );
        assert!(plans.is_err());

        // Nested loads
        let plans2 = ds
            .build_relation_plans("User", &["Profile.User".to_string()])
            .unwrap();
        assert_eq!(plans2.len(), 1);
        assert_eq!(plans2[0].relation_name, "Profile");
        assert_eq!(plans2[0].children.len(), 1);
        assert_eq!(plans2[0].children[0].relation_name, "User");
    }

    #[tokio::test]
    async fn test_build_relation_plans_from_loads() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };

        let loads = vec![RelationLoad {
            name: "Unknown".to_string(),
            query: None,
        }];
        assert!(ds.build_relation_plans_from_loads("User", &loads).is_err());
    }

    #[tokio::test]
    async fn test_enhance_plan() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };

        // Test enhance_plan with child (Profile has User child)
        let mut rows = vec![Record::from([("id".to_string(), Value::U64(1))])];
        let plan = ds
            .build_relation_plans("User", &["Profile.User".to_string()])
            .unwrap();
        ds.enhance_plan(&mut rows, &plan[0]).await.unwrap();

        assert!(rows[0].contains_key("Profile"));

        let mut rows2 = vec![Record::from([("id".to_string(), Value::U64(1))])];
        let plan2 = ds
            .build_relation_plans("User", &["Posts.Author".to_string()])
            .unwrap();
        ds.enhance_plan(&mut rows2, &plan2[0]).await.unwrap();

        assert!(rows2[0].contains_key("Posts"));
    }
    #[tokio::test]
    async fn test_relation_loads_and_plans() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };

        // By default, behavior relation_loads is empty unless behavior is defined, but we can just test relation_plans returns ok
        let loads = ds.relation_loads();
        assert!(loads.is_empty());
        let plans = ds.relation_plans().unwrap();
        assert!(plans.is_empty());

        let err = ds.relation_query("Missing", &[]).unwrap_err();
        assert!(matches!(err, RuntimeError::MissingRelation { .. }));
    }

    #[tokio::test]
    async fn test_enhance_relations_internal() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };
        let mut rows = vec![Record::from([("id".to_string(), Value::U64(1))])];
        // behavior relation loads empty, so it does nothing
        ds.enhance_relations_internal(&mut rows).await.unwrap();

        let mut query = SelectQuery::new("User");
        query.relations.push(RelationLoad { name: "Profile".to_string(), query: None });
        ds.enhance_query_relations_internal(&mut rows, &query).await.unwrap();
        assert!(rows[0].contains_key("Profile"));
    }

    #[tokio::test]
    async fn test_enhance_object_group_bys_empty_and_missing() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };

        let mut rows_empty = vec![];
        let group_by = ObjectGroupBy {
            property_name: "profile_obj".to_string(),
            storage_field: "profile_id".to_string(),
            query: SelectQuery::new("Profile"),
        };
        // empty ids branch
        ds.enhance_object_group_bys_internal(&mut rows_empty, &[group_by.clone()], &[]).await.unwrap();

        let mut rows_missing = vec![Record::from([("id".to_string(), Value::U64(1))])];
        // storage_field is missing
        ds.enhance_object_group_bys_internal(&mut rows_missing, &[group_by.clone()], &[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_enhance_child_queries_empty() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };
        let mut rows = vec![];
        ds.enhance_child_queries_internal(&mut rows, &[SelectQuery::new("Profile")], &[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_attach_relation_rows_branches() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };

        let mut rows = vec![Record::from([("id".to_string(), Value::U64(1))])];
        let plan = RelationLoadPlan {
            parent_entity: "User".to_string(),
            relation_name: "Posts".to_string(),
            path: "Posts".to_string(),
            target_entity: "Post".to_string(),
            local_key: "id".to_string(),
            foreign_key: "author_id".to_string(),
            many: true,
            query: None,
            children: vec![],
        };
        let child_rows = vec![
            Record::from([("id".to_string(), Value::U64(100)), ("author_id".to_string(), Value::U64(1))]),
            Record::from([("id".to_string(), Value::U64(101)), ("author_id".to_string(), Value::U64(1))]),
        ];
        
        // This exercises attach_relation_rows with many=true and an inverse relation Author (many=true in setup, wait actually Author is many: true in setup_context? Let's assume yes)
        ds.attach_relation_rows(&mut rows, &plan, child_rows);
        if let Some(Value::List(l)) = rows[0].get("Posts") {
            assert_eq!(l.len(), 2);
        } else {
            panic!("Expected Value::List");
        }
    }

    #[tokio::test]
    async fn test_enhance_plan_deep() {
        let ctx = setup_context();
        let executor = ctx.get_resource::<MyExecutor>().unwrap();
        let ds = EntityDataService {
            entity: "User".to_string(),
            data_service: ContextDataService {
                metadata: UserContextMetadata { context: &ctx },
                executor,
            },
            trace_context: Vec::new(),
        };

        let mut rows = vec![Record::from([("id".to_string(), Value::U64(1))])];
        // Profile doesn't have User in this test's child data natively returned from executor, but we can test the structure
        let plan = RelationLoadPlan {
            parent_entity: "User".to_string(),
            relation_name: "Posts".to_string(),
            path: "Posts".to_string(),
            target_entity: "Post".to_string(),
            local_key: "id".to_string(),
            foreign_key: "author_id".to_string(),
            many: true,
            query: None,
            children: vec![
                RelationLoadPlan {
                    parent_entity: "Post".to_string(),
                    relation_name: "Author".to_string(),
                    path: "Author".to_string(),
                    target_entity: "User".to_string(),
                    local_key: "author_id".to_string(),
                    foreign_key: "id".to_string(),
                    many: true,
                    query: None,
                    children: vec![],
                }
            ],
        };
        ds.enhance_plan(&mut rows, &plan).await.unwrap();
        assert!(rows[0].contains_key("Posts"));
    }
}
