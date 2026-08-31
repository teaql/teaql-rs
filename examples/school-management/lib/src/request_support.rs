#![allow(unused_imports)]
#![allow(async_fn_in_trait)]
use std::{collections::BTreeMap, future::Future, marker::PhantomData};

use serde_json::Value as JsonValue;
use teaql_core::{
    BinaryOp, CompactRow, Expr, RelationAggregate as RuntimeRelationAggregate, SelectQuery,
    SmartList,
};
use teaql_runtime::{
    ContextError, DataServiceError, EntityDataServiceBehavior, GraphNode, PurposedSelectQuery,
    RuntimeError, UserContext,
};

pub type TeaqlEntityStream<'a, T, E> =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<T, E>> + 'a>>;

// Re-export query builder types from teaql_core::request
pub use teaql_core::request::{
    apply_relation_selections, apply_runtime_metadata, attach_facets, dynamic_json_filter_expr,
    dynamic_json_operator, dynamic_json_u64_field, dynamic_json_value_to_teaql_value,
    dynamic_json_values, field_operator_column_expr, field_operator_expr,
    merge_outer_filter_into_facet_aggregates, remove_default_live_filter, remove_filter_expr,
    required_text, required_value, runtime_relation_aggregates, DateRange, EntityReference,
    FacetRequest, FieldOperator, ObjectGroupBy, QueryOptions, QuerySelection, RawDynamicProperty,
    RawProjection, RelationAggregate, RelationFilter, RelationSelection, UnsafeRawSqlSegment,
    COUNT_ALIAS, TYPE_FIELD, TYPE_GROUP_FIELD,
};

pub trait TeaqlQueryRepository {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn fetch_all(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<Vec<CompactRow>, DataServiceError<Self::Error>>;

    async fn fetch_smart_list(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<SmartList<CompactRow>, DataServiceError<Self::Error>>;

    async fn fetch_smart_list_with_relation_aggregates(
        &self,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
    ) -> Result<SmartList<CompactRow>, DataServiceError<Self::Error>>;

    async fn fetch_stream<'a>(
        &'a self,
        query: &PurposedSelectQuery,
    ) -> Result<
        teaql_data_service::QueryStream<'a, DataServiceError<Self::Error>>,
        DataServiceError<Self::Error>,
    >;
}

pub trait TeaqlEntityRepository: TeaqlQueryRepository {
    async fn fetch_enhanced_entities<T>(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<SmartList<T>, DataServiceError<Self::Error>>
    where
        T: teaql_core::Entity;

    async fn fetch_enhanced_entities_with_relation_aggregates<T>(
        &self,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<Self::Error>>
    where
        T: teaql_core::Entity;

    async fn fetch_enhanced_entities_with_relation_aggregates_owned<T>(
        &self,
        query: PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<Self::Error>>
    where
        T: teaql_core::Entity;
}

impl<'a, E> TeaqlQueryRepository for teaql_runtime::EntityDataService<'a, E>
where
    E: teaql_data_service::QueryExecutor
        + teaql_data_service::MutationExecutor
        + teaql_data_service::StreamQueryExecutor
        + Send
        + Sync
        + 'static,
{
    type Error = E::Error;

    async fn fetch_all(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<Vec<CompactRow>, DataServiceError<Self::Error>> {
        teaql_runtime::EntityDataService::fetch_all(self, query).await
    }

    async fn fetch_smart_list(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<SmartList<CompactRow>, DataServiceError<Self::Error>> {
        teaql_runtime::EntityDataService::fetch_smart_list(self, query).await
    }

    async fn fetch_smart_list_with_relation_aggregates(
        &self,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
    ) -> Result<SmartList<CompactRow>, DataServiceError<Self::Error>> {
        teaql_runtime::EntityDataService::fetch_smart_list_with_relation_aggregates(
            self,
            query,
            relation_aggregates,
        )
        .await
    }

    async fn fetch_stream<'b>(
        &'b self,
        query: &PurposedSelectQuery,
    ) -> Result<
        teaql_data_service::QueryStream<'b, DataServiceError<Self::Error>>,
        DataServiceError<Self::Error>,
    > {
        teaql_runtime::EntityDataService::fetch_stream(self, query).await
    }
}

impl<'a, E> TeaqlEntityRepository for teaql_runtime::EntityDataService<'a, E>
where
    E: teaql_data_service::QueryExecutor
        + teaql_data_service::MutationExecutor
        + teaql_data_service::StreamQueryExecutor
        + Send
        + Sync
        + 'static,
{
    async fn fetch_enhanced_entities<T>(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<SmartList<T>, DataServiceError<Self::Error>>
    where
        T: teaql_core::Entity,
    {
        teaql_runtime::EntityDataService::fetch_enhanced_entities(self, query).await
    }

    async fn fetch_enhanced_entities_with_relation_aggregates<T>(
        &self,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<Self::Error>>
    where
        T: teaql_core::Entity,
    {
        teaql_runtime::EntityDataService::fetch_enhanced_entities_with_relation_aggregates(
            self,
            query,
            relation_aggregates,
        )
        .await
    }

    async fn fetch_enhanced_entities_with_relation_aggregates_owned<T>(
        &self,
        query: PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<Self::Error>>
    where
        T: teaql_core::Entity,
    {
        teaql_runtime::EntityDataService::fetch_enhanced_entities_with_relation_aggregates_owned(
            self,
            query,
            relation_aggregates,
        )
        .await
    }
}

pub type TeaqlDataServiceError<R> = DataServiceError<<R as TeaqlQueryRepository>::Error>;

pub(crate) fn authorize_query(mut query: SelectQuery) -> Result<PurposedSelectQuery, RuntimeError> {
    if query
        .comment
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        return Err(RuntimeError::Graph(
            "generated query reached the repository without .comment(...)".to_owned(),
        ));
    }
    let purpose = query
        .trace_chain
        .pop()
        .map(|node| node.comment)
        .filter(|purpose| !purpose.trim().is_empty())
        .ok_or_else(|| {
            RuntimeError::Graph(
                "generated query reached the repository without .purpose(...)".to_owned(),
            )
        })?;
    Ok(PurposedSelectQuery::new(query, purpose))
}

pub trait TeaqlRuntime {
    fn user_context(&self) -> &UserContext;

    fn fetch_facet_smart_list(
        &self,
        entity: &str,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
        trace_context: Vec<teaql_core::TraceNode>,
    ) -> impl std::future::Future<Output = Result<SmartList<CompactRow>, RuntimeError>> + Send;
}

/// Internal trait for repository access. Application code should not use this trait directly.
#[doc(hidden)]
pub trait AuditedSave<'a, C>
where
    C: TeaqlRepositoryProvider + ?Sized + 'a,
{
    type Error;
    type Entity;
    fn save(
        self,
        context: &'a C,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Entity, Self::Error>> + '_>>;
}

pub trait TeaqlRepositoryProvider: TeaqlRuntime {
    type PlatformRepository<'a>: TeaqlEntityRepository + 'a
    where
        Self: 'a;

    fn platform_repository(&self) -> Result<Self::PlatformRepository<'_>, ContextError>;
    type SchoolTypeRepository<'a>: TeaqlEntityRepository + 'a
    where
        Self: 'a;

    fn school_type_repository(&self) -> Result<Self::SchoolTypeRepository<'_>, ContextError>;
    type SchoolRepository<'a>: TeaqlEntityRepository + 'a
    where
        Self: 'a;

    fn school_repository(&self) -> Result<Self::SchoolRepository<'_>, ContextError>;
}

#[allow(async_fn_in_trait)]
pub trait TeaqlUserContextExt {
    async fn transaction_data<F, Fut>(&self, f: F) -> Result<(), DataServiceError<<crate::runtime::DataServiceExecutor as teaql_data_service::DataServiceExecutor>::Error>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), DataServiceError<<crate::runtime::DataServiceExecutor as teaql_data_service::DataServiceExecutor>::Error>>>;
}

impl TeaqlUserContextExt for teaql_runtime::UserContext {
    async fn transaction_data<F, Fut>(&self, f: F) -> Result<(), DataServiceError<<crate::runtime::DataServiceExecutor as teaql_data_service::DataServiceExecutor>::Error>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), DataServiceError<<crate::runtime::DataServiceExecutor as teaql_data_service::DataServiceExecutor>::Error>>>,
    {
        let executor = self
            .require_resource::<crate::runtime::DataServiceExecutor>()
            .map_err(|err| {
                DataServiceError::Runtime(RuntimeError::Graph(format!(
                    "cannot start transaction without executor: {err}"
                )))
            })?;
        let root = self.entity_runtime_state();

        let tx = teaql_data_service::TransactionExecutor::begin(&*executor)
            .await
            .map_err(DataServiceError::Executor)?;
        root.push_change_set();

        let result = f().await;
        match result {
            Ok(()) => {
                root.pop_change_set();
                teaql_data_service::Transaction::commit(tx)
                    .await
                    .map_err(DataServiceError::Executor)?;
                Ok(())
            }
            Err(err) => {
                root.pop_change_set();
                teaql_data_service::Transaction::rollback(tx)
                    .await
                    .map_err(DataServiceError::Executor)?;
                Err(err)
            }
        }
    }
}

impl TeaqlRuntime for teaql_runtime::UserContext {
    fn user_context(&self) -> &UserContext {
        self
    }

    async fn fetch_facet_smart_list(
        &self,
        entity: &str,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
        trace_context: Vec<teaql_core::TraceNode>,
    ) -> Result<SmartList<CompactRow>, RuntimeError> {
        self.entity_data_service::<crate::runtime::DataServiceExecutor>(entity)
            .map_err(|err| RuntimeError::Graph(err.to_string()))?
            .with_trace_context(trace_context)
            .fetch_smart_list_with_relation_aggregates(query, relation_aggregates)
            .await
            .map_err(|err| RuntimeError::Graph(err.to_string()))
    }
}

impl TeaqlRepositoryProvider for teaql_runtime::UserContext {
    type PlatformRepository<'a>
        = teaql_runtime::EntityDataService<'a, crate::runtime::DataServiceExecutor>
    where
        Self: 'a;

    fn platform_repository(&self) -> Result<Self::PlatformRepository<'_>, ContextError> {
        self.entity_data_service::<crate::runtime::DataServiceExecutor>("Platform")
    }

    type SchoolTypeRepository<'a>
        = teaql_runtime::EntityDataService<'a, crate::runtime::DataServiceExecutor>
    where
        Self: 'a;

    fn school_type_repository(&self) -> Result<Self::SchoolTypeRepository<'_>, ContextError> {
        self.entity_data_service::<crate::runtime::DataServiceExecutor>("SchoolType")
    }

    type SchoolRepository<'a>
        = teaql_runtime::EntityDataService<'a, crate::runtime::DataServiceExecutor>
    where
        Self: 'a;

    fn school_repository(&self) -> Result<Self::SchoolRepository<'_>, ContextError> {
        self.entity_data_service::<crate::runtime::DataServiceExecutor>("School")
    }
}

pub(crate) async fn execute_facets<C>(
    context: &C,
    outer_query: &SelectQuery,
    options: &QueryOptions,
) -> Result<BTreeMap<String, SmartList<CompactRow>>, RuntimeError>
where
    C: TeaqlRuntime + ?Sized,
{
    let mut facets = BTreeMap::new();
    for facet in &options.facets {
        let mut selection = facet.query.clone();
        merge_outer_filter_into_facet_aggregates(&mut selection, outer_query);
        if !facet.include_all_facets {
            selection = restrict_facet_to_outer_query(
                context,
                selection,
                outer_query,
                &facet.relation_name,
            )?;
        }
        let relation_aggregates = runtime_relation_aggregates(&selection.query_options);
        let query = apply_runtime_metadata(
            selection.query,
            &selection.query_options,
            &selection.child_enhancements,
        );
        let entity = query.entity.clone();
        let mut chain = outer_query.trace_chain.clone();
        chain.push(teaql_core::TraceNode::new(
            query.entity.clone(),
            None,
            facet.facet_name.clone(),
        ));

        let query =
            PurposedSelectQuery::new(query, format!("Calculate facet {}", facet.facet_name));
        let facet_rows = context
            .fetch_facet_smart_list(&entity, &query, &relation_aggregates, chain)
            .await?;
        facets.insert(facet.facet_name.clone(), facet_rows);
    }
    Ok(facets)
}

pub(crate) fn restrict_facet_to_outer_query<C>(
    context: &C,
    mut selection: QuerySelection,
    outer_query: &SelectQuery,
    relation_name: &str,
) -> Result<QuerySelection, RuntimeError>
where
    C: TeaqlRuntime + ?Sized,
{
    let descriptor = context
        .user_context()
        .entity(&outer_query.entity)
        .cloned()
        .ok_or_else(|| RuntimeError::Graph(format!("missing entity: {}", outer_query.entity)))?;
    let relation = descriptor
        .relation_by_name(relation_name)
        .cloned()
        .ok_or_else(|| RuntimeError::MissingRelation {
            entity: outer_query.entity.clone(),
            relation: relation_name.to_owned(),
        })?;
    let mut subquery = outer_query.clone();
    subquery.projection.clear();
    subquery.expr_projection.clear();
    subquery.order_by.clear();
    subquery.slice = None;
    subquery.aggregates.clear();
    subquery.group_by.clear();
    subquery.relations.clear();
    selection.query = selection.query.and_filter(Expr::in_subquery(
        relation.foreign_key,
        descriptor,
        subquery,
        relation.local_key,
    ));
    Ok(selection)
}
