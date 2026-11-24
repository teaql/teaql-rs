
#![allow(unused_imports)]
#![allow(async_fn_in_trait)]
use std::{collections::BTreeMap, future::Future, marker::PhantomData};

use serde_json::Value as JsonValue;
use teaql_core::{
    BinaryOp, CompactRow, Expr,
    RelationAggregate as RuntimeRelationAggregate, SelectQuery, SmartList,
};
use teaql_runtime::{ContextError, GraphNode, EntityDataServiceBehavior, DataServiceError, PurposedSelectQuery, RuntimeError, UserContext};

pub type TeaqlEntityStream<'a, T, E> = std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<T, E>> + 'a>>;

// Re-export query builder types from teaql_core::request
pub use teaql_core::request::{
    COUNT_ALIAS, TYPE_FIELD, TYPE_GROUP_FIELD,
    FieldOperator, DateRange, EntityReference,
    QuerySelection, RelationSelection, RelationFilter, QueryOptions,
    UnsafeRawSqlSegment, RawDynamicProperty, RawProjection,
    RelationAggregate, FacetRequest, ObjectGroupBy,
    apply_relation_selections, apply_runtime_metadata,
    field_operator_expr, field_operator_column_expr,
    required_value, required_text,
    remove_default_live_filter, remove_filter_expr,
    dynamic_json_value_to_teaql_value, dynamic_json_values,
    dynamic_json_operator, dynamic_json_filter_expr,
    dynamic_json_u64_field,
    runtime_relation_aggregates,
    merge_outer_filter_into_facet_aggregates, attach_facets,
};


pub trait TeaqlQueryRepository {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn fetch_all(&self, query: &PurposedSelectQuery) -> Result<Vec<CompactRow>, DataServiceError<Self::Error>>;

    async fn fetch_smart_list(&self, query: &PurposedSelectQuery) -> Result<SmartList<CompactRow>, DataServiceError<Self::Error>>;

    async fn fetch_smart_list_with_relation_aggregates(
        &self,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
    ) -> Result<SmartList<CompactRow>, DataServiceError<Self::Error>>;

    async fn fetch_stream<'a>(&'a self, query: &PurposedSelectQuery) -> Result<teaql_data_service::QueryStream<'a, DataServiceError<Self::Error>>, DataServiceError<Self::Error>>;
}

pub trait TeaqlEntityRepository: TeaqlQueryRepository {
    async fn fetch_enhanced_entities<T>(&self, query: &PurposedSelectQuery) -> Result<SmartList<T>, DataServiceError<Self::Error>>
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
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + teaql_data_service::StreamQueryExecutor + Send + Sync,
{
    type Error = E::Error;

    async fn fetch_all(&self, query: &PurposedSelectQuery) -> Result<Vec<CompactRow>, DataServiceError<Self::Error>> {
        teaql_runtime::EntityDataService::fetch_all(self, query).await
    }

    async fn fetch_smart_list(&self, query: &PurposedSelectQuery) -> Result<SmartList<CompactRow>, DataServiceError<Self::Error>> {
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
        ).await
    }

    async fn fetch_stream<'b>(&'b self, query: &PurposedSelectQuery) -> Result<teaql_data_service::QueryStream<'b, DataServiceError<Self::Error>>, DataServiceError<Self::Error>> {
        teaql_runtime::EntityDataService::fetch_stream(self, query).await
    }
}

impl<'a, E> TeaqlEntityRepository for teaql_runtime::EntityDataService<'a, E>
where
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + teaql_data_service::StreamQueryExecutor + Send + Sync,
{
    async fn fetch_enhanced_entities<T>(&self, query: &PurposedSelectQuery) -> Result<SmartList<T>, DataServiceError<Self::Error>>
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
        ).await
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
        ).await
    }

}

pub type TeaqlDataServiceError<R> = DataServiceError<<R as TeaqlQueryRepository>::Error>;

pub(crate) fn authorize_query(mut query: SelectQuery) -> Result<PurposedSelectQuery, RuntimeError> {
    if query.comment.as_deref().map(str::trim).filter(|value| !value.is_empty()).is_none() {
        return Err(RuntimeError::Graph(
            "generated query reached the repository without .comment(...)".to_owned()
        ));
    }
    let purpose_index = query
        .trace_chain
        .iter()
        .rposition(|node| node.kind == teaql_core::TraceKind::Purpose)
        .ok_or_else(|| RuntimeError::Graph(
            "generated query reached the repository without .purpose(...)".to_owned()
        ))?;
    let purpose = query.trace_chain.remove(purpose_index).comment;
    if purpose.trim().is_empty() {
        return Err(RuntimeError::Graph(
            "generated query reached the repository without .purpose(...)".to_owned()
        ));
    }
    Ok(PurposedSelectQuery::new(query, purpose))
}

pub trait TeaqlRuntime: Sync {
    fn user_context(&self) -> &UserContext;

    fn save_audited_entity<'a, T>(
        &'a self,
        audited: teaql_core::Audited<T>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, RuntimeError>> + Send + 'a>>
    where
        T: teaql_runtime::LedgerEntity + Send + 'static;

    fn fetch_entity_smart_list<'a, T>(
        &'a self,
        entity: &'static str,
        query: PurposedSelectQuery,
        relation_aggregates: Vec<RuntimeRelationAggregate>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SmartList<T>, RuntimeError>> + Send + 'a>>
    where
        T: teaql_core::Entity + Send + 'a;

    fn fetch_compact_smart_list<'a>(
        &'a self,
        entity: &'static str,
        query: &'a PurposedSelectQuery,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SmartList<CompactRow>, RuntimeError>> + Send + 'a>>;

    fn fetch_compact_rows<'a>(
        &'a self,
        entity: &'static str,
        query: &'a PurposedSelectQuery,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<CompactRow>, RuntimeError>> + Send + 'a>>;

    fn fetch_entity_stream<'a, T>(
        &'a self,
        entity: &'static str,
        query: PurposedSelectQuery,
    ) -> TeaqlEntityStream<'a, T, RuntimeError>
    where
        T: teaql_core::Entity + Send + 'a;

    fn fetch_facet_smart_list<'a>(
        &'a self,
        entity: &'a str,
        query: &'a PurposedSelectQuery,
        relation_aggregates: &'a [RuntimeRelationAggregate],
        trace_context: Vec<teaql_core::TraceNode>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SmartList<CompactRow>, RuntimeError>> + Send + 'a>>;
}

/// Internal trait for repository access. Application code should not use this trait directly.
#[doc(hidden)]
pub trait AuditedSave<'a, C>
where
    C: TeaqlRuntime + ?Sized + 'a,
{
    type Error;
    type Entity;
    fn save(self, context: &'a C) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Entity, Self::Error>> + Send + '_>>;
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

impl TeaqlRuntime for teaql_runtime::UserContext {
    fn user_context(&self) -> &UserContext {
        self
    }

    fn save_audited_entity<'a, T>(
        &'a self,
        audited: teaql_core::Audited<T>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, RuntimeError>> + Send + 'a>>
    where
        T: teaql_runtime::LedgerEntity + Send + 'static,
    {
        Box::pin(teaql_runtime::save_audited_ledger_entity(audited, self))
    }

    fn fetch_entity_smart_list<'a, T>(
        &'a self,
        entity: &'static str,
        query: PurposedSelectQuery,
        relation_aggregates: Vec<RuntimeRelationAggregate>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SmartList<T>, RuntimeError>> + Send + 'a>>
    where
        T: teaql_core::Entity + Send + 'a,
    {
        Box::pin(async move {
            self.entity_data_service::<crate::runtime::DataServiceExecutor>(entity)
                .map_err(|error| RuntimeError::Graph(error.to_string()))?
                .fetch_enhanced_entities_with_relation_aggregates_owned(
                    query,
                    &relation_aggregates,
                )
                .await
                .map_err(|error| RuntimeError::Graph(error.to_string()))
        })
    }

    fn fetch_compact_smart_list<'a>(
        &'a self,
        entity: &'static str,
        query: &'a PurposedSelectQuery,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SmartList<CompactRow>, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            self.entity_data_service::<crate::runtime::DataServiceExecutor>(entity)
                .map_err(|error| RuntimeError::Graph(error.to_string()))?
                .fetch_smart_list(query)
                .await
                .map_err(|error| RuntimeError::Graph(error.to_string()))
        })
    }

    fn fetch_compact_rows<'a>(
        &'a self,
        entity: &'static str,
        query: &'a PurposedSelectQuery,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<CompactRow>, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            self.entity_data_service::<crate::runtime::DataServiceExecutor>(entity)
                .map_err(|error| RuntimeError::Graph(error.to_string()))?
                .fetch_all(query)
                .await
                .map_err(|error| RuntimeError::Graph(error.to_string()))
        })
    }

    fn fetch_entity_stream<'a, T>(
        &'a self,
        entity: &'static str,
        query: PurposedSelectQuery,
    ) -> TeaqlEntityStream<'a, T, RuntimeError>
    where
        T: teaql_core::Entity + Send + 'a,
    {
        Box::pin(async_stream::try_stream! {
            use futures_util::StreamExt;
            let repository = self.entity_data_service::<crate::runtime::DataServiceExecutor>(entity)
                .map_err(|error| RuntimeError::Graph(error.to_string()))?;
            let mut chunks = repository.fetch_stream(&query).await
                .map_err(|error| RuntimeError::Graph(error.to_string()))?;
            while let Some(chunk) = chunks.next().await {
                for row in chunk.map_err(|error| RuntimeError::Graph(error.to_string()))?.rows {
                    yield T::from_compact_row(row).map_err(|error| RuntimeError::Graph(error.to_string()))?;
                }
            }
        })
    }

    fn fetch_facet_smart_list<'a>(
        &'a self,
        entity: &'a str,
        query: &'a PurposedSelectQuery,
        relation_aggregates: &'a [RuntimeRelationAggregate],
        trace_context: Vec<teaql_core::TraceNode>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SmartList<CompactRow>, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            self.entity_data_service::<crate::runtime::DataServiceExecutor>(entity)
                .map_err(|err| RuntimeError::Graph(err.to_string()))?
                .with_trace_context(trace_context)
                .fetch_smart_list_with_relation_aggregates(query, relation_aggregates)
                .await
                .map_err(|err| RuntimeError::Graph(err.to_string()))
        })
    }
}

impl TeaqlRepositoryProvider for teaql_runtime::UserContext {
    type PlatformRepository<'a> = teaql_runtime::EntityDataService<'a, crate::runtime::DataServiceExecutor>
    where
        Self: 'a;

    fn platform_repository(&self) -> Result<Self::PlatformRepository<'_>, ContextError> {
        self.entity_data_service::<crate::runtime::DataServiceExecutor>("Platform")
    }

    type SchoolTypeRepository<'a> = teaql_runtime::EntityDataService<'a, crate::runtime::DataServiceExecutor>
    where
        Self: 'a;

    fn school_type_repository(&self) -> Result<Self::SchoolTypeRepository<'_>, ContextError> {
        self.entity_data_service::<crate::runtime::DataServiceExecutor>("SchoolType")
    }

    type SchoolRepository<'a> = teaql_runtime::EntityDataService<'a, crate::runtime::DataServiceExecutor>
    where
        Self: 'a;

    fn school_repository(&self) -> Result<Self::SchoolRepository<'_>, ContextError> {
        self.entity_data_service::<crate::runtime::DataServiceExecutor>("School")
    }
}

impl<'transaction> TeaqlRuntime
    for teaql_runtime::TransactionScope<'transaction, crate::runtime::DataServiceExecutor>
where
    for<'scope> <crate::runtime::DataServiceExecutor as teaql_data_service::TransactionExecutor>::Tx<'scope>:
        teaql_data_service::QueryExecutor
        + teaql_data_service::MutationExecutor
        + teaql_data_service::StreamQueryExecutor
        + Send
        + Sync,
{
    fn user_context(&self) -> &UserContext {
        self.context()
    }

    fn save_audited_entity<'a, T>(
        &'a self,
        audited: teaql_core::Audited<T>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, RuntimeError>> + Send + 'a>>
    where
        T: teaql_runtime::LedgerEntity + Send + 'static,
    {
        Box::pin(self.save_audited(audited))
    }

    fn fetch_entity_smart_list<'a, T>(
        &'a self,
        entity: &'static str,
        query: PurposedSelectQuery,
        relation_aggregates: Vec<RuntimeRelationAggregate>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SmartList<T>, RuntimeError>> + Send + 'a>>
    where
        T: teaql_core::Entity + Send + 'a,
    {
        Box::pin(async move {
            self.entity_data_service(entity)
                .map_err(|error| RuntimeError::Graph(error.to_string()))?
                .fetch_enhanced_entities_with_relation_aggregates_owned(
                    query,
                    &relation_aggregates,
                )
                .await
                .map_err(|error| RuntimeError::Graph(error.to_string()))
        })
    }

    fn fetch_compact_smart_list<'a>(
        &'a self,
        entity: &'static str,
        query: &'a PurposedSelectQuery,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SmartList<CompactRow>, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            self.entity_data_service(entity)
                .map_err(|error| RuntimeError::Graph(error.to_string()))?
                .fetch_smart_list(query)
                .await
                .map_err(|error| RuntimeError::Graph(error.to_string()))
        })
    }

    fn fetch_compact_rows<'a>(
        &'a self,
        entity: &'static str,
        query: &'a PurposedSelectQuery,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<CompactRow>, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            self.entity_data_service(entity)
                .map_err(|error| RuntimeError::Graph(error.to_string()))?
                .fetch_all(query)
                .await
                .map_err(|error| RuntimeError::Graph(error.to_string()))
        })
    }

    fn fetch_entity_stream<'a, T>(
        &'a self,
        entity: &'static str,
        query: PurposedSelectQuery,
    ) -> TeaqlEntityStream<'a, T, RuntimeError>
    where
        T: teaql_core::Entity + Send + 'a,
    {
        Box::pin(async_stream::try_stream! {
            use futures_util::StreamExt;
            let repository = self.entity_data_service(entity)
                .map_err(|error| RuntimeError::Graph(error.to_string()))?;
            let mut chunks = repository.fetch_stream(&query).await
                .map_err(|error| RuntimeError::Graph(error.to_string()))?;
            while let Some(chunk) = chunks.next().await {
                for row in chunk.map_err(|error| RuntimeError::Graph(error.to_string()))?.rows {
                    yield T::from_compact_row(row).map_err(|error| RuntimeError::Graph(error.to_string()))?;
                }
            }
        })
    }

    fn fetch_facet_smart_list<'a>(
        &'a self,
        entity: &'a str,
        query: &'a PurposedSelectQuery,
        relation_aggregates: &'a [RuntimeRelationAggregate],
        trace_context: Vec<teaql_core::TraceNode>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SmartList<CompactRow>, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            self.entity_data_service(entity)
                .map_err(|err| RuntimeError::Graph(err.to_string()))?
                .with_trace_context(trace_context)
                .fetch_smart_list_with_relation_aggregates(query, relation_aggregates)
                .await
                .map_err(|err| RuntimeError::Graph(err.to_string()))
        })
    }
}

// The generated module binds its executor; the runtime owns Facet semantics.
struct FacetRuntime<'a, C: ?Sized>(&'a C);

impl<C: TeaqlRuntime + ?Sized> teaql_runtime::generated_support::TeaqlRuntime
    for FacetRuntime<'_, C>
{
    fn user_context(&self) -> &UserContext {
        self.0.user_context()
    }

    async fn fetch_facet_smart_list(
        &self,
        entity: &str,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
        trace_context: Vec<teaql_core::TraceNode>,
    ) -> Result<SmartList<CompactRow>, RuntimeError> {
        self.0.fetch_facet_smart_list(entity, query, relation_aggregates, trace_context).await
    }
}

pub(crate) async fn execute_facets<C: TeaqlRuntime + ?Sized>(
    context: &C,
    outer_query: &SelectQuery,
    options: &QueryOptions,
) -> Result<BTreeMap<String, SmartList<CompactRow>>, RuntimeError> {
    teaql_runtime::generated_support::execute_facets(&FacetRuntime(context), outer_query, options).await
}
