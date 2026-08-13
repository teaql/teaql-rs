#![allow(unused_imports)]
#![allow(async_fn_in_trait)]

use crate::{DataServiceError, GraphNode, RuntimeError, UserContext};
use std::collections::BTreeMap;
use teaql_core::request::{
    QueryOptions, QuerySelection, apply_runtime_metadata, merge_outer_filter_into_facet_aggregates,
    runtime_relation_aggregates,
};
use teaql_core::{
    Expr, Record, RelationAggregate as RuntimeRelationAggregate, SelectQuery, SmartList, TraceNode,
};

pub trait TeaqlRecordDataService {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn fetch_all(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<Self::Error>>;

    async fn fetch_smart_list(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<SmartList<Record>, DataServiceError<Self::Error>>;

    async fn fetch_smart_list_with_relation_aggregates(
        &self,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
    ) -> Result<SmartList<Record>, DataServiceError<Self::Error>>;

    async fn fetch_stream(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures_core::Stream<
                        Item = Result<
                            teaql_data_service::StreamChunk,
                            DataServiceError<Self::Error>,
                        >,
                    > + '_,
            >,
        >,
        DataServiceError<Self::Error>,
    >;
}

pub trait TeaqlEntityDataService: TeaqlRecordDataService {
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
}

impl<'a, E> TeaqlRecordDataService for crate::EntityDataService<'a, E>
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
    ) -> Result<Vec<Record>, DataServiceError<Self::Error>> {
        crate::EntityDataService::fetch_all(self, query).await
    }

    async fn fetch_smart_list(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<SmartList<Record>, DataServiceError<Self::Error>> {
        crate::EntityDataService::fetch_smart_list(self, query).await
    }

    async fn fetch_smart_list_with_relation_aggregates(
        &self,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
    ) -> Result<SmartList<Record>, DataServiceError<Self::Error>> {
        crate::EntityDataService::fetch_smart_list_with_relation_aggregates(
            self,
            query,
            relation_aggregates,
        )
        .await
    }

    async fn fetch_stream(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures_core::Stream<
                        Item = Result<
                            teaql_data_service::StreamChunk,
                            DataServiceError<Self::Error>,
                        >,
                    > + '_,
            >,
        >,
        DataServiceError<Self::Error>,
    > {
        crate::EntityDataService::fetch_stream(self, query).await
    }
}

impl<'a, E> TeaqlEntityDataService for crate::EntityDataService<'a, E>
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
        crate::EntityDataService::fetch_enhanced_entities(self, query).await
    }

    async fn fetch_enhanced_entities_with_relation_aggregates<T>(
        &self,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<Self::Error>>
    where
        T: teaql_core::Entity,
    {
        crate::EntityDataService::fetch_enhanced_entities_with_relation_aggregates(
            self,
            query,
            relation_aggregates,
        )
        .await
    }
}

pub type TeaqlDataServiceError<R> = DataServiceError<<R as TeaqlRecordDataService>::Error>;

pub trait TeaqlRuntime {
    fn user_context(&self) -> &UserContext;

    fn fetch_facet_smart_list(
        &self,
        entity: &str,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
        trace_context: Vec<TraceNode>,
    ) -> impl std::future::Future<Output = Result<SmartList<Record>, RuntimeError>> + Send;
}

/// Internal trait for audited save access. Application code should not use this trait directly.
#[doc(hidden)]
pub trait AuditedSave<'a, C>
where
    C: TeaqlRuntime + ?Sized + 'a,
{
    type Error;
    fn save(
        self,
        ctx: &'a C,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GraphNode, Self::Error>> + '_>>;
}

pub struct PurposedQuery<T> {
    pub inner: T,
    pub purpose: String,
}

impl<T> PurposedQuery<T> {
    pub fn new(inner: T, purpose: impl Into<String>) -> Self {
        Self {
            inner,
            purpose: purpose.into(),
        }
    }
}

/// A low-level select query carrying an explicit, non-empty execution purpose.
///
/// Generated request builders construct this type after `.purpose(...)` unlocks
/// their terminal methods. Runtime execution APIs accept this wrapper rather
/// than a bare [`SelectQuery`], so infrastructure callers must also declare
/// intent explicitly.
#[derive(Debug, Clone)]
pub struct PurposedSelectQuery {
    query: SelectQuery,
}

impl PurposedSelectQuery {
    pub fn new(mut query: SelectQuery, purpose: impl Into<String>) -> Self {
        let purpose = purpose.into();
        assert!(
            !purpose.trim().is_empty(),
            "query purpose must not be empty"
        );
        query.trace_chain.push(TraceNode {
            entity_type: query.entity.clone(),
            entity_id: None,
            comment: purpose,
        });
        Self { query }
    }

    pub fn as_query(&self) -> &SelectQuery {
        &self.query
    }

    pub fn into_query(self) -> SelectQuery {
        self.query
    }
}

pub async fn execute_facets<C>(
    ctx: &C,
    outer_query: &SelectQuery,
    options: &QueryOptions,
) -> Result<BTreeMap<String, SmartList<Record>>, RuntimeError>
where
    C: TeaqlRuntime + ?Sized,
{
    let mut facets = BTreeMap::new();
    for facet in &options.facets {
        let mut selection = facet.query.clone();
        merge_outer_filter_into_facet_aggregates(&mut selection, outer_query);
        if !facet.include_all_facets {
            selection =
                restrict_facet_to_outer_query(ctx, selection, outer_query, &facet.relation_name)?;
        }
        let relation_aggregates = runtime_relation_aggregates(&selection.query_options);
        let query = apply_runtime_metadata(
            selection.query,
            &selection.query_options,
            &selection.child_enhancements,
        );
        let entity = query.entity.clone();
        let mut chain = outer_query.trace_chain.clone();
        chain.push(TraceNode {
            entity_type: query.entity.clone(),
            entity_id: None,
            comment: facet.facet_name.clone(),
        });

        let query =
            PurposedSelectQuery::new(query, format!("Calculate facet {}", facet.facet_name));
        let facet_rows = ctx
            .fetch_facet_smart_list(&entity, &query, &relation_aggregates, chain)
            .await?;
        facets.insert(facet.facet_name.clone(), facet_rows);
    }
    Ok(facets)
}

pub fn restrict_facet_to_outer_query<C>(
    ctx: &C,
    mut selection: QuerySelection,
    outer_query: &SelectQuery,
    relation_name: &str,
) -> Result<QuerySelection, RuntimeError>
where
    C: TeaqlRuntime + ?Sized,
{
    let descriptor = ctx
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
