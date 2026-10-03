#![allow(unused_imports)]
#![allow(async_fn_in_trait)]

use crate::{DataServiceError, GraphNode, RuntimeError, UserContext};
use std::collections::BTreeMap;
use teaql_core::request::{
    QueryOptions, QuerySelection, apply_runtime_metadata, merge_outer_filter_into_facet_aggregates,
    runtime_relation_aggregates,
};
use teaql_core::{
    CompactRow, Expr, RelationAggregate as RuntimeRelationAggregate, SelectQuery, SmartList,
    TraceNode,
};

pub trait TeaqlQueryDataService {
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

pub trait TeaqlEntityDataService: TeaqlQueryDataService {
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

impl<'a, E> TeaqlQueryDataService for crate::EntityDataService<'a, E>
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
        crate::EntityDataService::fetch_all(self, query).await
    }

    async fn fetch_smart_list(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<SmartList<CompactRow>, DataServiceError<Self::Error>> {
        crate::EntityDataService::fetch_smart_list(self, query).await
    }

    async fn fetch_smart_list_with_relation_aggregates(
        &self,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
    ) -> Result<SmartList<CompactRow>, DataServiceError<Self::Error>> {
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

pub type TeaqlDataServiceError<R> = DataServiceError<<R as TeaqlQueryDataService>::Error>;

pub trait TeaqlRuntime {
    fn user_context(&self) -> &UserContext;

    fn fetch_facet_smart_list(
        &self,
        entity: &str,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RuntimeRelationAggregate],
        trace_context: Vec<TraceNode>,
    ) -> impl std::future::Future<Output = Result<SmartList<CompactRow>, RuntimeError>> + Send;
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
        context: &'a C,
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
#[derive(Clone)]
pub struct PurposedSelectQuery {
    query: SelectQuery,
    // Local compiler provenance only: never serialized or stored on UserContext.
    diagnostic_source: Option<Box<SelectQuery>>,
}

impl std::fmt::Debug for PurposedSelectQuery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PurposedSelectQuery")
            .field("query", &self.query)
            .field("has_diagnostic_source", &self.diagnostic_source.is_some())
            .finish()
    }
}

impl PurposedSelectQuery {
    pub fn new(mut query: SelectQuery, purpose: impl Into<String>) -> Self {
        let purpose = purpose.into();
        assert!(
            !purpose.trim().is_empty(),
            "query purpose must not be empty"
        );
        query.purpose = Some(purpose.clone());
        query.trace_chain.push(TraceNode {
            kind: teaql_core::TraceKind::Purpose,
            entity_type: query.entity.clone(),
            entity_id: None,
            comment: purpose,
        });
        Self {
            query,
            diagnostic_source: None,
        }
    }

    /// Derive the root count without losing removed expressions' log privacy.
    /// The original query is classified locally; it is never executed for tracing.
    #[doc(hidden)]
    pub fn for_exact_count(mut self, alias: impl Into<String>) -> Self {
        if self.diagnostic_source.is_none() {
            self.diagnostic_source = Some(Box::new(self.query.clone()));
        }
        self.query.projection.clear();
        self.query.expr_projection.clear();
        self.query.order_by.clear();
        self.query.slice = None;
        self.query.relations.clear();
        self.query = self.query.count(alias);
        self
    }

    /// Classify future Facet bindings before the first root SQL statement.
    /// Sources remain diagnostic-only and compose with derived COUNT provenance.
    #[doc(hidden)]
    pub fn with_facet_diagnostics(mut self, options: &QueryOptions) -> Self {
        if options.facets.is_empty() {
            return self;
        }
        let source = self
            .diagnostic_source
            .get_or_insert_with(|| Box::new(self.query.clone()));
        source.child_enhancements.extend(
            options
                .facets
                .iter()
                .map(|facet| facet_diagnostic_source(&facet.query)),
        );
        self
    }

    pub(crate) fn diagnostic_source(&self) -> Option<&SelectQuery> {
        self.diagnostic_source.as_deref()
    }

    pub fn as_query(&self) -> &SelectQuery {
        &self.query
    }

    pub fn into_query(self) -> SelectQuery {
        self.query
    }
}

pub async fn execute_facets<C>(
    context: &C,
    outer_query: &SelectQuery,
    options: &QueryOptions,
) -> Result<BTreeMap<String, SmartList<CompactRow>>, RuntimeError>
where
    C: TeaqlRuntime + Sync + ?Sized,
{
    // Facet materialization can omit the root's bindings (include_all_facets).
    // Keep classification sources local to this invocation, never on Context or
    // on an executed query's child enhancements.
    let mut diagnostic_source = outer_query.clone();
    diagnostic_source.child_enhancements.extend(
        options
            .facets
            .iter()
            .map(|facet| facet_diagnostic_source(&facet.query)),
    );
    execute_facets_with_source(context, outer_query, options, &diagnostic_source).await
}

fn facet_diagnostic_source(selection: &QuerySelection) -> SelectQuery {
    let mut query = selection.clone().into_query();
    query.child_enhancements.extend(
        selection
            .query_options
            .relation_aggregates
            .iter()
            .map(|aggregate| facet_diagnostic_source(&aggregate.query)),
    );
    query.child_enhancements.extend(
        selection
            .query_options
            .facets
            .iter()
            .map(|facet| facet_diagnostic_source(&facet.query)),
    );
    query
}

fn execute_facets_with_source<'a, C>(
    context: &'a C,
    outer_query: &'a SelectQuery,
    options: &'a QueryOptions,
    diagnostic_source: &'a SelectQuery,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<BTreeMap<String, SmartList<CompactRow>>, RuntimeError>,
            > + Send
            + 'a,
    >,
>
where
    C: TeaqlRuntime + Sync + ?Sized,
{
    Box::pin(async move {
        let intent = teaql_core::QueryIntent::from_optional(
            outer_query.comment.as_deref(),
            outer_query.purpose.as_deref(),
        )?;
        let mut facets = BTreeMap::new();
        for facet in &options.facets {
            let descriptor = context
                .user_context()
                .entity(&outer_query.entity)
                .ok_or_else(|| RuntimeError::MissingEntity(outer_query.entity.clone()))?;
            let relation = descriptor
                .relation_by_name(&facet.relation_name)
                .ok_or_else(|| RuntimeError::MissingRelation {
                    entity: outer_query.entity.clone(),
                    relation: facet.relation_name.clone(),
                })?;
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
            let mut query = selection.clone().into_query();
            query.comment = Some(intent.comment().to_owned());
            let entity = query.entity.clone();
            query.trace_chain = outer_query.trace_chain.clone();
            query.trace_chain.push(TraceNode {
                kind: teaql_core::TraceKind::Relation,
                entity_type: relation.name.clone(),
                entity_id: None,
                comment: format!("{}.{}", outer_query.entity, relation.name),
            });

            let mut query = PurposedSelectQuery::new(query, intent.purpose());
            query.diagnostic_source = Some(Box::new(diagnostic_source.clone()));
            // The query already owns the complete ancestry. Passing it again as a
            // repository prefix duplicates the edge in derived aggregate SQL.
            let mut facet_rows = context
                .fetch_facet_smart_list(&entity, &query, &relation_aggregates, Vec::new())
                .await?;
            facet_rows.facets = execute_facets_with_source(
                context,
                query.as_query(),
                &selection.query_options,
                diagnostic_source,
            )
            .await?;
            facets.insert(facet.facet_name.clone(), facet_rows);
        }
        Ok(facets)
    })
}

pub fn restrict_facet_to_outer_query<C>(
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
