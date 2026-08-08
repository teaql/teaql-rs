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
    ) -> Result<Vec<teaql_data_service::StreamChunk>, DataServiceError<Self::Error>>;
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
    ) -> Result<Vec<teaql_data_service::StreamChunk>, DataServiceError<Self::Error>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UserContext;
    use teaql_core::request::{FacetRequest, QueryOptions, QuerySelection};
    use teaql_core::{EntityDescriptor, SelectQuery};

    #[test]
    fn test_purposed_query() {
        let q = PurposedQuery::new(42, "test purpose");
        assert_eq!(q.inner, 42);
        assert_eq!(q.purpose, "test purpose");
    }

    #[test]
    fn test_purposed_select_query() {
        let sq = SelectQuery::new("TestEntity".to_string());
        let psq = PurposedSelectQuery::new(sq.clone(), "test purpose");
        assert_eq!(psq.as_query().entity, "TestEntity");

        let extracted = psq.into_query();
        assert_eq!(extracted.entity, "TestEntity");
        assert_eq!(extracted.trace_chain.len(), 1);
        assert_eq!(extracted.trace_chain[0].entity_type, "TestEntity");
        assert_eq!(extracted.trace_chain[0].comment, "test purpose");
    }

    #[test]
    #[should_panic(expected = "query purpose must not be empty")]
    fn test_purposed_select_query_empty_purpose() {
        let sq = SelectQuery::new("TestEntity".to_string());
        PurposedSelectQuery::new(sq, "   ");
    }

    struct DummyRuntime {
        ctx: UserContext,
    }

    impl TeaqlRuntime for DummyRuntime {
        fn user_context(&self) -> &UserContext {
            &self.ctx
        }

        fn fetch_facet_smart_list(
            &self,
            _entity: &str,
            _query: &PurposedSelectQuery,
            _relation_aggregates: &[RuntimeRelationAggregate],
            _trace_context: Vec<TraceNode>,
        ) -> impl std::future::Future<Output = Result<SmartList<Record>, RuntimeError>> + Send
        {
            std::future::ready(Ok(SmartList::default()))
        }
    }

    #[test]
    fn test_restrict_facet_to_outer_query_missing_entity() {
        let ctx = DummyRuntime {
            ctx: UserContext::new(),
        };
        let outer_query = SelectQuery::new("MissingEntity".to_string());
        let selection = QuerySelection::new(SelectQuery::new("FacetEntity".to_string()));

        let result = restrict_facet_to_outer_query(&ctx, selection, &outer_query, "some_relation");
        assert!(result.is_err());
        if let Err(crate::RuntimeError::Graph(msg)) = result {
            assert_eq!(msg, "missing entity: MissingEntity");
        } else {
            panic!("Expected Graph error");
        }
    }

    #[tokio::test]
    async fn test_execute_facets_empty() {
        let ctx = DummyRuntime {
            ctx: UserContext::new(),
        };
        let outer_query = SelectQuery::new("TestEntity".to_string());
        let options = QueryOptions::default();

        let result = execute_facets(&ctx, &outer_query, &options).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_execute_facets_missing_entity() {
        let ctx = DummyRuntime {
            ctx: UserContext::new(),
        };
        let outer_query = SelectQuery::new("MissingEntity".to_string());
        let mut options = QueryOptions::default();

        let selection = QuerySelection::new(SelectQuery::new("FacetEntity".to_string()));
        options.facets.push(FacetRequest::new(
            "my_facet",
            "my_relation",
            selection,
            false,
        ));

        let result = execute_facets(&ctx, &outer_query, &options).await;
        assert!(result.is_err());
        if let Err(crate::RuntimeError::Graph(msg)) = result {
            assert_eq!(msg, "missing entity: MissingEntity");
        } else {
            panic!("Expected Graph error");
        }
    }

    #[test]
    fn test_restrict_facet_to_outer_query_success() {
        use teaql_core::{EntityDescriptor, RelationDescriptor};
        let mut descriptor = EntityDescriptor::new("TestEntity".to_string());
        descriptor.relations.push(RelationDescriptor {
            name: "my_relation".to_string(),
            target_entity: "FacetEntity".to_string(),
            local_key: "id".to_string(),
            foreign_key: "test_entity_id".to_string(),
            many: true,
            attach: false,
            delete_missing: false,
        });

        let metadata = crate::InMemoryMetadataStore::new().with_entity(descriptor);
        let ctx = DummyRuntime {
            ctx: UserContext::new().with_metadata(metadata),
        };

        let outer_query = SelectQuery::new("TestEntity".to_string());
        let selection = QuerySelection::new(SelectQuery::new("FacetEntity".to_string()));

        let result = restrict_facet_to_outer_query(&ctx, selection, &outer_query, "my_relation");
        assert!(result.is_ok());
        let new_selection = result.unwrap();
        assert!(new_selection.query.filter.is_some());
    }

    #[tokio::test]
    async fn test_execute_facets_include_all() {
        use teaql_core::{EntityDescriptor, RelationDescriptor};
        let mut descriptor = EntityDescriptor::new("TestEntity".to_string());
        descriptor.relations.push(RelationDescriptor {
            name: "my_relation".to_string(),
            target_entity: "FacetEntity".to_string(),
            local_key: "id".to_string(),
            foreign_key: "test_entity_id".to_string(),
            many: true,
            attach: false,
            delete_missing: false,
        });

        let metadata = crate::InMemoryMetadataStore::new().with_entity(descriptor);
        let ctx = DummyRuntime {
            ctx: UserContext::new().with_metadata(metadata),
        };

        let outer_query = SelectQuery::new("TestEntity".to_string());
        let mut options = QueryOptions::default();

        options.facets.push(FacetRequest::new(
            "my_facet_true",
            "my_relation",
            QuerySelection::new(SelectQuery::new("FacetEntity".to_string())),
            true,
        ));

        options.facets.push(FacetRequest::new(
            "my_facet_false",
            "my_relation",
            QuerySelection::new(SelectQuery::new("FacetEntity".to_string())),
            false,
        ));

        let result = execute_facets(&ctx, &outer_query, &options).await;
        assert!(result.is_ok());
        let facets = result.unwrap();
        assert!(facets.contains_key("my_facet_true"));
        assert!(facets.contains_key("my_facet_false"));
    }

    #[derive(Debug, PartialEq, Clone)]
    struct DummyEntity {
        pub id: String,
    }

    impl teaql_core::TeaqlEntity for DummyEntity {
        const ENTITY_NAME: &'static str = "TestEntity";
        fn entity_descriptor() -> teaql_core::EntityDescriptor {
            teaql_core::EntityDescriptor::new("TestEntity")
        }
    }

    impl teaql_core::Entity for DummyEntity {
        fn from_record(_record: teaql_core::Record) -> Result<Self, teaql_core::EntityError> {
            Ok(Self { id: "".into() })
        }
        fn into_record(self) -> teaql_core::Record {
            teaql_core::Record::new()
        }
        fn dirty_fields(&self) -> Option<std::collections::BTreeSet<String>> {
            None
        }
        fn original_values(&self) -> Option<teaql_core::Record> {
            None
        }
        fn is_marked_as_delete(&self) -> bool {
            false
        }
        fn get_comment(&self) -> Option<String> {
            None
        }
    }

    #[derive(Debug)]
    struct DummyError;
    impl std::fmt::Display for DummyError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "error")
        }
    }
    impl std::error::Error for DummyError {}

    struct DummyExecutor;

    impl teaql_data_service::DataServiceExecutor for DummyExecutor {
        type Error = DummyError;
        fn capabilities(&self) -> teaql_data_service::DataServiceCapabilities {
            teaql_data_service::DataServiceCapabilities::default()
        }
    }

    impl teaql_data_service::QueryExecutor for DummyExecutor {
        async fn query(
            &self,
            _r: teaql_data_service::QueryRequest,
        ) -> Result<teaql_data_service::QueryResult, Self::Error> {
            Err(DummyError)
        }
    }

    impl teaql_data_service::MutationExecutor for DummyExecutor {
        async fn mutate(
            &self,
            _r: teaql_data_service::MutationRequest,
        ) -> Result<teaql_data_service::MutationResult, Self::Error> {
            Err(DummyError)
        }
    }

    impl teaql_data_service::StreamQueryExecutor for DummyExecutor {
        async fn query_stream(
            &self,
            _r: teaql_data_service::QueryRequest,
            _s: usize,
        ) -> Result<Vec<teaql_data_service::StreamChunk>, Self::Error> {
            Err(DummyError)
        }
    }

    #[tokio::test]
    async fn test_teaql_record_and_entity_data_service() {
        let mut ctx = UserContext::new()
            .with_entity_registry(crate::InMemoryEntityRegistry::new().with_entity("TestEntity"));

        let metadata = crate::InMemoryMetadataStore::new().with_entity(<DummyEntity as teaql_core::TeaqlEntity>::entity_descriptor());
        ctx.set_metadata(metadata);

        let executor = DummyExecutor;
        ctx.register_executor(executor);

        let eds = ctx
            .entity_data_service::<DummyExecutor>("TestEntity")
            .unwrap();

        let query = PurposedSelectQuery::new(SelectQuery::new("TestEntity".to_string()), "test");
        let _ = TeaqlRecordDataService::fetch_all(&eds, &query).await;
        let _ = TeaqlRecordDataService::fetch_smart_list(&eds, &query).await;
        let _ =
            TeaqlRecordDataService::fetch_smart_list_with_relation_aggregates(&eds, &query, &[])
                .await;
        let _ = TeaqlRecordDataService::fetch_stream(&eds, &query).await;

        let _ = TeaqlEntityDataService::fetch_enhanced_entities::<DummyEntity>(&eds, &query).await;
        let _ = TeaqlEntityDataService::fetch_enhanced_entities_with_relation_aggregates::<
            DummyEntity,
        >(&eds, &query, &[])
        .await;
    }
}
