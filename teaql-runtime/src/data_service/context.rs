use std::sync::Arc;

use teaql_core::{
    DeleteCommand, Entity, InsertCommand, Record, RecoverCommand, SelectQuery, SmartList,
    UpdateCommand,
};

use crate::{
    ContextError, DataServiceError, GraphMutationPlan, GraphNode, RuntimeError, UserContext,
};

use super::{
    AggregationCacheBackend, ContextDataService, EntityDataService, InMemoryAggregationCache,
    RuntimeDataService, UserContextMetadata, helpers::invalidate_aggregation_cache_namespace,
};

impl UserContext {
    pub(crate) fn data_service_internal<E>(&self) -> Result<ContextDataService<'_, E>, ContextError>
    where
        E: teaql_data_service::QueryExecutor
            + teaql_data_service::MutationExecutor
            + Send
            + Sync
            + 'static,
    {
        if self.metadata.is_none() {
            return Err(ContextError::MissingResource("metadata".to_owned()));
        }

        let executor = self.require_resource::<E>()?;
        Ok(ContextDataService {
            metadata: UserContextMetadata { context: self },
            executor,
        })
    }

    pub fn entity_data_service<E>(
        &self,
        entity: impl Into<String>,
    ) -> Result<EntityDataService<'_, E>, ContextError>
    where
        E: teaql_data_service::QueryExecutor
            + teaql_data_service::MutationExecutor
            + Send
            + Sync
            + 'static,
    {
        let entity = entity.into();
        if !self.has_entity_data_service(&entity) {
            return Err(ContextError::MissingEntityDataService(entity));
        }
        Ok(EntityDataService {
            entity,
            data_service: self.data_service_internal::<E>()?,
            trace_context: Vec::new(),
        })
    }

    /// Register a data-service executor and automatically set up the
    /// type-erased graph saver so that
    /// [`Audited::save`](crate::AuditedSaveExt::save) works.
    pub fn register_executor<E>(&mut self, executor: E)
    where
        E: teaql_data_service::QueryExecutor
            + teaql_data_service::MutationExecutor
            + Send
            + Sync
            + 'static,
    {
        use std::sync::Arc;
        self.insert_resource::<Arc<dyn crate::entity_save::DynGraphSaver>>(Arc::new(
            crate::entity_save::GraphSaverFor::<E>::new(),
        ));
        self.insert_resource(executor);
    }
}

impl<'a, E> ContextDataService<'a, E>
where
    E: teaql_data_service::QueryExecutor
        + teaql_data_service::MutationExecutor
        + Send
        + Sync
        + 'static,
{
    fn data_service(&self) -> RuntimeDataService<'_, UserContextMetadata<'_>, E> {
        RuntimeDataService::new(&self.metadata, self.executor)
    }

    pub(crate) async fn fetch_all(
        &self,
        mut query: SelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        let final_comment = self.resolve_final_comment(&query.trace_chain, query.comment.clone());
        query.comment = final_comment;
        self.data_service().fetch_all(&query).await
    }

    pub(crate) async fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, DataServiceError<E::Error>> {
        self.data_service().fetch_smart_list(query).await
    }

    pub(crate) async fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        self.data_service().fetch_entities(query).await
    }

    pub(crate) async fn fetch_enhanced_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        self.data_service().fetch_enhanced_entities(query).await
    }

    pub(crate) async fn insert(
        &self,
        command: &InsertCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let affected = self.data_service().insert(command).await?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub(crate) async fn update(
        &self,
        command: &UpdateCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let affected = self.data_service().update(command).await?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub(crate) async fn batch_insert(
        &self,
        command: &teaql_core::BatchInsertCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let affected = self.data_service().batch_insert(command).await?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub(crate) async fn batch_update(
        &self,
        command: &teaql_core::BatchUpdateCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let affected = self.data_service().batch_update(command).await?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub(crate) async fn delete(
        &self,
        command: &DeleteCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let affected = self.data_service().delete(command).await?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub(crate) async fn recover(
        &self,
        command: &RecoverCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let affected = self.data_service().recover(command).await?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub(super) fn invalidate_aggregation_cache_for(&self, entity: &str) {
        if let Some(cache) = self
            .metadata
            .context
            .get_resource::<Arc<dyn AggregationCacheBackend>>()
        {
            invalidate_aggregation_cache_namespace(cache.as_ref(), entity);
        }
        if let Some(cache) = self
            .metadata
            .context
            .get_resource::<InMemoryAggregationCache>()
        {
            invalidate_aggregation_cache_namespace(cache, entity);
        }
    }

    pub(crate) fn resolve_final_comment(
        &self,
        trace_chain: &[teaql_core::TraceNode],
        comment: Option<String>,
    ) -> Option<String> {
        let chain_str = (!trace_chain.is_empty()).then(|| {
            trace_chain
                .iter()
                .map(|n| {
                    format!(
                        "{}({}): {}",
                        n.entity_type,
                        n.entity_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "pending".to_owned()),
                        n.comment
                    )
                })
                .collect::<Vec<_>>()
                .join(" -> ")
        });

        let business_comment = chain_str.or(comment);
        let user_id = self
            .metadata
            .context
            .user_identifier()
            .map(|s| s.to_owned());

        match (user_id, business_comment) {
            (Some(user), Some(bus)) if !user.is_empty() && !bus.is_empty() => {
                Some(format!("[{user}] {bus}"))
            }
            (Some(user), _) if !user.is_empty() => Some(format!("[{user}]")),
            (_, Some(bus)) if !bus.is_empty() => Some(bus),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryMetadataStore, UserContext};
    use std::sync::Arc;
    use teaql_core::{
        BatchInsertCommand, BatchUpdateCommand, DeleteCommand, Entity, EntityError, InsertCommand,
        Record, RecoverCommand, SelectQuery, TraceNode, Value,
    };
    use teaql_data_service::{
        DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
        MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest,
        QueryResult,
    };

    #[derive(Clone, Default)]
    struct DummyExecutor;

    #[derive(Debug)]
    struct DummyError;
    impl std::fmt::Display for DummyError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "dummy")
        }
    }
    impl std::error::Error for DummyError {}

    impl DataServiceExecutor for DummyExecutor {
        type Error = DummyError;
        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for DummyExecutor {
        async fn query(&self, _request: QueryRequest) -> Result<QueryResult, Self::Error> {
            Ok(QueryResult {
                rows: vec![Record::new()],
                metadata: ExecutionMetadata {
                    debug_query: None,
                    backend: "dummy".to_string(),
                    operation: DataServiceOperation::Query,
                    started_at: std::time::SystemTime::now(),
                    ended_at: std::time::SystemTime::now(),
                    affected_rows: None,
                    result_count: Some(1),
                    trace_chain: Vec::new(),
                    backend_request_id: None,
                    comment: None,
                },
            })
        }
    }

    impl MutationExecutor for DummyExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            Ok(MutationResult {
                affected_rows: 42,
                generated_values: Record::new(),
                metadata: ExecutionMetadata {
                    debug_query: None,
                    backend: "dummy".to_string(),
                    operation: DataServiceOperation::Insert,
                    started_at: std::time::SystemTime::now(),
                    ended_at: std::time::SystemTime::now(),
                    affected_rows: Some(42),
                    result_count: None,
                    trace_chain: Vec::new(),
                    backend_request_id: None,
                    comment: None,
                },
            })
        }
    }

    #[derive(Clone, Default)]
    struct DummyFailingExecutor;

    impl DataServiceExecutor for DummyFailingExecutor {
        type Error = DummyError;
        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for DummyFailingExecutor {
        async fn query(&self, _request: QueryRequest) -> Result<QueryResult, Self::Error> {
            Err(DummyError)
        }
    }

    impl MutationExecutor for DummyFailingExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            Err(DummyError)
        }
    }

    #[derive(Debug)]
    struct DummyEntity;
    impl teaql_core::TeaqlEntity for DummyEntity {
        const ENTITY_NAME: &'static str = "Dummy";
        fn entity_descriptor() -> teaql_core::EntityDescriptor {
            teaql_core::EntityDescriptor::new("Dummy")
        }
    }
    impl Entity for DummyEntity {
        fn from_record(_: Record) -> Result<Self, EntityError> {
            Ok(Self)
        }
        fn into_record(self) -> Record {
            Record::new()
        }
    }

    #[test]
    fn test_data_service_internal() {
        let mut ctx = UserContext::new();
        // no metadata => err
        assert!(ctx.data_service_internal::<DummyExecutor>().is_err());

        let ctx = UserContext::new().with_metadata(InMemoryMetadataStore::new());
        // no executor => err
        assert!(ctx.data_service_internal::<DummyExecutor>().is_err());

        let mut ctx = UserContext::new().with_metadata(InMemoryMetadataStore::new());
        ctx.register_executor(DummyExecutor);
        // now ok
        assert!(ctx.data_service_internal::<DummyExecutor>().is_ok());
    }

    #[test]
    fn test_entity_data_service() {
        let mut ctx = UserContext::new().with_metadata(InMemoryMetadataStore::new());
        ctx.register_executor(DummyExecutor);

        // Entity not in registry => err
        assert!(ctx.entity_data_service::<DummyExecutor>("Dummy").is_err());

        let schema = teaql_core::EntityDescriptor::new("Dummy");
        let mut ctx =
            UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(schema.clone()));
        ctx.register_executor(DummyExecutor);

        let service = ctx.entity_data_service::<DummyExecutor>("Dummy");
        assert!(service.is_ok());

        // Executor missing but entity exists => err from data_service_internal
        let ctx_no_executor =
            UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(schema.clone()));
        assert!(ctx_no_executor
            .entity_data_service::<DummyExecutor>("Dummy")
            .is_err());
    }

    #[tokio::test]
    async fn test_operations() {
        let schema = teaql_core::EntityDescriptor::new("Dummy");
        let mut ctx =
            UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(schema.clone()));
        ctx.register_executor(DummyExecutor);

        let eds = ctx.entity_data_service::<DummyExecutor>("Dummy").unwrap();
        let query = SelectQuery::new("Dummy");

        // test fetch_all
        let res1 = eds.data_service.fetch_all(query.clone()).await;
        assert!(res1.is_ok());

        // test fetch_smart_list
        let res2 = eds.data_service.fetch_smart_list(&query).await;
        assert!(res2.is_ok());

        // test fetch_entities
        let res3 = eds.data_service.fetch_entities::<DummyEntity>(&query).await;
        assert!(res3.is_ok());

        // test fetch_enhanced_entities
        let res4 = eds
            .data_service
            .fetch_enhanced_entities::<DummyEntity>(&query)
            .await;
        assert!(res4.is_ok());

        // test mutations
        let ins = InsertCommand::new("Dummy");
        assert_eq!(eds.data_service.insert(&ins).await.unwrap(), 42);

        let upd = UpdateCommand::new("Dummy", 1);
        assert_eq!(eds.data_service.update(&upd).await.unwrap(), 42);

        let b_ins = BatchInsertCommand::new("Dummy");
        assert_eq!(eds.data_service.batch_insert(&b_ins).await.unwrap(), 0);

        let b_upd = BatchUpdateCommand::new("Dummy", Vec::new());
        assert_eq!(eds.data_service.batch_update(&b_upd).await.unwrap(), 0);

        let del = DeleteCommand::new("Dummy", 1);
        assert_eq!(eds.data_service.delete(&del).await.unwrap(), 42);

        let rec = RecoverCommand::new("Dummy", 1, 1);
        assert_eq!(eds.data_service.recover(&rec).await.unwrap(), 42);

        // test failing operations
        let mut ctx_fail =
            UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(schema.clone()));
        ctx_fail.register_executor(DummyFailingExecutor);
        let eds_fail = ctx_fail
            .entity_data_service::<DummyFailingExecutor>("Dummy")
            .unwrap();

        assert!(eds_fail.data_service.fetch_all(query.clone()).await.is_err());
        assert!(eds_fail.data_service.insert(&ins).await.is_err());
        assert!(eds_fail.data_service.update(&upd).await.is_err());

        
        let mut b_ins_non_empty = BatchInsertCommand::new("Dummy");
        b_ins_non_empty.batch_values.push(Record::new());
        assert!(eds_fail.data_service.batch_insert(&b_ins_non_empty).await.is_err());
        
        let mut b_upd_non_empty = BatchUpdateCommand::new("Dummy", vec!["some_field".to_string()]);
        b_upd_non_empty.batch_ids.push(teaql_core::Value::I64(1));
        b_upd_non_empty.batch_values.push(Record::new());
        assert!(eds_fail.data_service.batch_update(&b_upd_non_empty).await.is_err());
        
        assert!(eds_fail.data_service.delete(&del).await.is_err());
        assert!(eds_fail.data_service.recover(&rec).await.is_err());
    }

    #[test]
    fn test_invalidate_aggregation_cache_for() {
        let mut ctx = UserContext::new().with_metadata(InMemoryMetadataStore::new());
        ctx.register_executor(DummyExecutor);

        let cache = crate::data_service::InMemoryAggregationCache::with_namespace("Dummy");
        ctx.insert_resource(cache);

        let cache_trait: Arc<dyn crate::data_service::AggregationCacheBackend> =
            Arc::new(crate::data_service::InMemoryAggregationCache::with_namespace("Dummy"));
        ctx.insert_resource(cache_trait);

        let service = ctx.data_service_internal::<DummyExecutor>().unwrap();
        service.invalidate_aggregation_cache_for("Dummy");

        // also test invalidating when no cache is registered
        let mut ctx_no_cache = UserContext::new().with_metadata(InMemoryMetadataStore::new());
        ctx_no_cache.register_executor(DummyExecutor);
        let service_no_cache = ctx_no_cache.data_service_internal::<DummyExecutor>().unwrap();
        service_no_cache.invalidate_aggregation_cache_for("Dummy");
    }

    #[test]
    fn test_resolve_final_comment() {
        let mut ctx = UserContext::new().with_metadata(InMemoryMetadataStore::new());
        ctx.register_executor(DummyExecutor);
        ctx.set_user_identifier("user123");

        let service = ctx.data_service_internal::<DummyExecutor>().unwrap();

        let trace = vec![TraceNode {
            entity_type: "Order".to_string(),
            entity_id: Some(1),
            comment: "creating".to_string(),
        }];

        let trace_no_id = vec![TraceNode {
            entity_type: "Order".to_string(),
            entity_id: None,
            comment: "creating".to_string(),
        }];

        // trace + user
        let comment = service.resolve_final_comment(&trace, None);
        assert_eq!(comment.unwrap(), "[user123] Order(1): creating");

        let comment_no_id = service.resolve_final_comment(&trace_no_id, None);
        assert_eq!(comment_no_id.unwrap(), "[user123] Order(pending): creating");

        // trace + user + incoming comment (trace takes precedence)
        let comment = service.resolve_final_comment(&trace, Some("my comment".to_string()));
        assert_eq!(comment.unwrap(), "[user123] Order(1): creating");

        // no trace + user + incoming comment
        let comment = service.resolve_final_comment(&[], Some("my comment".to_string()));
        assert_eq!(comment.unwrap(), "[user123] my comment");

        // no trace + user + empty incoming comment -> falls back to user only
        let comment = service.resolve_final_comment(&[], Some("".to_string()));
        assert_eq!(comment.unwrap(), "[user123]");

        // user + no comment
        let comment = service.resolve_final_comment(&[], None);
        assert_eq!(comment.unwrap(), "[user123]");

        // no user
        let mut ctx2 = UserContext::new().with_metadata(InMemoryMetadataStore::new());
        ctx2.set_user_identifier("");
        ctx2.register_executor(DummyExecutor);
        let service2 = ctx2.data_service_internal::<DummyExecutor>().unwrap();

        let comment = service2.resolve_final_comment(&trace, None);
        assert_eq!(comment.unwrap(), "Order(1): creating");

        let comment = service2.resolve_final_comment(&[], Some("my comment".to_string()));
        assert_eq!(comment.unwrap(), "my comment");

        let comment = service2.resolve_final_comment(&[], Some("".to_string()));
        assert!(comment.is_none());

        let comment = service2.resolve_final_comment(&[], None);
        assert!(comment.is_none());

        // Cover unused dummy methods
        assert_eq!(format!("{}", DummyError), "dummy");
        let _caps1 = DummyExecutor::default().capabilities();
        let _caps2 = DummyFailingExecutor::default().capabilities();
        let _desc = <DummyEntity as teaql_core::TeaqlEntity>::entity_descriptor();
        let entity = DummyEntity::from_record(Record::new()).unwrap();
        let _record = entity.into_record();
    }
}
