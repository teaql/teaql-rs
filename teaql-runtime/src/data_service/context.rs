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
    pub fn data_service<E>(&self) -> Result<ContextDataService<'_, E>, ContextError>
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
            data_service: self.data_service::<E>()?,
            trace_context: Vec::new(),
        })
    }

    /// Register a data-service executor and automatically set up the
    /// type-erased [`DynGraphSaver`](crate::DynGraphSaver) so that
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
        self.insert_resource::<Arc<dyn crate::DynGraphSaver>>(Arc::new(
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

    pub async fn fetch_all(
        &self,
        mut query: SelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        let final_comment = self.resolve_final_comment(&query.trace_chain, query.comment.clone());
        query.comment = final_comment;
        self.data_service().fetch_all(&query).await
    }

    pub async fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, DataServiceError<E::Error>> {
        self.data_service().fetch_smart_list(query).await
    }

    pub async fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        self.data_service().fetch_entities(query).await
    }

    pub async fn fetch_enhanced_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        self.data_service().fetch_enhanced_entities(query).await
    }

    pub async fn insert(&self, command: &InsertCommand) -> Result<u64, DataServiceError<E::Error>> {
        let affected = self.data_service().insert(command).await?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub async fn update(&self, command: &UpdateCommand) -> Result<u64, DataServiceError<E::Error>> {
        let affected = self.data_service().update(command).await?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub async fn batch_insert(
        &self,
        command: &teaql_core::BatchInsertCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let affected = self.data_service().batch_insert(command).await?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub async fn batch_update(
        &self,
        command: &teaql_core::BatchUpdateCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let affected = self.data_service().batch_update(command).await?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub async fn delete(&self, command: &DeleteCommand) -> Result<u64, DataServiceError<E::Error>> {
        let affected = self.data_service().delete(command).await?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub async fn recover(
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
