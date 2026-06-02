use std::sync::Arc;
use std::time::{Instant, SystemTime};

use teaql_core::{
    DeleteCommand, Entity, InsertCommand, Record, RecoverCommand, SelectQuery, SmartList,
    UpdateCommand,
};
use teaql_data_service::{MutationRequest, QueryRequest};

use crate::{
    ContextError, GraphMutationPlan, GraphNode, RepositoryError, RuntimeError,
    UserContext,
};

use super::{
    AggregationCacheBackend, ContextRepository, InMemoryAggregationCache,
    Repository, ResolvedRepository, UserContextMetadata,
    helpers::invalidate_aggregation_cache_namespace,
};

impl UserContext {
    pub fn repository<E>(&self) -> Result<ContextRepository<'_, E>, ContextError>
    where
        E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync + 'static,
    {
        if self.metadata.is_none() {
            return Err(ContextError::MissingResource("metadata".to_owned()));
        }

        let executor = self.require_resource::<E>()?;
        Ok(ContextRepository {
            metadata: UserContextMetadata { context: self },
            executor,
        })
    }

    #[doc(hidden)]
    pub fn resolve_repository<E>(
        &self,
        entity: impl Into<String>,
    ) -> Result<ResolvedRepository<'_, E>, ContextError>
    where
        E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync + 'static,
    {
        let entity = entity.into();
        if !self.has_repository(&entity) {
            return Err(ContextError::MissingRepository(entity));
        }
        Ok(ResolvedRepository {
            entity,
            repository: self.repository::<E>()?,
            trace_context: Vec::new(),
        })
    }

    pub fn plan_for_save_graph<E>(
        &self,
        node: GraphNode,
    ) -> Result<GraphMutationPlan, RepositoryError<E::Error>>
    where
        E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync + 'static,
    {
        let repository = self
            .resolve_repository::<E>(node.entity.clone())
            .map_err(|err| RepositoryError::Runtime(RuntimeError::Graph(err.to_string())))?;
        repository.plan_graph(node)
    }
}

impl<'a, E> ContextRepository<'a, E>
where
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor,
{
    fn repository(&self) -> Repository<'_, UserContextMetadata<'_>, E> {
        Repository::new(&self.metadata, self.executor)
    }

    pub fn fetch_all(&self, mut query: SelectQuery) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let final_comment = self.resolve_final_comment(&query.trace_chain, query.comment.clone());
        query.comment = final_comment;
        self.repository().fetch_all(&query)
    }

    pub fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, RepositoryError<E::Error>> {
        self.repository().fetch_smart_list(query)
    }

    pub fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.repository().fetch_entities(query)
    }

    pub fn fetch_enhanced_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.repository().fetch_enhanced_entities(query)
    }

    pub fn insert(&self, command: &InsertCommand) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.repository().insert(command)?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub fn update(&self, command: &UpdateCommand) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.repository().update(command)?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub fn batch_insert(&self, command: &teaql_core::BatchInsertCommand) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.repository().batch_insert(command)?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub fn batch_update(&self, command: &teaql_core::BatchUpdateCommand) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.repository().batch_update(command)?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub fn delete(&self, command: &DeleteCommand) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.repository().delete(command)?;
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub fn recover(&self, command: &RecoverCommand) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.repository().recover(command)?;
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

    pub(crate) fn resolve_final_comment(&self, trace_chain: &[teaql_core::TraceNode], comment: Option<String>) -> Option<String> {
        let chain_str = if trace_chain.is_empty() {
            None
        } else {
            let formatted = trace_chain.iter().map(|n| {
                format!("{}({}): {}", n.entity_type, n.entity_id.map(|id| id.to_string()).unwrap_or_else(|| "pending".to_owned()), n.comment)
            }).collect::<Vec<_>>().join(" -> ");
            Some(formatted)
        };

        let business_comment = chain_str.or(comment);
        let user_id = self.metadata.context.user_identifier().map(|s| s.to_owned());

        match (user_id, business_comment) {
            (Some(user), Some(bus)) if !user.is_empty() && !bus.is_empty() => {
                Some(format!("[{user}] {bus}"))
            }
            (Some(user), _) if !user.is_empty() => {
                Some(format!("[{user}]"))
            }
            (_, Some(bus)) if !bus.is_empty() => {
                Some(bus)
            }
            _ => None,
        }
    }
}
