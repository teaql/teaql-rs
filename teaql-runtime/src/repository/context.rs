use std::sync::Arc;
use std::time::{Instant, SystemTime};

use teaql_core::{
    DeleteCommand, Entity, InsertCommand, Record, RecoverCommand, SelectQuery, SmartList,
    UpdateCommand,
};
use teaql_sql::{CompiledQuery, SqlDialect};

use crate::{
    ContextError, GraphMutationPlan, GraphNode, RepositoryError, RuntimeError, SqlLogOperation,
    UserContext,
};

use super::{
    AggregationCacheBackend, ContextRepository, InMemoryAggregationCache, QueryExecutor,
    Repository, ResolvedRepository, UserContextMetadata,
    helpers::invalidate_aggregation_cache_namespace,
};

impl UserContext {
    pub fn repository<D, E>(&self) -> Result<ContextRepository<'_, D, E>, ContextError>
    where
        D: SqlDialect + Send + Sync + 'static,
        E: QueryExecutor + Send + Sync + 'static,
    {
        if self.metadata.is_none() {
            return Err(ContextError::MissingResource("metadata".to_owned()));
        }

        let dialect = self.require_resource::<D>()?;
        let executor = self.require_resource::<E>()?;
        Ok(ContextRepository {
            metadata: UserContextMetadata { context: self },
            dialect,
            executor,
        })
    }

    #[doc(hidden)]
    pub fn resolve_repository<D, E>(
        &self,
        entity: impl Into<String>,
    ) -> Result<ResolvedRepository<'_, D, E>, ContextError>
    where
        D: SqlDialect + Send + Sync + 'static,
        E: QueryExecutor + Send + Sync + 'static,
    {
        let entity = entity.into();
        if !self.has_repository(&entity) {
            return Err(ContextError::MissingRepository(entity));
        }
        Ok(ResolvedRepository {
            entity,
            repository: self.repository::<D, E>()?,
            trace_context: Vec::new(),
        })
    }

    pub fn plan_for_save_graph<D, E>(
        &self,
        node: GraphNode,
    ) -> Result<GraphMutationPlan, RepositoryError<E::Error>>
    where
        D: SqlDialect + Send + Sync + 'static,
        E: QueryExecutor + Send + Sync + 'static,
    {
        let repository = self
            .resolve_repository::<D, E>(node.entity.clone())
            .map_err(|err| RepositoryError::Runtime(RuntimeError::Graph(err.to_string())))?;
        repository.plan_graph(node)
    }
}

impl<'a, D, E> ContextRepository<'a, D, E>
where
    D: SqlDialect,
    E: QueryExecutor,
{
    fn repository(&self) -> Repository<'_, D, UserContextMetadata<'_>, E> {
        Repository::new(self.dialect, &self.metadata, self.executor)
    }

    pub fn compile(&self, query: &SelectQuery) -> Result<CompiledQuery, RuntimeError> {
        self.repository().compile(query)
    }

    pub fn fetch_all(&self, query: &SelectQuery) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let mut compiled = self.compile(query).map_err(RepositoryError::Runtime)?;
        let final_comment = self.resolve_final_comment(&query.trace_chain, query.comment.clone());
        compiled.comment = final_comment;

        let started_at = SystemTime::now();
        let started = Instant::now();
        let rows = self
            .executor
            .fetch_all(&compiled)
            .map_err(RepositoryError::Executor)?;
        self.log_sql_result(
            SqlLogOperation::Select,
            &compiled,
            started_at,
            started,
            Some(rows.len()),
            Some(query.entity.clone()),
            None,
            query.trace_chain.clone(),
        );
        Ok(rows)
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
        let mut compiled = self
            .repository()
            .compile_insert(command)
            .map_err(RepositoryError::Runtime)?;
        let final_comment = self.resolve_final_comment(&command.trace_chain, None);
        compiled.comment = final_comment;

        let started_at = SystemTime::now();
        let started = Instant::now();
        let affected = self
            .executor
            .execute(&compiled)
            .map_err(RepositoryError::Executor)?;
        self.log_sql_result(
            SqlLogOperation::Insert,
            &compiled,
            started_at,
            started,
            None,
            None,
            Some(affected),
            command.trace_chain.clone(),
        );
        self.invalidate_aggregation_cache_for(&command.entity);
        Ok(affected)
    }

    pub fn update(&self, command: &UpdateCommand) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.execute_mutation(
            SqlLogOperation::Update,
            &command.entity,
            self.repository()
                .compile_update(command)
                .map_err(RepositoryError::Runtime)?,
            command.trace_chain.clone(),
        )?;
        if command.expected_version.is_some() && affected == 0 {
            return Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: command.entity.clone(),
                    id: format!("{:?}", command.id),
                },
            ));
        }
        Ok(affected)
    }

    pub fn delete(&self, command: &DeleteCommand) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.execute_mutation(
            SqlLogOperation::Delete,
            &command.entity,
            self.repository()
                .compile_delete(command)
                .map_err(RepositoryError::Runtime)?,
            command.trace_chain.clone(),
        )?;
        if command.expected_version.is_some() && affected == 0 {
            return Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: command.entity.clone(),
                    id: format!("{:?}", command.id),
                },
            ));
        }
        Ok(affected)
    }

    pub fn recover(&self, command: &RecoverCommand) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.execute_mutation(
            SqlLogOperation::Recover,
            &command.entity,
            self.repository()
                .compile_recover(command)
                .map_err(RepositoryError::Runtime)?,
            command.trace_chain.clone(),
        )?;
        if affected == 0 {
            return Err(RepositoryError::Runtime(
                RuntimeError::OptimisticLockConflict {
                    entity: command.entity.clone(),
                    id: format!("{:?}", command.id),
                },
            ));
        }
        Ok(affected)
    }

    fn execute_mutation(
        &self,
        operation: SqlLogOperation,
        entity: &str,
        mut compiled: CompiledQuery,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) -> Result<u64, RepositoryError<E::Error>> {
        let final_comment = self.resolve_final_comment(&trace_chain, None);
        compiled.comment = final_comment;

        let started_at = SystemTime::now();
        let started = Instant::now();
        let affected = self
            .executor
            .execute(&compiled)
            .map_err(RepositoryError::Executor)?;
        self.log_sql_result(
            operation,
            &compiled,
            started_at,
            started,
            None,
            None,
            Some(affected),
            trace_chain,
        );
        self.invalidate_aggregation_cache_for(entity);
        Ok(affected)
    }

    pub(super) fn log_sql_result(
        &self,
        operation: SqlLogOperation,
        compiled: &CompiledQuery,
        started_at: SystemTime,
        started: Instant,
        result_count: Option<usize>,
        result_type: Option<String>,
        affected_rows: Option<u64>,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) {
        self.metadata.context.record_sql_log(
            operation,
            compiled,
            self.dialect.kind(),
            started_at,
            SystemTime::now(),
            started.elapsed(),
            result_count,
            result_type,
            affected_rows,
            trace_chain,
        );
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
