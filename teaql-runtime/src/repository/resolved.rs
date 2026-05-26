use std::sync::Arc;
use std::time::{Instant, SystemTime};

use teaql_core::{
    AggregationCacheOptions, DeleteCommand, Entity, Expr, InsertCommand, Record, RecoverCommand,
    RelationAggregate, SelectQuery, SmartList, UpdateCommand, Value,
};
use teaql_sql::{CompiledQuery, SqlDialect};

use crate::{
    CheckObjectStatus, EntityEvent, RepositoryBehavior, RepositoryError, RuntimeError,
    SqlLogOperation, clear_record_status, mark_record_status,
};

use super::{
    AggregationCacheBackend, ContextRepository, InMemoryAggregationCache, QueryExecutor,
    ResolvedRepository, UserContextMetadata, helpers::*,
};

impl<'a, D, E> ResolvedRepository<'a, D, E>
where
    D: SqlDialect,
    E: QueryExecutor,
{
    pub(super) fn query_behavior(&self, entity: &str) -> Option<Arc<dyn RepositoryBehavior>> {
        self.repository.metadata.context.repository_behavior(entity)
    }

    pub(super) fn behavior(&self) -> Option<Arc<dyn RepositoryBehavior>> {
        self.repository
            .metadata
            .context
            .repository_behavior(&self.entity)
    }

    pub fn entity(&self) -> &str {
        &self.entity
    }

    pub fn select(&self) -> SelectQuery {
        SelectQuery::new(self.entity.clone())
    }

    pub fn insert_command(&self) -> InsertCommand {
        InsertCommand::new(self.entity.clone())
    }

    fn enforce_insert_policy(&self, command: &mut InsertCommand) -> Result<(), RuntimeError> {
        if let Some(policy) = self.repository.metadata.context.request_policy.as_ref() {
            policy.enforce_insert(self.repository.metadata.context, command)?;
        }
        Ok(())
    }

    fn enforce_update_policy(&self, command: &mut UpdateCommand) -> Result<(), RuntimeError> {
        if let Some(policy) = self.repository.metadata.context.request_policy.as_ref() {
            policy.enforce_update(self.repository.metadata.context, command)?;
        }
        Ok(())
    }

    fn enforce_delete_policy(&self, command: &mut DeleteCommand) -> Result<(), RuntimeError> {
        if let Some(policy) = self.repository.metadata.context.request_policy.as_ref() {
            policy.enforce_delete(self.repository.metadata.context, command)?;
        }
        Ok(())
    }

    fn enforce_recover_policy(&self, command: &mut RecoverCommand) -> Result<(), RuntimeError> {
        if let Some(policy) = self.repository.metadata.context.request_policy.as_ref() {
            policy.enforce_recover(self.repository.metadata.context, command)?;
        }
        Ok(())
    }

    fn prepare_select_query(&self, query: &SelectQuery) -> Result<SelectQuery, RuntimeError> {
        let mut query = query.clone();
        if let Some(behavior) = self.query_behavior(&query.entity) {
            behavior.before_select(self.repository.metadata.context, &mut query)?;
        }
        if let Some(policy) = self.repository.metadata.context.request_policy.as_ref() {
            policy.enforce_select(self.repository.metadata.context, &mut query)?;
        }
        Ok(query)
    }

    pub fn prepare_insert_command(
        &self,
        command: &InsertCommand,
    ) -> Result<InsertCommand, RuntimeError> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior.before_insert(self.repository.metadata.context, &mut command)?;
        }
        self.enforce_insert_policy(&mut command)?;

        let entity = self
            .repository
            .metadata
            .context
            .require_entity(&command.entity)?;
        if let Some(id_property) = entity.id_property() {
            let needs_id = !command.values.contains_key(&id_property.name)
                || is_unassigned_id(command.values.get(&id_property.name));
            if needs_id {
                let id = self.repository.metadata.context.next_id(&command.entity)?;
                command
                    .values
                    .insert(id_property.name.clone(), Value::U64(id));
            }
        }
        ensure_initial_version(&mut command.values, entity);
        mark_record_status(&mut command.values, CheckObjectStatus::Create);
        let check_result = self
            .repository
            .metadata
            .context
            .check_and_fix_record(&command.entity, &mut command.values);
        clear_record_status(&mut command.values);
        check_result?;

        Ok(command)
    }

    pub fn update_command(&self, id: impl Into<Value>) -> UpdateCommand {
        UpdateCommand::new(self.entity.clone(), id)
    }

    pub fn prepare_update_command(
        &self,
        command: &UpdateCommand,
    ) -> Result<UpdateCommand, RuntimeError> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior.before_update(self.repository.metadata.context, &mut command)?;
        }
        self.enforce_update_policy(&mut command)?;
        mark_record_status(&mut command.values, CheckObjectStatus::Update);
        let check_result = self
            .repository
            .metadata
            .context
            .check_and_fix_record(&command.entity, &mut command.values);
        clear_record_status(&mut command.values);
        check_result?;
        Ok(command)
    }

    pub fn delete_command(&self, id: impl Into<Value>) -> DeleteCommand {
        DeleteCommand::new(self.entity.clone(), id)
    }

    pub fn recover_command(&self, id: impl Into<Value>, expected_version: i64) -> RecoverCommand {
        RecoverCommand::new(self.entity.clone(), id, expected_version)
    }

    pub fn compile(&self, query: &SelectQuery) -> Result<CompiledQuery, RuntimeError> {
        let query = self.prepare_select_query(query)?;
        self.repository.compile(&query)
    }

    pub fn fetch_all(&self, query: &SelectQuery) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let query = self
            .prepare_select_query(query)
            .map_err(RepositoryError::Runtime)?;
        let _guard = crate::context::QueryCommentGuard::new(self.repository.metadata.context, query.comment.clone());
        let mut rows = self.fetch_prepared_query(&query)?;
        self.enhance_object_group_bys(&mut rows, &query.object_group_bys)?;
        self.enhance_child_queries(&mut rows, &query.child_enhancements)?;
        Ok(rows)
    }

    fn fetch_prepared_query(
        &self,
        query: &SelectQuery,
    ) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let _guard = crate::context::QueryCommentGuard::new(self.repository.metadata.context, query.comment.clone());
        let compiled = self
            .repository
            .compile(query)
            .map_err(RepositoryError::Runtime)?;
        if let Some(options) = query.aggregation_cache.filter(|options| options.enabled) {
            if let Some(cache) = self
                .repository
                .metadata
                .context
                .get_resource::<Arc<dyn AggregationCacheBackend>>()
            {
                return self.fetch_prepared_query_with_cache(
                    query,
                    &compiled,
                    options,
                    cache.as_ref(),
                );
            }
            if let Some(cache) = self
                .repository
                .metadata
                .context
                .get_resource::<InMemoryAggregationCache>()
            {
                return self.fetch_prepared_query_with_cache(query, &compiled, options, cache);
            }
        }
        let started_at = SystemTime::now();
        let started = Instant::now();
        let rows = self
            .repository
            .executor
            .fetch_all(&compiled)
            .map_err(RepositoryError::Executor)?;
        self.repository.log_sql_result(
            SqlLogOperation::Select,
            &compiled,
            started_at,
            started,
            Some(rows.len()),
            Some(query.entity.clone()),
            None,
            query.comment.clone(),
        );
        Ok(rows)
    }

    fn fetch_prepared_query_with_cache(
        &self,
        query: &SelectQuery,
        compiled: &CompiledQuery,
        options: AggregationCacheOptions,
        cache: &dyn AggregationCacheBackend,
    ) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let _guard = crate::context::QueryCommentGuard::new(self.repository.metadata.context, query.comment.clone());
        let key = aggregation_cache_key(
            cache.namespace(),
            &aggregation_cache_namespace(&query.entity),
            compiled,
        );
        if let Some(rows) = cache.get(&key, options.cache_expired_millis) {
            return Ok(rows);
        }
        let started_at = SystemTime::now();
        let started = Instant::now();
        let rows = self
            .repository
            .executor
            .fetch_all(compiled)
            .map_err(RepositoryError::Executor)?;
        self.repository.log_sql_result(
            SqlLogOperation::Select,
            compiled,
            started_at,
            started,
            Some(rows.len()),
            Some(query.entity.clone()),
            None,
            query.comment.clone(),
        );
        cache.put(key, rows.clone());
        Ok(rows)
    }

    pub fn fetch_all_with_relation_aggregates(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let _guard = crate::context::QueryCommentGuard::new(self.repository.metadata.context, query.comment.clone());
        let mut rows = self.fetch_all(query)?;
        self.enhance_relation_aggregates(&mut rows, relation_aggregates, query.aggregation_cache)?;
        Ok(rows)
    }

    pub fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, RepositoryError<E::Error>> {
        let query = self
            .prepare_select_query(query)
            .map_err(RepositoryError::Runtime)?;
        let _guard = crate::context::QueryCommentGuard::new(self.repository.metadata.context, query.comment.clone());
        self.repository.fetch_smart_list(&query)
    }

    pub fn fetch_smart_list_with_relation_aggregates(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<Record>, RepositoryError<E::Error>> {
        self.fetch_all_with_relation_aggregates(query, relation_aggregates)
            .map(SmartList::from)
    }

    pub fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        let query = self
            .prepare_select_query(query)
            .map_err(RepositoryError::Runtime)?;
        let _guard = crate::context::QueryCommentGuard::new(self.repository.metadata.context, query.comment.clone());
        self.repository.fetch_entities(&query)
    }

    pub fn fetch_entities_with_relation_aggregates<T>(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_all_with_relation_aggregates(query, relation_aggregates)?
            .into_iter()
            .map(T::from_record)
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(RepositoryError::Entity)
    }

    pub fn fetch_enhanced_entities_with_relation_aggregates<T>(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        let query = self
            .prepare_select_query(query)
            .map_err(RepositoryError::Runtime)?;
        let _guard = crate::context::QueryCommentGuard::new(self.repository.metadata.context, query.comment.clone());

        let mut rows = self.repository.fetch_all(&query)?;
        self.enhance_relation_aggregates(&mut rows, relation_aggregates, query.aggregation_cache)?;
        self.enhance_query_relations(&mut rows, &query)?;
        self.enhance_relations(&mut rows)?;
        rows.into_iter()
            .map(T::from_record)
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(RepositoryError::Entity)
    }

    pub fn fetch_enhanced_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        let query = self
            .prepare_select_query(query)
            .map_err(RepositoryError::Runtime)?;
        let _guard = crate::context::QueryCommentGuard::new(self.repository.metadata.context, query.comment.clone());

        let mut rows = self.repository.fetch_all(&query)?;
        self.enhance_query_relations(&mut rows, &query)?;
        self.enhance_relations(&mut rows)?;
        rows.into_iter()
            .map(T::from_record)
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(RepositoryError::Entity)
    }

    pub fn insert(&self, command: &InsertCommand) -> Result<u64, RepositoryError<E::Error>> {
        let command = self
            .prepare_insert_command(command)
            .map_err(RepositoryError::Runtime)?;
        self.execute_prepared_insert(command)
    }

    pub fn update(&self, command: &UpdateCommand) -> Result<u64, RepositoryError<E::Error>> {
        let command = self
            .prepare_update_command(command)
            .map_err(RepositoryError::Runtime)?;
        self.execute_prepared_update(command)
    }

    pub fn delete(&self, command: &DeleteCommand) -> Result<u64, RepositoryError<E::Error>> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_delete(self.repository.metadata.context, &mut command)
                .map_err(RepositoryError::Runtime)?;
        }
        self.enforce_delete_policy(&mut command)
            .map_err(RepositoryError::Runtime)?;
        let old_values = self.fetch_current_event_row(&command.entity, &command.id)?;
        let affected = self.repository.delete(&command)?;
        self.emit_event(EntityEvent::deleted_with_old_values(
            command.entity,
            command.id,
            command.expected_version,
            old_values,
        ))
        .map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    pub fn recover(&self, command: &RecoverCommand) -> Result<u64, RepositoryError<E::Error>> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_recover(self.repository.metadata.context, &mut command)
                .map_err(RepositoryError::Runtime)?;
        }
        self.enforce_recover_policy(&mut command)
            .map_err(RepositoryError::Runtime)?;
        let old_values = self.fetch_current_event_row(&command.entity, &command.id)?;
        let affected = self.repository.recover(&command)?;
        self.emit_event(EntityEvent::recovered_with_old_values(
            command.entity,
            command.id,
            command.expected_version,
            old_values,
        ))
        .map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    fn emit_event(&self, event: EntityEvent) -> Result<(), RuntimeError> {
        self.repository.metadata.context.send_event(event)
    }

    pub(super) fn execute_prepared_insert(
        &self,
        command: InsertCommand,
    ) -> Result<u64, RepositoryError<E::Error>> {
        let affected = self.repository.insert(&command)?;
        self.emit_event(EntityEvent::created(command.entity, command.values))
            .map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    pub(super) fn execute_prepared_update(
        &self,
        command: UpdateCommand,
    ) -> Result<u64, RepositoryError<E::Error>> {
        let old_values = self.fetch_current_event_row(&command.entity, &command.id)?;
        let affected = self.repository.update(&command)?;
        let updated_fields = command.values.keys().cloned().collect();
        let mut values = command.values.clone();
        values.insert("id".to_owned(), command.id.clone());
        if let Some(version) = command.expected_version {
            values.insert("version".to_owned(), Value::I64(version + 1));
        }
        let mut new_values = old_values.clone().unwrap_or_default();
        for (field, value) in &values {
            new_values.insert(field.clone(), value.clone());
        }
        self.emit_event(EntityEvent::updated_with_old_values(
            command.entity,
            values,
            old_values,
            new_values,
            updated_fields,
        ))
        .map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    fn fetch_current_event_row(
        &self,
        entity: &str,
        id: &Value,
    ) -> Result<Option<Record>, RepositoryError<E::Error>> {
        if self.repository.metadata.context.event_sink.is_none() {
            return Ok(None);
        }
        let descriptor = self
            .repository
            .metadata
            .context
            .require_entity(entity)
            .map_err(RepositoryError::Runtime)?;
        let Some(id_property) = descriptor.id_property() else {
            return Ok(None);
        };
        let mut rows = self.fetch_all(
            &SelectQuery::new(entity).filter(Expr::eq(id_property.name.clone(), id.clone())),
        )?;
        Ok(rows.pop())
    }

    pub(super) fn scoped_repository(&self, entity: String) -> ResolvedRepository<'a, D, E> {
        ResolvedRepository {
            entity,
            repository: ContextRepository {
                metadata: UserContextMetadata {
                    context: self.repository.metadata.context,
                },
                dialect: self.repository.dialect,
                executor: self.repository.executor,
            },
        }
    }
}
