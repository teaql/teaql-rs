use std::sync::Arc;

use teaql_core::{
    AggregationCacheOptions, DeleteCommand, Entity, InsertCommand, Record, RecoverCommand,
    RelationAggregate, SelectQuery, SmartList, UpdateCommand, Value,
};

use crate::{
    CheckObjectStatus, RawAuditEvent, RepositoryBehavior, RepositoryError, RuntimeError,
    clear_record_status, mark_record_status,
};

use super::{
    AggregationCacheBackend, ContextRepository, InMemoryAggregationCache,
    ResolvedRepository, UserContextMetadata, helpers::*,
};

impl<'a, E> ResolvedRepository<'a, E>
where
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync + 'static,
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
        
        let mut full_trace = self.trace_context.clone();
        full_trace.extend(query.trace_chain);
        query.trace_chain = full_trace;

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

        Ok(command)
    }

    pub fn delete_command(&self, id: impl Into<Value>) -> DeleteCommand {
        DeleteCommand::new(self.entity.clone(), id)
    }

    pub fn recover_command(&self, id: impl Into<Value>, expected_version: i64) -> RecoverCommand {
        RecoverCommand::new(self.entity.clone(), id, expected_version)
    }

    pub async fn fetch_all(&self, query: &SelectQuery) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let query = self
            .prepare_select_query(query)
            .map_err(RepositoryError::Runtime)?;
        self.fetch_prepared_all(&query).await
    }

    async fn fetch_prepared_all(&self, query: &SelectQuery) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let mut rows = self.fetch_prepared_query(query).await?;
        self.enhance_object_group_bys(&mut rows, &query.object_group_bys, &query.trace_chain).await?;
        self.enhance_child_queries(&mut rows, &query.child_enhancements, &query.trace_chain).await?;
        self.enhance_query_relations(&mut rows, query).await?;
        Ok(rows)
    }

    async fn fetch_prepared_query(
        &self,
        query: &SelectQuery,
    ) -> Result<Vec<Record>, RepositoryError<E::Error>> {

        let final_comment = self.repository.resolve_final_comment(&query.trace_chain, query.comment.clone());
        let mut query = query.clone();
        query.comment = final_comment;
        if let Some(options) = query.aggregation_cache.filter(|options| options.enabled) {
            if let Some(cache) = self
                .repository
                .metadata
                .context
                .get_resource::<Arc<dyn AggregationCacheBackend>>()
            {
                return self.fetch_prepared_query_with_cache(
                    &query,
                    options,
                    cache.as_ref(),
                ).await;
            }
            if let Some(cache) = self
                .repository
                .metadata
                .context
                .get_resource::<InMemoryAggregationCache>()
            {
                return self.fetch_prepared_query_with_cache(&query, options, cache).await;
            }
        }
        let request = teaql_data_service::QueryRequest {
            query: query.clone(),
            trace_chain: query.trace_chain.clone(),
            comment: query.comment.clone(),
        };
        let res = self
            .repository
            .executor
            .query(request)
            .await.map_err(RepositoryError::Executor)?;
        self.repository.metadata.context.record_metadata_log(&res.metadata);
        Ok(res.rows)
    }

    async fn fetch_prepared_query_with_cache(
        &self,
        query: &SelectQuery,
        options: AggregationCacheOptions,
        cache: &dyn AggregationCacheBackend,
    ) -> Result<Vec<Record>, RepositoryError<E::Error>> {

        let key = aggregation_cache_key(
            cache.namespace(),
            &aggregation_cache_namespace(&query.entity),
            query,
        );
        if let Some(rows) = cache.get(&key, options.cache_expired_millis) {
            return Ok(rows);
        }
        let request = teaql_data_service::QueryRequest {
            query: query.clone(),
            trace_chain: query.trace_chain.clone(),
            comment: query.comment.clone(),
        };
        let res = self
            .repository
            .executor
            .query(request)
            .await.map_err(RepositoryError::Executor)?;
        self.repository.metadata.context.record_metadata_log(&res.metadata);
        let rows = res.rows;
        cache.put(key, rows.clone());
        Ok(rows)
    }

    pub async fn fetch_all_with_relation_aggregates(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<Vec<Record>, RepositoryError<E::Error>> {
        let query = self
            .prepare_select_query(query)
            .map_err(RepositoryError::Runtime)?;

        let mut rows = self.fetch_prepared_all(&query).await?;
        self.enhance_relation_aggregates(&mut rows, relation_aggregates, query.aggregation_cache, &query.trace_chain).await?;
        Ok(rows)
    }

    pub async fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, RepositoryError<E::Error>> {
        let query = self
            .prepare_select_query(query)
            .map_err(RepositoryError::Runtime)?;

        self.repository.fetch_smart_list(&query).await
    }

    pub async fn fetch_smart_list_with_relation_aggregates(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<Record>, RepositoryError<E::Error>> {
        self.fetch_all_with_relation_aggregates(query, relation_aggregates).await
            .map(SmartList::from)
    }

    pub async fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        let query = self
            .prepare_select_query(query)
            .map_err(RepositoryError::Runtime)?;

        self.repository.fetch_entities(&query).await
    }

    pub async fn fetch_entities_with_relation_aggregates<T>(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_all_with_relation_aggregates(query, relation_aggregates).await?
            .into_iter()
            .map(|record| {
                let mut entity = T::from_record(record)?;
                let root = crate::EntityRoot::default();
                entity.on_loaded(&root as &dyn std::any::Any);
                Ok(entity)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(RepositoryError::Entity)
    }

    pub async fn fetch_enhanced_entities_with_relation_aggregates<T>(
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

        let mut rows = self.fetch_prepared_all(&query).await?;
        self.enhance_relation_aggregates(&mut rows, relation_aggregates, query.aggregation_cache, &query.trace_chain).await?;
        self.enhance_relations(&mut rows).await?;
        rows.into_iter()
            .map(|record| {
                let mut entity = T::from_record(record)?;
                let root = crate::EntityRoot::default();
                entity.on_loaded(&root as &dyn std::any::Any);
                Ok(entity)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(RepositoryError::Entity)
    }

    pub async fn fetch_enhanced_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, RepositoryError<E::Error>>
    where
        T: Entity,
    {
        let query = self
            .prepare_select_query(query)
            .map_err(RepositoryError::Runtime)?;

        let mut rows = self.fetch_prepared_all(&query).await?;
        self.enhance_relations(&mut rows).await?;
        let root = self.repository.metadata.context.get_resource::<crate::EntityRoot>().cloned();
        rows.into_iter()
            .map(|record| {
                let mut entity = T::from_record(record)?;
                if let Some(ref root) = root {
                    entity.on_loaded(root as &dyn std::any::Any);
                }
                Ok(entity)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(RepositoryError::Entity)
    }

    pub async fn insert(&self, command: &InsertCommand) -> Result<u64, RepositoryError<E::Error>> {
        let command = self
            .prepare_insert_command(command)
            .map_err(RepositoryError::Runtime)?;
        self.execute_prepared_insert_with_comment(command, self.trace_context.clone()).await
    }

    pub async fn update(&self, command: &UpdateCommand) -> Result<u64, RepositoryError<E::Error>> {
        let command = self
            .prepare_update_command(command)
            .map_err(RepositoryError::Runtime)?;
        self.execute_prepared_update_with_comment(command, self.trace_context.clone()).await
    }

    pub async fn delete(&self, command: &DeleteCommand) -> Result<u64, RepositoryError<E::Error>> {
        self.delete_scoped(command, self.trace_context.clone()).await
    }

    pub async fn delete_scoped(
        &self,
        command: &DeleteCommand,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) -> Result<u64, RepositoryError<E::Error>> {
        let mut command = command.clone();
        command.trace_chain = trace_chain.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_delete(self.repository.metadata.context, &mut command)
                .map_err(RepositoryError::Runtime)?;
        }
        self.enforce_delete_policy(&mut command)
            .map_err(RepositoryError::Runtime)?;

        let old_values = self.fetch_current_event_row(&command.entity, &command.id, trace_chain.clone())?;
        let affected = self.repository.delete(&command).await?;

        let mut event = RawAuditEvent::deleted_with_old_values(
            command.entity,
            command.id,
            command.expected_version,
            old_values,
        );
        event.trace_chain = trace_chain;
        self.emit_event(event)
            .map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    pub async fn recover(&self, command: &RecoverCommand) -> Result<u64, RepositoryError<E::Error>> {
        let mut command = command.clone();
        command.trace_chain = self.trace_context.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_recover(self.repository.metadata.context, &mut command)
                .map_err(RepositoryError::Runtime)?;
        }
        self.enforce_recover_policy(&mut command)
            .map_err(RepositoryError::Runtime)?;
        let old_values = self.fetch_current_event_row(&command.entity, &command.id, command.trace_chain.clone())?;
        let affected = self.repository.recover(&command).await?;
        let event = RawAuditEvent::recovered_with_old_values(
            command.entity,
            command.id,
            command.expected_version,
            old_values,
        );
        self.emit_event(event)
            .map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    fn emit_event(&self, event: RawAuditEvent) -> Result<(), RuntimeError> {
        self.repository.metadata.context.send_event(event)
    }

    #[allow(dead_code)]
    pub(super) async fn execute_prepared_insert(
        &self,
        command: InsertCommand,
    ) -> Result<u64, RepositoryError<E::Error>> {
        self.execute_prepared_insert_with_comment(command, Vec::new()).await
    }

    pub(super) async fn execute_prepared_insert_with_comment(
        &self,
        mut command: InsertCommand,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) -> Result<u64, RepositoryError<E::Error>> {
        command.trace_chain = trace_chain.clone();
        let affected = self.repository.insert(&command).await?;
        let mut event = RawAuditEvent::created(command.entity, command.values);
        event.trace_chain = trace_chain;
        self.emit_event(event).map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    pub(super) async fn execute_prepared_batch_insert(
        &self,
        command: teaql_core::BatchInsertCommand,
    ) -> Result<u64, RepositoryError<E::Error>> {
        if command.batch_values.is_empty() {
            return Ok(0);
        }
        let affected = self.repository.batch_insert(&command).await?;
        
        let entity = command.entity.clone();
        for (i, values) in command.batch_values.into_iter().enumerate() {
            let mut event = RawAuditEvent::created(entity.clone(), values);
            if i < command.trace_chains.len() {
                event.trace_chain = command.trace_chains[i].clone();
            }
            self.emit_event(event).map_err(RepositoryError::Runtime)?;
        }
        Ok(affected)
    }

    #[allow(dead_code)]
    pub(super) async fn execute_prepared_update(
        &self,
        command: UpdateCommand,
    ) -> Result<u64, RepositoryError<E::Error>> {
        self.execute_prepared_update_with_comment(command, Vec::new()).await
    }

    pub(super) async fn execute_prepared_update_with_comment(
        &self,
        mut command: UpdateCommand,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) -> Result<u64, RepositoryError<E::Error>> {
        command.trace_chain = trace_chain.clone();
        
        let mut old_values = command.old_values.clone();
        let needs_fetch = match &old_values {
            Some(snapshot) => !command.values.keys().all(|k| snapshot.contains_key(k)),
            None => true,
        };
        if needs_fetch {
            old_values = self.fetch_current_event_row(&command.entity, &command.id, trace_chain.clone())?;
        }

        let affected = self.repository.update(&command).await?;
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
        let mut event = RawAuditEvent::updated_with_old_values(
            command.entity,
            values,
            old_values,
            new_values,
            updated_fields,
        );
        event.trace_chain = trace_chain;
        self.emit_event(event).map_err(RepositoryError::Runtime)?;
        Ok(affected)
    }

    pub(super) async fn execute_prepared_batch_update(
        &self,
        command: teaql_core::BatchUpdateCommand,
    ) -> Result<u64, RepositoryError<E::Error>> {
        if command.batch_values.is_empty() {
            return Ok(0);
        }
        let affected = self.repository.batch_update(&command).await?;
        
        let entity = command.entity.clone();
        for (i, values) in command.batch_values.into_iter().enumerate() {
            let mut full_values = values.clone();
            full_values.insert("id".to_owned(), command.batch_ids[i].clone());
            if let Some(Some(version)) = command.batch_expected_versions.get(i) {
                full_values.insert("version".to_owned(), teaql_core::Value::I64(*version + 1));
            }
            
            let old_values = command.batch_old_values.get(i).cloned().unwrap_or(None);
            let mut new_values = old_values.clone().unwrap_or_default();
            for (field, value) in &full_values {
                new_values.insert(field.clone(), value.clone());
            }
            
            let mut event = RawAuditEvent::updated_with_old_values(
                entity.clone(),
                full_values,
                old_values,
                new_values,
                command.update_fields.clone(),
            );
            if i < command.trace_chains.len() {
                event.trace_chain = command.trace_chains[i].clone();
            }
            self.emit_event(event).map_err(RepositoryError::Runtime)?;
        }
        Ok(affected)
    }

    fn fetch_current_event_row(
        &self,
        _entity: &str,
        _id: &Value,
        _trace_chain: Vec<teaql_core::TraceNode>,
    ) -> Result<Option<Record>, RepositoryError<E::Error>> {
        // PER THE USER: "我们不需要在审计的时候去抓旧的值"
        // Avoid DB queries during event emission. We rely on in-memory `original_values`.
        Ok(None)
    }


    pub fn scoped_repository(&self, entity: String) -> ResolvedRepository<'a, E> {
        ResolvedRepository {
            entity,
            repository: ContextRepository {
                metadata: UserContextMetadata {
                    context: self.repository.metadata.context,
                },
                executor: self.repository.executor,
            },
            trace_context: Vec::new(),
        }
    }
}
