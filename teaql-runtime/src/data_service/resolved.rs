use std::sync::Arc;

use teaql_core::{
    AggregationCacheOptions, DeleteCommand, Entity, InsertCommand, Record, RecoverCommand,
    RelationAggregate, SelectQuery, SmartList, UpdateCommand, Value,
};

use crate::{
    CheckObjectStatus, DataServiceError, EntityDataServiceBehavior, RawAuditEvent, RuntimeError,
    clear_record_status, mark_record_status,
};

use super::{
    AggregationCacheBackend, ContextDataService, EntityDataService, InMemoryAggregationCache,
    UserContextMetadata, helpers::*,
};

impl<'a, E> EntityDataService<'a, E>
where
    E: teaql_data_service::QueryExecutor
        + teaql_data_service::MutationExecutor
        + Send
        + Sync
        + 'static,
{
    pub(super) fn query_behavior(
        &self,
        entity: &str,
    ) -> Option<Arc<dyn EntityDataServiceBehavior>> {
        self.data_service
            .metadata
            .context
            .entity_data_service_behavior(entity)
    }

    pub(super) fn behavior(&self) -> Option<Arc<dyn EntityDataServiceBehavior>> {
        self.data_service
            .metadata
            .context
            .entity_data_service_behavior(&self.entity)
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
        if let Some(policy) = self.data_service.metadata.context.request_policy.as_ref() {
            policy.enforce_insert(self.data_service.metadata.context, command)?;
        }
        Ok(())
    }

    fn enforce_update_policy(&self, command: &mut UpdateCommand) -> Result<(), RuntimeError> {
        if let Some(policy) = self.data_service.metadata.context.request_policy.as_ref() {
            policy.enforce_update(self.data_service.metadata.context, command)?;
        }
        Ok(())
    }

    fn enforce_delete_policy(&self, command: &mut DeleteCommand) -> Result<(), RuntimeError> {
        if let Some(policy) = self.data_service.metadata.context.request_policy.as_ref() {
            policy.enforce_delete(self.data_service.metadata.context, command)?;
        }
        Ok(())
    }

    fn enforce_recover_policy(&self, command: &mut RecoverCommand) -> Result<(), RuntimeError> {
        if let Some(policy) = self.data_service.metadata.context.request_policy.as_ref() {
            policy.enforce_recover(self.data_service.metadata.context, command)?;
        }
        Ok(())
    }

    fn prepare_select_query(&self, query: &SelectQuery) -> Result<SelectQuery, RuntimeError> {
        let mut query = query.clone();

        let mut full_trace = self.trace_context.clone();
        full_trace.extend(query.trace_chain);
        query.trace_chain = full_trace;

        if let Some(behavior) = self.query_behavior(&query.entity) {
            behavior.before_select(self.data_service.metadata.context, &mut query)?;
        }
        if let Some(policy) = self.data_service.metadata.context.request_policy.as_ref() {
            policy.enforce_select(self.data_service.metadata.context, &mut query)?;
        }
        // Ensure local_key fields for relation loads are projected so that
        // enhance_query_relations can match parent rows to child records.
        if !query.relations.is_empty() {
            if let Some(descriptor) = self.data_service.metadata.context.entity(&query.entity) {
                for load in &query.relations {
                    if let Some(relation) = descriptor.relation_by_name(&load.name) {
                        if !query.projection.contains(&relation.local_key) {
                            query.projection.push(relation.local_key.clone());
                        }
                    }
                }
            }
        }
        Ok(query)
    }

    pub fn prepare_insert_command(
        &self,
        command: &InsertCommand,
    ) -> Result<InsertCommand, RuntimeError> {
        let mut command = command.clone();
        if let Some(behavior) = self.behavior() {
            behavior.before_insert(self.data_service.metadata.context, &mut command)?;
        }
        self.enforce_insert_policy(&mut command)?;

        let entity = self
            .data_service
            .metadata
            .context
            .require_entity(&command.entity)?;
        if let Some(id_property) = entity.id_property() {
            let needs_id = !command.values.contains_key(&id_property.name)
                || is_unassigned_id(command.values.get(&id_property.name));
            if needs_id {
                let id = self
                    .data_service
                    .metadata
                    .context
                    .next_id(&command.entity)?;
                command
                    .values
                    .insert(id_property.name.clone(), Value::U64(id));
            }
        }
        ensure_initial_version(&mut command.values, entity);
        mark_record_status(&mut command.values, CheckObjectStatus::Create);
        let check_result = self
            .data_service
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
            behavior.before_update(self.data_service.metadata.context, &mut command)?;
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

    pub async fn fetch_all(
        &self,
        query: &SelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        let query = self
            .prepare_select_query(query)
            .map_err(DataServiceError::Runtime)?;
        self.fetch_prepared_all(&query).await
    }

    /// Fetch records in streaming mode (chunked).
    /// Returns a Vec of chunks, each chunk containing up to `chunk_size` rows.
    /// Each chunk is enhanced (relations, children) before returning.
    /// Requires E to implement StreamQueryExecutor.
    pub async fn fetch_stream(
        &self,
        query: &SelectQuery,
    ) -> Result<Vec<teaql_data_service::StreamChunk>, DataServiceError<E::Error>>
    where
        E: teaql_data_service::StreamQueryExecutor,
    {
        let query = self
            .prepare_select_query(query)
            .map_err(DataServiceError::Runtime)?;

        let chunk_size = query
            .stream_config
            .as_ref()
            .map(|c| c.chunk_size)
            .unwrap_or(1000);

        let final_comment = self
            .data_service
            .resolve_final_comment(&query.trace_chain, query.comment.clone());
        let mut query = query.clone();
        query.comment = final_comment;

        let request = teaql_data_service::QueryRequest {
            query: query.clone(),
            trace_chain: query.trace_chain.clone(),
            comment: query.comment.clone(),
        };

        let chunks = self
            .data_service
            .executor
            .query_stream(request, chunk_size)
            .await
            .map_err(DataServiceError::Executor)?;

        // Enhance each chunk
        let mut enhanced_chunks = Vec::with_capacity(chunks.len());
        for mut chunk in chunks {
            self.enhance_object_group_bys(
                &mut chunk.rows,
                &query.object_group_bys,
                &query.trace_chain,
            )
            .await?;
            self.enhance_child_queries(
                &mut chunk.rows,
                &query.child_enhancements,
                &query.trace_chain,
            )
            .await?;
            self.enhance_query_relations(&mut chunk.rows, &query)
                .await?;
            enhanced_chunks.push(chunk);
        }

        Ok(enhanced_chunks)
    }

    async fn fetch_prepared_all(
        &self,
        query: &SelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        let mut rows = self.fetch_prepared_query(query).await?;
        self.enhance_object_group_bys(&mut rows, &query.object_group_bys, &query.trace_chain)
            .await?;
        self.enhance_child_queries(&mut rows, &query.child_enhancements, &query.trace_chain)
            .await?;
        self.enhance_query_relations(&mut rows, query).await?;
        Ok(rows)
    }

    async fn fetch_prepared_query(
        &self,
        query: &SelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        let final_comment = self
            .data_service
            .resolve_final_comment(&query.trace_chain, query.comment.clone());
        let mut query = query.clone();
        query.comment = final_comment;
        if let Some(options) = query.aggregation_cache.filter(|options| options.enabled) {
            if let Some(cache) = self
                .data_service
                .metadata
                .context
                .get_resource::<Arc<dyn AggregationCacheBackend>>()
            {
                return self
                    .fetch_prepared_query_with_cache(&query, options, cache.as_ref())
                    .await;
            }
            if let Some(cache) = self
                .data_service
                .metadata
                .context
                .get_resource::<InMemoryAggregationCache>()
            {
                return self
                    .fetch_prepared_query_with_cache(&query, options, cache)
                    .await;
            }
        }
        let request = teaql_data_service::QueryRequest {
            query: query.clone(),
            trace_chain: query.trace_chain.clone(),
            comment: query.comment.clone(),
        };
        let res = self
            .data_service
            .executor
            .query(request)
            .await
            .map_err(DataServiceError::Executor)?;
        self.data_service
            .metadata
            .context
            .record_metadata_log(&res.metadata);
        Ok(res.rows)
    }

    async fn fetch_prepared_query_with_cache(
        &self,
        query: &SelectQuery,
        options: AggregationCacheOptions,
        cache: &dyn AggregationCacheBackend,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
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
            .data_service
            .executor
            .query(request)
            .await
            .map_err(DataServiceError::Executor)?;
        self.data_service
            .metadata
            .context
            .record_metadata_log(&res.metadata);
        let rows = res.rows;
        cache.put(key, rows.clone());
        Ok(rows)
    }

    pub async fn fetch_all_with_relation_aggregates(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        let query = self
            .prepare_select_query(query)
            .map_err(DataServiceError::Runtime)?;

        let mut rows = self.fetch_prepared_all(&query).await?;
        self.enhance_relation_aggregates(
            &mut rows,
            relation_aggregates,
            query.aggregation_cache,
            &query.trace_chain,
        )
        .await?;
        Ok(rows)
    }

    pub async fn fetch_smart_list(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, DataServiceError<E::Error>> {
        let query = self
            .prepare_select_query(query)
            .map_err(DataServiceError::Runtime)?;

        self.data_service.fetch_smart_list(&query).await
    }

    pub async fn fetch_smart_list_with_relation_aggregates(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<Record>, DataServiceError<E::Error>> {
        self.fetch_all_with_relation_aggregates(query, relation_aggregates)
            .await
            .map(SmartList::from)
    }

    pub async fn fetch_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        let query = self
            .prepare_select_query(query)
            .map_err(DataServiceError::Runtime)?;

        self.data_service.fetch_entities(&query).await
    }

    pub async fn fetch_entities_with_relation_aggregates<T>(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_all_with_relation_aggregates(query, relation_aggregates)
            .await?
            .into_iter()
            .map(|record| {
                let mut entity = T::from_record(record)?;
                let root = crate::EntityRoot::default();
                entity.on_loaded(&root as &dyn std::any::Any);
                Ok(entity)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(DataServiceError::Entity)
    }

    pub async fn fetch_enhanced_entities_with_relation_aggregates<T>(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        let query = self
            .prepare_select_query(query)
            .map_err(DataServiceError::Runtime)?;

        let mut rows = self.fetch_prepared_all(&query).await?;
        self.enhance_relation_aggregates(
            &mut rows,
            relation_aggregates,
            query.aggregation_cache,
            &query.trace_chain,
        )
        .await?;
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
            .map_err(DataServiceError::Entity)
    }

    pub async fn fetch_enhanced_entities<T>(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        let query = self
            .prepare_select_query(query)
            .map_err(DataServiceError::Runtime)?;

        let mut rows = self.fetch_prepared_all(&query).await?;
        self.enhance_relations(&mut rows).await?;
        let root = self
            .data_service
            .metadata
            .context
            .get_resource::<crate::EntityRoot>()
            .cloned();
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
            .map_err(DataServiceError::Entity)
    }

    pub async fn insert(&self, command: &InsertCommand) -> Result<u64, DataServiceError<E::Error>> {
        let command = self
            .prepare_insert_command(command)
            .map_err(DataServiceError::Runtime)?;
        self.execute_prepared_insert_with_comment(command, self.trace_context.clone())
            .await
    }

    pub async fn update(&self, command: &UpdateCommand) -> Result<u64, DataServiceError<E::Error>> {
        let command = self
            .prepare_update_command(command)
            .map_err(DataServiceError::Runtime)?;
        self.execute_prepared_update_with_comment(command, self.trace_context.clone())
            .await
    }

    pub async fn delete(&self, command: &DeleteCommand) -> Result<u64, DataServiceError<E::Error>> {
        self.delete_scoped(command, self.trace_context.clone())
            .await
    }

    pub async fn delete_scoped(
        &self,
        command: &DeleteCommand,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let mut command = command.clone();
        command.trace_chain = trace_chain.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_delete(self.data_service.metadata.context, &mut command)
                .map_err(DataServiceError::Runtime)?;
        }
        self.enforce_delete_policy(&mut command)
            .map_err(DataServiceError::Runtime)?;

        let old_values =
            self.fetch_current_event_row(&command.entity, &command.id, trace_chain.clone())?;
        let affected = self.data_service.delete(&command).await?;

        let mut event = RawAuditEvent::deleted_with_old_values(
            command.entity,
            command.id,
            command.expected_version,
            old_values,
        );
        event.trace_chain = trace_chain;
        self.emit_event(event).map_err(DataServiceError::Runtime)?;
        Ok(affected)
    }

    pub async fn recover(
        &self,
        command: &RecoverCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let mut command = command.clone();
        command.trace_chain = self.trace_context.clone();
        if let Some(behavior) = self.behavior() {
            behavior
                .before_recover(self.data_service.metadata.context, &mut command)
                .map_err(DataServiceError::Runtime)?;
        }
        self.enforce_recover_policy(&mut command)
            .map_err(DataServiceError::Runtime)?;
        let old_values = self.fetch_current_event_row(
            &command.entity,
            &command.id,
            command.trace_chain.clone(),
        )?;
        let affected = self.data_service.recover(&command).await?;
        let event = RawAuditEvent::recovered_with_old_values(
            command.entity,
            command.id,
            command.expected_version,
            old_values,
        );
        self.emit_event(event).map_err(DataServiceError::Runtime)?;
        Ok(affected)
    }

    fn emit_event(&self, event: RawAuditEvent) -> Result<(), RuntimeError> {
        self.data_service.metadata.context.send_event(event)
    }

    #[allow(dead_code)]
    pub(super) async fn execute_prepared_insert(
        &self,
        command: InsertCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        self.execute_prepared_insert_with_comment(command, Vec::new())
            .await
    }

    pub(super) async fn execute_prepared_insert_with_comment(
        &self,
        mut command: InsertCommand,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) -> Result<u64, DataServiceError<E::Error>> {
        command.trace_chain = trace_chain.clone();
        let affected = self.data_service.insert(&command).await?;
        let mut event = RawAuditEvent::created(command.entity, command.values);
        event.trace_chain = trace_chain;
        self.emit_event(event).map_err(DataServiceError::Runtime)?;
        Ok(affected)
    }

    pub(super) async fn execute_prepared_batch_insert(
        &self,
        command: teaql_core::BatchInsertCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        if command.batch_values.is_empty() {
            return Ok(0);
        }
        let affected = self.data_service.batch_insert(&command).await?;

        let entity = command.entity.clone();
        for (i, values) in command.batch_values.into_iter().enumerate() {
            let mut event = RawAuditEvent::created(entity.clone(), values);
            if i < command.trace_chains.len() {
                event.trace_chain = command.trace_chains[i].clone();
            }
            self.emit_event(event).map_err(DataServiceError::Runtime)?;
        }
        Ok(affected)
    }

    #[allow(dead_code)]
    pub(super) async fn execute_prepared_update(
        &self,
        command: UpdateCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        self.execute_prepared_update_with_comment(command, Vec::new())
            .await
    }

    pub(super) async fn execute_prepared_update_with_comment(
        &self,
        mut command: UpdateCommand,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) -> Result<u64, DataServiceError<E::Error>> {
        command.trace_chain = trace_chain.clone();

        let mut old_values = command.old_values.clone();
        let needs_fetch = match &old_values {
            Some(snapshot) => !command.values.keys().all(|k| snapshot.contains_key(k)),
            None => true,
        };
        if needs_fetch {
            old_values =
                self.fetch_current_event_row(&command.entity, &command.id, trace_chain.clone())?;
        }

        let affected = self.data_service.update(&command).await?;
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
        self.emit_event(event).map_err(DataServiceError::Runtime)?;
        Ok(affected)
    }

    pub(super) async fn execute_prepared_batch_update(
        &self,
        command: teaql_core::BatchUpdateCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        if command.batch_values.is_empty() {
            return Ok(0);
        }
        let affected = self.data_service.batch_update(&command).await?;

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
            self.emit_event(event).map_err(DataServiceError::Runtime)?;
        }
        Ok(affected)
    }

    fn fetch_current_event_row(
        &self,
        _entity: &str,
        _id: &Value,
        _trace_chain: Vec<teaql_core::TraceNode>,
    ) -> Result<Option<Record>, DataServiceError<E::Error>> {
        // PER THE USER: "我们不需要在审计的时候去抓旧的值"
        // Avoid DB queries during event emission. We rely on in-memory `original_values`.
        Ok(None)
    }

    pub fn scoped_data_service(&self, entity: String) -> EntityDataService<'a, E> {
        EntityDataService {
            entity,
            data_service: ContextDataService {
                metadata: UserContextMetadata {
                    context: self.data_service.metadata.context,
                },
                executor: self.data_service.executor,
            },
            trace_context: Vec::new(),
        }
    }
}
