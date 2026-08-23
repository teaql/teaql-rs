use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use teaql_core::{
    AggregationCacheOptions, DeleteCommand, Entity, Expr, InsertCommand, Record, RecoverCommand,
    RelationAggregate, SelectQuery, SmartList, SortDirection, UpdateCommand, Value,
};

use crate::{
    CheckObjectStatus, ContinuousPageCursor, DataServiceError, EntityDataServiceBehavior,
    MetadataStore, PurposedSelectQuery, RawAuditEvent, RuntimeError, clear_record_status,
    mark_record_status,
};

use super::{
    AggregationCacheBackend, ContextDataService, EntityDataService, InMemoryAggregationCache,
    UserContextMetadata, helpers::*,
};

#[derive(Debug, Clone)]
struct ContinuousPageExecution {
    query_key: String,
    direction: SortDirection,
    page_size: u64,
    original_offset: u64,
    ttl_seconds: u64,
    optimized: bool,
    seek_cursor_id: Option<String>,
}

impl<'a, E> EntityDataService<'a, E>
where
    E: teaql_data_service::QueryExecutor
        + teaql_data_service::MutationExecutor
        + Send
        + Sync
        + 'static,
{
    fn flatten_relation_graph(
        &self,
        entity_name: &str,
        record: &mut Record,
        root: &crate::EntityRoot,
        graph: &mut crate::EntityGraphBuilder,
        installed: &mut BTreeSet<(String, u64)>,
    ) -> Result<(), teaql_core::EntityError> {
        let context = self.data_service.metadata.context;
        let relations = context
            .entity(entity_name)
            .map(|descriptor| descriptor.relations.clone())
            .unwrap_or_default();

        for relation in relations {
            if !context.has_entity_graph_decoder(&relation.target_entity) {
                continue;
            }
            let Some(value) = record.remove(&relation.name) else {
                continue;
            };
            if !relation.many && matches!(value, Value::Null | Value::TypedNull(_)) {
                record.insert(relation.name, value);
                continue;
            }
            let mut child_records = match value {
                Value::Object(child) => vec![child],
                Value::List(values) => values
                    .into_iter()
                    .filter_map(|value| match value {
                        Value::Object(child) => Some(child),
                        _ => None,
                    })
                    .collect(),
                Value::Null | Value::TypedNull(_) => Vec::new(),
                other => {
                    record.insert(relation.name, other);
                    continue;
                }
            };

            for child in &mut child_records {
                self.flatten_relation_graph(
                    &relation.target_entity,
                    child,
                    root,
                    graph,
                    installed,
                )?;
            }

            if relation.many || relation.local_key == "id" {
                let owner_id = record.get("id").and_then(Value::try_u64).ok_or_else(|| {
                    teaql_core::EntityError::new(
                        entity_name,
                        "loaded reverse relation owner is missing its u64 id",
                    )
                })?;
                if relation.many {
                    context.decode_entity_list_into_graph(
                        &relation.target_entity,
                        child_records,
                        root,
                        graph,
                        entity_name,
                        owner_id,
                        &relation.name,
                    )?;
                } else {
                    context.decode_entity_option_into_graph(
                        &relation.target_entity,
                        child_records,
                        root,
                        graph,
                        entity_name,
                        owner_id,
                        &relation.name,
                    )?;
                }
                continue;
            }

            for child in child_records {
                let id = child.get("id").and_then(Value::try_u64).ok_or_else(|| {
                    teaql_core::EntityError::new(
                        &relation.target_entity,
                        "loaded relation is missing its u64 id",
                    )
                })?;
                if installed.insert((relation.target_entity.clone(), id)) {
                    context.decode_entity_into_graph(
                        &relation.target_entity,
                        child,
                        root,
                        graph,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn attach_flat_relation_graph(
        &self,
        entity_name: &str,
        rows: &mut [Record],
    ) -> Result<crate::EntityRoot, teaql_core::EntityError> {
        let root = crate::EntityRoot::default();
        let mut graph = crate::EntityGraphBuilder::default();
        let mut installed = BTreeSet::new();
        for row in rows {
            self.flatten_relation_graph(entity_name, row, &root, &mut graph, &mut installed)?;
        }
        root.freeze_graph(graph).map_err(|_| {
            teaql_core::EntityError::new(entity_name, "identity graph was already frozen")
        })?;
        Ok(root)
    }

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
        self.prepare_select_query_owned(query.clone())
    }

    fn prepare_select_query_owned(
        &self,
        mut query: SelectQuery,
    ) -> Result<SelectQuery, RuntimeError> {
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

    pub(crate) async fn fetch_all_internal(
        &self,
        query: &SelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        let query = self
            .prepare_select_query(query)
            .map_err(DataServiceError::Runtime)?;
        let query = query
            .prepare_for_list()
            .map_err(|message| DataServiceError::Runtime(RuntimeError::Graph(message)))?;
        if query.continuous_page_fetch.is_none()
            && query.object_group_bys.is_empty()
            && query.child_enhancements.is_empty()
            && query.relations.is_empty()
        {
            return self.fetch_prepared_query_owned(query).await;
        }
        self.fetch_prepared_all(&query).await
    }

    async fn fetch_all_owned_internal(
        &self,
        query: SelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        let query = self
            .prepare_select_query_owned(query)
            .map_err(DataServiceError::Runtime)?
            .prepare_for_list()
            .map_err(|message| DataServiceError::Runtime(RuntimeError::Graph(message)))?;
        if query.continuous_page_fetch.is_none()
            && query.object_group_bys.is_empty()
            && query.child_enhancements.is_empty()
            && query.relations.is_empty()
        {
            return self.fetch_prepared_query_owned(query).await;
        }
        self.fetch_prepared_all(&query).await
    }

    async fn prepare_continuous_page(
        &self,
        query: SelectQuery,
    ) -> (SelectQuery, Option<ContinuousPageExecution>) {
        let Some(options) = query.continuous_page_fetch.as_ref() else {
            self.data_service
                .metadata
                .context
                .observe_continuous_page("DISABLED", None);
            return (query, None);
        };
        let Some(slice) = query.slice.as_ref() else {
            self.data_service
                .metadata
                .context
                .observe_continuous_page("OFFSET_FALLBACK:INVALID_SLICE", None);
            return (query, None);
        };
        let Some(page_size) = slice.limit else {
            self.data_service
                .metadata
                .context
                .observe_continuous_page("OFFSET_FALLBACK:INVALID_SLICE", None);
            return (query, None);
        };
        if query.partition_by.is_some()
            || !query.aggregates.is_empty()
            || !query.group_by.is_empty()
        {
            self.data_service
                .metadata
                .context
                .observe_continuous_page("OFFSET_FALLBACK:UNSUPPORTED_QUERY_SHAPE", None);
            return (query, None);
        }
        if query.order_by.len() != 1
            || query.order_by[0].field != "id"
            || query.order_by[0].expr.is_some()
        {
            self.data_service
                .metadata
                .context
                .observe_continuous_page("OFFSET_FALLBACK:ORDER_NOT_SEEKABLE_ID", None);
            return (query, None);
        }
        let direction = query.order_by[0].direction;
        let query_key = self.continuous_page_query_key(&query, &options.namespace);
        let execution = ContinuousPageExecution {
            query_key: query_key.clone(),
            direction,
            page_size,
            original_offset: slice.offset,
            ttl_seconds: options.ttl_seconds,
            optimized: false,
            seek_cursor_id: None,
        };
        if slice.offset == 0 {
            self.data_service
                .metadata
                .context
                .observe_continuous_page("OFFSET_FALLBACK:FIRST_PAGE", None);
            return (query, Some(execution));
        }
        let cursor = match self
            .data_service
            .metadata
            .context
            .continuous_page_cursor_store()
            .get(&query_key, slice.offset)
            .await
        {
            Ok(Some(cursor)) => cursor,
            Ok(None) => {
                self.data_service
                    .metadata
                    .context
                    .observe_continuous_page("OFFSET_FALLBACK:CACHE_MISS", None);
                return (query, Some(execution));
            }
            Err(_) => {
                self.data_service
                    .metadata
                    .context
                    .observe_continuous_page("OFFSET_FALLBACK:STORE_UNAVAILABLE", None);
                return (query, Some(execution));
            }
        };
        if cursor.entity != query.entity
            || cursor.direction != direction
            || cursor.page_size != page_size
            || cursor.next_offset != slice.offset
            || cursor.expires_at <= SystemTime::now()
        {
            self.data_service
                .metadata
                .context
                .observe_continuous_page("OFFSET_FALLBACK:CURSOR_INVALID", None);
            return (query, Some(execution));
        }
        let mut optimized = query;
        optimized.slice.as_mut().expect("validated slice").offset = 0;
        optimized = optimized.and_filter(match direction {
            SortDirection::Asc => Expr::gt("id", cursor.boundary.clone()),
            SortDirection::Desc => Expr::lt("id", cursor.boundary.clone()),
        });
        let seek_cursor_id = cursor.cursor_id;
        self.data_service
            .metadata
            .context
            .observe_continuous_page("CURSOR_SEEK", Some(seek_cursor_id.clone()));
        (
            optimized,
            Some(ContinuousPageExecution {
                optimized: true,
                seek_cursor_id: Some(seek_cursor_id),
                ..execution
            }),
        )
    }

    async fn register_continuous_page(
        &self,
        execution: &Option<ContinuousPageExecution>,
        rows: &[Record],
    ) {
        let Some(execution) = execution else { return };
        if rows.len() as u64 != execution.page_size {
            return;
        }
        let Some(boundary) = rows.last().and_then(|row| row.get("id")).cloned() else {
            return;
        };
        let cursor_id = format!(
            "cpg_{:x}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let cursor = ContinuousPageCursor {
            cursor_id,
            query_key: execution.query_key.clone(),
            entity: self.entity.clone(),
            direction: execution.direction,
            boundary,
            page_size: execution.page_size,
            next_offset: execution.original_offset + rows.len() as u64,
            expires_at: SystemTime::now() + Duration::from_secs(execution.ttl_seconds),
        };
        if self
            .data_service
            .metadata
            .context
            .continuous_page_cursor_store()
            .put(cursor)
            .await
            .is_err()
        {
            self.data_service
                .metadata
                .context
                .observe_continuous_page("OFFSET_FALLBACK:STORE_UNAVAILABLE", None);
        } else if execution.optimized {
            self.data_service
                .metadata
                .context
                .observe_continuous_page("CURSOR_SEEK", execution.seek_cursor_id.clone());
        } else {
            self.data_service
                .metadata
                .context
                .observe_continuous_page("OFFSET_FALLBACK:FIRST_PAGE", None);
        }
    }

    fn continuous_page_query_key(&self, query: &SelectQuery, namespace: &str) -> String {
        let mut normalized = query.clone();
        if let Some(slice) = normalized.slice.as_mut() {
            slice.offset = 0;
        }
        normalized.comment = None;
        normalized.trace_chain.clear();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        namespace.hash(&mut hasher);
        format!("{normalized:?}").hash(&mut hasher);
        self.data_service
            .metadata
            .context
            .user_identifier()
            .hash(&mut hasher);
        format!("teaql:continuous-page:v1:{:016x}", hasher.finish())
    }

    /// Fetch root records from the provider cursor without materializing them.
    /// Relation and aggregate enhancement needs a separate batched protocol and
    /// is rejected here instead of silently returning incomplete entities.
    pub(crate) async fn fetch_stream_internal(
        &self,
        query: &SelectQuery,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures_core::Stream<
                        Item = Result<teaql_data_service::StreamChunk, DataServiceError<E::Error>>,
                    > + '_,
            >,
        >,
        DataServiceError<E::Error>,
    >
    where
        E: teaql_data_service::StreamQueryExecutor,
    {
        let query = self
            .prepare_select_query(query)
            .map_err(DataServiceError::Runtime)?;
        let query = query
            .prepare_for_list()
            .map_err(|message| DataServiceError::Runtime(RuntimeError::Graph(message)))?;

        if !query.relations.is_empty()
            || !query.child_enhancements.is_empty()
            || !query.object_group_bys.is_empty()
        {
            return Err(DataServiceError::Runtime(RuntimeError::Graph(
                "streaming relation or aggregate enhancement is not supported; stream a root query or use execute_for_list"
                    .to_owned(),
            )));
        }

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
            capture_debug_query: self.data_service.metadata.capture_query_debug(),
        };

        let chunks = self.data_service.executor.query_stream(request, chunk_size);
        use futures_util::StreamExt;
        Ok(Box::pin(
            chunks.map(|item| item.map_err(DataServiceError::Executor)),
        ))
    }

    async fn fetch_prepared_all(
        &self,
        query: &SelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        let query = query
            .clone()
            .prepare_for_list()
            .map_err(|message| DataServiceError::Runtime(RuntimeError::Graph(message)))?;
        if query.continuous_page_fetch.is_none()
            && query.object_group_bys.is_empty()
            && query.child_enhancements.is_empty()
            && query.relations.is_empty()
        {
            return self.fetch_prepared_query(&query).await;
        }
        let (execution_query, continuous) = self.prepare_continuous_page(query).await;
        let mut rows = self.fetch_prepared_query(&execution_query).await?;
        self.enhance_object_group_bys_internal(
            &mut rows,
            &execution_query.object_group_bys,
            &execution_query.trace_chain,
        )
        .await?;
        self.enhance_child_queries_internal(
            &mut rows,
            &execution_query.child_enhancements,
            &execution_query.trace_chain,
        )
        .await?;
        self.enhance_query_relations_internal(&mut rows, &execution_query)
            .await?;
        self.register_continuous_page(&continuous, &rows).await;
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
            capture_debug_query: self.data_service.metadata.capture_query_debug(),
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

    async fn fetch_prepared_query_owned(
        &self,
        mut query: SelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        if query
            .aggregation_cache
            .is_some_and(|options| options.enabled)
        {
            return self.fetch_prepared_query(&query).await;
        }
        query.comment = self
            .data_service
            .resolve_final_comment(&query.trace_chain, query.comment.take());
        let trace_chain = std::mem::take(&mut query.trace_chain);
        let request = teaql_data_service::QueryRequest {
            trace_chain,
            comment: query.comment.clone(),
            capture_debug_query: self.data_service.metadata.capture_query_debug(),
            query,
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
        let scope = self.data_service.metadata.context.start_runtime_operation(
            crate::RuntimeOperation::new("cache", format!("{}.aggregation.get", query.entity))
                .attribute("teaql.cache.operation", "get"),
        );
        let result = scope
            .run(async {
                if let Some(rows) = cache.get(&key, options.cache_expired_millis) {
                    return Ok((rows, "hit"));
                }
                let request = teaql_data_service::QueryRequest {
                    query: query.clone(),
                    trace_chain: query.trace_chain.clone(),
                    comment: query.comment.clone(),
                    capture_debug_query: self.data_service.metadata.capture_query_debug(),
                };
                let provider_kind = std::any::type_name::<E>().to_owned();
                let provider_scope = self.data_service.metadata.context.start_runtime_operation(
                    crate::RuntimeOperation::new("provider", format!("{provider_kind}.query"))
                        .attribute("teaql.provider.kind", provider_kind)
                        .attribute("teaql.provider.operation", "query"),
                );
                let provider_result = provider_scope
                    .run(self.data_service.executor.query(request))
                    .await;
                let res = match provider_result {
                    Ok(value) => {
                        provider_scope.success(std::collections::BTreeMap::new());
                        value
                    }
                    Err(error) => {
                        provider_scope.failure("data_service_error");
                        return Err(DataServiceError::Executor(error));
                    }
                };
                self.data_service
                    .metadata
                    .context
                    .record_metadata_log(&res.metadata);
                let rows = res.rows;
                cache.put(key, rows.clone());
                Ok((rows, "miss"))
            })
            .await;
        match result {
            Ok((rows, cache_result)) => {
                scope.success(std::collections::BTreeMap::from([(
                    "teaql.cache.result".to_owned(),
                    crate::RuntimeAttributeValue::from(cache_result),
                )]));
                Ok(rows)
            }
            Err(error) => {
                scope.failure("cache_load_error");
                Err(error)
            }
        }
    }

    pub(crate) async fn fetch_all_with_relation_aggregates_internal(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        let query = self
            .prepare_select_query(query)
            .map_err(DataServiceError::Runtime)?;

        let mut rows = self.fetch_prepared_all(&query).await?;
        self.enhance_relation_aggregates_internal(
            &mut rows,
            relation_aggregates,
            query.aggregation_cache,
            &query.trace_chain,
        )
        .await?;
        Ok(rows)
    }

    pub(crate) async fn fetch_smart_list_internal(
        &self,
        query: &SelectQuery,
    ) -> Result<SmartList<Record>, DataServiceError<E::Error>> {
        let query = self
            .prepare_select_query(query)
            .map_err(DataServiceError::Runtime)?;

        self.data_service.fetch_smart_list(&query).await
    }

    pub(crate) async fn fetch_smart_list_with_relation_aggregates_internal(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<Record>, DataServiceError<E::Error>> {
        self.fetch_all_with_relation_aggregates_internal(query, relation_aggregates)
            .await
            .map(SmartList::from)
    }

    pub(crate) async fn fetch_entities_internal<T>(
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

    pub(crate) async fn fetch_entities_with_relation_aggregates_internal<T>(
        &self,
        query: &SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        let root = crate::EntityRoot::default();
        self.fetch_all_with_relation_aggregates_internal(query, relation_aggregates)
            .await?
            .into_iter()
            .map(|record| {
                let mut entity = T::from_record(record)?;
                entity.on_loaded(&root as &dyn std::any::Any);
                Ok(entity)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(DataServiceError::Entity)
    }

    pub(crate) async fn fetch_enhanced_entities_with_relation_aggregates_internal<T>(
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
        self.fetch_enhanced_entities_with_relation_aggregates_prepared(query, relation_aggregates)
            .await
    }

    async fn fetch_enhanced_entities_with_relation_aggregates_owned_internal<T>(
        &self,
        query: SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        let query = self
            .prepare_select_query_owned(query)
            .map_err(DataServiceError::Runtime)?;
        self.fetch_enhanced_entities_with_relation_aggregates_prepared(query, relation_aggregates)
            .await
    }

    async fn fetch_enhanced_entities_with_relation_aggregates_prepared<T>(
        &self,
        query: SelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        if relation_aggregates.is_empty()
            && query.continuous_page_fetch.is_none()
            && query.object_group_bys.is_empty()
            && query.child_enhancements.is_empty()
            && query.relations.is_empty()
        {
            let query = query
                .prepare_for_list()
                .map_err(|message| DataServiceError::Runtime(RuntimeError::Graph(message)))?;
            let root = crate::EntityRoot::default();
            return self
                .fetch_prepared_query_owned(query)
                .await?
                .into_iter()
                .map(|record| {
                    let mut entity = T::from_record(record)?;
                    entity.on_loaded(&root as &dyn std::any::Any);
                    Ok(entity)
                })
                .collect::<Result<Vec<_>, _>>()
                .map(SmartList::from)
                .map_err(DataServiceError::Entity);
        }

        let mut rows = self.fetch_prepared_all(&query).await?;
        self.enhance_relation_aggregates_internal(
            &mut rows,
            relation_aggregates,
            query.aggregation_cache,
            &query.trace_chain,
        )
        .await?;
        self.enhance_relations_internal(&mut rows).await?;
        let root = self
            .attach_flat_relation_graph(&query.entity, &mut rows)
            .map_err(DataServiceError::Entity)?;
        rows.into_iter()
            .map(|record| {
                let mut entity = T::from_record(record)?;
                entity.on_loaded(&root as &dyn std::any::Any);
                Ok(entity)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(DataServiceError::Entity)
    }

    pub(crate) async fn fetch_enhanced_entities_internal<T>(
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
        self.enhance_relations_internal(&mut rows).await?;
        let root = self
            .attach_flat_relation_graph(&query.entity, &mut rows)
            .map_err(DataServiceError::Entity)?;
        rows.into_iter()
            .map(|record| {
                let mut entity = T::from_record(record)?;
                entity.on_loaded(&root as &dyn std::any::Any);
                Ok(entity)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SmartList::from)
            .map_err(DataServiceError::Entity)
    }

    #[doc(hidden)]
    pub async fn fetch_all(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        self.fetch_all_internal(query.as_query()).await
    }

    #[doc(hidden)]
    pub async fn fetch_all_owned(
        &self,
        query: PurposedSelectQuery,
    ) -> Result<Vec<Record>, DataServiceError<E::Error>> {
        self.fetch_all_owned_internal(query.into_query()).await
    }

    #[doc(hidden)]
    pub async fn fetch_stream(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures_core::Stream<
                        Item = Result<teaql_data_service::StreamChunk, DataServiceError<E::Error>>,
                    > + '_,
            >,
        >,
        DataServiceError<E::Error>,
    >
    where
        E: teaql_data_service::StreamQueryExecutor,
    {
        self.fetch_stream_internal(query.as_query()).await
    }

    #[doc(hidden)]
    pub async fn fetch_smart_list(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<SmartList<Record>, DataServiceError<E::Error>> {
        self.fetch_smart_list_internal(query.as_query()).await
    }

    #[doc(hidden)]
    pub async fn fetch_smart_list_with_relation_aggregates(
        &self,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<Record>, DataServiceError<E::Error>> {
        self.fetch_smart_list_with_relation_aggregates_internal(
            query.as_query(),
            relation_aggregates,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn fetch_entities<T>(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_entities_internal(query.as_query()).await
    }

    #[doc(hidden)]
    pub async fn fetch_enhanced_entities<T>(
        &self,
        query: &PurposedSelectQuery,
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_enhanced_entities_internal(query.as_query())
            .await
    }

    #[doc(hidden)]
    pub async fn fetch_enhanced_entities_with_relation_aggregates<T>(
        &self,
        query: &PurposedSelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_enhanced_entities_with_relation_aggregates_internal(
            query.as_query(),
            relation_aggregates,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn fetch_enhanced_entities_with_relation_aggregates_owned<T>(
        &self,
        query: PurposedSelectQuery,
        relation_aggregates: &[RelationAggregate],
    ) -> Result<SmartList<T>, DataServiceError<E::Error>>
    where
        T: Entity,
    {
        self.fetch_enhanced_entities_with_relation_aggregates_owned_internal(
            query.into_query(),
            relation_aggregates,
        )
        .await
    }

    pub(crate) async fn insert_internal(
        &self,
        command: &InsertCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let command = self
            .prepare_insert_command(command)
            .map_err(DataServiceError::Runtime)?;
        self.execute_prepared_insert_with_comment(command, self.trace_context.clone())
            .await
    }

    pub(crate) async fn update_internal(
        &self,
        command: &UpdateCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        let command = self
            .prepare_update_command(command)
            .map_err(DataServiceError::Runtime)?;
        self.execute_prepared_update_with_comment(command, self.trace_context.clone())
            .await
    }

    pub(crate) async fn delete_internal(
        &self,
        command: &DeleteCommand,
    ) -> Result<u64, DataServiceError<E::Error>> {
        self.delete_scoped_internal(command, self.trace_context.clone())
            .await
    }

    pub(crate) async fn delete_scoped_internal(
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

    pub(crate) async fn recover_internal(
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

    pub(crate) fn scoped_data_service_internal(&self, entity: String) -> EntityDataService<'a, E> {
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
