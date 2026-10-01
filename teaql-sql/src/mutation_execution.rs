use super::*;
use crate::diagnostic_execution::{FailureJournal, StatementDiagnostic};
use teaql_data_service::{ExecutionObserver, SqlExecutionOutcome, SqlIntentRedactions};

/// No context/global mutable state: one adapter and one fallback journal per call.
struct ObservedTransport<'a, T> {
    inner: &'a T,
    backend: String,
    operation: DataServiceOperation,
    trace: Vec<teaql_core::TraceNode>,
    comment: Option<String>,
    observer: Option<ExecutionObserver<'static>>,
    intent_redactions: std::sync::Mutex<SqlIntentRedactions>,
    target_id: Option<teaql_core::Value>,
}
impl<T> ObservedTransport<'_, T> {
    fn metadata(
        &self,
        compiled: &CompiledQuery,
        operation: DataServiceOperation,
    ) -> ExecutionMetadata {
        let now = SystemTime::now();
        let mut sql_log = compiled.log_context.clone();
        if operation == DataServiceOperation::Query {
            sql_log.intent_redactions = self
                .intent_redactions
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
        }
        if let Some(id) = &self.target_id {
            sql_log.intent_redactions.capture_target_id(id);
        }
        ExecutionMetadata {
            statements: vec![],
            sql_log,
            backend: self.backend.clone(),
            operation,
            started_at: now,
            ended_at: now,
            affected_rows: None,
            result_count: None,
            trace_chain: self.trace.clone(),
            comment: self.comment.clone(),
            backend_request_id: None,
            parameterized_query: Some(compiled.sql.clone()),
            params: compiled.params.clone(),
            debug_query: None,
        }
    }
    fn emit(&self, metadata: ExecutionMetadata) {
        if let Some(observer) = &self.observer {
            observer(metadata);
        }
    }
}
impl<T: SqlTransport> SqlTransport for ObservedTransport<'_, T> {
    type Error = T::Error;
    async fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, Self::Error> {
        if self.observer.is_none() {
            return self.inner.fetch_all_compact_sql(query).await;
        }
        let mut diagnostic = StatementDiagnostic::new(
            Some(self.metadata(query, DataServiceOperation::Query)),
            self.observer.clone(),
        );
        let result = self.inner.fetch_all_compact_sql(query).await;
        let rows = result.inspect_err(|_| {
            diagnostic.fail();
        })?;
        let mut metadata = diagnostic.success().expect("observed readback");
        metadata.result_count = Some(rows.len());
        self.emit(metadata);
        Ok(rows)
    }
    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        if self.observer.is_none() {
            return self.inner.execute_sql(query).await;
        }
        *self
            .intent_redactions
            .lock()
            .unwrap_or_else(|p| p.into_inner()) =
            SqlIntentRedactions::from_bindings(&query.log_context, &query.params, &query.sql);
        let mut diagnostic = StatementDiagnostic::new(
            Some(self.metadata(query, self.operation)),
            self.observer.clone(),
        );
        let result = self.inner.execute_sql(query).await;
        let affected = result.inspect_err(|_| {
            diagnostic.fail();
        })?;
        let mut metadata = diagnostic.success().expect("observed mutation");
        metadata.affected_rows = Some(affected);
        self.emit(metadata);
        Ok(affected)
    }
}

pub(super) async fn execute<D, T, F>(
    dialect: &D,
    transport: &T,
    lookup: &F,
    cache: &RwLock<Vec<CachedSelectPlan>>,
    request: MutationRequest,
    readback: bool,
    observer: Option<ExecutionObserver<'_>>,
) -> Result<MutationResult, SqlExecutorError<T::Error>>
where
    D: SqlDialect + Sync,
    T: SqlTransport,
    F: Fn(&str) -> Option<Arc<EntityDescriptor>> + Sync,
{
    let mut journal = FailureJournal::new(observer);
    let result = execute_tree(
        dialect,
        transport,
        lookup,
        cache,
        request,
        readback,
        journal.recorder(),
    )
    .await;
    if result.is_ok() {
        journal.disarm();
    }
    result
}

async fn execute_tree<D, T, F>(
    dialect: &D,
    transport: &T,
    lookup: &F,
    cache: &RwLock<Vec<CachedSelectPlan>>,
    request: MutationRequest,
    readback: bool,
    observer: Option<ExecutionObserver<'static>>,
) -> Result<MutationResult, SqlExecutorError<T::Error>>
where
    D: SqlDialect + Sync,
    T: SqlTransport,
    F: Fn(&str) -> Option<Arc<EntityDescriptor>> + Sync,
{
    let entity_name = match &request.command {
        teaql_data_service::MutationCommand::Insert(cmd) => &cmd.entity,
        teaql_data_service::MutationCommand::Update(cmd) => &cmd.entity,
        teaql_data_service::MutationCommand::Delete(cmd) => &cmd.entity,
        teaql_data_service::MutationCommand::Recover(cmd) => &cmd.entity,
        teaql_data_service::MutationCommand::Batch(mutations) => {
            let start = SystemTime::now();
            let mut statements = Vec::new();
            let mut total = 0;
            let mut sql = Vec::new();
            let mut params = Vec::new();
            for child in mutations {
                let result = Box::pin(execute_tree(
                    dialect,
                    transport,
                    lookup,
                    cache,
                    child.clone(),
                    readback,
                    observer.clone(),
                ))
                .await?;
                total += result.affected_rows;
                if let Some(text) = &result.metadata.parameterized_query {
                    sql.push(text.clone());
                }
                params.extend(result.metadata.params.clone());
                statements.push(result.metadata);
            }
            return Ok(MutationResult {
                affected_rows: total,
                generated_values: GeneratedValues::default(),
                persisted_snapshot: None,
                metadata: ExecutionMetadata {
                    statements,
                    sql_log: Default::default(),
                    backend: format!("{:?}", dialect.kind()).to_ascii_lowercase(),
                    operation: DataServiceOperation::Batch,
                    started_at: start,
                    ended_at: SystemTime::now(),
                    affected_rows: Some(total),
                    result_count: None,
                    trace_chain: vec![],
                    comment: None,
                    backend_request_id: None,
                    parameterized_query: (!sql.is_empty()).then(|| sql.join("; ")),
                    params,
                    debug_query: None,
                },
            });
        }
    };
    let entity = lookup(entity_name).ok_or_else(|| {
        SqlExecutorError::Compile(SqlCompileError::UnknownEntity(entity_name.clone()))
    })?;
    let (compiled, operation, persisted_id) = match &request.command {
        teaql_data_service::MutationCommand::Insert(cmd) => (
            dialect.compile_insert(&entity, cmd),
            DataServiceOperation::Insert,
            cmd.values.get("id").cloned(),
        ),
        teaql_data_service::MutationCommand::Update(cmd) => (
            dialect.compile_update(&entity, cmd),
            DataServiceOperation::Update,
            Some(cmd.id.clone()),
        ),
        teaql_data_service::MutationCommand::Delete(cmd) => (
            dialect.compile_delete(&entity, cmd),
            DataServiceOperation::Delete,
            cmd.soft_delete.then(|| cmd.id.clone()),
        ),
        teaql_data_service::MutationCommand::Recover(cmd) => (
            dialect.compile_recover(&entity, cmd),
            DataServiceOperation::Recover,
            Some(cmd.id.clone()),
        ),
        teaql_data_service::MutationCommand::Batch(_) => unreachable!(),
    };
    let compiled = compiled.map_err(SqlExecutorError::Compile)?;
    let target_id = match &request.command {
        teaql_data_service::MutationCommand::Insert(cmd) => cmd.values.get("id").cloned(),
        teaql_data_service::MutationCommand::Update(cmd) => Some(cmd.id.clone()),
        teaql_data_service::MutationCommand::Delete(cmd) => Some(cmd.id.clone()),
        teaql_data_service::MutationCommand::Recover(cmd) => Some(cmd.id.clone()),
        teaql_data_service::MutationCommand::Batch(_) => None,
    };
    let observed = ObservedTransport {
        inner: transport,
        backend: format!("{:?}", dialect.kind()).to_ascii_lowercase(),
        operation,
        trace: request.trace_chain().to_vec(),
        comment: Some(request.comment().to_owned()),
        observer,
        intent_redactions: Default::default(),
        target_id,
    };
    let mut metadata = observed.metadata(&compiled, operation);
    let affected_rows = observed
        .execute_sql(&compiled)
        .await
        .map_err(SqlExecutorError::Transport)?;
    metadata.ended_at = SystemTime::now();
    metadata.affected_rows = Some(affected_rows);
    metadata.sql_log.execution_outcome = Some(SqlExecutionOutcome::Success);
    let persisted_snapshot = if readback && affected_rows == 1 {
        if let Some(id) = persisted_id {
            let query = SelectQuery::new(entity_name.clone()).filter(Expr::eq("id", id));
            let compiled_readback = compile_select_with_cache(dialect, cache, &entity, &query)
                .map_err(SqlExecutorError::Compile)?;
            let mut rows = observed
                .fetch_all_compact_sql(&compiled_readback)
                .await
                .map_err(SqlExecutorError::Transport)?;
            if rows.len() != 1 {
                return Err(SqlExecutorError::PersistedRecord(format!(
                    "persisted {entity_name} record could not be read back"
                )));
            }
            rows.pop().map(|row| EntitySnapshot::from(row.into_map()))
        } else {
            None
        }
    } else {
        None
    };
    Ok(MutationResult {
        affected_rows,
        generated_values: GeneratedValues::default(),
        persisted_snapshot,
        metadata,
    })
}

pub(super) async fn guarded<D: SqlDialect + Sync, T: SqlTransport>(
    dialect: &D,
    transport: &T,
    entity: &EntityDescriptor,
    cache: &RwLock<Vec<CachedSelectPlan>>,
    request: GuardedMutationRequest,
    observer: Option<ExecutionObserver<'_>>,
) -> Result<MutationResult, SqlExecutorError<T::Error>> {
    let operation = match &request.mutation.command {
        teaql_data_service::MutationCommand::Update(_) => DataServiceOperation::Update,
        teaql_data_service::MutationCommand::Delete(_) => DataServiceOperation::Delete,
        teaql_data_service::MutationCommand::Recover(_) => DataServiceOperation::Recover,
        _ => unreachable!("validated guarded request"),
    };
    let mut journal = FailureJournal::new(observer);
    let observed = ObservedTransport {
        inner: transport,
        backend: format!("{:?}", dialect.kind()).to_ascii_lowercase(),
        operation,
        trace: request.mutation.trace_chain().to_vec(),
        comment: Some(request.mutation.comment().to_owned()),
        observer: journal.recorder(),
        intent_redactions: Default::default(),
        target_id: match &request.mutation.command {
            teaql_data_service::MutationCommand::Update(cmd) => Some(cmd.id.clone()),
            teaql_data_service::MutationCommand::Delete(cmd) => Some(cmd.id.clone()),
            teaql_data_service::MutationCommand::Recover(cmd) => Some(cmd.id.clone()),
            _ => None,
        },
    };
    let mut result = execute_guarded_mutation(dialect, &observed, entity, cache, request).await;
    if let Ok(result) = &mut result {
        result.metadata.sql_log.execution_outcome = Some(SqlExecutionOutcome::Success);
        journal.disarm();
    }
    result
}
