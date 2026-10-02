use super::*;
use crate::diagnostic_execution::{FailureJournal, StatementDiagnostic};
use teaql_data_service::{ExecutionObserver, SqlExecutionOutcome, SqlIntentRedactions};

/// A native batch has intent but no entity identity of its own. Keep its
/// validated ancestry outside Context and materialize typed nodes only when
/// the physical item's entity is known. Explicit graph frames remain intact.
struct BatchIntentScope {
    parent: Option<Arc<BatchIntentScope>>,
    intent: teaql_core::MutationIntent,
}

#[derive(Clone)]
struct MutationExecutionScope {
    root_intent: teaql_core::MutationIntent,
    batch: Option<Arc<BatchIntentScope>>,
    redactions: Option<Arc<SqlIntentRedactions>>,
    observer: Option<ExecutionObserver<'static>>,
}

fn statement_trace(
    request: &MutationRequest,
    scope: Option<&Arc<BatchIntentScope>>,
) -> Vec<teaql_core::TraceNode> {
    use teaql_core::{TraceKind, TraceNode};
    let mut trace = request.execution_trace_chain();
    let Some(scope) = scope else { return trace };
    let mut scopes = Vec::new();
    let mut current = Some(scope.as_ref());
    while let Some(node) = current {
        scopes.push(node);
        current = node.parent.as_deref();
    }
    scopes.reverse();
    let source = trace
        .iter()
        .find(|node| node.kind == TraceKind::AuditReason)
        .expect("validated mutation owns an audit reason");
    let mut prefix = scopes
        .iter()
        .map(|scope| {
            TraceNode::typed(
                TraceKind::AuditReason,
                &source.entity_type,
                None,
                scope.intent.comment(),
            )
        })
        .collect::<Vec<_>>();
    // The existing first graph reason and its request intent are one slot, not
    // a second child reason. Retain its identity when the batch repeats it.
    let repeated = if source.comment == prefix[0].comment {
        Some(0)
    } else if source.comment == prefix.last().unwrap().comment {
        Some(prefix.len() - 1)
    } else {
        None
    };
    if let Some(repeated) = repeated {
        let index = trace
            .iter()
            .position(|node| node.kind == TraceKind::AuditReason)
            .unwrap();
        prefix[repeated] = trace.remove(index);
    }
    prefix.extend(trace);
    prefix
}

/// Collect privacy provenance, not SQL or validation results. A statement can
/// mention a future sibling in its inherited root intent; compile/transport
/// failure must still occur in the original execution order. Existing field
/// classification is authoritative, including old values and credential keys.
fn batch_intent_redactions<F>(request: &MutationRequest, lookup: &F) -> SqlIntentRedactions
where
    F: Fn(&str) -> Option<Arc<EntityDescriptor>>,
{
    use teaql_data_service::MutationCommand;
    use teaql_data_service::{SqlLogContext, SqlParameterLogPolicy};
    fn capture_values<'a>(
        redactions: &mut SqlIntentRedactions,
        entity: Option<&EntityDescriptor>,
        values: impl Iterator<Item = (&'a String, &'a Value)>,
    ) {
        let (params, policies): (Vec<_>, Vec<_>) = values
            .map(|(field, value)| {
                (
                    value.clone(),
                    entity
                        .map(|entity| crate::bindings::field_policy(entity, field))
                        .unwrap_or(SqlParameterLogPolicy::Unknown),
                )
            })
            .unzip();
        redactions.extend(&SqlIntentRedactions::from_bindings(
            &SqlLogContext {
                generated_sql: true,
                parameter_policies: policies,
                ..Default::default()
            },
            &params,
            "",
        ));
    }
    let mut redactions = SqlIntentRedactions::default();
    if !matches!(request.command, MutationCommand::Batch(_)) {
        return redactions;
    }
    let mut pending = vec![request];
    while let Some(request) = pending.pop() {
        match &request.command {
            MutationCommand::Batch(children) => pending.extend(children),
            MutationCommand::Insert(command) => {
                let entity = lookup(&command.entity);
                capture_values(&mut redactions, entity.as_deref(), command.values.iter());
                if let Some(id) = command.values.get("id") {
                    redactions.capture_target_id(id);
                }
            }
            MutationCommand::Update(command) => {
                let entity = lookup(&command.entity);
                capture_values(
                    &mut redactions,
                    entity.as_deref(),
                    command
                        .values
                        .iter()
                        .chain(command.old_values.iter().flat_map(|values| values.iter())),
                );
                redactions.capture_target_id(&command.id);
            }
            MutationCommand::Delete(command) => redactions.capture_target_id(&command.id),
            MutationCommand::Recover(command) => redactions.capture_target_id(&command.id),
        }
    }
    redactions
}

/// No context/global mutable state: one adapter and one fallback journal per call.
struct ObservedTransport<'a, T> {
    inner: &'a T,
    backend: String,
    operation: DataServiceOperation,
    trace: Vec<teaql_core::TraceNode>,
    comment: String,
    observer: Option<ExecutionObserver<'static>>,
    intent_redactions: std::sync::Mutex<SqlIntentRedactions>,
    inherited_redactions: Option<Arc<SqlIntentRedactions>>,
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
        if let Some(inherited) = &self.inherited_redactions {
            sql_log.intent_redactions.extend(inherited);
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
            trace_chain: if operation == DataServiceOperation::Query {
                // Readback is derived work, not a fresh caller operation.
                let root = self
                    .trace
                    .first()
                    .map(|node| node.entity_type.as_str())
                    .unwrap_or("unknown");
                let mut trace = vec![
                    teaql_core::TraceNode::typed(
                        teaql_core::TraceKind::Comment,
                        root,
                        None,
                        &self.comment,
                    ),
                    teaql_core::TraceNode::typed(
                        teaql_core::TraceKind::Purpose,
                        root,
                        None,
                        "verify the persisted mutation result",
                    ),
                ];
                trace.extend(
                    self.trace
                        .iter()
                        .filter(|node| {
                            !matches!(
                                node.kind,
                                teaql_core::TraceKind::Comment | teaql_core::TraceKind::Purpose
                            )
                        })
                        .cloned(),
                );
                trace
            } else {
                self.trace.clone()
            },
            comment: Some(self.comment.clone()),
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
    let scope = MutationExecutionScope {
        root_intent: request.intent().clone(),
        batch: None,
        redactions: matches!(
            request.command,
            teaql_data_service::MutationCommand::Batch(_)
        )
        .then(|| Arc::new(batch_intent_redactions(&request, lookup))),
        observer: journal.recorder(),
    };
    let result = execute_tree(dialect, transport, lookup, cache, request, readback, scope).await;
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
    mut scope: MutationExecutionScope,
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
            scope.batch = match scope.batch {
                Some(parent) if parent.intent.comment() == request.comment() => Some(parent),
                parent => Some(Arc::new(BatchIntentScope {
                    parent,
                    intent: request.intent().clone(),
                })),
            };
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
                    scope.clone(),
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
                    comment: Some(scope.root_intent.comment().to_owned()),
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
        trace: statement_trace(&request, scope.batch.as_ref()),
        comment: scope.root_intent.comment().to_owned(),
        observer: scope.observer,
        intent_redactions: Default::default(),
        inherited_redactions: scope.redactions,
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
        trace: request.mutation.execution_trace_chain(),
        comment: request.mutation.comment().to_owned(),
        observer: journal.recorder(),
        intent_redactions: Default::default(),
        inherited_redactions: None,
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
