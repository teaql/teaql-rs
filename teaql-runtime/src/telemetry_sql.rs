use crate::{RuntimeAttributeValue, RuntimeDiagnosticEvent, RuntimeOperation, RuntimeTelemetry};
use std::collections::BTreeMap;
use std::sync::Arc;
use teaql_data_service::statement_telemetry::{
    SqlExecutionCorrelation, SqlStatementTelemetry, SqlStatementTelemetryScope,
};

tokio::task_local! {
    // Poll-scoped ownership, never a shared mutable flag on UserContext.
    static GENERATED_EXECUTION_OWNER: usize;
}

struct StatementFactory(Arc<dyn RuntimeTelemetry>);
struct StatementScope(crate::FailOpenRuntimeTelemetryScope);
impl SqlStatementTelemetry for StatementFactory {
    fn start(
        &self,
        backend: &str,
        operation: teaql_data_service::DataServiceOperation,
    ) -> Box<dyn SqlStatementTelemetryScope> {
        Box::new(StatementScope(crate::start_runtime_operation(
            &self.0,
            RuntimeOperation::new("provider", format!("{backend}.statement"))
                .attribute("teaql.provider.kind", backend)
                .attribute("teaql.provider.execution", "statement")
                .attribute(
                    "teaql.provider.operation",
                    format!("{operation:?}").to_ascii_lowercase(),
                ),
        )))
    }
}
impl SqlStatementTelemetryScope for StatementScope {
    fn correlation(&self) -> Option<SqlExecutionCorrelation> {
        self.0.correlation()
    }
    fn with_context(&self, callback: &mut dyn FnMut()) {
        self.0.with_context(callback);
    }
    fn finish(&self, outcome: teaql_data_service::SqlExecutionOutcome) {
        match outcome {
            teaql_data_service::SqlExecutionOutcome::Success => self.0.success(BTreeMap::new()),
            teaql_data_service::SqlExecutionOutcome::Failure => {
                self.0.failure("sql_execution_failure")
            }
            teaql_data_service::SqlExecutionOutcome::Cancelled => {
                self.0.failure("sql_execution_cancelled")
            }
        }
    }
}

impl crate::UserContext {
    /// Generated execution boundary: one list owns count, facets and relations.
    #[doc(hidden)]
    pub fn observe_generated_list<'a, T: 'a, E: 'a, F>(
        &'a self,
        entity: &'a str,
        future: F,
    ) -> impl std::future::Future<Output = Result<teaql_core::SmartList<T>, E>> + 'a
    where
        F: std::future::Future<Output = Result<teaql_core::SmartList<T>, E>> + 'a,
    {
        if self.runtime_telemetry_is_noop() {
            return futures_util::future::Either::Left(future);
        }
        let future = Box::pin(future);
        futures_util::future::Either::Right(Box::pin(async move {
            let scope = self.start_runtime_operation(
                RuntimeOperation::new("query", format!("{entity}.list"))
                    .attribute("teaql.entity.type", entity),
            );
            let owner = std::ptr::from_ref(self) as usize;
            let result = scope
                .run(GENERATED_EXECUTION_OWNER.scope(owner, self.observe_sql_statements(future)))
                .await;
            match &result {
                Ok(rows) => scope.success(BTreeMap::from([(
                    "teaql.result.cardinality".to_owned(),
                    rows.len().into(),
                )])),
                Err(_) => scope.failure("query_error"),
            }
            result
        }))
    }

    pub(crate) fn sql_statement_factory(&self) -> Option<Arc<dyn SqlStatementTelemetry>> {
        self.records_sql_diagnostics().then(|| {
            Arc::new(StatementFactory(self.runtime_telemetry().clone()))
                as Arc<dyn SqlStatementTelemetry>
        })
    }
    pub(crate) fn observe_query_request<
        'a,
        T: 'a,
        E: 'a,
        F: std::future::Future<Output = Result<T, E>> + 'a,
    >(
        &'a self,
        entity: &'a str,
        future: F,
    ) -> impl std::future::Future<Output = Result<T, E>> + 'a {
        if self.runtime_telemetry_is_noop()
            || GENERATED_EXECUTION_OWNER
                .try_with(|owner| *owner == std::ptr::from_ref(self) as usize)
                .unwrap_or(false)
        {
            return futures_util::future::Either::Left(future);
        }
        // Only an enabled path allocates. No-op callers retain their original future
        // instead of embedding it in several nested async state-machine branches.
        let future = Box::pin(future);
        futures_util::future::Either::Right(Box::pin(async move {
            let scope = self.start_runtime_operation(
                RuntimeOperation::new("query", format!("{entity}.request"))
                    .attribute("teaql.entity.type", entity),
            );
            let result = scope.run(self.observe_sql_statements(future)).await;
            match &result {
                Ok(_) => scope.success(BTreeMap::new()),
                Err(_) => scope.failure("query_error"),
            }
            result
        }))
    }
    pub(crate) fn records_sql_diagnostics(&self) -> bool {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.runtime_telemetry().records_sql_diagnostics()
        }))
        .unwrap_or(false)
    }
    pub(crate) fn observe_sql_statements<'a, F: std::future::Future + 'a>(
        &'a self,
        future: F,
    ) -> impl std::future::Future<Output = F::Output> + 'a {
        let Some(factory) = self.sql_statement_factory() else {
            return futures_util::future::Either::Left(future);
        };
        futures_util::future::Either::Right(Box::pin(
            teaql_data_service::statement_telemetry::with_sql_statement_telemetry(
                factory,
                Box::pin(future),
            ),
        ))
    }
    pub(crate) fn export_sql_diagnostic(&self, entry: &crate::SqlLogEntry) {
        let Some(state) = &entry.log_context.telemetry else {
            return;
        };
        let Some(correlation) = state.correlation.clone() else {
            return;
        };
        let path = |nodes: &[teaql_core::TraceNode]| {
            serde_json::to_string(&nodes.iter().map(|node| serde_json::json!({
                    "kind": if node.kind == teaql_core::TraceKind::AuditReason { "audit_reason".to_owned() } else { format!("{:?}", node.kind).to_ascii_lowercase() }, "name": node.entity_type, "detail": node.comment
        })).collect::<Vec<_>>()).unwrap_or_else(|_| "[]".into())
        };
        let mut attributes = BTreeMap::<String, RuntimeAttributeValue>::from([
            ("teaql.sql.debug".into(), entry.debug_sql.clone().into()),
            (
                "teaql.sql.log_mode".into(),
                if entry.log_context.omission_reason.is_some() {
                    "omitted"
                } else {
                    "masked"
                }
                .into(),
            ),
            (
                "teaql.sql.operation".into(),
                format!("{:?}", entry.operation).to_ascii_lowercase().into(),
            ),
            (
                "teaql.sql.outcome".into(),
                entry
                    .log_context
                    .execution_outcome
                    .map(|o| o.as_str())
                    .unwrap_or("unknown")
                    .into(),
            ),
            (
                "teaql.sql.elapsed_us".into(),
                (entry.elapsed.as_micros().min(i64::MAX as u128) as i64).into(),
            ),
            (
                "teaql.intent.comment".into(),
                entry
                    .comment
                    .clone()
                    .or_else(|| entry.audit_reason.clone())
                    .unwrap_or_default()
                    .into(),
            ),
            (
                "teaql.intent.purpose".into(),
                entry.purpose.clone().unwrap_or_default().into(),
            ),
            ("teaql.trace_chain".into(), path(&entry.trace_chain).into()),
            ("teaql.trace_path".into(), path(&entry.trace_path).into()),
        ]);
        attributes.insert(
            "teaql.mutation_lineage".into(),
            path(&state.mutation_lineage).into(),
        );
        if let Some(count) = entry.result_count {
            attributes.insert("teaql.sql.result_count".into(), count.into());
        }
        if let Some(count) = entry.affected_rows {
            attributes.insert(
                "teaql.sql.affected_rows".into(),
                (count.min(i64::MAX as u64) as i64).into(),
            );
        }
        let event = RuntimeDiagnosticEvent {
            correlation,
            attributes,
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.runtime_telemetry().record_diagnostic(event)
        }));
    }
}

/// A generated cursor restores ownership only during poll/close. No SDK or
/// statement-factory activation survives a yielded row or Pending result.
pub(crate) fn with_generated_execution_context<T>(
    context: &crate::UserContext,
    scope: &crate::FailOpenRuntimeTelemetryScope,
    factory: Option<&Arc<dyn SqlStatementTelemetry>>,
    callback: impl FnOnce() -> T,
) -> T {
    let owner = std::ptr::from_ref(context) as usize;
    let mut callback = Some(callback);
    let mut result = None;
    scope.with_context(&mut || {
        let Some(callback) = callback.take() else {
            return;
        };
        result = Some(
            GENERATED_EXECUTION_OWNER.sync_scope(owner, || match factory {
                Some(factory) => {
                    teaql_data_service::statement_telemetry::with_sql_statement_telemetry_scope(
                        factory.clone(),
                        callback,
                    )
                }
                None => callback(),
            }),
        );
    });
    result.expect("fail-open telemetry must execute the callback once")
}
