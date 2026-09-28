use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use teaql_data_service::{ExecutionMetadata, ExecutionObserver, SqlExecutionOutcome};

/// Success metadata is returned to the existing runtime path. Exceptional exits
/// use the observer, including dropping an in-flight future. No error text is logged.
pub(crate) struct StatementDiagnostic<'a> {
    metadata: Option<ExecutionMetadata>,
    observer: Option<ExecutionObserver<'a>>,
}

impl<'a> StatementDiagnostic<'a> {
    pub(crate) fn new(
        metadata: Option<ExecutionMetadata>,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Self {
        Self { metadata, observer }
    }

    pub(crate) fn success(&mut self) -> Option<ExecutionMetadata> {
        self.metadata.take().map(|mut metadata| {
            metadata.ended_at = SystemTime::now();
            metadata.sql_log.execution_outcome = Some(SqlExecutionOutcome::Success);
            metadata
        })
    }

    pub(crate) fn fail(&mut self) {
        self.notify(SqlExecutionOutcome::Failure);
    }

    fn notify(&mut self, outcome: SqlExecutionOutcome) {
        if let Some(mut metadata) = self.metadata.take() {
            metadata.ended_at = SystemTime::now();
            metadata.sql_log.execution_outcome = Some(outcome);
            if let Some(observer) = &self.observer {
                let _ =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(metadata)));
            }
        }
    }
}

impl Drop for StatementDiagnostic<'_> {
    fn drop(&mut self) {
        self.notify(if std::thread::panicking() {
            SqlExecutionOutcome::Failure
        } else {
            SqlExecutionOutcome::Cancelled
        });
    }
}

/// Retain only this invocation's executed statements until its result is known.
/// On failure/Drop, publish in execution order. On success, the existing result
/// path owns diagnostics, so discard this fallback journal without duplication.
pub(crate) struct FailureJournal<'a> {
    observer: Option<ExecutionObserver<'a>>,
    entries: Option<Arc<Mutex<Vec<ExecutionMetadata>>>>,
}
impl<'a> FailureJournal<'a> {
    pub(crate) fn new(observer: Option<ExecutionObserver<'a>>) -> Self {
        let entries = observer.as_ref().map(|_| Arc::new(Mutex::new(Vec::new())));
        Self { observer, entries }
    }
    pub(crate) fn recorder(&self) -> Option<ExecutionObserver<'static>> {
        self.entries.as_ref().map(|entries| {
            let entries = entries.clone();
            Arc::new(move |metadata| {
                entries
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(metadata)
            }) as ExecutionObserver<'static>
        })
    }
    pub(crate) fn disarm(&mut self) {
        self.observer = None;
    }
}
impl Drop for FailureJournal<'_> {
    fn drop(&mut self) {
        if let (Some(observer), Some(entries)) = (self.observer.take(), self.entries.take()) {
            let entries = std::mem::take(&mut *entries.lock().unwrap_or_else(|p| p.into_inner()));
            for metadata in entries {
                let _ =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(metadata)));
            }
        }
    }
}
