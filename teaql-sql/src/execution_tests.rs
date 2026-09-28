use super::*;
use std::{
    future::Future,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use teaql_data_service::{ExecutionObserver, SqlExecutionOutcome};

struct Dialect;
impl SqlDialect for Dialect {
    fn kind(&self) -> crate::DatabaseKind {
        crate::DatabaseKind::Sqlite
    }
    fn quote_ident(&self, name: &str) -> String {
        format!("\"{name}\"")
    }
    fn placeholder(&self, _: usize) -> String {
        "?".into()
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Success,
    Failure,
    Pending,
    Panic,
}
struct Transport {
    mode: Mode,
    released: Arc<AtomicBool>,
}
struct Lease(Arc<AtomicBool>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
impl SqlTransport for Transport {
    type Error = std::io::Error;
    async fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, Self::Error> {
        let _lease = Lease(self.released.clone());
        assert_eq!(
            query.params[0],
            Value::from("Riverside"),
            "driver value changed"
        );
        match self.mode {
            Mode::Success => Ok(vec![]),
            Mode::Failure => Err(std::io::Error::other("original driver failure")),
            Mode::Pending => std::future::pending().await,
            Mode::Panic => panic!("controlled driver unwind"),
        }
    }
    async fn execute_sql(&self, _: &CompiledQuery) -> Result<u64, Self::Error> {
        unreachable!()
    }
}
fn input(capture: bool) -> (CompiledQuery, QueryRequest) {
    let entity = EntityDescriptor::new("Customer")
        .property(teaql_core::PropertyDescriptor::new(
            "name",
            teaql_core::DataType::Text,
        ))
        .audit_mask_fields(vec!["name".into()]);
    let query = SelectQuery::new("Customer")
        .filter(Expr::eq("name", "Riverside"))
        .limit(10)
        .comment("bounded diagnostic query");
    let compiled = Dialect.compile_select(&entity, &query).unwrap();
    (
        compiled,
        QueryRequest {
            trace_chain: vec![],
            comment: query.comment.clone(),
            query,
            capture_debug_query: false,
            capture_execution_metadata: capture,
        },
    )
}
type Entries = Arc<Mutex<Vec<ExecutionMetadata>>>;
fn fixture(mode: Mode, panic_sink: bool) -> (Transport, Entries, ExecutionObserver<'static>) {
    let entries = Arc::new(Mutex::new(Vec::new()));
    let released = Arc::new(AtomicBool::new(false));
    let observer_entries = entries.clone();
    let observer_released = released.clone();
    let observer = Arc::new(move |metadata| {
        assert!(
            observer_released.load(Ordering::SeqCst),
            "provider resource held by observer"
        );
        observer_entries.lock().unwrap().push(metadata);
        assert!(!panic_sink, "controlled sink panic");
    });
    (Transport { mode, released }, entries, observer)
}
fn check(entries: &Entries, outcome: SqlExecutionOutcome) {
    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 1);
    let metadata = &entries[0];
    assert_eq!(metadata.sql_log.execution_outcome, Some(outcome));
    assert_eq!(metadata.result_count, None);
    assert_eq!(metadata.affected_rows, None);
    assert_eq!(
        metadata.sql_log.parameter_policies[0],
        teaql_data_service::SqlParameterLogPolicy::Masked
    );
    assert!(
        metadata.debug_query.is_none(),
        "no plaintext rendering before projection"
    );
    assert_eq!(
        metadata.comment.as_deref(),
        Some("bounded diagnostic query")
    );
}

#[tokio::test]
async fn failed_query_releases_resources_and_keeps_original_error() {
    let (transport, entries, observer) = fixture(Mode::Failure, false);
    let (compiled, request) = input(true);
    let error = execute_compiled_query(&Dialect, &transport, compiled, request, Some(observer))
        .await
        .unwrap_err();
    assert!(
        matches!(error, SqlExecutorError::Transport(ref error) if error.to_string() == "original driver failure")
    );
    check(&entries, SqlExecutionOutcome::Failure);
}
#[tokio::test]
async fn observer_panic_cannot_replace_driver_error() {
    let (transport, entries, observer) = fixture(Mode::Failure, true);
    let (compiled, request) = input(true);
    let error = execute_compiled_query(&Dialect, &transport, compiled, request, Some(observer))
        .await
        .unwrap_err();
    assert!(
        matches!(error, SqlExecutorError::Transport(ref error) if error.to_string() == "original driver failure")
    );
    check(&entries, SqlExecutionOutcome::Failure);
}
#[test]
fn dropping_pending_query_is_cancelled_with_unknown_count() {
    let (transport, entries, observer) = fixture(Mode::Pending, false);
    let (compiled, request) = input(true);
    let mut future = Box::pin(execute_compiled_query(
        &Dialect,
        &transport,
        compiled,
        request,
        Some(observer),
    ));
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    drop(future);
    check(&entries, SqlExecutionOutcome::Cancelled);
}
#[tokio::test]
async fn provider_unwind_is_failure_and_releases_resources() {
    use futures_util::FutureExt;
    let (transport, entries, observer) = fixture(Mode::Panic, false);
    let (compiled, request) = input(true);
    let result = std::panic::AssertUnwindSafe(execute_compiled_query(
        &Dialect,
        &transport,
        compiled,
        request,
        Some(observer),
    ))
    .catch_unwind()
    .await;
    assert!(result.is_err());
    check(&entries, SqlExecutionOutcome::Failure);
}
#[tokio::test]
async fn success_returns_metadata_without_duplicate_observer_notification() {
    let (transport, entries, observer) = fixture(Mode::Success, false);
    let (compiled, request) = input(true);
    let result = execute_compiled_query(&Dialect, &transport, compiled, request, Some(observer))
        .await
        .unwrap();
    assert_eq!(result.metadata.result_count, Some(0));
    assert_eq!(
        result.metadata.sql_log.execution_outcome,
        Some(SqlExecutionOutcome::Success)
    );
    assert!(entries.lock().unwrap().is_empty());
}
#[tokio::test]
async fn disabled_capture_does_not_emit_failure_diagnostic() {
    let (transport, entries, observer) = fixture(Mode::Failure, false);
    let (compiled, request) = input(false);
    assert!(
        execute_compiled_query(&Dialect, &transport, compiled, request, Some(observer))
            .await
            .is_err()
    );
    assert!(entries.lock().unwrap().is_empty());
    assert!(transport.released.load(Ordering::SeqCst));
}
