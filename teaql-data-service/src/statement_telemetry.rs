//! SDK-neutral, poll-scoped factory. Identities are carried by individual statements.
use crate::{DataServiceOperation, SqlExecutionOutcome};
use std::cell::RefCell;
use std::future::Future;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlExecutionCorrelation {
    pub trace_id: String,
    pub span_id: String,
}

pub trait SqlStatementTelemetryScope: Send + Sync {
    fn correlation(&self) -> Option<SqlExecutionCorrelation>;
    fn with_context(&self, callback: &mut dyn FnMut());
    fn finish(&self, outcome: SqlExecutionOutcome);
}
pub trait SqlStatementTelemetry: Send + Sync {
    fn start(
        &self,
        backend: &str,
        operation: DataServiceOperation,
    ) -> Box<dyn SqlStatementTelemetryScope>;
}

thread_local! { static CURRENT: RefCell<Option<Arc<dyn SqlStatementTelemetry>>> = RefCell::new(None); }
struct Activation(Option<Arc<dyn SqlStatementTelemetry>>);
impl Drop for Activation {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current.replace(self.0.take());
        });
    }
}

/// Install only while polling this executor invocation, never across an await/yield.
#[doc(hidden)]
pub fn with_sql_statement_telemetry_scope<T>(
    factory: Arc<dyn SqlStatementTelemetry>,
    callback: impl FnOnce() -> T,
) -> T {
    let previous = CURRENT.with(|current| current.replace(Some(factory)));
    let _activation = Activation(previous);
    callback()
}

/// Install only while polling this executor invocation, never across an await/yield.
pub async fn with_sql_statement_telemetry<F: Future>(
    factory: Arc<dyn SqlStatementTelemetry>,
    future: F,
) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|task| {
        with_sql_statement_telemetry_scope(factory.clone(), || future.as_mut().poll(task))
    })
    .await
}

pub fn start_sql_statement(
    backend: &str,
    operation: DataServiceOperation,
) -> Option<Box<dyn SqlStatementTelemetryScope>> {
    let factory = CURRENT.with(|current| current.borrow().clone())?;
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        factory.start(backend, operation)
    }))
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll, Wake, Waker};
    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }
    struct Factory(&'static str);
    struct Scope(&'static str);
    impl SqlStatementTelemetry for Factory {
        fn start(&self, _: &str, _: DataServiceOperation) -> Box<dyn SqlStatementTelemetryScope> {
            Box::new(Scope(self.0))
        }
    }
    impl SqlStatementTelemetryScope for Scope {
        fn correlation(&self) -> Option<SqlExecutionCorrelation> {
            Some(SqlExecutionCorrelation {
                trace_id: self.0.into(),
                span_id: self.0.into(),
            })
        }
        fn with_context(&self, callback: &mut dyn FnMut()) {
            callback();
        }
        fn finish(&self, _: SqlExecutionOutcome) {}
    }
    fn identity() -> Option<String> {
        start_sql_statement("sqlite", DataServiceOperation::Query)
            .and_then(|scope| scope.correlation())
            .map(|c| c.trace_id)
    }

    #[test]
    fn nested_factories_restore_parent_and_leave_no_state_between_polls() {
        let mut suspended = false;
        let inner = std::future::poll_fn(move |_| {
            assert_eq!(identity().as_deref(), Some("inner"));
            if !suspended {
                suspended = true;
                Poll::Pending
            } else {
                Poll::Ready(7)
            }
        });
        let work = with_sql_statement_telemetry(Arc::new(Factory("outer")), async {
            assert_eq!(identity().as_deref(), Some("outer"));
            let value = with_sql_statement_telemetry(Arc::new(Factory("inner")), inner).await;
            assert_eq!(identity().as_deref(), Some("outer"));
            value
        });
        let mut work = std::pin::pin!(work);
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        assert!(work.as_mut().poll(&mut cx).is_pending());
        assert!(identity().is_none());
        assert_eq!(work.as_mut().poll(&mut cx), Poll::Ready(7));
        assert!(identity().is_none());
    }
    #[test]
    fn panic_restores_poll_scoped_factory() {
        let work = with_sql_statement_telemetry(Arc::new(Factory("private")), async {
            panic!("controlled fixture");
        });
        let mut work = std::pin::pin!(work);
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(&mut cx)))
                .is_err()
        );
        assert!(identity().is_none());
    }
}
