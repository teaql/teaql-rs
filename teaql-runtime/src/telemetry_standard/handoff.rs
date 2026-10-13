//! Serialize telemetry-only completion with shutdown, never business callbacks.
use super::health::{ExportHealth, LossReason, Signal};
use crate::{RuntimeAttributeValue, RuntimeTelemetryScope};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
pub(super) struct Handoff {
    pub(super) closed: Mutex<bool>,
}
impl Handoff {
    pub(super) fn is_closed(&self) -> bool {
        *self.closed.lock().unwrap_or_else(|p| p.into_inner())
    }
    pub(super) fn close(&self) -> bool {
        let mut closed = self.closed.lock().unwrap_or_else(|p| p.into_inner());
        let first = !*closed;
        *closed = true;
        first
    }
}
pub(super) struct Scope {
    pub(super) inner: Option<Box<dyn RuntimeTelemetryScope>>,
    pub(super) handoff: Arc<Handoff>,
    pub(super) health: Arc<ExportHealth>,
}
impl Scope {
    fn finish(&mut self, terminal: impl FnOnce(&mut dyn RuntimeTelemetryScope)) {
        let Some(mut inner) = self.inner.take() else {
            return;
        };
        let closed = self
            .handoff
            .closed
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if *closed {
            self.health.dropped(Signal::Traces, 1, LossReason::Shutdown);
            self.health.dropped(Signal::Logs, 1, LossReason::Shutdown);
        } else {
            terminal(inner.as_mut());
        }
        drop(inner); // Any final SDK span drop stays on the serialized side.
        drop(closed);
    }
}
impl RuntimeTelemetryScope for Scope {
    fn correlation(
        &self,
    ) -> Option<teaql_data_service::statement_telemetry::SqlExecutionCorrelation> {
        self.inner.as_ref().and_then(|inner| inner.correlation())
    }
    fn with_context(&self, callback: &mut dyn FnMut()) {
        // Work may await IO or itself request shutdown. Never hold the handoff lock.
        if let Some(inner) = &self.inner {
            inner.with_context(callback);
        } else {
            callback();
        }
    }
    fn success(&mut self, attributes: BTreeMap<String, RuntimeAttributeValue>) {
        self.finish(|inner| inner.success(attributes));
    }
    fn failure(&mut self, error: &str) {
        self.finish(|inner| inner.failure(error));
    }
    fn failure_with_attributes(
        &mut self,
        error: &str,
        attributes: BTreeMap<String, RuntimeAttributeValue>,
    ) {
        self.finish(|inner| inner.failure_with_attributes(error, attributes));
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let closed = self
            .handoff
            .closed
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        // An abandoned scope has a span drop, but no invented operation log.
        if *closed {
            self.health.dropped(Signal::Traces, 1, LossReason::Shutdown);
        }
        drop(inner);
        drop(closed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Delegate(Arc<AtomicUsize>);
    impl RuntimeTelemetryScope for Delegate {
        fn success(&mut self, _: BTreeMap<String, RuntimeAttributeValue>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn failure(&mut self, _: &str) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn scope() -> (Scope, Arc<Handoff>, Arc<ExportHealth>, Arc<AtomicUsize>) {
        let handoff = Arc::new(Handoff::default());
        let health = Arc::new(ExportHealth::default());
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Scope {
                inner: Some(Box::new(Delegate(calls.clone()))),
                handoff: handoff.clone(),
                health: health.clone(),
            },
            handoff,
            health,
            calls,
        )
    }
    #[test]
    fn business_callback_can_close_pipeline_and_keeps_its_result() {
        let (mut scope, handoff, health, calls) = scope();
        let mut result = 0;
        scope.with_context(&mut || {
            result = 42;
            assert!(handoff.close());
        });
        scope.success(BTreeMap::new());
        scope.failure("late completion");
        assert_eq!(result, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(health.loss(Signal::Traces, LossReason::Shutdown), 1);
        assert_eq!(health.loss(Signal::Logs, LossReason::Shutdown), 1);
    }
    #[test]
    fn ordinary_completion_is_once_only_and_does_not_invent_loss() {
        let (mut scope, _, health, calls) = scope();
        scope.success(BTreeMap::new());
        scope.failure("duplicate");
        drop(scope);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(health.counts(Signal::Traces).0, 0);
        assert_eq!(health.counts(Signal::Logs).0, 0);
    }
    #[test]
    fn abandoned_closed_scope_counts_only_span_drop_not_an_operation_log() {
        let (scope, handoff, health, _) = scope();
        handoff.close();
        drop(scope);
        assert_eq!(health.loss(Signal::Traces, LossReason::Shutdown), 1);
        assert_eq!(health.counts(Signal::Logs).0, 0);
    }
}
