//! Admission wraps the real SDK queues; exporting entries keep their permits.
use super::health::{ExportHealth, LossReason, Signal};
use opentelemetry::{Context, InstrumentationScope};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    logs::{BatchLogProcessor, LogProcessor, SdkLogRecord},
    trace::{BatchSpanProcessor, Span, SpanData, SpanProcessor},
};
use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct State {
    closed: bool,
    outstanding: usize,
}
pub(super) struct Quota {
    state: Mutex<State>,
    capacity: usize,
    signal: Signal,
    health: Arc<ExportHealth>,
}
impl fmt::Debug for Quota {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TeaQLQueueQuota")
            .field("capacity", &self.capacity)
            .field("signal", &self.signal)
            .finish()
    }
}
impl Quota {
    pub(super) fn new(capacity: usize, signal: Signal, health: Arc<ExportHealth>) -> Arc<Self> {
        assert!(capacity > 0);
        Arc::new(Self {
            state: Mutex::new(State::default()),
            capacity,
            signal,
            health,
        })
    }
    fn admit(&self, push: impl FnOnce()) {
        let failure = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.closed {
                Some(LossReason::Shutdown)
            } else if state.outstanding >= self.capacity {
                Some(LossReason::QueueOverflow)
            } else {
                state.outstanding += 1;
                // SDK queue handoff is try_send, never network IO or business work.
                if catch_unwind(AssertUnwindSafe(push)).is_err() {
                    state.outstanding -= 1;
                    Some(LossReason::HandoffFailure)
                } else {
                    None
                }
            }
        };
        if let Some(reason) = failure {
            self.health.dropped(self.signal, 1, reason);
        }
    }
    pub(super) fn close(&self) {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).closed = true;
    }
    pub(super) fn batch(self: &Arc<Self>, entries: usize) -> BatchPermit {
        BatchPermit {
            quota: self.clone(),
            entries,
        }
    }
    #[cfg(test)]
    fn outstanding(&self) -> usize {
        self.state.lock().unwrap().outstanding
    }
}
pub(super) struct BatchPermit {
    quota: Arc<Quota>,
    entries: usize,
}
impl Drop for BatchPermit {
    fn drop(&mut self) {
        let mut state = self.quota.state.lock().unwrap_or_else(|p| p.into_inner());
        state.outstanding = state.outstanding.saturating_sub(self.entries);
    }
}

#[derive(Debug)]
pub(super) struct SpanQueue {
    inner: BatchSpanProcessor,
    quota: Arc<Quota>,
}
impl SpanQueue {
    pub(super) fn new(inner: BatchSpanProcessor, quota: Arc<Quota>) -> Self {
        Self { inner, quota }
    }
}
impl SpanProcessor for SpanQueue {
    fn on_start(&self, span: &mut Span, context: &Context) {
        self.inner.on_start(span, context);
    }
    fn on_end(&self, span: SpanData) {
        self.quota.admit(|| self.inner.on_end(span));
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.quota.close();
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

#[derive(Debug)]
pub(super) struct LogQueue {
    inner: BatchLogProcessor,
    quota: Arc<Quota>,
}
impl LogQueue {
    pub(super) fn new(inner: BatchLogProcessor, quota: Arc<Quota>) -> Self {
        Self { inner, quota }
    }
}
impl LogProcessor for LogQueue {
    fn emit(&self, record: &mut SdkLogRecord, scope: &InstrumentationScope) {
        self.quota.admit(|| self.inner.emit(record, scope));
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.quota.close();
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

#[cfg(test)]
mod tests;
