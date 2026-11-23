use crate::{
    OpenTelemetryRuntimeTelemetry, RuntimeOperation, RuntimeTelemetry,
    RuntimeTelemetryPropagationContext, RuntimeTelemetryScope,
};
use opentelemetry::{
    KeyValue, global::BoxedTracer, logs::LoggerProvider, metrics::MeterProvider,
    trace::TracerProvider,
};
use opentelemetry_sdk::{
    Resource,
    logs::{BatchLogProcessor, SdkLoggerProvider},
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::{BatchSpanProcessor, Sampler, SdkTracerProvider},
};
use std::sync::Arc;
use std::time::Duration;
mod export;
mod handoff;
mod health;
mod queue;
use export::{Exporter, Transport};
use handoff::{Handoff, Scope};
use health::LossReason;
use health::{ExportHealth, Signal};
use queue::{LogQueue, Quota, SpanQueue};

struct Providers {
    traces: SdkTracerProvider,
    metrics: SdkMeterProvider,
    logs: SdkLoggerProvider,
    _health_instruments: Vec<opentelemetry::metrics::ObservableCounter<u64>>,
}
pub(crate) struct StandardTelemetry {
    delegate: OpenTelemetryRuntimeTelemetry,
    providers: Arc<Providers>,
    handoff: Arc<Handoff>,
    health: Arc<ExportHealth>,
    span_quota: Arc<Quota>,
    log_quota: Arc<Quota>,
}

pub(crate) fn build(service: &str, endpoint: &str) -> Result<Arc<StandardTelemetry>, &'static str> {
    build_with_capacity(service, endpoint, 2048, 128)
}

fn build_with_capacity(
    service: &str,
    endpoint: &str,
    capacity: usize,
    batch: usize,
) -> Result<Arc<StandardTelemetry>, &'static str> {
    // Use official protobuf transformations; own transport avoids SDK callbacks
    // that can log raw HTTP bodies or collector partial-success messages.
    let transport = Transport::new(endpoint)?;
    let health = Arc::new(ExportHealth::default());
    let span_quota = Quota::new(capacity, Signal::Traces, health.clone());
    let log_quota = Quota::new(capacity, Signal::Logs, health.clone());
    let span_exporter = Exporter::new(transport.clone(), health.clone(), Signal::Traces)
        .with_quota(span_quota.clone());
    let log_exporter = Exporter::new(transport.clone(), health.clone(), Signal::Logs)
        .with_quota(log_quota.clone());
    let metric_exporter = Exporter::new(transport, health.clone(), Signal::Metrics);
    let resource = Resource::builder_empty()
        .with_service_name(service.to_owned())
        .with_attribute(KeyValue::new("teaql.runtime.language", "rust"))
        .build();
    let span_processor = BatchSpanProcessor::builder(span_exporter)
        .with_batch_config(
            opentelemetry_sdk::trace::BatchConfigBuilder::default()
                .with_max_queue_size(capacity)
                .with_max_export_batch_size(batch)
                .with_scheduled_delay(Duration::from_secs(1))
                .build(),
        )
        .build();
    let log_processor = BatchLogProcessor::builder(log_exporter)
        .with_batch_config(
            opentelemetry_sdk::logs::BatchConfigBuilder::default()
                .with_max_queue_size(capacity)
                .with_max_export_batch_size(batch)
                .with_scheduled_delay(Duration::from_secs(1))
                .build(),
        )
        .build();
    let traces = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_resource(resource.clone())
        .with_span_processor(SpanQueue::new(span_processor, span_quota.clone()))
        .build();
    let logs = SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_log_processor(LogQueue::new(log_processor, log_quota.clone()))
        .build();
    let metrics = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(
            PeriodicReader::builder(metric_exporter)
                .with_interval(Duration::from_secs(10))
                .build(),
        )
        .build();
    let meter = metrics.meter("io.teaql.runtime");
    let health_instruments = health.instruments(&meter);
    let delegate = OpenTelemetryRuntimeTelemetry::new(
        BoxedTracer::new(Box::new(traces.tracer("io.teaql.runtime"))),
        meter,
    )
    .with_logger(logs.logger("io.teaql.runtime"))
    .with_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());
    Ok(Arc::new(StandardTelemetry {
        delegate,
        providers: Arc::new(Providers {
            traces,
            metrics,
            logs,
            _health_instruments: health_instruments,
        }),
        handoff: Arc::new(Handoff::default()),
        health,
        span_quota,
        log_quota,
    }))
}

impl StandardTelemetry {
    pub(crate) fn is_closed(&self) -> bool {
        self.handoff.is_closed()
    }
    fn lifecycle(&self, shutdown: bool) {
        if shutdown {
            if !self.handoff.close() {
                return;
            }
            self.span_quota.close();
            self.log_quota.close();
        } else if self.is_closed() {
            return;
        }
        let (sender, receiver) = std::sync::mpsc::channel();
        // SDK providers have different timeout guarantees. Bound the caller independently
        // and execute on ordinary worker threads, including when called from Tokio.
        for signal in ["traces", "logs", "metrics"] {
            let providers = self.providers.clone();
            let sender = sender.clone();
            let _ = std::thread::Builder::new()
                .name(format!("teaql-telemetry-{signal}"))
                .spawn(move || {
                    let result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            match (signal, shutdown) {
                                ("traces", true) => providers
                                    .traces
                                    .shutdown_with_timeout(Duration::from_secs(5)),
                                ("traces", false) => providers.traces.force_flush(),
                                ("logs", true) => {
                                    providers.logs.shutdown_with_timeout(Duration::from_secs(5))
                                }
                                ("logs", false) => providers.logs.force_flush(),
                                (_, true) => providers
                                    .metrics
                                    .shutdown_with_timeout(Duration::from_secs(5)),
                                (_, false) => providers.metrics.force_flush(),
                            }
                        }))
                        .is_ok_and(|result| result.is_ok());
                    let _ = sender.send(result);
                });
        }
        drop(sender);
        let deadline = std::time::Instant::now() + Duration::from_secs(6);
        let mut complete = true;
        for _ in 0..3 {
            complete &= receiver
                .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                .unwrap_or(false);
        }
        if !complete {
            crate::telemetry_startup::notice(
                "TeaQL telemetry: owned lifecycle/export incomplete; business outcome unchanged.",
            );
        }
    }
}
impl RuntimeTelemetry for StandardTelemetry {
    fn inject_context(&self, carrier: &mut std::collections::BTreeMap<String, String>) {
        self.delegate.inject_context(carrier);
    }
    fn records_sql_diagnostics(&self) -> bool {
        self.delegate.records_sql_diagnostics()
    }
    fn record_diagnostic(&self, event: crate::RuntimeDiagnosticEvent) {
        let closed = self
            .handoff
            .closed
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if *closed {
            self.health.dropped(Signal::Logs, 1, LossReason::Shutdown);
        } else {
            self.delegate.record_diagnostic(event);
        }
    }
    fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
        let closed = self
            .handoff
            .closed
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let inner = if *closed {
            crate::NoopRuntimeTelemetry.start(operation)
        } else {
            self.delegate.start(operation)
        };
        Box::new(Scope {
            inner: Some(inner),
            handoff: self.handoff.clone(),
            health: self.health.clone(),
        })
    }
    fn extract_context(
        &self,
        carrier: &std::collections::BTreeMap<String, String>,
    ) -> Box<dyn RuntimeTelemetryPropagationContext> {
        self.delegate.extract_context(carrier)
    }
    fn flush(&self) {
        self.lifecycle(false);
    }
    fn shutdown(&self) {
        self.lifecycle(true);
    }
}
impl Drop for StandardTelemetry {
    fn drop(&mut self) {
        self.lifecycle(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    #[test]
    fn completed_and_late_handoffs_are_accounted_after_owned_shutdown() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let pipeline = build("shutdown-accounting", &endpoint).unwrap();
        let mut earlier = pipeline.start(RuntimeOperation::new("query", "earlier.query"));
        let correlation = earlier.correlation().unwrap();
        pipeline.shutdown();
        earlier.success(BTreeMap::new());
        earlier.failure("duplicate");
        let mut later = pipeline.start(RuntimeOperation::new("query", "later.query"));
        let mut value = 0;
        later.with_context(&mut || value = 42);
        later.success(BTreeMap::new());
        assert_eq!(value, 42);
        assert!(later.correlation().is_none());
        pipeline.record_diagnostic(crate::RuntimeDiagnosticEvent {
            correlation,
            attributes: BTreeMap::new(),
        });
        assert_eq!(
            pipeline.health.loss(Signal::Traces, LossReason::Shutdown),
            2
        );
        assert_eq!(pipeline.health.loss(Signal::Logs, LossReason::Shutdown), 3);
        assert_eq!(
            pipeline
                .health
                .loss(Signal::Traces, LossReason::QueueOverflow),
            0
        );
        pipeline.shutdown();
        assert_eq!(
            pipeline.health.loss(Signal::Traces, LossReason::Shutdown),
            2
        );
    }
}
