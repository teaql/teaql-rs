use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use opentelemetry::global::BoxedTracer;
use opentelemetry::logs::{AnyValue, LogRecord, Logger, Severity};
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::{Span, Status, TraceContextExt, Tracer};
use opentelemetry::{global, Context, KeyValue, Value};

use crate::{
    RuntimeAttributeValue, RuntimeOperation, RuntimeTelemetry, RuntimeTelemetryPropagationContext,
    RuntimeTelemetryScope, runtime_error_category,
};

pub struct OpenTelemetryRuntimeTelemetry {
    tracer: BoxedTracer,
    duration: Histogram<f64>,
    operations: Counter<u64>,
    log_emitter: Option<RuntimeLogEmitter>,
    flush: Arc<dyn Fn() + Send + Sync>,
    shutdown: Arc<dyn Fn() + Send + Sync>,
}

type RuntimeLogEmitter = Arc<dyn Fn(&str, &str, &str, f64, Option<&str>) + Send + Sync>;

impl OpenTelemetryRuntimeTelemetry {
    pub fn new(tracer: BoxedTracer, meter: Meter) -> Self {
        Self {
            tracer,
            duration: meter
                .f64_histogram("teaql.runtime.operation.duration")
                .with_description("TeaQL runtime operation duration")
                .with_unit("ms")
                .build(),
            operations: meter
                .u64_counter("teaql.runtime.operation.count")
                .with_description("Completed TeaQL runtime operations")
                .with_unit("{operation}")
                .build(),
            log_emitter: None,
            flush: Arc::new(|| {}),
            shutdown: Arc::new(|| {}),
        }
    }

    pub fn with_lifecycle<F, S>(mut self, flush: F, shutdown: S) -> Self
    where
        F: Fn() + Send + Sync + 'static,
        S: Fn() + Send + Sync + 'static,
    {
        self.flush = Arc::new(flush);
        self.shutdown = Arc::new(shutdown);
        self
    }

    pub fn with_logger<L>(mut self, logger: L) -> Self
    where
        L: Logger + Send + Sync + 'static,
        L::LogRecord: Send,
    {
        self.log_emitter = Some(Arc::new(move |family, name, outcome, duration_ms, error_category| {
            let mut record = logger.create_log_record();
            record.set_severity_number(Severity::Info);
            record.set_severity_text("INFO");
            record.set_body("TeaQL runtime operation completed".into());
            record.add_attributes([
                ("teaql.operation.family", AnyValue::from(family.to_owned())),
                ("teaql.operation.name", AnyValue::from(name.to_owned())),
                (
                    "teaql.operation.outcome",
                    AnyValue::from(outcome.to_owned()),
                ),
                ("teaql.operation.duration_ms", AnyValue::from(duration_ms)),
            ]);
            if let Some(category) = error_category {
                record.add_attribute("teaql.error.category", AnyValue::from(category.to_owned()));
            }
            logger.emit(record);
        }));
        self
    }
}

impl RuntimeTelemetry for OpenTelemetryRuntimeTelemetry {
    fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
        let mut span = self.tracer.start(format!("teaql.{}", operation.family));
        for (key, value) in &operation.attributes {
            span.set_attribute(KeyValue::new(key.clone(), otel_value(value)));
        }
        Box::new(OpenTelemetryScope {
            context: Context::current_with_span(span),
            ended: Mutex::new(false),
            family: operation.family,
            name: operation.name,
            started_at: Instant::now(),
            duration: self.duration.clone(),
            operations: self.operations.clone(),
            log_emitter: self.log_emitter.clone(),
        })
    }

    fn extract_context(
        &self,
        carrier: &BTreeMap<String, String>,
    ) -> Box<dyn RuntimeTelemetryPropagationContext> {
        let context = global::get_text_map_propagator(|propagator| {
            propagator.extract(&CaseInsensitiveCarrier(carrier))
        });
        Box::new(OpenTelemetryPropagationContext { context })
    }

    fn flush(&self) {
        (self.flush)();
    }

    fn shutdown(&self) {
        (self.shutdown)();
    }
}

struct CaseInsensitiveCarrier<'a>(&'a BTreeMap<String, String>);

impl Extractor for CaseInsensitiveCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find_map(|(name, value)| name.eq_ignore_ascii_case(key).then_some(value.as_str()))
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

struct OpenTelemetryPropagationContext {
    context: Context,
}

impl RuntimeTelemetryPropagationContext for OpenTelemetryPropagationContext {
    fn with_context(&self, callback: &mut dyn FnMut()) {
        let _guard = self.context.clone().attach();
        callback();
    }
}

struct OpenTelemetryScope {
    context: Context,
    ended: Mutex<bool>,
    family: String,
    name: String,
    started_at: Instant,
    duration: Histogram<f64>,
    operations: Counter<u64>,
    log_emitter: Option<RuntimeLogEmitter>,
}

impl OpenTelemetryScope {
    fn finish(
        &self,
        outcome: &'static str,
        error_category: Option<&str>,
        action: impl FnOnce(opentelemetry::trace::SpanRef<'_>),
    ) {
        let Ok(mut ended) = self.ended.lock() else {
            return;
        };
        if *ended {
            return;
        }
        *ended = true;
        let _guard = self.context.clone().attach();
        let span = self.context.span();
        action(span);
        let dimensions = [
            KeyValue::new("teaql.operation.family", self.family.clone()),
            KeyValue::new("teaql.operation.outcome", outcome),
        ];
        let duration_ms = self.started_at.elapsed().as_secs_f64() * 1_000.0;
        self.duration.record(duration_ms, &dimensions);
        self.operations.add(1, &dimensions);
        if let Some(log_emitter) = &self.log_emitter {
            log_emitter(&self.family, &self.name, outcome, duration_ms, error_category);
        }
        self.context.span().end();
    }
}

impl RuntimeTelemetryScope for OpenTelemetryScope {
    fn with_context(&self, callback: &mut dyn FnMut()) {
        let _guard = self.context.clone().attach();
        callback();
    }

    fn success(&mut self, attributes: BTreeMap<String, RuntimeAttributeValue>) {
        self.finish("success", None, |mut span| {
            for (key, value) in attributes {
                if key == "teaql.result.cardinality" || key == "teaql.cache.result" {
                    span.set_attribute(KeyValue::new(key, otel_value(&value)));
                }
            }
            span.set_status(Status::Ok);
        });
    }

    fn failure(&mut self, error_type: &str) {
        let category = runtime_error_category(error_type);
        self.finish("failure", Some(category), |mut span| {
            span.set_attribute(KeyValue::new("teaql.error.type", error_type.to_owned()));
            span.set_attribute(KeyValue::new("teaql.error.category", category));
            span.set_status(Status::error("TeaQL operation failed"));
        });
    }
}

fn otel_value(value: &RuntimeAttributeValue) -> Value {
    match value {
        RuntimeAttributeValue::String(value) => Value::String(value.clone().into()),
        RuntimeAttributeValue::Integer(value) => Value::I64(*value),
        RuntimeAttributeValue::Float(value) => Value::F64(*value),
        RuntimeAttributeValue::Boolean(value) => Value::Bool(*value),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use opentelemetry::global;
    use opentelemetry::logs::LoggerProvider;
    use opentelemetry_sdk::logs::{InMemoryLogExporter, SdkLoggerProvider};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    use super::*;
    use crate::start_runtime_operation;

    static GLOBAL_OTEL_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[tokio::test(flavor = "current_thread")]
    async fn preserves_nested_context_across_async_polling() {
        let _global_otel_guard = GLOBAL_OTEL_TEST_LOCK.lock().expect("OTel test lock");
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        let log_exporter = InMemoryLogExporter::default();
        let logger_provider = SdkLoggerProvider::builder()
            .with_simple_exporter(log_exporter.clone())
            .build();
        let telemetry: Arc<dyn RuntimeTelemetry> = Arc::new(
            OpenTelemetryRuntimeTelemetry::new(
                global::tracer("io.teaql.runtime"),
                global::meter("io.teaql.runtime"),
            )
            .with_logger(logger_provider.logger("io.teaql.runtime")),
        );

        let outer =
            start_runtime_operation(&telemetry, RuntimeOperation::new("query", "School.list"));
        outer
            .run(async {
                tokio::task::yield_now().await;
                let inner = start_runtime_operation(
                    &telemetry,
                    RuntimeOperation::new("provider", "sqlite.query"),
                );
                inner.success(BTreeMap::new());
            })
            .await;
        outer.success(BTreeMap::new());
        provider.force_flush().expect("flush spans");

        let spans = exporter.get_finished_spans().expect("finished spans");
        let query = spans
            .iter()
            .find(|span| span.name == "teaql.query")
            .expect("query span");
        let provider_span = spans
            .iter()
            .find(|span| span.name == "teaql.provider")
            .expect("provider span");
        assert_eq!(provider_span.parent_span_id, query.span_context.span_id());
        let logs = log_exporter.get_emitted_logs().expect("emitted logs");
        let query_log = logs
            .iter()
            .find(|log| {
                log.record.attributes_iter().any(|(key, value)| {
                    key.as_str() == "teaql.operation.family"
                        && value == &opentelemetry::logs::AnyValue::String("query".into())
                })
            })
            .expect("query log");
        let log_context = query_log.record.trace_context().expect("log trace context");
        assert_eq!(log_context.trace_id, query.span_context.trace_id());
        assert_eq!(log_context.span_id, query.span_context.span_id());
        assert!(query_log
            .record
            .attributes_iter()
            .all(|(key, _)| key.as_str() != "teaql.entity.id"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn extracts_case_insensitive_w3c_carrier_as_direct_parent() {
        let _global_otel_guard = GLOBAL_OTEL_TEST_LOCK.lock().expect("OTel test lock");
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        global::set_text_map_propagator(TraceContextPropagator::new());
        let telemetry: Arc<dyn RuntimeTelemetry> = Arc::new(OpenTelemetryRuntimeTelemetry::new(
            global::tracer("io.teaql.runtime"),
            global::meter("io.teaql.runtime"),
        ));
        let trace_id = "0af7651916cd43dd8448eb211c80319c";
        let parent_span_id = "b7ad6b7169203331";
        let carrier = BTreeMap::from([(
            "TraceParent".to_owned(),
            format!("00-{trace_id}-{parent_span_id}-01"),
        )]);
        let propagated = crate::extract_runtime_context(&telemetry, &carrier);

        propagated
            .run(async {
                let server = start_runtime_operation(
                    &telemetry,
                    RuntimeOperation::new("tfp", "server.query")
                        .attribute("teaql.tfp.role", "server"),
                );
                server.success(BTreeMap::new());
            })
            .await;
        provider.force_flush().expect("flush spans");

        let spans = exporter.get_finished_spans().expect("finished spans");
        let server = spans
            .iter()
            .find(|span| span.name == "teaql.tfp")
            .expect("server span");
        assert_eq!(server.span_context.trace_id().to_string(), trace_id);
        assert_eq!(server.parent_span_id.to_string(), parent_span_id);
    }

    #[test]
    fn delegates_explicit_application_owned_lifecycle() {
        let flushes = Arc::new(AtomicUsize::new(0));
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let flush_probe = Arc::clone(&flushes);
        let shutdown_probe = Arc::clone(&shutdowns);
        let telemetry = OpenTelemetryRuntimeTelemetry::new(
            global::tracer("io.teaql.runtime.lifecycle"),
            global::meter("io.teaql.runtime.lifecycle"),
        )
        .with_lifecycle(
            move || {
                flush_probe.fetch_add(1, Ordering::SeqCst);
            },
            move || {
                shutdown_probe.fetch_add(1, Ordering::SeqCst);
            },
        );

        telemetry.flush();
        telemetry.shutdown();

        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }
}
