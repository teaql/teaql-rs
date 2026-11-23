use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use opentelemetry::global::BoxedTracer;
use opentelemetry::logs::{AnyValue, LogRecord, Logger, Severity};
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::{Span, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue, Value, global};

use crate::{
    RuntimeAttributeValue, RuntimeDiagnosticEvent, RuntimeOperation, RuntimeTelemetry,
    RuntimeTelemetryPropagationContext, RuntimeTelemetryScope, runtime_error_category,
};

pub struct OpenTelemetryRuntimeTelemetry {
    tracer: BoxedTracer,
    duration: Histogram<f64>,
    operations: Counter<u64>,
    log_emitter: Option<RuntimeLogEmitter>,
    diagnostic_emitter: Option<Arc<dyn Fn(RuntimeDiagnosticEvent) + Send + Sync>>,
    flush: Arc<dyn Fn() + Send + Sync>,
    shutdown: Arc<dyn Fn() + Send + Sync>,
    propagator: Option<Arc<dyn TextMapPropagator + Send + Sync>>,
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
            diagnostic_emitter: None,
            flush: Arc::new(|| {}),
            shutdown: Arc::new(|| {}),
            propagator: None,
        }
    }

    pub fn with_propagator(
        mut self,
        propagator: impl TextMapPropagator + Send + Sync + 'static,
    ) -> Self {
        self.propagator = Some(Arc::new(propagator));
        self
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
        let logger = Arc::new(logger);
        let operation_logger = logger.clone();
        self.log_emitter = Some(Arc::new(
            move |family, name, outcome, duration_ms, error_category| {
                let mut record = operation_logger.create_log_record();
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
                    record
                        .add_attribute("teaql.error.category", AnyValue::from(category.to_owned()));
                }
                operation_logger.emit(record);
            },
        ));
        self.diagnostic_emitter = Some(Arc::new(move |event| {
            use opentelemetry::trace::{SpanId, TraceFlags, TraceId};
            let (Ok(trace_id), Ok(span_id)) = (
                TraceId::from_hex(&event.correlation.trace_id),
                SpanId::from_hex(&event.correlation.span_id),
            ) else {
                return;
            };
            if trace_id == TraceId::INVALID || span_id == SpanId::INVALID {
                return;
            }
            let mut record = logger.create_log_record();
            record.set_severity_number(Severity::Info);
            record.set_severity_text("INFO");
            record.set_body("TeaQL SQL execution".into());
            record.set_trace_context(trace_id, span_id, Some(TraceFlags::SAMPLED));
            for (key, value) in event.attributes {
                let value = match value {
                    RuntimeAttributeValue::String(v) => AnyValue::from(v),
                    RuntimeAttributeValue::Integer(v) => AnyValue::from(v),
                    RuntimeAttributeValue::Float(v) => AnyValue::from(v),
                    RuntimeAttributeValue::Boolean(v) => AnyValue::from(v),
                };
                record.add_attribute(key, value);
            }
            logger.emit(record);
        }));
        self
    }
}

impl RuntimeTelemetry for OpenTelemetryRuntimeTelemetry {
    fn inject_context(&self, carrier: &mut BTreeMap<String, String>) {
        if let Some(propagator) = &self.propagator {
            propagator.inject_context(&Context::current(), &mut MapCarrier(carrier));
        } else {
            global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&Context::current(), &mut MapCarrier(carrier));
            });
        }
    }
    fn records_sql_diagnostics(&self) -> bool {
        self.diagnostic_emitter.is_some()
    }
    fn record_diagnostic(&self, event: RuntimeDiagnosticEvent) {
        if let Some(emitter) = &self.diagnostic_emitter {
            emitter(event);
        }
    }
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
        let context = if let Some(propagator) = &self.propagator {
            propagator.extract(&CaseInsensitiveCarrier(carrier))
        } else {
            global::get_text_map_propagator(|propagator| {
                propagator.extract(&CaseInsensitiveCarrier(carrier))
            })
        };
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

struct MapCarrier<'a>(&'a mut BTreeMap<String, String>);

impl Injector for MapCarrier<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

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
            log_emitter(
                &self.family,
                &self.name,
                outcome,
                duration_ms,
                error_category,
            );
        }
        self.context.span().end();
    }
}

impl RuntimeTelemetryScope for OpenTelemetryScope {
    fn correlation(
        &self,
    ) -> Option<teaql_data_service::statement_telemetry::SqlExecutionCorrelation> {
        let span = self.context.span();
        let context = span.span_context();
        context.is_valid().then(|| {
            teaql_data_service::statement_telemetry::SqlExecutionCorrelation {
                trace_id: context.trace_id().to_string(),
                span_id: context.span_id().to_string(),
            }
        })
    }
    fn with_context(&self, callback: &mut dyn FnMut()) {
        let _guard = self.context.clone().attach();
        callback();
    }

    fn success(&mut self, attributes: BTreeMap<String, RuntimeAttributeValue>) {
        self.finish("success", None, |span| {
            for (key, value) in attributes {
                if allowed_completion_attribute(&key) {
                    span.set_attribute(KeyValue::new(key, otel_value(&value)));
                }
            }
            span.set_status(Status::Ok);
        });
    }

    fn failure(&mut self, error_type: &str) {
        self.failure_with_attributes(error_type, BTreeMap::new());
    }

    fn failure_with_attributes(
        &mut self,
        error_type: &str,
        attributes: BTreeMap<String, RuntimeAttributeValue>,
    ) {
        let category = runtime_error_category(error_type);
        self.finish("failure", Some(category), |span| {
            for (key, value) in attributes {
                if allowed_completion_attribute(&key) {
                    span.set_attribute(KeyValue::new(key, otel_value(&value)));
                }
            }
            span.set_attribute(KeyValue::new("teaql.error.type", error_type.to_owned()));
            span.set_attribute(KeyValue::new("teaql.error.category", category));
            span.set_status(Status::error("TeaQL operation failed"));
        });
    }
}

fn allowed_completion_attribute(key: &str) -> bool {
    matches!(
        key,
        "teaql.result.cardinality"
            | "teaql.cache.result"
            | "teaql.http.outcome"
            | "teaql.http.attempts"
            | "teaql.http.elapsed_ms"
    )
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use opentelemetry::global;
    use opentelemetry::logs::LoggerProvider;
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::logs::{InMemoryLogExporter, SdkLoggerProvider};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    #[test]
    fn outbound_carrier_uses_the_active_runtime_span() {
        let exporter = InMemorySpanExporter::default();
        let traces = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let telemetry: Arc<dyn RuntimeTelemetry> = Arc::new(
            OpenTelemetryRuntimeTelemetry::new(
                opentelemetry::global::BoxedTracer::new(Box::new(traces.tracer("http"))),
                global::meter("http"),
            )
            .with_propagator(TraceContextPropagator::new()),
        );
        let call = crate::HttpCall::get(
            "orders",
            "orders/1?token=PRIVATE_QUERY_CANARY",
            "check inventory",
            "render order",
        )
        .unwrap()
        .with_parent_trace_chain(vec![
            teaql_core::TraceNode::typed(
                teaql_core::TraceKind::Purpose,
                "Order",
                None,
                "render order",
            ),
            teaql_core::TraceNode::typed(
                teaql_core::TraceKind::Comment,
                "Order",
                None,
                "load order graph",
            ),
        ])
        .unwrap();
        let scope = start_runtime_operation(
            &telemetry,
            RuntimeOperation::new("http", "orders")
                .attribute("teaql.http.trace_chain", call.business_trace_projection()),
        );
        let mut carrier = BTreeMap::new();
        scope.with_context(&mut || telemetry.inject_context(&mut carrier));
        scope.success(BTreeMap::from([
            (
                "teaql.http.outcome".to_owned(),
                RuntimeAttributeValue::String("success".to_owned()),
            ),
            (
                "teaql.http.attempts".to_owned(),
                RuntimeAttributeValue::Integer(1),
            ),
        ]));
        let parent = carrier
            .get("traceparent")
            .expect("active runtime span should propagate");
        assert!(parent.starts_with("00-"));
        assert_eq!(parent.len(), 55);
        let spans = exporter.get_finished_spans().unwrap();
        let trace = spans[0]
            .attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == "teaql.http.trace_chain")
            .unwrap()
            .value
            .to_string();
        assert!(trace.contains("load order graph"));
        assert!(!trace.contains("PRIVATE_QUERY_CANARY"));
        assert!(
            spans[0]
                .attributes
                .iter()
                .any(|attribute| attribute.key.as_str() == "teaql.http.outcome"
                    && attribute.value.to_string().contains("success"))
        );
        let failed = start_runtime_operation(&telemetry, RuntimeOperation::new("http", "orders"));
        failed.failure_with_attributes(
            "Timeout",
            BTreeMap::from([
                (
                    "teaql.http.outcome".to_owned(),
                    RuntimeAttributeValue::String("TIMEOUT".to_owned()),
                ),
                (
                    "teaql.http.attempts".to_owned(),
                    RuntimeAttributeValue::Integer(1),
                ),
            ]),
        );
        let spans = exporter.get_finished_spans().unwrap();
        assert!(
            spans[1]
                .attributes
                .iter()
                .any(|attribute| attribute.key.as_str() == "teaql.http.outcome"
                    && attribute.value.to_string().contains("TIMEOUT"))
        );
        traces.shutdown().unwrap();
    }

    #[cfg(feature = "standard-telemetry")]
    #[tokio::test(flavor = "current_thread")]
    async fn standard_owned_shutdown_preserves_customer_global_sdk() {
        let _global_otel_guard = GLOBAL_OTEL_TEST_LOCK.lock().await;
        let exporter = InMemorySpanExporter::default();
        let customer = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(customer.clone());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        drop(listener);
        let owned: Arc<dyn RuntimeTelemetry> = tokio::task::spawn_blocking(move || {
            crate::telemetry_standard::build("teaql-owned-coexistence", &endpoint)
        })
        .await
        .unwrap()
        .unwrap();
        let started = std::time::Instant::now();
        let scope =
            start_runtime_operation(&owned, RuntimeOperation::new("query", "owned.request"));
        scope.success(BTreeMap::new());
        owned.flush();
        owned.shutdown();
        owned.shutdown();
        assert!(started.elapsed() < std::time::Duration::from_secs(20));
        global::tracer("customer")
            .start("customer-after-owned-shutdown")
            .end();
        customer.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "customer-after-owned-shutdown");
        customer.shutdown().unwrap();
    }

    #[test]
    fn sql_diagnostics_use_carried_identity_not_an_unrelated_current_span() {
        let spans = InMemorySpanExporter::default();
        let traces = SdkTracerProvider::builder()
            .with_simple_exporter(spans.clone())
            .build();
        let logs = InMemoryLogExporter::default();
        let logger = SdkLoggerProvider::builder()
            .with_simple_exporter(logs.clone())
            .build();
        let telemetry: Arc<dyn RuntimeTelemetry> = Arc::new(
            OpenTelemetryRuntimeTelemetry::new(
                opentelemetry::global::BoxedTracer::new(Box::new(traces.tracer("local"))),
                global::meter("local"),
            )
            .with_logger(logger.logger("local")),
        );
        let original = start_runtime_operation(
            &telemetry,
            RuntimeOperation::new("provider", "sqlite.statement"),
        );
        let correlation = original.correlation().unwrap();
        original.success(BTreeMap::new());
        let unrelated =
            start_runtime_operation(&telemetry, RuntimeOperation::new("query", "other.request"));
        let other = unrelated.correlation().unwrap();
        unrelated.with_context(&mut || {
            telemetry.record_diagnostic(crate::RuntimeDiagnosticEvent {
                correlation: correlation.clone(),
                attributes: BTreeMap::from([("teaql.sql.log_mode".into(), "masked".into())]),
            })
        });
        unrelated.success(BTreeMap::new());
        let emitted = logs.get_emitted_logs().unwrap();
        let event = emitted
            .iter()
            .find(|log| {
                log.record.body()
                    == Some(&opentelemetry::logs::AnyValue::from("TeaQL SQL execution"))
            })
            .unwrap();
        let context = event.record.trace_context().unwrap();
        assert_eq!(context.trace_id.to_string(), correlation.trace_id);
        assert_eq!(context.span_id.to_string(), correlation.span_id);
        assert_ne!(context.span_id.to_string(), other.span_id);
        traces.shutdown().unwrap();
        logger.shutdown().unwrap();
    }

    use super::*;
    use crate::start_runtime_operation;

    static GLOBAL_OTEL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test(flavor = "current_thread")]
    async fn failure_telemetry_exports_type_without_driver_message() {
        struct DriverCanaryError;
        impl std::fmt::Display for DriverCanaryError {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("SQL failed for password=OTEL-FAILURE-CANARY")
            }
        }

        let _global_otel_guard = GLOBAL_OTEL_TEST_LOCK.lock().await;
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

        let _error = DriverCanaryError;
        start_runtime_operation(
            &telemetry,
            RuntimeOperation::new("provider", "sqlite.query"),
        )
        .failure(std::any::type_name::<DriverCanaryError>());
        provider.force_flush().expect("flush spans");

        let spans = exporter.get_finished_spans().expect("finished spans");
        let logs = log_exporter.get_emitted_logs().expect("emitted logs");
        assert_eq!(spans.len(), 1);
        assert_eq!(logs.len(), 1);
        assert!(spans[0].attributes.iter().any(|attribute| {
            attribute.key.as_str() == "teaql.error.category"
                && attribute.value.to_string() == "internal"
        }));
        let exported = format!("{:?} {:?}", spans[0], logs[0]);
        assert!(!exported.contains("OTEL-FAILURE-CANARY"));
        assert!(!exported.contains("password="));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn preserves_nested_context_across_async_polling() {
        let _global_otel_guard = GLOBAL_OTEL_TEST_LOCK.lock().await;
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
        assert!(
            query_log
                .record
                .attributes_iter()
                .all(|(key, _)| key.as_str() != "teaql.entity.id")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn extracts_case_insensitive_w3c_carrier_as_direct_parent() {
        let _global_otel_guard = GLOBAL_OTEL_TEST_LOCK.lock().await;
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
