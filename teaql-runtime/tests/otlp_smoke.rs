#![cfg(feature = "opentelemetry")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::global;
use opentelemetry::logs::LoggerProvider;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::{BatchSpanProcessor, SdkTracerProvider};
use teaql_runtime::{
    OpenTelemetryRuntimeTelemetry, RuntimeAttributeValue, RuntimeOperation, RuntimeTelemetry,
    start_runtime_operation,
};

#[test]
fn exports_query_trace_metric_and_log_through_otlp_http() {
    let Ok(service_name) = std::env::var("TEAQL_OTLP_SERVICE_NAME") else {
        return;
    };
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4318".to_owned());
    let expect_export_failure = std::env::var("TEAQL_EXPECT_EXPORT_FAILURE").as_deref() == Ok("1");
    let run_id = service_name.rsplit('-').next().expect("run id").to_owned();
    let resource = Resource::builder()
        .with_service_name(service_name)
        .with_attributes([
            opentelemetry::KeyValue::new("service.instance.id", run_id.clone()),
            opentelemetry::KeyValue::new("teaql.runtime.language", "rust"),
            opentelemetry::KeyValue::new("teaql.conformance.run_id", run_id),
        ])
        .build();

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(format!("{endpoint}/v1/traces"))
        .with_timeout(Duration::from_secs(1))
        .build()
        .expect("span exporter");
    let span_processor = BatchSpanProcessor::builder(span_exporter)
        .with_batch_config(
            opentelemetry_sdk::trace::BatchConfigBuilder::default()
                .with_max_queue_size(64)
                .with_max_export_batch_size(16)
                .build(),
        )
        .build();
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_span_processor(span_processor)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(format!("{endpoint}/v1/metrics"))
        .with_timeout(Duration::from_secs(1))
        .build()
        .expect("metric exporter");
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_periodic_exporter(metric_exporter)
        .build();
    global::set_meter_provider(meter_provider.clone());

    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(format!("{endpoint}/v1/logs"))
        .with_timeout(Duration::from_secs(1))
        .build()
        .expect("log exporter");
    let log_processor = opentelemetry_sdk::logs::BatchLogProcessor::builder(log_exporter)
        .with_batch_config(
            opentelemetry_sdk::logs::BatchConfigBuilder::default()
                .with_max_queue_size(64)
                .with_max_export_batch_size(16)
                .build(),
        )
        .build();
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_log_processor(log_processor)
        .build();
    let telemetry: Arc<dyn RuntimeTelemetry> = Arc::new(
        OpenTelemetryRuntimeTelemetry::new(
            global::tracer("io.teaql.runtime"),
            global::meter("io.teaql.runtime"),
        )
        .with_logger(logger_provider.logger("io.teaql.runtime")),
    );

    let operations = [
        RuntimeOperation::new("query", "ConformanceProbe.list")
            .attribute("teaql.entity.type", "ConformanceProbe"),
        RuntimeOperation::new("mutation", "ConformanceProbe.update")
            .attribute("teaql.entity.type", "ConformanceProbe")
            .attribute("teaql.mutation.kind", "update"),
        RuntimeOperation::new("relation_load", "ConformanceProbe.children")
            .attribute("teaql.entity.type", "ConformanceProbe")
            .attribute("teaql.relation.name", "children"),
        RuntimeOperation::new("provider", "sqlite.query")
            .attribute("teaql.provider.kind", "sqlite")
            .attribute("teaql.provider.operation", "query"),
        RuntimeOperation::new("cache", "local.get").attribute("teaql.cache.operation", "get"),
        RuntimeOperation::new("tfp", "server.query").attribute("teaql.tfp.role", "server"),
        RuntimeOperation::new("audit", "ConformanceProbe.audit")
            .attribute("teaql.entity.type", "ConformanceProbe")
            .attribute("teaql.mutation.kind", "update")
            .attribute("teaql.audit.changed_field_count", 1_i64),
    ];
    for operation in operations {
        let family = operation.family.clone();
        let mut failed_operation = operation.clone();
        failed_operation.name = format!("{}.failure", operation.name);
        failed_operation.attributes.insert(
            "teaql.operation.name".to_owned(),
            RuntimeAttributeValue::String(failed_operation.name.clone()),
        );
        let scope = start_runtime_operation(
            &telemetry,
            operation.attribute("teaql.entity.id", "must-not-export"),
        );
        let mut completion = BTreeMap::from([(
            "teaql.result.cardinality".to_owned(),
            RuntimeAttributeValue::Integer(1),
        )]);
        if family == "cache" {
            completion.insert(
                "teaql.cache.result".to_owned(),
                RuntimeAttributeValue::String("hit".to_owned()),
            );
        }
        scope.success(completion);
        start_runtime_operation(&telemetry, failed_operation).failure("ConformanceProbeError");
    }

    let trace_flushed = tracer_provider.force_flush().is_ok();
    let metric_flushed = meter_provider.force_flush().is_ok();
    let log_flushed = logger_provider.force_flush().is_ok();
    if expect_export_failure {
        assert!(!(trace_flushed && metric_flushed && log_flushed));
    } else {
        assert!(trace_flushed && metric_flushed && log_flushed);
    }
}
