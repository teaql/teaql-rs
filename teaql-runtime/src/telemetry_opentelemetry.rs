use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;

use opentelemetry::global::{BoxedSpan, BoxedTracer};
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::trace::{Span, Status, Tracer};
use opentelemetry::{KeyValue, Value};

use crate::{RuntimeAttributeValue, RuntimeOperation, RuntimeTelemetry, RuntimeTelemetryScope};

pub struct OpenTelemetryRuntimeTelemetry {
    tracer: BoxedTracer,
    duration: Histogram<f64>,
    operations: Counter<u64>,
}

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
        }
    }
}

impl RuntimeTelemetry for OpenTelemetryRuntimeTelemetry {
    fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
        let mut span = self.tracer.start(format!("teaql.{}", operation.family));
        for (key, value) in &operation.attributes {
            span.set_attribute(KeyValue::new(key.clone(), otel_value(value)));
        }
        Box::new(OpenTelemetryScope {
            span: Mutex::new(Some(span)),
            family: operation.family,
            started_at: Instant::now(),
            duration: self.duration.clone(),
            operations: self.operations.clone(),
        })
    }
}

struct OpenTelemetryScope {
    span: Mutex<Option<BoxedSpan>>,
    family: String,
    started_at: Instant,
    duration: Histogram<f64>,
    operations: Counter<u64>,
}

impl OpenTelemetryScope {
    fn finish(&self, outcome: &'static str, action: impl FnOnce(&mut BoxedSpan)) {
        let Ok(mut guard) = self.span.lock() else {
            return;
        };
        let Some(mut span) = guard.take() else { return };
        action(&mut span);
        let dimensions = [
            KeyValue::new("teaql.operation.family", self.family.clone()),
            KeyValue::new("teaql.operation.outcome", outcome),
        ];
        self.duration.record(
            self.started_at.elapsed().as_secs_f64() * 1_000.0,
            &dimensions,
        );
        self.operations.add(1, &dimensions);
        span.end();
    }
}

impl RuntimeTelemetryScope for OpenTelemetryScope {
    fn success(&mut self, attributes: BTreeMap<String, RuntimeAttributeValue>) {
        self.finish("success", |span| {
            for (key, value) in attributes {
                if key == "teaql.result.cardinality" || key == "teaql.cache.result" {
                    span.set_attribute(KeyValue::new(key, otel_value(&value)));
                }
            }
            span.set_status(Status::Ok);
        });
    }

    fn failure(&mut self, error_type: &str) {
        self.finish("failure", |span| {
            span.set_attribute(KeyValue::new("teaql.error.type", error_type.to_owned()));
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
