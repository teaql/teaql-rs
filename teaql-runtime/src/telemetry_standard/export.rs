//! Official OTLP encoders, with owned finite transport and safe acknowledgements.
//! No SDK HTTP callbacks, global error handlers, retry policy or response-body logs.
use super::health::{ExportHealth, ResponseKind, Signal};
use super::queue::Quota;
use opentelemetry_proto::{
    tonic::collector::{
        logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
        metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
        trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
    },
    transform::{
        common::tonic::ResourceAttributesWithSchema, logs::tonic::group_logs_by_resource_and_scope,
        trace::tonic::group_spans_by_resource_and_scope,
    },
};
use opentelemetry_sdk::{
    Resource,
    error::{OTelSdkError, OTelSdkResult},
    logs::{LogBatch, LogExporter},
    metrics::{Temporality, data::ResourceMetrics, exporter::PushMetricExporter},
    trace::{SpanData, SpanExporter},
};
use prost::Message;
use std::{
    fmt,
    io::Read,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::Duration,
};

const RESPONSE_LIMIT: u64 = 4 * 1024 * 1024;
const EXPORT_TIMEOUT: Duration = Duration::from_secs(3);
fn failure() -> OTelSdkError {
    OTelSdkError::InternalFailure("TeaQL OTLP export failed; remote detail masked".into())
}

pub(super) struct Transport {
    client: Mutex<Option<reqwest::blocking::Client>>,
    endpoint: String,
    closed_signals: AtomicU8,
}
impl Transport {
    pub(super) fn new(endpoint: &str) -> Result<Arc<Self>, &'static str> {
        let client = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(EXPORT_TIMEOUT)
            .connect_timeout(EXPORT_TIMEOUT)
            .build()
            .map_err(|_| "http-client")?;
        Ok(Arc::new(Self {
            client: Mutex::new(Some(client)),
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            closed_signals: AtomicU8::new(0),
        }))
    }
    fn send(
        &self,
        signal: Signal,
        body: Vec<u8>,
        maximum_rejected: usize,
        health: &ExportHealth,
    ) -> OTelSdkResult {
        let client = self
            .client
            .lock()
            .map_err(|_| failure())?
            .as_ref()
            .cloned()
            .ok_or_else(failure)?;
        let response = client
            .post(format!("{}/v1/{}", self.endpoint, signal.name()))
            .header("Content-Type", "application/x-protobuf")
            .body(body)
            .send()
            .map_err(|_| failure())?;
        // Reject errors/redirects before reading a potentially secret body or header.
        if !response.status().is_success() {
            return Err(failure());
        }
        if response
            .content_length()
            .is_some_and(|length| length > RESPONSE_LIMIT)
        {
            health.response(signal, 0, ResponseKind::Invalid);
            return Err(failure());
        }
        let mut bytes = Vec::new();
        response
            .take(RESPONSE_LIMIT + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| failure())?;
        if bytes.len() as u64 > RESPONSE_LIMIT {
            health.response(signal, 0, ResponseKind::Invalid);
            return Err(failure());
        }
        acknowledge(signal, &bytes, maximum_rejected, health)
    }
    fn close(&self, signal: Signal) {
        let bit = 1 << signal as u8;
        if self.closed_signals.fetch_or(bit, Ordering::AcqRel) | bit == 0b111 {
            // SDK shutdown runs on our lifecycle workers. Dispose the blocking
            // client's executor there, not later when UserContext drops in Tokio.
            let client = self.client.lock().unwrap_or_else(|p| p.into_inner()).take();
            drop(client);
        }
    }
}
fn acknowledge(
    signal: Signal,
    bytes: &[u8],
    maximum_rejected: usize,
    health: &ExportHealth,
) -> OTelSdkResult {
    let decoded = match signal {
        Signal::Traces => ExportTraceServiceResponse::decode(bytes).map(|r| {
            r.partial_success
                .map(|p| (p.rejected_spans, !p.error_message.is_empty()))
        }),
        Signal::Logs => ExportLogsServiceResponse::decode(bytes).map(|r| {
            r.partial_success
                .map(|p| (p.rejected_log_records, !p.error_message.is_empty()))
        }),
        Signal::Metrics => ExportMetricsServiceResponse::decode(bytes).map(|r| {
            r.partial_success
                .map(|p| (p.rejected_data_points, !p.error_message.is_empty()))
        }),
    };
    let Ok(partial) = decoded else {
        health.response(signal, 0, ResponseKind::Invalid);
        return Err(failure());
    };
    if let Some((rejected, warning)) = partial {
        if rejected < 0 || rejected as u64 > maximum_rejected as u64 {
            health.response(signal, 0, ResponseKind::Invalid);
            return Err(failure());
        }
        if rejected > 0 {
            health.response(signal, rejected as u64, ResponseKind::PartialSuccess);
        } else if warning {
            health.response(signal, 0, ResponseKind::Warning);
        }
        // An empty 0a00 partial wrapper is full acceptance, not a warning.
    }
    Ok(())
}

pub(super) struct Exporter {
    transport: Arc<Transport>,
    health: Arc<ExportHealth>,
    signal: Signal,
    resource: ResourceAttributesWithSchema,
    closed: AtomicBool,
    quota: Option<Arc<Quota>>,
}
impl fmt::Debug for Exporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TeaQLSafeOtlpExporter")
            .field("signal", &self.signal)
            .finish_non_exhaustive()
    }
}
impl Exporter {
    pub(super) fn new(
        transport: Arc<Transport>,
        health: Arc<ExportHealth>,
        signal: Signal,
    ) -> Self {
        Self {
            transport,
            health,
            signal,
            resource: ResourceAttributesWithSchema::default(),
            closed: AtomicBool::new(false),
            quota: None,
        }
    }
    pub(super) fn with_quota(mut self, quota: Arc<Quota>) -> Self {
        self.quota = Some(quota);
        self
    }
    fn send(&self, body: Vec<u8>, batch_entries: usize, maximum_rejected: usize) -> OTelSdkResult {
        let result = if self.closed.load(Ordering::Acquire) {
            Err(OTelSdkError::AlreadyShutdown)
        } else {
            self.transport
                .send(self.signal, body, maximum_rejected, &self.health)
        };
        if result.is_err() {
            self.health.lost(self.signal, batch_entries);
        }
        result
    }
    fn close(&self) -> OTelSdkResult {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.transport.close(self.signal);
        }
        Ok(())
    }
}
impl SpanExporter for Exporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let entries = batch.len();
        let _permit = self.quota.as_ref().map(|quota| quota.batch(entries));
        let request = ExportTraceServiceRequest {
            resource_spans: group_spans_by_resource_and_scope(batch, &self.resource),
        };
        self.send(request.encode_to_vec(), entries, entries)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.into();
    }
    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        self.close()
    }
    fn shutdown(&self) -> OTelSdkResult {
        self.close()
    }
}
impl LogExporter for Exporter {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let entries = batch.iter().count();
        let _permit = self.quota.as_ref().map(|quota| quota.batch(entries));
        let request = ExportLogsServiceRequest {
            resource_logs: group_logs_by_resource_and_scope(&batch, &self.resource),
        };
        self.send(request.encode_to_vec(), entries, entries)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.into();
    }
    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        self.close()
    }
    fn shutdown(&self) -> OTelSdkResult {
        self.close()
    }
}
impl PushMetricExporter for Exporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let entries = metrics
            .scope_metrics()
            .map(|scope| scope.metrics().count())
            .sum();
        let request: ExportMetricsServiceRequest = metrics.into();
        let points = request
            .resource_metrics
            .iter()
            .flat_map(|resource| &resource.scope_metrics)
            .flat_map(|scope| &scope.metrics)
            .map(|metric| {
                use opentelemetry_proto::tonic::metrics::v1::metric::Data;
                match &metric.data {
                    Some(Data::Gauge(v)) => v.data_points.len(),
                    Some(Data::Sum(v)) => v.data_points.len(),
                    Some(Data::Histogram(v)) => v.data_points.len(),
                    Some(Data::ExponentialHistogram(v)) => v.data_points.len(),
                    Some(Data::Summary(v)) => v.data_points.len(),
                    None => 0,
                }
            })
            .sum();
        self.send(request.encode_to_vec(), entries, points)
    }
    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        self.close()
    }
}

#[cfg(test)]
mod tests;
