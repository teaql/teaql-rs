use super::*;
use opentelemetry::{
    KeyValue,
    logs::{AnyValue, LogRecord, Logger, LoggerProvider},
    metrics::MeterProvider,
    trace::{Span, Tracer, TracerProvider},
};
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsPartialSuccess, metrics::v1::ExportMetricsPartialSuccess,
    trace::v1::ExportTracePartialSuccess,
};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::SdkTracerProvider,
};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    thread,
    time::{Instant, SystemTime},
};

const CANARY: &str = "TEAQL_REMOTE_RESPONSE_PRIVATE_CANARY";
struct Server {
    endpoint: String,
    worker: thread::JoinHandle<Vec<(String, Vec<u8>)>>,
}
fn read_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream.read_exact(&mut byte).unwrap();
        header.push(byte[0]);
        assert!(header.len() < 16384);
    }
    let header = String::from_utf8(header).unwrap();
    let path = header
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .to_owned();
    let size: usize = header
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().unwrap())
        })
        .unwrap();
    assert!(size < 16 * 1024 * 1024);
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes).unwrap();
    (path, bytes)
}
impl Server {
    fn new(replies: Vec<(u16, Vec<u8>)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let worker = thread::spawn(move || {
            let mut requests = Vec::new();
            for (status, body) in replies {
                let deadline = Instant::now() + Duration::from_secs(8);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(Instant::now() < deadline, "missing owned export request");
                            thread::sleep(Duration::from_millis(2));
                        }
                        Err(error) => panic!("fixture accept failed: {error}"),
                    }
                };
                requests.push(read_request(&mut stream));
                let header = format!(
                    "HTTP/1.1 {status} fixture\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).unwrap();
                let _ = stream.write_all(&body); // Error bodies may be discarded before reading.
            }
            requests
        });
        Self { endpoint, worker }
    }
    fn finish(self) -> Vec<(String, Vec<u8>)> {
        self.worker.join().unwrap()
    }
}
fn partial(signal: Signal, rejected: i64, detail: &str) -> Vec<u8> {
    match signal {
        Signal::Traces => ExportTraceServiceResponse {
            partial_success: Some(ExportTracePartialSuccess {
                rejected_spans: rejected,
                error_message: detail.into(),
            }),
        }
        .encode_to_vec(),
        Signal::Logs => ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: rejected,
                error_message: detail.into(),
            }),
        }
        .encode_to_vec(),
        Signal::Metrics => ExportMetricsServiceResponse {
            partial_success: Some(ExportMetricsPartialSuccess {
                rejected_data_points: rejected,
                error_message: detail.into(),
            }),
        }
        .encode_to_vec(),
    }
}
fn error_is_safe(error: OTelSdkError) {
    assert!(!format!("{error:?} {error}").contains(CANARY));
    assert!(error.to_string().contains("masked"));
}

#[test]
fn official_acknowledgements_count_rejections_without_exposing_remote_text() {
    let health = ExportHealth::default();
    for signal in Signal::ALL {
        assert!(acknowledge(signal, &partial(signal, 2, CANARY), 3, &health).is_ok());
        assert_eq!(health.counts(signal), (0, 2, [1, 0, 0]));
    }
}
#[test]
fn full_acceptance_and_empty_partial_wrapper_are_not_warnings() {
    let health = ExportHealth::default();
    for signal in Signal::ALL {
        for body in [Vec::new(), vec![0x0a, 0x00]] {
            acknowledge(signal, &body, 1, &health).unwrap();
        }
        assert_eq!(health.counts(signal), (0, 0, [0, 0, 0]));
    }
}
#[test]
fn zero_rejection_warning_is_visible_but_does_not_invent_loss() {
    let health = ExportHealth::default();
    for signal in Signal::ALL {
        acknowledge(signal, &partial(signal, 0, CANARY), 3, &health).unwrap();
        assert_eq!(health.counts(signal), (0, 0, [0, 1, 0]));
    }
}
#[test]
fn malformed_negative_and_impossible_rejection_counts_fail_closed_safely() {
    let health = ExportHealth::default();
    for signal in Signal::ALL {
        for body in [
            vec![0xff; 8],
            partial(signal, -1, CANARY),
            partial(signal, 4, CANARY),
        ] {
            error_is_safe(acknowledge(signal, &body, 3, &health).unwrap_err());
        }
        assert_eq!(health.counts(signal), (0, 0, [0, 0, 3]));
    }
}
#[test]
fn official_decoder_bounds_unknown_group_recursion() {
    let mut body = vec![0x7b; 150];
    body.extend(vec![0x7c; 150]);
    let health = ExportHealth::default();
    for signal in Signal::ALL {
        error_is_safe(acknowledge(signal, &body, 1, &health).unwrap_err());
    }
}
#[test]
fn actual_http_errors_are_not_retried_and_failed_batches_are_counted_once() {
    let server = Server::new(vec![(503, CANARY.as_bytes().to_vec()), (200, vec![])]);
    let health = Arc::new(ExportHealth::default());
    let transport = Transport::new(&server.endpoint).unwrap();
    let exporter = Exporter::new(transport, health.clone(), Signal::Traces);
    error_is_safe(exporter.send(vec![], 7, 7).unwrap_err());
    assert_eq!(health.counts(Signal::Traces), (7, 0, [0, 0, 0]));
    exporter.send(vec![], 2, 2).unwrap();
    assert_eq!(health.counts(Signal::Traces), (7, 0, [0, 0, 0]));
    assert_eq!(server.finish().len(), 2);
}
#[test]
fn actual_http_partial_warning_and_invalid_response_have_separate_meanings() {
    let server = Server::new(vec![
        (200, partial(Signal::Logs, 2, CANARY)),
        (200, partial(Signal::Logs, 0, CANARY)),
        (200, vec![0xff; 8]),
        (200, vec![0x0a, 0x00]),
    ]);
    let health = Arc::new(ExportHealth::default());
    let exporter = Exporter::new(
        Transport::new(&server.endpoint).unwrap(),
        health.clone(),
        Signal::Logs,
    );
    exporter.send(vec![], 5, 5).unwrap();
    exporter.send(vec![], 3, 3).unwrap();
    error_is_safe(exporter.send(vec![], 4, 4).unwrap_err());
    exporter.send(vec![], 1, 1).unwrap();
    assert_eq!(health.counts(Signal::Logs), (4, 2, [1, 1, 1]));
    assert_eq!(server.finish().len(), 4);
}
#[test]
fn oversized_http_body_is_rejected_without_forwarding_a_private_body() {
    let server = Server::new(vec![(200, vec![b'X'; RESPONSE_LIMIT as usize + 1])]);
    let health = Arc::new(ExportHealth::default());
    let exporter = Exporter::new(
        Transport::new(&server.endpoint).unwrap(),
        health.clone(),
        Signal::Traces,
    );
    error_is_safe(exporter.send(vec![], 2, 2).unwrap_err());
    assert_eq!(health.counts(Signal::Traces), (2, 0, [0, 0, 1]));
    server.finish();
}
#[test]
fn unknown_length_chunked_http_body_is_still_bounded() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        let header = format!(
            "HTTP/1.1 200 fixture\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:X}\r\n",
            RESPONSE_LIMIT + 1
        );
        stream.write_all(header.as_bytes()).unwrap();
        let _ = stream.write_all(&vec![b'X'; RESPONSE_LIMIT as usize + 1]);
        let _ = stream.write_all(b"\r\n0\r\n\r\n");
    });
    let health = Arc::new(ExportHealth::default());
    let exporter = Exporter::new(
        Transport::new(&endpoint).unwrap(),
        health.clone(),
        Signal::Logs,
    );
    error_is_safe(exporter.send(vec![], 2, 2).unwrap_err());
    assert_eq!(health.counts(Signal::Logs), (2, 0, [0, 0, 1]));
    worker.join().unwrap();
}
#[test]
fn connection_errors_and_post_shutdown_export_do_not_lose_the_business_result() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let health = Arc::new(ExportHealth::default());
    let transport = Transport::new(&endpoint).unwrap();
    let exporter = Exporter::new(transport.clone(), health.clone(), Signal::Traces);
    error_is_safe(exporter.send(vec![], 3, 3).unwrap_err());
    exporter.close().unwrap();
    exporter.close().unwrap();
    assert!(matches!(
        exporter.send(vec![], 2, 2),
        Err(OTelSdkError::AlreadyShutdown)
    ));
    assert_eq!(health.counts(Signal::Traces), (5, 0, [0, 0, 0]));
    assert!(!format!("{exporter:?}").contains(&endpoint));
    transport.close(Signal::Logs);
    transport.close(Signal::Metrics);
    assert!(transport.client.lock().unwrap().is_none());
}
#[test]
fn owned_http_deadline_is_finite_even_if_the_receiver_never_replies() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        thread::sleep(Duration::from_secs(4));
    });
    let health = Arc::new(ExportHealth::default());
    let exporter = Exporter::new(
        Transport::new(&endpoint).unwrap(),
        health.clone(),
        Signal::Traces,
    );
    let started = Instant::now();
    error_is_safe(exporter.send(vec![], 1, 1).unwrap_err());
    assert!(started.elapsed() < Duration::from_secs(6));
    assert_eq!(health.counts(Signal::Traces), (1, 0, [0, 0, 0]));
    worker.join().unwrap();
}
#[test]
fn real_sdk_encoders_keep_resource_scope_and_signal_units() {
    let server = Server::new(vec![
        (200, partial(Signal::Traces, 1, CANARY)),
        (200, partial(Signal::Logs, 1, CANARY)),
        (200, partial(Signal::Metrics, 2, CANARY)),
        (200, vec![]), // SDK shutdown exports a final cumulative snapshot.
    ]);
    let transport = Transport::new(&server.endpoint).unwrap();
    let health = Arc::new(ExportHealth::default());
    let resource = Resource::builder_empty()
        .with_service_name("owned-export-test")
        .build();
    let traces = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_simple_exporter(Exporter::new(
            transport.clone(),
            health.clone(),
            Signal::Traces,
        ))
        .build();
    traces
        .tracer("io.teaql.runtime")
        .start("owned operation")
        .end();
    let logs = SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_simple_exporter(Exporter::new(
            transport.clone(),
            health.clone(),
            Signal::Logs,
        ))
        .build();
    let logger = logs.logger("io.teaql.runtime");
    let mut record = logger.create_log_record();
    record.set_observed_timestamp(SystemTime::now());
    record.set_body(AnyValue::String("masked diagnostic".into()));
    logger.emit(record);
    let metrics = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(
            PeriodicReader::builder(Exporter::new(
                transport.clone(),
                health.clone(),
                Signal::Metrics,
            ))
            .with_interval(Duration::from_secs(3600))
            .build(),
        )
        .build();
    let counter = metrics
        .meter("io.teaql.runtime")
        .u64_counter("test.counter")
        .build();
    counter.add(1, &[KeyValue::new("kind", "a")]);
    counter.add(1, &[KeyValue::new("kind", "b")]);
    metrics.force_flush().unwrap();
    traces.shutdown().unwrap();
    logs.shutdown().unwrap();
    metrics.shutdown().unwrap();
    let requests = server.finish();
    assert_eq!(
        requests.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
        vec!["/v1/traces", "/v1/logs", "/v1/metrics", "/v1/metrics"]
    );
    let spans = ExportTraceServiceRequest::decode(requests[0].1.as_slice()).unwrap();
    assert_eq!(
        spans.resource_spans[0].scope_spans[0]
            .scope
            .as_ref()
            .unwrap()
            .name,
        "io.teaql.runtime"
    );
    let log = ExportLogsServiceRequest::decode(requests[1].1.as_slice()).unwrap();
    assert_eq!(log.resource_logs[0].scope_logs[0].log_records.len(), 1);
    let metric = ExportMetricsServiceRequest::decode(requests[2].1.as_slice()).unwrap();
    assert_eq!(metric.resource_metrics[0].scope_metrics[0].metrics.len(), 1);
    for resource in [
        spans.resource_spans[0].resource.as_ref().unwrap(),
        log.resource_logs[0].resource.as_ref().unwrap(),
        metric.resource_metrics[0].resource.as_ref().unwrap(),
    ] {
        assert!(resource.attributes.iter().any(|kv| kv.key == "service.name"
            && kv.value.as_ref().and_then(|v| v.value.as_ref())
                == Some(
                    &opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "owned-export-test".into()
                    )
                )));
    }
    let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum)) =
        &metric.resource_metrics[0].scope_metrics[0].metrics[0].data
    else {
        panic!("missing official metric data");
    };
    assert_eq!(sum.data_points.len(), 2);
    assert_eq!(health.counts(Signal::Traces), (0, 1, [1, 0, 0]));
    assert_eq!(health.counts(Signal::Logs), (0, 1, [1, 0, 0]));
    assert_eq!(health.counts(Signal::Metrics), (0, 2, [1, 0, 0]));
    assert!(transport.client.lock().unwrap().is_none());
}
#[test]
fn metric_failed_export_counts_metric_data_objects_not_sampling_points() {
    let server = Server::new(vec![(503, CANARY.as_bytes().to_vec())]);
    let health = Arc::new(ExportHealth::default());
    let metrics = SdkMeterProvider::builder()
        .with_reader(
            PeriodicReader::builder(Exporter::new(
                Transport::new(&server.endpoint).unwrap(),
                health.clone(),
                Signal::Metrics,
            ))
            .with_interval(Duration::from_secs(3600))
            .build(),
        )
        .build();
    let counter = metrics.meter("test").u64_counter("test.counter").build();
    counter.add(1, &[KeyValue::new("kind", "a")]);
    counter.add(1, &[KeyValue::new("kind", "b")]);
    metrics.force_flush().unwrap_err();
    assert_eq!(health.counts(Signal::Metrics), (1, 0, [0, 0, 0]));
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    // Shutdown may try a new cumulative snapshot; that is not a retry of this batch.
    metrics.shutdown().ok();
}
