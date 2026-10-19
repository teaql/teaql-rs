use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use teaql_core::{TraceKind, TraceNode};
use teaql_http_reqwest::{HttpServiceProfile, ReqwestHttpRequestService};
use teaql_runtime::{
    HttpCall, HttpIntentKind, HttpMethod, HttpRetryMode, NoopRuntimeTelemetry, RuntimeOperation,
    RuntimeTelemetry, RuntimeTelemetryScope, UserContext,
};

const TRACEPARENT: &str = "00-11111111111111111111111111111111-aaaaaaaaaaaaaaaa-01";

struct ExampleTelemetry {
    operations: Arc<Mutex<Vec<RuntimeOperation>>>,
}

impl RuntimeTelemetry for ExampleTelemetry {
    fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
        self.operations.lock().unwrap().push(operation.clone());
        NoopRuntimeTelemetry.start(operation)
    }

    fn inject_context(&self, carrier: &mut BTreeMap<String, String>) {
        carrier.insert("traceparent".to_owned(), TRACEPARENT.to_owned());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?;
    listener.set_nonblocking(true)?;
    let port = listener.local_addr()?.port();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut served = 0;
        while served < 2 {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    // Reqwest may reuse the connection for the POST. Handle both
                    // requests on one socket, or accept another after read timeout.
                    while served < 2 {
                        let mut first = String::new();
                        if !matches!(reader.read_line(&mut first), Ok(n) if n > 0) {
                            break;
                        }
                        if served == 0 {
                            assert!(first.starts_with("GET /api/orders/1?"));
                        } else {
                            assert!(first.starts_with("POST /api/orders/1?"));
                        }
                        first.clear();
                        let mut content_length = 0;
                        let mut traceparent = None;
                        while reader.read_line(&mut first).unwrap() > 0 && first != "\r\n" {
                            if first.to_ascii_lowercase().starts_with("content-length:") {
                                content_length = first[15..].trim().parse::<usize>().unwrap();
                            }
                            if first.to_ascii_lowercase().starts_with("traceparent:") {
                                traceparent = Some(first[12..].trim().to_owned());
                            }
                            first.clear();
                        }
                        assert_eq!(traceparent.as_deref(), Some(TRACEPARENT));
                        let mut incoming_body = vec![0; content_length];
                        reader.read_exact(&mut incoming_body).unwrap();
                        let body = b"{\"status\":\"submitted\"}";
                        write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                        body.len()
                    )
                    .unwrap();
                        stream.write_all(body).unwrap();
                        stream.flush().unwrap();
                        served += 1;
                    }
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(2))
                }
                Err(error) => panic!("loopback HTTP fixture failed: {error}"),
            }
        }
    });
    let profile = HttpServiceProfile::new(
        "orders",
        url::Url::parse(&format!("http://127.0.0.1:{port}/api/"))?,
    )?;
    let service = Arc::new(ReqwestHttpRequestService::new(vec![profile])?);
    let operations = Arc::new(Mutex::new(Vec::new()));
    let context = UserContext::new()
        .with_http_request_service(service)
        .with_runtime_telemetry(Arc::new(ExampleTelemetry {
            operations: operations.clone(),
        }));
    let call = HttpCall::get(
        "orders",
        "orders/1?token=PRIVATE_QUERY_CANARY",
        "load order status",
        "render order details",
    )?;
    let call = call.with_parent_trace_chain(vec![
        TraceNode::typed(TraceKind::Purpose, "Order", None, "render order details"),
        TraceNode::typed(TraceKind::Comment, "Order", None, "load order graph"),
    ])?;
    let response = context.http_requests()?.execute(&context, &call).await?;
    assert_eq!(response.status(), 200);
    assert_eq!(response.attempts(), 1);
    assert_eq!(response.json_value()?["status"], "submitted");
    let write_call = HttpCall::new(
        "orders",
        HttpMethod::Post,
        "orders/1?token=PRIVATE_QUERY_TWO",
        BTreeMap::from([(
            "Authorization".to_owned(),
            "Bearer PRIVATE_HEADER_CANARY".to_owned(),
        )]),
        b"PRIVATE_BODY_CANARY".to_vec(),
        Some("text/plain".to_owned()),
        "notify order service",
        HttpIntentKind::Purpose,
        "confirm delivery",
        HttpRetryMode::None,
    )?;
    let written = context
        .http_requests()?
        .execute(&context, &write_call)
        .await?;
    assert_eq!((written.status(), written.attempts()), (200, 1));
    server.join().expect("fixture thread should complete");
    let observed = operations.lock().unwrap();
    assert_eq!(observed.len(), 2);
    assert!(
        observed
            .iter()
            .all(|operation| operation.family == "http" && operation.name == "orders")
    );
    let diagnostic = format!("{observed:?}");
    assert!(diagnostic.contains("load order status"));
    assert!(diagnostic.contains("notify order service"));
    let first_trace = observed[0].attributes.get("teaql.http.trace_chain");
    assert_eq!(
        first_trace,
        Some(&teaql_runtime::RuntimeAttributeValue::String(
            call.business_trace_projection()
        ))
    );
    assert_eq!(call.business_trace_chain().len(), 3);
    assert!(!diagnostic.contains("PRIVATE_QUERY"));
    assert!(!diagnostic.contains("PRIVATE_HEADER"));
    assert!(!diagnostic.contains("PRIVATE_BODY"));
    println!("HTTP_RESILIENCE_PASS status=200 attempts=1 requests=2 trace=2 lineage=3");
    Ok(())
}
