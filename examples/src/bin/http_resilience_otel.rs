//! Controlled live OTLP probe for the request-owned HTTP business TraceChain.
//! Run only with the standard telemetry feature and a loopback Collector.
use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::sync::Arc;
use std::time::{Duration, Instant};

use teaql_core::{TraceKind, TraceNode};
use teaql_http_reqwest::{HttpServiceProfile, ReqwestHttpRequestService};
use teaql_runtime::{HttpCall, UserContext};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("TEAQL_HTTP_LIVE_OTLP").as_deref() != Ok("true")
        || std::env::var("TEAQL_TELEMETRY_ENABLED").as_deref() != Ok("true")
    {
        return Err("enable the controlled live HTTP OTLP probe explicitly".into());
    }
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?;
    listener.set_nonblocking(true)?;
    let port = listener.local_addr()?.port();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(8);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(error) => panic!("loopback HTTP probe failed: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut first = String::new();
        reader.read_line(&mut first).unwrap();
        assert!(first.starts_with("GET /api/orders/1?"));
        let mut carrier = None;
        first.clear();
        while reader.read_line(&mut first).unwrap() > 0 && first != "\r\n" {
            if first.to_ascii_lowercase().starts_with("traceparent:") {
                carrier = Some(first[12..].trim().to_owned());
            }
            first.clear();
        }
        let carrier = carrier.expect("the active runtime span must propagate");
        assert!(carrier.starts_with("00-") && carrier.len() == 55);
        let response = b"{\"status\":\"submitted\"}";
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            response.len()
        )
        .unwrap();
        stream.write_all(response).unwrap();
        carrier
    });
    let profile = HttpServiceProfile::new(
        "orders",
        url::Url::parse(&format!("http://127.0.0.1:{port}/api/"))?,
    )?;
    let context = UserContext::new()
        .with_http_request_service(Arc::new(ReqwestHttpRequestService::new(vec![profile])?));
    let call = HttpCall::get(
        "orders",
        "orders/1?token=PRIVATE_QUERY_CANARY",
        "load order status",
        "render order details",
    )?
    .with_parent_trace_chain(vec![
        TraceNode::typed(TraceKind::Purpose, "Order", None, "render order details"),
        TraceNode::typed(TraceKind::Comment, "Order", None, "load order graph"),
    ])?;
    let result = context.http_requests()?.execute(&context, &call).await?;
    assert_eq!(result.status(), 200);
    assert_eq!(result.json_value()?["status"], "submitted");
    let carrier = server.join().expect("loopback probe thread failed");
    context.runtime_telemetry().flush();
    context.runtime_telemetry().shutdown();
    println!("HTTP_OTLP_PROBE_PASS traceparent={carrier} lineage=3");
    Ok(())
}
