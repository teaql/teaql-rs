use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::sync::Arc;
use std::time::{Duration, Instant};

use teaql_http_reqwest::{HttpServiceProfile, ReqwestHttpRequestService};
use teaql_runtime::{HttpCall, HttpIntentKind, HttpMethod, HttpRetryMode, UserContext};

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
                    let mut first = String::new();
                    reader.read_line(&mut first).unwrap();
                    if served == 0 {
                        assert!(first.starts_with("GET /api/orders/1?"));
                    } else {
                        assert!(first.starts_with("POST /api/orders/1?"));
                    }
                    first.clear();
                    let mut content_length = 0;
                    while reader.read_line(&mut first).unwrap() > 0 && first != "\r\n" {
                        if first.to_ascii_lowercase().starts_with("content-length:") {
                            content_length = first[15..].trim().parse::<usize>().unwrap();
                        }
                        first.clear();
                    }
                    let mut incoming_body = vec![0; content_length];
                    reader.read_exact(&mut incoming_body).unwrap();
                    let body = b"{\"status\":\"submitted\"}";
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .unwrap();
                    stream.write_all(body).unwrap();
                    served += 1;
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
    let context = UserContext::new().with_http_request_service(service);
    let call = HttpCall::get(
        "orders",
        "orders/1?token=PRIVATE_QUERY_CANARY",
        "load order status",
        "render order details",
    )?;
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
    println!("HTTP_RESILIENCE_PASS status=200 attempts=1 requests=2");
    Ok(())
}
