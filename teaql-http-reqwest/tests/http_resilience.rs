use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use teaql_http_reqwest::{HttpOperationalClock, HttpServiceProfile, ReqwestHttpRequestService};
use teaql_runtime::{
    HttpCall, HttpCancellation, HttpFailure, HttpMethod, HttpRequestService, HttpRetryMode,
    NoopRuntimeTelemetry, RuntimeOperation, RuntimeTelemetry, RuntimeTelemetryScope, UserContext,
};
use url::Url;

struct Fixture {
    port: u16,
    inner: Arc<Inner>,
    server: Option<thread::JoinHandle<()>>,
}

#[derive(Default)]
struct Inner {
    closed: AtomicBool,
    requests: AtomicUsize,
    flaky: AtomicUsize,
    retry_post: AtomicUsize,
    traceparents: Mutex<Vec<String>>,
    retry_bodies: Mutex<Vec<Vec<u8>>>,
}

impl Fixture {
    fn new() -> Self {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let inner = Arc::new(Inner::default());
        let copy = inner.clone();
        let server = thread::spawn(move || {
            while !copy.closed.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let inner = copy.clone();
                        thread::spawn(move || handle(stream, &inner));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            port,
            inner,
            server: Some(server),
        }
    }

    fn profile(&self) -> HttpServiceProfile {
        HttpServiceProfile::new(
            "orders",
            Url::parse(&format!("http://127.0.0.1:{}/api/", self.port)).unwrap(),
        )
        .unwrap()
    }

    fn requests(&self) -> usize {
        self.inner.requests.load(Ordering::SeqCst)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        if let Some(server) = self.server.take() {
            server.join().unwrap();
        }
    }
}

#[tokio::test]
async fn stalled_tls_handshake_obeys_connection_budget() {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match listener.accept() {
                Ok((_stream, _)) => {
                    thread::sleep(Duration::from_millis(1200));
                    break;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(2));
                }
                Err(error) => panic!("TLS handshake fixture failed: {error}"),
            }
        }
    });
    let profile = HttpServiceProfile::new(
        "secure",
        Url::parse(&format!("https://127.0.0.1:{port}/api/")).unwrap(),
    )
    .unwrap()
    .budgets(
        Duration::from_millis(100),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
    .unwrap();
    let service = Arc::new(ReqwestHttpRequestService::new(vec![profile]).unwrap());
    let context = context(service.clone());
    let started = Instant::now();
    let result = service
        .execute(
            &context,
            &HttpCall::get("secure", "health", "check upstream", "assess health").unwrap(),
        )
        .await;
    let elapsed = started.elapsed();
    server.join().unwrap();
    assert_eq!(result, Err(HttpFailure::Timeout));
    assert!(
        elapsed < Duration::from_millis(900),
        "connection phase used request budget: {elapsed:?}"
    );
}

fn handle(mut stream: TcpStream, inner: &Inner) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let reader_stream = match stream.try_clone() {
        Ok(value) => value,
        Err(_) => return,
    };
    let mut reader = BufReader::new(reader_stream);
    let mut first = String::new();
    if reader.read_line(&mut first).is_err() || first.is_empty() {
        return;
    }
    inner.requests.fetch_add(1, Ordering::SeqCst);
    let path = first
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_owned();
    let mut content_length = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() || line == "\r\n" || line.is_empty() {
            break;
        }
        if line.to_ascii_lowercase().starts_with("traceparent:") {
            inner
                .traceparents
                .lock()
                .unwrap()
                .push(line[12..].trim().to_owned());
        }
        if line.to_ascii_lowercase().starts_with("content-length:") {
            content_length = line[15..].trim().parse::<usize>().unwrap();
        }
    }
    if path.contains("/retry-post") {
        let mut body = vec![0; content_length];
        if reader.read_exact(&mut body).is_err() {
            return;
        }
        inner.retry_bodies.lock().unwrap().push(body);
    }
    if path.contains("/close") {
        return;
    }
    if path.ends_with("/slow") {
        thread::sleep(Duration::from_millis(350));
    }
    let status = if path.contains("/redirect") {
        302
    } else if path.contains("/missing") {
        404
    } else if path.contains("/fail")
        || path.contains("/flaky") && inner.flaky.fetch_add(1, Ordering::SeqCst) < 2
        || path.contains("/retry-post") && inner.retry_post.fetch_add(1, Ordering::SeqCst) == 0
    {
        503
    } else {
        200
    };
    let body = if path.contains("/large") {
        vec![b'L'; 4096]
    } else if path.contains("/invalid-utf8") {
        vec![0xc3, 0x28]
    } else if path.contains("/bad-json") {
        b"{bad".to_vec()
    } else {
        b"okay".to_vec()
    };
    let redirect = if status == 302 {
        "Location: https://elsewhere.example/private\r\n"
    } else {
        ""
    };
    let head = format!(
        "HTTP/1.1 {status} Result\r\nContent-Length: {}\r\nConnection: close\r\n{redirect}\r\n",
        body.len()
    );
    if stream.write_all(head.as_bytes()).is_err() {
        return;
    }
    if path.contains("/slowbody") {
        if stream.write_all(&body[..1]).is_err() {
            return;
        }
        let _ = stream.flush();
        thread::sleep(Duration::from_millis(350));
        let _ = stream.write_all(&body[1..]);
    } else {
        let _ = stream.write_all(&body);
    }
    let _ = stream.flush();
}

fn get(path: &str) -> HttpCall {
    HttpCall::get(
        "orders",
        path,
        "load order status",
        "show the current order",
    )
    .unwrap()
}

fn context(service: Arc<ReqwestHttpRequestService>) -> UserContext {
    UserContext::new().with_http_request_service(service)
}

#[tokio::test]
async fn invalid_inputs_and_one_attempt_defaults_never_escape_the_profile() {
    let fixture = Fixture::new();
    let service = Arc::new(ReqwestHttpRequestService::new(vec![fixture.profile()]).unwrap());
    let context = context(service.clone());
    assert_eq!(
        HttpCall::get("orders", "ok", " ", "purpose"),
        Err(HttpFailure::InvalidInput)
    );
    assert_eq!(
        service
            .execute(
                &context,
                &HttpCall::get("other", "ok", "comment", "purpose").unwrap()
            )
            .await,
        Err(HttpFailure::ProfileNotConfigured)
    );
    for path in [
        "../private",
        "%2e%2e/private",
        "/escape",
        "//elsewhere/",
        "http://elsewhere/",
    ] {
        assert_eq!(
            service.execute(&context, &get(path)).await,
            Err(HttpFailure::InvalidInput),
            "{path}"
        );
    }
    assert_eq!(
        HttpCall::new(
            "orders",
            HttpMethod::Get,
            "ok",
            BTreeMap::from([("X-Private".to_owned(), "bad\r\nInjected: yes".to_owned())]),
            Vec::new(),
            None,
            "comment",
            teaql_runtime::HttpIntentKind::Purpose,
            "purpose",
            HttpRetryMode::None
        ),
        Err(HttpFailure::InvalidInput)
    );
    assert_eq!(fixture.requests(), 0);
    let first = service.execute(&context, &get("ok")).await.unwrap();
    assert_eq!(
        (first.status(), first.text().unwrap(), first.attempts()),
        (200, "okay", 1)
    );
    let post =
        HttpCall::post_text("orders", "ok", "PRIVATE-BODY-CANARY", "submit", "send").unwrap();
    assert_eq!(
        service.execute(&context, &post).await.unwrap().attempts(),
        1
    );
    assert_eq!(
        service
            .execute(&context, &get("redirect"))
            .await
            .unwrap()
            .status(),
        302
    );
    assert_eq!(fixture.requests(), 3);
}

#[tokio::test]
async fn retry_is_explicit_and_non_idempotent_transport_failure_is_ambiguous() {
    let fixture = Fixture::new();
    let profile = fixture
        .profile()
        .retry(
            3,
            Duration::from_millis(1),
            BTreeSet::from([HttpMethod::Get]),
        )
        .unwrap();
    let service = Arc::new(ReqwestHttpRequestService::new(vec![profile]).unwrap());
    let context = context(service.clone());
    assert_eq!(
        service
            .execute(&context, &get("flaky"))
            .await
            .unwrap()
            .status(),
        503
    );
    let retried = service
        .execute(
            &context,
            &get("flaky").with_retry_mode(HttpRetryMode::ConfirmedIdempotent),
        )
        .await
        .unwrap();
    assert_eq!((retried.status(), retried.attempts()), (200, 2));
    let before = fixture.requests();
    let post = HttpCall::post_text("orders", "close", "PAYMENT-CANARY", "submit", "pay").unwrap();
    assert_eq!(
        service.execute(&context, &post).await,
        Err(HttpFailure::AmbiguousOutcome)
    );
    assert_eq!(fixture.requests(), before + 1);
}

#[tokio::test]
async fn replayable_post_preserves_bytes_and_backoff_respects_the_whole_call_budget() {
    let fixture = Fixture::new();
    let service = Arc::new(
        ReqwestHttpRequestService::new(vec![
            fixture
                .profile()
                .retry(
                    3,
                    Duration::from_millis(1),
                    BTreeSet::from([HttpMethod::Post]),
                )
                .unwrap(),
        ])
        .unwrap(),
    );
    let request_context = context(service.clone());
    let payload = b"REPLAY-PAYLOAD-CANARY".to_vec();
    let replayable = HttpCall::new(
        "orders",
        HttpMethod::Post,
        "retry-post",
        BTreeMap::new(),
        payload.clone(),
        Some("text/plain".to_owned()),
        "submit idempotent request",
        teaql_runtime::HttpIntentKind::Purpose,
        "verify request replay",
        HttpRetryMode::ConfirmedIdempotent,
    )
    .unwrap();
    let response = service
        .execute(&request_context, &replayable)
        .await
        .unwrap();
    assert_eq!((response.status(), response.attempts()), (200, 2));
    assert_eq!(fixture.requests(), 2);
    assert_eq!(
        *fixture.inner.retry_bodies.lock().unwrap(),
        vec![payload.clone(), payload]
    );

    let clock = Arc::new(FakeClock::new());
    let budgeted = Arc::new(
        ReqwestHttpRequestService::with_clock(
            vec![
                fixture
                    .profile()
                    .budgets(
                        Duration::from_millis(500),
                        Duration::from_millis(500),
                        Duration::from_millis(500),
                    )
                    .unwrap()
                    .retry(3, Duration::from_secs(1), BTreeSet::from([HttpMethod::Get]))
                    .unwrap(),
            ],
            clock,
        )
        .unwrap(),
    );
    let before = fixture.requests();
    let result = budgeted
        .execute(
            &context(budgeted.clone()),
            &get("fail").with_retry_mode(HttpRetryMode::ConfirmedIdempotent),
        )
        .await;
    assert_eq!(result, Err(HttpFailure::Timeout));
    assert_eq!(fixture.requests(), before + 1);
}

#[tokio::test]
async fn stalled_headers_body_overflow_and_malformed_decoding_fail_closed() {
    let fixture = Fixture::new();
    let profile = fixture
        .profile()
        .budgets(
            Duration::from_millis(200),
            Duration::from_millis(120),
            Duration::from_millis(180),
        )
        .unwrap()
        .max_response_bytes(16)
        .unwrap();
    let service = Arc::new(ReqwestHttpRequestService::new(vec![profile]).unwrap());
    let context = context(service.clone());
    assert_eq!(
        service.execute(&context, &get("large")).await,
        Err(HttpFailure::ResponseTooLarge)
    );
    assert_eq!(
        service.execute(&context, &get("slow")).await,
        Err(HttpFailure::Timeout)
    );
    assert_eq!(
        service.execute(&context, &get("slowbody")).await,
        Err(HttpFailure::Timeout)
    );
    assert_eq!(
        service
            .execute(&context, &get("invalid-utf8"))
            .await
            .unwrap()
            .text(),
        Err(HttpFailure::MalformedResponse)
    );
    assert_eq!(
        service
            .execute(&context, &get("bad-json"))
            .await
            .unwrap()
            .json_value(),
        Err(HttpFailure::MalformedResponse)
    );
}

struct FakeClock {
    origin: Instant,
    nanos: AtomicU64,
}

impl FakeClock {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
            nanos: AtomicU64::new(0),
        }
    }
    fn advance(&self, duration: Duration) {
        self.nanos
            .fetch_add(duration.as_nanos() as u64, Ordering::SeqCst);
    }
}

#[async_trait]
impl HttpOperationalClock for FakeClock {
    fn now(&self) -> Instant {
        self.origin + Duration::from_nanos(self.nanos.load(Ordering::SeqCst))
    }
    async fn sleep(&self, duration: Duration) {
        self.advance(duration);
    }
}

#[tokio::test]
async fn circuit_rate_and_cancelled_half_open_probe_use_operational_time() {
    let fixture = Fixture::new();
    let clock = Arc::new(FakeClock::new());
    let profile = fixture
        .profile()
        .circuit(1, Duration::from_secs(5), 1)
        .unwrap();
    let service =
        Arc::new(ReqwestHttpRequestService::with_clock(vec![profile], clock.clone()).unwrap());
    let circuit_context = Arc::new(context(service.clone()).with_business_clock(
        teaql_runtime::FixedBusinessClock::new("1999-01-01T00:00:00Z".parse().unwrap()),
    ));
    assert_eq!(circuit_context.business_date().to_string(), "1999-01-01");
    assert_eq!(
        service
            .execute(&circuit_context, &get("missing"))
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        service
            .execute(&circuit_context, &get("fail"))
            .await
            .unwrap()
            .status(),
        503
    );
    let before = fixture.requests();
    assert_eq!(
        service.execute(&circuit_context, &get("ok")).await,
        Err(HttpFailure::CircuitOpen)
    );
    assert_eq!(fixture.requests(), before);
    clock.advance(Duration::from_secs(5));
    let slow_service = service.clone();
    let slow_context = circuit_context.clone();
    let cancellation = HttpCancellation::new();
    let slow_cancel = cancellation.clone();
    let slow = tokio::spawn(async move {
        slow_service
            .execute_cancellable(&slow_context, &get("slow"), &slow_cancel)
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    while fixture.requests() == before && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(fixture.requests(), before + 1);
    assert_eq!(
        service.execute(&circuit_context, &get("ok")).await,
        Err(HttpFailure::CircuitOpen),
        "only one half-open probe may run at a time"
    );
    cancellation.cancel();
    assert_eq!(slow.await.unwrap(), Err(HttpFailure::Cancelled));
    assert_eq!(
        service
            .execute(&circuit_context, &get("ok"))
            .await
            .unwrap()
            .status(),
        200
    );

    let limited = Arc::new(
        ReqwestHttpRequestService::with_clock(
            vec![
                fixture
                    .profile()
                    .budgets(
                        Duration::from_millis(200),
                        Duration::from_millis(200),
                        Duration::from_millis(200),
                    )
                    .unwrap()
                    .rate_per_second(1),
            ],
            clock,
        )
        .unwrap(),
    );
    let limited_context = context(limited.clone());
    assert_eq!(
        limited
            .execute(&limited_context, &get("ok"))
            .await
            .unwrap()
            .status(),
        200
    );
    let before = fixture.requests();
    assert_eq!(
        limited.execute(&limited_context, &get("ok")).await,
        Err(HttpFailure::RateLimited)
    );
    assert_eq!(fixture.requests(), before);
}

struct CarrierTelemetry {
    trace: &'static str,
    operations: Arc<Mutex<Vec<RuntimeOperation>>>,
}

impl RuntimeTelemetry for CarrierTelemetry {
    fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
        self.operations.lock().unwrap().push(operation.clone());
        NoopRuntimeTelemetry.start(operation)
    }
    fn inject_context(&self, carrier: &mut BTreeMap<String, String>) {
        carrier.insert("traceparent".to_owned(), self.trace.to_owned());
    }
}

#[tokio::test]
async fn shared_client_keeps_concurrent_carriers_independent() {
    let fixture = Fixture::new();
    let service = Arc::new(ReqwestHttpRequestService::new(vec![fixture.profile()]).unwrap());
    let first_trace = "00-11111111111111111111111111111111-aaaaaaaaaaaaaaaa-01";
    let second_trace = "00-22222222222222222222222222222222-bbbbbbbbbbbbbbbb-01";
    let first_ops = Arc::new(Mutex::new(Vec::new()));
    let second_ops = Arc::new(Mutex::new(Vec::new()));
    let first = context(service.clone()).with_runtime_telemetry(Arc::new(CarrierTelemetry {
        trace: first_trace,
        operations: first_ops.clone(),
    }));
    let second = context(service.clone()).with_runtime_telemetry(Arc::new(CarrierTelemetry {
        trace: second_trace,
        operations: second_ops.clone(),
    }));
    let first_call = HttpCall::get(
        "orders",
        "ok?token=PRIVATE-QUERY-ONE",
        "first request",
        "show first",
    )
    .unwrap();
    let second_call = HttpCall::get(
        "orders",
        "ok?token=PRIVATE-QUERY-TWO",
        "second request",
        "show second",
    )
    .unwrap();
    let (a, b) = tokio::join!(
        service.execute(&first, &first_call),
        service.execute(&second, &second_call)
    );
    assert_eq!(a.unwrap().status(), 200);
    assert_eq!(b.unwrap().status(), 200);
    let carriers = fixture.inner.traceparents.lock().unwrap().clone();
    assert_eq!(carriers.len(), 2);
    assert!(carriers.contains(&first_trace.to_owned()));
    assert!(carriers.contains(&second_trace.to_owned()));
    let first_diagnostic = format!("{:?}", first_ops.lock().unwrap());
    let second_diagnostic = format!("{:?}", second_ops.lock().unwrap());
    assert!(first_diagnostic.contains("first request"));
    assert!(!first_diagnostic.contains("second request"));
    assert!(second_diagnostic.contains("second request"));
    assert!(!second_diagnostic.contains("first request"));
    assert!(!first_diagnostic.contains("PRIVATE-QUERY"));
    assert!(!second_diagnostic.contains("PRIVATE-QUERY"));
}

struct SlowSleepClock {
    sleeping: AtomicBool,
}

#[async_trait]
impl HttpOperationalClock for SlowSleepClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    async fn sleep(&self, _duration: Duration) {
        self.sleeping.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[tokio::test]
async fn cancellation_during_retry_backoff_cannot_replay_the_request() {
    let fixture = Fixture::new();
    let clock = Arc::new(SlowSleepClock {
        sleeping: AtomicBool::new(false),
    });
    let profile = fixture
        .profile()
        .retry(
            3,
            Duration::from_millis(10),
            BTreeSet::from([HttpMethod::Get]),
        )
        .unwrap();
    let service =
        Arc::new(ReqwestHttpRequestService::with_clock(vec![profile], clock.clone()).unwrap());
    let context = Arc::new(context(service.clone()));
    let cancellation = HttpCancellation::new();
    let worker_cancel = cancellation.clone();
    let worker = tokio::spawn(async move {
        service
            .execute_cancellable(
                &context,
                &get("fail").with_retry_mode(HttpRetryMode::ConfirmedIdempotent),
                &worker_cancel,
            )
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    while !clock.sleeping.load(Ordering::SeqCst) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(clock.sleeping.load(Ordering::SeqCst));
    assert_eq!(fixture.requests(), 1);
    cancellation.cancel();
    assert_eq!(worker.await.unwrap(), Err(HttpFailure::Cancelled));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(fixture.requests(), 1);
}

#[tokio::test]
async fn cancellation_while_rate_limited_never_starts_a_second_request() {
    let fixture = Fixture::new();
    let clock = Arc::new(SlowSleepClock {
        sleeping: AtomicBool::new(false),
    });
    let profile = fixture.profile().rate_per_second(1);
    let service =
        Arc::new(ReqwestHttpRequestService::with_clock(vec![profile], clock.clone()).unwrap());
    let context = Arc::new(context(service.clone()));
    assert_eq!(
        service
            .execute(&context, &get("ok"))
            .await
            .unwrap()
            .status(),
        200
    );
    let cancellation = HttpCancellation::new();
    let worker_cancel = cancellation.clone();
    let worker = tokio::spawn(async move {
        service
            .execute_cancellable(&context, &get("ok"), &worker_cancel)
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    while !clock.sleeping.load(Ordering::SeqCst) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(clock.sleeping.load(Ordering::SeqCst));
    assert_eq!(fixture.requests(), 1);
    cancellation.cancel();
    assert_eq!(worker.await.unwrap(), Err(HttpFailure::Cancelled));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(fixture.requests(), 1);
}
