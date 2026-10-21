use aws_sdk_s3::{
    Client,
    config::{
        BehaviorVersion, ConfigBag, Credentials, Intercept, Region, RuntimeComponents,
        interceptors::BeforeTransmitInterceptorContextRef, retry::RetryConfig,
    },
};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use teaql_object_storage_s3::S3ObjectStorage;
use teaql_runtime::{
    HttpCall, HttpFailure, HttpRequestService, HttpResponse, UserContext, object_storage::*,
};

#[derive(Debug)]
struct Snapshot {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
}
#[derive(Debug)]
struct Capture {
    endpoint: String,
    signed: Arc<Mutex<Vec<Snapshot>>>,
}
impl Intercept for Capture {
    fn name(&self) -> &'static str {
        "SignedSdkCoexistenceCapture"
    }
    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        let request = context.request();
        let mut headers: BTreeMap<String, String> = request
            .headers()
            .iter()
            .map(|(key, value)| (key.to_ascii_lowercase(), value.to_owned()))
            .collect();
        headers
            .entry("host".into())
            .or_insert_with(|| self.endpoint.trim_start_matches("http://").to_owned());
        self.signed.lock().unwrap().push(Snapshot {
            method: request.method().to_owned(),
            target: request
                .uri()
                .strip_prefix(&self.endpoint)
                .unwrap()
                .to_owned(),
            headers,
        });
        Ok(())
    }
}
struct Trap(AtomicUsize);
#[async_trait::async_trait]
impl HttpRequestService for Trap {
    async fn execute(
        &self,
        _context: &UserContext,
        _call: &HttpCall,
    ) -> Result<HttpResponse, HttpFailure> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(HttpFailure::ProfileNotConfigured)
    }
}
struct Server {
    endpoint: String,
    status: Arc<AtomicU16>,
    received: Arc<Mutex<Vec<Snapshot>>>,
    stop: Arc<AtomicBool>,
    task: Option<thread::JoinHandle<()>>,
}
impl Server {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let status = Arc::new(AtomicU16::new(200));
        let received = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (code, wire, done) = (status.clone(), received.clone(), stop.clone());
        let task = thread::spawn(move || {
            while !done.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        let mut input = Vec::new();
                        while !input.windows(4).any(|w| w == b"\r\n\r\n") && input.len() < 64 * 1024
                        {
                            let mut bytes = [0; 1024];
                            let Ok(n) = stream.read(&mut bytes) else {
                                break;
                            };
                            if n == 0 {
                                break;
                            }
                            input.extend_from_slice(&bytes[..n]);
                        }
                        if input.is_empty() {
                            continue;
                        }
                        let text = String::from_utf8(input).unwrap();
                        let mut lines = text.lines();
                        let mut first = lines.next().unwrap().split_whitespace();
                        let method = first.next().unwrap().to_owned();
                        let target = first.next().unwrap().to_owned();
                        let headers = lines
                            .filter(|line| !line.is_empty())
                            .map(|line| {
                                let (key, value) = line.split_once(':').unwrap();
                                (key.to_ascii_lowercase(), value.trim().to_owned())
                            })
                            .collect();
                        wire.lock().unwrap().push(Snapshot {
                            method,
                            target,
                            headers,
                        });
                        let _ = write!(
                            stream,
                            "HTTP/1.1 {} Test\r\nContent-Length: 0\r\nETag: \"sdk-proof-etag\"\r\nConnection: close\r\n\r\n",
                            code.load(Ordering::SeqCst)
                        );
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(error) => panic!("SDK fixture accept failed: {error}"),
                }
            }
        });
        Self {
            endpoint,
            status,
            received,
            stop,
            task: Some(task),
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.task.take().unwrap().join().unwrap();
    }
}

#[tokio::test]
async fn signed_sdk_transport_does_not_use_context_http_or_change_signed_fields() {
    let server = Server::new();
    let signed = Arc::new(Mutex::new(Vec::new()));
    let trap = Arc::new(Trap(AtomicUsize::new(0)));
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(&server.endpoint)
        .force_path_style(true)
        .credentials_provider(Credentials::new(
            "sdk-proof-access",
            "sdk-proof-secret",
            None,
            None,
            "test",
        ))
        .retry_config(RetryConfig::disabled())
        .timeout_config(
            aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(3))
                .build(),
        )
        .interceptor(Capture {
            endpoint: server.endpoint.clone(),
            signed: signed.clone(),
        })
        .build();
    let context = UserContext::new()
        .with_http_request_service(trap.clone())
        .with_object_storage(Arc::new(
            S3ObjectStorage::new(Client::from_conf(config), "bucket").unwrap(),
        ));
    let control = HttpCall::get(
        "trap",
        "control",
        "check SPI installation",
        "verify SDK isolation",
    )
    .unwrap();
    assert_eq!(
        context
            .http_requests()
            .unwrap()
            .execute(&context, &control)
            .await,
        Err(HttpFailure::ProfileNotConfigured)
    );
    assert_eq!(trap.0.load(Ordering::SeqCst), 1);
    let storage = context
        .object_storage("verify signed SDK isolation")
        .unwrap();
    let key = ObjectKey::new("sdk-proof").unwrap();
    assert!(storage.exists(&key).await.unwrap());
    server.status.store(503, Ordering::SeqCst);
    assert_eq!(
        storage.exists(&key).await.unwrap_err().0,
        StorageErrorCode::Unavailable
    );
    assert_eq!(
        trap.0.load(Ordering::SeqCst),
        1,
        "runtime HTTP must not wrap or retry SDK calls"
    );
    let signed = signed.lock().unwrap();
    let received = server.received.lock().unwrap();
    assert_eq!(signed.len(), 2);
    assert_eq!(received.len(), 2);
    for (before, wire) in signed.iter().zip(received.iter()) {
        assert_eq!(before.method, wire.method);
        assert_eq!(before.target, wire.target);
        let authorization = before.headers.get("authorization").unwrap();
        assert!(authorization.starts_with("AWS4-HMAC-SHA256 "));
        assert!(
            Some(authorization) == wire.headers.get("authorization"),
            "authorization changed after SDK signing"
        );
        let names = authorization
            .split_once("SignedHeaders=")
            .unwrap()
            .1
            .split(',')
            .next()
            .unwrap();
        for name in names.split(';') {
            assert!(
                before.headers.get(name) == wire.headers.get(name),
                "SDK signed header changed: {name}"
            );
        }
        assert!(!wire.headers.contains_key("traceparent"));
    }
    println!(
        "HTTP_SDK_COEXISTENCE_PASS language=rust control_calls=1 runtime_sdk_calls=0 sdk_attempts=2 wire_requests=2 signatures=2"
    );
}
