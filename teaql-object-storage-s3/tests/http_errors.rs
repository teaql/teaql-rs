use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig},
};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use teaql_object_storage_s3::S3ObjectStorage;
use teaql_runtime::{UserContext, object_storage::*};

struct Server {
    url: String,
    status: Arc<AtomicU16>,
    calls: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    task: Option<thread::JoinHandle<()>>,
}
impl Server {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let status = Arc::new(AtomicU16::new(403));
        let calls = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (s, c, done) = (status.clone(), calls.clone(), stop.clone());
        let task = thread::spawn(move || {
            while !done.load(Ordering::SeqCst) {
                let Ok((mut connection, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                connection
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut b = [0; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let Ok(n) = connection.read(&mut b) else {
                        break;
                    };
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&b[..n]);
                }
                c.fetch_add(1, Ordering::SeqCst);
                let mut code = s.load(Ordering::SeqCst);
                if code == 0 {
                    thread::sleep(Duration::from_millis(400));
                    code = 503;
                }
                let request_line = String::from_utf8_lossy(&request);
                let path = request_line.split_whitespace().nth(1).unwrap_or("");
                if request_line.starts_with("HEAD ")
                    && path.trim_end_matches('/') == "/bucket"
                    && code == 404
                {
                    code = 200;
                }
                let response = format!(
                    "HTTP/1.1 {code} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = connection.write_all(response.as_bytes());
            }
        });
        Self {
            url,
            status,
            calls,
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
async fn object_storage_http_errors_and_invalid_upload_semantics() {
    let server = Server::new();
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(&server.url)
        .force_path_style(true)
        .credentials_provider(Credentials::new("test", "secret", None, None, "test"))
        .retry_config(RetryConfig::disabled())
        .timeout_config(
            aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                .read_timeout(Duration::from_millis(200))
                .operation_timeout(Duration::from_secs(3))
                .build(),
        )
        .build();
    let context = UserContext::new().with_object_storage(Arc::new(
        S3ObjectStorage::new(Client::from_conf(config), "bucket").unwrap(),
    ));
    let storage = context
        .object_storage("verify provider error semantics")
        .unwrap();
    let key = ObjectKey::new("PRIVATE-KEY-CANARY").unwrap();
    for (status, expected) in [
        (403, StorageErrorCode::AccessDenied),
        (500, StorageErrorCode::Unavailable),
        (503, StorageErrorCode::Unavailable),
        (408, StorageErrorCode::Timeout),
        (504, StorageErrorCode::Timeout),
        (501, StorageErrorCode::Unsupported),
    ] {
        server.status.store(status, Ordering::SeqCst);
        let error = storage.exists(&key).await.unwrap_err();
        assert_eq!(error.0, expected);
        assert!(!format!("{error:?}").contains("CANARY"));
    }
    server.status.store(404, Ordering::SeqCst);
    assert!(!storage.exists(&key).await.unwrap());
    server.status.store(200, Ordering::SeqCst);
    assert_eq!(
        storage.exists(&key).await.unwrap_err().0,
        StorageErrorCode::Unavailable
    );
    let before = server.calls.load(Ordering::SeqCst);
    for size in [0, 2] {
        assert_eq!(
            storage
                .put_bytes(
                    &key,
                    Bytes::from_static(b"x"),
                    &PutOptions::new(size, WriteMode::CreateOnly).unwrap()
                )
                .await
                .err()
                .unwrap()
                .0,
            StorageErrorCode::InvalidInput
        );
    }
    let stream = Box::pin(futures_util::stream::iter([Err(StorageError(
        StorageErrorCode::Io,
    ))]));
    assert_eq!(
        storage
            .put(
                &key,
                stream,
                &PutOptions::new(1, WriteMode::CreateOnly).unwrap()
            )
            .await
            .err()
            .unwrap()
            .0,
        StorageErrorCode::Io
    );
    assert_eq!(server.calls.load(Ordering::SeqCst), before);
    server.status.store(0, Ordering::SeqCst);
    assert_eq!(
        storage.exists(&key).await.unwrap_err().0,
        StorageErrorCode::Timeout
    );
    drop(server);
    assert_eq!(
        storage.exists(&key).await.unwrap_err().0,
        StorageErrorCode::Unavailable
    );
}
