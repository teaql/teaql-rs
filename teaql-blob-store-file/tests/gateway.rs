#![cfg(feature = "http-gateway")]
use futures_util::stream;
use reqwest::{Client, StatusCode};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    net::{Ipv4Addr, SocketAddr},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};
use teaql_blob_contracts::*;
use teaql_blob_store_file::*;
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

const NOW: i64 = 1800000000000;
struct Clock(AtomicI64);
impl BlobGatewayClock for Clock {
    fn now_millis(&self) -> BlobResult<i64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}
struct Host {
    gateway: Arc<FileBlobHttpGateway>,
    address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    worker: Option<JoinHandle<BlobResult<()>>>,
}
impl Drop for Host {
    fn drop(&mut self) {
        if let Some(worker) = &self.worker {
            worker.abort();
        }
    }
}
fn keys() -> BTreeMap<String, Vec<u8>> {
    BTreeMap::from([
        ("first".into(), vec![0x57; 32]),
        ("second".into(), vec![0xa3; 32]),
    ])
}
impl Host {
    async fn at(
        files: FileBlobStore,
        clock: Arc<dyn BlobGatewayClock>,
        active: &str,
        keys: BTreeMap<String, Vec<u8>>,
        address: Option<SocketAddr>,
    ) -> Self {
        let listener = TcpListener::bind(
            address.unwrap_or_else(|| SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
        )
        .await
        .unwrap();
        let address = listener.local_addr().unwrap();
        let gateway = Arc::new(
            FileBlobHttpGateway::new(
                files,
                &format!("http://{address}/blob/read"),
                active,
                keys,
                clock,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        assert_eq!(
            gateway.validate_ready().await.unwrap_err().code,
            BlobErrorCode::CapabilityMissing
        );
        let (send, stop) = oneshot::channel();
        let service = gateway.clone();
        let worker = tokio::spawn(async move {
            service
                .serve(listener, async {
                    let _ = stop.await;
                })
                .await
        });
        let result = Self {
            gateway,
            address,
            stop: Some(send),
            worker: Some(worker),
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            while result.gateway.validate_ready().await.is_err() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        result
    }
    async fn close(mut self) -> SocketAddr {
        self.stop.take().unwrap().send(()).unwrap();
        self.worker.take().unwrap().await.unwrap().unwrap();
        assert_eq!(
            self.gateway.validate_ready().await.unwrap_err().code,
            BlobErrorCode::CapabilityMissing
        );
        self.address
    }
}
async fn files() -> (TempDir, FileBlobStore) {
    let temp = tempfile::tempdir().unwrap();
    let files = FileBlobStore::new(temp.path().join("store"), 1048576, 8).unwrap();
    files.initialize().await.unwrap();
    (temp, files)
}
async fn put(files: &FileBlobStore, id: &str, content: impl Into<Bytes>) -> StoredBlob {
    let target = files.create_upload(id).await.unwrap();
    files
        .write(
            &target,
            Box::pin(stream::iter([Ok(content.into())])),
            1048576,
        )
        .await
        .unwrap()
}
fn request(content: &StoredBlob, mime: &str, lifetime: u64, cache: CachePolicy) -> BlobReadRequest {
    BlobReadRequest::new(
        content.location(),
        content.content_version(),
        mime,
        "文档 test.pdf",
        Disposition::Attachment,
        Duration::from_secs(lifetime),
        cache,
    )
    .unwrap()
}
async fn sign(host: &Host, request: BlobReadRequest) -> SignedBlobRead {
    let mut signed = host.gateway.sign_read_batch(&[request]).await.unwrap();
    assert_eq!(signed.len(), 1);
    signed.remove(0)
}
fn hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_ten_file_http_head_ranges_and_independent_curl() {
    let (_temp, files) = files().await;
    let host = Host::at(
        files.clone(),
        Arc::new(SystemBlobGatewayClock),
        "first",
        keys(),
        None,
    )
    .await;
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let fixtures_json: serde_json::Value =
        serde_json::from_slice(&fs::read(fixtures.join("files.json")).unwrap()).unwrap();
    let client = Client::new();
    let cases = fixtures_json["files"].as_array().unwrap();
    assert_eq!(cases.len(), 10);
    for fixture in cases {
        let data = fs::read(fixtures.join(fixture["path"].as_str().unwrap())).unwrap();
        let content = put(&files, fixture["extension"].as_str().unwrap(), data.clone()).await;
        let signed = sign(
            &host,
            request(
                &content,
                fixture["contentType"].as_str().unwrap(),
                300,
                CachePolicy::PrivateMaxAge(300),
            ),
        )
        .await;
        let now = SystemBlobGatewayClock.now_millis().unwrap();
        assert!(signed.expires_at_millis() > now && signed.expires_at_millis() <= now + 60000);
        assert!(
            signed
                .as_str()
                .starts_with(&format!("http://{}/blob/read?token=", host.address))
        );
        let response = client.get(signed.as_str()).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            fixture["contentType"].as_str().unwrap()
        );
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert_eq!(response.headers()["accept-ranges"], "bytes");
        assert!(
            response.headers()["content-disposition"]
                .to_str()
                .unwrap()
                .contains("filename*=UTF-8''%E6%96%87%E6%A1%A3%20test.pdf")
        );
        let cache = response.headers()["cache-control"]
            .to_str()
            .unwrap()
            .strip_prefix("private, max-age=")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert!(cache <= 60);
        let received = response.bytes().await.unwrap();
        assert_eq!(received.as_ref(), &data);
        assert_eq!(
            hex(&Sha256::digest(&received)),
            fixture["sha256"].as_str().unwrap()
        );
        let response = client.head(signed.as_str()).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-length"]
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            data.len()
        );
        assert!(response.bytes().await.unwrap().is_empty());
        for (range, expected) in [
            ("bytes=3-12".to_owned(), data[3..13].to_vec()),
            ("bytes=-5".into(), data[data.len() - 5..].to_vec()),
            (
                format!("bytes={}-", data.len() - 5),
                data[data.len() - 5..].to_vec(),
            ),
        ] {
            let response = client
                .get(signed.as_str())
                .header("range", range)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert!(
                response.headers()["content-range"]
                    .to_str()
                    .unwrap()
                    .ends_with(&format!("/{}", data.len()))
            );
            assert_eq!(response.bytes().await.unwrap().as_ref(), expected);
        }
        if fixture["extension"] == "mp4" {
            let url = signed.as_str().to_owned();
            let output = tokio::task::spawn_blocking(move || {
                // URL goes through stdin, never argv or test output.
                let mut curl = Command::new("/usr/bin/curl")
                    .args(["--silent", "--show-error", "--fail", "--config", "-"])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                curl.stdin
                    .take()
                    .unwrap()
                    .write_all(format!("url = \"{url}\"\n").as_bytes())
                    .unwrap();
                curl.wait_with_output().unwrap()
            })
            .await
            .unwrap();
            assert!(output.status.success());
            assert_eq!(output.stdout, data);
            assert!(output.stderr.is_empty());
        }
    }
    let address = host.close().await;
    let rebound = TcpListener::bind(address).await.unwrap();
    drop(rebound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wall_clock_expiry_reissue_and_content_survives() {
    let (_temp, files) = files().await;
    let content = put(&files, "expiry", "persistent content").await;
    let host = Host::at(
        files.clone(),
        Arc::new(SystemBlobGatewayClock),
        "first",
        keys(),
        None,
    )
    .await;
    let client = Client::new();
    let signed = sign(
        &host,
        request(&content, "application/pdf", 1, CachePolicy::NoStore),
    )
    .await;
    assert_eq!(
        client.get(signed.as_str()).send().await.unwrap().status(),
        StatusCode::OK
    );
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let expired = client.get(signed.as_str()).send().await.unwrap();
    assert_eq!(expired.status(), StatusCode::FORBIDDEN);
    assert_eq!(expired.headers()["cache-control"], "private, no-store");
    assert_eq!(
        files.stat(content.location()).await.unwrap(),
        Some(content.clone())
    );
    let refreshed = sign(
        &host,
        request(&content, "application/pdf", 10, CachePolicy::NoStore),
    )
    .await;
    assert!(refreshed.expires_at_millis() > signed.expires_at_millis());
    assert_eq!(
        client
            .get(refreshed.as_str())
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"persistent content"
    );
    host.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tamper_origin_method_ranges_and_generation_fail_closed() {
    let (_temp, files) = files().await;
    let content = put(&files, "versions", "0123456789abcdef").await;
    let clock = Arc::new(Clock(AtomicI64::new(NOW)));
    let host = Host::at(files.clone(), clock.clone(), "first", keys(), None).await;
    let other = Host::at(files.clone(), clock, "first", keys(), None).await;
    let client = Client::new();
    let signed = sign(
        &host,
        request(&content, "application/pdf", 30, CachePolicy::NoStore),
    )
    .await;
    let mut tampered = signed.as_str().as_bytes().to_vec();
    let last = tampered.last_mut().unwrap();
    *last = if *last == b'A' { b'B' } else { b'A' };
    assert_eq!(
        client
            .get(String::from_utf8(tampered).unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    for url in [
        format!("{}&token=duplicate", signed.as_str()),
        signed
            .as_str()
            .replace(&host.address.to_string(), &other.address.to_string()),
        format!("http://{}/blob/read", host.address),
    ] {
        assert_eq!(
            client.get(url).send().await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }
    for range in [
        "bytes=0-1,3-4",
        "bytes=-0",
        "bytes=999999-",
        "bytes=2-1",
        "items=0-1",
        "bytes=+1-3",
        "bytes=0--1",
    ] {
        let response = client
            .get(signed.as_str())
            .header("range", range)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.headers()["content-range"], "bytes */16");
    }
    let response = client.post(signed.as_str()).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers()["allow"], "GET, HEAD");
    assert_eq!(
        client
            .get(signed.as_str())
            .body("x")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        files
            .delete_if_version(content.location(), content.content_version())
            .await
            .unwrap(),
        BlobDeleteResult::Deleted
    );
    let replacement = put(&files, "versions", "different generation").await;
    assert_ne!(content.content_version(), replacement.content_version());
    assert_eq!(
        client.get(signed.as_str()).send().await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    let fresh = sign(
        &host,
        request(&replacement, "application/pdf", 30, CachePolicy::NoStore),
    )
    .await;
    assert_eq!(
        client
            .get(fresh.as_str())
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"different generation"
    );
    host.close().await;
    other.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn key_rotation_accepts_retained_key_and_rejects_retired_key() {
    let (_temp, files) = files().await;
    let content = put(&files, "rotation", "rotate").await;
    let clock = Arc::new(Clock(AtomicI64::new(NOW)));
    let host = Host::at(files.clone(), clock.clone(), "first", keys(), None).await;
    let signed = sign(
        &host,
        request(&content, "application/pdf", 30, CachePolicy::NoStore),
    )
    .await;
    let address = host.close().await;
    let host = Host::at(
        files.clone(),
        clock.clone(),
        "second",
        keys(),
        Some(address),
    )
    .await;
    let client = Client::new();
    assert_eq!(
        client.get(signed.as_str()).send().await.unwrap().status(),
        StatusCode::OK
    );
    let newer = sign(
        &host,
        request(&content, "application/pdf", 30, CachePolicy::NoStore),
    )
    .await;
    let address = host.close().await;
    let host = Host::at(
        files,
        clock,
        "second",
        BTreeMap::from([("second".into(), vec![0xa3; 32])]),
        Some(address),
    )
    .await;
    assert_eq!(
        client.get(signed.as_str()).send().await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client.get(newer.as_str()).send().await.unwrap().status(),
        StatusCode::OK
    );
    host.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_remaining_lifetime_and_expiry_after_actual_lock_wait() {
    let (temp, files) = files().await;
    let content = put(&files, "lock-expiry", "0123456789").await;
    let clock = Arc::new(Clock(AtomicI64::new(NOW)));
    let host = Host::at(files, clock.clone(), "first", keys(), None).await;
    let signed = sign(
        &host,
        request(
            &content,
            "application/pdf",
            10,
            CachePolicy::PrivateMaxAge(10),
        ),
    )
    .await;
    clock.0.store(NOW + 7001, Ordering::SeqCst);
    let client = Client::new();
    let response = client.get(signed.as_str()).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "private, max-age=2");
    let lock = fs::OpenOptions::new()
        .write(true)
        .mode(0o600)
        .open(
            temp.path()
                .join("store/locks")
                .join(format!("{}.lock", &content.location()[..2])),
        )
        .unwrap();
    rustix::fs::flock(&lock, rustix::fs::FlockOperation::LockExclusive).unwrap();
    let url = signed.as_str().to_owned();
    let waiting =
        tokio::spawn(async move { Client::new().get(url).send().await.unwrap().status() });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiting.is_finished());
    clock.0.store(NOW + 11000, Ordering::SeqCst);
    drop(lock);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap(),
        StatusCode::FORBIDDEN
    );
    host.close().await;
}

#[tokio::test]
async fn invalid_transport_configuration_and_wire_values_are_rejected() {
    let (_temp, files) = files().await;
    for endpoint in [
        "relative",
        "ftp://example.invalid/blob",
        "http://u:p@example.invalid/blob",
        "http://@example.invalid/blob",
        "http://example.invalid/\nblob",
        "http://example.invalid/blob?q=x",
        "http://example.invalid/blob#x",
        "http://example.invalid:0/blob",
        "http://example.invalid/",
    ] {
        assert!(
            FileBlobHttpGateway::new(
                files.clone(),
                endpoint,
                "first",
                keys(),
                Arc::new(SystemBlobGatewayClock),
                Duration::from_secs(60)
            )
            .is_err()
        );
    }
    assert!(
        FileBlobHttpGateway::new(
            files,
            "http://example.invalid/blob",
            "missing",
            keys(),
            Arc::new(SystemBlobGatewayClock),
            Duration::from_secs(60)
        )
        .is_err()
    );
    assert!(SignedBlobRead::new("/relative", NOW).is_err());
    assert!(SignedBlobRead::new("https://u:p@example.invalid/secret", NOW).is_err());
    assert!(SignedBlobRead::new("https://@example.invalid/read", NOW).is_err());
    assert!(SignedBlobRead::new("https://example.invalid/\nread", NOW).is_err());
    assert!(SignedBlobRead::new("http://example.invalid:0/read", NOW).is_err());
    let signed =
        SignedBlobRead::new("https://example.invalid/read?token=PRIVATE-URL", NOW).unwrap();
    assert!(!format!("{signed:?}").contains("PRIVATE"));
    for (mime, name, disposition, lifetime) in [
        (
            "text/plain\r\nX: bad",
            "x",
            Disposition::Inline,
            Duration::from_secs(1),
        ),
        (
            "text/plain",
            "x\n",
            Disposition::Inline,
            Duration::from_secs(1),
        ),
        ("text/plain", "x", Disposition::Auto, Duration::from_secs(1)),
        (
            "text/plain",
            "x",
            Disposition::Inline,
            Duration::from_nanos(1),
        ),
    ] {
        assert!(
            BlobReadRequest::new(
                "location",
                "version",
                mime,
                name,
                disposition,
                lifetime,
                CachePolicy::NoStore
            )
            .is_err()
        );
    }
}
