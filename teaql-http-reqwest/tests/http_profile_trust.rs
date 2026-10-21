//! Standalone loopback private-CA proofs, independent of Secret Provider.
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use teaql_http_reqwest::{HttpServiceProfile, ReqwestHttpRequestService};
use teaql_runtime::{HttpCall, HttpFailure, HttpRequestService, UserContext};
use url::Url;

struct Certificates {
    directory: tempfile::TempDir,
    roots: Vec<Vec<u8>>,
}

fn openssl(directory: &Path, args: &[&str]) {
    let mut child = Command::new("openssl")
        .args(args)
        .current_dir(directory)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("openssl TLS fixture generator");
    let start = Instant::now();
    loop {
        match child.try_wait().unwrap() {
            Some(status) => {
                assert!(status.success(), "TLS fixture generator failed");
                return;
            }
            None if start.elapsed() > Duration::from_secs(30) => {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("TLS fixture generator exceeded budget");
            }
            None => thread::sleep(Duration::from_millis(5)),
        }
    }
}

impl Certificates {
    fn new() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("teaql-http-trust-")
            .tempdir()
            .unwrap();
        let path = directory.path();
        let mut roots = Vec::new();
        for i in 0..4 {
            openssl(
                path,
                &[
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-subj",
                    &format!("/CN=TeaQL HTTP fixture root {i}"),
                    "-addext",
                    "basicConstraints=critical,CA:TRUE",
                    "-addext",
                    "keyUsage=critical,keyCertSign,cRLSign",
                    "-keyout",
                    &format!("root-{i}.key"),
                    "-out",
                    &format!("root-{i}.pem"),
                ],
            );
            roots.push(std::fs::read(path.join(format!("root-{i}.pem"))).unwrap());
        }
        openssl(
            path,
            &[
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                "/CN=HTTP fixture",
                "-addext",
                "subjectAltName=IP:127.0.0.1",
                "-keyout",
                "server.key",
                "-out",
                "server.csr",
            ],
        );
        for i in 0..2 {
            openssl(
                path,
                &[
                    "x509",
                    "-req",
                    "-days",
                    "1",
                    "-CAcreateserial",
                    "-copy_extensions",
                    "copy",
                    "-in",
                    "server.csr",
                    "-CA",
                    &format!("root-{i}.pem"),
                    "-CAkey",
                    &format!("root-{i}.key"),
                    "-out",
                    &format!("server-{i}.pem"),
                ],
            );
        }
        openssl(
            path,
            &[
                "req",
                "-new",
                "-key",
                "server.key",
                "-subj",
                "/CN=HTTP wrong host fixture",
                "-addext",
                "subjectAltName=IP:127.0.0.2",
                "-out",
                "wrong-host.csr",
            ],
        );
        openssl(
            path,
            &[
                "x509",
                "-req",
                "-days",
                "1",
                "-CAcreateserial",
                "-copy_extensions",
                "copy",
                "-in",
                "wrong-host.csr",
                "-CA",
                "root-0.pem",
                "-CAkey",
                "root-0.key",
                "-out",
                "wrong-host.pem",
            ],
        );
        Self { directory, roots }
    }
}

struct Peer {
    port: u16,
    requests: Arc<AtomicUsize>,
    closed: Arc<AtomicBool>,
    server: Option<thread::JoinHandle<()>>,
}

impl Peer {
    fn new(certificates: &Certificates, name: &str, root: usize) -> Self {
        let path = certificates.directory.path();
        let leaf =
            CertificateDer::from_pem_slice(&std::fs::read(path.join(name)).unwrap()).unwrap();
        let root = CertificateDer::from_pem_slice(&certificates.roots[root]).unwrap();
        let key = PrivateKeyDer::from_pem_slice(&std::fs::read(path.join("server.key")).unwrap())
            .unwrap();
        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![leaf, root], key)
            .unwrap();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        let config = Arc::new(config);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let (count, stop) = (requests.clone(), closed.clone());
        let server = thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((socket, _)) => serve(socket, &config, &count),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(error) => panic!("owned TLS listener failed: {error:?}"),
                }
            }
        });
        Self {
            port,
            requests,
            closed,
            server: Some(server),
        }
    }
    fn uri(&self) -> Url {
        Url::parse(&format!("https://127.0.0.1:{}/", self.port)).unwrap()
    }
    fn count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

fn serve(socket: TcpStream, config: &Arc<rustls::ServerConfig>, count: &AtomicUsize) {
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let connection = rustls::ServerConnection::new(config.clone()).unwrap();
    let mut stream = rustls::StreamOwned::new(connection, socket);
    let mut reader = BufReader::new(&mut stream);
    let mut line = String::new();
    // Invalid/untrusted/hostname-mismatched clients cannot enter HTTP.
    if reader.read_line(&mut line).is_err() || line.is_empty() {
        return;
    }
    assert!(line.starts_with("GET /status HTTP/1.1"));
    loop {
        line.clear();
        if reader.read_line(&mut line).is_err() || line.is_empty() {
            return;
        }
        if line == "\r\n" {
            break;
        }
    }
    count.fetch_add(1, Ordering::SeqCst);
    let _ =
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
    let _ = stream.flush();
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(server) = self.server.take() {
            if !thread::panicking() {
                server.join().unwrap();
            } else {
                let _ = server.join();
            }
        }
    }
}

fn profile(alias: &str, uri: Url) -> HttpServiceProfile {
    HttpServiceProfile::new(alias, uri)
        .unwrap()
        .budgets(
            Duration::from_secs(3),
            Duration::from_secs(3),
            Duration::from_secs(5),
        )
        .unwrap()
}
fn service(profiles: Vec<HttpServiceProfile>) -> ReqwestHttpRequestService {
    ReqwestHttpRequestService::new(profiles).unwrap()
}
async fn success(service: &ReqwestHttpRequestService, alias: &str) {
    let call = HttpCall::get(
        alias,
        "status",
        "load private upstream status",
        "verify profile-scoped HTTPS trust",
    )
    .unwrap();
    let response = service.execute(&UserContext::new(), &call).await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.body(), b"ok");
}
async fn rejected(service: &ReqwestHttpRequestService, alias: &str) {
    let call = HttpCall::get(
        alias,
        "status",
        "load private upstream status",
        "verify profile-scoped HTTPS trust",
    )
    .unwrap();
    assert_eq!(
        service.execute(&UserContext::new(), &call).await,
        Err(HttpFailure::UpstreamFailure)
    );
}

#[test]
fn rejects_invalid_trust_inputs_and_accepts_one_to_four_distinct_roots() {
    let certificates = Certificates::new();
    let roots = &certificates.roots;
    let old = &roots[0];
    let next = &roots[1];
    let bundled = [old.as_slice(), next.as_slice()].concat();
    let crlf = String::from_utf8(old.clone())
        .unwrap()
        .replace('\n', "\r\n")
        .into_bytes();
    let empty = Vec::new();
    let oversized = vec![0; 65537];
    let invalid: Vec<Vec<&[u8]>> = vec![
        vec![],
        vec![&empty],
        vec![&oversized],
        vec![b"PRIVATE_ROOT_CANARY"],
        vec![&bundled],
        vec![old, old],
        vec![old, &crlf],
        vec![old, next, &roots[2], &roots[3], old],
    ];
    for input in invalid {
        let result = profile("upstream", Url::parse("https://127.0.0.1/").unwrap())
            .trusted_roots_pem(&input);
        assert!(matches!(result, Err(HttpFailure::InvalidInput)));
        assert!(!format!("{result:?}").contains("PRIVATE_ROOT_CANARY"));
    }
    assert!(
        profile("upstream", Url::parse("http://127.0.0.1/").unwrap())
            .trusted_root_pem(old)
            .is_err()
    );
    for count in 1..=4 {
        let input: Vec<&[u8]> = roots[..count].iter().map(Vec::as_slice).collect();
        assert!(
            profile("upstream", Url::parse("https://127.0.0.1/").unwrap())
                .trusted_roots_pem(&input)
                .is_ok()
        );
    }
    println!(
        "HTTP_TRUST_INPUT_PASS language=rust bounded=1 malformed=1 duplicate=1 bundle=1 https_only=1"
    );
}

#[tokio::test]
async fn custom_roots_stay_profile_local_and_do_not_disable_hostname_checks() {
    let certificates = Certificates::new();
    let root = &certificates.roots[0];
    let peer = Peer::new(&certificates, "server-0.pem", 0);
    let wrong = Peer::new(&certificates, "wrong-host.pem", 0);
    let service = service(vec![
        profile("system", peer.uri()),
        profile("trusted", peer.uri())
            .trusted_root_pem(root)
            .unwrap(),
        profile("wrong_host", wrong.uri())
            .trusted_root_pem(root)
            .unwrap(),
    ]);
    rejected(&service, "system").await;
    assert_eq!(peer.count(), 0);
    success(&service, "trusted").await;
    assert_eq!(peer.count(), 1);
    rejected(&service, "system").await;
    assert_eq!(peer.count(), 1);
    rejected(&service, "wrong_host").await;
    assert_eq!(wrong.count(), 0);
    println!(
        "HTTP_TRUST_ISOLATION_PASS language=rust system_reject=2 custom_success=1 hostname_reject=1"
    );
}

#[tokio::test]
async fn overlapping_roots_rotate_across_both_live_peers_without_reset() {
    let certificates = Certificates::new();
    let root = &certificates.roots[0];
    let next_root = &certificates.roots[1];
    let old = Peer::new(&certificates, "server-0.pem", 0);
    let next = Peer::new(&certificates, "server-1.pem", 1);
    let old_only = service(vec![
        profile("old", old.uri()).trusted_root_pem(root).unwrap(),
        profile("next", next.uri()).trusted_root_pem(root).unwrap(),
    ]);
    success(&old_only, "old").await;
    rejected(&old_only, "next").await;
    assert_eq!(next.count(), 0);
    let overlap = service(vec![
        profile("old", old.uri())
            .trusted_roots_pem(&[root, next_root])
            .unwrap(),
        profile("next", next.uri())
            .trusted_roots_pem(&[root, next_root])
            .unwrap(),
    ]);
    let new_only = service(vec![
        profile("old", old.uri())
            .trusted_root_pem(next_root)
            .unwrap(),
        profile("next", next.uri())
            .trusted_root_pem(next_root)
            .unwrap(),
    ]);
    for round in 1..=2 {
        success(&overlap, "old").await;
        success(&overlap, "next").await;
        rejected(&new_only, "old").await;
        success(&new_only, "next").await;
        assert_eq!(old.count(), 1 + round);
        assert_eq!(next.count(), 2 * round);
        println!(
            "HTTP_TRUST_ROTATION_PASS language=rust round={round} old_requests={} new_requests={}",
            old.count(),
            next.count()
        );
    }
}

#[tokio::test]
async fn cloned_profiles_and_owned_roots_preserve_independent_snapshots() {
    let certificates = Certificates::new();
    let peer = Peer::new(&certificates, "server-0.pem", 0);
    let mut input = certificates.roots[0].clone();
    let before = profile("trusted", peer.uri())
        .trusted_root_pem(&input)
        .unwrap();
    input.fill(0);
    let changed = before
        .clone()
        .trusted_root_pem(&certificates.roots[1])
        .unwrap();
    assert!(
        before
            .clone()
            .trusted_roots_pem(&[&certificates.roots[1], &[]])
            .is_err()
    );
    success(&service(vec![before]), "trusted").await;
    rejected(&service(vec![changed]), "trusted").await;
    assert_eq!(peer.count(), 1);
}

#[test]
fn profile_limits_and_duplicate_aliases_fail_before_use() {
    let uri = || Url::parse("https://127.0.0.1/").unwrap();
    for invalid in [
        Duration::ZERO,
        Duration::from_secs(3600) + Duration::from_nanos(1),
    ] {
        for position in 0..3 {
            let mut budgets = [Duration::from_secs(1); 3];
            budgets[position] = invalid;
            assert!(
                profile("valid", uri())
                    .budgets(budgets[0], budgets[1], budgets[2])
                    .is_err()
            );
        }
    }
    for bytes in [0, 16 * 1024 * 1024 + 1] {
        assert!(profile("valid", uri()).max_response_bytes(bytes).is_err());
    }
    for attempts in [0, 6] {
        assert!(
            profile("valid", uri())
                .retry(attempts, Duration::from_millis(1), BTreeSet::new())
                .is_err()
        );
    }
    assert!(
        profile("valid", uri())
            .circuit(1, Duration::from_secs(1), 0)
            .is_err()
    );
    assert!(
        ReqwestHttpRequestService::new(vec![profile("same", uri()), profile("same", uri())])
            .is_err()
    );
}
