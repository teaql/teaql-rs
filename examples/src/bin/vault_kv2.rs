use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use teaql_http_reqwest::{HttpServiceProfile, ReqwestHttpRequestService};
use teaql_runtime::{
    BoundSecretProvider, RuntimeAttributeValue, RuntimeOperation, RuntimeTelemetry,
    RuntimeTelemetryScope, SecretFailure, SecretProvider, SecretRef, SecretValue, UserContext,
    VaultKv2SecretProvider,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use tokio_rustls::{rustls::ServerConfig, TlsAcceptor};

struct BootstrapToken;

struct CapturedTelemetry(Arc<Mutex<Vec<String>>>);

impl RuntimeTelemetry for CapturedTelemetry {
    fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
        self.0.lock().unwrap().push(format!("start:{operation:?}"));
        Box::new(CapturedScope(self.0.clone()))
    }
}

struct CapturedScope(Arc<Mutex<Vec<String>>>);

impl RuntimeTelemetryScope for CapturedScope {
    fn success(&mut self, attributes: std::collections::BTreeMap<String, RuntimeAttributeValue>) {
        self.0
            .lock()
            .unwrap()
            .push(format!("success:{attributes:?}"));
    }

    fn failure(&mut self, error_type: &str) {
        self.0.lock().unwrap().push(format!("failure:{error_type}"));
    }

    fn failure_with_attributes(
        &mut self,
        error_type: &str,
        attributes: std::collections::BTreeMap<String, RuntimeAttributeValue>,
    ) {
        self.0
            .lock()
            .unwrap()
            .push(format!("failure:{error_type}:{attributes:?}"));
    }
}

#[async_trait]
impl SecretProvider for BootstrapToken {
    async fn resolve(
        &self,
        _context: &UserContext,
        _reference: &SecretRef,
    ) -> Result<SecretValue, SecretFailure> {
        SecretValue::new(b"PRIVATE-VAULT-TOKEN".to_vec(), None, None)
    }
}

fn vault_context(
    port: u16,
    timeout: Duration,
) -> Result<(UserContext, SecretRef), Box<dyn std::error::Error>> {
    let profile = HttpServiceProfile::new(
        "vault",
        url::Url::parse(&format!("http://127.0.0.1:{port}/"))?,
    )?
    .budgets(timeout, timeout, timeout)?
    .max_response_bytes(64 * 1024)?;
    let reference = SecretRef::new("payment", "api_key")?;
    let mut provider = VaultKv2SecretProvider::new(
        "vault",
        "secret",
        Arc::new(BootstrapToken),
        SecretRef::new("bootstrap", "vault_token")?,
    )?;
    provider.bind(reference.clone(), "payment/api", "value")?;
    Ok((
        UserContext::new()
            .with_http_request_service(Arc::new(ReqwestHttpRequestService::new(vec![profile])?))
            .with_secret_provider(Arc::new(provider)),
        reference,
    ))
}

fn tls_http_service(
    port: u16,
    root_pem: Option<&[u8]>,
) -> Result<Arc<ReqwestHttpRequestService>, Box<dyn std::error::Error>> {
    let mut profile = HttpServiceProfile::new(
        "vault",
        url::Url::parse(&format!("https://127.0.0.1:{port}/"))?,
    )?
    .budgets(
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(3),
    )?;
    if let Some(root) = root_pem {
        profile = profile.trusted_root_pem(root)?;
    }
    Ok(Arc::new(ReqwestHttpRequestService::new(vec![profile])?))
}

fn tls_context(
    port: u16,
    root_pem: Option<&[u8]>,
    telemetry_events: Arc<Mutex<Vec<String>>>,
) -> Result<(UserContext, SecretRef), Box<dyn std::error::Error>> {
    let reference = SecretRef::new("payment", "api_key")?;
    let mut provider = VaultKv2SecretProvider::new(
        "vault",
        "secret",
        Arc::new(BootstrapToken),
        SecretRef::new("bootstrap", "vault_token")?,
    )?;
    provider.bind(reference.clone(), "payment/api", "value")?;
    Ok((
        UserContext::new()
            .with_runtime_telemetry(Arc::new(CapturedTelemetry(telemetry_events)))
            .with_http_request_service(tls_http_service(port, root_pem)?)
            .with_secret_provider(Arc::new(provider)),
        reference,
    ))
}

async fn verify_tls_vault() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root_path = directory.path().join("root.pem");
    let root_key_path = directory.path().join("root.key");
    let request_path = directory.path().join("server.csr");
    let certificate_path = directory.path().join("server.pem");
    let key_path = directory.path().join("server.key");
    let root = Command::new("openssl")
        .args([
            "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        ])
        .args(["-subj", "/CN=TeaQL test root"])
        .args(["-addext", "basicConstraints=critical,CA:TRUE"])
        .args(["-addext", "keyUsage=critical,keyCertSign,cRLSign"])
        .arg("-keyout")
        .arg(&root_key_path)
        .arg("-out")
        .arg(&root_path)
        .output()?;
    assert!(root.status.success(), "local TLS root generation failed");
    let request = Command::new("openssl")
        .args(["req", "-new", "-newkey", "rsa:2048", "-nodes"])
        .args(["-subj", "/CN=127.0.0.1"])
        .args(["-addext", "subjectAltName=IP:127.0.0.1"])
        .arg("-keyout")
        .arg(&key_path)
        .arg("-out")
        .arg(&request_path)
        .output()?;
    assert!(
        request.status.success(),
        "local TLS server request generation failed"
    );
    let signed = Command::new("openssl")
        .args([
            "x509",
            "-req",
            "-days",
            "1",
            "-CAcreateserial",
            "-copy_extensions",
            "copy",
        ])
        .arg("-in")
        .arg(&request_path)
        .arg("-CA")
        .arg(&root_path)
        .arg("-CAkey")
        .arg(&root_key_path)
        .arg("-out")
        .arg(&certificate_path)
        .output()?;
    assert!(signed.status.success(), "local TLS server signing failed");
    let root_pem = std::fs::read(root_path)?;
    let certificate_pem = std::fs::read(certificate_path)?;
    let key_pem = std::fs::read(key_path)?;
    let certificate = CertificateDer::from_pem_slice(&certificate_pem)?;
    let key = PrivateKeyDer::from_pem_slice(&key_pem)?;
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)?;
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    let server = tokio::spawn(async move {
        for connection in 0..3 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls = match acceptor.accept(stream).await {
                Ok(tls) => tls,
                Err(_) if connection != 1 => continue,
                Err(error) => panic!("trusted TLS handshake failed: {error:?}"),
            };
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            while !request.ends_with(b"\r\n\r\n") && request.len() < 8192 {
                let count = tls.read(&mut buffer).await.unwrap_or(0);
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..count]);
            }
            if connection != 1 {
                assert!(
                    request.is_empty(),
                    "untrusted peer received an HTTP request"
                );
                continue;
            }
            let text = String::from_utf8(request).unwrap();
            assert!(text.starts_with("GET /v1/secret/data/payment/api HTTP/1.1"));
            assert!(text
                .to_ascii_lowercase()
                .contains("x-vault-token: private-vault-token"));
            let body = br#"{"data":{"data":{"value":"PRIVATE-VAULT-VALUE"},"metadata":{"version":7,"destroyed":false,"deletion_time":""}}}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            tls.write_all(header.as_bytes()).await.unwrap();
            tls.write_all(body).await.unwrap();
            tls.flush().await.unwrap();
        }
    });

    let telemetry_events = Arc::new(Mutex::new(Vec::new()));
    let (mut context, reference) = tls_context(port, None, telemetry_events.clone())?;
    assert_eq!(
        context
            .resolve_secret(&reference, "read configured credential")
            .await
            .unwrap_err(),
        SecretFailure::Unavailable
    );
    context.set_http_request_service(tls_http_service(port, Some(&root_pem))?);
    let value = context
        .resolve_secret(&reference, "read configured credential")
        .await?;
    assert_eq!(value.expose_bytes()?, b"PRIVATE-VAULT-VALUE");
    assert_eq!(value.version(), Some("vault-kv2:7"));
    context.set_http_request_service(tls_http_service(port, None)?);
    assert_eq!(
        context
            .resolve_secret(&reference, "reject removed TLS trust")
            .await
            .unwrap_err(),
        SecretFailure::Unavailable
    );
    tokio::time::timeout(Duration::from_secs(5), server).await??;
    let captured = telemetry_events.lock().unwrap().join("\n");
    assert!(captured.contains("start:RuntimeOperation"));
    assert!(captured.contains("success:"));
    assert!(captured.contains("failure:"));
    assert!(!captured.contains("PRIVATE-VAULT-TOKEN"));
    assert!(!captured.contains("PRIVATE-VAULT-VALUE"));
    Ok(())
}

fn live_context(
    endpoint: &url::Url,
    root_pem: Option<&[u8]>,
    token_variable: &str,
    path: &str,
    telemetry_events: Arc<Mutex<Vec<String>>>,
) -> Result<(UserContext, SecretRef), Box<dyn std::error::Error>> {
    let mut profile = HttpServiceProfile::new("vault", endpoint.clone())?
        .budgets(
            Duration::from_secs(2),
            Duration::from_secs(2),
            Duration::from_secs(4),
        )?
        .max_response_bytes(64 * 1024)?;
    if let Some(root) = root_pem {
        profile = profile.trusted_root_pem(root)?;
    }
    let token_reference = SecretRef::new("bootstrap", "vault_token")?;
    let mut bootstrap = BoundSecretProvider::new(None)?;
    bootstrap.bind_environment(token_reference.clone(), token_variable, None, None)?;
    let reference = SecretRef::new("payment", "api_key")?;
    let mut vault =
        VaultKv2SecretProvider::new("vault", "secret", Arc::new(bootstrap), token_reference)?;
    vault.bind(reference.clone(), path, "value")?;
    Ok((
        UserContext::new()
            .with_runtime_telemetry(Arc::new(CapturedTelemetry(telemetry_events)))
            .with_http_request_service(Arc::new(ReqwestHttpRequestService::new(vec![profile])?))
            .with_secret_provider(Arc::new(vault)),
        reference,
    ))
}

async fn verify_live_vault() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = url::Url::parse(&std::env::var("TEAQL_TEST_VAULT_URL")?)?;
    if endpoint.scheme() != "https" || endpoint.host() != Some(url::Host::Ipv4(Ipv4Addr::LOCALHOST))
    {
        return Err("live Vault test requires a literal-loopback HTTPS endpoint".into());
    }
    let root_pem = std::fs::read(std::env::var("TEAQL_TEST_VAULT_CA_PEM")?)?;
    let expected = std::env::var("TEAQL_TEST_VAULT_EXPECTED_VALUE")?;
    let version = std::env::var("TEAQL_TEST_VAULT_EXPECTED_VERSION")?;
    let telemetry_events = Arc::new(Mutex::new(Vec::new()));

    let (context, reference) = live_context(
        &endpoint,
        Some(&root_pem),
        "TEAQL_TEST_VAULT_TOKEN",
        "payment/api",
        telemetry_events.clone(),
    )?;
    let value = context
        .resolve_secret(&reference, "verify live Vault credential")
        .await?;
    assert_eq!(value.expose_bytes()?, expected.as_bytes());
    assert_eq!(
        value.version(),
        Some(format!("vault-kv2:{version}").as_str())
    );

    let (denied_context, reference) = live_context(
        &endpoint,
        Some(&root_pem),
        "TEAQL_TEST_VAULT_BAD_TOKEN",
        "payment/api",
        telemetry_events.clone(),
    )?;
    assert_eq!(
        denied_context
            .resolve_secret(&reference, "verify denied Vault credential")
            .await
            .unwrap_err(),
        SecretFailure::Denied
    );

    let (missing_context, reference) = live_context(
        &endpoint,
        Some(&root_pem),
        "TEAQL_TEST_VAULT_TOKEN",
        "payment/absent",
        telemetry_events.clone(),
    )?;
    assert_eq!(
        missing_context
            .resolve_secret(&reference, "verify missing Vault credential")
            .await
            .unwrap_err(),
        SecretFailure::Missing
    );

    let (untrusted_context, reference) = live_context(
        &endpoint,
        None,
        "TEAQL_TEST_VAULT_TOKEN",
        "payment/api",
        telemetry_events.clone(),
    )?;
    assert_eq!(
        untrusted_context
            .resolve_secret(&reference, "verify untrusted Vault certificate")
            .await
            .unwrap_err(),
        SecretFailure::Unavailable
    );
    let diagnostics = telemetry_events.lock().unwrap().join("\n");
    assert!(diagnostics.contains("success:"));
    assert!(diagnostics.contains("failure:"));
    assert!(!diagnostics.contains(&std::env::var("TEAQL_TEST_VAULT_TOKEN")?));
    assert!(!diagnostics.contains(&std::env::var("TEAQL_TEST_VAULT_BAD_TOKEN")?));
    assert!(!diagnostics.contains(&expected));
    println!("VAULT_LIVE_PASS success=1 denied=1 missing=1 unknown_ca=1 diagnostics=1");
    Ok(())
}

fn listener() -> Result<TcpListener, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let success_listener = listener()?;
    let success_port = success_listener.local_addr()?.port();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match success_listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    assert!(line.starts_with("GET /v1/secret/data/payment/api HTTP/1.1"));
                    let mut token_seen = false;
                    loop {
                        line.clear();
                        reader.read_line(&mut line).unwrap();
                        if line == "\r\n" {
                            break;
                        }
                        if line.to_ascii_lowercase().starts_with("x-vault-token:") {
                            token_seen = line.trim().ends_with("PRIVATE-VAULT-TOKEN");
                        }
                    }
                    assert!(token_seen);
                    let body = br#"{"data":{"data":{"value":"PRIVATE-VAULT-VALUE"},"metadata":{"version":7,"destroyed":false,"deletion_time":""}}}"#;
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .unwrap();
                    stream.write_all(body).unwrap();
                    break;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(error) => panic!("Vault fixture did not receive a request: {error}"),
            }
        }
    });
    let (context, reference) = vault_context(success_port, Duration::from_secs(2))?;
    let value = context
        .resolve_secret(&reference, "read configured credential")
        .await?;
    assert_eq!(value.expose_bytes()?, b"PRIVATE-VAULT-VALUE");
    assert_eq!(value.version(), Some("vault-kv2:7"));
    assert!(!format!("{value:?}").contains("PRIVATE-VAULT-VALUE"));
    server.join().expect("Vault fixture should complete");

    let slow_listener = listener()?;
    let slow_port = slow_listener.local_addr()?.port();
    let slow_server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match slow_listener.accept() {
                Ok((_stream, _)) => {
                    std::thread::sleep(Duration::from_millis(250));
                    break;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(error) => panic!("slow Vault fixture did not receive a request: {error}"),
            }
        }
    });
    let (slow_context, slow_reference) = vault_context(slow_port, Duration::from_millis(50))?;
    let failure = slow_context
        .resolve_secret(&slow_reference, "read slow credential")
        .await
        .unwrap_err();
    assert_eq!(failure, SecretFailure::Unavailable);
    assert!(!format!("{failure:?} {failure}").contains("PRIVATE-VAULT"));
    slow_server
        .join()
        .expect("slow Vault fixture should complete");
    verify_tls_vault().await?;
    println!("VAULT_KV2_PASS path=1 token=1 version=1 timeout=1 masking=1 tls=1 diagnostics=1");
    if std::env::var_os("TEAQL_TEST_VAULT_URL").is_some() {
        verify_live_vault().await?;
    }
    Ok(())
}
