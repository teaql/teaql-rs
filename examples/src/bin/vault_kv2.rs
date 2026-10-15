use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use teaql_http_reqwest::{HttpServiceProfile, ReqwestHttpRequestService};
use teaql_runtime::{
    SecretFailure, SecretProvider, SecretRef, SecretValue, UserContext, VaultKv2SecretProvider,
};

struct BootstrapToken;

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
    println!("VAULT_KV2_PASS path=1 token=1 version=1 timeout=1 masking=1");
    Ok(())
}
