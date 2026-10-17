//! Controlled Vault KV v2 -> standard OTLP probe. Backend readback belongs to conformance.
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use teaql_http_reqwest::{HttpServiceProfile, ReqwestHttpRequestService};
use teaql_runtime::{
    SecretFailure, SecretProvider, SecretRef, SecretValue, UserContext, VaultKv2SecretProvider,
};

const TOKEN: &str = "PRIVATE_VAULT_TOKEN_CANARY";
const VALUE: &str = "PRIVATE_VAULT_VALUE_CANARY";

struct BootstrapToken;

#[async_trait]
impl SecretProvider for BootstrapToken {
    async fn resolve(
        &self,
        _context: &UserContext,
        _reference: &SecretRef,
    ) -> Result<SecretValue, SecretFailure> {
        SecretValue::new(TOKEN.as_bytes().to_vec(), None, None)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("TEAQL_SECRET_LIVE_OTLP").as_deref() != Ok("true")
        || std::env::var("TEAQL_TELEMETRY_ENABLED").as_deref() != Ok("true")
    {
        return Err("enable the controlled live Secret Provider OTLP probe explicitly".into());
    }
    let outcome = std::env::var("TEAQL_SECRET_PROBE_OUTCOME").unwrap_or_else(|_| "success".into());
    if !matches!(
        outcome.as_str(),
        "success" | "denied" | "missing" | "malformed" | "expired" | "timeout"
    ) {
        return Err("unsupported Secret Provider probe outcome".into());
    }
    let server_outcome = outcome.clone();
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
                Err(error) => panic!("loopback Vault probe failed: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        while !request.ends_with(b"\r\n\r\n") && request.len() < 8192 {
            let count = stream.read(&mut buffer).unwrap();
            if count == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..count]);
        }
        let request = String::from_utf8(request).unwrap();
        assert!(request.starts_with("GET /v1/secret/data/payment/api HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains(&format!("x-vault-token: {TOKEN}").to_ascii_lowercase())
        );
        let carrier = request
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .starts_with("traceparent:")
                    .then(|| line[12..].trim().to_owned())
            })
            .expect("the active runtime span must propagate");
        assert!(carrier.starts_with("00-") && carrier.len() == 55);
        if server_outcome == "timeout" {
            std::thread::sleep(Duration::from_millis(1800));
            return carrier;
        }
        let (status, body) = match server_outcome.as_str() {
            "denied" => ("403 Forbidden", format!("{{\"errors\":[\"{VALUE}\"]}}")),
            "missing" => ("404 Not Found", format!("{{\"errors\":[\"{VALUE}\"]}}")),
            "malformed" => ("200 OK", format!("{{\"data\":\"{VALUE}\",")),
            "expired" => (
                "200 OK",
                format!(
                    "{{\"data\":{{\"data\":{{\"value\":\"{VALUE}\"}},\"metadata\":{{\"version\":7,\"destroyed\":false,\"deletion_time\":\"2020-01-01T00:00:00Z\"}}}}}}"
                ),
            ),
            _ => (
                "200 OK",
                format!(
                    "{{\"data\":{{\"data\":{{\"value\":\"{VALUE}\"}},\"metadata\":{{\"version\":7,\"destroyed\":false,\"deletion_time\":\"\"}}}}}}"
                ),
            ),
        };
        write!(
            stream,
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        carrier
    });
    let mut profile = HttpServiceProfile::new(
        "vault",
        url::Url::parse(&format!("http://127.0.0.1:{port}/"))?,
    )?;
    if outcome == "timeout" {
        profile = profile.budgets(
            Duration::from_secs(2),
            Duration::from_millis(700),
            Duration::from_secs(3),
        )?;
    }
    let reference = SecretRef::new("payment", "api_key")?;
    let mut provider = VaultKv2SecretProvider::new(
        "vault",
        "secret",
        Arc::new(BootstrapToken),
        SecretRef::new("bootstrap", "vault_token")?,
    )?;
    provider.bind(reference.clone(), "payment/api", "value")?;
    let context = UserContext::new()
        .with_http_request_service(Arc::new(ReqwestHttpRequestService::new(vec![profile])?))
        .with_secret_provider(Arc::new(provider));
    if outcome == "success" {
        let value = context
            .resolve_secret(&reference, "verify Vault adapter")
            .await?;
        assert_eq!(value.expose_bytes()?, VALUE.as_bytes());
    } else {
        let expected = match outcome.as_str() {
            "denied" => SecretFailure::Denied,
            "missing" => SecretFailure::Missing,
            "expired" => SecretFailure::Expired,
            _ => SecretFailure::Unavailable,
        };
        assert_eq!(
            context
                .resolve_secret(&reference, "verify Vault adapter")
                .await
                .unwrap_err(),
            expected
        );
    }
    let carrier = server.join().expect("loopback Vault probe thread failed");
    context.runtime_telemetry().flush();
    context.runtime_telemetry().shutdown();
    println!("SECRET_OTLP_PROBE_PASS outcome={outcome} traceparent={carrier}");
    Ok(())
}
