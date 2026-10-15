use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use super::{MAX_SECRET_BYTES, SecretFailure, SecretProvider, SecretRef, SecretValue, safe_name};
use crate::{HttpCall, HttpIntentKind, HttpMethod, HttpRetryMode, UserContext};

struct VaultBinding {
    path: String,
    field: String,
}

/// Explicit Vault KV v2 adapter. Its HTTP profile and every secret path are
/// fixed at trusted startup; the request can choose only a registered alias.
pub struct VaultKv2SecretProvider {
    http_profile: String,
    mount: String,
    token_provider: Arc<dyn SecretProvider>,
    token_reference: SecretRef,
    bindings: BTreeMap<SecretRef, VaultBinding>,
}

impl VaultKv2SecretProvider {
    pub fn new(
        http_profile: impl Into<String>,
        mount: impl Into<String>,
        token_provider: Arc<dyn SecretProvider>,
        token_reference: SecretRef,
    ) -> Result<Self, SecretFailure> {
        let http_profile = http_profile.into();
        let mount = mount.into();
        if !safe_name(&http_profile) || !safe_name(&mount) {
            return Err(SecretFailure::InvalidInput);
        }
        Ok(Self {
            http_profile,
            mount,
            token_provider,
            token_reference,
            bindings: BTreeMap::new(),
        })
    }

    pub fn bind(
        &mut self,
        reference: SecretRef,
        path: impl Into<String>,
        field: impl Into<String>,
    ) -> Result<(), SecretFailure> {
        let path = path.into();
        let field = field.into();
        if !valid_vault_path(&path) || !safe_name(&field) {
            return Err(SecretFailure::InvalidInput);
        }
        self.bindings
            .insert(reference, VaultBinding { path, field });
        Ok(())
    }
}

fn valid_vault_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 256
        && path.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        })
}

#[async_trait]
impl SecretProvider for VaultKv2SecretProvider {
    async fn resolve(
        &self,
        context: &UserContext,
        reference: &SecretRef,
    ) -> Result<SecretValue, SecretFailure> {
        let binding = self
            .bindings
            .get(reference)
            .ok_or(SecretFailure::UnknownBinding)?;
        let token = self
            .token_provider
            .resolve(context, &self.token_reference)
            .await?;
        let token =
            std::str::from_utf8(token.expose_bytes()?).map_err(|_| SecretFailure::Unavailable)?;
        if token.is_empty() || token.chars().any(char::is_control) {
            return Err(SecretFailure::Unavailable);
        }
        let call = HttpCall::new(
            &self.http_profile,
            HttpMethod::Get,
            format!("v1/{}/data/{}", self.mount, binding.path),
            BTreeMap::from([("X-Vault-Token".to_owned(), token.to_owned())]),
            Vec::new(),
            None,
            "read configured Vault secret",
            HttpIntentKind::Purpose,
            "resolve runtime credential",
            HttpRetryMode::None,
        )
        .map_err(|_| SecretFailure::Unavailable)?;
        let response = context
            .http_requests()
            .map_err(|_| SecretFailure::Unavailable)?
            .execute(context, &call)
            .await
            .map_err(|_| SecretFailure::Unavailable)?;
        match response.status() {
            200 => parse_vault_response(response.body(), &binding.field),
            401 | 403 => Err(SecretFailure::Denied),
            404 => Err(SecretFailure::Missing),
            _ => Err(SecretFailure::Unavailable),
        }
    }
}

fn parse_vault_response(body: &[u8], field: &str) -> Result<SecretValue, SecretFailure> {
    if body.len() > 64 * 1024 {
        return Err(SecretFailure::Unavailable);
    }
    let document: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| SecretFailure::Unavailable)?;
    let data = document
        .get("data")
        .and_then(|value| value.as_object())
        .ok_or(SecretFailure::Unavailable)?;
    let metadata = data
        .get("metadata")
        .and_then(|value| value.as_object())
        .ok_or(SecretFailure::Unavailable)?;
    let destroyed = metadata
        .get("destroyed")
        .and_then(|value| value.as_bool())
        .ok_or(SecretFailure::Unavailable)?;
    if destroyed {
        return Err(SecretFailure::Missing);
    }
    let version = metadata
        .get("version")
        .and_then(|value| value.as_u64())
        .filter(|version| *version > 0 && *version <= i64::MAX as u64)
        .ok_or(SecretFailure::Unavailable)?;
    let deletion_time = metadata
        .get("deletion_time")
        .and_then(|value| value.as_str())
        .ok_or(SecretFailure::Unavailable)?;
    let mut expires_at = if deletion_time.is_empty() {
        None
    } else {
        let parsed = chrono::DateTime::parse_from_rfc3339(deletion_time)
            .map_err(|_| SecretFailure::Unavailable)?;
        Some(SystemTime::from(parsed))
    };
    let lease = document
        .get("lease_duration")
        .map(|value| value.as_u64().ok_or(SecretFailure::Unavailable))
        .transpose()?
        .unwrap_or(0);
    if lease > 0 {
        let lease_expiry = SystemTime::now()
            .checked_add(Duration::from_secs(lease))
            .ok_or(SecretFailure::Unavailable)?;
        expires_at = Some(expires_at.map_or(lease_expiry, |expiry| expiry.min(lease_expiry)));
    }
    let value = data
        .get("data")
        .and_then(|value| value.get(field))
        .and_then(|value| value.as_str())
        .ok_or(SecretFailure::Missing)?;
    if value.is_empty() || value.len() > MAX_SECRET_BYTES {
        return Err(SecretFailure::Unavailable);
    }
    SecretValue::new(
        value.as_bytes().to_vec(),
        Some(format!("vault-kv2:{version}")),
        expires_at,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HttpFailure, HttpRequestService, HttpResponse};
    use std::sync::Mutex;

    struct FixedToken;

    #[async_trait]
    impl SecretProvider for FixedToken {
        async fn resolve(
            &self,
            _context: &UserContext,
            _reference: &SecretRef,
        ) -> Result<SecretValue, SecretFailure> {
            SecretValue::new(b"PRIVATE-VAULT-TOKEN".to_vec(), None, None)
        }
    }

    struct ScriptedHttp {
        result: Result<HttpResponse, HttpFailure>,
        call: Mutex<Option<HttpCall>>,
    }

    #[async_trait]
    impl HttpRequestService for ScriptedHttp {
        async fn execute(
            &self,
            _context: &UserContext,
            call: &HttpCall,
        ) -> Result<HttpResponse, HttpFailure> {
            *self.call.lock().unwrap() = Some(call.clone());
            self.result.clone()
        }
    }

    fn provider() -> (VaultKv2SecretProvider, SecretRef) {
        let reference = SecretRef::new("payments", "api_key").unwrap();
        let mut provider = VaultKv2SecretProvider::new(
            "vault",
            "secret",
            Arc::new(FixedToken),
            SecretRef::new("bootstrap", "vault_token").unwrap(),
        )
        .unwrap();
        provider
            .bind(reference.clone(), "payments/api", "value")
            .unwrap();
        (provider, reference)
    }

    #[test]
    fn path_binding_rejects_escape_and_dynamic_url_syntax() {
        for path in [
            "../key",
            "/key",
            "key?version=1",
            "key%2Fother",
            "key\\other",
            "a//b",
        ] {
            assert!(!valid_vault_path(path));
        }
        assert!(valid_vault_path("payments/service-key"));
    }

    #[test]
    fn kv_v2_payload_requires_version_and_respects_deletion() {
        let body = br#"{"data":{"data":{"value":"secret"},"metadata":{"version":7,"destroyed":false,"deletion_time":""}}}"#;
        let value = parse_vault_response(body, "value").unwrap();
        assert_eq!(value.expose_bytes().unwrap(), b"secret");
        assert_eq!(value.version(), Some("vault-kv2:7"));
        assert_eq!(
            parse_vault_response(b"{}", "value").unwrap_err(),
            SecretFailure::Unavailable
        );
        let deleted = br#"{"data":{"data":{"value":"secret"},"metadata":{"version":7,"destroyed":false,"deletion_time":"2020-01-01T00:00:00Z"}}}"#;
        assert_eq!(
            parse_vault_response(deleted, "value").unwrap_err(),
            SecretFailure::Expired
        );
        let destroyed = br#"{"data":{"data":{"value":"secret"},"metadata":{"version":7,"destroyed":true,"deletion_time":""}}}"#;
        assert_eq!(
            parse_vault_response(destroyed, "value").unwrap_err(),
            SecretFailure::Missing
        );
        let bad_lease = br#"{"lease_duration":-1,"data":{"data":{"value":"secret"},"metadata":{"version":7,"destroyed":false,"deletion_time":""}}}"#;
        assert_eq!(
            parse_vault_response(bad_lease, "value").unwrap_err(),
            SecretFailure::Unavailable
        );
    }

    #[tokio::test]
    async fn context_resolution_uses_bound_path_and_masks_diagnostics() {
        let (provider, reference) = provider();
        let response = br#"{"data":{"data":{"value":"PRIVATE-VAULT-VALUE"},"metadata":{"version":7,"destroyed":false,"deletion_time":""}}}"#;
        let http = Arc::new(ScriptedHttp {
            result: Ok(HttpResponse::new(200, response.to_vec(), 1).unwrap()),
            call: Mutex::new(None),
        });
        let context = UserContext::new()
            .with_http_request_service(http.clone())
            .with_secret_provider(Arc::new(provider));
        let value = context
            .resolve_secret(&reference, "read payment credential")
            .await
            .unwrap();
        assert_eq!(value.expose_bytes().unwrap(), b"PRIVATE-VAULT-VALUE");
        assert_eq!(value.version(), Some("vault-kv2:7"));
        assert!(!format!("{value:?}").contains("PRIVATE-VAULT-VALUE"));
        let call = http.call.lock().unwrap().clone().unwrap();
        assert_eq!(call.profile(), "vault");
        assert_eq!(call.relative_path(), "v1/secret/data/payments/api");
        assert_eq!(
            call.headers().get("X-Vault-Token").map(String::as_str),
            Some("PRIVATE-VAULT-TOKEN")
        );
        assert_eq!(call.retry_mode(), HttpRetryMode::None);
        assert!(!format!("{call:?}").contains("PRIVATE-VAULT-TOKEN"));
        assert_eq!(
            context
                .resolve_secret(&SecretRef::new("payments", "unknown").unwrap(), "read")
                .await
                .unwrap_err(),
            SecretFailure::UnknownBinding
        );
    }

    #[tokio::test]
    async fn vault_failures_are_distinct_and_never_reveal_payloads() {
        for (result, expected) in [
            (
                Ok(HttpResponse::new(403, b"PRIVATE-VAULT-VALUE".to_vec(), 1).unwrap()),
                SecretFailure::Denied,
            ),
            (
                Ok(HttpResponse::new(404, b"PRIVATE-VAULT-VALUE".to_vec(), 1).unwrap()),
                SecretFailure::Missing,
            ),
            (
                Ok(HttpResponse::new(200, b"PRIVATE-VAULT-VALUE".to_vec(), 1).unwrap()),
                SecretFailure::Unavailable,
            ),
            (Err(HttpFailure::Timeout), SecretFailure::Unavailable),
        ] {
            let (provider, reference) = provider();
            let context = UserContext::new()
                .with_http_request_service(Arc::new(ScriptedHttp {
                    result,
                    call: Mutex::new(None),
                }))
                .with_secret_provider(Arc::new(provider));
            let failure = context
                .resolve_secret(&reference, "read payment credential")
                .await
                .unwrap_err();
            assert_eq!(failure, expected);
            assert!(!format!("{failure:?} {failure}").contains("PRIVATE-VAULT"));
        }
    }
}
