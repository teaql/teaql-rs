#[cfg(unix)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::time::SystemTime;
    use teaql_runtime::{
        BoundSecretProvider, EntityKey, FixedBusinessClock, HttpCall, HttpFailure,
        HttpRequestService, HttpResponse, SecretFailure, SecretProvider, SecretRef, SecretValue,
        UserContext, VaultKv2SecretProvider,
    };

    struct ContextDateProvider(tokio::sync::Barrier);

    #[async_trait::async_trait]
    impl SecretProvider for ContextDateProvider {
        async fn resolve(
            &self,
            context: &UserContext,
            _reference: &SecretRef,
        ) -> Result<SecretValue, SecretFailure> {
            self.0.wait().await;
            SecretValue::new(context.business_date().to_string().into_bytes(), None, None)
        }
    }

    let directory = tempfile::tempdir()?;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    let key_file = directory.path().join("signing-key");
    std::fs::write(&key_file, b"private-example-key-v1")?;
    std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o600))?;
    let reference = SecretRef::new("document", "signing")?;
    let mut provider = BoundSecretProvider::new(Some(directory.path().to_path_buf()))?;
    provider.bind_file(reference.clone(), "signing-key", Some("v1".into()), None)?;
    let mut context = UserContext::new().with_secret_provider(Arc::new(provider));

    let first = context.resolve_secret(&reference, "sign document").await?;
    assert_eq!(first.expose_bytes()?, b"private-example-key-v1");
    assert_eq!(first.version(), Some("v1"));
    std::fs::write(&key_file, b"private-example-key-v2")?;
    let mut replacement = BoundSecretProvider::new(Some(directory.path().to_path_buf()))?;
    replacement.bind_file(reference.clone(), "signing-key", Some("v2".into()), None)?;
    context.set_secret_provider(Arc::new(replacement));
    let second = context
        .resolve_secret(&reference, "sign next document")
        .await?;
    assert_eq!(second.expose_bytes()?, b"private-example-key-v2");
    assert_eq!(second.version(), Some("v2"));
    assert!(!format!("{second:?}").contains("private-example-key"));
    assert_eq!(
        context.resolve_secret(&reference, " ").await.unwrap_err(),
        SecretFailure::InvalidInput
    );

    // Secret expiry is operational, even if an application freezes business time.
    let mut expired_provider = BoundSecretProvider::new(Some(directory.path().to_path_buf()))?;
    expired_provider.bind_file(
        reference.clone(),
        "signing-key",
        Some("expired".into()),
        Some(SystemTime::UNIX_EPOCH),
    )?;
    let frozen_time =
        chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).expect("valid frozen business time");
    let expired_context = UserContext::new()
        .with_business_clock(FixedBusinessClock::new(frozen_time))
        .with_secret_provider(Arc::new(expired_provider));
    assert_eq!(
        expired_context
            .resolve_secret(&reference, "check operational expiry")
            .await
            .unwrap_err(),
        SecretFailure::Expired
    );

    // One custom SPI instance serves independent Contexts concurrently, with
    // no environment, file or Vault fallback and no shared business state.
    let custom = Arc::new(ContextDateProvider(tokio::sync::Barrier::new(2)));
    let first_context = UserContext::new()
        .with_business_clock(FixedBusinessClock::new(
            chrono::DateTime::<chrono::Utc>::from_timestamp(946_684_800, 0)
                .expect("valid first business time"),
        ))
        .with_secret_provider(custom.clone());
    let second_context = UserContext::new()
        .with_business_clock(FixedBusinessClock::new(
            chrono::DateTime::<chrono::Utc>::from_timestamp(4_102_444_800, 0)
                .expect("valid second business time"),
        ))
        .with_secret_provider(custom);
    // The two graphs intentionally use the same type and ID. Sharing the
    // provider must not cause their pending mutation ledgers to alias.
    let first_state = first_context.entity_runtime_state();
    let second_state = second_context.entity_runtime_state();
    let first_task_state = first_state.clone();
    let second_task_state = second_state.clone();
    let key = EntityKey::new("School", 42_u64);
    let first_key = key.clone();
    let second_key = key.clone();
    let first_reference = reference.clone();
    let second_reference = reference.clone();
    let first_call = tokio::spawn(async move {
        let resolved = first_context
            .resolve_secret(&first_reference, "first custom context")
            .await?;
        first_task_state.set(first_key, "name", "first pending");
        Ok::<_, SecretFailure>(resolved)
    });
    let second_call = tokio::spawn(async move {
        let resolved = second_context
            .resolve_secret(&second_reference, "second custom context")
            .await?;
        second_task_state.set(second_key, "name", "second pending");
        Ok::<_, SecretFailure>(resolved)
    });
    let (first_value, second_value) =
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(first_call, second_call)
        })
        .await?;
    assert_eq!(first_value??.expose_bytes()?, b"2000-01-01");
    assert_eq!(second_value??.expose_bytes()?, b"2100-01-01");
    assert_eq!(first_state.get(&key, "name"), Some("first pending".into()));
    assert_eq!(
        second_state.get(&key, "name"),
        Some("second pending".into())
    );
    first_state.clear_current_change_set();
    assert_eq!(first_state.get(&key, "name"), None);
    assert_eq!(
        second_state.get(&key, "name"),
        Some("second pending".into())
    );
    assert!(second_state.changed_field_names(&key).contains("name"));

    struct ConcurrentVaultHttp {
        entered: tokio::sync::Barrier,
        calls: tokio::sync::Mutex<Vec<HttpCall>>,
    }

    #[async_trait::async_trait]
    impl HttpRequestService for ConcurrentVaultHttp {
        fn profile_is_encrypted_or_loopback(&self, profile: &str) -> bool {
            profile == "vault"
        }

        async fn execute(
            &self,
            caller: &UserContext,
            call: &HttpCall,
        ) -> Result<HttpResponse, HttpFailure> {
            self.calls.lock().await.push(call.clone());
            self.entered.wait().await;
            let token = call
                .headers()
                .get("X-Vault-Token")
                .expect("Vault token header");
            assert_eq!(token, &caller.business_date().to_string());
            assert_eq!(call.relative_path(), "v1/secret/data/payment/api");
            let value = match token.as_str() {
                "2000-01-01" => "value-a",
                "2100-01-01" => "value-b",
                _ => panic!("unexpected context token"),
            };
            let body = serde_json::json!({
                "data": {
                    "data": {"value": value},
                    "metadata": {"version": 7, "destroyed": false, "deletion_time": ""}
                }
            });
            HttpResponse::new(200, body.to_string().into_bytes(), 1)
        }
    }

    let vault_http = Arc::new(ConcurrentVaultHttp {
        entered: tokio::sync::Barrier::new(2),
        calls: tokio::sync::Mutex::new(Vec::new()),
    });
    let token_provider = Arc::new(ContextDateProvider(tokio::sync::Barrier::new(2)));
    let vault_reference = SecretRef::new("payment", "api_key")?;
    let mut vault = VaultKv2SecretProvider::new(
        "vault",
        "secret",
        token_provider,
        SecretRef::new("bootstrap", "vault_token")?,
    )?;
    vault.bind(vault_reference.clone(), "payment/api", "value")?;
    let vault = Arc::new(vault);
    let first_vault_context = UserContext::new()
        .with_business_clock(FixedBusinessClock::new(
            chrono::DateTime::<chrono::Utc>::from_timestamp(946_684_800, 0)
                .expect("valid first business time"),
        ))
        .with_http_request_service(vault_http.clone())
        .with_secret_provider(vault.clone());
    let second_vault_context = UserContext::new()
        .with_business_clock(FixedBusinessClock::new(
            chrono::DateTime::<chrono::Utc>::from_timestamp(4_102_444_800, 0)
                .expect("valid second business time"),
        ))
        .with_http_request_service(vault_http.clone())
        .with_secret_provider(vault);
    let first_vault_reference = vault_reference.clone();
    let first_vault = tokio::spawn(async move {
        first_vault_context
            .resolve_secret(&first_vault_reference, "first Vault context")
            .await
    });
    let second_vault = tokio::spawn(async move {
        second_vault_context
            .resolve_secret(&vault_reference, "second Vault context")
            .await
    });
    let (first_vault_value, second_vault_value) =
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(first_vault, second_vault)
        })
        .await?;
    assert_eq!(first_vault_value??.expose_bytes()?, b"value-a");
    assert_eq!(second_vault_value??.expose_bytes()?, b"value-b");
    let calls = vault_http.calls.lock().await;
    assert_eq!(calls.len(), 2);
    let tokens = calls
        .iter()
        .map(|call| call.headers().get("X-Vault-Token").unwrap().as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(tokens, ["2000-01-01", "2100-01-01"].into_iter().collect());

    println!(
        "SECRET_PROVIDER_PASS binding=1 rotation=1 masking=1 operational_expiry=1 shared_custom_spi=1 ledger_isolation=1 vault_request_isolation=1"
    );
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    panic!("The file-secret example requires Unix owner-only file permissions");
}
