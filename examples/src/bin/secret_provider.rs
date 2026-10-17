#[cfg(unix)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::time::SystemTime;
    use teaql_runtime::{
        BoundSecretProvider, FixedBusinessClock, SecretFailure, SecretProvider, SecretRef,
        SecretValue, UserContext,
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
    let first_reference = reference.clone();
    let second_reference = reference.clone();
    let first_call = tokio::spawn(async move {
        first_context
            .resolve_secret(&first_reference, "first custom context")
            .await
    });
    let second_call = tokio::spawn(async move {
        second_context
            .resolve_secret(&second_reference, "second custom context")
            .await
    });
    let (first_value, second_value) =
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(first_call, second_call)
        })
        .await?;
    assert_eq!(first_value??.expose_bytes()?, b"2000-01-01");
    assert_eq!(second_value??.expose_bytes()?, b"2100-01-01");

    println!(
        "SECRET_PROVIDER_PASS binding=1 rotation=1 masking=1 operational_expiry=1 shared_custom_spi=1"
    );
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    panic!("The file-secret example requires Unix owner-only file permissions");
}
