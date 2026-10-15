#[cfg(unix)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use teaql_runtime::{BoundSecretProvider, SecretFailure, SecretRef, UserContext};

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
    println!("SECRET_PROVIDER_PASS binding=1 rotation=1 masking=1");
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    panic!("The file-secret example requires Unix owner-only file permissions");
}
