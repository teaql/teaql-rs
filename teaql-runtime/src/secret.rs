//! Context-owned access to startup-bound small secrets. A request names only a
//! profile and alias; it never supplies an environment variable or file path.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::UserContext;

pub const MAX_SECRET_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SecretRef {
    profile: String,
    alias: String,
}

impl SecretRef {
    pub fn new(
        profile: impl Into<String>,
        alias: impl Into<String>,
    ) -> Result<Self, SecretFailure> {
        let profile = profile.into();
        let alias = alias.into();
        if !safe_name(&profile) || !safe_name(&alias) {
            return Err(SecretFailure::InvalidInput);
        }
        Ok(Self { profile, alias })
    }

    pub fn profile(&self) -> &str {
        &self.profile
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }
}

fn safe_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=64).contains(&bytes.len())
        && bytes[0].is_ascii_alphabetic()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'-')
}

pub struct SecretValue {
    bytes: Vec<u8>,
    version: Option<String>,
    expires_at: Option<SystemTime>,
}

impl SecretValue {
    pub fn new(
        bytes: Vec<u8>,
        version: Option<String>,
        expires_at: Option<SystemTime>,
    ) -> Result<Self, SecretFailure> {
        if bytes.is_empty() || bytes.len() > MAX_SECRET_BYTES {
            return Err(SecretFailure::InvalidInput);
        }
        if expires_at.is_some_and(|expiry| expiry <= SystemTime::now()) {
            return Err(SecretFailure::Expired);
        }
        Ok(Self {
            bytes,
            version,
            expires_at,
        })
    }

    /// Deliberate adapter-only plaintext access. Never send this value to logs.
    pub fn expose_bytes(&self) -> Result<&[u8], SecretFailure> {
        if self
            .expires_at
            .is_some_and(|expiry| expiry <= SystemTime::now())
        {
            return Err(SecretFailure::Expired);
        }
        Ok(&self.bytes)
    }

    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    pub fn expires_at(&self) -> Option<SystemTime> {
        self.expires_at
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue[masked]")
    }
}

impl fmt::Display for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue[masked]")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretFailure {
    InvalidInput,
    ProviderNotConfigured,
    UnknownBinding,
    Missing,
    Denied,
    Expired,
    Unavailable,
}

impl fmt::Display for SecretFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "secret {}",
            match self {
                Self::InvalidInput => "invalid input",
                Self::ProviderNotConfigured => "provider not configured",
                Self::UnknownBinding => "binding not configured",
                Self::Missing => "missing",
                Self::Denied => "denied",
                Self::Expired => "expired",
                Self::Unavailable => "unavailable",
            }
        )
    }
}

impl std::error::Error for SecretFailure {}

#[async_trait]
pub trait SecretProvider: Send + Sync {
    async fn resolve(
        &self,
        context: &UserContext,
        reference: &SecretRef,
    ) -> Result<SecretValue, SecretFailure>;
}

pub(crate) struct SecretProviderResource(pub Arc<dyn SecretProvider>);

#[derive(Clone)]
enum BindingSource {
    Environment(String),
    File(PathBuf),
}

#[derive(Clone)]
struct Binding {
    source: BindingSource,
    version: Option<String>,
    expires_at: Option<SystemTime>,
}

/// A no-cache baseline. File bindings are a single owner-only filename below
/// an owner-controlled directory. Every resolution re-reads its configured
/// source; replace the provider binding when rotating version metadata.
type EnvironmentLookup = dyn Fn(&str) -> Result<Vec<u8>, SecretFailure> + Send + Sync;

pub struct BoundSecretProvider {
    root: Option<PathBuf>,
    bindings: BTreeMap<SecretRef, Binding>,
    environment: Arc<EnvironmentLookup>,
}

impl BoundSecretProvider {
    pub fn new(file_root: Option<PathBuf>) -> Result<Self, SecretFailure> {
        let root = match file_root {
            Some(path) => {
                let metadata =
                    std::fs::symlink_metadata(&path).map_err(|_| SecretFailure::Unavailable)?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(SecretFailure::Denied);
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if metadata.permissions().mode() & 0o022 != 0 {
                        return Err(SecretFailure::Denied);
                    }
                }
                Some(std::fs::canonicalize(path).map_err(|_| SecretFailure::Unavailable)?)
            }
            None => None,
        };
        Ok(Self {
            root,
            bindings: BTreeMap::new(),
            environment: Arc::new(|variable| {
                std::env::var(variable)
                    .map(String::into_bytes)
                    .map_err(|error| match error {
                        std::env::VarError::NotPresent => SecretFailure::Missing,
                        std::env::VarError::NotUnicode(_) => SecretFailure::Unavailable,
                    })
            }),
        })
    }

    pub fn bind_environment(
        &mut self,
        reference: SecretRef,
        variable: impl Into<String>,
        version: Option<String>,
        expires_at: Option<SystemTime>,
    ) -> Result<(), SecretFailure> {
        let variable = variable.into();
        if variable.is_empty()
            || !variable
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(SecretFailure::InvalidInput);
        }
        self.bindings.insert(
            reference,
            Binding {
                source: BindingSource::Environment(variable),
                version,
                expires_at,
            },
        );
        Ok(())
    }

    pub fn bind_file(
        &mut self,
        reference: SecretRef,
        relative_path: impl Into<PathBuf>,
        version: Option<String>,
        expires_at: Option<SystemTime>,
    ) -> Result<(), SecretFailure> {
        if self.root.is_none() {
            return Err(SecretFailure::ProviderNotConfigured);
        }
        let relative_path = relative_path.into();
        if !valid_relative_path(&relative_path) {
            return Err(SecretFailure::InvalidInput);
        }
        self.bindings.insert(
            reference,
            Binding {
                source: BindingSource::File(relative_path),
                version,
                expires_at,
            },
        );
        Ok(())
    }
}

fn valid_relative_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && path.components().count() == 1
        && path
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
}

#[async_trait]
impl SecretProvider for BoundSecretProvider {
    async fn resolve(
        &self,
        _context: &UserContext,
        reference: &SecretRef,
    ) -> Result<SecretValue, SecretFailure> {
        let binding = self
            .bindings
            .get(reference)
            .ok_or(SecretFailure::UnknownBinding)?;
        if binding
            .expires_at
            .is_some_and(|expiry| expiry <= SystemTime::now())
        {
            return Err(SecretFailure::Expired);
        }
        let bytes = match &binding.source {
            BindingSource::Environment(variable) => (self.environment)(variable)?,
            BindingSource::File(path) => {
                let root = self
                    .root
                    .clone()
                    .ok_or(SecretFailure::ProviderNotConfigured)?;
                let path = path.clone();
                tokio::task::spawn_blocking(move || read_bound_file(&root, &path))
                    .await
                    .map_err(|_| SecretFailure::Unavailable)??
            }
        };
        SecretValue::new(bytes, binding.version.clone(), binding.expires_at)
    }
}

#[cfg(unix)]
fn read_bound_file(root: &Path, relative: &Path) -> Result<Vec<u8>, SecretFailure> {
    use std::io::Read;

    let mut candidate = root.to_path_buf();
    for part in relative.components() {
        candidate.push(part);
        let metadata = std::fs::symlink_metadata(&candidate).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                SecretFailure::Missing
            } else {
                SecretFailure::Unavailable
            }
        })?;
        if metadata.file_type().is_symlink() {
            return Err(SecretFailure::Denied);
        }
    }
    let actual = std::fs::canonicalize(&candidate).map_err(|_| SecretFailure::Unavailable)?;
    if !actual.starts_with(root) {
        return Err(SecretFailure::Denied);
    }
    let metadata = std::fs::metadata(&candidate).map_err(|_| SecretFailure::Unavailable)?;
    if !metadata.is_file() || metadata.len() > MAX_SECRET_BYTES as u64 {
        return Err(SecretFailure::Denied);
    }
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(SecretFailure::Denied);
        }
    }
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(candidate)
            .map_err(|error| {
                if error.raw_os_error() == Some(libc::ELOOP) {
                    SecretFailure::Denied
                } else {
                    SecretFailure::Unavailable
                }
            })?
    };
    {
        use std::os::unix::fs::PermissionsExt;
        let opened = file.metadata().map_err(|_| SecretFailure::Unavailable)?;
        if !opened.is_file()
            || opened.len() > MAX_SECRET_BYTES as u64
            || opened.permissions().mode() & 0o077 != 0
        {
            return Err(SecretFailure::Denied);
        }
    }
    let mut bytes = Vec::new();
    file.take((MAX_SECRET_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| SecretFailure::Unavailable)?;
    if bytes.len() > MAX_SECRET_BYTES {
        return Err(SecretFailure::Denied);
    }
    Ok(bytes)
}

#[cfg(not(unix))]
fn read_bound_file(_root: &Path, _relative: &Path) -> Result<Vec<u8>, SecretFailure> {
    Err(SecretFailure::Denied)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CustomProvider;

    #[async_trait]
    impl SecretProvider for CustomProvider {
        async fn resolve(
            &self,
            _context: &UserContext,
            _reference: &SecretRef,
        ) -> Result<SecretValue, SecretFailure> {
            SecretValue::new(b"custom".to_vec(), None, None)
        }
    }

    #[test]
    fn reference_rejects_request_supplied_paths() {
        assert!(SecretRef::new("signing", "current").is_ok());
        assert_eq!(
            SecretRef::new("signing", "../key"),
            Err(SecretFailure::InvalidInput)
        );
        assert_eq!(
            SecretRef::new("signing", "/tmp/key"),
            Err(SecretFailure::InvalidInput)
        );
    }

    #[tokio::test]
    async fn context_requires_provider_and_comment_and_masks_value() {
        let reference = SecretRef::new("signing", "current").unwrap();
        let mut context = UserContext::new();
        assert_eq!(
            context.resolve_secret(&reference, "why").await.unwrap_err(),
            SecretFailure::ProviderNotConfigured
        );
        let mut provider = BoundSecretProvider::new(None).unwrap();
        provider
            .bind_environment(
                reference.clone(),
                "TEAQL_TEST_SECRET_THAT_IS_NOT_SET",
                None,
                None,
            )
            .unwrap();
        context.set_secret_provider(Arc::new(provider));
        assert_eq!(
            context.resolve_secret(&reference, " ").await.unwrap_err(),
            SecretFailure::InvalidInput
        );
        assert_eq!(
            context.resolve_secret(&reference, "why").await.unwrap_err(),
            SecretFailure::Missing
        );
        let secret =
            SecretValue::new(b"canary-do-not-log".to_vec(), Some("v1".into()), None).unwrap();
        assert!(!format!("{secret:?} {secret}").contains("canary"));
        assert_eq!(secret.expose_bytes().unwrap(), b"canary-do-not-log");

        let custom = Arc::new(CustomProvider);
        let first = UserContext::new().with_secret_provider(custom.clone());
        let second = UserContext::new().with_secret_provider(custom);
        assert_eq!(
            first
                .resolve_secret(&reference, "first")
                .await
                .unwrap()
                .expose_bytes()
                .unwrap(),
            b"custom"
        );
        assert_eq!(
            second
                .resolve_secret(&reference, "second")
                .await
                .unwrap()
                .expose_bytes()
                .unwrap(),
            b"custom"
        );
    }

    #[tokio::test]
    async fn expiration_and_unknown_binding_fail_closed() {
        let reference = SecretRef::new("signing", "key").unwrap();
        let provider = BoundSecretProvider::new(None).unwrap();
        let context = UserContext::new().with_secret_provider(Arc::new(provider));
        assert_eq!(
            context
                .resolve_secret(&reference, "read")
                .await
                .unwrap_err(),
            SecretFailure::UnknownBinding
        );
        assert_eq!(
            SecretValue::new(b"x".to_vec(), None, Some(SystemTime::UNIX_EPOCH)).unwrap_err(),
            SecretFailure::Expired
        );
    }

    #[tokio::test]
    async fn environment_binding_uses_only_configured_variable_and_can_refresh() {
        let reference = SecretRef::new("signing", "key").unwrap();
        let current = Arc::new(std::sync::Mutex::new(b"first".to_vec()));
        let lookup = current.clone();
        let mut provider = BoundSecretProvider::new(None).unwrap();
        provider.environment = Arc::new(move |variable| {
            if variable != "CONFIGURED_KEY" {
                return Err(SecretFailure::UnknownBinding);
            }
            Ok(lookup.lock().unwrap().clone())
        });
        provider
            .bind_environment(reference.clone(), "CONFIGURED_KEY", None, None)
            .unwrap();
        let context = UserContext::new().with_secret_provider(Arc::new(provider));
        assert_eq!(
            context
                .resolve_secret(&reference, "read")
                .await
                .unwrap()
                .expose_bytes()
                .unwrap(),
            b"first"
        );
        *current.lock().unwrap() = b"second".to_vec();
        assert_eq!(
            context
                .resolve_secret(&reference, "refresh")
                .await
                .unwrap()
                .expose_bytes()
                .unwrap(),
            b"second"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_rotation_permissions_and_symlinks() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let root = std::env::temp_dir().join(format!(
            "teaql-secret-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let file = root.join("key");
        std::fs::write(&file, b"first").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let reference = SecretRef::new("signing", "key").unwrap();
        let mut provider = BoundSecretProvider::new(Some(root.clone())).unwrap();
        assert_eq!(
            provider.bind_file(reference.clone(), "../key", None, None),
            Err(SecretFailure::InvalidInput)
        );
        assert_eq!(
            provider.bind_file(reference.clone(), "nested/key", None, None),
            Err(SecretFailure::InvalidInput)
        );
        provider
            .bind_file(reference.clone(), "key", Some("v1".into()), None)
            .unwrap();
        let mut context = UserContext::new().with_secret_provider(Arc::new(provider));
        assert_eq!(
            context
                .resolve_secret(&reference, "read signing key")
                .await
                .unwrap()
                .expose_bytes()
                .unwrap(),
            b"first"
        );
        std::fs::write(&file, b"second").unwrap();
        let mut replacement = BoundSecretProvider::new(Some(root.clone())).unwrap();
        replacement
            .bind_file(reference.clone(), "key", Some("v2".into()), None)
            .unwrap();
        context.set_secret_provider(Arc::new(replacement));
        let rotated = context
            .resolve_secret(&reference, "refresh signing key")
            .await
            .unwrap();
        assert_eq!(rotated.expose_bytes().unwrap(), b"second");
        assert_eq!(rotated.version(), Some("v2"));
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            context
                .resolve_secret(&reference, "read signing key")
                .await
                .unwrap_err(),
            SecretFailure::Denied
        );
        std::fs::remove_file(&file).unwrap();
        symlink(root.join("missing"), &file).unwrap();
        assert_eq!(
            context
                .resolve_secret(&reference, "read signing key")
                .await
                .unwrap_err(),
            SecretFailure::Denied
        );
        std::fs::remove_file(&file).unwrap();
        std::fs::remove_dir(&root).unwrap();
    }
}
