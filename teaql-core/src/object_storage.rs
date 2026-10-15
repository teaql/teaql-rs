//! Provider-neutral binary storage values. No S3 SDK or business tenancy in core.
pub use bytes::Bytes;
use std::{
    collections::BTreeMap,
    fmt,
    pin::Pin,
    time::{Duration, SystemTime},
};
pub type ContentStream =
    Pin<Box<dyn futures_core::Stream<Item = Result<Bytes, StorageError>> + Send>>;
pub const MAX_PUT_SIZE: u64 = 5 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageErrorCode {
    InvalidInput,
    NotConfigured,
    NotFound,
    AccessDenied,
    Conflict,
    Timeout,
    Unavailable,
    Unsupported,
    TooLarge,
    Io,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageError(pub StorageErrorCode);
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Object storage: {:?}", self.0)
    }
}
impl std::error::Error for StorageError {}
fn invalid() -> StorageError {
    StorageError(StorageErrorCode::InvalidInput)
}

#[derive(Clone, PartialEq, Eq)]
pub struct ObjectKey(String);
impl ObjectKey {
    pub fn new(value: impl Into<String>) -> Result<Self, StorageError> {
        let value = value.into();
        if value.is_empty() || value.len() > 1024 || value.chars().any(char::is_control) {
            return Err(invalid());
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ObjectKey[masked]")
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    CreateOnly,
    Replace,
}
#[derive(Clone)]
pub struct PutOptions {
    size: u64,
    content_type: String,
    metadata: BTreeMap<String, String>,
    mode: WriteMode,
}
impl PutOptions {
    pub fn new(size: u64, mode: WriteMode) -> Result<Self, StorageError> {
        if size > MAX_PUT_SIZE {
            return Err(StorageError(StorageErrorCode::TooLarge));
        }
        Ok(Self {
            size,
            mode,
            content_type: "application/octet-stream".into(),
            metadata: BTreeMap::new(),
        })
    }
    pub fn with_content_type(mut self, value: impl Into<String>) -> Result<Self, StorageError> {
        let value = value.into();
        header(&value)?;
        self.content_type = value;
        Ok(self)
    }
    pub fn with_metadata(
        mut self,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self, StorageError> {
        for (key, value) in &metadata {
            if key.is_empty()
                || !key
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                || key.starts_with('-')
            {
                return Err(invalid());
            }
            header(value)?;
        }
        self.metadata = metadata;
        Ok(self)
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn mode(&self) -> WriteMode {
        self.mode
    }
    pub fn content_type(&self) -> &str {
        &self.content_type
    }
    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }
}
impl fmt::Debug for PutOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PutOptions[size={}, mode={:?}, details=masked]",
            self.size, self.mode
        )
    }
}
#[derive(Clone)]
pub struct ObjectMetadata {
    pub key: ObjectKey,
    pub size: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<SystemTime>,
    pub custom_metadata: BTreeMap<String, String>,
}
impl fmt::Debug for ObjectMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectMetadata[size={}, details=masked]", self.size)
    }
}
pub struct ObjectContent {
    pub metadata: ObjectMetadata,
    pub stream: ContentStream,
}
#[derive(Debug, Clone, Copy)]
pub enum Disposition {
    Inline,
    Attachment,
}
#[derive(Clone)]
pub struct ReadUrlOptions {
    lifetime: Duration,
    content_disposition: Option<String>,
}
impl Default for ReadUrlOptions {
    fn default() -> Self {
        Self {
            lifetime: Duration::from_secs(300),
            content_disposition: None,
        }
    }
}
impl ReadUrlOptions {
    pub fn new(lifetime: Duration) -> Result<Self, StorageError> {
        if lifetime.is_zero() || lifetime.subsec_nanos() != 0 || lifetime.as_secs() > 604800 {
            return Err(invalid());
        }
        Ok(Self {
            lifetime,
            content_disposition: None,
        })
    }
    pub fn with_disposition(
        mut self,
        disposition: Disposition,
        filename: Option<&str>,
    ) -> Result<Self, StorageError> {
        let mut value = match disposition {
            Disposition::Inline => "inline",
            Disposition::Attachment => "attachment",
        }
        .to_owned();
        if let Some(filename) = filename {
            header(filename)?;
            if filename.contains(['/', '\\', '"']) {
                return Err(invalid());
            }
            value.push_str(&format!("; filename=\"{filename}\""));
        }
        self.content_disposition = Some(value);
        Ok(self)
    }
    pub fn lifetime(&self) -> Duration {
        self.lifetime
    }
    pub fn content_disposition(&self) -> Option<&str> {
        self.content_disposition.as_deref()
    }
}
#[derive(Clone)]
pub struct ObjectReadUrl {
    url: String,
    pub expires_at: SystemTime,
}
impl ObjectReadUrl {
    pub fn new(url: String, expires_at: SystemTime) -> Self {
        Self { url, expires_at }
    }
    pub fn as_str(&self) -> &str {
        &self.url
    }
}
impl fmt::Debug for ObjectReadUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ObjectReadUrl[url=masked, expires_at={:?}]",
            self.expires_at
        )
    }
}
fn header(value: &str) -> Result<(), StorageError> {
    if value.is_empty() || !value.bytes().all(|b| (32..=126).contains(&b)) {
        Err(invalid())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn object_storage_validation_and_private_debug() {
        for key in ["".to_owned(), "x".repeat(1025), "nul\0key".into()] {
            assert!(ObjectKey::new(key).is_err());
        }
        assert!(PutOptions::new(MAX_PUT_SIZE + 1, WriteMode::Replace).is_err());
        assert!(
            PutOptions::new(0, WriteMode::CreateOnly)
                .unwrap()
                .with_content_type("x\r\ny")
                .is_err()
        );
        assert!(
            PutOptions::new(0, WriteMode::CreateOnly)
                .unwrap()
                .with_metadata(BTreeMap::from([("BAD".into(), "value".into())]))
                .is_err()
        );
        for ttl in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_secs(604801),
        ] {
            assert!(ReadUrlOptions::new(ttl).is_err());
        }
        assert!(
            ReadUrlOptions::default()
                .with_disposition(Disposition::Attachment, Some("bad\r\nname"))
                .is_err()
        );
        assert!(!format!("{:?}", ObjectKey::new("PRIVATE-KEY").unwrap()).contains("PRIVATE-KEY"));
        let url = ObjectReadUrl::new(
            "https://example.invalid/?secret=PRIVATE".into(),
            SystemTime::now(),
        );
        assert!(!format!("{url:?}").contains("PRIVATE"));
    }
}
