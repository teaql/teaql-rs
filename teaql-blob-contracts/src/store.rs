//! Trusted transport SPI. The orchestrator authorizes every operation before calling a store.
//! Locators and content versions are private infrastructure values, never write credentials.
use crate::{BlobErrorCode as Code, BlobResult, CachePolicy, Disposition};
use std::{fmt, pin::Pin, time::Duration};
pub use teaql_core::object_storage::Bytes;

pub type BlobContentStream = Pin<Box<dyn futures_core::Stream<Item = BlobResult<Bytes>> + Send>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobStoreCapabilities {
    pub direct_upload: bool,
    pub range: bool,
    pub signed_reads: bool,
    pub context_checked_reads: bool,
    pub version_conditional_delete: bool,
    pub maximum_read_ttl: Duration,
}

#[derive(Clone, PartialEq, Eq)]
pub struct BlobUploadTarget {
    upload_id: String,
    location: String,
}
impl BlobUploadTarget {
    pub fn new(upload_id: impl Into<String>, location: impl Into<String>) -> BlobResult<Self> {
        let value = Self {
            upload_id: upload_id.into(),
            location: location.into(),
        };
        text(&value.upload_id)?;
        text(&value.location)?;
        Ok(value)
    }
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }
    pub fn location(&self) -> &str {
        &self.location
    }
}
impl fmt::Debug for BlobUploadTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobUploadTarget([redacted])")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StoredBlob {
    location: String,
    content_version: String,
    size_bytes: u64,
    sha256: String,
}
impl StoredBlob {
    pub fn new(
        location: impl Into<String>,
        content_version: impl Into<String>,
        size_bytes: u64,
        sha256: impl Into<String>,
    ) -> BlobResult<Self> {
        let value = Self {
            location: location.into(),
            content_version: content_version.into(),
            size_bytes,
            sha256: sha256.into(),
        };
        text(&value.location)?;
        text(&value.content_version)?;
        if value.sha256.len() != 64
            || !value
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Code::ReferenceInvalid.into());
        }
        Ok(value)
    }
    pub fn location(&self) -> &str {
        &self.location
    }
    pub fn content_version(&self) -> &str {
        &self.content_version
    }
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}
impl fmt::Debug for StoredBlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StoredBlob([redacted])")
    }
}

pub struct BlobRangeContent {
    pub content: StoredBlob,
    pub offset: u64,
    pub length: u64,
    pub stream: BlobContentStream,
}
impl fmt::Debug for BlobRangeContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobRangeContent([redacted])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobDeleteResult {
    Deleted,
    AlreadyMissing,
    VersionMismatch,
}

#[derive(Clone)]
pub struct BlobReadRequest {
    location: String,
    content_version: String,
    content_type: String,
    file_name: String,
    disposition: Disposition,
    lifetime: Duration,
    cache: CachePolicy,
}
impl BlobReadRequest {
    pub fn new(
        location: impl Into<String>,
        content_version: impl Into<String>,
        content_type: impl Into<String>,
        file_name: impl Into<String>,
        disposition: Disposition,
        lifetime: Duration,
        cache: CachePolicy,
    ) -> BlobResult<Self> {
        let value = Self {
            location: location.into(),
            content_version: content_version.into(),
            content_type: content_type.into(),
            file_name: file_name.into(),
            disposition,
            lifetime,
            cache,
        };
        text(&value.location)?;
        text(&value.content_version)?;
        text(&value.file_name)?;
        if value.file_name.len() > 1024
            || value.content_type.len() > 200
            || !value.content_type.split_once('/').is_some_and(|(a, b)| {
                !a.is_empty()
                    && !b.is_empty()
                    && a.bytes().chain(b.bytes()).all(|c| {
                        c.is_ascii_lowercase() || c.is_ascii_digit() || b".+-".contains(&c)
                    })
            })
            || disposition == Disposition::Auto
            || lifetime.is_zero()
            || lifetime.subsec_nanos() != 0
            || lifetime.as_millis() > i64::MAX as u128
            || matches!(cache, CachePolicy::PrivateMaxAge(age) if age > lifetime.as_secs())
        {
            return Err(Code::PolicyInvalid.into());
        }
        Ok(value)
    }
    pub fn location(&self) -> &str {
        &self.location
    }
    pub fn content_version(&self) -> &str {
        &self.content_version
    }
    pub fn content_type(&self) -> &str {
        &self.content_type
    }
    pub fn file_name(&self) -> &str {
        &self.file_name
    }
    pub fn disposition(&self) -> Disposition {
        self.disposition
    }
    pub fn lifetime(&self) -> Duration {
        self.lifetime
    }
    pub fn cache(&self) -> &CachePolicy {
        &self.cache
    }
}
impl fmt::Debug for BlobReadRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobReadRequest([redacted])")
    }
}
#[derive(Clone)]
pub struct SignedBlobRead {
    url: String,
    expires_at_millis: i64,
}
impl SignedBlobRead {
    pub fn new(url: impl Into<String>, expires_at_millis: i64) -> BlobResult<Self> {
        let url = url.into();
        if url.len() > 16384
            || expires_at_millis <= 0
            || url.chars().any(|c| c.is_control() || c.is_whitespace())
            || url.split_once("://").is_some_and(|(_, rest)| {
                rest.split(['/', '?', '#'])
                    .next()
                    .is_some_and(|authority| authority.contains('@'))
            })
        {
            return Err(Code::SignFailed.into());
        }
        let parsed = url::Url::parse(&url).map_err(|_| crate::BlobError::from(Code::SignFailed))?;
        if !["http", "https"].contains(&parsed.scheme())
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
            || parsed.port() == Some(0)
        {
            return Err(Code::SignFailed.into());
        }
        Ok(Self {
            url,
            expires_at_millis,
        })
    }
    pub fn as_str(&self) -> &str {
        &self.url
    }
    pub fn expires_at_millis(&self) -> i64 {
        self.expires_at_millis
    }
}
impl fmt::Debug for SignedBlobRead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SignedBlobRead([redacted])")
    }
}

#[async_trait::async_trait]
pub trait BlobStore: Send + Sync {
    /// Explicit, read-only readiness. Construction must remain passive.
    async fn validate_ready(&self) -> BlobResult<()>;
    fn capabilities(&self) -> BlobStoreCapabilities;
    fn maximum_write_bytes(&self) -> u64;
    async fn create_upload(&self, upload_id: &str) -> BlobResult<BlobUploadTarget>;
    /// Bounded immutable write. An identical retry returns the original version.
    async fn write(
        &self,
        target: &BlobUploadTarget,
        source: BlobContentStream,
        maximum_bytes: u64,
    ) -> BlobResult<StoredBlob>;
    async fn stat(&self, location: &str) -> BlobResult<Option<StoredBlob>>;
    /// A committed deleting claim precedes this call. The returned generation is
    /// fenced against delayed publication before absence may acknowledge deletion.
    async fn seal_for_deletion(
        &self,
        location: &str,
        expected_version: Option<&str>,
    ) -> BlobResult<Option<StoredBlob>>;
    async fn open_range(
        &self,
        location: &str,
        expected_version: &str,
        offset: u64,
        length: u64,
    ) -> BlobResult<BlobRangeContent>;
    /// Atomic version precondition; stat plus unconditional delete is insufficient.
    async fn delete_if_version(
        &self,
        location: &str,
        expected_version: &str,
    ) -> BlobResult<BlobDeleteResult>;
    /// Return complete absolute URLs and actual expiries. Unsupported stores fail explicitly.
    async fn sign_read_batch(
        &self,
        _requests: &[BlobReadRequest],
    ) -> BlobResult<Vec<SignedBlobRead>> {
        Err(Code::CapabilityMissing.into())
    }
}

fn text(value: &str) -> BlobResult<()> {
    if value.trim().is_empty() || value.len() > 2048 || value.chars().any(char::is_control) {
        Err(Code::ReferenceInvalid.into())
    } else {
        Ok(())
    }
}
