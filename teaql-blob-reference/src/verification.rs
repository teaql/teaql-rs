use crate::{BlobContentStream, BlobErrorCode as Code, BlobResult};
use std::fmt;

/// Actual bytes/format facts from a trusted verifier. Never deserializable client input.
#[derive(Clone, PartialEq, Eq)]
pub struct VerifiedBlobContent {
    content_type: String,
    size_bytes: u64,
    sha256: String,
}
impl VerifiedBlobContent {
    pub fn new(
        content_type: impl Into<String>,
        size_bytes: u64,
        sha256: impl Into<String>,
    ) -> BlobResult<Self> {
        let value = Self {
            content_type: content_type.into(),
            size_bytes,
            sha256: sha256.into(),
        };
        if !crate::policy::FORMATS.into_iter().any(|extension| {
            teaql_core::blob::content_type(extension) == Some(value.content_type.as_str())
        }) || value.sha256.len() != 64
            || !value
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Code::FormatNotAllowed.into());
        }
        Ok(value)
    }
    pub fn content_type(&self) -> &str {
        &self.content_type
    }
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}
impl fmt::Debug for VerifiedBlobContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VerifiedBlobContent([redacted])")
    }
}
#[async_trait::async_trait]
pub trait BlobContentVerifier: Send + Sync {
    async fn validate_ready(&self) -> BlobResult<()>;
    async fn verify(
        &self,
        source: BlobContentStream,
        extension: &str,
        maximum_bytes: u64,
        allow_empty: bool,
    ) -> BlobResult<VerifiedBlobContent>;
}
