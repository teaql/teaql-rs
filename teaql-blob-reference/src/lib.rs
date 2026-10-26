//! Optional Blob infrastructure. No default runtime installation or framework dependency.
mod catalog;
mod policy;
mod retention;
#[cfg(feature = "runtime-integration")]
mod upload_service;
pub use catalog::{
    BindingChange, BlobBinding, BlobCatalogRecord, BlobState, BlobUploadLease, PostgresBlobCatalog,
    PreparedBlobBindings, UploadDraft, VerifiedContent,
};
pub use policy::{
    BlobPolicies, BlobPolicyProfile, BlobPolicySnapshot, PolicyLimits, PolicyOverrides,
    ResponseShape, Sharing, UploadMode,
};
pub use retention::BlobRetention;
pub use teaql_blob_contracts::*;
#[cfg(feature = "runtime-integration")]
pub use upload_service::*;

pub(crate) fn name(text: &str, max: usize) -> BlobResult<()> {
    if text.trim().is_empty()
        || text.trim() != text
        || text.chars().count() > max
        || text
            .chars()
            .any(|c| c <= '\u{1f}' || ('\u{7f}'..='\u{9f}').contains(&c))
    {
        Err(BlobErrorCode::ReferenceInvalid.into())
    } else {
        Ok(())
    }
}
pub(crate) fn add_seconds(now: i64, seconds: u64) -> BlobResult<i64> {
    seconds
        .checked_mul(1000)
        .and_then(|value| i64::try_from(value).ok())
        .and_then(|value| now.checked_add(value))
        .ok_or_else(|| BlobErrorCode::PolicyInvalid.into())
}
