//! Optional Blob infrastructure. No default runtime installation or framework dependency.
mod catalog;
mod policy;
mod retention;
pub use catalog::{
    BindingChange, BlobBinding, BlobCatalogRecord, BlobState, PostgresBlobCatalog,
    PreparedBlobBindings, UploadDraft, VerifiedContent,
};
pub use policy::{
    BlobPolicies, BlobPolicyProfile, BlobPolicySnapshot, CachePolicy, Disposition, PolicyLimits,
    PolicyOverrides, ResponseShape, Sharing, UploadMode,
};
pub use retention::BlobRetention;
use std::fmt;
use teaql_data_service::infrastructure_sql::{InfrastructureSqlError, PhysicalTransactionOutcome};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobErrorCode {
    CapabilityMissing,
    SchemaMissing,
    PolicyInvalid,
    ReferenceInvalid,
    FormatNotAllowed,
    SizeExceeded,
    UploadExpired,
    ReferenceExpired,
    UploadIncomplete,
    BindDenied,
    ReadDenied,
    NotFound,
    TransactionRequired,
    IdempotencyConflict,
    CatalogFailed,
}
impl BlobErrorCode {
    pub const fn code(self) -> &'static str {
        match self {
            Self::CapabilityMissing => "BLOB_CAPABILITY_MISSING",
            Self::SchemaMissing => "BLOB_SCHEMA_MISSING",
            Self::PolicyInvalid => "BLOB_POLICY_INVALID",
            Self::ReferenceInvalid => "BLOB_REFERENCE_INVALID",
            Self::FormatNotAllowed => "BLOB_FORMAT_NOT_ALLOWED",
            Self::SizeExceeded => "BLOB_SIZE_EXCEEDED",
            Self::UploadExpired => "BLOB_UPLOAD_EXPIRED",
            Self::ReferenceExpired => "BLOB_REFERENCE_EXPIRED",
            Self::UploadIncomplete => "BLOB_UPLOAD_INCOMPLETE",
            Self::BindDenied => "BLOB_BIND_DENIED",
            Self::ReadDenied => "BLOB_READ_DENIED",
            Self::NotFound => "BLOB_NOT_FOUND",
            Self::TransactionRequired => "BLOB_TRANSACTION_REQUIRED",
            Self::IdempotencyConflict => "BLOB_IDEMPOTENCY_CONFLICT",
            Self::CatalogFailed => "BLOB_CATALOG_FAILED",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobError {
    pub code: BlobErrorCode,
    pub completion: Option<PhysicalTransactionOutcome>,
}
impl From<BlobErrorCode> for BlobError {
    fn from(code: BlobErrorCode) -> Self {
        Self {
            code,
            completion: None,
        }
    }
}
impl From<InfrastructureSqlError> for BlobError {
    fn from(error: InfrastructureSqlError) -> Self {
        let mut result: Self = match error {
            InfrastructureSqlError::Unsupported => BlobErrorCode::CapabilityMissing,
            InfrastructureSqlError::TransactionRequired
            | InfrastructureSqlError::TransactionUnknown => BlobErrorCode::TransactionRequired,
            InfrastructureSqlError::Conflict => BlobErrorCode::IdempotencyConflict,
            _ => BlobErrorCode::CatalogFailed,
        }
        .into();
        if error == InfrastructureSqlError::TransactionUnknown {
            result.completion = Some(PhysicalTransactionOutcome::Unknown);
        }
        result
    }
}
impl fmt::Display for BlobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code.code())
    }
}
impl std::error::Error for BlobError {}
pub type BlobResult<T> = Result<T, BlobError>;

#[derive(Clone, PartialEq, Eq)]
pub struct BlobScope {
    realm: String,
    boundary: String,
}
impl BlobScope {
    pub fn new(realm: impl Into<String>, boundary: impl Into<String>) -> BlobResult<Self> {
        let value = Self {
            realm: realm.into(),
            boundary: boundary.into(),
        };
        for text in [&value.realm, &value.boundary] {
            if text.trim().is_empty()
                || text.trim() != text
                || text.len() > 256
                || text.contains('\0')
            {
                return Err(BlobErrorCode::BindDenied.into());
            }
        }
        Ok(value)
    }
    pub fn realm(&self) -> &str {
        &self.realm
    }
    pub fn boundary(&self) -> &str {
        &self.boundary
    }
}
impl fmt::Debug for BlobScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobScope([redacted])")
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlobFieldTarget {
    entity: String,
    id: Option<u64>,
    field: String,
}
impl BlobFieldTarget {
    pub fn new(
        entity: impl Into<String>,
        id: Option<u64>,
        field: impl Into<String>,
    ) -> BlobResult<Self> {
        let value = Self {
            entity: entity.into(),
            id,
            field: field.into(),
        };
        name(&value.entity, 128)?;
        name(&value.field, 128)?;
        if id.is_some_and(|value| value == 0 || value > i64::MAX as u64) {
            return Err(BlobErrorCode::ReferenceInvalid.into());
        }
        Ok(value)
    }
    pub fn entity(&self) -> &str {
        &self.entity
    }
    pub fn id(&self) -> Option<u64> {
        self.id
    }
    pub fn field(&self) -> &str {
        &self.field
    }
}
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
