//! Explicit, framework-independent preparation after business transactions complete.
mod document;
mod graph;
mod service;

pub use document::{AcceptedDocumentView, DocumentView, DocumentViewError};
pub use graph::{
    AuthorizedResponseGraph, ViewBlobTarget, ViewEntity, ViewEntityTarget, ViewField, ViewInput,
    ViewPath, ViewPathSegment,
};
pub use service::{
    BlobViewProjection, BlobViewRead, CurrentBlobViewValidation, CurrentDocumentViewValidation,
    DocumentContext, DocumentViewProjection, ViewBlobContributor, ViewDocumentContributor,
    ViewPreparationService,
};

use crate::UserContext;
use chrono::{DateTime, Utc};
use std::{fmt, sync::Arc, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewMode {
    ReadOnly,
    Governed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BusinessCompletion {
    Query,
    Committed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewErrorCode {
    CapabilityMissing,
    AccessDenied,
    TransactionUnfinished,
    ResultUnsupported,
    LimitExceeded,
    PreparedMismatch,
    PreparationFailed,
    Expired,
}
impl ViewErrorCode {
    pub const fn code(self) -> &'static str {
        match self {
            Self::CapabilityMissing => "VIEW_CAPABILITY_MISSING",
            Self::AccessDenied => "VIEW_ACCESS_DENIED",
            Self::TransactionUnfinished => "VIEW_TRANSACTION_UNFINISHED",
            Self::ResultUnsupported => "VIEW_RESULT_UNSUPPORTED",
            Self::LimitExceeded => "VIEW_LIMIT_EXCEEDED",
            Self::PreparedMismatch => "VIEW_PREPARED_MISMATCH",
            Self::PreparationFailed => "VIEW_PREPARATION_FAILED",
            Self::Expired => "VIEW_EXPIRED",
        }
    }
}
impl fmt::Display for ViewErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for ViewErrorCode {}

/// Contains no provider cause, credentials, business body or mutation callback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ViewPreparationError {
    pub code: ViewErrorCode,
    pub business_completion: BusinessCompletion,
}
impl fmt::Display for ViewPreparationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code.code())
    }
}
impl std::error::Error for ViewPreparationError {}

/// A fresh value denotes one presentation. It deliberately has no Clone/Deserialize.
pub struct ViewOptions {
    pub(crate) presentation: Arc<()>,
    pub(crate) mode: ViewMode,
    pub(crate) completion: BusinessCompletion,
    pub(crate) cache_lifetime: Duration,
    pub(crate) read_lifetime: Option<Duration>,
    pub(crate) context_checked_read: bool,
}
impl ViewOptions {
    pub fn read_only() -> Self {
        Self::new(ViewMode::ReadOnly, BusinessCompletion::Query)
    }
    pub fn governed() -> Self {
        Self::new(ViewMode::Governed, BusinessCompletion::Query)
    }
    fn new(mode: ViewMode, completion: BusinessCompletion) -> Self {
        Self {
            presentation: Arc::new(()),
            mode,
            completion,
            cache_lifetime: Duration::ZERO,
            read_lifetime: None,
            context_checked_read: false,
        }
    }
    pub fn after_commit(mut self) -> Self {
        self.completion = BusinessCompletion::Committed;
        self
    }
    pub fn with_cache_lifetime(mut self, value: Duration) -> Self {
        self.cache_lifetime = value;
        self
    }
    pub fn with_read_lifetime(mut self, value: Duration) -> Result<Self, ViewPreparationError> {
        if value.is_zero() || value.subsec_nanos() != 0 {
            return Err(self.error(ViewErrorCode::PreparationFailed));
        }
        self.read_lifetime = Some(value);
        Ok(self)
    }
    pub fn with_context_checked_read(mut self) -> Self {
        self.context_checked_read = true;
        self
    }
    pub fn mode(&self) -> ViewMode {
        self.mode
    }
    pub fn business_completion(&self) -> BusinessCompletion {
        self.completion
    }
    pub fn cache_lifetime(&self) -> Duration {
        self.cache_lifetime
    }
    pub fn read_lifetime(&self) -> Option<Duration> {
        self.read_lifetime
    }
    pub fn context_checked_read(&self) -> bool {
        self.context_checked_read
    }
    pub(crate) fn error(&self, code: ViewErrorCode) -> ViewPreparationError {
        ViewPreparationError {
            code,
            business_completion: self.completion,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewIdentity {
    pub realm: String,
    pub subject: String,
    pub boundary: String,
}
impl ViewIdentity {
    pub fn new(
        realm: impl Into<String>,
        subject: impl Into<String>,
        boundary: impl Into<String>,
    ) -> Result<Self, ViewErrorCode> {
        let value = Self {
            realm: realm.into(),
            subject: subject.into(),
            boundary: boundary.into(),
        };
        value.validate()?;
        Ok(value)
    }
    pub(crate) fn validate(&self) -> Result<(), ViewErrorCode> {
        for value in [&self.realm, &self.subject, &self.boundary] {
            require_text(value)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViewFieldDecision {
    Hidden,
    Mask(Option<String>),
    Value {
        writable: bool,
        complete_relation: bool,
    },
}
impl ViewFieldDecision {
    pub fn value() -> Self {
        Self::Value {
            writable: false,
            complete_relation: false,
        }
    }
}

/// Required host policy, invoked before reading a loaded business field.
pub trait ViewAuthorization: Send + Sync {
    fn identity(&self, context: &UserContext) -> Result<ViewIdentity, ViewErrorCode>;
    fn can_read_entity(
        &self,
        context: &UserContext,
        target: &ViewEntityTarget,
        path: &ViewPath,
    ) -> Result<bool, ViewErrorCode>;
    fn field(
        &self,
        context: &UserContext,
        target: &ViewEntityTarget,
        canonical: &str,
        path: &ViewPath,
    ) -> Result<ViewFieldDecision, ViewErrorCode>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewCachePolicy {
    NoStore,
    PrivateMaxAge(Duration),
}

/// Read-only output with private, process-local issuer and presentation provenance.
/// It cannot be constructed/deserialized from client JSON.
pub struct PreparedView {
    pub(crate) issuer: Arc<()>,
    pub(crate) presentation: Arc<()>,
    pub(crate) identity: ViewIdentity,
    pub(crate) graph: AuthorizedResponseGraph,
    pub(crate) value: serde_json::Value,
    pub(crate) document: Option<DocumentViewProjection>,
    pub(crate) blobs: Option<BlobViewProjection>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) cache_policy: ViewCachePolicy,
    pub(crate) options: (
        ViewMode,
        BusinessCompletion,
        Duration,
        Option<Duration>,
        bool,
    ),
}
impl PreparedView {
    pub fn value(&self) -> &serde_json::Value {
        &self.value
    }
    pub fn document_context(&self) -> Option<&DocumentContext> {
        self.document.as_ref().map(|d| &d.context)
    }
    pub fn document_credentials(&self) -> Option<&serde_json::Value> {
        self.document.as_ref().map(|d| &d.credentials)
    }
    pub fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expires_at
    }
    pub fn business_completion(&self) -> BusinessCompletion {
        self.options.1
    }
    pub fn cache_policy_at(&self, observed_at: DateTime<Utc>) -> ViewCachePolicy {
        match (self.cache_policy, self.expires_at) {
            (ViewCachePolicy::PrivateMaxAge(max), Some(expiry)) => {
                let remaining = (expiry - observed_at).to_std().unwrap_or(Duration::ZERO);
                if remaining.as_secs() == 0 {
                    ViewCachePolicy::NoStore
                } else {
                    ViewCachePolicy::PrivateMaxAge(Duration::from_secs(
                        max.as_secs().min(remaining.as_secs()),
                    ))
                }
            }
            (value, _) => value,
        }
    }
}
impl fmt::Debug for PreparedView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PreparedView([redacted])")
    }
}

pub(crate) fn require_text(value: &str) -> Result<(), ViewErrorCode> {
    if value.trim().is_empty()
        || value.len() > 4096
        || value
            .chars()
            .any(|c| c <= '\u{1f}' || ('\u{7f}'..='\u{9f}').contains(&c))
    {
        Err(ViewErrorCode::ResultUnsupported)
    } else {
        Ok(())
    }
}

pub(crate) struct ViewServiceResource(pub Arc<ViewPreparationService>);
