use crate::{DocumentRoundTripError, DocumentSnapshot, VerifiedDocumentSnapshot};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value, json};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

pub const UNSAFE_EXPOSE_RAW_ENTITY_IDS_ENVIRONMENT: &str = "TEAQL_UNSAFE_EXPOSE_RAW_ENTITY_IDS";
pub const UNSAFE_EXPOSE_RAW_ENTITY_IDS_ACKNOWLEDGEMENT: &str =
    "I_UNDERSTAND_THIS_EXPOSES_INTERNAL_ENTITY_IDS_FOR_LOCAL_DEBUGGING_ONLY";
const TOKEN_PREFIX: &str = "tqr1";
const MAX_BOUND_TEXT_LENGTH: usize = 4_096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReferenceMode {
    Governed,
    Raw,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustedReferencePrincipal {
    pub authentication_realm: String,
    pub subject: String,
    pub domain_root_type: String,
    pub domain_root_id: u64,
}

impl TrustedReferencePrincipal {
    pub fn new(
        authentication_realm: impl Into<String>,
        subject: impl Into<String>,
        domain_root_type: impl Into<String>,
        domain_root_id: u64,
    ) -> Result<Self, RoundTripReferenceError> {
        let value = Self {
            authentication_realm: authentication_realm.into(),
            subject: subject.into(),
            domain_root_type: domain_root_type.into(),
            domain_root_id,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), RoundTripReferenceError> {
        require_text(&self.authentication_realm)?;
        require_text(&self.subject)?;
        require_text(&self.domain_root_type)?;
        if self.domain_root_id == 0 {
            return Err(RoundTripReferenceError::invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceDocumentScope {
    pub document_id: String,
    pub purpose: String,
    pub aggregate_type: String,
    pub aggregate_id: u64,
    pub aggregate_revision: i64,
}

impl ReferenceDocumentScope {
    pub fn new(
        document_id: impl Into<String>,
        purpose: impl Into<String>,
        aggregate_type: impl Into<String>,
        aggregate_id: u64,
        aggregate_revision: i64,
    ) -> Result<Self, RoundTripReferenceError> {
        let value = Self {
            document_id: document_id.into(),
            purpose: purpose.into(),
            aggregate_type: aggregate_type.into(),
            aggregate_id,
            aggregate_revision,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), RoundTripReferenceError> {
        require_text(&self.document_id)?;
        require_text(&self.purpose)?;
        require_text(&self.aggregate_type)?;
        if self.aggregate_id == 0 || self.aggregate_revision < 0 {
            return Err(RoundTripReferenceError::invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceIdentity {
    pub entity_type: String,
    pub id: u64,
    pub version: i64,
}

impl ReferenceIdentity {
    pub fn new(
        entity_type: impl Into<String>,
        id: u64,
        version: i64,
    ) -> Result<Self, RoundTripReferenceError> {
        let value = Self {
            entity_type: entity_type.into(),
            id,
            version,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), RoundTripReferenceError> {
        require_text(&self.entity_type)?;
        if self.id == 0 || self.version < 0 {
            return Err(RoundTripReferenceError::invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalEntityReference {
    Governed(String),
    Raw { id: u64, version: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedEntityReference {
    pub identity: ReferenceIdentity,
    pub mode: ReferenceMode,
    pub key_id: Option<String>,
    pub issued_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceStartupNotice {
    pub level: &'static str,
    pub message: &'static str,
    pub deployment_profile: &'static str,
    pub telemetry_key: &'static str,
    pub telemetry_value: &'static str,
    pub response_headers: [(&'static str, &'static str); 2],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoundTripReferenceError {
    code: &'static str,
}

impl RoundTripReferenceError {
    pub const fn invalid() -> Self {
        Self {
            code: "ROUND_TRIP_REFERENCE_INVALID",
        }
    }

    pub const fn expired() -> Self {
        Self {
            code: "ROUND_TRIP_REFERENCE_EXPIRED",
        }
    }

    pub const fn scope_mismatch() -> Self {
        Self {
            code: "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH",
        }
    }

    pub const fn authorization_required() -> Self {
        Self {
            code: "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED",
        }
    }

    pub const fn configuration() -> Self {
        Self {
            code: "ROUND_TRIP_REFERENCE_CONFIGURATION_INVALID",
        }
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl Display for RoundTripReferenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code)
    }
}

impl std::error::Error for RoundTripReferenceError {}

#[derive(Clone, Copy, Debug, Default)]
pub struct ReferenceWireCodec;

impl ReferenceWireCodec {
    pub fn serialize(reference: &ExternalEntityReference) -> Value {
        match reference {
            ExternalEntityReference::Governed(token) => Value::String(token.clone()),
            ExternalEntityReference::Raw { id, version } => {
                json!({"id": id, "version": version})
            }
        }
    }

    pub fn deserialize(
        value: &Value,
        mode: ReferenceMode,
    ) -> Result<ExternalEntityReference, RoundTripReferenceError> {
        match (mode, value) {
            (ReferenceMode::Governed, Value::String(token))
                if token.starts_with(&format!("{TOKEN_PREFIX}.")) =>
            {
                Ok(ExternalEntityReference::Governed(token.clone()))
            }
            (ReferenceMode::Raw, Value::Object(object)) => decode_raw_wire(object),
            _ => Err(RoundTripReferenceError::invalid()),
        }
    }
}

/// Lightweight context-bound reference capability; implementations are independently installed.
pub trait RoundTripReferenceProvider: Send + Sync {
    fn mode(&self) -> ReferenceMode;
    fn startup_notice(&self) -> Option<ReferenceStartupNotice>;
    fn issue(
        &self,
        principal: &TrustedReferencePrincipal,
        scope: &ReferenceDocumentScope,
        identity: ReferenceIdentity,
        lifetime: Duration,
    ) -> Result<ExternalEntityReference, RoundTripReferenceError>;
    fn consume(
        &self,
        principal: &TrustedReferencePrincipal,
        scope: &ReferenceDocumentScope,
        reference: &ExternalEntityReference,
        expected_entity_type: &str,
    ) -> Result<ResolvedEntityReference, RoundTripReferenceError>;
    fn issue_document_snapshot(
        &self,
        principal: &TrustedReferencePrincipal,
        snapshot: DocumentSnapshot,
        lifetime: Duration,
    ) -> Result<String, DocumentRoundTripError>;
    fn consume_document_snapshot(
        &self,
        principal: &TrustedReferencePrincipal,
        token: &str,
        expected_aggregate_type: &str,
        expected_purpose: &str,
    ) -> Result<VerifiedDocumentSnapshot, DocumentRoundTripError>;
}

#[derive(Clone)]
pub(crate) struct ContextBoundReferenceRuntimeResource(pub Arc<dyn RoundTripReferenceProvider>);
#[derive(Clone)]
pub(crate) struct TrustedReferencePrincipalResource(pub TrustedReferencePrincipal);

fn decode_raw_wire(
    object: &Map<String, Value>,
) -> Result<ExternalEntityReference, RoundTripReferenceError> {
    if object.len() != 2 {
        return Err(RoundTripReferenceError::invalid());
    }
    let id = object
        .get("id")
        .and_then(Value::as_u64)
        .filter(|id| *id > 0)
        .ok_or_else(RoundTripReferenceError::invalid)?;
    let version = object
        .get("version")
        .and_then(Value::as_i64)
        .filter(|version| *version >= 0)
        .ok_or_else(RoundTripReferenceError::invalid)?;
    Ok(ExternalEntityReference::Raw { id, version })
}

fn require_text(value: &str) -> Result<(), RoundTripReferenceError> {
    if value.is_empty() || value.len() > MAX_BOUND_TEXT_LENGTH || value.trim() != value {
        return Err(RoundTripReferenceError::invalid());
    }
    Ok(())
}
