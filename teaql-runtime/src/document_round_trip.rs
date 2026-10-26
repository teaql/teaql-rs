use crate::UserContext;
use crate::round_trip_reference::{
    ReferenceDocumentScope, ReferenceIdentity, RoundTripReferenceError, TrustedReferencePrincipal,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use teaql_core::BlobRef;

const MAX_TEXT_LENGTH: usize = 16_384;
const MAX_ENTITIES: usize = 10_000;
const MAX_FIELDS: usize = 1_024;
const MAX_RELATIONS: usize = 1_024;
const MAX_RELATION_ROWS: usize = 10_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DocumentMutationKind {
    Update,
    CreateChild,
    RemoveChild,
}

impl DocumentMutationKind {
    pub fn wire_value(self) -> u8 {
        match self {
            Self::Update => 1,
            Self::CreateChild => 2,
            Self::RemoveChild => 3,
        }
    }

    pub fn from_wire(value: u8) -> Result<Self, DocumentRoundTripError> {
        match value {
            1 => Ok(Self::Update),
            2 => Ok(Self::CreateChild),
            3 => Ok(Self::RemoveChild),
            _ => Err(DocumentRoundTripError::invalid()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentRelationCompleteness {
    Complete,
    Partial,
}

impl DocumentRelationCompleteness {
    pub fn wire_value(self) -> u8 {
        match self {
            Self::Complete => 1,
            Self::Partial => 2,
        }
    }

    pub fn from_wire(value: u8) -> Result<Self, DocumentRoundTripError> {
        match value {
            1 => Ok(Self::Complete),
            2 => Ok(Self::Partial),
            _ => Err(DocumentRoundTripError::invalid()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentEntitySnapshot {
    pub row_key: String,
    pub identity: ReferenceIdentity,
    pub loaded_fields: BTreeSet<String>,
    pub writable_fields: BTreeSet<String>,
    pub allowed_mutations: BTreeSet<DocumentMutationKind>,
}

impl DocumentEntitySnapshot {
    pub fn new<L, W, S>(
        row_key: impl Into<String>,
        identity: ReferenceIdentity,
        loaded_fields: L,
        writable_fields: W,
        allowed_mutations: impl IntoIterator<Item = DocumentMutationKind>,
    ) -> Result<Self, DocumentRoundTripError>
    where
        L: IntoIterator<Item = S>,
        W: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let value = Self {
            row_key: row_key.into(),
            identity,
            loaded_fields: loaded_fields.into_iter().map(Into::into).collect(),
            writable_fields: writable_fields.into_iter().map(Into::into).collect(),
            allowed_mutations: allowed_mutations.into_iter().collect(),
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), DocumentRoundTripError> {
        require_text(&self.row_key)?;
        require_text(&self.identity.entity_type)?;
        if self.identity.id == 0
            || self.identity.version < 0
            || self.loaded_fields.len() > MAX_FIELDS
            || self.writable_fields.len() > MAX_FIELDS
            || !self.writable_fields.is_subset(&self.loaded_fields)
            || self.allowed_mutations.is_empty()
        {
            return Err(DocumentRoundTripError::projection_violation());
        }
        for field in self.loaded_fields.iter().chain(&self.writable_fields) {
            require_text(field)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentRelationSnapshot {
    pub relation_name: String,
    pub row_keys: Vec<String>,
    pub completeness: DocumentRelationCompleteness,
    pub writable: bool,
    pub allowed_mutations: BTreeSet<DocumentMutationKind>,
}

impl DocumentRelationSnapshot {
    pub fn new<S>(
        relation_name: impl Into<String>,
        row_keys: impl IntoIterator<Item = S>,
        completeness: DocumentRelationCompleteness,
        writable: bool,
        allowed_mutations: impl IntoIterator<Item = DocumentMutationKind>,
    ) -> Result<Self, DocumentRoundTripError>
    where
        S: Into<String>,
    {
        let value = Self {
            relation_name: relation_name.into(),
            row_keys: row_keys.into_iter().map(Into::into).collect(),
            completeness,
            writable,
            allowed_mutations: allowed_mutations.into_iter().collect(),
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), DocumentRoundTripError> {
        require_text(&self.relation_name)?;
        if self.row_keys.len() > MAX_RELATION_ROWS
            || (!self.writable && !self.allowed_mutations.is_empty())
        {
            return Err(DocumentRoundTripError::projection_violation());
        }
        let mut unique = BTreeSet::new();
        for row_key in &self.row_keys {
            require_text(row_key)?;
            if !unique.insert(row_key) {
                return Err(DocumentRoundTripError::projection_violation());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentSnapshot {
    pub model_fingerprint: String,
    pub business_id: String,
    pub scope: ReferenceDocumentScope,
    pub aggregate: ReferenceIdentity,
    pub entities: Vec<DocumentEntitySnapshot>,
    pub relations: Vec<DocumentRelationSnapshot>,
}

impl DocumentSnapshot {
    pub fn new(
        model_fingerprint: impl Into<String>,
        business_id: impl Into<String>,
        scope: ReferenceDocumentScope,
        aggregate: ReferenceIdentity,
        entities: Vec<DocumentEntitySnapshot>,
        relations: Vec<DocumentRelationSnapshot>,
    ) -> Result<Self, DocumentRoundTripError> {
        let value = Self {
            model_fingerprint: model_fingerprint.into(),
            business_id: business_id.into(),
            scope,
            aggregate,
            entities,
            relations,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), DocumentRoundTripError> {
        require_text(&self.model_fingerprint)?;
        require_text(&self.business_id)?;
        require_text(&self.scope.document_id)?;
        require_text(&self.scope.purpose)?;
        require_text(&self.scope.aggregate_type)?;
        if self.entities.len() > MAX_ENTITIES || self.relations.len() > MAX_RELATIONS {
            return Err(DocumentRoundTripError::projection_violation());
        }
        if self.aggregate.entity_type != self.scope.aggregate_type
            || self.aggregate.id != self.scope.aggregate_id
            || self.aggregate.version != self.scope.aggregate_revision
        {
            return Err(DocumentRoundTripError::projection_violation());
        }
        let mut row_keys = BTreeSet::new();
        for entity in &self.entities {
            entity.validate()?;
            if !row_keys.insert(entity.row_key.as_str()) {
                return Err(DocumentRoundTripError::projection_violation());
            }
        }
        for relation in &self.relations {
            relation.validate()?;
            if relation
                .row_keys
                .iter()
                .any(|row_key| !row_keys.contains(row_key.as_str()))
            {
                return Err(DocumentRoundTripError::projection_violation());
            }
        }
        Ok(())
    }

    pub fn entity(&self, row_key: &str) -> Option<&DocumentEntitySnapshot> {
        self.entities
            .iter()
            .find(|entity| entity.row_key == row_key)
    }

    pub fn relation(&self, relation_name: &str) -> Option<&DocumentRelationSnapshot> {
        self.relations
            .iter()
            .find(|relation| relation.relation_name == relation_name)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContextualDocument {
    pub business_id: String,
    pub document_token: String,
    pub body: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SubmittedContextualDocument {
    pub business_id: String,
    pub document_token: String,
    pub command_id: String,
    pub body: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentOpenRequest {
    pub aggregate_type: String,
    pub business_id: String,
    pub purpose: String,
    pub lifetime: Duration,
}

impl DocumentOpenRequest {
    pub fn new(
        aggregate_type: impl Into<String>,
        business_id: impl Into<String>,
        purpose: impl Into<String>,
        lifetime: Duration,
    ) -> Result<Self, DocumentRoundTripError> {
        let value = Self {
            aggregate_type: aggregate_type.into(),
            business_id: business_id.into(),
            purpose: purpose.into(),
            lifetime,
        };
        require_text(&value.aggregate_type)?;
        require_text(&value.business_id)?;
        require_text(&value.purpose)?;
        if value.lifetime.is_zero() {
            return Err(DocumentRoundTripError::invalid());
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DocumentAcceptRequest {
    pub expected_aggregate_type: String,
    pub purpose: String,
    pub submitted: SubmittedContextualDocument,
}

impl DocumentAcceptRequest {
    pub fn new(
        expected_aggregate_type: impl Into<String>,
        purpose: impl Into<String>,
        submitted: SubmittedContextualDocument,
    ) -> Result<Self, DocumentRoundTripError> {
        let value = Self {
            expected_aggregate_type: expected_aggregate_type.into(),
            purpose: purpose.into(),
            submitted,
        };
        require_text(&value.expected_aggregate_type)?;
        require_text(&value.purpose)?;
        require_text(&value.submitted.business_id)?;
        require_text(&value.submitted.document_token)?;
        require_text(&value.submitted.command_id)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AcceptedContextualDocument {
    pub document: ContextualDocument,
    pub previous_revision: i64,
    pub new_revision: i64,
    pub changed_entities: Vec<ReferenceIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedDocumentSnapshot {
    pub snapshot: DocumentSnapshot,
    pub key_id: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// One authenticated tqd2 file binding. The stable Key is never exposed in a view body.
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentBlobBindingSnapshot {
    pub row_key: String,
    pub canonical_field: String,
    pub entity_type: String,
    pub reference: Option<BlobRef>,
    pub binding_version: u64,
    pub nullable: bool,
    pub writable: bool,
}

impl DocumentBlobBindingSnapshot {
    pub fn validate(&self) -> Result<(), DocumentRoundTripError> {
        for value in [&self.row_key, &self.canonical_field, &self.entity_type] {
            require_text(value)?;
            if value.trim() != value {
                return Err(DocumentRoundTripError::projection_violation());
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for DocumentBlobBindingSnapshot {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DocumentBlobBindingSnapshot[redacted]")
    }
}

/// tqd2 authority: a complete tqd1 snapshot plus a canonical encrypted binding suffix.
#[derive(Clone, PartialEq, Eq)]
pub struct BoundDocumentSnapshot {
    pub snapshot: DocumentSnapshot,
    pub bindings: Vec<DocumentBlobBindingSnapshot>,
}

impl BoundDocumentSnapshot {
    pub fn new(
        snapshot: DocumentSnapshot,
        mut bindings: Vec<DocumentBlobBindingSnapshot>,
    ) -> Result<Self, DocumentRoundTripError> {
        bindings.sort_by(|left, right| {
            (&left.row_key, &left.canonical_field).cmp(&(&right.row_key, &right.canonical_field))
        });
        let result = Self { snapshot, bindings };
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<(), DocumentRoundTripError> {
        self.snapshot.validate()?;
        if self.bindings.len() > 10_000 {
            return Err(DocumentRoundTripError::projection_violation());
        }
        let mut previous: Option<(&str, &str)> = None;
        for binding in &self.bindings {
            binding.validate()?;
            let key = (binding.row_key.as_str(), binding.canonical_field.as_str());
            if previous.is_some_and(|last| last >= key) {
                return Err(DocumentRoundTripError::projection_violation());
            }
            let entity = self
                .snapshot
                .entity(&binding.row_key)
                .ok_or_else(DocumentRoundTripError::projection_violation)?;
            if entity.identity.entity_type != binding.entity_type
                || !entity.loaded_fields.contains(&binding.canonical_field)
                || binding.writable && !entity.writable_fields.contains(&binding.canonical_field)
            {
                return Err(DocumentRoundTripError::projection_violation());
            }
            previous = Some(key);
        }
        Ok(())
    }
}

impl std::fmt::Debug for BoundDocumentSnapshot {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BoundDocumentSnapshot[redacted]")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedBoundDocumentSnapshot {
    pub document: BoundDocumentSnapshot,
    pub key_id: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobSelectionTargetKind {
    ExistingRow,
    NewClientKey,
}

impl BlobSelectionTargetKind {
    pub fn wire_value(self) -> u8 {
        match self {
            Self::ExistingRow => 1,
            Self::NewClientKey => 2,
        }
    }
    pub fn from_wire(value: u8) -> Result<Self, DocumentRoundTripError> {
        match value {
            1 => Ok(Self::ExistingRow),
            2 => Ok(Self::NewClientKey),
            _ => Err(DocumentRoundTripError::invalid()),
        }
    }
}

/// The exact scope, field, row/client key and command authorized for one selection.
#[derive(Clone, PartialEq, Eq)]
pub struct BlobSelectionTarget {
    pub scope: ReferenceDocumentScope,
    pub target_entity: String,
    pub canonical_field: String,
    pub kind: BlobSelectionTargetKind,
    pub target_key: String,
    pub command_id: String,
}

impl BlobSelectionTarget {
    pub fn validate(&self) -> Result<(), DocumentRoundTripError> {
        for value in [
            &self.target_entity,
            &self.canonical_field,
            &self.target_key,
            &self.command_id,
        ] {
            require_text(value)?;
            if value.trim() != value {
                return Err(DocumentRoundTripError::validation_failed());
            }
        }
        require_text(&self.scope.document_id)?;
        require_text(&self.scope.purpose)?;
        require_text(&self.scope.aggregate_type)?;
        if self.scope.aggregate_id == 0 || self.scope.aggregate_revision < 0 {
            return Err(DocumentRoundTripError::validation_failed());
        }
        Ok(())
    }
}

impl std::fmt::Debug for BlobSelectionTarget {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BlobSelectionTarget[redacted]")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct BlobSelectionClaim {
    pub target: BlobSelectionTarget,
    pub reference: BlobRef,
}

impl BlobSelectionClaim {
    pub fn validate(&self) -> Result<(), DocumentRoundTripError> {
        self.target.validate()
    }
}

impl std::fmt::Debug for BlobSelectionClaim {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BlobSelectionClaim[redacted]")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedBlobSelection {
    pub claim: BlobSelectionClaim,
    pub key_id: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentRoundTripError {
    code: &'static str,
}

impl DocumentRoundTripError {
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
    pub const fn revision_conflict() -> Self {
        Self {
            code: "DOCUMENT_REVISION_CONFLICT",
        }
    }
    pub const fn projection_violation() -> Self {
        Self {
            code: "DOCUMENT_PROJECTION_VIOLATION",
        }
    }
    pub const fn validation_failed() -> Self {
        Self {
            code: "DOCUMENT_VALIDATION_FAILED",
        }
    }
    pub const fn not_found() -> Self {
        Self {
            code: "DOCUMENT_NOT_FOUND",
        }
    }
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl From<RoundTripReferenceError> for DocumentRoundTripError {
    fn from(value: RoundTripReferenceError) -> Self {
        Self { code: value.code() }
    }
}

impl Display for DocumentRoundTripError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code)
    }
}

impl std::error::Error for DocumentRoundTripError {}

#[async_trait]
pub trait ContextBoundDocumentService: Send + Sync {
    fn supports_view_preparation(&self) -> bool {
        false
    }

    /// Current application document/purpose authority for an already issued outbound view.
    /// Must not reload its business graph, save or issue another token. Existing services
    /// opt in explicitly; returning an ordinary ContextualDocument alone is not provenance.
    async fn validate_prepared_document(
        &self,
        _context: &UserContext,
        _principal: &TrustedReferencePrincipal,
        _document: &VerifiedDocumentSnapshot,
    ) -> Result<(), DocumentRoundTripError> {
        Err(DocumentRoundTripError::configuration())
    }

    async fn open_document(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        request: DocumentOpenRequest,
    ) -> Result<ContextualDocument, DocumentRoundTripError>;

    async fn accept_document(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        request: DocumentAcceptRequest,
    ) -> Result<AcceptedContextualDocument, DocumentRoundTripError>;
}

#[derive(Clone)]
pub(crate) struct ContextBoundDocumentServiceResource(pub Arc<dyn ContextBoundDocumentService>);

fn require_text(value: &str) -> Result<(), DocumentRoundTripError> {
    if value.trim().is_empty() || value.len() > MAX_TEXT_LENGTH {
        return Err(DocumentRoundTripError::invalid());
    }
    Ok(())
}
