use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::Value;
use subtle::ConstantTimeEq;

use crate::UserContext;
use crate::round_trip_reference::{
    ContextBoundReferenceRuntime, ReferenceDocumentScope, ReferenceIdentity,
    RoundTripReferenceError, TrustedReferencePrincipal, derive_key,
};

const TOKEN_PREFIX: &str = "tqd1";
const TOKEN_VERSION: u8 = 1;
const MAX_TOKEN_LENGTH: usize = 256_000;
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
    fn wire_value(self) -> u8 {
        match self {
            Self::Update => 1,
            Self::CreateChild => 2,
            Self::RemoveChild => 3,
        }
    }

    fn from_wire(value: u8) -> Result<Self, DocumentRoundTripError> {
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
    fn wire_value(self) -> u8 {
        match self {
            Self::Complete => 1,
            Self::Partial => 2,
        }
    }

    fn from_wire(value: u8) -> Result<Self, DocumentRoundTripError> {
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

    fn validate(&self) -> Result<(), DocumentRoundTripError> {
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

    fn validate(&self) -> Result<(), DocumentRoundTripError> {
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

    fn validate(&self) -> Result<(), DocumentRoundTripError> {
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

impl ContextBoundReferenceRuntime {
    pub fn issue_document_snapshot(
        &self,
        principal: &TrustedReferencePrincipal,
        snapshot: DocumentSnapshot,
        lifetime: Duration,
    ) -> Result<String, DocumentRoundTripError> {
        principal.validate()?;
        snapshot.validate()?;
        if lifetime.is_zero() {
            return Err(DocumentRoundTripError::invalid());
        }
        self.authorize_identity(principal, &snapshot.scope, &snapshot.aggregate)?;
        for entity in &snapshot.entities {
            self.authorize_identity(principal, &snapshot.scope, &entity.identity)?;
        }

        let key = self.current_reference_key()?;
        let now = self.now();
        let expires_at = now
            .checked_add_signed(
                TimeDelta::from_std(lifetime).map_err(|_| DocumentRoundTripError::invalid())?,
            )
            .ok_or_else(DocumentRoundTripError::invalid)?;
        let plaintext = encode_document_payload(
            &snapshot,
            self.actor_fingerprint_for(&key, principal)?,
            now.timestamp(),
            expires_at.timestamp(),
        )?;
        let encryption_key = derive_key(
            key.secret(),
            b"document-snapshot-encryption",
            self.service_name(),
            self.environment_name(),
            &key.key_id,
        )?;
        let aad = encode_document_aad(
            &key.key_id,
            self.service_name(),
            self.environment_name(),
            &snapshot.scope.purpose,
        )?;
        let nonce = self.next_nonce();
        let cipher = Aes256Gcm::new_from_slice(&encryption_key)
            .map_err(|_| DocumentRoundTripError::configuration())?;
        let encrypted = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| DocumentRoundTripError::invalid())?;
        let mut envelope = Vec::with_capacity(nonce.len() + encrypted.len());
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&encrypted);
        Ok(format!(
            "{TOKEN_PREFIX}.{}.{}",
            key.key_id,
            URL_SAFE_NO_PAD.encode(envelope)
        ))
    }

    pub fn consume_document_snapshot(
        &self,
        principal: &TrustedReferencePrincipal,
        token: &str,
        expected_aggregate_type: &str,
        expected_purpose: &str,
    ) -> Result<VerifiedDocumentSnapshot, DocumentRoundTripError> {
        principal.validate()?;
        require_text(expected_aggregate_type)?;
        require_text(expected_purpose)?;
        let (key_id, encoded) = parse_document_token(token)?;
        let key = self
            .reference_key_by_id(key_id)?
            .ok_or_else(DocumentRoundTripError::invalid)?;
        let envelope = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| DocumentRoundTripError::invalid())?;
        if envelope.len() < 28 {
            return Err(DocumentRoundTripError::invalid());
        }
        let encryption_key = derive_key(
            key.secret(),
            b"document-snapshot-encryption",
            self.service_name(),
            self.environment_name(),
            &key.key_id,
        )?;
        let aad = encode_document_aad(
            &key.key_id,
            self.service_name(),
            self.environment_name(),
            expected_purpose,
        )?;
        let cipher = Aes256Gcm::new_from_slice(&encryption_key)
            .map_err(|_| DocumentRoundTripError::configuration())?;
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(&envelope[..12]),
                Payload {
                    msg: &envelope[12..],
                    aad: &aad,
                },
            )
            .map_err(|_| DocumentRoundTripError::invalid())?;
        let decoded = decode_document_payload(&plaintext)?;
        let now = self.now();
        if decoded.expires_at <= now.timestamp() {
            return Err(DocumentRoundTripError::expired());
        }
        if decoded.issued_at > now.timestamp() + 60 {
            return Err(DocumentRoundTripError::invalid());
        }
        let expected_actor = self.actor_fingerprint_for(&key, principal)?;
        if !bool::from(decoded.actor_fingerprint.ct_eq(&expected_actor))
            || decoded.snapshot.scope.purpose != expected_purpose
            || decoded.snapshot.scope.aggregate_type != expected_aggregate_type
        {
            return Err(DocumentRoundTripError::scope_mismatch());
        }
        decoded.snapshot.validate()?;
        self.authorize_identity(
            principal,
            &decoded.snapshot.scope,
            &decoded.snapshot.aggregate,
        )?;
        for entity in &decoded.snapshot.entities {
            self.authorize_identity(principal, &decoded.snapshot.scope, &entity.identity)?;
        }
        Ok(VerifiedDocumentSnapshot {
            snapshot: decoded.snapshot,
            key_id: key.key_id,
            issued_at: DateTime::from_timestamp(decoded.issued_at, 0)
                .ok_or_else(DocumentRoundTripError::invalid)?,
            expires_at: DateTime::from_timestamp(decoded.expires_at, 0)
                .ok_or_else(DocumentRoundTripError::invalid)?,
        })
    }
}

struct DecodedDocumentPayload {
    actor_fingerprint: [u8; 32],
    snapshot: DocumentSnapshot,
    issued_at: i64,
    expires_at: i64,
}

fn encode_document_aad(
    key_id: &str,
    service: &str,
    environment: &str,
    purpose: &str,
) -> Result<Vec<u8>, DocumentRoundTripError> {
    let mut output = Vec::new();
    write_text(&mut output, TOKEN_PREFIX)?;
    write_text(&mut output, key_id)?;
    write_text(&mut output, service)?;
    write_text(&mut output, environment)?;
    write_text(&mut output, purpose)?;
    Ok(output)
}

fn encode_document_payload(
    snapshot: &DocumentSnapshot,
    actor_fingerprint: [u8; 32],
    issued_at: i64,
    expires_at: i64,
) -> Result<Vec<u8>, DocumentRoundTripError> {
    let mut output = Vec::new();
    output.push(TOKEN_VERSION);
    output.extend_from_slice(&actor_fingerprint);
    output.extend_from_slice(&issued_at.to_be_bytes());
    output.extend_from_slice(&expires_at.to_be_bytes());
    write_text(&mut output, &snapshot.model_fingerprint)?;
    write_text(&mut output, &snapshot.business_id)?;
    encode_scope(&mut output, &snapshot.scope)?;
    encode_identity(&mut output, &snapshot.aggregate)?;
    write_count(&mut output, snapshot.entities.len())?;
    for entity in &snapshot.entities {
        write_text(&mut output, &entity.row_key)?;
        encode_identity(&mut output, &entity.identity)?;
        encode_text_set(&mut output, &entity.loaded_fields)?;
        encode_text_set(&mut output, &entity.writable_fields)?;
        write_count(&mut output, entity.allowed_mutations.len())?;
        for mutation in &entity.allowed_mutations {
            output.push(mutation.wire_value());
        }
    }
    write_count(&mut output, snapshot.relations.len())?;
    for relation in &snapshot.relations {
        write_text(&mut output, &relation.relation_name)?;
        output.push(relation.completeness.wire_value());
        output.push(u8::from(relation.writable));
        write_count(&mut output, relation.allowed_mutations.len())?;
        for mutation in &relation.allowed_mutations {
            output.push(mutation.wire_value());
        }
        write_count(&mut output, relation.row_keys.len())?;
        for row_key in &relation.row_keys {
            write_text(&mut output, row_key)?;
        }
    }
    Ok(output)
}

fn decode_document_payload(data: &[u8]) -> Result<DecodedDocumentPayload, DocumentRoundTripError> {
    let mut offset = 0;
    if read_u8(data, &mut offset)? != TOKEN_VERSION {
        return Err(DocumentRoundTripError::invalid());
    }
    let actor_fingerprint: [u8; 32] = read_exact(data, &mut offset, 32)?
        .try_into()
        .map_err(|_| DocumentRoundTripError::invalid())?;
    let issued_at = read_i64(data, &mut offset)?;
    let expires_at = read_i64(data, &mut offset)?;
    let model_fingerprint = read_text(data, &mut offset)?;
    let business_id = read_text(data, &mut offset)?;
    let scope = decode_scope(data, &mut offset)?;
    let aggregate = decode_identity(data, &mut offset)?;
    let entity_count = read_count(data, &mut offset, MAX_ENTITIES)?;
    let mut entities = Vec::with_capacity(entity_count);
    for _ in 0..entity_count {
        let row_key = read_text(data, &mut offset)?;
        let identity = decode_identity(data, &mut offset)?;
        let loaded_fields = decode_text_set(data, &mut offset)?;
        let writable_fields = decode_text_set(data, &mut offset)?;
        let mutation_count = read_count(data, &mut offset, 3)?;
        let mut mutations = BTreeSet::new();
        for _ in 0..mutation_count {
            if !mutations.insert(DocumentMutationKind::from_wire(read_u8(
                data,
                &mut offset,
            )?)?) {
                return Err(DocumentRoundTripError::invalid());
            }
        }
        entities.push(DocumentEntitySnapshot::new(
            row_key,
            identity,
            loaded_fields,
            writable_fields,
            mutations,
        )?);
    }
    let relation_count = read_count(data, &mut offset, MAX_RELATIONS)?;
    let mut relations = Vec::with_capacity(relation_count);
    for _ in 0..relation_count {
        let relation_name = read_text(data, &mut offset)?;
        let completeness = DocumentRelationCompleteness::from_wire(read_u8(data, &mut offset)?)?;
        let writable = match read_u8(data, &mut offset)? {
            0 => false,
            1 => true,
            _ => return Err(DocumentRoundTripError::invalid()),
        };
        let mutation_count = read_count(data, &mut offset, 3)?;
        let mut allowed_mutations = BTreeSet::new();
        for _ in 0..mutation_count {
            if !allowed_mutations.insert(DocumentMutationKind::from_wire(read_u8(
                data,
                &mut offset,
            )?)?) {
                return Err(DocumentRoundTripError::invalid());
            }
        }
        let row_count = read_count(data, &mut offset, MAX_RELATION_ROWS)?;
        let mut row_keys = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            row_keys.push(read_text(data, &mut offset)?);
        }
        relations.push(DocumentRelationSnapshot::new(
            relation_name,
            row_keys,
            completeness,
            writable,
            allowed_mutations,
        )?);
    }
    if offset != data.len() {
        return Err(DocumentRoundTripError::invalid());
    }
    Ok(DecodedDocumentPayload {
        actor_fingerprint,
        snapshot: DocumentSnapshot::new(
            model_fingerprint,
            business_id,
            scope,
            aggregate,
            entities,
            relations,
        )?,
        issued_at,
        expires_at,
    })
}

fn encode_scope(
    output: &mut Vec<u8>,
    scope: &ReferenceDocumentScope,
) -> Result<(), DocumentRoundTripError> {
    write_text(output, &scope.document_id)?;
    write_text(output, &scope.purpose)?;
    write_text(output, &scope.aggregate_type)?;
    output.extend_from_slice(&scope.aggregate_id.to_be_bytes());
    output.extend_from_slice(&scope.aggregate_revision.to_be_bytes());
    Ok(())
}

fn decode_scope(
    data: &[u8],
    offset: &mut usize,
) -> Result<ReferenceDocumentScope, DocumentRoundTripError> {
    Ok(ReferenceDocumentScope::new(
        read_text(data, offset)?,
        read_text(data, offset)?,
        read_text(data, offset)?,
        read_u64(data, offset)?,
        read_i64(data, offset)?,
    )?)
}

fn encode_identity(
    output: &mut Vec<u8>,
    identity: &ReferenceIdentity,
) -> Result<(), DocumentRoundTripError> {
    write_text(output, &identity.entity_type)?;
    output.extend_from_slice(&identity.id.to_be_bytes());
    output.extend_from_slice(&identity.version.to_be_bytes());
    Ok(())
}

fn decode_identity(
    data: &[u8],
    offset: &mut usize,
) -> Result<ReferenceIdentity, DocumentRoundTripError> {
    Ok(ReferenceIdentity::new(
        read_text(data, offset)?,
        read_u64(data, offset)?,
        read_i64(data, offset)?,
    )?)
}

fn encode_text_set(
    output: &mut Vec<u8>,
    values: &BTreeSet<String>,
) -> Result<(), DocumentRoundTripError> {
    write_count(output, values.len())?;
    for value in values {
        write_text(output, value)?;
    }
    Ok(())
}

fn decode_text_set(
    data: &[u8],
    offset: &mut usize,
) -> Result<BTreeSet<String>, DocumentRoundTripError> {
    let count = read_count(data, offset, MAX_FIELDS)?;
    let mut values = BTreeSet::new();
    for _ in 0..count {
        if !values.insert(read_text(data, offset)?) {
            return Err(DocumentRoundTripError::invalid());
        }
    }
    Ok(values)
}

fn parse_document_token(token: &str) -> Result<(&str, &str), DocumentRoundTripError> {
    if token.len() > MAX_TOKEN_LENGTH {
        return Err(DocumentRoundTripError::invalid());
    }
    let mut parts = token.split('.');
    if parts.next() != Some(TOKEN_PREFIX) {
        return Err(DocumentRoundTripError::invalid());
    }
    let key_id = parts.next().ok_or_else(DocumentRoundTripError::invalid)?;
    let encoded = parts.next().ok_or_else(DocumentRoundTripError::invalid)?;
    if parts.next().is_some() || key_id.is_empty() || encoded.is_empty() {
        return Err(DocumentRoundTripError::invalid());
    }
    Ok((key_id, encoded))
}

fn require_text(value: &str) -> Result<(), DocumentRoundTripError> {
    if value.trim().is_empty() || value.len() > MAX_TEXT_LENGTH {
        return Err(DocumentRoundTripError::invalid());
    }
    Ok(())
}

fn write_count(output: &mut Vec<u8>, value: usize) -> Result<(), DocumentRoundTripError> {
    output.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| DocumentRoundTripError::invalid())?
            .to_be_bytes(),
    );
    Ok(())
}

fn write_text(output: &mut Vec<u8>, value: &str) -> Result<(), DocumentRoundTripError> {
    require_text(value)?;
    write_count(output, value.len())?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_exact<'a>(
    data: &'a [u8],
    offset: &mut usize,
    length: usize,
) -> Result<&'a [u8], DocumentRoundTripError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(DocumentRoundTripError::invalid)?;
    let value = data
        .get(*offset..end)
        .ok_or_else(DocumentRoundTripError::invalid)?;
    *offset = end;
    Ok(value)
}

fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8, DocumentRoundTripError> {
    Ok(read_exact(data, offset, 1)?[0])
}

fn read_u32(data: &[u8], offset: &mut usize) -> Result<u32, DocumentRoundTripError> {
    Ok(u32::from_be_bytes(
        read_exact(data, offset, 4)?
            .try_into()
            .map_err(|_| DocumentRoundTripError::invalid())?,
    ))
}

fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64, DocumentRoundTripError> {
    Ok(u64::from_be_bytes(
        read_exact(data, offset, 8)?
            .try_into()
            .map_err(|_| DocumentRoundTripError::invalid())?,
    ))
}

fn read_i64(data: &[u8], offset: &mut usize) -> Result<i64, DocumentRoundTripError> {
    Ok(i64::from_be_bytes(
        read_exact(data, offset, 8)?
            .try_into()
            .map_err(|_| DocumentRoundTripError::invalid())?,
    ))
}

fn read_count(
    data: &[u8],
    offset: &mut usize,
    maximum: usize,
) -> Result<usize, DocumentRoundTripError> {
    let value =
        usize::try_from(read_u32(data, offset)?).map_err(|_| DocumentRoundTripError::invalid())?;
    if value > maximum {
        return Err(DocumentRoundTripError::invalid());
    }
    Ok(value)
}

fn read_text(data: &[u8], offset: &mut usize) -> Result<String, DocumentRoundTripError> {
    let length = read_count(data, offset, MAX_TEXT_LENGTH)?;
    let value = String::from_utf8(read_exact(data, offset, length)?.to_vec())
        .map_err(|_| DocumentRoundTripError::invalid())?;
    require_text(&value)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeploymentProfile, ReferenceKey, StaticReferenceKeyProvider};

    fn principal(subject: &str) -> TrustedReferencePrincipal {
        TrustedReferencePrincipal::new("oidc", subject, "Merchant", 7).unwrap()
    }

    fn allow_all(
        _: &TrustedReferencePrincipal,
        _: &ReferenceDocumentScope,
        _: &ReferenceIdentity,
    ) -> Result<(), RoundTripReferenceError> {
        Ok(())
    }

    fn test_runtime(environment: &str) -> ContextBoundReferenceRuntime {
        ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "order-service",
            environment,
            Arc::new(
                StaticReferenceKeyProvider::new(
                    "k1",
                    [ReferenceKey::new("k1", [0x42; 32]).unwrap()],
                )
                .unwrap(),
            ),
            Arc::new(allow_all),
        )
        .unwrap()
        .with_clock(|| DateTime::from_timestamp(1_800_000_000, 0).unwrap())
        .with_nonce_source(|| [0x33; 12])
    }

    fn rotating_runtime(
        current_key_id: &str,
        keys: impl IntoIterator<Item = ReferenceKey>,
        now: i64,
    ) -> ContextBoundReferenceRuntime {
        ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "order-service",
            "test",
            Arc::new(StaticReferenceKeyProvider::new(current_key_id, keys).unwrap()),
            Arc::new(allow_all),
        )
        .unwrap()
        .with_clock(move || DateTime::from_timestamp(now, 0).unwrap())
        .with_nonce_source(|| [0x44; 12])
    }

    fn snapshot() -> DocumentSnapshot {
        let scope = ReferenceDocumentScope::new("doc-1", "edit-order", "Order", 10, 4).unwrap();
        DocumentSnapshot::new(
            "model-v1",
            "ORD-X1",
            scope,
            ReferenceIdentity::new("Order", 10, 4).unwrap(),
            vec![
                DocumentEntitySnapshot::new(
                    "item:0",
                    ReferenceIdentity::new("OrderItem", 20, 2).unwrap(),
                    ["product_code", "quantity"],
                    ["quantity"],
                    [DocumentMutationKind::Update],
                )
                .unwrap(),
            ],
            vec![
                DocumentRelationSnapshot::new(
                    "items",
                    ["item:0"],
                    DocumentRelationCompleteness::Complete,
                    true,
                    [
                        DocumentMutationKind::CreateChild,
                        DocumentMutationKind::RemoveChild,
                    ],
                )
                .unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn snapshot_round_trips_and_is_bound_to_actor_and_environment() {
        let runtime = test_runtime("test");
        let token = runtime
            .issue_document_snapshot(&principal("alice"), snapshot(), Duration::from_secs(600))
            .unwrap();
        assert!(token.starts_with("tqd1.k1."));
        assert_eq!(
            runtime
                .consume_document_snapshot(&principal("alice"), &token, "Order", "edit-order")
                .unwrap()
                .snapshot,
            snapshot()
        );
        assert_eq!(
            runtime
                .consume_document_snapshot(&principal("bob"), &token, "Order", "edit-order")
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH"
        );
        assert_eq!(
            test_runtime("production")
                .consume_document_snapshot(&principal("alice"), &token, "Order", "edit-order")
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_INVALID"
        );
    }

    #[test]
    fn writable_fields_must_be_loaded() {
        let error = DocumentEntitySnapshot::new(
            "item:0",
            ReferenceIdentity::new("OrderItem", 20, 2).unwrap(),
            ["product_code"],
            ["quantity"],
            [DocumentMutationKind::Update],
        )
        .unwrap_err();
        assert_eq!(error.code(), "DOCUMENT_PROJECTION_VIOLATION");
    }

    #[test]
    fn document_tokens_support_decode_only_old_keys_and_expire() {
        let issued = rotating_runtime(
            "k1",
            [ReferenceKey::new("k1", [0x11; 32]).unwrap()],
            1_800_000_000,
        )
        .issue_document_snapshot(&principal("alice"), snapshot(), Duration::from_secs(60))
        .unwrap();
        let rotated = rotating_runtime(
            "k2",
            [
                ReferenceKey::new("k1", [0x11; 32]).unwrap(),
                ReferenceKey::new("k2", [0x22; 32]).unwrap(),
            ],
            1_800_000_030,
        );
        assert_eq!(
            rotated
                .consume_document_snapshot(&principal("alice"), &issued, "Order", "edit-order")
                .unwrap()
                .key_id,
            "k1"
        );
        assert_eq!(
            rotating_runtime(
                "k2",
                [
                    ReferenceKey::new("k1", [0x11; 32]).unwrap(),
                    ReferenceKey::new("k2", [0x22; 32]).unwrap(),
                ],
                1_800_000_061,
            )
            .consume_document_snapshot(&principal("alice"), &issued, "Order", "edit-order")
            .unwrap_err()
            .code(),
            "ROUND_TRIP_REFERENCE_EXPIRED"
        );
    }
}
