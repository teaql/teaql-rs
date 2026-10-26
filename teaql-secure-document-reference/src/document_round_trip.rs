use crate::round_trip_reference::{ContextBoundReferenceRuntime, derive_key};
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, TimeDelta};
use std::collections::BTreeSet;
#[cfg(test)]
use std::sync::Arc;
use std::time::Duration;
use subtle::ConstantTimeEq;
#[cfg(test)]
use teaql_runtime::RoundTripReferenceError;
use teaql_runtime::{
    DocumentEntitySnapshot, DocumentMutationKind, DocumentRelationCompleteness,
    DocumentRelationSnapshot, DocumentRoundTripError, DocumentSnapshot, ReferenceDocumentScope,
    ReferenceIdentity, TrustedReferencePrincipal, VerifiedDocumentSnapshot,
};
mod document_blob_v2;
const TOKEN_PREFIX: &str = "tqd1";
const TOKEN_VERSION: u8 = 1;
const MAX_TOKEN_LENGTH: usize = 256_000;
const MAX_TEXT_LENGTH: usize = 16_384;
const MAX_ENTITIES: usize = 10_000;
const MAX_FIELDS: usize = 1_024;
const MAX_RELATIONS: usize = 1_024;
const MAX_RELATION_ROWS: usize = 10_000;

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
    encode_document_aad_with_prefix(TOKEN_PREFIX, key_id, service, environment, purpose)
}

fn encode_document_aad_with_prefix(
    prefix: &str,
    key_id: &str,
    service: &str,
    environment: &str,
    purpose: &str,
) -> Result<Vec<u8>, DocumentRoundTripError> {
    let mut output = Vec::new();
    write_text(&mut output, prefix)?;
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
    let (decoded, offset) = decode_document_payload_prefix(data, TOKEN_VERSION)?;
    if offset != data.len() {
        return Err(DocumentRoundTripError::invalid());
    }
    Ok(decoded)
}

fn decode_document_payload_prefix(
    data: &[u8],
    expected_version: u8,
) -> Result<(DecodedDocumentPayload, usize), DocumentRoundTripError> {
    let mut offset = 0;
    if read_u8(data, &mut offset)? != expected_version {
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
    Ok((
        DecodedDocumentPayload {
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
        },
        offset,
    ))
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
    parse_token_with_prefix(token, TOKEN_PREFIX)
}

fn parse_token_with_prefix<'a>(
    token: &'a str,
    prefix: &str,
) -> Result<(&'a str, &'a str), DocumentRoundTripError> {
    if token.len() > MAX_TOKEN_LENGTH {
        return Err(DocumentRoundTripError::invalid());
    }
    let mut parts = token.split('.');
    if parts.next() != Some(prefix) {
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
    use sha2::Digest;

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
        assert_eq!(
            format!("{:x}", sha2::Sha256::digest(token.as_bytes())),
            "69fee1ec01cd4a5b69dd3f795aeaffb3a3546e50d48e25cfc9b8f169a527b267"
        );
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
