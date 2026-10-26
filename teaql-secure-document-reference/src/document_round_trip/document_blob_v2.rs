//! Versioned encrypted Blob binding and document-bound selection credentials.
use super::*;
use crate::ReferenceKey;
use teaql_core::BlobRef;
use teaql_runtime::{
    BlobSelectionClaim, BlobSelectionTarget, BlobSelectionTargetKind, BoundDocumentSnapshot,
    DocumentBlobBindingSnapshot, VerifiedBlobSelection, VerifiedBoundDocumentSnapshot,
};

const BOUND_PREFIX: &str = "tqd2";
const SELECTION_PREFIX: &str = "tqs1";
const MAX_BINDINGS: usize = 10_000;

impl ContextBoundReferenceRuntime {
    pub fn issue_bound_document_snapshot(
        &self,
        principal: &TrustedReferencePrincipal,
        document: BoundDocumentSnapshot,
        lifetime: Duration,
    ) -> Result<String, DocumentRoundTripError> {
        principal.validate()?;
        document.validate()?;
        if self.mode() != teaql_runtime::ReferenceMode::Governed || lifetime.is_zero() {
            return Err(DocumentRoundTripError::invalid());
        }
        self.authorize_document(principal, &document.snapshot)?;
        let key = self.current_reference_key()?;
        let now = self.now();
        let expires = expiry(now, lifetime)?;
        let plaintext = encode_bound_payload(
            &document,
            self.actor_fingerprint_for(&key, principal)?,
            now.timestamp(),
            expires.timestamp(),
        )?;
        self.seal(
            &key,
            BOUND_PREFIX,
            b"document-snapshot-v2-encryption",
            &document.snapshot.scope.purpose,
            &plaintext,
        )
    }

    pub fn consume_bound_document_snapshot(
        &self,
        principal: &TrustedReferencePrincipal,
        token: &str,
        expected_aggregate_type: &str,
        expected_purpose: &str,
    ) -> Result<VerifiedBoundDocumentSnapshot, DocumentRoundTripError> {
        principal.validate()?;
        require_text(expected_aggregate_type)?;
        require_text(expected_purpose)?;
        let (key, plaintext) = self.open(
            token,
            BOUND_PREFIX,
            b"document-snapshot-v2-encryption",
            expected_purpose,
        )?;
        let decoded = decode_bound_payload(&plaintext)?;
        let now = self.now().timestamp();
        if decoded.expires_at <= now {
            return Err(DocumentRoundTripError::expired());
        }
        if decoded.issued_at > now.saturating_add(60) {
            return Err(DocumentRoundTripError::invalid());
        }
        if !bool::from(
            decoded
                .actor_fingerprint
                .ct_eq(&self.actor_fingerprint_for(&key, principal)?),
        ) || decoded.document.snapshot.scope.purpose != expected_purpose
            || decoded.document.snapshot.scope.aggregate_type != expected_aggregate_type
        {
            return Err(DocumentRoundTripError::scope_mismatch());
        }
        self.authorize_document(principal, &decoded.document.snapshot)?;
        Ok(VerifiedBoundDocumentSnapshot {
            document: decoded.document,
            key_id: key.key_id,
            issued_at: timestamp(decoded.issued_at)?,
            expires_at: timestamp(decoded.expires_at)?,
        })
    }

    pub fn issue_blob_selection(
        &self,
        principal: &TrustedReferencePrincipal,
        document_token: &str,
        claim: BlobSelectionClaim,
        lifetime: Duration,
    ) -> Result<String, DocumentRoundTripError> {
        claim.validate()?;
        if lifetime.is_zero() {
            return Err(DocumentRoundTripError::invalid());
        }
        let target = &claim.target;
        let document = self.consume_bound_document_snapshot(
            principal,
            document_token,
            &target.scope.aggregate_type,
            &target.scope.purpose,
        )?;
        require_selection_target(&document.document, target)?;
        let key = self.current_reference_key()?;
        let now = self.now();
        let expires = expiry(now, lifetime)?;
        if expires > document.expires_at {
            return Err(DocumentRoundTripError::invalid());
        }
        let plaintext = encode_selection_payload(
            &claim,
            self.actor_fingerprint_for(&key, principal)?,
            now.timestamp(),
            expires.timestamp(),
        )?;
        self.seal(
            &key,
            SELECTION_PREFIX,
            b"blob-selection-v1-encryption",
            &target.scope.purpose,
            &plaintext,
        )
    }

    pub fn consume_blob_selection(
        &self,
        principal: &TrustedReferencePrincipal,
        document_token: &str,
        token: &str,
        expected_target: &BlobSelectionTarget,
    ) -> Result<VerifiedBlobSelection, DocumentRoundTripError> {
        expected_target.validate()?;
        let document = self.consume_bound_document_snapshot(
            principal,
            document_token,
            &expected_target.scope.aggregate_type,
            &expected_target.scope.purpose,
        )?;
        require_selection_target(&document.document, expected_target)?;
        let (key, plaintext) = self.open(
            token,
            SELECTION_PREFIX,
            b"blob-selection-v1-encryption",
            &expected_target.scope.purpose,
        )?;
        let decoded = decode_selection_payload(&plaintext)?;
        let now = self.now().timestamp();
        if decoded.expires_at <= now {
            return Err(DocumentRoundTripError::expired());
        }
        if decoded.issued_at > now.saturating_add(60)
            || decoded.expires_at > document.expires_at.timestamp()
        {
            return Err(DocumentRoundTripError::invalid());
        }
        if !bool::from(
            decoded
                .actor_fingerprint
                .ct_eq(&self.actor_fingerprint_for(&key, principal)?),
        ) || decoded.claim.target != *expected_target
        {
            return Err(DocumentRoundTripError::scope_mismatch());
        }
        Ok(VerifiedBlobSelection {
            claim: decoded.claim,
            key_id: key.key_id,
            issued_at: timestamp(decoded.issued_at)?,
            expires_at: timestamp(decoded.expires_at)?,
        })
    }

    fn authorize_document(
        &self,
        principal: &TrustedReferencePrincipal,
        snapshot: &DocumentSnapshot,
    ) -> Result<(), DocumentRoundTripError> {
        self.authorize_identity(principal, &snapshot.scope, &snapshot.aggregate)?;
        for entity in &snapshot.entities {
            self.authorize_identity(principal, &snapshot.scope, &entity.identity)?;
        }
        Ok(())
    }

    fn seal(
        &self,
        key: &ReferenceKey,
        prefix: &str,
        label: &[u8],
        purpose: &str,
        plaintext: &[u8],
    ) -> Result<String, DocumentRoundTripError> {
        let encryption_key = derive_key(
            key.secret(),
            label,
            self.service_name(),
            self.environment_name(),
            &key.key_id,
        )?;
        let aad = encode_document_aad_with_prefix(
            prefix,
            &key.key_id,
            self.service_name(),
            self.environment_name(),
            purpose,
        )?;
        let nonce = self.next_nonce();
        let cipher = Aes256Gcm::new_from_slice(&encryption_key)
            .map_err(|_| DocumentRoundTripError::configuration())?;
        let encrypted = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| DocumentRoundTripError::invalid())?;
        let mut envelope = Vec::with_capacity(nonce.len() + encrypted.len());
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&encrypted);
        let token = format!(
            "{prefix}.{}.{}",
            key.key_id,
            URL_SAFE_NO_PAD.encode(envelope)
        );
        if token.len() > MAX_TOKEN_LENGTH {
            return Err(DocumentRoundTripError::invalid());
        }
        Ok(token)
    }

    fn open(
        &self,
        token: &str,
        prefix: &str,
        label: &[u8],
        purpose: &str,
    ) -> Result<(ReferenceKey, Vec<u8>), DocumentRoundTripError> {
        if self.mode() != teaql_runtime::ReferenceMode::Governed {
            return Err(DocumentRoundTripError::invalid());
        }
        let (key_id, encoded) = parse_token_with_prefix(token, prefix)?;
        let key = self
            .reference_key_by_id(key_id)?
            .ok_or_else(DocumentRoundTripError::invalid)?;
        let envelope = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| DocumentRoundTripError::invalid())?;
        if envelope.len() < 28 || URL_SAFE_NO_PAD.encode(&envelope) != encoded {
            return Err(DocumentRoundTripError::invalid());
        }
        let encryption_key = derive_key(
            key.secret(),
            label,
            self.service_name(),
            self.environment_name(),
            &key.key_id,
        )?;
        let aad = encode_document_aad_with_prefix(
            prefix,
            &key.key_id,
            self.service_name(),
            self.environment_name(),
            purpose,
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
        Ok((key, plaintext))
    }
}

fn expiry(
    now: DateTime<chrono::Utc>,
    lifetime: Duration,
) -> Result<DateTime<chrono::Utc>, DocumentRoundTripError> {
    now.checked_add_signed(
        TimeDelta::from_std(lifetime).map_err(|_| DocumentRoundTripError::invalid())?,
    )
    .ok_or_else(DocumentRoundTripError::invalid)
}

fn timestamp(seconds: i64) -> Result<DateTime<chrono::Utc>, DocumentRoundTripError> {
    DateTime::from_timestamp(seconds, 0).ok_or_else(DocumentRoundTripError::invalid)
}

struct DecodedBound {
    actor_fingerprint: [u8; 32],
    issued_at: i64,
    expires_at: i64,
    document: BoundDocumentSnapshot,
}

fn encode_bound_payload(
    document: &BoundDocumentSnapshot,
    actor: [u8; 32],
    issued_at: i64,
    expires_at: i64,
) -> Result<Vec<u8>, DocumentRoundTripError> {
    let mut output = encode_document_payload(&document.snapshot, actor, issued_at, expires_at)?;
    output[0] = 2;
    write_count(&mut output, document.bindings.len())?;
    for binding in &document.bindings {
        write_text(&mut output, &binding.row_key)?;
        write_text(&mut output, &binding.canonical_field)?;
        write_text(&mut output, &binding.entity_type)?;
        output.push(u8::from(binding.reference.is_some()));
        if let Some(reference) = &binding.reference {
            write_text(&mut output, reference.as_str())?;
        }
        output.extend_from_slice(&binding.binding_version.to_be_bytes());
        output.push(u8::from(binding.nullable));
        output.push(u8::from(binding.writable));
    }
    Ok(output)
}

fn decode_bound_payload(data: &[u8]) -> Result<DecodedBound, DocumentRoundTripError> {
    let (base, mut offset) = decode_document_payload_prefix(data, 2)?;
    let count = read_count(data, &mut offset, MAX_BINDINGS)?;
    let mut bindings = Vec::with_capacity(count);
    for _ in 0..count {
        let row_key = read_text(data, &mut offset)?;
        let canonical_field = read_text(data, &mut offset)?;
        let entity_type = read_text(data, &mut offset)?;
        let reference = match read_u8(data, &mut offset)? {
            0 => None,
            1 => Some(
                BlobRef::new(read_text(data, &mut offset)?)
                    .map_err(|_| DocumentRoundTripError::invalid())?,
            ),
            _ => return Err(DocumentRoundTripError::invalid()),
        };
        let binding_version = read_u64(data, &mut offset)?;
        let nullable = read_flag(data, &mut offset)?;
        let writable = read_flag(data, &mut offset)?;
        bindings.push(DocumentBlobBindingSnapshot {
            row_key,
            canonical_field,
            entity_type,
            reference,
            binding_version,
            nullable,
            writable,
        });
    }
    if offset != data.len() {
        return Err(DocumentRoundTripError::invalid());
    }
    // Validate original order before the constructor's canonical sort.
    if bindings.windows(2).any(|pair| {
        (&pair[0].row_key, &pair[0].canonical_field) >= (&pair[1].row_key, &pair[1].canonical_field)
    }) {
        return Err(DocumentRoundTripError::invalid());
    }
    Ok(DecodedBound {
        actor_fingerprint: base.actor_fingerprint,
        issued_at: base.issued_at,
        expires_at: base.expires_at,
        document: BoundDocumentSnapshot::new(base.snapshot, bindings)?,
    })
}

struct DecodedSelection {
    actor_fingerprint: [u8; 32],
    issued_at: i64,
    expires_at: i64,
    claim: BlobSelectionClaim,
}

fn encode_selection_payload(
    claim: &BlobSelectionClaim,
    actor: [u8; 32],
    issued_at: i64,
    expires_at: i64,
) -> Result<Vec<u8>, DocumentRoundTripError> {
    let mut output = Vec::new();
    output.push(1);
    output.extend_from_slice(&actor);
    output.extend_from_slice(&issued_at.to_be_bytes());
    output.extend_from_slice(&expires_at.to_be_bytes());
    encode_scope(&mut output, &claim.target.scope)?;
    write_text(&mut output, &claim.target.target_entity)?;
    write_text(&mut output, &claim.target.canonical_field)?;
    output.push(claim.target.kind.wire_value());
    write_text(&mut output, &claim.target.target_key)?;
    write_text(&mut output, claim.reference.as_str())?;
    write_text(&mut output, &claim.target.command_id)?;
    Ok(output)
}

fn decode_selection_payload(data: &[u8]) -> Result<DecodedSelection, DocumentRoundTripError> {
    let mut offset = 0;
    if read_u8(data, &mut offset)? != 1 {
        return Err(DocumentRoundTripError::invalid());
    }
    let actor_fingerprint: [u8; 32] = read_exact(data, &mut offset, 32)?
        .try_into()
        .map_err(|_| DocumentRoundTripError::invalid())?;
    let issued_at = read_i64(data, &mut offset)?;
    let expires_at = read_i64(data, &mut offset)?;
    let scope = decode_scope(data, &mut offset)?;
    let target_entity = read_text(data, &mut offset)?;
    let canonical_field = read_text(data, &mut offset)?;
    let kind = BlobSelectionTargetKind::from_wire(read_u8(data, &mut offset)?)?;
    let target_key = read_text(data, &mut offset)?;
    let reference = BlobRef::new(read_text(data, &mut offset)?)
        .map_err(|_| DocumentRoundTripError::invalid())?;
    let command_id = read_text(data, &mut offset)?;
    if offset != data.len() {
        return Err(DocumentRoundTripError::invalid());
    }
    let claim = BlobSelectionClaim {
        target: BlobSelectionTarget {
            scope,
            target_entity,
            canonical_field,
            kind,
            target_key,
            command_id,
        },
        reference,
    };
    claim.validate()?;
    Ok(DecodedSelection {
        actor_fingerprint,
        issued_at,
        expires_at,
        claim,
    })
}

fn read_flag(data: &[u8], offset: &mut usize) -> Result<bool, DocumentRoundTripError> {
    match read_u8(data, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DocumentRoundTripError::invalid()),
    }
}

fn require_selection_target(
    document: &BoundDocumentSnapshot,
    target: &BlobSelectionTarget,
) -> Result<(), DocumentRoundTripError> {
    if document.snapshot.scope != target.scope {
        return Err(DocumentRoundTripError::scope_mismatch());
    }
    let allowed = match target.kind {
        BlobSelectionTargetKind::ExistingRow => {
            document
                .snapshot
                .entity(&target.target_key)
                .is_some_and(|entity| {
                    entity.identity.entity_type == target.target_entity
                        && entity.writable_fields.contains(&target.canonical_field)
                })
                && document.bindings.iter().any(|binding| {
                    binding.row_key == target.target_key
                        && binding.canonical_field == target.canonical_field
                        && binding.writable
                })
        }
        BlobSelectionTargetKind::NewClientKey => {
            document.snapshot.relations.iter().any(|relation| {
                relation.completeness == DocumentRelationCompleteness::Complete
                    && relation.writable
                    && relation
                        .allowed_mutations
                        .contains(&DocumentMutationKind::CreateChild)
            })
        }
    };
    if !allowed {
        return Err(DocumentRoundTripError::projection_violation());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeploymentProfile, ReferenceKey, StaticReferenceKeyProvider};
    use sha2::Digest;
    use std::sync::Arc;

    fn principal(subject: &str) -> TrustedReferencePrincipal {
        TrustedReferencePrincipal::new("oidc", subject, "Merchant", 7).unwrap()
    }

    fn runtime(environment: &str, now: i64) -> ContextBoundReferenceRuntime {
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
            Arc::new(
                |_: &TrustedReferencePrincipal,
                 _: &ReferenceDocumentScope,
                 _: &ReferenceIdentity| Ok(()),
            ),
        )
        .unwrap()
        .with_clock(move || DateTime::from_timestamp(now, 0).unwrap())
        .with_nonce_source(|| [0x33; 12])
    }

    fn bound_document() -> BoundDocumentSnapshot {
        let scope = ReferenceDocumentScope::new("doc-1", "edit-order", "Order", 10, 4).unwrap();
        let snapshot = DocumentSnapshot::new(
            "model-v1",
            "ORD-X1",
            scope,
            ReferenceIdentity::new("Order", 10, 4).unwrap(),
            vec![
                DocumentEntitySnapshot::new(
                    "item:0",
                    ReferenceIdentity::new("OrderItem", 20, 2).unwrap(),
                    ["attachment", "product_code", "quantity"],
                    ["attachment", "quantity"],
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
        .unwrap();
        BoundDocumentSnapshot::new(
            snapshot,
            vec![DocumentBlobBindingSnapshot {
                row_key: "item:0".into(),
                canonical_field: "attachment".into(),
                entity_type: "OrderItem".into(),
                reference: Some(BlobRef::new("blob_A").unwrap()),
                binding_version: u64::MAX,
                nullable: true,
                writable: true,
            }],
        )
        .unwrap()
    }

    fn target(command: &str) -> BlobSelectionTarget {
        BlobSelectionTarget {
            scope: bound_document().snapshot.scope,
            target_entity: "OrderItem".into(),
            canonical_field: "attachment".into(),
            kind: BlobSelectionTargetKind::ExistingRow,
            target_key: "item:0".into(),
            command_id: command.into(),
        }
    }

    #[test]
    fn java_tqd2_and_tqs1_vectors_match_byte_for_byte() {
        let runtime = runtime("test", 1_800_000_000);
        let principal = principal("alice");
        let document = runtime
            .issue_bound_document_snapshot(&principal, bound_document(), Duration::from_secs(300))
            .unwrap();
        assert_eq!(
            format!("{:x}", sha2::Sha256::digest(document.as_bytes())),
            "e7c81ca089b0a3e0585faefcc275aeb44b456d666d9a4a17949c92b1891089d7"
        );
        assert!(document.starts_with("tqd2.k1."));
        assert!(!document.contains("blob_A"));
        assert_eq!(
            runtime
                .consume_bound_document_snapshot(&principal, &document, "Order", "edit-order")
                .unwrap()
                .document,
            bound_document()
        );

        let claim = BlobSelectionClaim {
            target: target("command-1"),
            reference: BlobRef::new("blob_B").unwrap(),
        };
        let selection = runtime
            .issue_blob_selection(
                &principal,
                &document,
                claim.clone(),
                Duration::from_secs(120),
            )
            .unwrap();
        assert_eq!(
            format!("{:x}", sha2::Sha256::digest(selection.as_bytes())),
            "31303e3e251dd0e8963bf2a4d8897d1eccde63a485aaf287f88f29d40757ba96"
        );
        assert!(selection.starts_with("tqs1.k1."));
        assert!(!selection.contains("blob_B"));
        for _ in 0..2 {
            assert_eq!(
                runtime
                    .consume_blob_selection(&principal, &document, &selection, &claim.target)
                    .unwrap()
                    .claim,
                claim
            );
        }
    }

    #[test]
    fn bound_selection_rejects_foreign_actor_command_environment_and_expiry() {
        let provider = runtime("test", 1_800_000_000);
        let alice = principal("alice");
        let document = provider
            .issue_bound_document_snapshot(&alice, bound_document(), Duration::from_secs(300))
            .unwrap();
        let claim = BlobSelectionClaim {
            target: target("command-1"),
            reference: BlobRef::new("blob_B").unwrap(),
        };
        let selection = provider
            .issue_blob_selection(&alice, &document, claim.clone(), Duration::from_secs(120))
            .unwrap();
        assert_eq!(
            provider
                .consume_blob_selection(&principal("bob"), &document, &selection, &claim.target)
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH"
        );
        assert_eq!(
            provider
                .consume_blob_selection(&alice, &document, &selection, &target("other-command"))
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH"
        );
        assert_eq!(
            runtime("production", 1_800_000_000)
                .consume_bound_document_snapshot(&alice, &document, "Order", "edit-order")
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_INVALID"
        );
        assert_eq!(
            runtime("test", 1_800_000_121)
                .consume_blob_selection(&alice, &document, &selection, &claim.target)
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_EXPIRED"
        );
        assert_eq!(
            provider
                .issue_blob_selection(&alice, &document, claim, Duration::from_secs(301))
                .unwrap_err()
                .code(),
            "ROUND_TRIP_REFERENCE_INVALID"
        );
    }

    #[test]
    fn new_client_key_needs_complete_writable_create_relation() {
        let provider = runtime("test", 1_800_000_000);
        let alice = principal("alice");
        let document = provider
            .issue_bound_document_snapshot(&alice, bound_document(), Duration::from_secs(300))
            .unwrap();
        let mut new_target = target("create-1");
        new_target.kind = BlobSelectionTargetKind::NewClientKey;
        new_target.target_key = "client-row-7".into();
        let claim = BlobSelectionClaim {
            target: new_target.clone(),
            reference: BlobRef::new("blob_C").unwrap(),
        };
        let selection = provider
            .issue_blob_selection(&alice, &document, claim.clone(), Duration::from_secs(120))
            .unwrap();
        assert_eq!(
            provider
                .consume_blob_selection(&alice, &document, &selection, &new_target)
                .unwrap()
                .claim,
            claim
        );

        let mut incomplete = bound_document();
        incomplete.snapshot.relations[0].completeness = DocumentRelationCompleteness::Partial;
        let incomplete_document = provider
            .issue_bound_document_snapshot(&alice, incomplete, Duration::from_secs(300))
            .unwrap();
        assert_eq!(
            provider
                .issue_blob_selection(
                    &alice,
                    &incomplete_document,
                    claim,
                    Duration::from_secs(120)
                )
                .unwrap_err()
                .code(),
            "DOCUMENT_PROJECTION_VIOLATION"
        );
    }
}
