//! Startup-owned authenticated ingress; never a Query/Mutation/TFP route.
use crate::committed_events::{
    bootstrap::SnapshotReceipt,
    snapshot::{SnapshotImportMode, SnapshotLimits, SnapshotRecord},
};
use crate::{
    SqliteMutationExecutor,
    committed_events::{CommittedEventError, CommittedEventTarget, InboxOutcome, OutboxRecord},
};
use hmac::{Hmac, Mac};
use rusqlite::params;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, fmt, sync::Arc};
use teaql_runtime::UserContext;
use teaql_sql::{SqlTransaction, SqlTransactionTransport};

const MAX_PAYLOAD: usize = 1024 * 1024;
const MAX_ID: usize = 256;
const MAX_SNAPSHOT_PAYLOAD: usize = 16 * 1024 * 1024;
type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct SignedDelivery {
    pub event: OutboxRecord,
    pub key_id: String,
    pub target_id: String,
    pub signature: Vec<u8>,
}
impl fmt::Debug for SignedDelivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CommittedDelivery[content=<redacted>]")
    }
}
#[derive(Clone)]
pub struct SignedSnapshotDelivery {
    pub snapshot: SnapshotRecord,
    pub key_id: String,
    pub target_id: String,
    pub signature: Vec<u8>,
}
impl fmt::Debug for SignedSnapshotDelivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CommittedSnapshotDelivery[content=<redacted>]")
    }
}
/// Safe verdict; unexpected storage/transaction outcomes retain a typed cause
/// for trusted recovery code, never a claim of confirmed business rollback.
pub struct ReceiverError {
    code: &'static str,
    apply_error: Option<CommittedEventError>,
}
impl ReceiverError {
    pub fn code(&self) -> &'static str {
        self.code
    }
    pub fn apply_error(&self) -> Option<&CommittedEventError> {
        self.apply_error.as_ref()
    }
}
impl fmt::Display for ReceiverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "committed receiver: {}", self.code)
    }
}
impl fmt::Debug for ReceiverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for ReceiverError {}
/// Preserve closed permanent rejection codes at trusted transport adapters;
/// unknown storage/transaction causes stay typed, never confirmed rejections.
impl From<ReceiverError> for CommittedEventError {
    fn from(error: ReceiverError) -> Self {
        use crate::committed_observation::DeliveryRejection as R;
        let reason = match error.code {
            "UNAUTHORIZED_SOURCE" => Some(R::UnauthorizedSource),
            "UNAUTHORIZED_TARGET" => Some(R::UnauthorizedTarget),
            "UNKNOWN_KEY" => Some(R::UnknownKey),
            "INVALID_SIGNATURE" => Some(R::InvalidSignature),
            "INVALID_ENVELOPE" => Some(R::InvalidEnvelope),
            "MODEL_MISMATCH" => Some(R::ModelMismatch),
            "SEQUENCE_GAP" => Some(R::SequenceGap),
            "DUPLICATE_CONFLICT" => Some(R::DuplicateConflict),
            "VERSION_CONFLICT" => Some(R::VersionConflict),
            "SEQUENCE_EXHAUSTED" => Some(R::SequenceExhausted),
            "INVALID_SNAPSHOT" => Some(R::InvalidSnapshot),
            "SNAPSHOT_CONFLICT" => Some(R::SnapshotConflict),
            "TARGET_NOT_EMPTY" => Some(R::TargetNotEmpty),
            _ => None,
        };
        match reason {
            Some(reason) => Self::DeliveryRejected(reason),
            None => error.apply_error.unwrap_or(Self::TransportUnavailable),
        }
    }
}
fn rejected(code: &'static str) -> ReceiverError {
    ReceiverError {
        code,
        apply_error: None,
    }
}

pub struct SqliteCommittedReceiver {
    executor: SqliteMutationExecutor,
    source: String,
    target: String,
    model: String,
    keys: HashMap<String, Vec<u8>>,
    snapshot_limits: SnapshotLimits,
    max_snapshot_bytes: usize,
    snapshot_mode: SnapshotImportMode,
}
impl fmt::Debug for SqliteCommittedReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SqliteCommittedReceiver[authority=<redacted>]")
    }
}
impl SqliteCommittedReceiver {
    pub fn new(
        executor: SqliteMutationExecutor,
        source: impl Into<String>,
        target: impl Into<String>,
        model: impl Into<String>,
        keys: HashMap<String, Vec<u8>>,
    ) -> Result<Self, ReceiverError> {
        let source = source.into();
        let target = target.into();
        let model = model.into();
        field(&source, MAX_ID)?;
        field(&target, MAX_ID)?;
        CommittedEventTarget::new(&source, &model)
            .map_err(|_| rejected("INVALID_CONFIGURATION"))?;
        if keys.is_empty() || keys.len() > 16 {
            return Err(rejected("INVALID_CONFIGURATION"));
        }
        for (id, value) in &keys {
            field(id, MAX_ID)?;
            key(value)?;
        }
        Ok(Self {
            executor,
            source,
            target,
            model,
            keys,
            snapshot_limits: SnapshotLimits::default(),
            max_snapshot_bytes: MAX_SNAPSHOT_PAYLOAD,
            snapshot_mode: SnapshotImportMode::RequireEmpty,
        })
    }
    /// Trusted startup policy. Wire messages cannot choose a destructive mode
    /// or relax these limits.
    pub fn with_snapshot_limits(
        mut self,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self, ReceiverError> {
        if max_bytes > MAX_SNAPSHOT_PAYLOAD {
            return Err(rejected("INVALID_CONFIGURATION"));
        }
        self.snapshot_limits = SnapshotLimits::new(max_rows, max_bytes)
            .map_err(|_| rejected("INVALID_CONFIGURATION"))?;
        self.max_snapshot_bytes = max_bytes;
        Ok(self)
    }
    /// Local startup policy only. No message can select destructive replacement,
    /// and the provider still refuses replacement after stream initialization.
    pub fn with_snapshot_import_mode(mut self, mode: SnapshotImportMode) -> Self {
        self.snapshot_mode = mode;
        self
    }
    fn context(&self, context: &UserContext) -> Result<(), ReceiverError> {
        let installed = context
            .get_resource::<SqliteMutationExecutor>()
            .ok_or_else(|| rejected("INVALID_CONFIGURATION"))?;
        let authority = context
            .get_resource::<CommittedEventTarget>()
            .ok_or_else(|| rejected("INVALID_CONFIGURATION"))?;
        if !Arc::ptr_eq(&self.executor.connection(), &installed.connection())
            || authority.source_runtime_id != self.source
            || authority.model_hash != self.model
        {
            return Err(rejected("INVALID_CONFIGURATION"));
        }
        Ok(())
    }
    fn independent(&self) -> Result<(), ReceiverError> {
        if !self
            .executor
            .lock()
            .map_err(|_| rejected("APPLY_UNAVAILABLE"))?
            .is_autocommit()
        {
            return Err(rejected("AMBIENT_TRANSACTION"));
        }
        Ok(())
    }
    /// Explicit provider-infrastructure installation, no domain-row seeding.
    pub fn ensure_schema(&self, context: &UserContext) -> Result<(), ReceiverError> {
        self.context(context)?;
        self.independent()?;
        self.executor
            .ensure_committed_event_schema()
            .map_err(|_| rejected("APPLY_UNAVAILABLE"))?;
        self.executor.lock().map_err(|_|rejected("APPLY_UNAVAILABLE"))?.execute_batch(
            "CREATE TABLE IF NOT EXISTS teaql_event_rejection(audit_id INTEGER PRIMARY KEY,reason_code TEXT NOT NULL,source_sequence INTEGER NOT NULL,source_fingerprint TEXT NOT NULL,change_fingerprint TEXT NOT NULL,target_fingerprint TEXT NOT NULL,payload_fingerprint TEXT NOT NULL)"
        ).map_err(|_|rejected("APPLY_UNAVAILABLE"))?;
        Ok(())
    }
    pub fn sign(
        key_id: &str,
        target: &str,
        secret: &[u8],
        event: OutboxRecord,
    ) -> Result<SignedDelivery, ReceiverError> {
        field(key_id, MAX_ID)?;
        field(target, MAX_ID)?;
        key(secret)?;
        record(&event)?;
        let signature = mac(key_id, target, secret, &event)
            .finalize()
            .into_bytes()
            .to_vec();
        Ok(SignedDelivery {
            event,
            key_id: key_id.into(),
            target_id: target.into(),
            signature,
        })
    }
    pub fn sign_snapshot(
        key_id: &str,
        target: &str,
        secret: &[u8],
        snapshot: SnapshotRecord,
    ) -> Result<SignedSnapshotDelivery, ReceiverError> {
        field(key_id, MAX_ID)?;
        field(target, MAX_ID)?;
        key(secret)?;
        snapshot_record(&snapshot, MAX_SNAPSHOT_PAYLOAD)?;
        let signature = snapshot_mac(key_id, target, secret, &snapshot)
            .finalize()
            .into_bytes()
            .to_vec();
        Ok(SignedSnapshotDelivery {
            snapshot,
            key_id: key_id.into(),
            target_id: target.into(),
            signature,
        })
    }
    fn verify_snapshot(&self, delivery: &SignedSnapshotDelivery) -> Result<(), ReceiverError> {
        field(&delivery.key_id, MAX_ID)?;
        field(&delivery.target_id, MAX_ID)?;
        snapshot_record(&delivery.snapshot, self.max_snapshot_bytes)?;
        if delivery.signature.len() != 32 {
            return Err(rejected("INVALID_ENVELOPE"));
        }
        if delivery.snapshot.source_runtime_id != self.source {
            return Err(rejected("UNAUTHORIZED_SOURCE"));
        }
        if delivery.target_id != self.target {
            return Err(rejected("UNAUTHORIZED_TARGET"));
        }
        if delivery.snapshot.model_hash != self.model {
            return Err(rejected("MODEL_MISMATCH"));
        }
        let secret = self
            .keys
            .get(&delivery.key_id)
            .ok_or_else(|| rejected("UNKNOWN_KEY"))?;
        snapshot_mac(
            &delivery.key_id,
            &delivery.target_id,
            secret,
            &delivery.snapshot,
        )
        .verify_slice(&delivery.signature)
        .map_err(|_| rejected("INVALID_SIGNATURE"))
    }
    /// Separate authenticated snapshot boundary. Restore uses trusted startup policy;
    /// receipts are constructed only after the authoritative transaction commits.
    pub async fn receive_snapshot(
        &self,
        context: &UserContext,
        delivery: &SignedSnapshotDelivery,
    ) -> Result<SnapshotReceipt, ReceiverError> {
        self.context(context)?;
        self.independent()?;
        if let Err(error) = self.verify_snapshot(delivery) {
            self.audit_snapshot(delivery, error.code).await?;
            return Err(error);
        }
        match self
            .executor
            .initialize_snapshot_and_acknowledge(
                context,
                &delivery.snapshot,
                &self.target,
                self.snapshot_mode,
                self.snapshot_limits,
            )
            .await
        {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                let code = match &error {
                    CommittedEventError::UnauthorizedSource => "UNAUTHORIZED_SOURCE",
                    CommittedEventError::ModelMismatch => "MODEL_MISMATCH",
                    CommittedEventError::InvalidEnvelope(_) | CommittedEventError::Json(_) => {
                        "INVALID_SNAPSHOT"
                    }
                    CommittedEventError::DuplicateConflict => "SNAPSHOT_CONFLICT",
                    CommittedEventError::VersionConflict { .. } => "VERSION_CONFLICT",
                    CommittedEventError::SequenceExhausted => "SEQUENCE_EXHAUSTED",
                    _ => {
                        return Err(ReceiverError {
                            code: "APPLY_FAILED",
                            apply_error: Some(error),
                        });
                    }
                };
                self.audit_snapshot(delivery, code).await?;
                Err(rejected(code))
            }
        }
    }
    fn verify(&self, delivery: &SignedDelivery) -> Result<(), ReceiverError> {
        field(&delivery.key_id, MAX_ID)?;
        field(&delivery.target_id, MAX_ID)?;
        record(&delivery.event)?;
        if delivery.signature.len() != 32 {
            return Err(rejected("INVALID_ENVELOPE"));
        }
        if delivery.event.source_runtime_id != self.source {
            return Err(rejected("UNAUTHORIZED_SOURCE"));
        }
        if delivery.target_id != self.target {
            return Err(rejected("UNAUTHORIZED_TARGET"));
        }
        if delivery.event.model_hash != self.model {
            return Err(rejected("MODEL_MISMATCH"));
        }
        let secret = self
            .keys
            .get(&delivery.key_id)
            .ok_or_else(|| rejected("UNKNOWN_KEY"))?;
        mac(
            &delivery.key_id,
            &delivery.target_id,
            secret,
            &delivery.event,
        )
        .verify_slice(&delivery.signature)
        .map_err(|_| rejected("INVALID_SIGNATURE"))
    }
    /// Authentication precedes metadata and domain writes. Exact in-stream
    /// retries retain the existing stable duplicate acknowledgement.
    pub async fn receive(
        &self,
        context: &UserContext,
        delivery: &SignedDelivery,
    ) -> Result<InboxOutcome, ReceiverError> {
        self.context(context)?;
        self.independent()?;
        if let Err(error) = self.verify(delivery) {
            self.audit(delivery, error.code).await?;
            return Err(error);
        }
        match self
            .executor
            .apply_model_checked_committed_set(context, &delivery.event)
            .await
        {
            Ok(value) => Ok(value),
            Err(error) => {
                let code = match &error {
                    CommittedEventError::UnauthorizedSource => "UNAUTHORIZED_SOURCE",
                    CommittedEventError::ModelMismatch => "MODEL_MISMATCH",
                    CommittedEventError::InvalidEnvelope(_) | CommittedEventError::Json(_) => {
                        "INVALID_ENVELOPE"
                    }
                    CommittedEventError::SequenceGap { .. } => "SEQUENCE_GAP",
                    CommittedEventError::DuplicateConflict => "DUPLICATE_CONFLICT",
                    CommittedEventError::VersionConflict { .. } => "VERSION_CONFLICT",
                    CommittedEventError::SequenceExhausted => "SEQUENCE_EXHAUSTED",
                    _ => {
                        return Err(ReceiverError {
                            code: "APPLY_FAILED",
                            apply_error: Some(error),
                        });
                    }
                };
                self.audit(delivery, code).await?;
                Err(rejected(code))
            }
        }
    }
    async fn audit(
        &self,
        delivery: &SignedDelivery,
        code: &'static str,
    ) -> Result<(), ReceiverError> {
        let e = &delivery.event;
        self.audit_fields(
            &e.source_runtime_id,
            e.source_sequence,
            &e.change_set_id,
            &e.payload_json,
            MAX_PAYLOAD,
            code,
        )
        .await
    }
    async fn audit_snapshot(
        &self,
        delivery: &SignedSnapshotDelivery,
        code: &'static str,
    ) -> Result<(), ReceiverError> {
        let s = &delivery.snapshot;
        let reason = format!("SNAPSHOT_{code}");
        self.audit_fields(
            &s.source_runtime_id,
            s.source_sequence,
            &s.snapshot_id,
            &s.payload_json,
            MAX_SNAPSHOT_PAYLOAD,
            &reason,
        )
        .await
    }
    async fn audit_fields(
        &self,
        source: &str,
        sequence: u64,
        identity: &str,
        payload: &str,
        payload_limit: usize,
        code: &str,
    ) -> Result<(), ReceiverError> {
        let tx = self
            .executor
            .begin_sql()
            .await
            .map_err(|_| rejected("AUDIT_UNAVAILABLE"))?;
        let result = (|| -> Result<(), CommittedEventError> {
            let changed=self.executor.lock()?.execute(
                "INSERT INTO teaql_event_rejection(reason_code,source_sequence,source_fingerprint,change_fingerprint,target_fingerprint,payload_fingerprint) VALUES (?1,?2,?3,?4,?5,?6)",
                params![code,i64::try_from(sequence).unwrap_or(0),fingerprint(source,MAX_ID),fingerprint(identity,MAX_ID),fingerprint(&self.target,MAX_ID),fingerprint(payload,payload_limit)])?;
            if changed != 1 {
                return Err(CommittedEventError::InvalidEnvelope("audit row missing"));
            }
            Ok(())
        })();
        match result {
            Ok(()) => tx
                .commit_sql()
                .await
                .map_err(|_| rejected("AUDIT_UNAVAILABLE")),
            Err(_) => {
                let _ = tx.rollback_sql().await;
                Err(rejected("AUDIT_UNAVAILABLE"))
            }
        }
    }
}
fn field(text: &str, limit: usize) -> Result<(), ReceiverError> {
    if text.is_empty() || text.len() > limit {
        Err(rejected("INVALID_ENVELOPE"))
    } else {
        Ok(())
    }
}
fn key(secret: &[u8]) -> Result<(), ReceiverError> {
    if !(32..=128).contains(&secret.len()) {
        Err(rejected("INVALID_CONFIGURATION"))
    } else {
        Ok(())
    }
}
fn record(e: &OutboxRecord) -> Result<(), ReceiverError> {
    if e.source_sequence == 0 || e.source_sequence > i64::MAX as u64 {
        return Err(rejected("INVALID_ENVELOPE"));
    }
    for text in [
        &e.source_runtime_id,
        &e.change_set_id,
        &e.model_hash,
        &e.payload_sha256,
    ] {
        field(text, MAX_ID)?;
    }
    field(&e.payload_json, MAX_PAYLOAD)
}
fn mac(key_id: &str, target: &str, secret: &[u8], e: &OutboxRecord) -> HmacSha256 {
    let sequence = e.source_sequence.to_string();
    transcript_mac(
        secret,
        b"teaql.committed.receiver.v1\0",
        &[
            key_id,
            target,
            &e.source_runtime_id,
            &sequence,
            &e.change_set_id,
            &e.model_hash,
            &e.payload_sha256,
            &e.payload_json,
        ],
    )
}
fn snapshot_record(s: &SnapshotRecord, limit: usize) -> Result<(), ReceiverError> {
    if s.source_sequence > i64::MAX as u64 {
        return Err(rejected("INVALID_ENVELOPE"));
    }
    for text in [
        &s.source_runtime_id,
        &s.snapshot_id,
        &s.model_hash,
        &s.payload_sha256,
    ] {
        field(text, MAX_ID)?;
    }
    field(&s.payload_json, limit)
}
fn snapshot_mac(key_id: &str, target: &str, secret: &[u8], s: &SnapshotRecord) -> HmacSha256 {
    let sequence = s.source_sequence.to_string();
    transcript_mac(
        secret,
        b"teaql.committed.snapshot.receiver.v1\0",
        &[
            key_id,
            target,
            &s.source_runtime_id,
            &sequence,
            &s.snapshot_id,
            &s.model_hash,
            &s.payload_sha256,
            &s.payload_json,
        ],
    )
}
fn transcript_mac(secret: &[u8], domain: &[u8], fields: &[&str]) -> HmacSha256 {
    let mut mac = HmacSha256::new_from_slice(secret).expect("validated HMAC key");
    mac.update(domain);
    for text in fields {
        mac.update(&(text.len() as u32).to_be_bytes());
        mac.update(text.as_bytes());
    }
    mac
}
fn fingerprint(text: &str, limit: usize) -> String {
    if field(text, limit).is_err() {
        "invalid_or_oversize".into()
    } else {
        format!("{:x}", Sha256::digest(text.as_bytes()))
    }
}
