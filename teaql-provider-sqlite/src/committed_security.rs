//! Startup-owned authenticated ingress; never a Query/Mutation/TFP route.
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
        })
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
        let tx = self
            .executor
            .begin_sql()
            .await
            .map_err(|_| rejected("AUDIT_UNAVAILABLE"))?;
        let e = &delivery.event;
        let result = (|| -> Result<(), CommittedEventError> {
            let changed=self.executor.lock()?.execute(
                "INSERT INTO teaql_event_rejection(reason_code,source_sequence,source_fingerprint,change_fingerprint,target_fingerprint,payload_fingerprint) VALUES (?1,?2,?3,?4,?5,?6)",
                params![code,i64::try_from(e.source_sequence).unwrap_or(0),fingerprint(&e.source_runtime_id,MAX_ID),fingerprint(&e.change_set_id,MAX_ID),fingerprint(&self.target,MAX_ID),fingerprint(&e.payload_json,MAX_PAYLOAD)])?;
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
    let mut mac = HmacSha256::new_from_slice(secret).expect("validated HMAC key");
    mac.update(b"teaql.committed.receiver.v1\0");
    let sequence = e.source_sequence.to_string();
    for text in [
        key_id,
        target,
        &e.source_runtime_id,
        &sequence,
        &e.change_set_id,
        &e.model_hash,
        &e.payload_sha256,
        &e.payload_json,
    ] {
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
