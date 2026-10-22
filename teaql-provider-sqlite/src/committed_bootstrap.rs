//! Durable per-target startup bootstrap, separate from business mutation APIs.
use super::snapshot::{self, SnapshotImportMode, SnapshotLimits, SnapshotRecord};
use super::*;
use std::future::Future;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedSnapshot {
    pub snapshot: SnapshotRecord,
    pub acknowledged: bool,
}
/// Receipt correlation, not authentication. The transport must authenticate its peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotReceipt {
    pub source_runtime_id: String,
    pub target_id: String,
    pub snapshot_id: String,
    pub source_sequence: u64,
    pub model_hash: String,
    pub payload_sha256: String,
    pub duplicate: bool,
}
fn invalid(message: &'static str) -> CommittedEventError {
    CommittedEventError::InvalidEnvelope(message)
}
fn identity<'a>(
    executor: &SqliteMutationExecutor,
    context: &'a UserContext,
    target: &str,
    id: &str,
) -> Result<&'a teaql_runtime::CommittedEventSource, CommittedEventError> {
    snapshot::provider(context, executor)?;
    let source = context
        .committed_event_source()
        .ok_or(invalid("snapshot source is not configured"))?;
    CommittedEventTarget::new(&source.source_runtime_id, &source.model_hash)?;
    if target.trim().is_empty() || id.trim().is_empty() {
        return Err(invalid("snapshot/target identity is required"));
    }
    Ok(source)
}
fn cursor(
    db: &Connection,
    source: &str,
    target: &str,
) -> Result<Option<(i64, String)>, CommittedEventError> {
    Ok(db.query_row("SELECT acked_sequence,model_hash FROM teaql_event_delivery_cursor WHERE source_runtime_id=?1 AND target_id=?2",params![source,target],|r|Ok((r.get(0)?,r.get(1)?))).optional()?)
}
fn existing(
    db: &Connection,
    source: &teaql_runtime::CommittedEventSource,
    target: &str,
    id: &str,
    limits: SnapshotLimits,
) -> Result<Option<PreparedSnapshot>, CommittedEventError> {
    let row: Option<(String,i64,String,String,String,i64)> = db.query_row("SELECT snapshot_id,source_sequence,model_hash,payload_sha256,payload_json,acknowledged FROM teaql_event_snapshot_publication WHERE source_runtime_id=?1 AND target_id=?2",params![source.source_runtime_id,target],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
    let Some((stored, n, model, hash, payload, flag)) = row else {
        return Ok(None);
    };
    if stored != id {
        return Err(CommittedEventError::DuplicateConflict);
    }
    if n < 0 || ![0, 1].contains(&flag) {
        return Err(invalid("invalid prepared snapshot state"));
    }
    let record = SnapshotRecord {
        source_runtime_id: source.source_runtime_id.clone(),
        source_sequence: n as u64,
        snapshot_id: stored,
        model_hash: model,
        payload_sha256: hash,
        payload_json: payload,
    };
    snapshot::validate(
        &record,
        &CommittedEventTarget::new(&source.source_runtime_id, &source.model_hash)?,
        limits,
    )?;
    match (flag, cursor(db, &source.source_runtime_id, target)?) {
        (0, None) => {}
        (1, Some((position, model))) if position >= n && model == source.model_hash => {}
        _ => return Err(CommittedEventError::CursorConflict),
    }
    Ok(Some(PreparedSnapshot {
        snapshot: record,
        acknowledged: flag == 1,
    }))
}

impl SqliteMutationExecutor {
    /// Capture once, retain immutable bytes before exposing them for transport.
    pub async fn prepare_committed_snapshot(
        &self,
        context: &UserContext,
        target: &str,
        id: &str,
        comment: &str,
        purpose: &str,
        limits: SnapshotLimits,
    ) -> Result<PreparedSnapshot, CommittedEventError> {
        let source = identity(self, context, target, id)?;
        if comment.trim().is_empty() || purpose.trim().is_empty() {
            return Err(invalid("snapshot intent is required"));
        }
        let tx = self.begin_snapshot_transaction(true)?;
        let prior = {
            let db = self.lock()?;
            existing(&db, source, target, id, limits)
        };
        if let Some(prepared) = snapshot::finish(tx, prior).await? {
            return Ok(prepared);
        }
        let captured = self
            .export_model_checked_snapshot(context, id, comment, purpose, limits)
            .await?;
        let tx = self.begin_snapshot_transaction(false)?;
        let result = (|| {
            let db = self.lock()?;
            if let Some(raced) = existing(&db, source, target, id, limits)? {
                return Ok(raced);
            }
            if cursor(&db, &source.source_runtime_id, target)?.is_some() {
                return Err(CommittedEventError::CursorConflict);
            }
            db.execute("INSERT INTO teaql_event_snapshot_publication(source_runtime_id,target_id,snapshot_id,source_sequence,model_hash,payload_sha256,payload_json,acknowledged) VALUES(?1,?2,?3,?4,?5,?6,?7,0)",params![source.source_runtime_id,target,captured.snapshot_id,captured.source_sequence as i64,captured.model_hash,captured.payload_sha256,captured.payload_json])?;
            Ok(PreparedSnapshot {
                snapshot: captured,
                acknowledged: false,
            })
        })();
        snapshot::finish(tx, result).await
    }

    /// One attempt; an ambiguous response leaves the original job pending.
    pub async fn publish_prepared_snapshot<F, Fut>(
        &self,
        context: &UserContext,
        target: &str,
        id: &str,
        limits: SnapshotLimits,
        deliver: F,
    ) -> Result<PreparedSnapshot, CommittedEventError>
    where
        F: FnOnce(SnapshotRecord) -> Fut,
        Fut: Future<Output = Result<SnapshotReceipt, CommittedEventError>>,
    {
        let source = identity(self, context, target, id)?;
        let tx = self.begin_snapshot_transaction(true)?;
        let prior = {
            let db = self.lock()?;
            existing(&db, source, target, id, limits)
        };
        let prepared = snapshot::finish(tx, prior)
            .await?
            .ok_or(invalid("snapshot has not been durably prepared"))?;
        if prepared.acknowledged {
            return Ok(prepared);
        }
        let record = prepared.snapshot;
        let attempt = self
            .journal_begin(
                &source.source_runtime_id,
                target,
                &source.model_hash,
                "snapshot",
                record.source_sequence,
                &record.snapshot_id,
                &record.payload_sha256,
            )
            .await?;
        let ack = match deliver(record.clone()).await {
            Ok(receipt) => receipt,
            Err(error) => {
                self.journal_failed(&source.source_runtime_id, target, attempt, &error)
                    .await?;
                return Err(error);
            }
        }; // No source SQL transaction held across transport.
        if ack.source_runtime_id != record.source_runtime_id
            || ack.target_id != target
            || ack.snapshot_id != record.snapshot_id
            || ack.source_sequence != record.source_sequence
            || ack.model_hash != record.model_hash
            || ack.payload_sha256 != record.payload_sha256
        {
            let error = CommittedEventError::AcknowledgementMismatch;
            self.journal_failed(&source.source_runtime_id, target, attempt, &error)
                .await?;
            return Err(error);
        }
        let tx = self.begin_snapshot_transaction(false)?;
        let result = (|| {
            let db = self.lock()?;
            let current = existing(&db, source, target, id, limits)?
                .ok_or(invalid("prepared snapshot disappeared"))?;
            if current.snapshot != record {
                return Err(CommittedEventError::DuplicateConflict);
            }
            if current.acknowledged {
                return Ok(current);
            } // Never rewind a tail already acknowledged.
            let high: i64 = db
                .query_row(
                    "SELECT current_sequence FROM teaql_event_sequence WHERE source_runtime_id=?1",
                    [&source.source_runtime_id],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(0);
            if high < 0 || (high as u64) < record.source_sequence {
                return Err(invalid("snapshot is ahead of source"));
            }
            db.execute("INSERT INTO teaql_event_delivery_cursor(source_runtime_id,target_id,acked_sequence,model_hash) VALUES(?1,?2,?3,?4)",params![source.source_runtime_id,target,record.source_sequence as i64,source.model_hash])?;
            let changed=db.execute("UPDATE teaql_event_snapshot_publication SET acknowledged=1 WHERE source_runtime_id=?1 AND target_id=?2 AND snapshot_id=?3 AND payload_sha256=?4 AND acknowledged=0",params![source.source_runtime_id,target,id,record.payload_sha256])?;
            if changed != 1 {
                return Err(CommittedEventError::CursorConflict);
            }
            crate::committed_observation::journal_ack(
                &db,
                &source.source_runtime_id,
                target,
                attempt,
            )?;
            Ok(PreparedSnapshot {
                snapshot: record,
                acknowledged: true,
            })
        })();
        snapshot::finish(tx, result).await
    }

    /// A target receipt exists only after its authoritative transaction commits.
    pub async fn initialize_snapshot_and_acknowledge(
        &self,
        context: &UserContext,
        record: &SnapshotRecord,
        target: &str,
        mode: SnapshotImportMode,
        limits: SnapshotLimits,
    ) -> Result<SnapshotReceipt, CommittedEventError> {
        if target.trim().is_empty() {
            return Err(invalid("target identity is required"));
        }
        let outcome = self
            .initialize_model_checked_snapshot(context, record, mode, limits)
            .await?;
        let (checkpoint, duplicate) = match outcome {
            InboxOutcome::Applied { checkpoint } => (checkpoint, false),
            InboxOutcome::Duplicate { checkpoint } => (checkpoint, true),
        };
        if checkpoint != record.source_sequence {
            return Err(invalid("target snapshot position differs"));
        }
        Ok(SnapshotReceipt {
            source_runtime_id: record.source_runtime_id.clone(),
            target_id: target.into(),
            snapshot_id: record.snapshot_id.clone(),
            source_sequence: record.source_sequence,
            model_hash: record.model_hash.clone(),
            payload_sha256: record.payload_sha256.clone(),
            duplicate,
        })
    }
}
