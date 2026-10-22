//! Durable observations, separate source ACK and target checkpoint authorities.
use crate::{
    SqliteMutationExecutor,
    committed_events::{CommittedEventError, CommittedEventTarget, snapshot},
};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use teaql_runtime::{RuntimeOperation, UserContext, start_runtime_operation};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceObservation {
    pub source_high_watermark: u64,
    pub acked_sequence: u64,
    pub sequence_lag: u64,
    pub backlog: u64,
    pub pending_snapshots: u64,
    pub attempts: u64,
    pub retries: u64,
    pub failures: u64,
    pub rejections: u64,
    pub dead_letters: u64,
    pub unresolved_attempts: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetObservation {
    pub checkpoint: u64,
    pub snapshot_baseline: u64,
    pub inbox_rows: u64,
    pub sequence_lag: Option<u64>,
    pub rejections: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    pub kind: String,
    pub sequence: u64,
    pub identity_fingerprint: String,
    pub payload_fingerprint: String,
    pub attempts: u64,
    pub reason: String,
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct Attempt {
    pub kind: &'static str,
    pub sequence: u64,
    pub ordinal: i64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryRejection {
    UnauthorizedSource,
    UnauthorizedTarget,
    UnknownKey,
    InvalidSignature,
    InvalidEnvelope,
    ModelMismatch,
    SequenceGap,
    DuplicateConflict,
    VersionConflict,
    SequenceExhausted,
    InvalidSnapshot,
    SnapshotConflict,
    TargetNotEmpty,
}
impl DeliveryRejection {
    pub fn code(self) -> &'static str {
        match self {
            Self::UnauthorizedSource => "UNAUTHORIZED_SOURCE",
            Self::UnauthorizedTarget => "UNAUTHORIZED_TARGET",
            Self::UnknownKey => "UNKNOWN_KEY",
            Self::InvalidSignature => "INVALID_SIGNATURE",
            Self::InvalidEnvelope => "INVALID_ENVELOPE",
            Self::ModelMismatch => "MODEL_MISMATCH",
            Self::SequenceGap => "SEQUENCE_GAP",
            Self::DuplicateConflict => "DUPLICATE_CONFLICT",
            Self::VersionConflict => "VERSION_CONFLICT",
            Self::SequenceExhausted => "SEQUENCE_EXHAUSTED",
            Self::InvalidSnapshot => "INVALID_SNAPSHOT",
            Self::SnapshotConflict => "SNAPSHOT_CONFLICT",
            Self::TargetNotEmpty => "TARGET_NOT_EMPTY",
        }
    }
}
pub(crate) const JOURNAL_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS teaql_event_delivery_journal (source_runtime_id TEXT NOT NULL,target_id TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('event','snapshot')),source_sequence INTEGER NOT NULL CHECK(source_sequence>=0),model_hash TEXT NOT NULL,identity_fingerprint TEXT NOT NULL,payload_fingerprint TEXT NOT NULL,attempts INTEGER NOT NULL CHECK(typeof(attempts)='integer' AND attempts>0),failures INTEGER NOT NULL CHECK(typeof(failures)='integer' AND failures>=0),rejections INTEGER NOT NULL CHECK(typeof(rejections)='integer' AND rejections>=0),dead_letter INTEGER NOT NULL CHECK(dead_letter IN (0,1)),last_outcome TEXT NOT NULL CHECK(last_outcome IN ('STARTED','ACKNOWLEDGED','FAILED')),last_reason TEXT NOT NULL,dead_reason TEXT NOT NULL,PRIMARY KEY(source_runtime_id,target_id,kind,source_sequence));";
fn unavailable() -> CommittedEventError {
    CommittedEventError::ObservationUnavailable
}
fn number(n: i64) -> Result<u64, CommittedEventError> {
    u64::try_from(n).map_err(|_| unavailable())
}
pub(crate) fn fingerprint(s: &str) -> String {
    format!("{:x}", Sha256::digest(s.as_bytes()))
}
fn valid(source: &str, target: &str, model: &str) -> Result<(), CommittedEventError> {
    CommittedEventTarget::new(source, model)?;
    if target.trim().is_empty() {
        return Err(unavailable());
    }
    Ok(())
}
fn emit(context: &UserContext, source: &str, target: &str, name: &str, values: &[(&str, u64)]) {
    let mut op = RuntimeOperation::new("replication", name)
        .attribute("teaql.replication.source_fingerprint", fingerprint(source))
        .attribute("teaql.replication.target_fingerprint", fingerprint(target));
    for (k, v) in values {
        op = op.attribute(format!("teaql.replication.{k}"), *v as i64);
    }
    start_runtime_operation(context.runtime_telemetry(), op).success(Default::default());
}
pub(crate) fn journal_ack(
    db: &Connection,
    source: &str,
    target: &str,
    attempt: Attempt,
) -> Result<(), CommittedEventError> {
    if db.execute("UPDATE teaql_event_delivery_journal SET last_outcome='ACKNOWLEDGED',last_reason='NONE',dead_letter=0,dead_reason='NONE' WHERE source_runtime_id=?1 AND target_id=?2 AND kind=?3 AND source_sequence=?4 AND attempts=?5 AND last_outcome='STARTED'",params![source,target,attempt.kind,attempt.sequence as i64,attempt.ordinal])?!=1{return Err(CommittedEventError::CursorConflict);}
    Ok(())
}
impl SqliteMutationExecutor {
    pub(crate) async fn journal_begin(
        &self,
        source: &str,
        target: &str,
        model: &str,
        kind: &'static str,
        sequence: u64,
        identity: &str,
        payload: &str,
    ) -> Result<Attempt, CommittedEventError> {
        valid(source, target, model)?;
        let n = i64::try_from(sequence).map_err(|_| unavailable())?;
        if !["event", "snapshot"].contains(&kind)
            || payload.len() != 64
            || !payload
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(unavailable());
        }
        let tx = self.begin_snapshot_transaction(false)?;
        let result = (|| {
            let db = self.lock()?;
            let row:Option<(i64,String,String,String,String)>=db.query_row("SELECT attempts,model_hash,identity_fingerprint,payload_fingerprint,last_outcome FROM teaql_event_delivery_journal WHERE source_runtime_id=?1 AND target_id=?2 AND kind=?3 AND source_sequence=?4",params![source,target,kind,n],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
            let ordinal = if let Some((previous, m, id, hash, outcome)) = row {
                if previous < 1
                    || m != model
                    || id != fingerprint(identity)
                    || hash != payload
                    || outcome == "ACKNOWLEDGED"
                {
                    return Err(CommittedEventError::CursorConflict);
                }
                let next = previous.checked_add(1).ok_or_else(unavailable)?;
                if db.execute("UPDATE teaql_event_delivery_journal SET attempts=?1,last_outcome='STARTED',last_reason='NONE' WHERE source_runtime_id=?2 AND target_id=?3 AND kind=?4 AND source_sequence=?5 AND attempts=?6",params![next,source,target,kind,n,previous])?!=1{return Err(CommittedEventError::CursorConflict);}
                next
            } else {
                db.execute("INSERT INTO teaql_event_delivery_journal VALUES(?1,?2,?3,?4,?5,?6,?7,1,0,0,0,'STARTED','NONE','NONE')",params![source,target,kind,n,model,fingerprint(identity),payload])?;
                1
            };
            Ok(Attempt {
                kind,
                sequence,
                ordinal,
            })
        })();
        snapshot::finish(tx, result).await
    }
    pub(crate) async fn journal_failed(
        &self,
        source: &str,
        target: &str,
        attempt: Attempt,
        error: &CommittedEventError,
    ) -> Result<(), CommittedEventError> {
        let (permanent, code) = match error {
            CommittedEventError::DeliveryRejected(r) => (true, r.code()),
            CommittedEventError::UnauthorizedSource => (true, "UNAUTHORIZED_SOURCE"),
            CommittedEventError::ModelMismatch => (true, "MODEL_MISMATCH"),
            CommittedEventError::VersionConflict { .. } => (true, "VERSION_CONFLICT"),
            CommittedEventError::SequenceGap { .. } => (true, "SEQUENCE_GAP"),
            CommittedEventError::DuplicateConflict => (true, "DUPLICATE_CONFLICT"),
            CommittedEventError::SequenceExhausted => (true, "SEQUENCE_EXHAUSTED"),
            CommittedEventError::AcknowledgementMismatch => (false, "ACK_MISMATCH"),
            CommittedEventError::CursorConflict => (false, "CURSOR_CONFLICT"),
            _ => (false, "TRANSPORT_UNAVAILABLE"),
        };
        let tx = self.begin_snapshot_transaction(false)?;
        let result = (|| {
            let changed=self.lock()?.execute("UPDATE teaql_event_delivery_journal SET failures=failures+1,rejections=rejections+?1,dead_letter=CASE WHEN ?1=1 THEN 1 ELSE dead_letter END,dead_reason=CASE WHEN ?1=1 THEN ?2 ELSE dead_reason END,last_outcome='FAILED',last_reason=?2 WHERE source_runtime_id=?3 AND target_id=?4 AND kind=?5 AND source_sequence=?6 AND attempts=?7 AND last_outcome='STARTED'",params![i64::from(permanent),code,source,target,attempt.kind,attempt.sequence as i64,attempt.ordinal])?;
            if changed != 1 {
                return Err(CommittedEventError::CursorConflict);
            }
            Ok(())
        })();
        snapshot::finish(tx, result).await
    }
    pub async fn observe_committed_source(
        &self,
        context: &UserContext,
        source: &str,
        target: &str,
        model: &str,
    ) -> Result<SourceObservation, CommittedEventError> {
        snapshot::provider(context, self)?;
        valid(source, target, model)?;
        let tx = self.begin_snapshot_transaction(true)?;
        let result = (|| {
            let db = self.lock()?;
            let wrong_model:i64=db.query_row("SELECT count(*) FROM teaql_event_delivery_journal WHERE source_runtime_id=?1 AND target_id=?2 AND model_hash<>?3",params![source,target,model],|r|r.get(0))?;
            if wrong_model != 0 {
                return Err(unavailable());
            }
            let cursor:Option<(i64,String)>=db.query_row("SELECT acked_sequence,model_hash FROM teaql_event_delivery_cursor WHERE source_runtime_id=?1 AND target_id=?2",params![source,target],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            let ack = match cursor {
                Some((n, m)) if m == model => number(n)?,
                Some(_) => return Err(unavailable()),
                None => 0,
            };
            let high = number(
                db.query_row(
                    "SELECT current_sequence FROM teaql_event_sequence WHERE source_runtime_id=?1",
                    [source],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(0),
            )?;
            if high < ack {
                return Err(unavailable());
            }
            let counts:(i64,i64,i64,i64,i64,i64)=db.query_row("SELECT COALESCE(SUM(attempts),0),COALESCE(SUM(attempts-1),0),COALESCE(SUM(failures),0),COALESCE(SUM(rejections),0),COALESCE(SUM(dead_letter),0),COALESCE(SUM(CASE WHEN last_outcome='STARTED' THEN 1 ELSE 0 END),0) FROM teaql_event_delivery_journal WHERE source_runtime_id=?1 AND target_id=?2 AND model_hash=?3",params![source,target,model],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
            let backlog=number(db.query_row("SELECT count(*) FROM teaql_event_outbox WHERE source_runtime_id=?1 AND source_sequence>?2",params![source,ack as i64],|r|r.get(0))?)?;
            let pending=number(db.query_row("SELECT count(*) FROM teaql_event_snapshot_publication WHERE source_runtime_id=?1 AND target_id=?2 AND model_hash=?3 AND acknowledged=0",params![source,target,model],|r|r.get(0))?)?;
            Ok(SourceObservation {
                source_high_watermark: high,
                acked_sequence: ack,
                sequence_lag: high - ack,
                backlog,
                pending_snapshots: pending,
                attempts: number(counts.0)?,
                retries: number(counts.1)?,
                failures: number(counts.2)?,
                rejections: number(counts.3)?,
                dead_letters: number(counts.4)?,
                unresolved_attempts: number(counts.5)?,
            })
        })();
        let s = snapshot::finish(tx, result).await?;
        emit(
            context,
            source,
            target,
            "source",
            &[
                ("source_watermark", s.source_high_watermark),
                ("source_ack", s.acked_sequence),
                ("sequence_lag", s.sequence_lag),
                ("backlog", s.backlog),
                ("pending_snapshots", s.pending_snapshots),
                ("attempts", s.attempts),
                ("retries", s.retries),
                ("failures", s.failures),
                ("rejections", s.rejections),
                ("dead_letters", s.dead_letters),
                ("unresolved_attempts", s.unresolved_attempts),
            ],
        );
        Ok(s)
    }
    pub async fn observe_committed_target(
        &self,
        context: &UserContext,
        source: &str,
        target: &str,
        model: &str,
        observed_source_high: Option<u64>,
    ) -> Result<TargetObservation, CommittedEventError> {
        snapshot::provider(context, self)?;
        valid(source, target, model)?;
        if observed_source_high.is_some_and(|n| n > i64::MAX as u64) {
            return Err(unavailable());
        }
        let tx = self.begin_snapshot_transaction(true)?;
        let result = (|| {
            let db = self.lock()?;
            let row:Option<(i64,String)>=db.query_row("SELECT source_sequence,model_hash FROM teaql_event_checkpoint WHERE source_runtime_id=?1",[source],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            let checkpoint = match row {
                Some((n, m)) if m == model => number(n)?,
                Some(_) => return Err(unavailable()),
                None => 0,
            };
            let lag = observed_source_high
                .map(|high| high.checked_sub(checkpoint).ok_or_else(unavailable))
                .transpose()?;
            let row:Option<(i64,String)>=db.query_row("SELECT source_sequence,model_hash FROM teaql_event_snapshot WHERE source_runtime_id=?1",[source],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            let baseline = match row {
                Some((n, m)) if m == model => number(n)?,
                Some(_) => return Err(unavailable()),
                None => 0,
            };
            if baseline > checkpoint {
                return Err(unavailable());
            }
            let inbox = number(db.query_row(
                "SELECT count(*) FROM teaql_event_inbox WHERE source_runtime_id=?1",
                [source],
                |r| r.get(0),
            )?)?;
            let rejections = number(db.query_row(
                "SELECT count(*) FROM teaql_event_rejection WHERE target_fingerprint=?1",
                [fingerprint(target)],
                |r| r.get(0),
            )?)?;
            Ok(TargetObservation {
                checkpoint,
                snapshot_baseline: baseline,
                inbox_rows: inbox,
                sequence_lag: lag,
                rejections,
            })
        })();
        let s = snapshot::finish(tx, result).await?;
        let mut values = vec![
            ("target_watermark", s.checkpoint),
            ("snapshot_baseline", s.snapshot_baseline),
            ("inbox_rows", s.inbox_rows),
            ("rejections", s.rejections),
        ];
        if let Some(lag) = s.sequence_lag {
            values.push(("sequence_lag", lag));
        }
        emit(context, source, target, "target", &values);
        Ok(s)
    }
    pub async fn committed_dead_letters(
        &self,
        context: &UserContext,
        source: &str,
        target: &str,
        model: &str,
        limit: usize,
    ) -> Result<Vec<DeadLetter>, CommittedEventError> {
        snapshot::provider(context, self)?;
        valid(source, target, model)?;
        if limit == 0 || limit > 1000 {
            return Err(unavailable());
        }
        let tx = self.begin_snapshot_transaction(true)?;
        let result = (|| {
            let db = self.lock()?;
            let mut query=db.prepare("SELECT kind,source_sequence,identity_fingerprint,payload_fingerprint,attempts,dead_reason FROM teaql_event_delivery_journal WHERE source_runtime_id=?1 AND target_id=?2 AND model_hash=?3 AND dead_letter=1 ORDER BY source_sequence,kind LIMIT ?4")?;
            let rows = query.query_map(params![source, target, model, limit as i64], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, String>(5)?,
                ))
            })?;
            rows.map(|row| {
                let (kind, n, id, payload, attempts, reason) = row?;
                Ok(DeadLetter {
                    kind,
                    sequence: number(n)?,
                    identity_fingerprint: id,
                    payload_fingerprint: payload,
                    attempts: number(attempts)?,
                    reason,
                })
            })
            .collect::<Result<Vec<_>, CommittedEventError>>()
        })();
        snapshot::finish(tx, result).await
    }
}
