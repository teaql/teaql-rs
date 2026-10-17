//! Ordered, bounded publication from SQLite's durable Outbox.
//!
//! A transport supplies one acknowledgement per event. It may be a local test
//! adapter or an authenticated remote channel; this module does not claim that
//! the callback itself authenticates a network peer.

use std::future::Future;

use rusqlite::{OptionalExtension, params};

use crate::{
    SqliteMutationExecutor,
    committed_events::{CommittedEventError, CommittedEventTarget, InboxOutcome, OutboxRecord},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryStatus {
    pub acked_sequence: u64,
    pub source_high_watermark: u64,
    pub backlog: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishBatchOutcome {
    pub acknowledged: usize,
    pub status: DeliveryStatus,
}

impl SqliteMutationExecutor {
    /// The cursor is source-owned and durable per target. It advances only
    /// after an acknowledgement for the exact event sequence. A lost response
    /// leaves the cursor behind so restart re-delivers the event; the target
    /// Inbox turns that retry into a stable duplicate acknowledgement.
    pub fn committed_delivery_status(
        &self,
        source_runtime_id: &str,
        target_id: &str,
        model_hash: &str,
    ) -> Result<DeliveryStatus, CommittedEventError> {
        validate_identity(source_runtime_id, target_id, model_hash)?;
        let connection = self.lock()?;
        let cursor: Option<(i64, String)> = connection
            .query_row(
                "SELECT acked_sequence, model_hash FROM teaql_event_delivery_cursor \
                 WHERE source_runtime_id = ?1 AND target_id = ?2",
                params![source_runtime_id, target_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let acked = match cursor {
            Some((sequence, stored_hash)) if stored_hash == model_hash => sequence as u64,
            Some(_) => return Err(CommittedEventError::ModelMismatch),
            None => 0,
        };
        let high_watermark: i64 = connection
            .query_row(
                "SELECT current_sequence FROM teaql_event_sequence WHERE source_runtime_id = ?1",
                params![source_runtime_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        let high_watermark = high_watermark as u64;
        if high_watermark < acked {
            return Err(CommittedEventError::InvalidEnvelope(
                "delivery cursor is ahead of source sequence",
            ));
        }
        Ok(DeliveryStatus {
            acked_sequence: acked,
            source_high_watermark: high_watermark,
            backlog: high_watermark - acked,
        })
    }

    /// Make at most one bounded attempt per event. The caller supplies a
    /// trusted sink; network authentication, retry budgets and backoff belong
    /// to that transport. A failed/ambiguous response never advances the
    /// durable source cursor.
    pub async fn publish_committed_batch<F, Fut>(
        &self,
        source_runtime_id: &str,
        target_id: &str,
        model_hash: &str,
        limit: usize,
        mut deliver: F,
    ) -> Result<PublishBatchOutcome, CommittedEventError>
    where
        F: FnMut(OutboxRecord) -> Fut,
        Fut: Future<Output = Result<InboxOutcome, CommittedEventError>>,
    {
        let mut status =
            self.committed_delivery_status(source_runtime_id, target_id, model_hash)?;
        let records =
            self.committed_outbox_after(source_runtime_id, status.acked_sequence, limit)?;
        if records.is_empty() && status.backlog > 0 {
            return Err(CommittedEventError::InvalidEnvelope(
                "source Outbox has an unacknowledged gap",
            ));
        }
        let mut acknowledged = 0;
        for record in records {
            let expected = status
                .acked_sequence
                .checked_add(1)
                .ok_or(CommittedEventError::SequenceExhausted)?;
            if record.source_sequence != expected {
                return Err(CommittedEventError::SequenceGap {
                    expected,
                    received: record.source_sequence,
                });
            }
            if record.model_hash != model_hash {
                return Err(CommittedEventError::ModelMismatch);
            }
            let sequence = record.source_sequence;
            let outcome = deliver(record).await?;
            let checkpoint = match outcome {
                InboxOutcome::Applied { checkpoint } | InboxOutcome::Duplicate { checkpoint } => {
                    checkpoint
                }
            };
            if checkpoint != sequence {
                return Err(CommittedEventError::InvalidEnvelope(
                    "target acknowledgement does not match delivered sequence",
                ));
            }
            self.acknowledge_committed_delivery(
                source_runtime_id,
                target_id,
                model_hash,
                status.acked_sequence,
                sequence,
            )?;
            status.acked_sequence = sequence;
            acknowledged += 1;
        }
        Ok(PublishBatchOutcome {
            acknowledged,
            status: self.committed_delivery_status(source_runtime_id, target_id, model_hash)?,
        })
    }

    fn acknowledge_committed_delivery(
        &self,
        source_runtime_id: &str,
        target_id: &str,
        model_hash: &str,
        prior: u64,
        sequence: u64,
    ) -> Result<(), CommittedEventError> {
        let prior = i64::try_from(prior).map_err(|_| CommittedEventError::SequenceExhausted)?;
        let sequence =
            i64::try_from(sequence).map_err(|_| CommittedEventError::SequenceExhausted)?;
        let changed = self.lock()?.execute(
            "INSERT INTO teaql_event_delivery_cursor \
             (source_runtime_id, target_id, acked_sequence, model_hash) VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(source_runtime_id, target_id) DO UPDATE SET \
             acked_sequence = excluded.acked_sequence \
             WHERE teaql_event_delivery_cursor.acked_sequence = ?5 \
               AND teaql_event_delivery_cursor.model_hash = excluded.model_hash",
            params![source_runtime_id, target_id, sequence, model_hash, prior],
        )?;
        if changed != 1 {
            return Err(CommittedEventError::CursorConflict);
        }
        Ok(())
    }
}

fn validate_identity(
    source_runtime_id: &str,
    target_id: &str,
    model_hash: &str,
) -> Result<(), CommittedEventError> {
    CommittedEventTarget::new(source_runtime_id, model_hash)?;
    if target_id.trim().is_empty() {
        return Err(CommittedEventError::InvalidEnvelope(
            "delivery target identity is empty",
        ));
    }
    Ok(())
}
