//! SQLite durability primitives for committed mutation sets.
//!
//! This provider boundary only stores validated logical envelopes and enforces
//! source/target transaction rules. It does not infer business mutations from
//! an Entity Mutation Ledger or expose replication through Query/Mutation APIs.

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::{MutationExecutorError, SqliteMutationExecutor, SqliteTransaction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRecord {
    pub source_runtime_id: String,
    pub source_sequence: u64,
    pub change_set_id: String,
    pub model_hash: String,
    pub payload_sha256: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxOutcome {
    Applied { checkpoint: u64 },
    Duplicate { checkpoint: u64 },
}

#[derive(Debug)]
pub enum CommittedEventError {
    Storage(MutationExecutorError),
    Json(serde_json::Error),
    InvalidEnvelope(&'static str),
    ModelMismatch,
    SequenceGap { expected: u64, received: u64 },
    DuplicateConflict,
    SequenceExhausted,
}

impl std::fmt::Display for CommittedEventError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "committed-event storage failed: {error}"),
            Self::Json(_) => write!(formatter, "committed-event JSON is invalid"),
            Self::InvalidEnvelope(reason) => {
                write!(formatter, "invalid committed-event envelope: {reason}")
            }
            Self::ModelMismatch => write!(formatter, "committed-event model hash mismatch"),
            Self::SequenceGap { expected, received } => write!(
                formatter,
                "committed-event sequence gap: expected {expected}, received {received}"
            ),
            Self::DuplicateConflict => write!(
                formatter,
                "committed-event identity has conflicting content"
            ),
            Self::SequenceExhausted => {
                write!(formatter, "committed-event source sequence exhausted")
            }
        }
    }
}

impl std::error::Error for CommittedEventError {}

impl From<MutationExecutorError> for CommittedEventError {
    fn from(value: MutationExecutorError) -> Self {
        Self::Storage(value)
    }
}
impl From<rusqlite::Error> for CommittedEventError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Storage(value.into())
    }
}
impl From<serde_json::Error> for CommittedEventError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl SqliteMutationExecutor {
    /// Install the independent infrastructure tables before enabling the
    /// committed-event source or target. This never seeds domain rows.
    pub fn ensure_committed_event_schema(&self) -> Result<(), CommittedEventError> {
        self.lock()?.execute_batch(
            "CREATE TABLE IF NOT EXISTS teaql_event_sequence (\
                source_runtime_id TEXT PRIMARY KEY, \
                current_sequence INTEGER NOT NULL CHECK(current_sequence >= 0));\
             CREATE TABLE IF NOT EXISTS teaql_event_outbox (\
                source_runtime_id TEXT NOT NULL, \
                source_sequence INTEGER NOT NULL CHECK(source_sequence > 0), \
                change_set_id TEXT NOT NULL, \
                model_hash TEXT NOT NULL, \
                payload_sha256 TEXT NOT NULL, \
                payload_json TEXT NOT NULL, \
                PRIMARY KEY(source_runtime_id, source_sequence), \
                UNIQUE(source_runtime_id, change_set_id));\
             CREATE TABLE IF NOT EXISTS teaql_event_inbox (\
                source_runtime_id TEXT NOT NULL, \
                change_set_id TEXT NOT NULL, \
                source_sequence INTEGER NOT NULL CHECK(source_sequence > 0), \
                payload_sha256 TEXT NOT NULL, \
                PRIMARY KEY(source_runtime_id, change_set_id));\
             CREATE TABLE IF NOT EXISTS teaql_event_checkpoint (\
                source_runtime_id TEXT PRIMARY KEY, \
                source_sequence INTEGER NOT NULL CHECK(source_sequence >= 0), \
                model_hash TEXT NOT NULL);",
        )?;
        Ok(())
    }

    /// A bounded publisher read, ordered by one source's durable sequence.
    pub fn committed_outbox_after(
        &self,
        source_runtime_id: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<OutboxRecord>, CommittedEventError> {
        if source_runtime_id.is_empty() || limit == 0 || limit > 1000 {
            return Err(CommittedEventError::InvalidEnvelope(
                "invalid source or read limit",
            ));
        }
        let after = i64::try_from(after).map_err(|_| CommittedEventError::SequenceExhausted)?;
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT source_runtime_id, source_sequence, change_set_id, model_hash, \
                    payload_sha256, payload_json FROM teaql_event_outbox \
             WHERE source_runtime_id = ?1 AND source_sequence > ?2 \
             ORDER BY source_sequence ASC LIMIT ?3",
        )?;
        let rows = statement.query_map(params![source_runtime_id, after, limit as i64], |row| {
            let sequence: i64 = row.get(1)?;
            Ok(OutboxRecord {
                source_runtime_id: row.get(0)?,
                source_sequence: sequence as u64,
                change_set_id: row.get(2)?,
                model_hash: row.get(3)?,
                payload_sha256: row.get(4)?,
                payload_json: row.get(5)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}

impl SqliteTransaction {
    /// Allocate the next source sequence and append one immutable envelope in
    /// the same already-open transaction as the caller's business changes.
    /// The builder receives the allocated sequence so it can include it in the
    /// logical envelope; a failed builder or insert rolls back this savepoint.
    pub fn append_committed_event<F>(
        &self,
        source_runtime_id: &str,
        change_set_id: &str,
        model_hash: &str,
        build_payload: F,
    ) -> Result<OutboxRecord, CommittedEventError>
    where
        F: FnOnce(u64) -> Result<String, CommittedEventError>,
    {
        if !self.active
            || source_runtime_id.is_empty()
            || change_set_id.is_empty()
            || model_hash.is_empty()
        {
            return Err(CommittedEventError::InvalidEnvelope(
                "inactive transaction or missing identity",
            ));
        }
        let connection = self.executor.lock()?;
        with_savepoint(&connection, "teaql_event_append", || {
            connection.execute(
                "INSERT INTO teaql_event_sequence(source_runtime_id, current_sequence) \
                 VALUES (?1, 0) ON CONFLICT(source_runtime_id) DO NOTHING",
                params![source_runtime_id],
            )?;
            connection.execute(
                "UPDATE teaql_event_sequence SET current_sequence = current_sequence + 1 \
                 WHERE source_runtime_id = ?1 AND current_sequence < 9223372036854775807",
                params![source_runtime_id],
            )?;
            let sequence: i64 = connection.query_row(
                "SELECT current_sequence FROM teaql_event_sequence WHERE source_runtime_id = ?1",
                params![source_runtime_id],
                |row| row.get(0),
            )?;
            if sequence <= 0 || sequence == i64::MAX {
                return Err(CommittedEventError::SequenceExhausted);
            }
            let payload_json = build_payload(sequence as u64)?;
            let payload: JsonValue = serde_json::from_str(&payload_json)?;
            validate_header(
                &payload,
                source_runtime_id,
                sequence as u64,
                change_set_id,
                model_hash,
            )?;
            let payload_sha256 = digest(payload_json.as_bytes());
            connection.execute(
                "INSERT INTO teaql_event_outbox \
                 (source_runtime_id, source_sequence, change_set_id, model_hash, payload_sha256, payload_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![source_runtime_id, sequence, change_set_id, model_hash, payload_sha256, payload_json],
            )?;
            Ok(OutboxRecord {
                source_runtime_id: source_runtime_id.to_owned(),
                source_sequence: sequence as u64,
                change_set_id: change_set_id.to_owned(),
                model_hash: model_hash.to_owned(),
                payload_sha256,
                payload_json,
            })
        })
    }

    /// Restricted target infrastructure apply. The callback must only write
    /// projection rows from validated committed facts; it must not call normal
    /// audited entity save or rerun Checker/Fix/business behavior. The savepoint
    /// prevents a caller from committing partial projection writes on failure.
    pub fn apply_committed_event<F>(
        &self,
        record: &OutboxRecord,
        expected_model_hash: &str,
        materialize: F,
    ) -> Result<InboxOutcome, CommittedEventError>
    where
        F: FnOnce(&Connection, &JsonValue) -> Result<(), CommittedEventError>,
    {
        if !self.active || expected_model_hash.is_empty() {
            return Err(CommittedEventError::InvalidEnvelope(
                "inactive transaction or missing model hash",
            ));
        }
        if record.model_hash != expected_model_hash {
            return Err(CommittedEventError::ModelMismatch);
        }
        let sequence = i64::try_from(record.source_sequence)
            .map_err(|_| CommittedEventError::SequenceExhausted)?;
        let payload: JsonValue = serde_json::from_str(&record.payload_json)?;
        validate_header(
            &payload,
            &record.source_runtime_id,
            record.source_sequence,
            &record.change_set_id,
            &record.model_hash,
        )?;
        if digest(record.payload_json.as_bytes()) != record.payload_sha256 {
            return Err(CommittedEventError::InvalidEnvelope(
                "payload digest mismatch",
            ));
        }
        let connection = self.executor.lock()?;
        let checkpoint: Option<(i64, String)> = connection.query_row(
            "SELECT source_sequence, model_hash FROM teaql_event_checkpoint WHERE source_runtime_id = ?1",
            params![record.source_runtime_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;
        if let Some((_, hash)) = &checkpoint
            && hash != expected_model_hash
        {
            return Err(CommittedEventError::ModelMismatch);
        }
        let last = checkpoint
            .as_ref()
            .map_or(0, |(sequence, _)| *sequence as u64);
        let prior: Option<(i64, String)> = connection
            .query_row(
                "SELECT source_sequence, payload_sha256 FROM teaql_event_inbox \
             WHERE source_runtime_id = ?1 AND change_set_id = ?2",
                params![record.source_runtime_id, record.change_set_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((sequence, digest)) = prior {
            if sequence as u64 == record.source_sequence && digest == record.payload_sha256 {
                return Ok(InboxOutcome::Duplicate { checkpoint: last });
            }
            return Err(CommittedEventError::DuplicateConflict);
        }
        let expected = last
            .checked_add(1)
            .ok_or(CommittedEventError::SequenceExhausted)?;
        if record.source_sequence != expected {
            return Err(CommittedEventError::SequenceGap {
                expected,
                received: record.source_sequence,
            });
        }
        with_savepoint(&connection, "teaql_event_apply", || {
            materialize(&connection, &payload)?;
            connection.execute(
                "INSERT INTO teaql_event_inbox \
                 (source_runtime_id, change_set_id, source_sequence, payload_sha256) VALUES (?1, ?2, ?3, ?4)",
                params![record.source_runtime_id, record.change_set_id, sequence, record.payload_sha256],
            )?;
            connection.execute(
                "INSERT INTO teaql_event_checkpoint(source_runtime_id, source_sequence, model_hash) \
                 VALUES (?1, ?2, ?3) ON CONFLICT(source_runtime_id) DO UPDATE SET \
                 source_sequence = excluded.source_sequence, model_hash = excluded.model_hash",
                params![record.source_runtime_id, sequence, record.model_hash],
            )?;
            Ok(InboxOutcome::Applied {
                checkpoint: record.source_sequence,
            })
        })
    }
}

fn validate_header(
    payload: &JsonValue,
    source: &str,
    sequence: u64,
    change_set_id: &str,
    model_hash: &str,
) -> Result<(), CommittedEventError> {
    let sequence_text = sequence.to_string();
    if payload.get("protocol_version").and_then(JsonValue::as_u64) != Some(1)
        || payload.get("source_runtime_id").and_then(JsonValue::as_str) != Some(source)
        || payload.get("source_sequence").and_then(JsonValue::as_str)
            != Some(sequence_text.as_str())
        || payload.get("change_set_id").and_then(JsonValue::as_str) != Some(change_set_id)
        || payload.get("source_model_hash").and_then(JsonValue::as_str) != Some(model_hash)
        || !payload
            .get("mutations")
            .and_then(JsonValue::as_array)
            .is_some_and(|items| !items.is_empty())
    {
        return Err(CommittedEventError::InvalidEnvelope(
            "required v1 header or mutations mismatch",
        ));
    }
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut hex = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(hex, "{byte:02x}").expect("write to String cannot fail");
    }
    hex
}

fn with_savepoint<T>(
    connection: &Connection,
    name: &str,
    work: impl FnOnce() -> Result<T, CommittedEventError>,
) -> Result<T, CommittedEventError> {
    connection.execute_batch(&format!("SAVEPOINT {name}"))?;
    match work() {
        Ok(value) => match connection.execute_batch(&format!("RELEASE SAVEPOINT {name}")) {
            Ok(()) => Ok(value),
            Err(release_error) => {
                if connection
                    .execute_batch(&format!(
                        "ROLLBACK TO SAVEPOINT {name}; RELEASE SAVEPOINT {name}"
                    ))
                    .is_err()
                {
                    let _ = connection.execute_batch("ROLLBACK");
                }
                Err(release_error.into())
            }
        },
        Err(error) => {
            if let Err(rollback_error) = connection.execute_batch(&format!(
                "ROLLBACK TO SAVEPOINT {name}; RELEASE SAVEPOINT {name}"
            )) {
                let _ = connection.execute_batch("ROLLBACK");
                return Err(rollback_error.into());
            }
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use teaql_sql::{SqlTransaction, SqlTransactionTransport};

    const MODEL_HASH: &str =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    fn payload(source: &str, sequence: u64, change_set_id: &str, status: &str) -> String {
        let mutations = if status == "DRAFT" {
            serde_json::json!([
                {"entity_type": "order", "entity_id": "42", "operation": "create", "base_version": null, "committed_version": "1", "fields": {"status": status}},
                {"entity_type": "order_item", "entity_id": "43", "operation": "create", "base_version": null, "committed_version": "1", "fields": {"order": {"entity_type": "order", "entity_id": "42"}, "quantity": 2}}
            ])
        } else {
            serde_json::json!([
                {"entity_type": "order", "entity_id": "42", "operation": "update", "base_version": "1", "committed_version": "2", "fields": {"status": status}},
                {"entity_type": "order_item", "entity_id": "43", "operation": "delete", "base_version": "1", "committed_version": "-2", "fields": {}}
            ])
        };
        serde_json::json!({
            "protocol_version": 1,
            "source_runtime_id": source,
            "source_sequence": sequence.to_string(),
            "change_set_id": change_set_id,
            "source_model_hash": MODEL_HASH,
            "mutations": mutations
        })
        .to_string()
    }

    #[tokio::test]
    async fn source_rollback_and_restart_keep_outbox_atomic_and_ordered() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("source.sqlite");
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open(&database).unwrap());
        executor.ensure_committed_event_schema().unwrap();
        executor
            .lock()
            .unwrap()
            .execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL)",
                [],
            )
            .unwrap();
        executor.lock().unwrap().execute(
            "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL, quantity INTEGER NOT NULL)",
            [],
        ).unwrap();

        let transaction = executor.begin_sql().await.unwrap();
        transaction
            .executor
            .lock()
            .unwrap()
            .execute("INSERT INTO orders(id, status) VALUES (42, 'DRAFT')", [])
            .unwrap();
        transaction
            .executor
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO order_items(id, order_id, quantity) VALUES (43, 42, 2)",
                [],
            )
            .unwrap();
        transaction
            .append_committed_event("source-a", "change-a", MODEL_HASH, |sequence| {
                Ok(payload("source-a", sequence, "change-a", "DRAFT"))
            })
            .unwrap();
        transaction.rollback_sql().await.unwrap();
        assert_eq!(
            executor
                .lock()
                .unwrap()
                .query_row("SELECT count(*) FROM orders", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            executor
                .lock()
                .unwrap()
                .query_row("SELECT count(*) FROM order_items", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(
            executor
                .committed_outbox_after("source-a", 0, 10)
                .unwrap()
                .is_empty()
        );

        let transaction = executor.begin_sql().await.unwrap();
        transaction
            .executor
            .lock()
            .unwrap()
            .execute("INSERT INTO orders(id, status) VALUES (42, 'DRAFT')", [])
            .unwrap();
        transaction
            .executor
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO order_items(id, order_id, quantity) VALUES (43, 42, 2)",
                [],
            )
            .unwrap();
        let first = transaction
            .append_committed_event("source-a", "change-a", MODEL_HASH, |sequence| {
                Ok(payload("source-a", sequence, "change-a", "DRAFT"))
            })
            .unwrap();
        assert_eq!(
            first.source_sequence, 1,
            "rolled-back sequence must be reused"
        );
        transaction.commit_sql().await.unwrap();

        let transaction = executor.begin_sql().await.unwrap();
        transaction
            .executor
            .lock()
            .unwrap()
            .execute("UPDATE orders SET status = 'SUBMITTED' WHERE id = 42", [])
            .unwrap();
        transaction
            .executor
            .lock()
            .unwrap()
            .execute("DELETE FROM order_items WHERE id = 43", [])
            .unwrap();
        let second = transaction
            .append_committed_event("source-a", "change-b", MODEL_HASH, |sequence| {
                Ok(payload("source-a", sequence, "change-b", "SUBMITTED"))
            })
            .unwrap();
        assert_eq!(second.source_sequence, 2);
        transaction.commit_sql().await.unwrap();
        drop(executor);

        let reopened =
            SqliteMutationExecutor::from_connection(Connection::open(&database).unwrap());
        let retained = reopened.committed_outbox_after("source-a", 0, 10).unwrap();
        assert_eq!(retained, vec![first, second]);
        assert_eq!(
            reopened
                .committed_outbox_after("source-a", 1, 10)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            reopened
                .lock()
                .unwrap()
                .query_row("SELECT status FROM orders WHERE id = 42", [], |row| row
                    .get::<_, String>(
                    0
                ))
                .unwrap(),
            "SUBMITTED"
        );
        assert_eq!(
            reopened
                .lock()
                .unwrap()
                .query_row("SELECT count(*) FROM order_items", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn target_deduplicates_and_rolls_back_failed_projection_and_checkpoint() {
        let source = SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        source.ensure_committed_event_schema().unwrap();
        let transaction = source.begin_sql().await.unwrap();
        let first = transaction
            .append_committed_event("source-a", "change-a", MODEL_HASH, |sequence| {
                Ok(payload("source-a", sequence, "change-a", "DRAFT"))
            })
            .unwrap();
        let second = transaction
            .append_committed_event("source-a", "change-b", MODEL_HASH, |sequence| {
                Ok(payload("source-a", sequence, "change-b", "SUBMITTED"))
            })
            .unwrap();
        transaction.commit_sql().await.unwrap();

        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("target.sqlite");
        let target = SqliteMutationExecutor::from_connection(Connection::open(&database).unwrap());
        target.ensure_committed_event_schema().unwrap();
        target
            .lock()
            .unwrap()
            .execute(
                "CREATE TABLE projection_orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL)",
                [],
            )
            .unwrap();
        target.lock().unwrap().execute(
            "CREATE TABLE projection_items (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL, quantity INTEGER NOT NULL)", [],
        ).unwrap();

        let transaction = target.begin_sql().await.unwrap();
        assert!(matches!(
            transaction.apply_committed_event(&second, MODEL_HASH, |_, _| panic!(
                "gap reached projection"
            )),
            Err(CommittedEventError::SequenceGap {
                expected: 1,
                received: 2
            })
        ));
        transaction.rollback_sql().await.unwrap();

        let transaction = target.begin_sql().await.unwrap();
        assert_eq!(
            transaction
                .apply_committed_event(&first, MODEL_HASH, |connection, _| {
                    connection.execute(
                        "INSERT INTO projection_orders(id, status) VALUES (42, 'DRAFT')",
                        [],
                    )?;
                    connection.execute(
                        "INSERT INTO projection_items(id, order_id, quantity) VALUES (43, 42, 2)",
                        [],
                    )?;
                    Ok(())
                })
                .unwrap(),
            InboxOutcome::Applied { checkpoint: 1 }
        );
        transaction.commit_sql().await.unwrap();
        drop(target);
        let target = SqliteMutationExecutor::from_connection(Connection::open(&database).unwrap());

        let transaction = target.begin_sql().await.unwrap();
        assert_eq!(
            transaction
                .apply_committed_event(&first, MODEL_HASH, |_, _| panic!(
                    "duplicate reached projection"
                ))
                .unwrap(),
            InboxOutcome::Duplicate { checkpoint: 1 }
        );
        transaction.commit_sql().await.unwrap();

        let transaction = target.begin_sql().await.unwrap();
        let failed = transaction.apply_committed_event(&second, MODEL_HASH, |connection, _| {
            connection.execute(
                "UPDATE projection_orders SET status = 'BROKEN' WHERE id = 42",
                [],
            )?;
            connection.execute("DELETE FROM projection_items WHERE id = 43", [])?;
            Err(CommittedEventError::InvalidEnvelope(
                "injected projection failure",
            ))
        });
        assert!(matches!(
            failed,
            Err(CommittedEventError::InvalidEnvelope(_))
        ));
        // Even a caller that mistakenly commits after an error cannot retain
        // the failed projection, Inbox identity or checkpoint.
        transaction.commit_sql().await.unwrap();
        assert_eq!(
            target
                .lock()
                .unwrap()
                .query_row(
                    "SELECT status FROM projection_orders WHERE id = 42",
                    [],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            "DRAFT"
        );
        assert_eq!(
            target
                .lock()
                .unwrap()
                .query_row(
                    "SELECT count(*) FROM projection_items WHERE id = 43",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            target
                .lock()
                .unwrap()
                .query_row(
                    "SELECT source_sequence FROM teaql_event_checkpoint",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            target
                .lock()
                .unwrap()
                .query_row("SELECT count(*) FROM teaql_event_inbox", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );

        let transaction = target.begin_sql().await.unwrap();
        assert_eq!(
            transaction
                .apply_committed_event(&second, MODEL_HASH, |connection, _| {
                    connection.execute(
                        "UPDATE projection_orders SET status = 'SUBMITTED' WHERE id = 42",
                        [],
                    )?;
                    connection.execute("DELETE FROM projection_items WHERE id = 43", [])?;
                    Ok(())
                })
                .unwrap(),
            InboxOutcome::Applied { checkpoint: 2 }
        );
        transaction.commit_sql().await.unwrap();
        assert_eq!(
            target
                .lock()
                .unwrap()
                .query_row("SELECT count(*) FROM projection_items", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );

        let mut altered = second.clone();
        altered.payload_json = payload("source-a", 2, "change-b", "ALTERED");
        altered.payload_sha256 = digest(altered.payload_json.as_bytes());
        let transaction = target.begin_sql().await.unwrap();
        assert!(matches!(
            transaction.apply_committed_event(&altered, MODEL_HASH, |_, _| panic!(
                "conflict reached projection"
            )),
            Err(CommittedEventError::DuplicateConflict)
        ));
        assert!(matches!(
            transaction.apply_committed_event(&second, "sha256:other", |_, _| panic!(
                "model mismatch reached projection"
            )),
            Err(CommittedEventError::ModelMismatch)
        ));
        transaction.rollback_sql().await.unwrap();
    }
}
