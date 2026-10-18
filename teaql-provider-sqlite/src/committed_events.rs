//! SQLite durability primitives for committed mutation sets.
//!
//! This provider boundary only stores validated logical envelopes and enforces
//! source/target transaction rules. It does not infer business mutations from
//! an Entity Mutation Ledger or expose replication through Query/Mutation APIs.

use std::str::FromStr;

use chrono::{DateTime, NaiveDate};
use rusqlite::{Connection, OptionalExtension, params, types::Value as SqliteValue};
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};
use teaql_runtime::UserContext;

use crate::{
    MutationExecutorError, SqliteMutationExecutor, SqliteTransaction, quote_ident,
    strip_identifier_quotes,
};

#[derive(Clone, PartialEq, Eq)]
pub struct OutboxRecord {
    pub source_runtime_id: String,
    pub source_sequence: u64,
    pub change_set_id: String,
    pub model_hash: String,
    pub payload_sha256: String,
    pub payload_json: String,
}

impl std::fmt::Debug for OutboxRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OutboxRecord")
            .field("source_runtime_id", &self.source_runtime_id)
            .field("source_sequence", &self.source_sequence)
            .field("payload", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxOutcome {
    Applied { checkpoint: u64 },
    Duplicate { checkpoint: u64 },
}

/// Startup-owned target authority for one source stream and one accepted
/// semantic model. This is not a wire token or a business-facing request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedEventTarget {
    pub source_runtime_id: String,
    pub model_hash: String,
}

impl CommittedEventTarget {
    pub fn new(
        source_runtime_id: impl Into<String>,
        model_hash: impl Into<String>,
    ) -> Result<Self, CommittedEventError> {
        let source_runtime_id = source_runtime_id.into();
        let model_hash = model_hash.into();
        if source_runtime_id.trim().is_empty()
            || !model_hash.strip_prefix("sha256:").is_some_and(|hex| {
                hex.len() == 64
                    && hex
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            })
        {
            return Err(CommittedEventError::InvalidEnvelope(
                "target source identity or SHA-256 model hash is invalid",
            ));
        }
        Ok(Self {
            source_runtime_id,
            model_hash,
        })
    }
}

#[derive(Debug)]
pub enum CommittedEventError {
    Storage(MutationExecutorError),
    Json(serde_json::Error),
    InvalidEnvelope(&'static str),
    UnauthorizedSource,
    ModelMismatch,
    VersionConflict {
        expected: Option<i64>,
        received: Option<i64>,
    },
    SequenceGap {
        expected: u64,
        received: u64,
    },
    DuplicateConflict,
    SequenceExhausted,
    CursorConflict,
    TransportUnavailable,
}

impl std::fmt::Display for CommittedEventError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "committed-event storage failed: {error}"),
            Self::Json(_) => write!(formatter, "committed-event JSON is invalid"),
            Self::InvalidEnvelope(reason) => {
                write!(formatter, "invalid committed-event envelope: {reason}")
            }
            Self::UnauthorizedSource => {
                write!(formatter, "committed-event source is not authorized")
            }
            Self::ModelMismatch => write!(formatter, "committed-event model hash mismatch"),
            Self::VersionConflict { expected, received } => write!(
                formatter,
                "committed-event version conflict: expected {expected:?}, received {received:?}"
            ),
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
            Self::CursorConflict => write!(
                formatter,
                "committed-event delivery cursor changed concurrently"
            ),
            Self::TransportUnavailable => {
                write!(formatter, "committed-event delivery transport unavailable")
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
             CREATE TABLE IF NOT EXISTS teaql_event_applied_payload (\
                source_runtime_id TEXT NOT NULL, \
                source_sequence INTEGER NOT NULL CHECK(source_sequence > 0), \
                change_set_id TEXT NOT NULL, \
                payload_json TEXT NOT NULL, \
                PRIMARY KEY(source_runtime_id, source_sequence), \
                UNIQUE(source_runtime_id, change_set_id));\
             CREATE TABLE IF NOT EXISTS teaql_event_checkpoint (\
                source_runtime_id TEXT PRIMARY KEY, \
                source_sequence INTEGER NOT NULL CHECK(source_sequence >= 0), \
                model_hash TEXT NOT NULL);\
             CREATE TABLE IF NOT EXISTS teaql_event_delivery_cursor (\
                source_runtime_id TEXT NOT NULL, \
                target_id TEXT NOT NULL, \
                acked_sequence INTEGER NOT NULL CHECK(acked_sequence >= 0), \
                model_hash TEXT NOT NULL, \
                PRIMARY KEY(source_runtime_id, target_id));",
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

    /// Apply a trusted logical set through metadata-checked SQL, atomically
    /// with its Inbox identity and checkpoint. Ordinary Query/Mutation/TFP
    /// requests never route to this infrastructure-only entry point.
    pub async fn apply_model_checked_committed_set(
        &self,
        context: &UserContext,
        record: &OutboxRecord,
    ) -> Result<InboxOutcome, CommittedEventError> {
        use teaql_sql::{SqlTransaction, SqlTransactionTransport};

        let target = context.get_resource::<CommittedEventTarget>().ok_or(
            CommittedEventError::InvalidEnvelope("target authority is not configured"),
        )?;
        let installed = context.get_resource::<SqliteMutationExecutor>().ok_or(
            CommittedEventError::InvalidEnvelope("target SQLite provider is not configured"),
        )?;
        if !std::sync::Arc::ptr_eq(&self.connection, &installed.connection) {
            return Err(CommittedEventError::InvalidEnvelope(
                "target provider does not match Context",
            ));
        }
        if record.source_runtime_id != target.source_runtime_id {
            return Err(CommittedEventError::UnauthorizedSource);
        }
        let transaction = self.begin_sql().await?;
        let result =
            transaction.apply_committed_event(record, &target.model_hash, |connection, payload| {
                materialize_model_checked_set(connection, context, payload)
            });
        match result {
            Ok(outcome) => {
                transaction.commit_sql().await?;
                Ok(outcome)
            }
            Err(error) => {
                transaction.rollback_sql().await?;
                Err(error)
            }
        }
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
        let result = self.append_committed_event_inner(
            source_runtime_id,
            change_set_id,
            model_hash,
            build_payload,
        );
        self.mark_failed_operation(&result);
        result
    }

    fn append_committed_event_inner<F>(
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
    pub(crate) fn apply_committed_event<F>(
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
        if let Some((sequence, prior_digest)) = prior {
            if sequence as u64 == record.source_sequence && prior_digest == record.payload_sha256 {
                let retained: Option<(i64, String)> = connection
                    .query_row(
                        "SELECT source_sequence, payload_json FROM teaql_event_applied_payload \
                         WHERE source_runtime_id = ?1 AND change_set_id = ?2",
                        params![record.source_runtime_id, record.change_set_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                if !retained.is_some_and(|(retained_sequence, payload)| {
                    retained_sequence == sequence && digest(payload.as_bytes()) == prior_digest
                }) {
                    return Err(CommittedEventError::InvalidEnvelope(
                        "prior Inbox identity lacks retained source evidence",
                    ));
                }
                if last < record.source_sequence {
                    return Err(CommittedEventError::InvalidEnvelope(
                        "prior Inbox identity is ahead of target checkpoint",
                    ));
                }
                // A retry must receive the acknowledgement for its original
                // set, even if the stream has since advanced further.
                return Ok(InboxOutcome::Duplicate {
                    checkpoint: record.source_sequence,
                });
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
                "INSERT INTO teaql_event_applied_payload \
                 (source_runtime_id, source_sequence, change_set_id, payload_json) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    record.source_runtime_id,
                    sequence,
                    record.change_set_id,
                    record.payload_json
                ],
            )?;
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum ModelOperation {
    Create,
    Update,
    Delete,
    Recover,
}

struct ModelMutation {
    table: String,
    id_column: String,
    version_column: String,
    id: i64,
    base_version: Option<i64>,
    committed_version: i64,
    operation: ModelOperation,
    fields: Vec<ModelField>,
}

struct ModelField {
    column: String,
    data_type: DataType,
    value: SqliteValue,
}

fn materialize_model_checked_set(
    connection: &Connection,
    context: &UserContext,
    payload: &JsonValue,
) -> Result<(), CommittedEventError> {
    let audit = payload
        .get("audit")
        .and_then(JsonValue::as_object)
        .ok_or(CommittedEventError::InvalidEnvelope("audit is missing"))?;
    for field in ["comment", "purpose"] {
        if !audit
            .get(field)
            .and_then(JsonValue::as_str)
            .is_some_and(|value| !value.trim().is_empty())
        {
            return Err(CommittedEventError::InvalidEnvelope(
                "audit intent is missing",
            ));
        }
    }
    if !payload
        .get("source_transaction_id")
        .and_then(JsonValue::as_str)
        .is_some_and(|value| !value.trim().is_empty())
        || !payload
            .get("committed_at")
            .and_then(JsonValue::as_str)
            .is_some_and(|value| DateTime::parse_from_rfc3339(value).is_ok())
    {
        return Err(CommittedEventError::InvalidEnvelope(
            "source transaction or commit time is invalid",
        ));
    }
    let facts = payload
        .get("mutations")
        .and_then(JsonValue::as_array)
        .ok_or(CommittedEventError::InvalidEnvelope(
            "mutations are missing",
        ))?;
    let mut mutations = Vec::with_capacity(facts.len());
    let mut seen = std::collections::BTreeSet::new();
    for fact in facts {
        let mutation = decode_model_mutation(context, fact)?;
        if !seen.insert((mutation.table.clone(), mutation.id)) {
            return Err(CommittedEventError::InvalidEnvelope(
                "one set mutates the same entity twice",
            ));
        }
        mutations.push(mutation);
    }
    for mutation in &mutations {
        apply_model_mutation(connection, mutation)?;
    }
    Ok(())
}

fn decode_model_mutation(
    context: &UserContext,
    fact: &JsonValue,
) -> Result<ModelMutation, CommittedEventError> {
    let fact = fact
        .as_object()
        .ok_or(CommittedEventError::InvalidEnvelope(
            "mutation must be an object",
        ))?;
    validate_trace_chain(fact.get("trace_chain"))?;
    let entity_type = fact.get("entity_type").and_then(JsonValue::as_str).ok_or(
        CommittedEventError::InvalidEnvelope("entity type is missing"),
    )?;
    let descriptor = context
        .require_entity(entity_type)
        .map_err(|_| CommittedEventError::InvalidEnvelope("unknown entity type"))?;
    let id_property = descriptor
        .id_property()
        .ok_or(CommittedEventError::InvalidEnvelope(
            "entity has no ID property",
        ))?;
    let version_property =
        descriptor
            .version_property()
            .ok_or(CommittedEventError::InvalidEnvelope(
                "entity has no version property",
            ))?;
    let id = parse_positive_i64(fact.get("entity_id"))?;
    let operation = match fact.get("operation").and_then(JsonValue::as_str) {
        Some("create") => ModelOperation::Create,
        Some("update") => ModelOperation::Update,
        Some("delete") => ModelOperation::Delete,
        Some("recover") => ModelOperation::Recover,
        _ => {
            return Err(CommittedEventError::InvalidEnvelope(
                "unknown mutation operation",
            ));
        }
    };
    let base_version = match fact.get("base_version") {
        Some(JsonValue::Null) if operation == ModelOperation::Create => None,
        Some(value) if operation != ModelOperation::Create => Some(parse_i64_text(value)?),
        _ => {
            return Err(CommittedEventError::InvalidEnvelope(
                "base version is invalid",
            ));
        }
    };
    let committed_version = parse_i64_text(fact.get("committed_version").ok_or(
        CommittedEventError::InvalidEnvelope("committed version is missing"),
    )?)?;
    let valid_version = match (operation, base_version) {
        (ModelOperation::Create, None) => committed_version == 1,
        (ModelOperation::Update, Some(base)) if base > 0 => {
            base.checked_add(1) == Some(committed_version)
        }
        (ModelOperation::Delete, Some(base)) if base > 0 => {
            base.checked_add(1).and_then(|next| next.checked_neg()) == Some(committed_version)
        }
        (ModelOperation::Recover, Some(base)) if base < 0 => {
            base.checked_neg()
                .and_then(|positive| positive.checked_add(1))
                == Some(committed_version)
        }
        _ => false,
    };
    if !valid_version {
        return Err(CommittedEventError::InvalidEnvelope(
            "invalid version transition",
        ));
    }
    let supplied = fact.get("fields").and_then(JsonValue::as_object).ok_or(
        CommittedEventError::InvalidEnvelope("mutation fields are missing"),
    )?;
    for field in supplied.keys() {
        let property = descriptor
            .property_by_name(field)
            .ok_or(CommittedEventError::InvalidEnvelope("unknown model field"))?;
        if property.is_id || property.is_version {
            return Err(CommittedEventError::InvalidEnvelope(
                "identity/version must not be fields",
            ));
        }
    }
    if operation == ModelOperation::Delete && !supplied.is_empty() {
        return Err(CommittedEventError::InvalidEnvelope(
            "delete must be a tombstone",
        ));
    }
    let mut fields = Vec::new();
    for property in &descriptor.properties {
        if property.is_id || property.is_version {
            continue;
        }
        match supplied.get(&property.name) {
            Some(value) => fields.push(ModelField {
                column: quote_ident(strip_identifier_quotes(&property.column_name)),
                data_type: property.data_type,
                value: decode_model_value(context, descriptor, property, value)?,
            }),
            None if operation == ModelOperation::Create => {
                return Err(CommittedEventError::InvalidEnvelope(
                    "create omitted a modeled field",
                ));
            }
            None => {}
        }
    }
    Ok(ModelMutation {
        table: quote_ident(&descriptor.table_name),
        id_column: quote_ident(strip_identifier_quotes(&id_property.column_name)),
        version_column: quote_ident(strip_identifier_quotes(&version_property.column_name)),
        id,
        base_version,
        committed_version,
        operation,
        fields,
    })
}

fn validate_trace_chain(value: Option<&JsonValue>) -> Result<(), CommittedEventError> {
    let nodes = value
        .and_then(JsonValue::as_array)
        .ok_or(CommittedEventError::InvalidEnvelope(
            "mutation trace chain is missing",
        ))?;
    for node in nodes {
        let fields = node
            .as_object()
            .ok_or(CommittedEventError::InvalidEnvelope(
                "trace node has wrong shape",
            ))?;
        if fields.len() != 4
            || !matches!(
                fields.get("kind").and_then(JsonValue::as_str),
                Some(
                    "OPERATION"
                        | "REQUEST"
                        | "RELATION"
                        | "ENTITY"
                        | "PROVIDER"
                        | "SQL"
                        | "COMMENT"
                        | "PURPOSE"
                        | "AUDIT_REASON"
                )
            )
            || !fields.get("name").is_some_and(JsonValue::is_string)
            || !fields.get("comment").is_some_and(JsonValue::is_string)
            || !fields.get("entity_id").is_some_and(|id| {
                id.is_null()
                    || id.as_str().is_some_and(|text| {
                        !text.is_empty()
                            && !text.starts_with('0')
                            && text.bytes().all(|byte| byte.is_ascii_digit())
                    })
            })
        {
            return Err(CommittedEventError::InvalidEnvelope(
                "trace node has wrong shape",
            ));
        }
    }
    Ok(())
}

fn parse_i64_text(value: &JsonValue) -> Result<i64, CommittedEventError> {
    value
        .as_str()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(CommittedEventError::InvalidEnvelope(
            "version must be a decimal string",
        ))
}

fn parse_positive_i64(value: Option<&JsonValue>) -> Result<i64, CommittedEventError> {
    value
        .and_then(JsonValue::as_str)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(CommittedEventError::InvalidEnvelope(
            "identity is outside the SQLite ID range",
        ))
}

fn canonical_decimal_text(value: &JsonValue, signed: bool) -> Option<&str> {
    let text = value.as_str()?;
    let digits = if signed && text.starts_with('-') {
        &text[1..]
    } else {
        text
    };
    if digits.is_empty()
        || digits.len() > 1 && digits.starts_with('0')
        || text == "-0"
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    Some(text)
}

fn decode_model_value(
    context: &UserContext,
    descriptor: &EntityDescriptor,
    property: &PropertyDescriptor,
    value: &JsonValue,
) -> Result<SqliteValue, CommittedEventError> {
    if value.is_null() {
        return if property.nullable {
            Ok(SqliteValue::Null)
        } else {
            Err(CommittedEventError::InvalidEnvelope(
                "non-null field is null",
            ))
        };
    }
    if let Some(relation) = descriptor
        .relations
        .iter()
        .find(|relation| !relation.many && relation.local_key == property.name)
    {
        context
            .require_entity(&relation.target_entity)
            .map_err(|_| CommittedEventError::InvalidEnvelope("reference target is unknown"))?;
        let reference = value
            .as_object()
            .ok_or(CommittedEventError::InvalidEnvelope(
                "reference must be typed",
            ))?;
        if reference.len() != 2
            || reference.get("entity_type").and_then(JsonValue::as_str)
                != Some(relation.target_entity.as_str())
        {
            return Err(CommittedEventError::InvalidEnvelope(
                "reference target does not match model",
            ));
        }
        return Ok(SqliteValue::Integer(parse_positive_i64(
            reference.get("entity_id"),
        )?));
    }
    match property.data_type {
        DataType::Bool => value
            .as_bool()
            .map(|value| SqliteValue::Integer(i64::from(value)))
            .ok_or(CommittedEventError::InvalidEnvelope(
                "Boolean field has wrong type",
            )),
        DataType::I64 => canonical_decimal_text(value, true)
            .and_then(|value| value.parse::<i64>().ok())
            .map(SqliteValue::Integer)
            .ok_or(CommittedEventError::InvalidEnvelope(
                "i64 field has wrong type",
            )),
        DataType::U64 => canonical_decimal_text(value, false)
            .and_then(|value| value.parse::<u64>().ok())
            .and_then(|value| i64::try_from(value).ok())
            .map(SqliteValue::Integer)
            .ok_or(CommittedEventError::InvalidEnvelope(
                "u64 field exceeds SQLite range",
            )),
        DataType::F64 => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(SqliteValue::Real)
            .ok_or(CommittedEventError::InvalidEnvelope(
                "f64 field has wrong type",
            )),
        DataType::Decimal => {
            let decimal = value
                .as_str()
                .and_then(|value| Decimal::from_str(value).ok())
                .ok_or(CommittedEventError::InvalidEnvelope(
                    "Decimal field has wrong type",
                ))?;
            if !decimal_fits_model(decimal, property) {
                return Err(CommittedEventError::InvalidEnvelope(
                    "Decimal lacks or exceeds modeled precision or scale",
                ));
            }
            Ok(SqliteValue::Text(decimal.to_string()))
        }
        DataType::Text | DataType::LargeText => {
            let text = value.as_str().ok_or(CommittedEventError::InvalidEnvelope(
                "text field has wrong type",
            ))?;
            if property
                .max_length
                .is_some_and(|max| text.chars().count() > max as usize)
            {
                return Err(CommittedEventError::InvalidEnvelope(
                    "text exceeds modeled length",
                ));
            }
            Ok(SqliteValue::Text(text.to_owned()))
        }
        DataType::Json => serde_json::to_string(value)
            .map(SqliteValue::Text)
            .map_err(|_| CommittedEventError::InvalidEnvelope("JSON field cannot be encoded")),
        DataType::Date => value
            .as_str()
            .filter(|value| {
                NaiveDate::parse_from_str(value, "%Y-%m-%d")
                    .is_ok_and(|date| date.to_string() == *value)
            })
            .map(|value| SqliteValue::Text(value.to_owned()))
            .ok_or(CommittedEventError::InvalidEnvelope(
                "Date field has wrong type",
            )),
        DataType::Timestamp => canonical_decimal_text(value, true)
            .and_then(|value| value.parse::<i64>().ok())
            .map(SqliteValue::Integer)
            .ok_or(CommittedEventError::InvalidEnvelope(
                "Timestamp field has wrong type",
            )),
    }
}

fn decimal_fits_model(value: Decimal, property: &PropertyDescriptor) -> bool {
    let (Some(precision), Some(scale)) = (property.numeric_precision, property.numeric_scale)
    else {
        return false;
    };
    if precision == 0 || scale > precision {
        return false;
    }
    let normalized = value.normalize().to_string();
    let unsigned = normalized.strip_prefix('-').unwrap_or(&normalized);
    let (integer, fractional) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let integer_digits = integer.trim_start_matches('0').len();
    integer_digits <= (precision - scale) as usize && fractional.len() <= scale as usize
}

fn apply_model_mutation(
    connection: &Connection,
    mutation: &ModelMutation,
) -> Result<(), CommittedEventError> {
    let lookup = format!(
        "SELECT {} FROM {} WHERE {} = ?1",
        mutation.version_column, mutation.table, mutation.id_column
    );
    let current: Option<i64> = connection
        .query_row(&lookup, params![mutation.id], |row| row.get(0))
        .optional()?;
    if current != mutation.base_version {
        return Err(CommittedEventError::VersionConflict {
            expected: mutation.base_version,
            received: current,
        });
    }
    match mutation.operation {
        ModelOperation::Create => {
            let mut columns = vec![mutation.id_column.clone(), mutation.version_column.clone()];
            let mut values = vec![
                SqliteValue::Integer(mutation.id),
                SqliteValue::Integer(mutation.committed_version),
            ];
            for field in &mutation.fields {
                columns.push(field.column.clone());
                values.push(field.value.clone());
            }
            let placeholders = (1..=values.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                mutation.table,
                columns.join(", "),
                placeholders
            );
            connection.execute(&sql, rusqlite::params_from_iter(values))?;
        }
        ModelOperation::Update | ModelOperation::Delete | ModelOperation::Recover => {
            let mut assignments = vec![format!("{} = ?1", mutation.version_column)];
            let mut values = vec![SqliteValue::Integer(mutation.committed_version)];
            for field in &mutation.fields {
                assignments.push(format!("{} = ?{}", field.column, values.len() + 1));
                values.push(field.value.clone());
            }
            let id_index = values.len() + 1;
            values.push(SqliteValue::Integer(mutation.id));
            let base_index = values.len() + 1;
            values.push(SqliteValue::Integer(
                mutation.base_version.expect("validated version"),
            ));
            let sql = format!(
                "UPDATE {} SET {} WHERE {} = ?{} AND {} = ?{}",
                mutation.table,
                assignments.join(", "),
                mutation.id_column,
                id_index,
                mutation.version_column,
                base_index
            );
            let changed = connection.execute(&sql, rusqlite::params_from_iter(values))?;
            if changed != 1 {
                return Err(CommittedEventError::VersionConflict {
                    expected: mutation.base_version,
                    received: None,
                });
            }
        }
    }
    verify_committed_row(connection, mutation)
}

fn verify_committed_row(
    connection: &Connection,
    mutation: &ModelMutation,
) -> Result<(), CommittedEventError> {
    let columns = std::iter::once(mutation.version_column.as_str())
        .chain(mutation.fields.iter().map(|field| field.column.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = ?1",
        columns, mutation.table, mutation.id_column
    );
    let row = connection
        .query_row(&sql, params![mutation.id], |row| {
            let version: i64 = row.get(0)?;
            let values = (0..mutation.fields.len())
                .map(|index| row.get::<_, SqliteValue>(index + 1))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((version, values))
        })
        .optional()?
        .ok_or(CommittedEventError::InvalidEnvelope(
            "target row disappeared after application",
        ))?;
    if row.0 != mutation.committed_version {
        return Err(CommittedEventError::InvalidEnvelope(
            "target did not retain committed version",
        ));
    }
    for (field, actual) in mutation.fields.iter().zip(row.1.iter()) {
        if !same_committed_value(field, actual) {
            return Err(CommittedEventError::InvalidEnvelope(
                "target did not retain committed field value",
            ));
        }
    }
    Ok(())
}

fn same_committed_value(field: &ModelField, actual: &SqliteValue) -> bool {
    if field.data_type == DataType::Decimal {
        if field.value == SqliteValue::Null {
            return *actual == SqliteValue::Null;
        }
        let as_decimal = |value: &SqliteValue| match value {
            SqliteValue::Text(value) => Decimal::from_str(value).ok(),
            SqliteValue::Integer(value) => Some(Decimal::from(*value)),
            // A REAL readback is approximate even when its shortest decimal
            // rendering matches the source text (for example, 0.1).
            SqliteValue::Real(_) => None,
            _ => None,
        };
        return match (as_decimal(&field.value), as_decimal(actual)) {
            (Some(expected), Some(actual)) => expected == actual,
            _ => false,
        };
    }
    field.value == *actual
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
    use teaql_core::{EntityDescriptor, PropertyDescriptor, RelationDescriptor};
    use teaql_data_service::CommittedSetAppend;
    use teaql_runtime::InMemoryMetadataStore;

    const MODEL_HASH: &str =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    async fn model_target() -> (SqliteMutationExecutor, UserContext) {
        use crate::SqliteProviderExt;

        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let order = EntityDescriptor::new("order")
            .table_name("replica_order")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("order_number", DataType::Text).not_null())
            .property(PropertyDescriptor::new("status", DataType::Text).not_null())
            .property(PropertyDescriptor::new("planned_date", DataType::Date))
            .property(PropertyDescriptor::new("occurred_at", DataType::Timestamp).not_null())
            .property(
                PropertyDescriptor::new("amount", DataType::Decimal)
                    .numeric_precision(19)
                    .numeric_scale(7),
            )
            .property(PropertyDescriptor::new("active", DataType::Bool).not_null())
            .property(PropertyDescriptor::new("ratio", DataType::F64).not_null())
            .property(PropertyDescriptor::new("optional_note", DataType::Text))
            .property(PropertyDescriptor::new("optional_label", DataType::Text));
        let item = EntityDescriptor::new("order_item")
            .table_name("replica_order_item")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("order", DataType::U64)
                    .column_name("order_id")
                    .not_null(),
            )
            .property(PropertyDescriptor::new("quantity", DataType::I64).not_null())
            .relation(RelationDescriptor::new("order_ref", "order").local_key("order"));
        let metadata = InMemoryMetadataStore::new()
            .with_entity(order)
            .with_entity(item);
        let mut context = UserContext::new().with_metadata(metadata);
        context
            .insert_resource(CommittedEventTarget::new("sqlite-order-source", MODEL_HASH).unwrap());
        context.use_sqlite_provider(executor.clone());
        context.ensure_schema().await.unwrap();
        (executor, context)
    }

    fn order_set(sequence: u64) -> OutboxRecord {
        let (change_set_id, mutations) = match sequence {
            1 => (
                "order-graph-0001",
                serde_json::json!([
                    {
                        "entity_type": "order",
                        "entity_id": "9007199254740993",
                        "operation": "create",
                        "base_version": null,
                        "committed_version": "1",
                        "fields": {
                            "order_number": "ORD-0001",
                            "status": "DRAFT",
                            "planned_date": "2026-10-16",
                            "occurred_at": "-315619199877",
                            "amount": "0",
                            "active": true,
                            "ratio": 0.5,
                            "optional_note": null,
                            "optional_label": "initial label"
                        }
                    },
                    {
                        "entity_type": "order_item",
                        "entity_id": "9007199254740994",
                        "operation": "create",
                        "base_version": null,
                        "committed_version": "1",
                        "fields": {
                            "order": { "entity_type": "order", "entity_id": "9007199254740993" },
                            "quantity": "2"
                        }
                    }
                ]),
            ),
            2 => (
                "order-graph-0002",
                serde_json::json!([
                    {
                        "entity_type": "order",
                        "entity_id": "9007199254740993",
                        "operation": "update",
                        "base_version": "1",
                        "committed_version": "2",
                        "fields": {
                            "status": "SUBMITTED",
                            "optional_note": "submitted note",
                            "optional_label": null
                        }
                    },
                    {
                        "entity_type": "order_item",
                        "entity_id": "9007199254740994",
                        "operation": "delete",
                        "base_version": "1",
                        "committed_version": "-2",
                        "fields": {}
                    }
                ]),
            ),
            _ => unreachable!(),
        };
        make_record(sequence, change_set_id, mutations)
    }

    fn make_record(sequence: u64, change_set_id: &str, mut mutations: JsonValue) -> OutboxRecord {
        for fact in mutations
            .as_array_mut()
            .expect("test mutations must be an array")
        {
            fact["trace_chain"] = serde_json::json!([]);
        }
        let payload_json = serde_json::json!({
            "protocol_version": 1,
            "change_set_id": change_set_id,
            "source_runtime_id": "sqlite-order-source",
            "source_sequence": sequence.to_string(),
            "source_model_hash": MODEL_HASH,
            "source_transaction_id": format!("source-tx-{sequence}"),
            "committed_at": "2026-10-16T00:00:00Z",
            "audit": {
                "actor_ref": "fixture:operator",
                "comment": "Apply order graph",
                "purpose": "Verify model-checked target application",
                "trace_id": "fixture-trace"
            },
            "mutations": mutations,
        })
        .to_string();
        OutboxRecord {
            source_runtime_id: "sqlite-order-source".into(),
            source_sequence: sequence,
            change_set_id: change_set_id.into(),
            model_hash: MODEL_HASH.into(),
            payload_sha256: digest(payload_json.as_bytes()),
            payload_json,
        }
    }

    #[test]
    fn model_checked_target_applies_shared_fixture_when_configured() {
        let Some(path) = std::env::var_os("TEAQL_COMMITTED_EVENT_FIXTURE") else {
            eprintln!("shared fixture test skipped: TEAQL_COMMITTED_EVENT_FIXTURE is unset");
            return;
        };
        futures_executor::block_on(async {
            let fixture: JsonValue =
                serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
            assert_eq!(fixture["fixture_version"], 1);
            let sets = fixture["sets"].as_array().unwrap();
            assert_eq!(sets.len(), 2);
            let (executor, context) = model_target().await;
            for set in sets {
                let sequence = set["source_sequence"].as_str().unwrap().parse().unwrap();
                let payload_json = set.to_string();
                let record = OutboxRecord {
                    source_runtime_id: set["source_runtime_id"].as_str().unwrap().into(),
                    source_sequence: sequence,
                    change_set_id: set["change_set_id"].as_str().unwrap().into(),
                    model_hash: set["source_model_hash"].as_str().unwrap().into(),
                    payload_sha256: digest(payload_json.as_bytes()),
                    payload_json,
                };
                assert_eq!(
                    executor
                        .apply_model_checked_committed_set(&context, &record)
                        .await
                        .unwrap(),
                    InboxOutcome::Applied {
                        checkpoint: sequence
                    }
                );
            }
            let connection = executor.lock().unwrap();
            let order: (i64, String) = connection
                .query_row(
                    "SELECT version, status FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(order, (2, "SUBMITTED".into()));
            let planned_date: String = connection
                .query_row(
                    "SELECT planned_date FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(planned_date, "2026-10-16");
            let occurred_at: (String, i64) = connection
                .query_row(
                    "SELECT typeof(occurred_at), occurred_at FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(occurred_at, ("integer".into(), -315_619_199_877));
            let scalars: (i64, f64, Option<String>, Option<String>) = connection
                .query_row(
                    "SELECT active, ratio, optional_note, optional_label FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();
            assert_eq!(scalars, (1, 0.5, Some("submitted note".into()), None));
            let item_version: i64 = connection
                .query_row(
                    "SELECT version FROM replica_order_item WHERE id = ?1",
                    params![9_007_199_254_740_994_i64],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(item_version, -2);
        });
    }

    #[test]
    fn model_checked_target_applies_order_graph_and_rejects_gap_and_duplicate() {
        futures_executor::block_on(async {
            let (executor, context) = model_target().await;
            let first = order_set(1);
            let second = order_set(2);
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &second)
                    .await,
                Err(CommittedEventError::SequenceGap {
                    expected: 1,
                    received: 2
                })
            ));
            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, &first)
                    .await
                    .unwrap(),
                InboxOutcome::Applied { checkpoint: 1 }
            );
            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, &first)
                    .await
                    .unwrap(),
                InboxOutcome::Duplicate { checkpoint: 1 }
            );
            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, &second)
                    .await
                    .unwrap(),
                InboxOutcome::Applied { checkpoint: 2 }
            );
            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, &first)
                    .await
                    .unwrap(),
                InboxOutcome::Duplicate { checkpoint: 1 }
            );
            let connection = executor.lock().unwrap();
            let order: (i64, String, String) = connection
                .query_row(
                    "SELECT version, order_number, status FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(order, (2, "ORD-0001".into(), "SUBMITTED".into()));
            let item: (i64, i64, i64) = connection
                .query_row(
                    "SELECT version, order_id, quantity FROM replica_order_item WHERE id = ?1",
                    params![9_007_199_254_740_994_i64],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(item, (-2, 9_007_199_254_740_993_i64, 2));
            let retained: String = connection
                .query_row(
                    "SELECT payload_json FROM teaql_event_applied_payload \
                 WHERE source_runtime_id = ?1 AND source_sequence = 2",
                    params!["sqlite-order-source"],
                    |row| row.get(0),
                )
                .unwrap();
            let retained: JsonValue = serde_json::from_str(&retained).unwrap();
            assert_eq!(retained["audit"]["comment"], "Apply order graph");
            assert_eq!(retained["audit"]["trace_id"], "fixture-trace");
        });
    }

    #[test]
    fn duplicate_requires_checkpoint_covering_retained_inbox() {
        futures_executor::block_on(async {
            let (executor, context) = model_target().await;
            let first = order_set(1);
            let second = order_set(2);
            executor
                .apply_model_checked_committed_set(&context, &first)
                .await
                .unwrap();
            executor
                .apply_model_checked_committed_set(&context, &second)
                .await
                .unwrap();
            {
                let connection = executor.lock().unwrap();
                connection
                    .execute(
                        "UPDATE teaql_event_checkpoint SET source_sequence = 1 \
                         WHERE source_runtime_id = ?1",
                        params!["sqlite-order-source"],
                    )
                    .unwrap();
            }
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &second)
                    .await,
                Err(CommittedEventError::InvalidEnvelope(_))
            ));
            {
                let connection = executor.lock().unwrap();
                connection
                    .execute(
                        "DELETE FROM teaql_event_checkpoint WHERE source_runtime_id = ?1",
                        params!["sqlite-order-source"],
                    )
                    .unwrap();
            }
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &first)
                    .await,
                Err(CommittedEventError::InvalidEnvelope(_))
            ));
        });
    }

    #[test]
    fn model_checked_target_rejects_unknown_fields_wrong_source_and_partial_graph() {
        futures_executor::block_on(async {
            let (executor, context) = model_target().await;
            let mut unauthorized = order_set(1);
            unauthorized.source_runtime_id = "other-source".into();
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &unauthorized)
                    .await,
                Err(CommittedEventError::UnauthorizedSource)
            ));
            let mut unknown = order_set(1);
            let mut body: JsonValue = serde_json::from_str(&unknown.payload_json).unwrap();
            body["mutations"][1]["fields"]["not_in_model"] = serde_json::json!(1);
            unknown.payload_json = body.to_string();
            unknown.payload_sha256 = digest(unknown.payload_json.as_bytes());
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &unknown)
                    .await,
                Err(CommittedEventError::InvalidEnvelope("unknown model field"))
            ));
            let mut unknown_entity = order_set(1);
            let mut body: JsonValue = serde_json::from_str(&unknown_entity.payload_json).unwrap();
            body["mutations"][1]["entity_type"] = serde_json::json!("not_in_model");
            unknown_entity.payload_json = body.to_string();
            unknown_entity.payload_sha256 = digest(unknown_entity.payload_json.as_bytes());
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &unknown_entity)
                    .await,
                Err(CommittedEventError::InvalidEnvelope("unknown entity type"))
            ));
            let mut invalid_trace = order_set(1);
            let mut body: JsonValue = serde_json::from_str(&invalid_trace.payload_json).unwrap();
            body["mutations"][0]["trace_chain"] = serde_json::json!([{
                "kind": "AuditReason",
                "entity_type": "order",
                "entity_id": "9007199254740993",
                "comment": "Create order"
            }]);
            invalid_trace.payload_json = body.to_string();
            invalid_trace.payload_sha256 = digest(invalid_trace.payload_json.as_bytes());
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &invalid_trace)
                    .await,
                Err(CommittedEventError::InvalidEnvelope(
                    "trace node has wrong shape"
                ))
            ));
            let mut missing_trace = order_set(1);
            let mut body: JsonValue = serde_json::from_str(&missing_trace.payload_json).unwrap();
            body["mutations"][0]
                .as_object_mut()
                .unwrap()
                .remove("trace_chain");
            missing_trace.payload_json = body.to_string();
            missing_trace.payload_sha256 = digest(missing_trace.payload_json.as_bytes());
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &missing_trace)
                    .await,
                Err(CommittedEventError::InvalidEnvelope(
                    "mutation trace chain is missing"
                ))
            ));
            let mut unsupported = order_set(1);
            let mut body: JsonValue = serde_json::from_str(&unsupported.payload_json).unwrap();
            body["protocol_version"] = serde_json::json!(2);
            unsupported.payload_json = body.to_string();
            unsupported.payload_sha256 = digest(unsupported.payload_json.as_bytes());
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &unsupported)
                    .await,
                Err(CommittedEventError::InvalidEnvelope(
                    "required v1 header or mutations mismatch"
                ))
            ));
            let connection = executor.lock().unwrap();
            let count: i64 = connection
                .query_row("SELECT COUNT(*) FROM replica_order", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 0);
            let checkpoint: i64 = connection
                .query_row("SELECT COUNT(*) FROM teaql_event_checkpoint", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(checkpoint, 0);
        });
    }

    #[test]
    fn model_checked_target_preserves_large_i64_without_json_number_rounding() {
        futures_executor::block_on(async {
            let (executor, context) = model_target().await;
            let mut numeric = order_set(1);
            let mut body: JsonValue = serde_json::from_str(&numeric.payload_json).unwrap();
            body["mutations"][1]["fields"]["quantity"] =
                serde_json::json!(9_007_199_254_740_993_i64);
            numeric.payload_json = body.to_string();
            numeric.payload_sha256 = digest(numeric.payload_json.as_bytes());
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &numeric)
                    .await,
                Err(CommittedEventError::InvalidEnvelope(
                    "i64 field has wrong type"
                ))
            ));
            for invalid in ["02", "-0", "9223372036854775808"] {
                let mut malformed = order_set(1);
                let mut body: JsonValue = serde_json::from_str(&malformed.payload_json).unwrap();
                body["mutations"][1]["fields"]["quantity"] = serde_json::json!(invalid);
                malformed.payload_json = body.to_string();
                malformed.payload_sha256 = digest(malformed.payload_json.as_bytes());
                assert!(matches!(
                    executor
                        .apply_model_checked_committed_set(&context, &malformed)
                        .await,
                    Err(CommittedEventError::InvalidEnvelope(
                        "i64 field has wrong type"
                    ))
                ));
            }

            let mut textual = order_set(1);
            let mut body: JsonValue = serde_json::from_str(&textual.payload_json).unwrap();
            body["mutations"][1]["fields"]["quantity"] = serde_json::json!("9007199254740993");
            textual.payload_json = body.to_string();
            textual.payload_sha256 = digest(textual.payload_json.as_bytes());
            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, &textual)
                    .await
                    .unwrap(),
                InboxOutcome::Applied { checkpoint: 1 }
            );
            let stored: i64 = executor
                .lock()
                .unwrap()
                .query_row(
                    "SELECT quantity FROM replica_order_item WHERE id = ?1",
                    params![9_007_199_254_740_994_i64],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(stored, 9_007_199_254_740_993_i64);
        });
    }

    #[test]
    fn model_checked_target_accepts_only_canonical_calendar_date() {
        futures_executor::block_on(async {
            let (executor, context) = model_target().await;
            for invalid in [
                serde_json::json!("2026-2-3"),
                serde_json::json!("2026-02-30"),
                serde_json::json!("2026-10-16T00:00:00Z"),
                serde_json::json!(20261016),
            ] {
                let mut malformed = order_set(1);
                let mut body: JsonValue = serde_json::from_str(&malformed.payload_json).unwrap();
                body["mutations"][0]["fields"]["planned_date"] = invalid;
                malformed.payload_json = body.to_string();
                malformed.payload_sha256 = digest(malformed.payload_json.as_bytes());
                assert!(matches!(
                    executor
                        .apply_model_checked_committed_set(&context, &malformed)
                        .await,
                    Err(CommittedEventError::InvalidEnvelope(
                        "Date field has wrong type"
                    ))
                ));
            }
            let count: i64 = executor
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM replica_order", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 0);

            let mut valid = order_set(1);
            let mut body: JsonValue = serde_json::from_str(&valid.payload_json).unwrap();
            body["mutations"][0]["fields"]["planned_date"] = serde_json::json!("2026-10-16");
            valid.payload_json = body.to_string();
            valid.payload_sha256 = digest(valid.payload_json.as_bytes());
            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, &valid)
                    .await
                    .unwrap(),
                InboxOutcome::Applied { checkpoint: 1 }
            );
            let date: String = executor
                .lock()
                .unwrap()
                .query_row(
                    "SELECT planned_date FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(date, "2026-10-16");
        });
    }

    #[test]
    fn model_checked_target_accepts_only_lossless_instant_millisecond_text() {
        futures_executor::block_on(async {
            let (executor, context) = model_target().await;
            for invalid in [
                serde_json::json!(-315619199877_i64),
                serde_json::json!("1960-01-01T00:00:00.123Z"),
                serde_json::json!("1960-01-01T00:00:00.123"),
                serde_json::json!("-0315619199877"),
                serde_json::json!("-0"),
                serde_json::json!("9223372036854775808"),
            ] {
                let mut malformed = order_set(1);
                let mut body: JsonValue = serde_json::from_str(&malformed.payload_json).unwrap();
                body["mutations"][0]["fields"]["occurred_at"] = invalid;
                malformed.payload_json = body.to_string();
                malformed.payload_sha256 = digest(malformed.payload_json.as_bytes());
                assert!(matches!(
                    executor
                        .apply_model_checked_committed_set(&context, &malformed)
                        .await,
                    Err(CommittedEventError::InvalidEnvelope(
                        "Timestamp field has wrong type"
                    ))
                ));
            }
            assert_eq!(
                executor
                    .lock()
                    .unwrap()
                    .query_row("SELECT count(*) FROM replica_order", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                0
            );
            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, &order_set(1))
                    .await
                    .unwrap(),
                InboxOutcome::Applied { checkpoint: 1 }
            );
            let stored: (String, i64) = executor
                .lock()
                .unwrap()
                .query_row(
                    "SELECT typeof(occurred_at), occurred_at FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(stored, ("integer".into(), -315_619_199_877));
        });
    }

    #[test]
    fn modeled_decimal_bounds_accept_equivalent_trailing_zeros() {
        let property = PropertyDescriptor::new("amount", DataType::Decimal)
            .numeric_precision(19)
            .numeric_scale(7);
        for value in ["0", "0.100000000", "123456789012.1234567", "-0.0000001"] {
            assert!(decimal_fits_model(
                Decimal::from_str(value).unwrap(),
                &property
            ));
        }
        for value in ["1234567890123", "0.12345678", "-1234567890123.01"] {
            assert!(!decimal_fits_model(
                Decimal::from_str(value).unwrap(),
                &property
            ));
        }
        for property in [
            PropertyDescriptor::new("amount", DataType::Decimal),
            PropertyDescriptor::new("amount", DataType::Decimal).numeric_precision(19),
            PropertyDescriptor::new("amount", DataType::Decimal).numeric_scale(7),
        ] {
            assert!(!decimal_fits_model(Decimal::ZERO, &property));
        }
    }

    #[test]
    fn model_checked_target_rejects_sqlite_real_coercion_of_exact_decimal() {
        futures_executor::block_on(async {
            let (executor, context) = model_target().await;
            for (decimal, reason) in [
                ("0.1", "target did not retain committed field value"),
                (
                    "123456789012345.6789",
                    "Decimal lacks or exceeds modeled precision or scale",
                ),
                (
                    "0.12345678",
                    "Decimal lacks or exceeds modeled precision or scale",
                ),
            ] {
                let mut inexact = order_set(1);
                let mut body: JsonValue = serde_json::from_str(&inexact.payload_json).unwrap();
                body["mutations"][0]["fields"]["amount"] = serde_json::json!(decimal);
                inexact.payload_json = body.to_string();
                inexact.payload_sha256 = digest(inexact.payload_json.as_bytes());
                assert!(matches!(
                    executor
                        .apply_model_checked_committed_set(&context, &inexact)
                        .await,
                    Err(CommittedEventError::InvalidEnvelope(actual)) if actual == reason
                ));
                let connection = executor.lock().unwrap();
                let row_count: i64 = connection
                    .query_row("SELECT COUNT(*) FROM replica_order", [], |row| row.get(0))
                    .unwrap();
                let inbox_count: i64 = connection
                    .query_row("SELECT COUNT(*) FROM teaql_event_inbox", [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                let checkpoint_count: i64 = connection
                    .query_row("SELECT COUNT(*) FROM teaql_event_checkpoint", [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                let retained_count: i64 = connection
                    .query_row(
                        "SELECT COUNT(*) FROM teaql_event_applied_payload",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(
                    (row_count, inbox_count, checkpoint_count, retained_count),
                    (0, 0, 0, 0)
                );
            }

            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, &order_set(1))
                    .await
                    .unwrap(),
                InboxOutcome::Applied { checkpoint: 1 }
            );
            let storage: (String, i64) = executor
                .lock()
                .unwrap()
                .query_row(
                    "SELECT typeof(amount), amount FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(storage, ("integer".into(), 0));
        });
    }

    #[test]
    fn model_checked_target_preserves_bool_float_and_explicit_null() {
        futures_executor::block_on(async {
            let (executor, context) = model_target().await;
            for (field, wrong, expected) in [
                (
                    "active",
                    serde_json::json!("true"),
                    "Boolean field has wrong type",
                ),
                (
                    "ratio",
                    serde_json::json!("0.5"),
                    "f64 field has wrong type",
                ),
                ("active", JsonValue::Null, "non-null field is null"),
            ] {
                let mut malformed = order_set(1);
                let mut body: JsonValue = serde_json::from_str(&malformed.payload_json).unwrap();
                body["mutations"][0]["fields"][field] = wrong;
                malformed.payload_json = body.to_string();
                malformed.payload_sha256 = digest(malformed.payload_json.as_bytes());
                match executor
                    .apply_model_checked_committed_set(&context, &malformed)
                    .await
                {
                    Err(CommittedEventError::InvalidEnvelope(message)) => {
                        assert_eq!(message, expected)
                    }
                    other => panic!("expected {expected}, got {other:?}"),
                }
            }
            let mut omitted = order_set(1);
            let mut body: JsonValue = serde_json::from_str(&omitted.payload_json).unwrap();
            body["mutations"][0]["fields"]
                .as_object_mut()
                .unwrap()
                .remove("optional_note");
            omitted.payload_json = body.to_string();
            omitted.payload_sha256 = digest(omitted.payload_json.as_bytes());
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &omitted)
                    .await,
                Err(CommittedEventError::InvalidEnvelope(
                    "create omitted a modeled field"
                ))
            ));
            let rows: i64 = executor
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM replica_order", [], |row| row.get(0))
                .unwrap();
            assert_eq!(rows, 0);

            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, &order_set(1))
                    .await
                    .unwrap(),
                InboxOutcome::Applied { checkpoint: 1 }
            );
            let stored: (i64, f64, Option<String>) = executor
                .lock()
                .unwrap()
                .query_row(
                    "SELECT active, ratio, optional_note FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(stored, (1, 0.5, None));
        });
    }

    #[test]
    fn model_checked_target_applies_nullable_transitions_without_clearing_omitted_fields() {
        futures_executor::block_on(async {
            let (executor, context) = model_target().await;
            executor
                .apply_model_checked_committed_set(&context, &order_set(1))
                .await
                .unwrap();
            for (field, value, reason) in [
                ("active", JsonValue::Null, "non-null field is null"),
                (
                    "optional_note",
                    serde_json::json!(123),
                    "text field has wrong type",
                ),
            ] {
                let mut malformed = order_set(2);
                let mut body: JsonValue = serde_json::from_str(&malformed.payload_json).unwrap();
                body["mutations"][0]["fields"][field] = value;
                malformed.payload_json = body.to_string();
                malformed.payload_sha256 = digest(malformed.payload_json.as_bytes());
                assert!(matches!(
                    executor
                        .apply_model_checked_committed_set(&context, &malformed)
                        .await,
                    Err(CommittedEventError::InvalidEnvelope(message)) if message == reason
                ));
            }
            let prior: (i64, Option<String>, Option<String>) = executor
                .lock()
                .unwrap()
                .query_row(
                    "SELECT version, optional_note, optional_label FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(prior, (1, None, Some("initial label".into())));
            assert_eq!(
                executor
                    .apply_model_checked_committed_set(&context, &order_set(2))
                    .await
                    .unwrap(),
                InboxOutcome::Applied { checkpoint: 2 }
            );
            let final_values: (i64, i64, Option<String>, Option<String>) = executor
                .lock()
                .unwrap()
                .query_row(
                    "SELECT version, active, optional_note, optional_label FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();
            assert_eq!(final_values, (2, 1, Some("submitted note".into()), None));
        });
    }

    #[test]
    fn model_checked_target_rolls_back_partial_set_and_rejects_version_and_model_mismatch() {
        futures_executor::block_on(async {
            let (executor, context) = model_target().await;
            executor
                .apply_model_checked_committed_set(&context, &order_set(1))
                .await
                .unwrap();
            executor
                .apply_model_checked_committed_set(&context, &order_set(2))
                .await
                .unwrap();

            let partial = make_record(
                3,
                "order-graph-0003",
                serde_json::json!([
                    {
                        "entity_type": "order",
                        "entity_id": "9007199254740993",
                        "operation": "update",
                        "base_version": "2",
                        "committed_version": "3",
                        "fields": { "status": "FULFILLED" }
                    },
                    {
                        "entity_type": "order_item",
                        "entity_id": "9007199254740995",
                        "operation": "create",
                        "base_version": null,
                        "committed_version": "1",
                        "fields": {
                            "order": { "entity_type": "order", "entity_id": "999" },
                            "quantity": "1"
                        }
                    }
                ]),
            );
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &partial)
                    .await,
                Err(CommittedEventError::Storage(_))
            ));

            let stale = make_record(
                3,
                "order-graph-0003-stale",
                serde_json::json!([{
                    "entity_type": "order",
                    "entity_id": "9007199254740993",
                    "operation": "update",
                    "base_version": "1",
                    "committed_version": "2",
                    "fields": { "status": "STALE" }
                }]),
            );
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &stale)
                    .await,
                Err(CommittedEventError::VersionConflict {
                    expected: Some(1),
                    received: Some(2)
                })
            ));

            let mut wrong_model = stale.clone();
            wrong_model.model_hash = format!("sha256:{}", "1".repeat(64));
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &wrong_model)
                    .await,
                Err(CommittedEventError::ModelMismatch)
            ));
            let connection = executor.lock().unwrap();
            let actual: (i64, String) = connection
                .query_row(
                    "SELECT version, status FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(actual, (2, "SUBMITTED".into()));
            let checkpoint: i64 = connection.query_row(
                "SELECT source_sequence FROM teaql_event_checkpoint WHERE source_runtime_id = ?1",
                params!["sqlite-order-source"],
                |row| row.get(0),
            ).unwrap();
            assert_eq!(checkpoint, 2);
            let inbox: i64 = connection
                .query_row("SELECT COUNT(*) FROM teaql_event_inbox", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(inbox, 2);
            let retained: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM teaql_event_applied_payload",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(retained, 2);
        });
    }

    #[test]
    fn model_checked_target_rejects_storage_that_rewrites_committed_values() {
        futures_executor::block_on(async {
            let (executor, context) = model_target().await;
            {
                let connection = executor.lock().unwrap();
                connection
                    .execute_batch(
                        "CREATE TRIGGER override_committed_status AFTER INSERT ON replica_order \
                         BEGIN UPDATE replica_order SET status = 'TARGET_OVERRIDE' WHERE id = NEW.id; END;",
                    )
                    .unwrap();
            }
            assert!(matches!(
                executor
                    .apply_model_checked_committed_set(&context, &order_set(1))
                    .await,
                Err(CommittedEventError::InvalidEnvelope(
                    "target did not retain committed field value"
                ))
            ));
            let connection = executor.lock().unwrap();
            for table in [
                "replica_order",
                "replica_order_item",
                "teaql_event_inbox",
                "teaql_event_applied_payload",
                "teaql_event_checkpoint",
            ] {
                let count: i64 = connection
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(count, 0, "{table} must roll back with the graph");
            }
        });
    }

    #[test]
    fn ordered_publisher_recovers_ambiguous_ack_after_source_restart() {
        use teaql_sql::{SqlTransaction, SqlTransactionTransport};

        futures_executor::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let source_path = directory.path().join("source.sqlite");
            let source =
                SqliteMutationExecutor::from_connection(Connection::open(&source_path).unwrap());
            source.ensure_committed_event_schema().unwrap();
            for change in ["order-graph-0001", "order-graph-0002"] {
                let transaction = source.begin_sql().await.unwrap();
                transaction
                    .append_committed_event("sqlite-order-source", change, MODEL_HASH, |sequence| {
                        Ok(order_set(sequence).payload_json)
                    })
                    .unwrap();
                transaction.commit_sql().await.unwrap();
            }
            let (target, context) = model_target().await;
            assert_eq!(
                source
                    .committed_delivery_status("sqlite-order-source", "analytics", MODEL_HASH)
                    .unwrap()
                    .backlog,
                2
            );
            assert!(matches!(
                source
                    .publish_committed_batch(
                        "sqlite-order-source",
                        "analytics",
                        MODEL_HASH,
                        2,
                        |_record| async { Ok(InboxOutcome::Applied { checkpoint: 99 }) },
                    )
                    .await,
                Err(CommittedEventError::InvalidEnvelope(
                    "target acknowledgement does not match delivered sequence"
                ))
            ));
            assert_eq!(
                source
                    .committed_delivery_status("sqlite-order-source", "analytics", MODEL_HASH)
                    .unwrap()
                    .acked_sequence,
                0
            );

            assert!(matches!(
                source
                    .publish_committed_batch(
                        "sqlite-order-source",
                        "analytics",
                        MODEL_HASH,
                        2,
                        |record| {
                            let target = &target;
                            let context = &context;
                            async move {
                                let _ = target
                                    .apply_model_checked_committed_set(context, &record)
                                    .await?;
                                Err(CommittedEventError::TransportUnavailable)
                            }
                        },
                    )
                    .await,
                Err(CommittedEventError::TransportUnavailable)
            ));
            assert_eq!(
                source
                    .committed_delivery_status("sqlite-order-source", "analytics", MODEL_HASH)
                    .unwrap()
                    .acked_sequence,
                0
            );
            drop(source);

            let restarted =
                SqliteMutationExecutor::from_connection(Connection::open(&source_path).unwrap());
            let outcome = restarted
                .publish_committed_batch(
                    "sqlite-order-source",
                    "analytics",
                    MODEL_HASH,
                    2,
                    |record| {
                        let target = &target;
                        let context = &context;
                        async move {
                            target
                                .apply_model_checked_committed_set(context, &record)
                                .await
                        }
                    },
                )
                .await
                .unwrap();
            assert_eq!(outcome.acknowledged, 2);
            assert_eq!(outcome.status.acked_sequence, 2);
            assert_eq!(outcome.status.backlog, 0);
            let empty = restarted
                .publish_committed_batch(
                    "sqlite-order-source",
                    "analytics",
                    MODEL_HASH,
                    2,
                    |_record| async {
                        panic!("an acknowledged Outbox record must not be delivered again")
                    },
                )
                .await
                .unwrap();
            assert_eq!(empty.acknowledged, 0);
            assert_eq!(
                restarted
                    .committed_delivery_status("sqlite-order-source", "second-target", MODEL_HASH)
                    .unwrap()
                    .backlog,
                2
            );
            assert!(matches!(
                restarted.committed_delivery_status(
                    "sqlite-order-source",
                    "analytics",
                    &format!("sha256:{}", "1".repeat(64)),
                ),
                Err(CommittedEventError::ModelMismatch)
            ));
            restarted
                .lock()
                .unwrap()
                .execute(
                    "DELETE FROM teaql_event_outbox WHERE source_runtime_id = ?1 AND source_sequence = 1",
                    params!["sqlite-order-source"],
                )
                .unwrap();
            assert!(matches!(
                restarted
                    .publish_committed_batch(
                        "sqlite-order-source",
                        "second-target",
                        MODEL_HASH,
                        2,
                        |_record| async { panic!("a gap must be rejected before delivery") },
                    )
                    .await,
                Err(CommittedEventError::SequenceGap {
                    expected: 1,
                    received: 2
                })
            ));
            let connection = target.lock().unwrap();
            let status: String = connection
                .query_row(
                    "SELECT status FROM replica_order WHERE id = ?1",
                    params![9_007_199_254_740_993_i64],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(status, "SUBMITTED");
        });
    }
    use futures_util::StreamExt;
    use teaql_sql::{
        CompiledQuery, SqlTransaction, SqlTransactionTransport, SqlTransport, StreamingSqlTransport,
    };

    fn payload(source: &str, sequence: u64, change_set_id: &str, status: &str) -> String {
        let mutations = if status == "DRAFT" {
            serde_json::json!([
                {"entity_type": "order", "entity_id": "42", "operation": "create", "base_version": null, "committed_version": "1", "fields": {"status": status}},
                {"entity_type": "order_item", "entity_id": "43", "operation": "create", "base_version": null, "committed_version": "1", "fields": {"order": {"entity_type": "order", "entity_id": "42"}, "quantity": "2"}}
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
    async fn provider_neutral_transaction_append_allocates_sequence_and_rolls_back() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        executor.ensure_committed_event_schema().unwrap();
        let mut logical: JsonValue =
            serde_json::from_str(&payload("source-a", 0, "change-a", "DRAFT")).unwrap();
        logical.as_object_mut().unwrap().remove("source_sequence");
        let append = CommittedSetAppend {
            source_runtime_id: "source-a".into(),
            change_set_id: "change-a".into(),
            model_hash: MODEL_HASH.into(),
            payload_json: logical.to_string(),
        };
        assert!(!format!("{append:?}").contains("DRAFT"));

        let transaction = executor.begin_sql().await.unwrap();
        assert!(transaction.append_committed_set_sql(&append).await.unwrap());
        assert_eq!(
            executor
                .committed_outbox_after("source-a", 0, 10)
                .unwrap()
                .len(),
            1
        );
        transaction.rollback_sql().await.unwrap();
        assert!(
            executor
                .committed_outbox_after("source-a", 0, 10)
                .unwrap()
                .is_empty()
        );

        let transaction = executor.begin_sql().await.unwrap();
        assert!(transaction.append_committed_set_sql(&append).await.unwrap());
        transaction.commit_sql().await.unwrap();
        let retained = executor.committed_outbox_after("source-a", 0, 10).unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].source_sequence, 1);
        assert_eq!(
            serde_json::from_str::<JsonValue>(&retained[0].payload_json).unwrap()["source_sequence"],
            "1"
        );

        // This is the actual generic transaction type seen by GraphSaverFor.
        let service = teaql_sql::SqlDataServiceExecutor::new(
            crate::SqliteDialect,
            executor.clone(),
            InMemoryMetadataStore::new(),
        );
        let mut next_payload: JsonValue =
            serde_json::from_str(&payload("source-a", 0, "change-b", "SUBMITTED")).unwrap();
        next_payload
            .as_object_mut()
            .unwrap()
            .remove("source_sequence");
        let next = CommittedSetAppend {
            change_set_id: "change-b".into(),
            payload_json: next_payload.to_string(),
            ..append.clone()
        };
        let transaction = teaql_data_service::TransactionExecutor::begin(&service)
            .await
            .unwrap();
        assert!(
            teaql_data_service::Transaction::append_committed_set(&transaction, &next)
                .await
                .unwrap()
        );
        teaql_data_service::Transaction::commit(transaction)
            .await
            .unwrap();
        assert_eq!(
            executor.committed_outbox_after("source-a", 0, 10).unwrap()[1].source_sequence,
            2
        );

        let transaction = executor.begin_sql().await.unwrap();
        let mut forged = append.clone();
        forged.payload_json = payload("source-a", 99, "change-a", "DRAFT");
        assert!(transaction.append_committed_set_sql(&forged).await.is_err());
        transaction.rollback_sql().await.unwrap();
        assert_eq!(
            executor
                .committed_outbox_after("source-a", 0, 10)
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn caught_append_failure_cannot_commit_earlier_business_write() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        executor.ensure_committed_event_schema().unwrap();
        executor
            .lock()
            .unwrap()
            .execute_batch("CREATE TABLE business_order (id INTEGER PRIMARY KEY)")
            .unwrap();
        let transaction = executor.begin_sql().await.unwrap();
        transaction
            .execute_sql(&CompiledQuery {
                log_context: Default::default(),
                sql: "INSERT INTO business_order (id) VALUES (1)".into(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let invalid_append = CommittedSetAppend {
            source_runtime_id: "source-a".into(),
            change_set_id: "change-a".into(),
            model_hash: MODEL_HASH.into(),
            payload_json: r#"{"source_sequence":"99"}"#.into(),
        };
        assert!(
            transaction
                .append_committed_set_sql(&invalid_append)
                .await
                .is_err()
        );
        assert!(matches!(
            transaction.commit_sql().await,
            Err(MutationExecutorError::RollbackOnly)
        ));
        let count: i64 = executor
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM business_order", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn caught_direct_append_builder_failure_cannot_commit_earlier_business_write() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        executor.ensure_committed_event_schema().unwrap();
        executor
            .lock()
            .unwrap()
            .execute_batch("CREATE TABLE business_order (id INTEGER PRIMARY KEY)")
            .unwrap();
        let transaction = executor.begin_sql().await.unwrap();
        transaction
            .execute_sql(&CompiledQuery {
                log_context: Default::default(),
                sql: "INSERT INTO business_order (id) VALUES (1)".into(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        assert!(
            transaction
                .append_committed_event("source-a", "change-a", MODEL_HASH, |_| {
                    Err(CommittedEventError::InvalidEnvelope("builder failed"))
                })
                .is_err()
        );
        assert!(matches!(
            transaction.commit_sql().await,
            Err(MutationExecutorError::RollbackOnly)
        ));
        let count: i64 = executor
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM business_order", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn caught_sql_failure_cannot_commit_earlier_business_write() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        executor
            .lock()
            .unwrap()
            .execute_batch("CREATE TABLE business_order (id INTEGER PRIMARY KEY)")
            .unwrap();
        let transaction = executor.begin_sql().await.unwrap();
        let insert = CompiledQuery {
            log_context: Default::default(),
            sql: "INSERT INTO business_order (id) VALUES (1)".into(),
            params: vec![],
            comment: None,
        };
        transaction.execute_sql(&insert).await.unwrap();
        assert!(transaction.execute_sql(&insert).await.is_err());
        assert!(matches!(
            transaction.commit_sql().await,
            Err(MutationExecutorError::RollbackOnly)
        ));
        let count: i64 = executor
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM business_order", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);

        let next = executor.begin_sql().await.unwrap();
        next.execute_sql(&insert).await.unwrap();
        next.commit_sql().await.unwrap();
        let count: i64 = executor
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM business_order", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn caught_stream_failure_cannot_commit_earlier_business_write() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        executor
            .lock()
            .unwrap()
            .execute_batch("CREATE TABLE business_order (id INTEGER PRIMARY KEY)")
            .unwrap();
        let transaction = executor.begin_sql().await.unwrap();
        transaction
            .execute_sql(&CompiledQuery {
                log_context: Default::default(),
                sql: "INSERT INTO business_order (id) VALUES (1)".into(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let mut stream = transaction.stream_sql(
            CompiledQuery {
                log_context: Default::default(),
                sql: "SELECT id FROM missing_business_table".into(),
                params: vec![],
                comment: None,
            },
            1,
        );
        assert!(stream.next().await.unwrap().is_err());
        drop(stream);
        assert!(matches!(
            transaction.commit_sql().await,
            Err(MutationExecutorError::RollbackOnly)
        ));
        let count: i64 = executor
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM business_order", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
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
        assert!(!format!("{first:?}").contains("DRAFT"));
        assert!(!format!("{first:?}").contains("order_item"));
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
