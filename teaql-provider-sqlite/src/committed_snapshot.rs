//! Complete model snapshots, separate from ordinary committed mutation envelopes.
use super::*;
use serde::{
    Deserialize, Deserializer,
    de::{Error as _, MapAccess, SeqAccess, Visitor},
};
use serde_json::json;
use std::collections::BTreeSet;
use teaql_sql::SqlTransaction;

#[derive(Clone, Copy, Debug)]
pub struct SnapshotLimits {
    max_rows: usize,
    max_payload_bytes: usize,
}
impl SnapshotLimits {
    pub fn new(max_rows: usize, max_payload_bytes: usize) -> Result<Self, CommittedEventError> {
        if max_rows == 0
            || max_rows == usize::MAX
            || i64::try_from(max_rows + 1).is_err()
            || max_payload_bytes == 0
        {
            return Err(invalid("positive snapshot budgets are required"));
        }
        Ok(Self {
            max_rows,
            max_payload_bytes,
        })
    }
}
impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            max_rows: 100_000,
            max_payload_bytes: 16 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotImportMode {
    RequireEmpty,
    ReplaceUninitialized,
}
#[derive(Clone, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub source_runtime_id: String,
    pub source_sequence: u64,
    pub snapshot_id: String,
    pub model_hash: String,
    pub payload_sha256: String,
    pub payload_json: String,
}
impl std::fmt::Debug for SnapshotRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotRecord")
            .field("source_sequence", &self.source_sequence)
            .field("payload", &"redacted")
            .finish()
    }
}
fn invalid(reason: &'static str) -> CommittedEventError {
    CommittedEventError::InvalidEnvelope(reason)
}
fn catalog(context: &UserContext) -> Result<Vec<&EntityDescriptor>, CommittedEventError> {
    let mut tables = context.all_entities();
    let mut names = BTreeSet::new();
    let mut physical = BTreeSet::new();
    if tables.is_empty() {
        return Err(invalid("snapshot requires an installed model"));
    }
    for entity in &tables {
        if entity.name.trim().is_empty()
            || entity.table_name.trim().is_empty()
            || entity.properties.iter().filter(|p| p.is_id).count() != 1
            || entity.properties.iter().filter(|p| p.is_version).count() != 1
            || entity.properties.iter().any(|p| p.is_id && p.is_version)
            || !names.insert(&entity.name)
            || entity
                .table_name
                .to_ascii_lowercase()
                .starts_with("teaql_event_")
            || !physical.insert(entity.table_name.to_ascii_lowercase())
        {
            return Err(invalid("unsupported or ambiguous snapshot table"));
        }
        let mut columns = BTreeSet::new();
        let mut properties = BTreeSet::new();
        for property in &entity.properties {
            if property.name.trim().is_empty()
                || strip_identifier_quotes(&property.column_name)
                    .trim()
                    .is_empty()
                || !properties.insert(&property.name)
                || !columns
                    .insert(strip_identifier_quotes(&property.column_name).to_ascii_lowercase())
            {
                return Err(invalid("duplicate snapshot column mapping"));
            }
        }
    }
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(tables)
}
pub(super) fn provider(
    context: &UserContext,
    executor: &SqliteMutationExecutor,
) -> Result<(), CommittedEventError> {
    let installed = context
        .get_resource::<SqliteMutationExecutor>()
        .ok_or(invalid("snapshot provider is not installed"))?;
    if !std::sync::Arc::ptr_eq(&installed.connection, &executor.connection) {
        return Err(invalid("snapshot provider differs from Context"));
    }
    Ok(())
}
fn bounded(text: &str, limits: SnapshotLimits) -> Result<(), CommittedEventError> {
    if text.len() > limits.max_payload_bytes {
        Err(invalid("snapshot byte budget exceeded"))
    } else {
        Ok(())
    }
}
pub(super) async fn finish<T>(
    transaction: SqliteTransaction,
    result: Result<T, CommittedEventError>,
) -> Result<T, CommittedEventError> {
    match result {
        Ok(value) => {
            transaction.commit_sql().await?;
            Ok(value)
        }
        Err(error) => {
            transaction.rollback_sql().await?;
            Err(error)
        }
    }
}

impl SqliteMutationExecutor {
    /// Dedicated deferred read transaction: N and all rows share one SQLite view.
    pub async fn export_model_checked_snapshot(
        &self,
        context: &UserContext,
        snapshot_id: &str,
        comment: &str,
        purpose: &str,
        limits: SnapshotLimits,
    ) -> Result<SnapshotRecord, CommittedEventError> {
        provider(context, self)?;
        let source = context
            .committed_event_source()
            .ok_or(invalid("snapshot source is not configured"))?;
        CommittedEventTarget::new(&source.source_runtime_id, &source.model_hash)?;
        if [snapshot_id, comment, purpose]
            .iter()
            .any(|text| text.trim().is_empty())
        {
            return Err(invalid("snapshot identity and intent are required"));
        }
        let transaction = self.begin_snapshot_transaction(true)?;
        let result = (|| {
            let connection = self.lock()?;
            let n: i64 = connection
                .query_row(
                    "SELECT current_sequence FROM teaql_event_sequence WHERE source_runtime_id=?1",
                    [&source.source_runtime_id],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(0);
            let latest: Option<(i64,String)> = connection.query_row("SELECT source_sequence,model_hash FROM teaql_event_outbox WHERE source_runtime_id=?1 ORDER BY source_sequence DESC LIMIT 1", [&source.source_runtime_id], |r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            if n < 0
                || n == 0 && latest.is_some()
                || n > 0 && latest.as_ref() != Some(&(n, source.model_hash.clone()))
            {
                return Err(invalid("source sequence/model is inconsistent"));
            }
            // Listing metadata after the first SELECT also provides a deterministic
            // consistency-test barrier while a second WAL connection commits N+1.
            let tables = catalog(context)?;
            let mut rows = Vec::new();
            let mut used_bytes = 0usize;
            for entity in &tables {
                let id = entity.id_property().unwrap();
                let version = entity.version_property().unwrap();
                let fields: Vec<_> = entity
                    .properties
                    .iter()
                    .filter(|p| !p.is_id && !p.is_version)
                    .collect();
                let columns = std::iter::once(id)
                    .chain(std::iter::once(version))
                    .chain(fields.iter().copied())
                    .map(|p| quote_ident(strip_identifier_quotes(&p.column_name)))
                    .collect::<Vec<_>>()
                    .join(",");
                let query = format!(
                    "SELECT {columns} FROM {} ORDER BY {} LIMIT ?1",
                    quote_ident(&entity.table_name),
                    quote_ident(strip_identifier_quotes(&id.column_name))
                );
                let mut statement = connection.prepare(&query)?;
                let mut values = statement
                    .query([i64::try_from(limits.max_rows - rows.len() + 1)
                        .map_err(|_| invalid("snapshot row budget is invalid"))?])?;
                while let Some(row) = values.next()? {
                    if rows.len() == limits.max_rows {
                        return Err(invalid("snapshot row budget exceeded"));
                    }
                    let id: i64 = row.get(0)?;
                    let version: i64 = row.get(1)?;
                    let mut encoded = serde_json::Map::new();
                    for (index, property) in fields.iter().enumerate() {
                        let value: SqliteValue = row.get(index + 2)?;
                        encoded.insert(
                            property.name.clone(),
                            encode(context, entity, property, &value)?,
                        );
                    }
                    let fact = json!({"entity_type":entity.name,"entity_id":id.to_string(),"version":version.to_string(),"fields":encoded});
                    decode_snapshot_row(context, &fact)?;
                    used_bytes = used_bytes
                        .checked_add(fact.to_string().len())
                        .ok_or(invalid("snapshot byte budget exceeded"))?;
                    if used_bytes > limits.max_payload_bytes {
                        return Err(invalid("snapshot byte budget exceeded"));
                    }
                    rows.push(fact);
                }
            }
            let payload = json!({"snapshot_version":1,"snapshot_id":snapshot_id,"source_runtime_id":source.source_runtime_id,"source_sequence":n.to_string(),"source_model_hash":source.model_hash,
                "audit":{"comment":comment,"purpose":purpose},"entity_types":tables.iter().map(|e|&e.name).collect::<Vec<_>>(),"rows":rows}).to_string();
            bounded(&payload, limits)?;
            Ok(SnapshotRecord {
                source_runtime_id: source.source_runtime_id.clone(),
                source_sequence: n as u64,
                snapshot_id: snapshot_id.into(),
                model_hash: source.model_hash.clone(),
                payload_sha256: digest(payload.as_bytes()),
                payload_json: payload,
            })
        })();
        finish(transaction, result).await
    }

    /// Startup-only atomic restore. A new snapshot can never reset an active stream.
    pub async fn initialize_model_checked_snapshot(
        &self,
        context: &UserContext,
        record: &SnapshotRecord,
        mode: SnapshotImportMode,
        limits: SnapshotLimits,
    ) -> Result<InboxOutcome, CommittedEventError> {
        provider(context, self)?;
        let target = context
            .get_resource::<CommittedEventTarget>()
            .ok_or(invalid("snapshot target authority is not configured"))?;
        let payload = validate(record, target, limits)?;
        let transaction = self.begin_snapshot_transaction(false)?;
        let result = (|| {
            let connection = self.lock()?;
            let prior: Option<(String,i64,String,String,String)> = connection.query_row("SELECT snapshot_id,source_sequence,model_hash,payload_sha256,payload_json FROM teaql_event_snapshot WHERE source_runtime_id=?1",[&record.source_runtime_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
            if let Some((id, n, model, hash, original)) = prior {
                if id != record.snapshot_id
                    || n < 0
                    || n as u64 != record.source_sequence
                    || model != record.model_hash
                    || hash != record.payload_sha256
                    || digest(original.as_bytes()) != hash
                {
                    return Err(CommittedEventError::DuplicateConflict);
                }
                let checkpoint: Option<(i64,String)> = connection.query_row("SELECT source_sequence,model_hash FROM teaql_event_checkpoint WHERE source_runtime_id=?1",[&record.source_runtime_id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
                if !checkpoint
                    .is_some_and(|(position, model)| position >= n && model == record.model_hash)
                {
                    return Err(invalid("snapshot checkpoint is inconsistent"));
                }
                return Ok(InboxOutcome::Duplicate {
                    checkpoint: record.source_sequence,
                });
            }
            for table in [
                "teaql_event_snapshot",
                "teaql_event_checkpoint",
                "teaql_event_inbox",
                "teaql_event_applied_payload",
                "teaql_event_outbox",
                "teaql_event_sequence",
                "teaql_event_snapshot_publication",
            ] {
                if count(&connection, table)? != 0 {
                    return Err(CommittedEventError::DuplicateConflict);
                }
            }
            let tables = catalog(context)?;
            let mut supplied = BTreeSet::new();
            for name in payload["entity_types"]
                .as_array()
                .ok_or(invalid("snapshot catalog is missing"))?
            {
                if !supplied.insert(
                    name.as_str()
                        .ok_or(invalid("snapshot catalog type is invalid"))?,
                ) {
                    return Err(invalid("duplicate snapshot catalog type"));
                }
            }
            if supplied
                != tables
                    .iter()
                    .map(|e| e.name.as_str())
                    .collect::<BTreeSet<_>>()
            {
                return Err(invalid("snapshot catalog differs from model"));
            }
            let mut mutations = Vec::new();
            let mut seen = BTreeSet::new();
            for row in payload["rows"].as_array().unwrap() {
                let mutation = decode_snapshot_row(context, row)?;
                if !seen.insert((mutation.table.clone(), mutation.id)) {
                    return Err(invalid("duplicate snapshot row"));
                }
                mutations.push(mutation);
            }
            connection.execute_batch("PRAGMA defer_foreign_keys=ON")?;
            for entity in &tables {
                if mode == SnapshotImportMode::RequireEmpty
                    && count(&connection, &entity.table_name)? != 0
                {
                    return Err(CommittedEventError::DuplicateConflict);
                }
                if mode == SnapshotImportMode::ReplaceUninitialized {
                    connection.execute(
                        &format!("DELETE FROM {}", quote_ident(&entity.table_name)),
                        [],
                    )?;
                }
            }
            for mutation in &mutations {
                apply_model_mutation(&connection, mutation)?;
            }
            for mutation in &mutations {
                verify_committed_row(&connection, mutation)?;
            }
            for entity in &tables {
                let expected = mutations
                    .iter()
                    .filter(|m| m.table == quote_ident(&entity.table_name))
                    .count() as i64;
                if count(&connection, &entity.table_name)? != expected {
                    return Err(invalid("target snapshot row count differs"));
                }
                let mut fk = connection.prepare(&format!(
                    "PRAGMA foreign_key_check({})",
                    quote_ident(&entity.table_name)
                ))?;
                if fk.query([])?.next()?.is_some() {
                    return Err(invalid("snapshot has a dangling reference"));
                }
            }
            connection.execute("INSERT INTO teaql_event_snapshot(source_runtime_id,snapshot_id,source_sequence,model_hash,payload_sha256,payload_json) VALUES (?1,?2,?3,?4,?5,?6)",params![record.source_runtime_id,record.snapshot_id,record.source_sequence as i64,record.model_hash,record.payload_sha256,record.payload_json])?;
            connection.execute("INSERT INTO teaql_event_checkpoint(source_runtime_id,source_sequence,model_hash) VALUES (?1,?2,?3)",params![record.source_runtime_id,record.source_sequence as i64,record.model_hash])?;
            Ok(InboxOutcome::Applied {
                checkpoint: record.source_sequence,
            })
        })();
        finish(transaction, result).await
    }
}
fn count(connection: &Connection, table: &str) -> Result<i64, CommittedEventError> {
    Ok(connection.query_row(
        &format!("SELECT count(*) FROM {}", quote_ident(table)),
        [],
        |r| r.get(0),
    )?)
}
pub(super) fn validate(
    record: &SnapshotRecord,
    target: &CommittedEventTarget,
    limits: SnapshotLimits,
) -> Result<JsonValue, CommittedEventError> {
    CommittedEventTarget::new(&target.source_runtime_id, &target.model_hash)?;
    if record.source_runtime_id != target.source_runtime_id {
        return Err(CommittedEventError::UnauthorizedSource);
    }
    if record.model_hash != target.model_hash {
        return Err(CommittedEventError::ModelMismatch);
    }
    if record.snapshot_id.trim().is_empty() || record.source_sequence > i64::MAX as u64 {
        return Err(invalid("snapshot identity/position is invalid"));
    }
    bounded(&record.payload_json, limits)?;
    if digest(record.payload_json.as_bytes()) != record.payload_sha256 {
        return Err(invalid("snapshot digest mismatch"));
    }
    let body = strict_json(&record.payload_json)?;
    if body.as_object().is_none_or(|o| o.len() != 8)
        || body["snapshot_version"].as_u64() != Some(1)
        || body["snapshot_id"].as_str() != Some(&record.snapshot_id)
        || body["source_runtime_id"].as_str() != Some(&record.source_runtime_id)
        || body["source_model_hash"].as_str() != Some(&record.model_hash)
        || body["source_sequence"].as_str() != Some(record.source_sequence.to_string().as_str())
        || body["rows"]
            .as_array()
            .is_none_or(|rows| rows.len() > limits.max_rows)
        || ["comment", "purpose"].iter().any(|key| {
            body["audit"][key]
                .as_str()
                .is_none_or(|s| s.trim().is_empty())
        })
    {
        return Err(invalid("snapshot header or rows are invalid"));
    }
    Ok(body)
}
fn encode(
    context: &UserContext,
    entity: &EntityDescriptor,
    property: &PropertyDescriptor,
    value: &SqliteValue,
) -> Result<JsonValue, CommittedEventError> {
    let wire = if *value == SqliteValue::Null {
        JsonValue::Null
    } else if let Some(relation) = entity
        .relations
        .iter()
        .find(|r| !r.many && r.local_key == property.name)
    {
        if let SqliteValue::Integer(id) = value {
            json!({"entity_type":relation.target_entity,"entity_id":id.to_string()})
        } else {
            return Err(invalid("source reference is not an integer"));
        }
    } else {
        match (property.data_type, value) {
            (DataType::Bool, SqliteValue::Integer(value)) if *value == 0 || *value == 1 => {
                json!(*value == 1)
            }
            (DataType::I64 | DataType::U64 | DataType::Timestamp, SqliteValue::Integer(value)) => {
                json!(value.to_string())
            }
            (DataType::F64, SqliteValue::Real(value)) if value.is_finite() => json!(value),
            (DataType::Decimal, SqliteValue::Integer(value)) => json!(value.to_string()),
            (
                DataType::Decimal | DataType::Text | DataType::LargeText | DataType::Date,
                SqliteValue::Text(value),
            ) => json!(value),
            (DataType::Json, SqliteValue::Text(value)) => {
                let json = strict_json(value)?;
                // The v1 wire uses null for SQL NULL. A stored JSON-text null is
                // distinct and has no lossless representation in this version.
                if json.is_null() {
                    return Err(invalid("JSON text null cannot be represented losslessly"));
                }
                json
            }
            _ => return Err(invalid("source storage cannot be represented losslessly")),
        }
    };
    let decoded = decode_model_value(context, entity, property, &wire)?;
    let field = ModelField {
        column: property.column_name.clone(),
        data_type: property.data_type,
        value: decoded,
    };
    let same = if property.data_type == DataType::Json && *value != SqliteValue::Null {
        if let SqliteValue::Text(text) = value {
            strict_json(text)? == wire
        } else {
            false
        }
    } else {
        same_committed_value(&field, value)
    };
    if !same {
        return Err(invalid("source field cannot be represented losslessly"));
    }
    Ok(wire)
}

// serde_json::Value normally accepts duplicate object keys. Snapshot input must
// reject them (and trailing data), matching the Java strict parser.
struct Strict(JsonValue);
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Strict;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("strict JSON")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Strict, E> {
                Ok(Strict(JsonValue::Null))
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Strict, E> {
                Ok(Strict(json!(v)))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Strict, E> {
                Ok(Strict(json!(v)))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Strict, E> {
                Ok(Strict(json!(v)))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Strict, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| Strict(JsonValue::Number(n)))
                    .ok_or_else(|| E::custom("non-finite JSON"))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Strict, E> {
                Ok(Strict(json!(v)))
            }
            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Strict, E> {
                Ok(Strict(JsonValue::String(v)))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                let mut v = Vec::new();
                while let Some(Strict(item)) = a.next_element()? {
                    v.push(item);
                }
                Ok(Strict(JsonValue::Array(v)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                let mut v = serde_json::Map::new();
                while let Some((key, Strict(item))) = a.next_entry::<String, Strict>()? {
                    if v.insert(key, item).is_some() {
                        return Err(A::Error::custom("duplicate JSON key"));
                    }
                }
                Ok(Strict(JsonValue::Object(v)))
            }
        }
        d.deserialize_any(V)
    }
}
fn strict_json(text: &str) -> Result<JsonValue, CommittedEventError> {
    let mut d = serde_json::Deserializer::from_str(text);
    let Strict(value) =
        Strict::deserialize(&mut d).map_err(|_| invalid("snapshot JSON is invalid"))?;
    d.end()
        .map_err(|_| invalid("snapshot JSON has trailing data"))?;
    Ok(value)
}
