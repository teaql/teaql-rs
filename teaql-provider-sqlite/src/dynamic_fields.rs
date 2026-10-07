//! Typed extension storage on the native SQLite connection. The ambient executor
//! can initialize/read; only its exclusive transaction handle can mutate.

use super::{SqliteMutationExecutor, transaction::SqliteTransaction};
use rusqlite::{Connection, params, params_from_iter};
use std::collections::HashMap;
use teaql_core::dynamic_fields::{
    DynamicFieldDefinitions, DynamicFieldError, DynamicFieldMutation, DynamicFieldSelection,
};
use teaql_core::{DataType, MutationIntent, QueryIntent, Value};
use teaql_data_service::dynamic_fields::{
    DynamicFieldStore, DynamicFieldWrite, DynamicStorageFuture, DynamicStorageRows,
};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS teaql_dynamic_field_storage_v1 (
    namespace TEXT NOT NULL, owner_type TEXT NOT NULL, owner_id TEXT NOT NULL,
    code TEXT NOT NULL, definition_revision TEXT NOT NULL, data_type TEXT NOT NULL,
    is_null INTEGER NOT NULL CHECK (is_null IN (0,1)), payload TEXT,
    PRIMARY KEY (namespace, owner_type, owner_id, code),
    CHECK ((is_null=1 AND payload IS NULL) OR (is_null=0 AND payload IS NOT NULL)))";
const UPSERT: &str = "INSERT INTO teaql_dynamic_field_storage_v1
    (namespace, owner_type, owner_id, code, definition_revision, data_type, is_null, payload)
    VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
    ON CONFLICT(namespace,owner_type,owner_id,code) DO UPDATE SET
    definition_revision=excluded.definition_revision, data_type=excluded.data_type,
    is_null=excluded.is_null, payload=excluded.payload";
const DELETE: &str = "DELETE FROM teaql_dynamic_field_storage_v1
    WHERE namespace=?1 AND owner_type=?2 AND owner_id=?3 AND code=?4";

fn error(code: &'static str, field: impl Into<String>) -> DynamicFieldError {
    DynamicFieldError {
        code,
        field: field.into(),
    }
}
fn database_error(_: impl std::fmt::Display) -> DynamicFieldError {
    // Driver diagnostics can contain arbitrary stored data. Keep them out of
    // default errors; transaction failure/rollback is reported by the owner.
    error("DYNAMIC_FIELD_STORAGE_FAILED", "storage")
}
fn check_namespace(namespace: &str) -> Result<(), DynamicFieldError> {
    if namespace.trim().is_empty() {
        return Err(error("DYNAMIC_FIELD_NAMESPACE_REQUIRED", "namespace"));
    }
    Ok(())
}

fn validate(writes: &[DynamicFieldWrite]) -> Result<(), DynamicFieldError> {
    for write in writes {
        check_namespace(&write.namespace)?;
        if write.owner_id == 0 {
            return Err(error(
                "DYNAMIC_FIELD_OWNER_ID_REQUIRED",
                write.definitions.owner_type(),
            ));
        }
        for (code, mutation) in &write.changes {
            write.definitions.data_type(code)?;
            if let DynamicFieldMutation::Set(value) = mutation {
                if matches!(value, Value::F64(number) if !number.is_finite()) {
                    return Err(error("DYNAMIC_FIELD_NONFINITE_NUMBER", code));
                }
                write.definitions.validate_value(code, value)?;
            }
        }
    }
    Ok(())
}

fn apply(connection: &Connection, writes: &[DynamicFieldWrite]) -> Result<(), DynamicFieldError> {
    // Reject any unsupported operand before issuing the first statement.
    validate(writes)?;
    let mut upsert = connection.prepare_cached(UPSERT).map_err(database_error)?;
    let mut delete = connection.prepare_cached(DELETE).map_err(database_error)?;
    for write in writes {
        let id = write.owner_id.to_string();
        for (code, mutation) in &write.changes {
            match mutation {
                DynamicFieldMutation::Delete => {
                    delete
                        .execute(params![
                            write.namespace,
                            write.definitions.owner_type(),
                            id,
                            code
                        ])
                        .map_err(database_error)?;
                }
                DynamicFieldMutation::Set(value) => {
                    let is_null = matches!(value, Value::Null | Value::TypedNull(_));
                    let payload = (!is_null).then(|| match value {
                        // Preserve every finite f64 bit, including negative zero.
                        // Decimal uses its exact decimal string, never a float.
                        Value::F64(number) => format!("\"{:016x}\"", number.to_bits()),
                        other => other.to_json_value().to_string(),
                    });
                    let data_type = format!("{:?}", write.definitions.data_type(code)?);
                    upsert
                        .execute(params![
                            write.namespace,
                            write.definitions.owner_type(),
                            id,
                            code,
                            write.definitions.revision(),
                            data_type,
                            is_null,
                            payload
                        ])
                        .map_err(database_error)?;
                }
            }
        }
    }
    Ok(())
}

fn decode(
    data_type: DataType,
    is_null: bool,
    payload: Option<String>,
    code: &str,
) -> Result<Value, DynamicFieldError> {
    if is_null {
        if payload.is_some() {
            return Err(error("DYNAMIC_FIELD_INVALID_STORAGE", code));
        }
        return Ok(Value::Null);
    }
    let payload = payload.ok_or_else(|| error("DYNAMIC_FIELD_INVALID_STORAGE", code))?;
    let json: serde_json::Value =
        serde_json::from_str(&payload).map_err(|_| error("DYNAMIC_FIELD_INVALID_STORAGE", code))?;
    let invalid = || error("DYNAMIC_FIELD_INVALID_STORAGE", code);
    Ok(match data_type {
        DataType::Bool => Value::Bool(json.as_bool().ok_or_else(invalid)?),
        DataType::I64 => Value::I64(json.as_i64().ok_or_else(invalid)?),
        DataType::U64 => Value::U64(json.as_u64().ok_or_else(invalid)?),
        DataType::F64 => {
            let bits = u64::from_str_radix(json.as_str().ok_or_else(invalid)?, 16)
                .map_err(|_| invalid())?;
            let number = f64::from_bits(bits);
            if !number.is_finite() {
                return Err(invalid());
            }
            Value::F64(number)
        }
        DataType::Decimal => Value::Decimal(
            json.as_str()
                .ok_or_else(invalid)?
                .parse()
                .map_err(|_| invalid())?,
        ),
        DataType::Text | DataType::LargeText => {
            Value::Text(json.as_str().ok_or_else(invalid)?.to_owned())
        }
        DataType::Json => Value::Json(json),
        DataType::Date => Value::Date(
            json.as_str()
                .ok_or_else(invalid)?
                .parse()
                .map_err(|_| invalid())?,
        ),
        DataType::Timestamp => Value::Timestamp(teaql_core::time::Timestamp(
            json.as_i64().ok_or_else(invalid)?,
        )),
    })
}

fn load(
    connection: &Connection,
    namespace: &str,
    definitions: &DynamicFieldDefinitions,
    ids: &[u64],
    selection: &DynamicFieldSelection,
) -> Result<DynamicStorageRows, DynamicFieldError> {
    check_namespace(namespace)?;
    selection.validate(definitions)?;
    let mut result: DynamicStorageRows = ids.iter().map(|id| (*id, HashMap::new())).collect();
    // Bounded bind batches also work with SQLite builds using the older 999 limit.
    for chunk in ids.chunks(400) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT owner_id,code,definition_revision,data_type,is_null,payload
            FROM teaql_dynamic_field_storage_v1 WHERE namespace=? AND owner_type=? AND owner_id IN ({placeholders})");
        let mut bindings = Vec::with_capacity(chunk.len() + 2);
        bindings.push(namespace.to_owned());
        bindings.push(definitions.owner_type().to_owned());
        bindings.extend(chunk.iter().map(u64::to_string));
        let mut statement = connection.prepare_cached(&sql).map_err(database_error)?;
        let rows = statement
            .query_map(params_from_iter(bindings), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })
            .map_err(database_error)?;
        for row in rows {
            let (id, code, revision, stored_type, is_null, payload) =
                row.map_err(database_error)?;
            if !selection.contains(&code) {
                continue;
            }
            let data_type = definitions.data_type(&code)?;
            if revision != definitions.revision() || stored_type != format!("{data_type:?}") {
                return Err(error("DYNAMIC_FIELD_DEFINITION_MISMATCH", code));
            }
            let id: u64 = id
                .parse()
                .map_err(|_| error("DYNAMIC_FIELD_INVALID_STORAGE", "owner_id"))?;
            let target = result
                .get_mut(&id)
                .ok_or_else(|| error("DYNAMIC_FIELD_UNEXPECTED_OWNER", definitions.owner_type()))?;
            let value = decode(data_type, is_null, payload, &code)?;
            target.insert(code, value);
        }
    }
    Ok(result)
}

impl DynamicFieldStore for SqliteMutationExecutor {
    fn source_identity(&self) -> Option<u64> {
        Some(self.storage_instance_id)
    }
    fn is_transaction_bound(&self) -> bool {
        false
    }
    fn binding_key(&self) -> usize {
        std::sync::Arc::as_ptr(&self.transaction_lease) as usize
    }
    fn ensure_schema(&self) -> DynamicStorageFuture<'_, ()> {
        Box::pin(async move {
            let _lease = self.transaction_lease.lock().await;
            self.lock()
                .map_err(database_error)?
                .execute_batch(SCHEMA)
                .map_err(database_error)
        })
    }
    fn load_values<'a>(
        &'a self,
        namespace: &'a str,
        definitions: &'a DynamicFieldDefinitions,
        ids: &'a [u64],
        selection: &'a DynamicFieldSelection,
        _intent: &'a QueryIntent,
    ) -> DynamicStorageFuture<'a, DynamicStorageRows> {
        Box::pin(async move {
            let _lease = self.transaction_lease.lock().await;
            load(
                &*self.lock().map_err(database_error)?,
                namespace,
                definitions,
                ids,
                selection,
            )
        })
    }
    fn validate_writes(&self, _writes: &[DynamicFieldWrite]) -> Result<(), DynamicFieldError> {
        Err(error("DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED", "save"))
    }
    fn apply_writes<'a>(
        &'a self,
        _writes: &'a [DynamicFieldWrite],
        _intent: &'a MutationIntent,
    ) -> DynamicStorageFuture<'a, ()> {
        Box::pin(async { Err(error("DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED", "save")) })
    }
}

impl DynamicFieldStore for SqliteTransaction {
    fn source_identity(&self) -> Option<u64> {
        self.executor.source_identity()
    }
    fn is_transaction_bound(&self) -> bool {
        self.active
    }
    fn binding_key(&self) -> usize {
        self.executor.binding_key()
    }
    fn ensure_schema(&self) -> DynamicStorageFuture<'_, ()> {
        Box::pin(async move {
            self.executor
                .lock()
                .map_err(database_error)?
                .execute_batch(SCHEMA)
                .map_err(database_error)
        })
    }
    fn load_values<'a>(
        &'a self,
        namespace: &'a str,
        definitions: &'a DynamicFieldDefinitions,
        ids: &'a [u64],
        selection: &'a DynamicFieldSelection,
        _intent: &'a QueryIntent,
    ) -> DynamicStorageFuture<'a, DynamicStorageRows> {
        Box::pin(async move {
            load(
                &*self.executor.lock().map_err(database_error)?,
                namespace,
                definitions,
                ids,
                selection,
            )
        })
    }
    fn validate_writes(&self, writes: &[DynamicFieldWrite]) -> Result<(), DynamicFieldError> {
        if !self.active {
            return Err(error("DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED", "save"));
        }
        validate(writes)
    }
    fn apply_writes<'a>(
        &'a self,
        writes: &'a [DynamicFieldWrite],
        _intent: &'a MutationIntent,
    ) -> DynamicStorageFuture<'a, ()> {
        Box::pin(async move {
            self.validate_writes(writes)?;
            apply(&*self.executor.lock().map_err(database_error)?, writes)
        })
    }
}
