//! Typed extension storage on the PostgreSQL provider's own connection.
//! Ambient handles may initialize/read; only the physical transaction writes.

use super::{PgMutationExecutor, PgTransactionExecutor};
use deadpool_postgres::{GenericClient, Object};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use teaql_core::dynamic_fields::{
    DynamicFieldDefinitions, DynamicFieldError, DynamicFieldMutation, DynamicFieldSelection,
};
use teaql_core::{DataType, MutationIntent, QueryIntent, Value};
use teaql_data_service::dynamic_fields::{
    DynamicFieldStore, DynamicFieldWrite, DynamicStorageFuture, DynamicStorageRows,
};
use tokio_postgres::types::ToSql;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS teaql_dynamic_field_storage_v1 (
    namespace TEXT NOT NULL, owner_type TEXT NOT NULL, owner_id TEXT NOT NULL,
    code TEXT NOT NULL, definition_revision TEXT NOT NULL, data_type TEXT NOT NULL,
    is_null BOOLEAN NOT NULL, payload TEXT,
    PRIMARY KEY (namespace, owner_type, owner_id, code),
    CHECK ((is_null AND payload IS NULL) OR (NOT is_null AND payload IS NOT NULL)))";
const SELECT: &str = "SELECT owner_id,code,definition_revision,data_type,is_null,payload
    FROM teaql_dynamic_field_storage_v1
    WHERE namespace=$1 AND owner_type=$2 AND owner_id=ANY($3::text[])";
const UPSERT: &str = "INSERT INTO teaql_dynamic_field_storage_v1
    (namespace, owner_type, owner_id, code, definition_revision, data_type, is_null, payload)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT(namespace,owner_type,owner_id,code) DO UPDATE SET
    definition_revision=excluded.definition_revision, data_type=excluded.data_type,
    is_null=excluded.is_null, payload=excluded.payload";
const DELETE: &str = "DELETE FROM teaql_dynamic_field_storage_v1
    WHERE namespace=$1 AND owner_type=$2 AND owner_id=$3 AND code=$4";

fn error(code: &'static str, field: impl Into<String>) -> DynamicFieldError {
    DynamicFieldError {
        code,
        field: field.into(),
    }
}

fn database_error(_: impl std::fmt::Display) -> DynamicFieldError {
    // Driver diagnostics may contain stored values or SQL parameters.
    error("DYNAMIC_FIELD_STORAGE_FAILED", "storage")
}

fn check_namespace(namespace: &str) -> Result<(), DynamicFieldError> {
    if namespace.trim().is_empty() {
        return Err(error("DYNAMIC_FIELD_NAMESPACE_REQUIRED", "namespace"));
    }
    Ok(())
}

fn check_binding(
    namespace: &str,
    definitions: &DynamicFieldDefinitions,
    source_identity: u64,
) -> Result<(), DynamicFieldError> {
    check_namespace(namespace)?;
    if !definitions.storage_binding_matches(namespace, source_identity) {
        return Err(error(
            "DYNAMIC_FIELD_STORAGE_PROVENANCE_MISMATCH",
            definitions.owner_type(),
        ));
    }
    Ok(())
}

fn validate(writes: &[DynamicFieldWrite], source_identity: u64) -> Result<(), DynamicFieldError> {
    for write in writes {
        check_binding(&write.namespace, &write.definitions, source_identity)?;
        if write.owner_id == 0 {
            return Err(error(
                "DYNAMIC_FIELD_OWNER_ID_REQUIRED",
                write.definitions.owner_type(),
            ));
        }
        for (code, mutation) in &write.changes {
            let data_type = write.definitions.data_type(code)?;
            if data_type == DataType::BlobRef {
                return Err(error("BLOB_CAPABILITY_MISSING", code));
            }
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

async fn apply(client: &Object, writes: &[DynamicFieldWrite]) -> Result<(), DynamicFieldError> {
    for write in writes {
        let id = write.owner_id.to_string();
        let owner_type = write.definitions.owner_type();
        for (code, mutation) in &write.changes {
            match mutation {
                DynamicFieldMutation::Delete => {
                    let params: [&(dyn ToSql + Sync); 4] =
                        [&write.namespace, &owner_type, &id, code];
                    client
                        .execute(DELETE, &params)
                        .await
                        .map_err(database_error)?;
                }
                DynamicFieldMutation::Set(value) => {
                    let is_null = matches!(value, Value::Null | Value::TypedNull(_));
                    let payload = (!is_null).then(|| match value {
                        // Keep every finite f64 bit and every decimal digit.
                        Value::F64(number) => format!("\"{:016x}\"", number.to_bits()),
                        other => other.to_json_value().to_string(),
                    });
                    let data_type = format!("{:?}", write.definitions.data_type(code)?);
                    let revision = write.definitions.revision();
                    let params: [&(dyn ToSql + Sync); 8] = [
                        &write.namespace,
                        &owner_type,
                        &id,
                        code,
                        &revision,
                        &data_type,
                        &is_null,
                        &payload,
                    ];
                    client
                        .execute(UPSERT, &params)
                        .await
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
        DataType::BlobRef => return Err(error("BLOB_CAPABILITY_MISSING", code)),
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

async fn load(
    client: &Object,
    namespace: &str,
    definitions: &DynamicFieldDefinitions,
    source_identity: u64,
    ids: &[u64],
    selection: &DynamicFieldSelection,
) -> Result<DynamicStorageRows, DynamicFieldError> {
    check_binding(namespace, definitions, source_identity)?;
    selection.validate(definitions)?;
    let mut result: DynamicStorageRows = ids.iter().map(|id| (*id, HashMap::new())).collect();
    for chunk in ids.chunks(400) {
        let owners = chunk.iter().map(u64::to_string).collect::<Vec<_>>();
        let owner_type = definitions.owner_type();
        let params: [&(dyn ToSql + Sync); 3] = [&namespace, &owner_type, &owners];
        let rows = client
            .query(SELECT, &params)
            .await
            .map_err(database_error)?;
        for row in rows {
            let id: String = row.try_get(0).map_err(database_error)?;
            let code: String = row.try_get(1).map_err(database_error)?;
            if !selection.contains(&code) {
                continue;
            }
            let revision: String = row.try_get(2).map_err(database_error)?;
            let stored_type: String = row.try_get(3).map_err(database_error)?;
            let is_null: bool = row.try_get(4).map_err(database_error)?;
            let payload: Option<String> = row.try_get(5).map_err(database_error)?;
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
            if target
                .insert(code.clone(), decode(data_type, is_null, payload, &code)?)
                .is_some()
            {
                return Err(error("DYNAMIC_FIELD_INVALID_STORAGE", code));
            }
        }
    }
    Ok(result)
}

impl DynamicFieldStore for PgMutationExecutor {
    fn source_identity(&self) -> Option<u64> {
        Some(*self.dynamic_storage_identity)
    }
    fn is_transaction_bound(&self) -> bool {
        false
    }
    fn binding_key(&self) -> usize {
        Arc::as_ptr(&self.dynamic_storage_identity) as usize
    }
    fn ensure_schema(&self) -> DynamicStorageFuture<'_, ()> {
        Box::pin(async move {
            let client = self.pool.get().await.map_err(database_error)?;
            client.batch_execute(SCHEMA).await.map_err(database_error)
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
            let client = self.pool.get().await.map_err(database_error)?;
            load(
                &client,
                namespace,
                definitions,
                *self.dynamic_storage_identity,
                ids,
                selection,
            )
            .await
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

impl DynamicFieldStore for PgTransactionExecutor {
    fn source_identity(&self) -> Option<u64> {
        Some(*self.dynamic_storage_identity)
    }
    fn is_transaction_bound(&self) -> bool {
        self.identity.outcome()
            == teaql_data_service::infrastructure_sql::PhysicalTransactionOutcome::Active
            && !self.rollback_only.load(Ordering::SeqCst)
    }
    fn binding_key(&self) -> usize {
        Arc::as_ptr(&self.dynamic_storage_identity) as usize
    }
    fn ensure_schema(&self) -> DynamicStorageFuture<'_, ()> {
        Box::pin(async move {
            if !self.is_transaction_bound() {
                return Err(error(
                    "DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED",
                    "schema",
                ));
            }
            let result = self
                .client()
                .batch_execute(SCHEMA)
                .await
                .map_err(database_error);
            if result.is_err() {
                self.rollback_only.store(true, Ordering::SeqCst);
            }
            result
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
            if !self.is_transaction_bound() {
                return Err(error("DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED", "read"));
            }
            let result = load(
                self.client(),
                namespace,
                definitions,
                *self.dynamic_storage_identity,
                ids,
                selection,
            )
            .await;
            if result.is_err() {
                self.rollback_only.store(true, Ordering::SeqCst);
            }
            result
        })
    }
    fn validate_writes(&self, writes: &[DynamicFieldWrite]) -> Result<(), DynamicFieldError> {
        if !self.is_transaction_bound() {
            return Err(error("DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED", "save"));
        }
        validate(writes, *self.dynamic_storage_identity)
    }
    fn apply_writes<'a>(
        &'a self,
        writes: &'a [DynamicFieldWrite],
        _intent: &'a MutationIntent,
    ) -> DynamicStorageFuture<'a, ()> {
        Box::pin(async move {
            self.validate_writes(writes)?;
            let result = apply(self.client(), writes).await;
            if result.is_err() {
                self.rollback_only.store(true, Ordering::SeqCst);
            }
            result
        })
    }
}
