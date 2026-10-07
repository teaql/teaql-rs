//! Framework-only extension storage bound to the executor used for native data.
//! No raw SQL, implicit fetch, tenant concept, or independent entity-save API.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use teaql_core::dynamic_fields::{
    DynamicFieldDefinitions, DynamicFieldError, DynamicFieldMutation, DynamicFieldSelection,
};
use teaql_core::{MutationIntent, QueryIntent, Value};

/// Trusted, non-wire read plan for enhancement on the cursor's own connection.
/// It cannot choose storage independently of the executing native provider.
#[doc(hidden)]
#[derive(Clone)]
pub struct DynamicFieldStreamPlan {
    namespace: String,
    definitions: Arc<DynamicFieldDefinitions>,
    selection: DynamicFieldSelection,
    binding_key: usize,
    source_identity: u64,
    _intent: QueryIntent,
}

impl DynamicFieldStreamPlan {
    pub fn new(
        namespace: String,
        definitions: Arc<DynamicFieldDefinitions>,
        selection: DynamicFieldSelection,
        store: &dyn DynamicFieldStore,
        intent: QueryIntent,
    ) -> Result<Self, DynamicFieldError> {
        selection.validate(&definitions)?;
        if namespace.trim().is_empty() {
            return Err(DynamicFieldError {
                code: "DYNAMIC_FIELD_NAMESPACE_REQUIRED",
                field: "namespace".into(),
            });
        }
        let source_identity = store.source_identity().ok_or_else(|| DynamicFieldError {
            code: "DYNAMIC_FIELD_STORAGE_IDENTITY_REQUIRED",
            field: definitions.owner_type().into(),
        })?;
        Ok(Self {
            definitions: definitions.with_storage_binding(&namespace, source_identity),
            namespace,
            selection,
            binding_key: store.binding_key(),
            source_identity,
            _intent: intent,
        })
    }

    pub fn validate_store(&self, store: &dyn DynamicFieldStore) -> Result<(), DynamicFieldError> {
        if store.binding_key() != self.binding_key
            || store.source_identity() != Some(self.source_identity)
        {
            return Err(DynamicFieldError {
                code: "DYNAMIC_FIELD_STORAGE_PROVENANCE_MISMATCH",
                field: self.definitions.owner_type().into(),
            });
        }
        Ok(())
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn definitions(&self) -> &DynamicFieldDefinitions {
        &self.definitions
    }
    pub fn selection(&self) -> &DynamicFieldSelection {
        &self.selection
    }

    pub fn owner_ids(
        &self,
        rows: &[teaql_core::CompactRow],
    ) -> Result<Vec<u64>, DynamicFieldError> {
        rows.iter()
            .map(|row| {
                row.get("id")
                    .and_then(Value::try_u64)
                    .filter(|id| *id != 0)
                    .ok_or_else(|| DynamicFieldError {
                        code: "DYNAMIC_FIELD_OWNER_ID_REQUIRED",
                        field: self.definitions.owner_type().into(),
                    })
            })
            .collect()
    }

    /// Validate the complete batch before its rows can become visible to callers.
    pub fn merge(
        &self,
        rows: &mut [teaql_core::CompactRow],
        ids: &[u64],
        mut stored: DynamicStorageRows,
    ) -> Result<(), DynamicFieldError> {
        let invalid = |code| DynamicFieldError {
            code,
            field: self.definitions.owner_type().into(),
        };
        if rows.len() != ids.len() {
            return Err(invalid("DYNAMIC_FIELD_BATCH_OMITTED_OWNER"));
        }
        if self.owner_ids(rows)?.as_slice() != ids {
            return Err(invalid("DYNAMIC_FIELD_OWNER_MISMATCH"));
        }
        let mut occurrences = HashMap::new();
        for id in ids {
            *occurrences.entry(*id).or_insert(0_usize) += 1;
        }
        if stored.keys().any(|id| !occurrences.contains_key(id)) {
            return Err(invalid("DYNAMIC_FIELD_UNREQUESTED_OWNER"));
        }
        let mut payloads = Vec::with_capacity(ids.len());
        for id in ids {
            let count = occurrences.get_mut(id).expect("requested owner");
            *count -= 1;
            let values = if *count == 0 {
                stored.remove(id)
            } else {
                stored.get(id).cloned()
            }
            .ok_or_else(|| invalid("DYNAMIC_FIELD_BATCH_OMITTED_OWNER"))?;
            if values.keys().any(|code| !self.selection.contains(code)) {
                return Err(invalid("DYNAMIC_FIELD_UNSELECTED_VALUE"));
            }
            payloads.push(values);
        }
        let values = teaql_core::dynamic_fields::DynamicFieldValues::from_batch(
            self.definitions.clone(),
            payloads,
        )?;
        let mut shapes = teaql_core::dynamic_fields::DynamicFieldMergeShapes::default();
        for (row, fields) in rows.iter_mut().zip(values) {
            row.merge_loaded_dynamic_fields(fields, &mut shapes)?;
        }
        Ok(())
    }
}

pub type DynamicStorageFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DynamicFieldError>> + Send + 'a>>;
pub type DynamicStorageRows = HashMap<u64, HashMap<String, Value>>;

/// A trusted provider chooses this opaque storage namespace, never a request field.
/// Application tenancy/roles remain outside the runtime's model-independent core.
#[derive(Clone)]
pub struct DynamicFieldWrite {
    pub namespace: String,
    pub definitions: Arc<DynamicFieldDefinitions>,
    pub owner_id: u64,
    pub changes: BTreeMap<String, DynamicFieldMutation>,
}

impl std::fmt::Debug for DynamicFieldWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicFieldWrite")
            .field("owner_type", &self.definitions.owner_type())
            .field("changed_fields", &self.changes.keys())
            .finish_non_exhaustive()
    }
}

/// Low-level executor SPI. Runtime graph save, not application code, consumes it.
/// Writes are supported only by the exclusive native transaction handle.
#[doc(hidden)]
pub trait DynamicFieldStore: Send + Sync {
    fn is_transaction_bound(&self) -> bool;

    /// Opaque operation-binding identity. Compare only; never serialize/log it.
    /// Clones share it, but separately constructed executors must not, even if
    /// they borrow the same underlying database connection.
    fn binding_key(&self) -> usize;

    /// Non-reused instance identity for views that can outlive the original executor.
    /// Missing identity fails closed for durable view provenance.
    fn source_identity(&self) -> Option<u64> {
        None
    }

    fn ensure_schema(&self) -> DynamicStorageFuture<'_, ()>;

    fn load_values<'a>(
        &'a self,
        namespace: &'a str,
        definitions: &'a DynamicFieldDefinitions,
        ids: &'a [u64],
        selection: &'a DynamicFieldSelection,
        intent: &'a QueryIntent,
    ) -> DynamicStorageFuture<'a, DynamicStorageRows>;

    /// Validate the entire batch before the graph issues any native write.
    fn validate_writes(&self, writes: &[DynamicFieldWrite]) -> Result<(), DynamicFieldError>;

    fn apply_writes<'a>(
        &'a self,
        writes: &'a [DynamicFieldWrite],
        intent: &'a MutationIntent,
    ) -> DynamicStorageFuture<'a, ()>;
}
