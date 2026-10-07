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
