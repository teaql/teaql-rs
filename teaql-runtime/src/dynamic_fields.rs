//! Context-owned persistent-extension SPI. Loading is not mutation and owns no ledger.

use crate::UserContext;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use teaql_core::dynamic_fields::{
    DynamicFieldDefinitions, DynamicFieldError, DynamicFieldSelection, DynamicFieldValues,
};
use teaql_core::{QueryIntent, Value};
use teaql_data_service::dynamic_fields::{DynamicFieldStore, DynamicFieldWrite};

pub struct DynamicFieldBatch {
    pub definitions: Arc<DynamicFieldDefinitions>,
    /// Every requested owner must have an entry, even when no stored extension exists.
    pub rows: HashMap<u64, HashMap<String, Value>>,
}

impl DynamicFieldBatch {
    /// One validation contract for root and relation batches. Duplicate views
    /// keep private payloads while definitions and selected-code metadata share.
    pub(crate) fn into_values(
        mut self,
        owner_type: &str,
        ids: &[u64],
        selection: &DynamicFieldSelection,
    ) -> Result<Vec<DynamicFieldValues>, DynamicFieldError> {
        let invalid = |code| DynamicFieldError {
            code,
            field: owner_type.to_owned(),
        };
        if self.definitions.owner_type() != owner_type {
            return Err(invalid("DYNAMIC_FIELD_OWNER_MISMATCH"));
        }
        selection.validate(&self.definitions)?;
        let mut occurrences = HashMap::new();
        for id in ids {
            *occurrences.entry(*id).or_insert(0_usize) += 1;
        }
        if self.rows.keys().any(|id| !occurrences.contains_key(id)) {
            return Err(invalid("DYNAMIC_FIELD_UNREQUESTED_OWNER"));
        }
        let mut payloads = Vec::with_capacity(ids.len());
        for id in ids {
            let remaining = occurrences.get_mut(id).expect("requested owner");
            *remaining -= 1;
            let values = if *remaining == 0 {
                self.rows.remove(id)
            } else {
                self.rows.get(id).cloned()
            }
            .ok_or_else(|| invalid("DYNAMIC_FIELD_BATCH_OMITTED_OWNER"))?;
            if values.keys().any(|code| !selection.contains(code)) {
                return Err(invalid("DYNAMIC_FIELD_UNSELECTED_VALUE"));
            }
            payloads.push(values);
        }
        DynamicFieldValues::from_batch(self.definitions, payloads)
    }
}

pub(crate) fn mutation_audit_value(
    mutation: &teaql_core::dynamic_fields::DynamicFieldMutation,
    actual: Option<&Value>,
) -> Value {
    use teaql_core::dynamic_fields::DynamicFieldMutation;
    let mut details = BTreeMap::new();
    match mutation {
        DynamicFieldMutation::Set(proposed) => {
            details.insert("operation".into(), Value::Text("SET".into()));
            details.insert("value".into(), actual.unwrap_or(proposed).clone());
        }
        DynamicFieldMutation::Delete => {
            details.insert("operation".into(), Value::Text("DELETE".into()));
        }
    }
    Value::Object(details)
}

#[async_trait::async_trait]
pub trait DynamicFieldsProvider: Send + Sync {
    fn supports_graph_save(&self) -> bool {
        false
    }

    /// Resolve trusted configuration and verify binding before any native DML.
    fn prepare_graph_write(
        &self,
        _context: &UserContext,
        _store: &dyn DynamicFieldStore,
        _definitions: &Arc<DynamicFieldDefinitions>,
        _owner_id: u64,
        _changes: &BTreeMap<String, teaql_core::dynamic_fields::DynamicFieldMutation>,
    ) -> Result<DynamicFieldWrite, DynamicFieldError> {
        Err(DynamicFieldError {
            code: "DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED",
            field: "save".into(),
        })
    }
    /// Context-owned schema initialization. Immutable fixture providers need no DDL.
    async fn ensure_schema(&self, _context: &UserContext) -> Result<(), DynamicFieldError> {
        Ok(())
    }
    async fn load_values(
        &self,
        context: &UserContext,
        owner_type: &str,
        ids: &[u64],
        selection: &DynamicFieldSelection,
        intent: &QueryIntent,
    ) -> Result<DynamicFieldBatch, DynamicFieldError>;
}

struct DynamicFieldsProviderResource(Arc<dyn DynamicFieldsProvider>);

impl UserContext {
    pub fn set_dynamic_fields_provider(&mut self, provider: Arc<dyn DynamicFieldsProvider>) {
        self.insert_resource(DynamicFieldsProviderResource(provider));
    }

    pub(crate) fn dynamic_graph_provider(
        &self,
    ) -> Result<&dyn DynamicFieldsProvider, crate::RuntimeError> {
        self.get_resource::<DynamicFieldsProviderResource>()
            .filter(|provider| provider.0.supports_graph_save())
            .map(|provider| provider.0.as_ref())
            .ok_or_else(|| {
                crate::RuntimeError::DynamicField(DynamicFieldError {
                    code: "DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED",
                    field: "save".into(),
                })
            })
    }

    pub(crate) async fn load_dynamic_fields(
        &self,
        owner_type: &str,
        ids: &[u64],
        selection: &DynamicFieldSelection,
        intent: &QueryIntent,
    ) -> Result<DynamicFieldBatch, DynamicFieldError> {
        let provider = self
            .get_resource::<DynamicFieldsProviderResource>()
            .ok_or_else(|| DynamicFieldError {
                code: "DYNAMIC_FIELD_PROVIDER_MISSING",
                field: owner_type.to_owned(),
            })?;
        provider
            .0
            .load_values(self, owner_type, ids, selection, intent)
            .await
    }

    pub(crate) async fn ensure_dynamic_fields_schema(&self) -> Result<(), DynamicFieldError> {
        if let Some(provider) = self.get_resource::<DynamicFieldsProviderResource>() {
            provider.0.ensure_schema(self).await?;
        }
        Ok(())
    }
}

/// Durable query provider using storage exposed by the registered native executor.
/// Definitions are trusted provider configuration, not mutable request data.
/// Graph writes must later consume the *transaction* store, never this ambient one.
pub struct DatabaseDynamicFieldsProvider<E> {
    namespace: String,
    definitions: HashMap<String, Arc<DynamicFieldDefinitions>>,
    _executor: std::marker::PhantomData<fn() -> E>,
}

impl<E> DatabaseDynamicFieldsProvider<E> {
    pub fn new(
        namespace: impl Into<String>,
        definitions: impl IntoIterator<Item = Arc<DynamicFieldDefinitions>>,
    ) -> Result<Self, DynamicFieldError> {
        let namespace = namespace.into();
        if namespace.trim().is_empty() {
            return Err(DynamicFieldError {
                code: "DYNAMIC_FIELD_NAMESPACE_REQUIRED",
                field: "namespace".into(),
            });
        }
        let mut owners = HashMap::new();
        for definition in definitions {
            let owner = definition.owner_type().to_owned();
            if owners.insert(owner.clone(), definition).is_some() {
                return Err(DynamicFieldError {
                    code: "DYNAMIC_FIELD_DUPLICATE_OWNER",
                    field: owner,
                });
            }
        }
        Ok(Self {
            namespace,
            definitions: owners,
            _executor: std::marker::PhantomData,
        })
    }
}

impl<E: teaql_data_service::QueryExecutor + Send + Sync + 'static>
    DatabaseDynamicFieldsProvider<E>
{
    /// Context-bound metadata for new entities; no data query or implicit fetch.
    pub fn definitions_for_context(
        &self,
        context: &UserContext,
        owner_type: &str,
    ) -> Result<Arc<DynamicFieldDefinitions>, DynamicFieldError> {
        let invalid = |code| DynamicFieldError {
            code,
            field: owner_type.into(),
        };
        let definitions = self
            .definitions
            .get(owner_type)
            .ok_or_else(|| invalid("DYNAMIC_FIELD_DEFINITIONS_MISSING"))?;
        let executor = context
            .require_resource::<E>()
            .map_err(|_| invalid("DYNAMIC_FIELD_EXECUTOR_MISSING"))?;
        let store = executor
            .dynamic_field_store()
            .ok_or_else(|| invalid("DYNAMIC_FIELD_STORAGE_UNSUPPORTED"))?;
        let identity = store
            .source_identity()
            .ok_or_else(|| invalid("DYNAMIC_FIELD_STORAGE_IDENTITY_REQUIRED"))?;
        Ok(definitions.with_storage_binding(&self.namespace, identity))
    }
}

#[async_trait::async_trait]
impl<E> DynamicFieldsProvider for DatabaseDynamicFieldsProvider<E>
where
    E: teaql_data_service::QueryExecutor + Send + Sync + 'static,
{
    fn supports_graph_save(&self) -> bool {
        true
    }

    fn prepare_graph_write(
        &self,
        context: &UserContext,
        store: &dyn DynamicFieldStore,
        expected: &Arc<DynamicFieldDefinitions>,
        owner_id: u64,
        changes: &BTreeMap<String, teaql_core::dynamic_fields::DynamicFieldMutation>,
    ) -> Result<DynamicFieldWrite, DynamicFieldError> {
        let invalid = |code| DynamicFieldError {
            code,
            field: expected.owner_type().into(),
        };
        let definitions = self
            .definitions
            .get(expected.owner_type())
            .ok_or_else(|| invalid("DYNAMIC_FIELD_DEFINITIONS_MISSING"))?;
        if !definitions.same_schema(expected) {
            return Err(invalid("DYNAMIC_FIELD_DEFINITION_MISMATCH"));
        }
        let ambient = context
            .require_resource::<E>()
            .map_err(|_| invalid("DYNAMIC_FIELD_EXECUTOR_MISSING"))?
            .dynamic_field_store()
            .ok_or_else(|| invalid("DYNAMIC_FIELD_STORAGE_UNSUPPORTED"))?;
        if !store.is_transaction_bound() || ambient.binding_key() != store.binding_key() {
            return Err(invalid("DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED"));
        }
        let identity = ambient
            .source_identity()
            .ok_or_else(|| invalid("DYNAMIC_FIELD_STORAGE_IDENTITY_REQUIRED"))?;
        if !expected.storage_binding_matches(&self.namespace, identity) {
            return Err(invalid("DYNAMIC_FIELD_STORAGE_PROVENANCE_MISMATCH"));
        }
        Ok(DynamicFieldWrite {
            namespace: self.namespace.clone(),
            definitions: expected.clone(),
            owner_id,
            changes: changes.clone(),
        })
    }
    async fn ensure_schema(&self, context: &UserContext) -> Result<(), DynamicFieldError> {
        let executor = context
            .require_resource::<E>()
            .map_err(|_| DynamicFieldError {
                code: "DYNAMIC_FIELD_EXECUTOR_MISSING",
                field: "schema".into(),
            })?;
        let store = executor
            .dynamic_field_store()
            .ok_or_else(|| DynamicFieldError {
                code: "DYNAMIC_FIELD_STORAGE_UNSUPPORTED",
                field: "schema".into(),
            })?;
        store.ensure_schema().await
    }

    async fn load_values(
        &self,
        context: &UserContext,
        owner_type: &str,
        ids: &[u64],
        selection: &DynamicFieldSelection,
        intent: &QueryIntent,
    ) -> Result<DynamicFieldBatch, DynamicFieldError> {
        let definitions =
            self.definitions
                .get(owner_type)
                .cloned()
                .ok_or_else(|| DynamicFieldError {
                    code: "DYNAMIC_FIELD_DEFINITIONS_MISSING",
                    field: owner_type.into(),
                })?;
        selection.validate(&definitions)?;
        let executor = context
            .require_resource::<E>()
            .map_err(|_| DynamicFieldError {
                code: "DYNAMIC_FIELD_EXECUTOR_MISSING",
                field: owner_type.into(),
            })?;
        let store = executor
            .dynamic_field_store()
            .ok_or_else(|| DynamicFieldError {
                code: "DYNAMIC_FIELD_STORAGE_UNSUPPORTED",
                field: owner_type.into(),
            })?;
        let rows = store
            .load_values(&self.namespace, &definitions, ids, selection, intent)
            .await?;
        let identity = store.source_identity().ok_or_else(|| DynamicFieldError {
            code: "DYNAMIC_FIELD_STORAGE_IDENTITY_REQUIRED",
            field: owner_type.into(),
        })?;
        let definitions = definitions.with_storage_binding(&self.namespace, identity);
        Ok(DynamicFieldBatch { definitions, rows })
    }
}

/// Immutable in-memory fixtures; not durable storage or an alternate entity-save API.
pub struct InMemoryDynamicFieldsProvider {
    definitions: HashMap<String, Arc<DynamicFieldDefinitions>>,
    values: HashMap<String, HashMap<u64, HashMap<String, Value>>>,
}

impl InMemoryDynamicFieldsProvider {
    pub fn from_owners(
        owners: impl IntoIterator<
            Item = (
                Arc<DynamicFieldDefinitions>,
                HashMap<u64, HashMap<String, Value>>,
            ),
        >,
    ) -> Result<Self, DynamicFieldError> {
        let mut provider = Self {
            definitions: HashMap::new(),
            values: HashMap::new(),
        };
        for (definitions, rows) in owners {
            let owner = definitions.owner_type().to_owned();
            if provider
                .definitions
                .insert(owner.clone(), definitions.clone())
                .is_some()
            {
                return Err(DynamicFieldError {
                    code: "DYNAMIC_FIELD_DUPLICATE_OWNER",
                    field: owner,
                });
            }
            let stored = provider.values.entry(owner).or_default();
            for (id, values) in rows {
                let fields = DynamicFieldValues::from_values(definitions.clone(), values)?;
                stored.insert(id, fields.into_values());
            }
        }
        Ok(provider)
    }
}

#[async_trait::async_trait]
impl DynamicFieldsProvider for InMemoryDynamicFieldsProvider {
    async fn load_values(
        &self,
        _context: &UserContext,
        owner_type: &str,
        ids: &[u64],
        selection: &DynamicFieldSelection,
        _intent: &QueryIntent,
    ) -> Result<DynamicFieldBatch, DynamicFieldError> {
        let definitions =
            self.definitions
                .get(owner_type)
                .cloned()
                .ok_or_else(|| DynamicFieldError {
                    code: "DYNAMIC_FIELD_DEFINITIONS_MISSING",
                    field: owner_type.to_owned(),
                })?;
        selection.validate(&definitions)?;
        let stored = self.values.get(owner_type);
        let mut rows = HashMap::with_capacity(ids.len());
        for &id in ids {
            if let std::collections::hash_map::Entry::Vacant(entry) = rows.entry(id) {
                let values = stored
                    .and_then(|rows| rows.get(&id))
                    .map(|values| {
                        values
                            .iter()
                            .filter(|(code, _)| selection.contains(code))
                            .map(|(code, value)| (code.clone(), value.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                entry.insert(values);
            }
        }
        Ok(DynamicFieldBatch { definitions, rows })
    }
}
