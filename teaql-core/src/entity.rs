use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CompactRow, Decimal, EntityDescriptor, EntitySnapshot, MutationIntent, MutationValues, Value,
    record_to_json_value,
};

/// A presentation-only, path-local guard. Never touches an entity's ledger.
#[doc(hidden)]
#[derive(Default)]
pub struct EntityJsonTraversal {
    path: Vec<(&'static str, usize)>,
}

impl EntityJsonTraversal {
    pub fn enter(&mut self, entity: &'static str, address: usize) -> bool {
        let expand = self.path.len() < 128 && !self.path.contains(&(entity, address));
        self.path.push((entity, address));
        expand
    }

    pub fn leave(&mut self) {
        self.path.pop();
    }
}

pub trait TeaqlEntity {
    const ENTITY_NAME: &'static str;

    fn entity_descriptor() -> EntityDescriptor;

    /// Fixed metadata supplied by generation; hand-written adapters may remain unindexed.
    fn field_layout() -> Result<Option<std::sync::Arc<crate::FieldLayout>>, EntityError> {
        Ok(None)
    }

    fn register_into(store: &mut impl EntityDescriptorStore) {
        store.register_descriptor(Self::entity_descriptor());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityError {
    pub entity: String,
    pub message: String,
}

impl EntityError {
    pub fn new(entity: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for EntityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.entity, self.message)
    }
}

impl std::error::Error for EntityError {}

pub trait Entity: TeaqlEntity + Sized {
    /// Optional readonly schema: a known type does not make a missing property present.
    fn dynamic_property_type(&self, key: &str) -> Option<crate::DataType> {
        let state = self.loaded_state_snapshot()?;
        state.dynamic_property_definitions()?.data_type(key)
    }
    /// Framework JSON hydration capability; readonly properties remain unnumbered data.
    #[doc(hidden)]
    fn supports_dynamic_property_load() -> bool {
        false
    }

    /// Borrow a readonly derived property using its full `_`-prefixed name.
    /// Missing properties and explicit null both return `None`; real zero,
    /// false and empty text remain values. This never loads data or changes state.
    fn dynamic_property(&self, _key: &str) -> Option<&Value> {
        None
    }

    /// Presence is separate from the nullable derived-property read contract.
    /// An explicit null is present; an absent key is not. Native fields and
    /// persistent `#` extensions are not part of this namespace.
    fn has_dynamic_property(&self, _key: &str) -> bool {
        false
    }

    /// Runtime-owned persistent-extension carrier, separate from readonly dynamic properties.
    fn dynamic_field_values(&self) -> Option<&crate::dynamic_fields::DynamicFieldValues> {
        None
    }

    fn has_pending_dynamic_mutations(&self) -> bool {
        false
    }

    /// Type-erased framework bridge without a core -> runtime dependency.
    #[doc(hidden)]
    fn __teaql_runtime_state_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    fn update_dynamic_field(
        &mut self,
        code: &str,
        _value: Value,
    ) -> Result<(), crate::dynamic_fields::DynamicFieldError> {
        Err(crate::dynamic_fields::DynamicFieldError {
            code: "DYNAMIC_FIELD_CARRIER_MISSING",
            field: code.to_owned(),
        })
    }

    fn delete_dynamic_field(
        &mut self,
        code: &str,
    ) -> Result<(), crate::dynamic_fields::DynamicFieldError> {
        Err(crate::dynamic_fields::DynamicFieldError {
            code: "DYNAMIC_FIELD_CARRIER_MISSING",
            field: code.to_owned(),
        })
    }

    fn supports_dynamic_field_load() -> bool {
        false
    }

    fn loaded_state_snapshot(&self) -> Option<std::sync::Arc<crate::LoadedSnapshot>> {
        None
    }

    /// Framework hydration only: it must not create mutation intent or share row payloads.
    #[doc(hidden)]
    fn install_loaded_dynamic_fields(
        &mut self,
        _values: crate::dynamic_fields::DynamicFieldValues,
        _state: std::sync::Arc<crate::LoadedSnapshot>,
    ) -> Result<(), EntityError> {
        Err(EntityError::new(
            Self::ENTITY_NAME,
            "entity lacks the runtime-owned indexed dynamic-field carrier",
        ))
    }

    fn from_compact_row(row: CompactRow) -> Result<Self, EntityError>;

    fn from_compact_row_with_context(
        row: CompactRow,
        context: &dyn std::any::Any,
    ) -> Result<Self, EntityError> {
        let mut entity = Self::from_compact_row(row)?;
        entity.on_loaded(context);
        Ok(entity)
    }

    fn into_values(self) -> MutationValues;

    /// Presentation bridge for macro-generated typed relation carriers.
    /// Kept separate so JSON conversion cannot change the mutation contract.
    #[doc(hidden)]
    fn into_json_values(self) -> BTreeMap<String, Value> {
        self.into_values().into()
    }

    /// Borrow an indexed graph view without cloning entities or triggering I/O.
    /// Older/manual carriers retain their existing consuming presentation path.
    #[doc(hidden)]
    fn borrowed_json(&self, _traversal: &mut EntityJsonTraversal) -> Option<serde_json::Value> {
        None
    }

    /// Whether a model field was loaded in the entity snapshot used for a
    /// mutation. Implementations without projection tracking remain fully
    /// loaded for backwards compatibility.
    fn is_field_loaded(&self, _field: &str) -> bool {
        true
    }

    /// Restore the load boundary while materializing the typed Checker view.
    /// This is runtime metadata, not mutation intent.
    fn set_checker_loaded_fields(&mut self, _fields: BTreeSet<String>) {}

    /// Restore the caller's exact mutation boundary while materializing the
    /// typed Checker view. This metadata is used only during validation and
    /// must not widen the eventual database update.
    fn set_checker_dirty_fields(&mut self, _fields: BTreeSet<String>, _values: &MutationValues) {}

    /// Returns the set of field names that have been modified since the entity was loaded.
    /// Returns `None` if dirty tracking is not available (backwards compatible default).
    /// This is the Rust equivalent of Java's `entity.getUpdatedProperties()`.
    fn dirty_fields(&self) -> Option<BTreeSet<String>> {
        None
    }

    /// Returns true if this entity has been marked for deletion.
    fn is_marked_as_delete(&self) -> bool {
        false
    }

    /// Returns true if this entity was explicitly constructed as a new entity.
    fn is_new(&self) -> bool {
        false
    }

    /// Mark this entity as a newly created entity, bypassing database existence checks.
    fn mark_as_new(&mut self) {}

    /// Get the annotation comment, if any.
    fn get_comment(&self) -> Option<String> {
        None
    }

    /// Set an annotation comment for this entity instance.
    fn set_comment(&mut self, _comment: String) {}

    /// Attach an audit comment and return a `Commented<Self>` wrapper.
    /// This is the only way to unlock the `.save()` method.
    fn audit_as(self, comment: impl Into<String>) -> Audited<Self> {
        Audited::new(self, comment)
    }

    /// Get the original snapshot values when this entity was loaded from the repository, if available.
    fn original_values(&self) -> Option<EntitySnapshot> {
        None
    }

    /// Invoked immediately after the entity is loaded from the repository.
    /// Used by implementations to attach runtime contexts or initialize internal states.
    #[allow(unused_variables)]
    fn on_loaded(&mut self, context: &dyn std::any::Any) {}

    fn into_json(self) -> serde_json::Value {
        if let Some(json) = self.borrowed_json(&mut EntityJsonTraversal::default()) {
            return json;
        }
        // Persistent extensions are a separate carrier, never native mutation
        // columns. Serialize only supplied values; absent definitions stay absent.
        let extensions: serde_json::Map<String, serde_json::Value> = self
            .dynamic_field_values()
            .map(|fields| {
                fields
                    .values()
                    .iter()
                    .map(|(code, value)| (format!("#{code}"), value.to_json_value()))
                    .collect()
            })
            .unwrap_or_default();
        let mut values = self.into_json_values();
        // These exact framework-owned keys belong to the mutation/checker
        // contract, not to an external entity representation. Other `_` keys
        // are legitimate readonly dynamic properties and must be preserved.
        for key in [
            "_comment",
            "_dirty_fields",
            "_original_values",
            "_is_new",
            "_is_deleted",
        ] {
            values.remove(key);
        }
        let mut json = record_to_json_value(&values);
        json.as_object_mut()
            .expect("entity serialization is an object")
            .extend(extensions);
        json
    }
}

/// A wrapper that carries a mandatory audit comment with an entity.
/// Only `Audited<T>` has a `.save()` method — bare entities cannot be saved directly.
/// This enforces the "must comment on save" policy at compile time.
pub struct Audited<T: Entity> {
    inner: T,
    comment: MutationIntent,
}

impl<T: Entity> Audited<T> {
    /// Create an audited wrapper with a validated, request-owned root reason.
    ///
    /// The fluent constructor remains infallible in its type signature, but
    /// invalid input panics with the same code, location and repair guidance as
    /// `MutationIntent`. Callers handling untrusted input can validate it with
    /// `MutationIntent::new` before constructing this wrapper.
    pub fn new(entity: T, comment: impl Into<String>) -> Self {
        let comment = MutationIntent::new(comment).unwrap_or_else(|error| panic!("{error}"));
        Self {
            inner: entity,
            comment,
        }
    }

    /// Access the inner entity by reference.
    pub fn entity(&self) -> &T {
        &self.inner
    }

    /// Access the inner entity by mutable reference.
    pub fn entity_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Consume and return the inner entity with comment applied.
    pub fn into_entity(self) -> T {
        let mut entity = self.inner;
        entity.set_comment(self.comment.into_comment());
        entity
    }

    /// Get the comment.
    pub fn get_comment(&self) -> &str {
        self.comment.comment()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct BaseEntityData {
    pub id: u64,
    pub version: i64,
    pub dynamic: BTreeMap<String, Value>,
}

impl BaseEntityData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: u64) -> Self {
        self.id = id;
        self
    }

    pub fn with_version(mut self, version: i64) -> Self {
        self.version = version;
        self
    }

    pub fn with_dynamic(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.dynamic.insert(key.into(), value.into());
        self
    }

    pub fn dynamic(&self, key: &str) -> Option<&Value> {
        self.dynamic.get(key)
    }

    pub fn dynamic_i64(&self, key: &str) -> Option<i64> {
        self.dynamic(key).and_then(Value::try_i64)
    }

    pub fn dynamic_u64(&self, key: &str) -> Option<u64> {
        self.dynamic(key).and_then(Value::try_u64)
    }

    pub fn dynamic_decimal(&self, key: &str) -> Option<Decimal> {
        self.dynamic(key).and_then(Value::try_decimal)
    }

    pub fn dynamic_f64(&self, key: &str) -> Option<f64> {
        self.dynamic(key).and_then(Value::try_f64)
    }

    pub fn dynamic_text(&self, key: &str) -> Option<&str> {
        self.dynamic(key).and_then(Value::try_text)
    }

    pub fn dynamic_bool(&self, key: &str) -> Option<bool> {
        self.dynamic(key).and_then(Value::try_bool)
    }

    pub fn put_dynamic(
        &mut self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Option<Value> {
        self.dynamic.insert(key.into(), value.into())
    }

    pub fn remove_dynamic(&mut self, key: &str) -> Option<Value> {
        self.dynamic.remove(key)
    }

    pub fn to_values_map(&self) -> BTreeMap<String, Value> {
        let mut values = BTreeMap::new();
        values.insert("id".to_owned(), Value::U64(self.id));
        values.insert("version".to_owned(), Value::I64(self.version));
        for (key, value) in &self.dynamic {
            values.insert(key.clone(), value.clone());
        }
        values
    }

    pub fn from_values_map(values: &BTreeMap<String, Value>) -> Result<Self, EntityError> {
        let id = match values.get("id") {
            Some(Value::U64(v)) => *v,
            Some(Value::I64(v)) if *v >= 0 => *v as u64,
            Some(Value::Null) | None => 0,
            other => {
                return Err(EntityError::new(
                    "BaseEntity",
                    format!("invalid id field: {other:?}"),
                ));
            }
        };

        let version = match values.get("version") {
            Some(Value::I64(v)) => *v,
            Some(Value::Null) | None => 0,
            other => {
                return Err(EntityError::new(
                    "BaseEntity",
                    format!("invalid version field: {other:?}"),
                ));
            }
        };

        let dynamic = values
            .iter()
            .filter(|(key, _)| key.as_str() != "id" && key.as_str() != "version")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

        Ok(Self {
            id,
            version,
            dynamic,
        })
    }
}

pub trait BaseEntity: Entity {
    fn base(&self) -> &BaseEntityData;
    fn base_mut(&mut self) -> &mut BaseEntityData;

    fn id(&self) -> u64 {
        self.base().id
    }

    fn set_id(&mut self, id: u64) {
        self.base_mut().id = id;
    }

    fn version_value(&self) -> i64 {
        self.base().version
    }

    fn set_version(&mut self, version: i64) {
        self.base_mut().version = version;
    }

    fn dynamic(&self, key: &str) -> Option<&Value> {
        self.base().dynamic(key)
    }

    fn dynamic_i64(&self, key: &str) -> Option<i64> {
        self.base().dynamic_i64(key)
    }

    fn dynamic_u64(&self, key: &str) -> Option<u64> {
        self.base().dynamic_u64(key)
    }

    fn dynamic_decimal(&self, key: &str) -> Option<Decimal> {
        self.base().dynamic_decimal(key)
    }

    fn dynamic_f64(&self, key: &str) -> Option<f64> {
        self.base().dynamic_f64(key)
    }

    fn dynamic_text(&self, key: &str) -> Option<&str> {
        self.base().dynamic_text(key)
    }

    fn dynamic_bool(&self, key: &str) -> Option<bool> {
        self.base().dynamic_bool(key)
    }

    fn put_dynamic(&mut self, key: impl Into<String>, value: impl Into<Value>) -> Option<Value> {
        self.base_mut().put_dynamic(key, value)
    }
}

pub trait IdentifiableEntity: Entity {
    fn id_value(&self) -> Value;
}

pub trait VersionedEntity: Entity {
    fn version(&self) -> i64;
}

pub trait TeaqlBoxedRelations: Sized {
    fn extend_descriptor(descriptor: &mut EntityDescriptor);
    fn extract_from_values(values: &CompactRow) -> Result<Self, EntityError>;
    fn inject_into_values(self, values: &mut BTreeMap<String, Value>);
    #[doc(hidden)]
    fn inject_into_json_values(
        self,
        values: &mut BTreeMap<String, Value>,
        _loaded: Option<&crate::eval::LoadState>,
    ) {
        self.inject_into_values(values);
    }
}

impl<T: TeaqlBoxedRelations> TeaqlBoxedRelations for Box<T> {
    fn extend_descriptor(descriptor: &mut EntityDescriptor) {
        T::extend_descriptor(descriptor);
    }
    fn extract_from_values(values: &CompactRow) -> Result<Self, EntityError> {
        Ok(Box::new(T::extract_from_values(values)?))
    }
    fn inject_into_values(self, values: &mut BTreeMap<String, Value>) {
        (*self).inject_into_values(values);
    }
    fn inject_into_json_values(
        self,
        values: &mut BTreeMap<String, Value>,
        loaded: Option<&crate::eval::LoadState>,
    ) {
        (*self).inject_into_json_values(values, loaded);
    }
}

pub trait EntityDescriptorStore {
    fn register_descriptor(&mut self, descriptor: EntityDescriptor);
}

#[macro_export]
macro_rules! register_entities {
    ($store:expr, $($entity:ty),+ $(,)?) => {{
        $(
            <$entity as $crate::TeaqlEntity>::register_into($store);
        )+
    }};
}
