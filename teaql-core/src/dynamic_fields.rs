//! Persistable object extensions. Names are runtime-defined and never receive fixed bit indexes.
//! Readonly computed properties (`_name`) are a different namespace and are not accepted here.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::{DataType, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicFieldError {
    pub code: &'static str,
    pub field: String,
}

impl std::fmt::Display for DynamicFieldError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.field)
    }
}

impl std::error::Error for DynamicFieldError {}

fn error(code: &'static str, field: impl Into<String>) -> DynamicFieldError {
    DynamicFieldError {
        code,
        field: field.into(),
    }
}

/// Runtime names/types, not SQL columns or generated field slots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DynamicFieldSelection {
    All,
    Fields(BTreeMap<String, DataType>),
}

impl DynamicFieldSelection {
    pub fn fields(
        fields: impl IntoIterator<Item = (String, DataType)>,
    ) -> Result<Self, DynamicFieldError> {
        let mut selected = BTreeMap::new();
        for (code, data_type) in fields {
            if code.trim().is_empty() || code.starts_with(['#', '_']) {
                return Err(error("DYNAMIC_FIELD_INVALID_CODE", code));
            }
            if selected.insert(code.clone(), data_type).is_some() {
                return Err(error("DYNAMIC_FIELD_DUPLICATE_CODE", code));
            }
        }
        Ok(Self::Fields(selected))
    }

    pub fn validate(&self, definitions: &DynamicFieldDefinitions) -> Result<(), DynamicFieldError> {
        if let Self::Fields(fields) = self {
            for (code, selected_type) in fields {
                if definitions.data_type(code)? != *selected_type {
                    return Err(error("DYNAMIC_FIELD_TYPE_MISMATCH", code));
                }
            }
        }
        Ok(())
    }

    pub fn contains(&self, code: &str) -> bool {
        match self {
            Self::All => true,
            Self::Fields(fields) => fields.contains_key(code),
        }
    }
}

/// Immutable owner-specific definitions, shared by all compatible row views.
#[derive(Debug, PartialEq, Eq)]
pub struct DynamicFieldDefinitions {
    owner_type: String,
    revision: String,
    fields: Arc<HashMap<String, DataType>>,
    storage_binding: Option<DynamicFieldStorageBinding>,
}

#[derive(PartialEq, Eq)]
struct DynamicFieldStorageBinding {
    profile: String,
    executor: u64,
}

impl std::fmt::Debug for DynamicFieldStorageBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DynamicFieldStorageBinding(<opaque>)")
    }
}

impl DynamicFieldDefinitions {
    pub fn new(
        owner_type: impl Into<String>,
        revision: impl Into<String>,
        fields: impl IntoIterator<Item = (String, DataType)>,
    ) -> Result<Arc<Self>, DynamicFieldError> {
        let owner_type = owner_type.into();
        let revision = revision.into();
        if owner_type.trim().is_empty() || revision.trim().is_empty() {
            return Err(error("DYNAMIC_FIELD_INVALID_DEFINITIONS", owner_type));
        }
        let mut definitions = HashMap::new();
        for (code, data_type) in fields {
            if code.trim().is_empty() || code.starts_with(['#', '_']) {
                return Err(error("DYNAMIC_FIELD_INVALID_CODE", code));
            }
            if definitions.insert(code.clone(), data_type).is_some() {
                return Err(error("DYNAMIC_FIELD_DUPLICATE_CODE", code));
            }
        }
        Ok(Arc::new(Self {
            owner_type,
            revision,
            fields: Arc::new(definitions),
            storage_binding: None,
        }))
    }

    pub fn owner_type(&self) -> &str {
        &self.owner_type
    }
    pub fn revision(&self) -> &str {
        &self.revision
    }
    pub fn fields(&self) -> &HashMap<String, DataType> {
        &self.fields
    }

    /// Trusted provider metadata only; not tenant/permission semantics or wire input.
    #[doc(hidden)]
    pub fn with_storage_binding(self: &Arc<Self>, profile: &str, executor: u64) -> Arc<Self> {
        if self.storage_binding_matches(profile, executor) {
            return self.clone();
        }
        Arc::new(Self {
            owner_type: self.owner_type.clone(),
            revision: self.revision.clone(),
            fields: self.fields.clone(),
            storage_binding: Some(DynamicFieldStorageBinding {
                profile: profile.into(),
                executor,
            }),
        })
    }

    #[doc(hidden)]
    pub fn storage_binding_matches(&self, profile: &str, executor: u64) -> bool {
        self.storage_binding
            .as_ref()
            .is_some_and(|binding| binding.profile == profile && binding.executor == executor)
    }

    #[doc(hidden)]
    pub fn same_schema(&self, other: &Self) -> bool {
        self.owner_type == other.owner_type
            && self.revision == other.revision
            && self.fields == other.fields
    }

    pub fn data_type(&self, code: &str) -> Result<DataType, DynamicFieldError> {
        self.fields
            .get(code)
            .copied()
            .ok_or_else(|| error("DYNAMIC_FIELD_NOT_FOUND", code))
    }

    /// Validate a borrowed operand without cloning its strings/JSON or building
    /// an intermediate row map. Providers use the same type contract as carriers.
    pub fn validate_value(&self, code: &str, value: &Value) -> Result<(), DynamicFieldError> {
        validate_field(self, code, value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicFieldState {
    Value,
    Null,
    NotLoaded,
}

/// Explicit persistence intent. A stored NULL is Set(Null), never Delete or NotLoaded.
#[derive(Debug, Clone, PartialEq)]
pub enum DynamicFieldMutation {
    Set(Value),
    Delete,
}

/// A borrowed wrapper: reading a missing known field allocates no value or string and performs no I/O.
#[derive(Debug, Clone, Copy)]
pub struct DynamicFieldValue<'a> {
    data_type: DataType,
    stored: Option<&'a Value>,
}

impl<'a> DynamicFieldValue<'a> {
    pub fn data_type(&self) -> DataType {
        self.data_type
    }
    pub fn state(&self) -> DynamicFieldState {
        match self.stored {
            None => DynamicFieldState::NotLoaded,
            Some(Value::Null | Value::TypedNull(_)) => DynamicFieldState::Null,
            Some(_) => DynamicFieldState::Value,
        }
    }
    /// Nullable payload does not erase the separate Null / NotLoaded state.
    pub fn value(&self) -> Option<&'a Value> {
        (self.state() == DynamicFieldState::Value)
            .then_some(self.stored)
            .flatten()
    }
    pub fn is_loaded(&self) -> bool {
        self.state() != DynamicFieldState::NotLoaded
    }
}

/// Values belong to the row. Only immutable definitions and selected-name geometry are shared.
#[derive(Debug, Clone, PartialEq)]
pub struct DynamicFieldValues {
    definitions: Arc<DynamicFieldDefinitions>,
    selected_codes: Arc<HashSet<String>>,
    values: HashMap<String, Value>,
}

/// Operation-local, value-free selection unions. Retaining both source Arcs
/// prevents allocator address reuse from mixing unrelated selection shapes.
#[doc(hidden)]
#[derive(Default)]
pub struct DynamicFieldMergeShapes {
    #[allow(clippy::type_complexity)]
    unions: HashMap<
        (usize, usize),
        (
            Arc<HashSet<String>>,
            Arc<HashSet<String>>,
            Arc<HashSet<String>>,
        ),
    >,
}

impl DynamicFieldMergeShapes {
    fn union(
        &mut self,
        left: &Arc<HashSet<String>>,
        right: &Arc<HashSet<String>>,
    ) -> Arc<HashSet<String>> {
        if right.is_subset(left) {
            return left.clone();
        }
        if left.is_subset(right) {
            return right.clone();
        }
        let key = (Arc::as_ptr(left) as usize, Arc::as_ptr(right) as usize);
        self.unions
            .entry(key)
            .or_insert_with(|| {
                (
                    left.clone(),
                    right.clone(),
                    Arc::new(left.union(right).cloned().collect()),
                )
            })
            .2
            .clone()
    }
}

impl DynamicFieldValues {
    #[doc(hidden)]
    pub fn validate_merge(&self, other: &Self) -> Result<(), DynamicFieldError> {
        if !Arc::ptr_eq(&self.definitions, &other.definitions)
            && self.definitions != other.definitions
        {
            return Err(error(
                "DYNAMIC_FIELD_INCOMPATIBLE_VIEW",
                self.definitions.owner_type(),
            ));
        }
        Ok(())
    }

    /// Merge actual loaded values, not requested-but-absent names. Incoming
    /// values win; missing operands never clear an already loaded value.
    #[doc(hidden)]
    pub fn merge_loaded(
        &mut self,
        other: Self,
        shapes: &mut DynamicFieldMergeShapes,
    ) -> Result<(), DynamicFieldError> {
        self.validate_merge(&other)?;
        self.selected_codes = shapes.union(&self.selected_codes, &other.selected_codes);
        self.values.extend(other.values);
        Ok(())
    }
    pub fn from_values(
        definitions: Arc<DynamicFieldDefinitions>,
        values: HashMap<String, Value>,
    ) -> Result<Self, DynamicFieldError> {
        validate_values(&definitions, &values)?;
        let selected_codes = Arc::new(values.keys().cloned().collect());
        Ok(Self {
            definitions,
            selected_codes,
            values,
        })
    }

    /// Build each actual selected-name set once, not one copied HashSet per row.
    /// Missing and loaded-NULL rows may have different shapes; never union their fields.
    pub fn from_batch(
        definitions: Arc<DynamicFieldDefinitions>,
        rows: impl IntoIterator<Item = HashMap<String, Value>>,
    ) -> Result<Vec<Self>, DynamicFieldError> {
        use std::hash::{Hash, Hasher};
        let mut shapes: HashMap<u64, Vec<Arc<HashSet<String>>>> = HashMap::new();
        let mut result = Vec::new();
        for values in rows {
            validate_values(&definitions, &values)?;
            let hash = values.keys().fold(0_u64, |total, code| {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                code.hash(&mut hasher);
                total.wrapping_add(hasher.finish())
            });
            let bucket = shapes.entry(hash).or_default();
            let selected_codes = match bucket.iter().find(|shape| {
                shape.len() == values.len() && values.keys().all(|code| shape.contains(code))
            }) {
                Some(shape) => shape.clone(),
                None => {
                    let shape: Arc<HashSet<String>> = Arc::new(values.keys().cloned().collect());
                    bucket.push(shape.clone());
                    shape
                }
            };
            result.push(Self {
                definitions: definitions.clone(),
                selected_codes,
                values,
            });
        }
        Ok(result)
    }

    pub fn definitions(&self) -> &Arc<DynamicFieldDefinitions> {
        &self.definitions
    }
    pub fn selected_codes(&self) -> &Arc<HashSet<String>> {
        &self.selected_codes
    }
    pub fn values(&self) -> &HashMap<String, Value> {
        &self.values
    }

    pub fn into_values(self) -> HashMap<String, Value> {
        self.values
    }

    /// Row-local update. Existing selected geometry retains its exact Arc reference.
    pub fn assign(&mut self, code: &str, value: Value) -> Result<(), DynamicFieldError> {
        validate_field(&self.definitions, code, &value)?;
        self.values.insert(code.to_owned(), value);
        if !self.selected_codes.contains(code) {
            Arc::make_mut(&mut self.selected_codes).insert(code.to_owned());
        }
        Ok(())
    }

    /// Explicit delete changes this view to NotLoaded; it is distinct from assigning NULL.
    pub fn delete(&mut self, code: &str) -> Result<(), DynamicFieldError> {
        self.definitions.data_type(code)?;
        self.values.remove(code);
        if self.selected_codes.contains(code) {
            Arc::make_mut(&mut self.selected_codes).remove(code);
        }
        Ok(())
    }

    pub fn field(&self, code: &str) -> Result<DynamicFieldValue<'_>, DynamicFieldError> {
        Ok(DynamicFieldValue {
            data_type: self.definitions.data_type(code)?,
            stored: self.values.get(code),
        })
    }
}

fn validate_values(
    definitions: &DynamicFieldDefinitions,
    values: &HashMap<String, Value>,
) -> Result<(), DynamicFieldError> {
    for (code, value) in values {
        validate_field(definitions, code, value)?;
    }
    Ok(())
}

fn validate_field(
    definitions: &DynamicFieldDefinitions,
    code: &str,
    value: &Value,
) -> Result<(), DynamicFieldError> {
    let declared = definitions.data_type(code)?;
    let matches = match value {
        Value::Null => true,
        Value::TypedNull(data_type) => *data_type == declared,
        Value::Bool(_) => declared == DataType::Bool,
        Value::I64(_) => declared == DataType::I64,
        Value::U64(_) => declared == DataType::U64,
        Value::F64(_) => declared == DataType::F64,
        Value::Decimal(_) => declared == DataType::Decimal,
        Value::Text(_) => matches!(declared, DataType::Text | DataType::LargeText),
        Value::Json(_) => declared == DataType::Json,
        Value::Date(_) => declared == DataType::Date,
        Value::Timestamp(_) => declared == DataType::Timestamp,
        Value::Object(_) | Value::List(_) => false,
    };
    if !matches {
        return Err(error("DYNAMIC_FIELD_TYPE_MISMATCH", code));
    }
    Ok(())
}
