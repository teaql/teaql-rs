use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use crate::{Record, Value};

/// Field values intentionally supplied to a database mutation.
///
/// This is distinct from a loaded entity snapshot and from provider-generated
/// values. Keeping the types separate prevents a generic row map from becoming
/// the runtime's mutation model.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MutationValues(BTreeMap<String, Value>);

impl MutationValues {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Deref for MutationValues {
    type Target = BTreeMap<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MutationValues {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Record> for MutationValues {
    fn from(values: Record) -> Self {
        Self(values)
    }
}

impl From<MutationValues> for Record {
    fn from(values: MutationValues) -> Self {
        values.0
    }
}

impl IntoIterator for MutationValues {
    type Item = (String, Value);
    type IntoIter = std::collections::btree_map::IntoIter<String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a MutationValues {
    type Item = (&'a String, &'a Value);
    type IntoIter = std::collections::btree_map::Iter<'a, String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Values assigned by a persistence provider, such as generated identifiers
/// or database defaults. They are provider output, not mutation input.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GeneratedValues(BTreeMap<String, Value>);

impl GeneratedValues {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Deref for GeneratedValues {
    type Target = BTreeMap<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for GeneratedValues {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Record> for GeneratedValues {
    fn from(values: Record) -> Self {
        Self(values)
    }
}

impl From<GeneratedValues> for Record {
    fn from(values: GeneratedValues) -> Self {
        values.0
    }
}

/// Previously persisted field values used for optimistic checks and audit
/// comparison. It cannot be passed where new mutation values are expected.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EntitySnapshot(BTreeMap<String, Value>);

impl EntitySnapshot {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Deref for EntitySnapshot {
    type Target = BTreeMap<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for EntitySnapshot {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Record> for EntitySnapshot {
    fn from(values: Record) -> Self {
        Self(values)
    }
}

impl From<EntitySnapshot> for Record {
    fn from(values: EntitySnapshot) -> Self {
        values.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    Insert,
    Update,
    Delete,
    Recover,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertCommand {
    pub entity: String,
    pub values: MutationValues,
    pub trace_chain: Vec<crate::TraceNode>,
}

impl InsertCommand {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            values: MutationValues::new(),
            trace_chain: Vec::new(),
        }
    }

    pub fn value(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.values.insert(field.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateCommand {
    pub entity: String,
    pub id: Value,
    pub expected_version: Option<i64>,
    pub values: MutationValues,
    pub trace_chain: Vec<crate::TraceNode>,
    pub old_values: Option<EntitySnapshot>,
}

impl UpdateCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version: None,
            values: MutationValues::new(),
            trace_chain: Vec::new(),
            old_values: None,
        }
    }

    pub fn expected_version(mut self, version: i64) -> Self {
        self.expected_version = Some(version);
        self
    }

    pub fn value(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.values.insert(field.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchInsertCommand {
    pub entity: String,
    pub batch_values: Vec<MutationValues>,
    pub trace_chains: Vec<Vec<crate::TraceNode>>,
}

impl BatchInsertCommand {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            batch_values: Vec::new(),
            trace_chains: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchUpdateCommand {
    pub entity: String,
    pub batch_ids: Vec<Value>,
    pub batch_expected_versions: Vec<Option<i64>>,
    pub batch_values: Vec<MutationValues>,
    pub update_fields: Vec<String>,
    pub trace_chains: Vec<Vec<crate::TraceNode>>,
    pub batch_old_values: Vec<Option<EntitySnapshot>>,
}

impl BatchUpdateCommand {
    pub fn new(entity: impl Into<String>, update_fields: Vec<String>) -> Self {
        Self {
            entity: entity.into(),
            batch_ids: Vec::new(),
            batch_expected_versions: Vec::new(),
            batch_values: Vec::new(),
            update_fields,
            trace_chains: Vec::new(),
            batch_old_values: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteCommand {
    pub entity: String,
    pub id: Value,
    pub expected_version: Option<i64>,
    pub soft_delete: bool,
    pub trace_chain: Vec<crate::TraceNode>,
}

impl DeleteCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version: None,
            soft_delete: true,
            trace_chain: Vec::new(),
        }
    }

    pub fn expected_version(mut self, version: i64) -> Self {
        self.expected_version = Some(version);
        self
    }

    pub fn hard_delete(mut self) -> Self {
        self.soft_delete = false;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoverCommand {
    pub entity: String,
    pub id: Value,
    pub expected_version: i64,
    pub trace_chain: Vec<crate::TraceNode>,
}

impl RecoverCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>, expected_version: i64) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version,
            trace_chain: Vec::new(),
        }
    }
}
