use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use crate::Value;

/// Sparse generated field slots with an explicit named compatibility boundary.
/// Cloning generated values copies one compact list, never field names or tree nodes.
#[derive(Debug, Clone)]
pub struct MutationValues(Storage);

#[derive(Debug, Clone)]
enum Storage {
    Named(BTreeMap<String, Value>),
    Indexed(Box<IndexedValues>),
}

#[derive(Debug)]
struct IndexedValues {
    symbols: &'static crate::EntitySymbols,
    values: Vec<(crate::FieldSlot, Value)>,
    extras: BTreeMap<String, Value>,
    // Only legacy APIs requiring an actual BTreeMap pay for this view.
    legacy: std::sync::OnceLock<BTreeMap<String, Value>>,
}

impl Clone for IndexedValues {
    fn clone(&self) -> Self {
        Self {
            symbols: self.symbols,
            values: self.values.clone(),
            extras: self.extras.clone(),
            legacy: Default::default(),
        }
    }
}

/// Owned transport identity. A slot is always accompanied by its generating layout.
/// Equality/order intentionally compare names for legacy batch ordering. Containers
/// validate layout ownership independently before accepting a fixed field.
#[derive(Debug, Clone)]
pub enum MutationField {
    Fixed(&'static crate::EntitySymbols, crate::FieldSlot),
    Named(String),
}
impl MutationField {
    pub fn name(&self) -> &str {
        match self {
            Self::Fixed(symbols, slot) => symbols.field(*slot).expect("validated field slot"),
            Self::Named(name) => name,
        }
    }
}

impl From<String> for MutationField {
    fn from(name: String) -> Self {
        Self::Named(name)
    }
}
impl From<&str> for MutationField {
    fn from(name: &str) -> Self {
        Self::Named(name.to_owned())
    }
}
impl PartialEq for MutationField {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}
impl Eq for MutationField {}
impl PartialOrd for MutationField {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MutationField {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name().cmp(other.name())
    }
}

impl Default for MutationValues {
    fn default() -> Self {
        Self(Storage::Named(BTreeMap::new()))
    }
}
impl PartialEq for MutationValues {
    fn eq(&self, other: &Self) -> bool {
        self.named_entries().eq(other.named_entries())
    }
}

impl MutationValues {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn indexed(symbols: &'static crate::EntitySymbols) -> Self {
        Self::indexed_with_capacity(symbols, 0)
    }
    pub fn indexed_with_capacity(symbols: &'static crate::EntitySymbols, capacity: usize) -> Self {
        Self(Storage::Indexed(Box::new(IndexedValues {
            symbols,
            values: Vec::with_capacity(capacity.min(symbols.fields.len())),
            extras: BTreeMap::new(),
            legacy: Default::default(),
        })))
    }
    pub fn clone_with_capacity(&self, extra: usize) -> Self {
        match &self.0 {
            Storage::Named(_) => self.clone(),
            Storage::Indexed(v) => {
                let mut values = Vec::with_capacity(
                    v.values
                        .len()
                        .saturating_add(extra)
                        .min(v.symbols.fields.len()),
                );
                values.extend(v.values.iter().cloned());
                Self(Storage::Indexed(Box::new(IndexedValues {
                    symbols: v.symbols,
                    values,
                    extras: v.extras.clone(),
                    legacy: Default::default(),
                })))
            }
        }
    }
    pub fn symbols(&self) -> Option<&'static crate::EntitySymbols> {
        match &self.0 {
            Storage::Indexed(v) => Some(v.symbols),
            _ => None,
        }
    }
    /// Resolve a legacy input record once, keeping unknown fields in the named path.
    pub fn bind_symbols(&mut self, symbols: &'static crate::EntitySymbols) {
        if self.symbols().is_some_and(|s| s.same_layout(symbols)) {
            return;
        }
        assert!(
            self.symbols().is_none(),
            "cannot mix generated symbol layouts"
        );
        let capacity = self.len();
        let old = std::mem::replace(self, Self::indexed_with_capacity(symbols, capacity));
        for (field, value) in old.into_fields() {
            self.insert_named(field.name(), value);
        }
    }
    pub fn len(&self) -> usize {
        match &self.0 {
            Storage::Named(v) => v.len(),
            Storage::Indexed(v) => v.values.len() + v.extras.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn get(&self, name: &str) -> Option<&Value> {
        match &self.0 {
            Storage::Named(v) => v.get(name),
            Storage::Indexed(v) => match v.symbols.slot(name) {
                Some(slot) => v
                    .values
                    .binary_search_by_key(&slot, |(s, _)| *s)
                    .ok()
                    .map(|i| &v.values[i].1),
                None => v.extras.get(name),
            },
        }
    }
    pub fn get_slot(
        &self,
        symbols: &'static crate::EntitySymbols,
        slot: crate::FieldSlot,
    ) -> Option<&Value> {
        let name = symbols.field(slot)?;
        match &self.0 {
            Storage::Indexed(v) if v.symbols.same_layout(symbols) => v
                .values
                .binary_search_by_key(&slot, |(s, _)| *s)
                .ok()
                .map(|i| &v.values[i].1),
            Storage::Indexed(_) => None,
            Storage::Named(_) => self.get(name),
        }
    }
    pub fn contains_key(&self, name: &str) -> bool {
        self.get(name).is_some()
    }
    pub fn insert(&mut self, name: String, value: Value) -> Option<Value> {
        if let Some(symbols) = self.symbols()
            && let Some(slot) = symbols.slot(&name)
        {
            return self.insert_slot(symbols, slot, value);
        }
        match &mut self.0 {
            Storage::Named(v) => v.insert(name, value),
            Storage::Indexed(v) => {
                v.legacy.take();
                v.extras.insert(name, value)
            }
        }
    }
    pub fn insert_named(&mut self, name: &str, value: Value) -> Option<Value> {
        if let Some(symbols) = self.symbols()
            && let Some(slot) = symbols.slot(name)
        {
            return self.insert_slot(symbols, slot, value);
        }
        self.insert(name.to_owned(), value)
    }
    pub fn insert_slot(
        &mut self,
        symbols: &'static crate::EntitySymbols,
        slot: crate::FieldSlot,
        value: Value,
    ) -> Option<Value> {
        let name = symbols
            .field(slot)
            .expect("field slot outside generated layout");
        match &mut self.0 {
            Storage::Indexed(v) if v.symbols.same_layout(symbols) => {
                v.legacy.take();
                match v.values.binary_search_by_key(&slot, |(s, _)| *s) {
                    Ok(i) => Some(std::mem::replace(&mut v.values[i].1, value)),
                    Err(i) => {
                        v.values.insert(i, (slot, value));
                        None
                    }
                }
            }
            Storage::Indexed(_) => panic!("cannot mix generated symbol layouts"),
            Storage::Named(_) => self.insert_named(name, value),
        }
    }
    pub fn insert_field(&mut self, field: MutationField, value: Value) -> Option<Value> {
        match field {
            MutationField::Fixed(s, f) => self.insert_slot(s, f, value),
            MutationField::Named(n) => self.insert(n, value),
        }
    }
    pub fn remove(&mut self, name: &str) -> Option<Value> {
        match &mut self.0 {
            Storage::Named(v) => v.remove(name),
            Storage::Indexed(v) => {
                v.legacy.take();
                match v.symbols.slot(name) {
                    Some(slot) => v
                        .values
                        .binary_search_by_key(&slot, |(s, _)| *s)
                        .ok()
                        .map(|i| v.values.remove(i).1),
                    None => v.extras.remove(name),
                }
            }
        }
    }
    pub fn named_entries(&self) -> NamedEntries<'_> {
        match &self.0 {
            Storage::Named(v) => NamedEntries {
                named: v.iter().peekable(),
                fixed: [].iter().peekable(),
                symbols: None,
            },
            Storage::Indexed(v) => NamedEntries {
                named: v.extras.iter().peekable(),
                fixed: v.values.iter().peekable(),
                symbols: Some(v.symbols),
            },
        }
    }
    pub fn fields(&self) -> FieldEntries<'_> {
        match &self.0 {
            Storage::Named(v) => FieldEntries {
                named: v.iter().peekable(),
                fixed: [].iter().peekable(),
                symbols: None,
            },
            Storage::Indexed(v) => FieldEntries {
                named: v.extras.iter().peekable(),
                fixed: v.values.iter().peekable(),
                symbols: Some(v.symbols),
            },
        }
    }
    pub fn into_fields(self) -> impl Iterator<Item = (MutationField, Value)> {
        let (symbols, fixed, named) = match self.0 {
            Storage::Named(v) => (None, Vec::new(), v),
            Storage::Indexed(v) => (Some(v.symbols), v.values, v.extras),
        };
        fixed
            .into_iter()
            .map(move |(slot, value)| {
                (
                    MutationField::Fixed(symbols.expect("fixed layout"), slot),
                    value,
                )
            })
            .chain(
                named
                    .into_iter()
                    .map(|(name, value)| (MutationField::Named(name), value)),
            )
    }
    pub fn to_compact_row(&self) -> crate::CompactRow {
        let mut names = Vec::with_capacity(self.len());
        let mut values = Vec::with_capacity(self.len());
        for (name, value) in self.named_entries() {
            names.push(name.to_owned());
            values.push(value.clone());
        }
        crate::CompactRow::new(names.into(), values)
    }

    pub fn merge(&mut self, values: Self) {
        if self.is_empty() && self.symbols().is_none() {
            *self = values;
            return;
        }
        for (field, value) in values.into_fields() {
            self.insert_field(field, value);
        }
    }
}

pub struct FieldEntries<'a> {
    named: std::iter::Peekable<std::collections::btree_map::Iter<'a, String, Value>>,
    fixed: std::iter::Peekable<std::slice::Iter<'a, (crate::FieldSlot, Value)>>,
    symbols: Option<&'static crate::EntitySymbols>,
}
impl<'a> Iterator for FieldEntries<'a> {
    type Item = (MutationField, &'a Value);
    fn next(&mut self) -> Option<Self::Item> {
        let fixed = self
            .fixed
            .peek()
            .map(|(slot, _)| self.symbols.unwrap().field(*slot).unwrap());
        if fixed.is_some_and(|f| self.named.peek().is_none_or(|(name, _)| f < name.as_str())) {
            let (slot, value) = self.fixed.next().unwrap();
            Some((MutationField::Fixed(self.symbols.unwrap(), *slot), value))
        } else {
            self.named
                .next()
                .map(|(name, value)| (MutationField::Named(name.clone()), value))
        }
    }
}
pub struct NamedEntries<'a> {
    named: std::iter::Peekable<std::collections::btree_map::Iter<'a, String, Value>>,
    fixed: std::iter::Peekable<std::slice::Iter<'a, (crate::FieldSlot, Value)>>,
    symbols: Option<&'static crate::EntitySymbols>,
}
impl<'a> Iterator for NamedEntries<'a> {
    type Item = (&'a str, &'a Value);
    fn next(&mut self) -> Option<Self::Item> {
        let fixed = self
            .fixed
            .peek()
            .map(|(slot, _)| self.symbols.unwrap().field(*slot).unwrap());
        if fixed.is_some_and(|f| self.named.peek().is_none_or(|(name, _)| f < name.as_str())) {
            let (slot, value) = self.fixed.next().unwrap();
            Some((self.symbols.unwrap().field(*slot).unwrap(), value))
        } else {
            self.named
                .next()
                .map(|(name, value)| (name.as_str(), value))
        }
    }
}
impl Deref for MutationValues {
    type Target = BTreeMap<String, Value>;
    fn deref(&self) -> &Self::Target {
        match &self.0 {
            Storage::Named(v) => v,
            Storage::Indexed(v) => v.legacy.get_or_init(|| {
                self.named_entries()
                    .map(|(k, v)| (k.to_owned(), v.clone()))
                    .collect()
            }),
        }
    }
}
impl DerefMut for MutationValues {
    fn deref_mut(&mut self) -> &mut Self::Target {
        if matches!(self.0, Storage::Indexed(_)) {
            let old = std::mem::take(self);
            self.0 = Storage::Named(old.into());
        }
        match &mut self.0 {
            Storage::Named(v) => v,
            _ => unreachable!(),
        }
    }
}
impl From<BTreeMap<String, Value>> for MutationValues {
    fn from(values: BTreeMap<String, Value>) -> Self {
        Self(Storage::Named(values))
    }
}
impl From<MutationValues> for BTreeMap<String, Value> {
    fn from(values: MutationValues) -> Self {
        match values.0 {
            Storage::Named(v) => v,
            Storage::Indexed(v) => {
                let mut named = v.extras;
                named.extend(
                    v.values
                        .into_iter()
                        .map(|(slot, value)| (v.symbols.field(slot).unwrap().to_owned(), value)),
                );
                named
            }
        }
    }
}
impl IntoIterator for MutationValues {
    type Item = (String, Value);
    type IntoIter = std::collections::btree_map::IntoIter<String, Value>;
    fn into_iter(self) -> Self::IntoIter {
        BTreeMap::from(self).into_iter()
    }
}
impl<'a> IntoIterator for &'a MutationValues {
    type Item = (&'a String, &'a Value);
    type IntoIter = std::collections::btree_map::Iter<'a, String, Value>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
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

impl From<BTreeMap<String, Value>> for GeneratedValues {
    fn from(values: BTreeMap<String, Value>) -> Self {
        Self(values)
    }
}

impl From<GeneratedValues> for BTreeMap<String, Value> {
    fn from(values: GeneratedValues) -> Self {
        values.0
    }
}

/// Previously persisted field values used for optimistic checks and audit
/// comparison. It cannot be passed where new mutation values are expected.
/// Clones share immutable storage; mutation detaches so old/new snapshots stay
/// independent. Conversion to an owned map moves unique storage or copies it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EntitySnapshot(std::sync::Arc<BTreeMap<String, Value>>);

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
        std::sync::Arc::make_mut(&mut self.0)
    }
}

impl From<BTreeMap<String, Value>> for EntitySnapshot {
    fn from(values: BTreeMap<String, Value>) -> Self {
        Self(std::sync::Arc::new(values))
    }
}

impl From<EntitySnapshot> for BTreeMap<String, Value> {
    fn from(values: EntitySnapshot) -> Self {
        std::sync::Arc::try_unwrap(values.0).unwrap_or_else(|shared| (*shared).clone())
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
    /// Optional model-admitted edits committed with the one recovery version.
    pub values: MutationValues,
    pub trace_chain: Vec<crate::TraceNode>,
}

impl RecoverCommand {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>, expected_version: i64) -> Self {
        Self {
            entity: entity.into(),
            id: id.into(),
            expected_version,
            values: MutationValues::new(),
            trace_chain: Vec::new(),
        }
    }

    pub fn value(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.values.insert(field.into(), value.into());
        self
    }
}

#[cfg(test)]
mod shared_snapshot_tests {
    use super::*;

    #[test]
    fn snapshot_clones_detach_on_nested_mutation_and_owned_conversion() {
        let original = EntitySnapshot::from(BTreeMap::from([
            ("id".into(), Value::U64(7)),
            ("nullable".into(), Value::Null),
            (
                "nested".into(),
                Value::Object(BTreeMap::from([("label".into(), Value::from("old"))])),
            ),
        ]));
        let mut changed = original.clone();
        assert!(std::ptr::eq(&*original, &*changed));
        let Value::Object(nested) = changed.get_mut("nested").unwrap() else {
            panic!("object")
        };
        nested.insert("label".into(), Value::from("new"));
        changed.remove("nullable");
        assert!(!std::ptr::eq(&*original, &*changed));
        assert_eq!(original.get("nullable"), Some(&Value::Null));
        assert!(!original.contains_key("missing"));
        assert_eq!(
            original.get("nested"),
            Some(&Value::Object(BTreeMap::from([(
                "label".into(),
                Value::from("old")
            )])))
        );
        let mut owned: BTreeMap<String, Value> = original.clone().into();
        owned.insert("id".into(), Value::U64(99));
        assert_eq!(original.get("id"), Some(&Value::U64(7)));
    }

    #[test]
    fn unique_snapshot_into_map_moves_payload_without_string_copy() {
        let snapshot =
            EntitySnapshot::from(BTreeMap::from([("name".into(), Value::from("payload"))]));
        let payload = snapshot.get("name").unwrap().try_text().unwrap().as_ptr();
        let values: BTreeMap<String, Value> = snapshot.into();
        assert_eq!(
            payload,
            values.get("name").unwrap().try_text().unwrap().as_ptr()
        );
    }
}
