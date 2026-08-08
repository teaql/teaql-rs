use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use teaql_core::{Record, Value};

#[derive(Debug, Clone)]
pub struct EntityKey {
    pub entity: String,
    pub id: Value,
    id_key: String,
}

impl EntityKey {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>) -> Self {
        let id = id.into();
        Self {
            entity: entity.into(),
            id_key: value_key(&id),
            id,
        }
    }
}

impl PartialEq for EntityKey {
    fn eq(&self, other: &Self) -> bool {
        self.entity == other.entity && self.id_key == other.id_key
    }
}

impl Eq for EntityKey {}

impl PartialOrd for EntityKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EntityKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.entity
            .cmp(&other.entity)
            .then_with(|| self.id_key.cmp(&other.id_key))
    }
}

fn value_key(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => format!("bool:{value}"),
        Value::I64(value) => format!("i64:{value}"),
        Value::U64(value) => format!("u64:{value}"),
        Value::F64(value) => format!("f64:{value}"),
        Value::Decimal(value) => format!("decimal:{value}"),
        Value::Text(value) => format!("text:{value}"),
        Value::Json(value) => format!("json:{value}"),
        Value::Date(value) => format!("date:{value}"),
        Value::Timestamp(value) => format!("timestamp:{}", value.0),
        Value::Object(_) => "object".to_owned(),
        Value::List(_) => "list".to_owned(),
        Value::TypedNull(_) => "null".to_owned(),
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct EntityChangeSet {
    changes: BTreeMap<EntityKey, Record>,
}

impl EntityChangeSet {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn set(&mut self, key: EntityKey, field: impl Into<String>, value: Value) {
        self.changes
            .entry(key)
            .or_default()
            .insert(field.into(), value);
    }

    pub fn get(&self, key: &EntityKey, field: &str) -> Option<&Value> {
        self.changes.get(key).and_then(|changes| changes.get(field))
    }

    pub fn changes(&self) -> &BTreeMap<EntityKey, Record> {
        &self.changes
    }

    /// Remove all pending changes for a specific entity key.
    pub fn clear_entity(&mut self, key: &EntityKey) {
        self.changes.remove(key);
    }

    /// Get the set of field names that have been modified for a given entity key.
    pub fn field_names(&self, key: &EntityKey) -> BTreeSet<String> {
        self.changes
            .get(key)
            .map(|record| record.keys().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChangeSetStack {
    stack: Vec<EntityChangeSet>,
}

impl ChangeSetStack {
    pub fn current_mut(&mut self) -> &mut EntityChangeSet {
        if self.stack.is_empty() {
            self.stack.push(EntityChangeSet::default());
        }
        self.stack.last_mut().expect("change set stack has current")
    }

    pub fn current(&self) -> Option<&EntityChangeSet> {
        self.stack.last()
    }

    pub fn push(&mut self) {
        self.stack.push(EntityChangeSet::default());
    }

    pub fn pop(&mut self) -> Option<EntityChangeSet> {
        self.stack.pop()
    }

    pub fn get(&self, key: &EntityKey, field: &str) -> Option<Value> {
        self.stack
            .iter()
            .rev()
            .find_map(|change_set| change_set.get(key, field).cloned())
    }

    pub fn set(&mut self, key: EntityKey, field: impl Into<String>, value: Value) {
        self.current_mut().set(key, field, value);
    }

    pub fn clear_current(&mut self) {
        if let Some(current) = self.stack.last_mut() {
            *current = EntityChangeSet::default();
        }
    }

    /// Remove all pending changes for a specific entity key across all stack levels.
    pub fn clear_entity(&mut self, key: &EntityKey) {
        for change_set in &mut self.stack {
            change_set.clear_entity(key);
        }
    }

    /// Get the union of all changed field names for a given entity key across all stack levels.
    /// This is the Rust equivalent of Java's `entity.getUpdatedProperties()`.
    pub fn changed_field_names(&self, key: &EntityKey) -> BTreeSet<String> {
        let mut fields = BTreeSet::new();
        for change_set in &self.stack {
            fields.extend(change_set.field_names(key));
        }
        fields
    }
}

#[derive(Debug, Default)]
pub struct RootContext {
    change_sets: ChangeSetStack,
    /// Annotation comment for observability during graph save.
    comment: Option<String>,
    /// Entity keys that have been marked for deletion.
    /// When the entity is saved, the graph save pipeline will treat these as Remove operations.
    deleted_keys: std::collections::BTreeSet<EntityKey>,
    /// Entity keys that have been marked as newly inserted.
    new_keys: std::collections::BTreeSet<EntityKey>,
    /// The original loaded snapshot record, used to avoid redundant fetching during save.
    original_record: Option<Record>,
    /// Trace chains associated with each entity key.
    trace_chains: std::collections::BTreeMap<EntityKey, Vec<teaql_core::TraceNode>>,
    /// Original versions of entities to perform optimistic concurrency control.
    original_versions: std::collections::BTreeMap<EntityKey, i64>,
    /// Indicates if this entity root is entirely new.
    is_new: bool,
}

#[derive(Debug, Clone, Default)]
pub struct EntityRoot {
    inner: Arc<Mutex<RootContext>>,
}

impl PartialEq for EntityRoot {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl EntityRoot {
    pub fn push_change_set(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .change_sets
            .push();
    }

    pub fn pop_change_set(&self) -> Option<EntityChangeSet> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .change_sets
            .pop()
    }

    pub fn clear_current_change_set(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .change_sets
            .clear_current();
    }

    pub fn set(&self, key: EntityKey, field: impl Into<String>, value: impl Into<Value>) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .change_sets
            .set(key, field, value.into());
    }

    pub fn get(&self, key: &EntityKey, field: &str) -> Option<Value> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .change_sets
            .get(key, field)
    }

    pub fn current_change_set(&self) -> EntityChangeSet {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .change_sets
            .current()
            .cloned()
            .unwrap_or_default()
    }

    /// Set an annotation comment on this entity root.
    /// The comment propagates through the graph save process for observability.
    pub fn set_comment(&self, comment: impl Into<String>) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).comment = Some(comment.into());
    }

    /// Get the annotation comment, if any.
    pub fn get_comment(&self) -> Option<String> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .comment
            .clone()
    }

    /// Mark this entity root as a newly created entity in memory.
    pub fn mark_as_new(&self, key: EntityKey) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .new_keys
            .insert(key);
    }

    /// Check if this entity root is marked as newly created.
    pub fn is_new(&self, key: &EntityKey) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .new_keys
            .contains(key)
    }

    /// Store the original record when loaded from DB.
    pub fn set_original_record(&self, record: Record) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .original_record = Some(record);
    }

    /// Retrieve the original record.
    pub fn original_record(&self) -> Option<Record> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .original_record
            .clone()
    }

    /// Mark an entity as deleted. The next `save()` call will treat this entity
    /// as a Remove operation in the graph save pipeline.
    /// Any pending field changes for this entity are cleared — they are irrelevant
    /// when the entity is being deleted.
    pub fn mark_as_delete(&self, key: EntityKey) {
        let mut ctx = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        ctx.change_sets.clear_entity(&key);
        ctx.deleted_keys.insert(key);
    }

    /// Check whether an entity has been marked for deletion.
    pub fn is_marked_as_delete(&self, key: &EntityKey) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .deleted_keys
            .contains(key)
    }

    /// Get the set of field names that have been modified for the given entity key.
    /// This is the Rust equivalent of Java's `entity.getUpdatedProperties()`.
    pub fn changed_field_names(&self, key: &EntityKey) -> BTreeSet<String> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .change_sets
            .changed_field_names(key)
    }
    pub fn deleted_keys(&self) -> std::collections::BTreeSet<EntityKey> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .deleted_keys
            .clone()
    }

    pub fn new_keys(&self) -> std::collections::BTreeSet<EntityKey> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .new_keys
            .clone()
    }

    pub fn get_original_version(&self, key: &EntityKey) -> Option<i64> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .original_versions
            .get(key)
            .cloned()
    }

    pub fn get_trace_chain(&self, key: &EntityKey) -> Vec<teaql_core::TraceNode> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .trace_chains
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    pub fn set_original_version(&self, key: EntityKey, version: i64) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .original_versions
            .insert(key, version);
    }
}

pub trait LedgerEntity: teaql_core::Entity {
    fn entity_root(&self) -> Option<EntityRoot>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_key() {
        let key1 = EntityKey::new("User", 1_i64);
        let key2 = EntityKey::new("User", 1_i64);
        let key3 = EntityKey::new("User", "1".to_owned());

        assert_eq!(key1, key2);
        assert_ne!(key1, key3); // i64 vs text

        assert_eq!(key1.cmp(&key2), std::cmp::Ordering::Equal);
        assert_eq!(key1.partial_cmp(&key2), Some(std::cmp::Ordering::Equal));
        assert!(key1 < key3 || key1 > key3); // test ord
    }

    #[test]
    fn test_value_key_variants() {
        let _ = EntityKey::new("e", Value::Null);
        let _ = EntityKey::new("e", Value::Bool(true));
        let _ = EntityKey::new("e", Value::I64(1));
        let _ = EntityKey::new("e", Value::U64(1));
        let _ = EntityKey::new("e", Value::Text("t".into()));
        let _ = EntityKey::new("e", Value::Object(BTreeMap::new()));
        let _ = EntityKey::new("e", Value::List(vec![]));
        let _ = EntityKey::new("e", Value::TypedNull(teaql_core::DataType::I64));
        let _ = EntityKey::new("e", Value::Json(serde_json::json!({})));

        use std::str::FromStr;
        if let Ok(d) = rust_decimal::Decimal::from_str("1.23") {
            let _ = EntityKey::new("e", Value::Decimal(d));
        }
    }

    #[test]
    fn test_entity_change_set() {
        let mut changes = EntityChangeSet::default();
        let key = EntityKey::new("User", 1_i64);

        assert!(changes.is_empty());
        changes.set(key.clone(), "name", Value::Text("Alice".into()));
        assert!(!changes.is_empty());

        assert_eq!(
            changes.get(&key, "name"),
            Some(&Value::Text("Alice".into()))
        );
        assert_eq!(changes.get(&key, "age"), None);

        let field_names = changes.field_names(&key);
        assert!(field_names.contains("name"));

        let mut expected_map = BTreeMap::new();
        expected_map.insert("name".to_string(), Value::Text("Alice".into()));
        assert_eq!(changes.changes().get(&key), Some(&expected_map));

        changes.clear_entity(&key);
        assert!(changes.is_empty());
        assert!(changes.field_names(&key).is_empty());
    }

    #[test]
    fn test_change_set_stack() {
        let mut stack = ChangeSetStack::default();
        let key = EntityKey::new("User", 1_i64);

        assert!(stack.current().is_none());
        stack.set(key.clone(), "name", Value::Text("Alice".into()));
        assert!(stack.current().is_some());

        stack.push();
        stack.set(key.clone(), "name", Value::Text("Bob".into()));
        stack.set(key.clone(), "age", Value::I64(30));

        assert_eq!(stack.get(&key, "name"), Some(Value::Text("Bob".into())));
        assert_eq!(stack.get(&key, "age"), Some(Value::I64(30)));

        let fields = stack.changed_field_names(&key);
        assert!(fields.contains("name"));
        assert!(fields.contains("age"));

        stack.clear_current();
        assert_eq!(stack.get(&key, "name"), Some(Value::Text("Alice".into())));
        assert_eq!(stack.get(&key, "age"), None);

        stack.set(key.clone(), "age", Value::I64(40));
        stack.clear_entity(&key);
        assert_eq!(stack.get(&key, "name"), None);
        assert_eq!(stack.get(&key, "age"), None);

        stack.push();
        let popped = stack.pop();
        assert!(popped.is_some());

        // drain rest
        stack.pop();
        stack.pop();
        assert!(stack.pop().is_none());
    }

    #[test]
    fn test_entity_root() {
        let root = EntityRoot::default();
        let root2 = root.clone();
        assert_eq!(root, root2);

        let key = EntityKey::new("User", 1_i64);

        root.set(key.clone(), "name", Value::Text("Bob".into()));
        assert_eq!(root.get(&key, "name"), Some(Value::Text("Bob".into())));
        assert!(root.changed_field_names(&key).contains("name"));

        let cs = root.current_change_set();
        assert!(!cs.is_empty());

        root.push_change_set();
        root.set(key.clone(), "age", Value::I64(20));
        assert_eq!(root.get(&key, "age"), Some(Value::I64(20)));
        root.clear_current_change_set();
        assert_eq!(root.get(&key, "age"), None);
        let _ = root.pop_change_set();

        root.set_comment("test comment");
        assert_eq!(root.get_comment(), Some("test comment".into()));

        root.mark_as_new(key.clone());
        assert!(root.is_new(&key));
        assert!(root.new_keys().contains(&key));

        let mut rec = Record::new();
        rec.insert("id".to_string(), Value::I64(1));
        root.set_original_record(rec.clone());
        assert_eq!(root.original_record(), Some(rec));

        root.mark_as_delete(key.clone());
        assert!(root.is_marked_as_delete(&key));
        assert!(root.deleted_keys().contains(&key));

        root.set_original_version(key.clone(), 42);
        assert_eq!(root.get_original_version(&key), Some(42));

        let trace = root.get_trace_chain(&key);
        assert!(trace.is_empty());
    }
}
