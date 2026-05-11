use std::collections::BTreeMap;
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
        Value::Timestamp(value) => format!("timestamp:{}", value.to_rfc3339()),
        Value::Object(_) => "object".to_owned(),
        Value::List(_) => "list".to_owned(),
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
}

#[derive(Debug, Default)]
pub struct RootContext {
    change_sets: ChangeSetStack,
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
            .expect("entity root mutex")
            .change_sets
            .push();
    }

    pub fn pop_change_set(&self) -> Option<EntityChangeSet> {
        self.inner
            .lock()
            .expect("entity root mutex")
            .change_sets
            .pop()
    }

    pub fn clear_current_change_set(&self) {
        self.inner
            .lock()
            .expect("entity root mutex")
            .change_sets
            .clear_current();
    }

    pub fn set(&self, key: EntityKey, field: impl Into<String>, value: impl Into<Value>) {
        self.inner
            .lock()
            .expect("entity root mutex")
            .change_sets
            .set(key, field, value.into());
    }

    pub fn get(&self, key: &EntityKey, field: &str) -> Option<Value> {
        self.inner
            .lock()
            .expect("entity root mutex")
            .change_sets
            .get(key, field)
    }

    pub fn current_change_set(&self) -> EntityChangeSet {
        self.inner
            .lock()
            .expect("entity root mutex")
            .change_sets
            .current()
            .cloned()
            .unwrap_or_default()
    }
}
