use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use teaql_core::{EntitySnapshot, MutationValues, SmartList, Value};

#[derive(Debug, Clone)]
pub struct EntityKey {
    pub entity: Cow<'static, str>,
    pub id: Value,
    id_key: EntityIdentityKey,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum EntityIdentityKey {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(u64),
    Decimal(rust_decimal::Decimal),
    Text(String),
    Date(chrono::NaiveDate),
    Timestamp(i64),
    Other(String),
}

impl EntityKey {
    pub fn new(entity: impl Into<String>, id: impl Into<Value>) -> Self {
        let id = id.into();
        Self {
            entity: Cow::Owned(entity.into()),
            id_key: entity_identity_key(&id),
            id,
        }
    }

    pub fn new_static(entity: &'static str, id: impl Into<Value>) -> Self {
        let id = id.into();
        Self {
            entity: Cow::Borrowed(entity),
            id_key: entity_identity_key(&id),
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

fn entity_identity_key(value: &Value) -> EntityIdentityKey {
    match value {
        Value::Null | Value::TypedNull(_) => EntityIdentityKey::Null,
        Value::Bool(value) => EntityIdentityKey::Bool(*value),
        Value::I64(value) => EntityIdentityKey::I64(*value),
        Value::U64(value) => EntityIdentityKey::U64(*value),
        Value::F64(value) => EntityIdentityKey::F64(value.to_bits()),
        Value::Decimal(value) => EntityIdentityKey::Decimal(*value),
        Value::Text(value) => EntityIdentityKey::Text(value.clone()),
        Value::Json(value) => EntityIdentityKey::Other(format!("json:{value}")),
        Value::Date(value) => EntityIdentityKey::Date(*value),
        Value::Timestamp(value) => EntityIdentityKey::Timestamp(value.0),
        Value::Object(_) => EntityIdentityKey::Other("object".to_owned()),
        Value::List(_) => EntityIdentityKey::Other("list".to_owned()),
    }
}

#[derive(Default)]
pub struct EntityGraphBuilder {
    tables: HashMap<TypeId, HashMap<u64, Box<dyn Any + Send + Sync>>>,
    relation_lists: HashMap<RelationListKey, Box<dyn Any + Send + Sync>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RelationListKey {
    owner_entity: String,
    owner_id: u64,
    relation: String,
}

impl EntityGraphBuilder {
    pub fn install<T>(&mut self, id: u64, entity: T)
    where
        T: Any + Send + Sync,
    {
        self.tables
            .entry(TypeId::of::<T>())
            .or_default()
            .insert(id, Box::new(entity));
    }

    pub fn entity_count(&self) -> usize {
        self.tables.values().map(HashMap::len).sum()
    }

    pub fn install_relation_list<T>(
        &mut self,
        owner_entity: impl Into<String>,
        owner_id: u64,
        relation: impl Into<String>,
        list: SmartList<T>,
    ) where
        T: Any + Send + Sync,
    {
        self.relation_lists.insert(
            RelationListKey {
                owner_entity: crate::canonical_id_space_entity(&owner_entity.into()),
                owner_id,
                relation: relation.into(),
            },
            Box::new(list),
        );
    }

    pub fn install_relation_option<T>(
        &mut self,
        owner_entity: impl Into<String>,
        owner_id: u64,
        relation: impl Into<String>,
        value: Option<T>,
    ) where
        T: Any + Send + Sync,
    {
        self.relation_lists.insert(
            RelationListKey {
                owner_entity: crate::canonical_id_space_entity(&owner_entity.into()),
                owner_id,
                relation: relation.into(),
            },
            Box::new(value),
        );
    }

    pub fn relation_list_count(&self) -> usize {
        self.relation_lists.len()
    }

    fn freeze(self) -> FrozenEntityGraph {
        FrozenEntityGraph {
            tables: self.tables,
            relation_lists: self.relation_lists,
        }
    }
}

impl std::fmt::Debug for EntityGraphBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EntityGraphBuilder")
            .field("entity_types", &self.tables.len())
            .field("entities", &self.entity_count())
            .field("relation_lists", &self.relation_list_count())
            .finish()
    }
}

struct FrozenEntityGraph {
    tables: HashMap<TypeId, HashMap<u64, Box<dyn Any + Send + Sync>>>,
    relation_lists: HashMap<RelationListKey, Box<dyn Any + Send + Sync>>,
}

impl std::fmt::Debug for FrozenEntityGraph {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrozenEntityGraph")
            .field("entity_types", &self.tables.len())
            .field(
                "entities",
                &self.tables.values().map(HashMap::len).sum::<usize>(),
            )
            .field("relation_lists", &self.relation_lists.len())
            .finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct EntityChangeSet {
    changes: BTreeMap<EntityKey, MutationValues>,
}

#[derive(Debug, Default)]
struct OriginalVersions {
    first: Option<(EntityKey, i64)>,
    overflow: BTreeMap<EntityKey, i64>,
}

impl OriginalVersions {
    fn clear(&mut self) {
        self.first = None;
        self.overflow.clear();
    }

    fn get(&self, key: &EntityKey) -> Option<i64> {
        self.first
            .as_ref()
            .and_then(|(first_key, version)| (first_key == key).then_some(*version))
            .or_else(|| self.overflow.get(key).copied())
    }

    fn insert(&mut self, key: EntityKey, version: i64) {
        match &mut self.first {
            None => self.first = Some((key, version)),
            Some((first_key, first_version)) if first_key == &key => *first_version = version,
            Some(_) => {
                self.overflow.insert(key, version);
            }
        }
    }
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

    pub fn changes(&self) -> &BTreeMap<EntityKey, MutationValues> {
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
    /// The original loaded snapshot, used to avoid redundant fetching during save.
    original_snapshot: Option<OriginalSnapshot>,
    /// Trace chains associated with each entity key.
    trace_chains: std::collections::BTreeMap<EntityKey, Vec<teaql_core::TraceNode>>,
    /// Original versions of entities to perform optimistic concurrency control.
    original_versions: OriginalVersions,
    /// Indicates if this entity root is entirely new.
    is_new: bool,
}

#[derive(Debug, Clone, Default)]
pub struct EntityRoot {
    inner: Arc<Mutex<RootContext>>,
    graph: Arc<OnceLock<FrozenEntityGraph>>,
}

impl std::panic::UnwindSafe for EntityRoot {}
impl std::panic::RefUnwindSafe for EntityRoot {}

#[derive(Debug)]
enum OriginalSnapshot {
    Materialized(EntitySnapshot),
    Compact(teaql_core::CompactRow),
}

impl PartialEq for EntityRoot {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl EntityRoot {
    /// Make this root resolve entities from the same flat graph as `source`.
    /// Existing snapshots and mutation ledger state remain owned by this root.
    pub fn with_shared_graph(&self, source: &EntityRoot) -> Self {
        Self {
            inner: self.inner.clone(),
            graph: source.graph.clone(),
        }
    }

    /// Publish a completely assembled graph. It becomes immutable after this call.
    pub fn freeze_graph(&self, builder: EntityGraphBuilder) -> Result<(), EntityGraphBuilder> {
        self.graph
            .set(builder.freeze())
            .map_err(|graph| EntityGraphBuilder {
                tables: graph.tables,
                relation_lists: graph.relation_lists,
            })
    }

    /// Resolve an entity by type and ID without locking or reference cloning.
    pub fn resolve_entity<T>(&self, id: u64) -> Option<&T>
    where
        T: Any + Send + Sync,
    {
        self.graph
            .get()?
            .tables
            .get(&TypeId::of::<T>())?
            .get(&id)?
            .downcast_ref::<T>()
    }

    pub fn resolve_relation_list<T>(
        &self,
        owner_entity: &str,
        owner_id: u64,
        relation: &str,
    ) -> Option<&SmartList<T>>
    where
        T: Any + Send + Sync,
    {
        self.graph
            .get()?
            .relation_lists
            .get(&RelationListKey {
                owner_entity: crate::canonical_id_space_entity(owner_entity),
                owner_id,
                relation: relation.to_owned(),
            })?
            .downcast_ref::<SmartList<T>>()
    }

    pub fn resolve_relation_option<T>(
        &self,
        owner_entity: &str,
        owner_id: u64,
        relation: &str,
    ) -> Option<&Option<T>>
    where
        T: Any + Send + Sync,
    {
        self.graph
            .get()?
            .relation_lists
            .get(&RelationListKey {
                owner_entity: crate::canonical_id_space_entity(owner_entity),
                owner_id,
                relation: relation.to_owned(),
            })?
            .downcast_ref::<Option<T>>()
    }

    pub fn has_relation_view(&self, owner_entity: &str, owner_id: u64, relation: &str) -> bool {
        self.graph.get().is_some_and(|graph| {
            graph.relation_lists.contains_key(&RelationListKey {
                owner_entity: crate::canonical_id_space_entity(owner_entity),
                owner_id,
                relation: relation.to_owned(),
            })
        })
    }

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

    /// Clear all state consumed by a successfully committed ledger save.
    /// Failed saves must not call this method so their pending intent remains retryable.
    pub fn clear_committed(&self) {
        let mut context = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        context.change_sets = ChangeSetStack::default();
        context.deleted_keys.clear();
        context.new_keys.clear();
        context.original_versions.clear();
        context.trace_chains.clear();
        context.original_snapshot = None;
        context.comment = None;
        context.is_new = false;
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

    /// Store an original loaded entity snapshot.
    pub fn set_original_snapshot(&self, snapshot: EntitySnapshot) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .original_snapshot = Some(OriginalSnapshot::Materialized(snapshot));
    }

    /// Store a shared-schema snapshot without eagerly allocating a map.
    pub fn set_original_compact_row(&self, row: teaql_core::CompactRow) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .original_snapshot = Some(OriginalSnapshot::Compact(row));
    }

    /// Retrieve the original loaded entity snapshot.
    pub fn original_snapshot(&self) -> Option<EntitySnapshot> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .original_snapshot
            .as_ref()
            .map(|snapshot| match snapshot {
                OriginalSnapshot::Materialized(snapshot) => snapshot.clone(),
                OriginalSnapshot::Compact(row) => EntitySnapshot::from(row.clone().into_map()),
            })
    }

    /// Mark an entity as deleted. The next `save()` call will treat this entity
    /// as a Remove operation in the graph save pipeline.
    /// Any pending field changes for this entity are cleared — they are irrelevant
    /// when the entity is being deleted.
    pub fn mark_as_delete(&self, key: EntityKey) {
        let mut context = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        context.change_sets.clear_entity(&key);
        context.deleted_keys.insert(key);
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
