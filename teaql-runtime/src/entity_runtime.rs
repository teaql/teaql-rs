use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use teaql_core::{EntitySnapshot, MutationValues, SmartList, Value};

/// The explicit load state of a relation stored in the runtime identity graph.
///
/// Reading this state never performs I/O. `NotLoaded` means exactly that the
/// current query did not install a value for the relation; callers must issue
/// an explicit query if they need it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadedRelation {
    Loaded,
    Empty,
    NotLoaded,
}

/// A borrowed view of a relation in the runtime identity graph.
///
/// `value()` is present for both `Loaded` and loaded-empty collection values.
/// It is absent for a null to-one relation and for `NotLoaded`.
#[derive(Debug, Clone, Copy)]
pub struct RelationHandle<'a, T> {
    state: LoadedRelation,
    value: Option<&'a T>,
}

impl<'a, T> RelationHandle<'a, T> {
    fn new(state: LoadedRelation, value: Option<&'a T>) -> Self {
        Self { state, value }
    }

    pub fn state(&self) -> LoadedRelation {
        self.state
    }

    pub fn value(&self) -> Option<&'a T> {
        self.value
    }

    pub fn is_loaded(&self) -> bool {
        self.state != LoadedRelation::NotLoaded
    }

    pub fn is_empty(&self) -> bool {
        self.state == LoadedRelation::Empty
    }
}

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
    tables: HashMap<TypeId, EntityTable>,
    relation_lists: HashMap<RelationListKey, Box<dyn Any + Send + Sync>>,
}

type EntityTable = HashMap<u64, Box<dyn Any + Send + Sync>>;

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
    tables: HashMap<TypeId, EntityTable>,
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
struct EntityMutationLedger {
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

#[derive(Debug)]
pub struct EntityRuntimeState {
    // The OnceLock itself is shared so entities composed before the first mutation
    // still materialize exactly one graph-owned ledger.
    inner: Arc<OnceLock<Arc<Mutex<EntityMutationLedger>>>>,
    graph: EntityGraphReference,
    loaded_snapshot: Option<teaql_core::CompactRow>,
}

#[derive(Debug)]
enum EntityGraphReference {
    Strong(Arc<OnceLock<FrozenEntityGraph>>),
    Weak(Weak<OnceLock<FrozenEntityGraph>>),
}

impl EntityGraphReference {
    fn preserve(&self) -> Self {
        match self {
            Self::Strong(graph) => Self::Strong(graph.clone()),
            Self::Weak(graph) => Self::Weak(graph.clone()),
        }
    }

    fn promote(&self) -> Self {
        match self {
            Self::Strong(graph) => Self::Strong(graph.clone()),
            Self::Weak(graph) => graph
                .upgrade()
                .map(Self::Strong)
                .unwrap_or_else(|| Self::Strong(Arc::default())),
        }
    }

    fn weak(&self) -> Self {
        match self {
            Self::Strong(graph) => Self::Weak(Arc::downgrade(graph)),
            Self::Weak(graph) => Self::Weak(graph.clone()),
        }
    }

    fn pointer(&self) -> *const OnceLock<FrozenEntityGraph> {
        match self {
            Self::Strong(graph) => Arc::as_ptr(graph),
            Self::Weak(graph) => graph.as_ptr(),
        }
    }

    fn strong(&self) -> Option<&Arc<OnceLock<FrozenEntityGraph>>> {
        match self {
            Self::Strong(graph) => Some(graph),
            Self::Weak(_) => None,
        }
    }

    fn frozen(&self) -> Option<&FrozenEntityGraph> {
        match self {
            Self::Strong(graph) => graph.get(),
            Self::Weak(graph) => {
                let owner = graph.upgrade()?;
                let frozen = owner.get()? as *const FrozenEntityGraph;
                // SAFETY: weak graph references are only installed into entities owned by the
                // same frozen graph. Such an entity can only be borrowed while an owning root
                // keeps the graph alive. Cloning EntityRuntimeState promotes the weak reference to a
                // strong owner, so an entity moved out through safe code also anchors the graph.
                Some(unsafe { &*frozen })
            }
        }
    }
}

impl Default for EntityRuntimeState {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
            graph: EntityGraphReference::Strong(Arc::default()),
            loaded_snapshot: None,
        }
    }
}

impl Clone for EntityRuntimeState {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            graph: self.graph.promote(),
            loaded_snapshot: self.loaded_snapshot.clone(),
        }
    }
}

impl std::panic::UnwindSafe for EntityRuntimeState {}
impl std::panic::RefUnwindSafe for EntityRuntimeState {}

#[derive(Debug)]
enum OriginalSnapshot {
    Materialized(EntitySnapshot),
    Compact(teaql_core::CompactRow),
}

impl PartialEq for EntityRuntimeState {
    fn eq(&self, other: &Self) -> bool {
        if Arc::ptr_eq(&self.inner, &other.inner) {
            return true;
        }
        match (self.inner.get(), other.inner.get()) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (None, None) => false,
            _ => false,
        }
    }
}

impl EntityRuntimeState {
    #[cfg(test)]
    fn has_mutation_context(&self) -> bool {
        self.inner.get().is_some()
    }

    fn context(&self) -> &Arc<Mutex<EntityMutationLedger>> {
        self.inner
            .get_or_init(|| Arc::new(Mutex::new(EntityMutationLedger::default())))
    }

    fn read_context<R>(&self, default: R, read: impl FnOnce(&EntityMutationLedger) -> R) -> R {
        let Some(context) = self.inner.get() else {
            return default;
        };
        let context = context.lock().unwrap_or_else(|error| error.into_inner());
        read(&context)
    }

    fn write_context<R>(&self, write: impl FnOnce(&mut EntityMutationLedger) -> R) -> R {
        let mut context = self
            .context()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        write(&mut context)
    }

    pub fn fresh_with_shared_graph(source: &EntityRuntimeState) -> Self {
        Self {
            inner: Arc::default(),
            graph: source.graph.preserve(),
            loaded_snapshot: None,
        }
    }

    /// Create a root view for an entity stored inside the graph itself. The weak view prevents
    /// the graph from strongly owning an entity that strongly owns the graph in return.
    pub(crate) fn fresh_with_weak_graph(source: &EntityRuntimeState) -> Self {
        Self {
            inner: Arc::default(),
            graph: source.graph.weak(),
            loaded_snapshot: None,
        }
    }

    /// Make this root resolve entities from the same flat graph as `source`.
    /// Existing snapshots and mutation ledger state remain owned by this root.
    pub fn with_shared_graph(&self, source: &EntityRuntimeState) -> Self {
        Self {
            inner: self.inner.clone(),
            graph: source.graph.preserve(),
            loaded_snapshot: self.loaded_snapshot.clone(),
        }
    }

    /// Publish a completely assembled graph. It becomes immutable after this call.
    pub fn freeze_graph(&self, builder: EntityGraphBuilder) -> Result<(), EntityGraphBuilder> {
        let Some(graph) = self.graph.strong() else {
            return Err(builder);
        };
        graph
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
            .frozen()?
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
            .frozen()?
            .relation_lists
            .get(&RelationListKey {
                owner_entity: crate::canonical_id_space_entity(owner_entity),
                owner_id,
                relation: relation.to_owned(),
            })?
            .downcast_ref::<SmartList<T>>()
    }

    /// Resolve a to-many relation without performing an implicit database read.
    pub fn relation_list<T>(
        &self,
        owner_entity: &str,
        owner_id: u64,
        relation: &str,
    ) -> RelationHandle<'_, SmartList<T>>
    where
        T: Any + Send + Sync,
    {
        let Some(graph) = self.graph.frozen() else {
            return RelationHandle::new(LoadedRelation::NotLoaded, None);
        };
        let key = RelationListKey {
            owner_entity: crate::canonical_id_space_entity(owner_entity),
            owner_id,
            relation: relation.to_owned(),
        };
        let Some(stored) = graph.relation_lists.get(&key) else {
            return RelationHandle::new(LoadedRelation::NotLoaded, None);
        };
        let list = stored.downcast_ref::<SmartList<T>>().unwrap_or_else(|| {
            panic!(
                "relation view type mismatch: owner={} id={} relation={}",
                owner_entity, owner_id, relation
            )
        });
        if list.is_empty() {
            RelationHandle::new(LoadedRelation::Empty, Some(list))
        } else {
            RelationHandle::new(LoadedRelation::Loaded, Some(list))
        }
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
            .frozen()?
            .relation_lists
            .get(&RelationListKey {
                owner_entity: crate::canonical_id_space_entity(owner_entity),
                owner_id,
                relation: relation.to_owned(),
            })?
            .downcast_ref::<Option<T>>()
    }

    /// Resolve a to-one relation without performing an implicit database read.
    pub fn relation_option<T>(
        &self,
        owner_entity: &str,
        owner_id: u64,
        relation: &str,
    ) -> RelationHandle<'_, T>
    where
        T: Any + Send + Sync,
    {
        let Some(graph) = self.graph.frozen() else {
            return RelationHandle::new(LoadedRelation::NotLoaded, None);
        };
        let key = RelationListKey {
            owner_entity: crate::canonical_id_space_entity(owner_entity),
            owner_id,
            relation: relation.to_owned(),
        };
        let Some(stored) = graph.relation_lists.get(&key) else {
            return RelationHandle::new(LoadedRelation::NotLoaded, None);
        };
        let value = stored.downcast_ref::<Option<T>>().unwrap_or_else(|| {
            panic!(
                "relation view type mismatch: owner={} id={} relation={}",
                owner_entity, owner_id, relation
            )
        });
        match value {
            Some(value) => RelationHandle::new(LoadedRelation::Loaded, Some(value)),
            None => RelationHandle::new(LoadedRelation::Empty, None),
        }
    }

    pub fn has_relation_view(&self, owner_entity: &str, owner_id: u64, relation: &str) -> bool {
        self.graph.frozen().is_some_and(|graph| {
            graph.relation_lists.contains_key(&RelationListKey {
                owner_entity: crate::canonical_id_space_entity(owner_entity),
                owner_id,
                relation: relation.to_owned(),
            })
        })
    }

    pub fn push_change_set(&self) {
        self.write_context(|context| context.change_sets.push());
    }

    pub fn pop_change_set(&self) -> Option<EntityChangeSet> {
        self.inner.get()?;
        self.write_context(|context| context.change_sets.pop())
    }

    pub fn clear_current_change_set(&self) {
        if self.inner.get().is_some() {
            self.write_context(|context| context.change_sets.clear_current());
        }
    }

    /// Clear all state consumed by a successfully committed ledger save.
    /// Failed saves must not call this method so their pending intent remains retryable.
    pub fn clear_committed(&self) {
        if self.inner.get().is_some() {
            self.write_context(|context| {
                context.change_sets = ChangeSetStack::default();
                context.deleted_keys.clear();
                context.new_keys.clear();
                context.original_versions.clear();
                context.trace_chains.clear();
                context.original_snapshot = None;
                context.comment = None;
                context.is_new = false;
            });
        }
    }

    pub fn set(&self, key: EntityKey, field: impl Into<String>, value: impl Into<Value>) {
        self.write_context(|context| context.change_sets.set(key, field, value.into()));
    }

    pub fn get(&self, key: &EntityKey, field: &str) -> Option<Value> {
        self.read_context(None, |context| context.change_sets.get(key, field))
    }

    pub fn current_change_set(&self) -> EntityChangeSet {
        self.read_context(EntityChangeSet::default(), |context| {
            context.change_sets.current().cloned().unwrap_or_default()
        })
    }

    /// Set an annotation comment on this entity root.
    /// The comment propagates through the graph save process for observability.
    pub fn set_comment(&self, comment: impl Into<String>) {
        self.write_context(|context| context.comment = Some(comment.into()));
    }

    /// Get the annotation comment, if any.
    pub fn get_comment(&self) -> Option<String> {
        self.read_context(None, |context| context.comment.clone())
    }

    /// Mark this entity root as a newly created entity in memory.
    pub fn mark_as_new(&self, key: EntityKey) {
        self.write_context(|context| {
            context.new_keys.insert(key);
        });
    }

    /// Check if this entity root is marked as newly created.
    pub fn is_new(&self, key: &EntityKey) -> bool {
        self.read_context(false, |context| context.new_keys.contains(key))
    }

    /// Store an original loaded entity snapshot.
    pub fn set_original_snapshot(&self, snapshot: EntitySnapshot) {
        self.write_context(|context| {
            context.original_snapshot = Some(OriginalSnapshot::Materialized(snapshot));
        });
    }

    /// Store a shared-schema snapshot without allocating a mutation ledger.
    pub fn set_original_compact_row(&mut self, row: teaql_core::CompactRow) {
        self.loaded_snapshot = Some(row);
    }

    /// Retrieve the original loaded entity snapshot.
    pub fn original_snapshot(&self) -> Option<EntitySnapshot> {
        if let Some(row) = &self.loaded_snapshot {
            return Some(EntitySnapshot::from(row.clone().into_map()));
        }
        self.read_context(None, |context| {
            context
                .original_snapshot
                .as_ref()
                .map(|snapshot| match snapshot {
                    OriginalSnapshot::Materialized(snapshot) => snapshot.clone(),
                    OriginalSnapshot::Compact(row) => EntitySnapshot::from(row.clone().into_map()),
                })
        })
    }

    /// Mark an entity as deleted. The next `save()` call will treat this entity
    /// as a Remove operation in the graph save pipeline.
    /// Any pending field changes for this entity are cleared — they are irrelevant
    /// when the entity is being deleted.
    pub fn mark_as_delete(&self, key: EntityKey) {
        self.write_context(|context| {
            context.change_sets.clear_entity(&key);
            context.deleted_keys.insert(key);
        });
    }

    /// Check whether an entity has been marked for deletion.
    pub fn is_marked_as_delete(&self, key: &EntityKey) -> bool {
        self.read_context(false, |context| context.deleted_keys.contains(key))
    }

    /// Get the set of field names that have been modified for the given entity key.
    /// This is the Rust equivalent of Java's `entity.getUpdatedProperties()`.
    pub fn changed_field_names(&self, key: &EntityKey) -> BTreeSet<String> {
        self.read_context(BTreeSet::new(), |context| {
            context.change_sets.changed_field_names(key)
        })
    }
    pub fn deleted_keys(&self) -> std::collections::BTreeSet<EntityKey> {
        self.read_context(BTreeSet::new(), |context| context.deleted_keys.clone())
    }

    pub fn new_keys(&self) -> std::collections::BTreeSet<EntityKey> {
        self.read_context(BTreeSet::new(), |context| context.new_keys.clone())
    }

    pub fn get_original_version(&self, key: &EntityKey) -> Option<i64> {
        self.read_context(None, |context| context.original_versions.get(key))
            .or_else(|| {
                self.loaded_snapshot
                    .as_ref()?
                    .get("id")?
                    .try_u64()
                    .filter(|id| Some(*id) == key.id.try_u64())?;
                self.loaded_snapshot.as_ref()?.get("version")?.try_i64()
            })
    }

    pub fn get_trace_chain(&self, key: &EntityKey) -> Vec<teaql_core::TraceNode> {
        self.read_context(Vec::new(), |context| {
            context.trace_chains.get(key).cloned().unwrap_or_default()
        })
    }

    pub fn set_original_version(&self, key: EntityKey, version: i64) {
        self.write_context(|context| context.original_versions.insert(key, version));
    }
}

pub trait LedgerEntity: teaql_core::Entity {
    fn entity_runtime_state(&self) -> Option<EntityRuntimeState>;
}

#[cfg(test)]
mod lazy_root_tests {
    use super::*;

    #[derive(Clone)]
    struct GraphChild {
        root: EntityRuntimeState,
    }

    #[test]
    fn loaded_snapshot_does_not_allocate_ledger_until_mutation() {
        let mut root = EntityRuntimeState::default();
        root.set_original_compact_row(teaql_core::CompactRow::new(
            Arc::from(["id".to_owned(), "version".to_owned()]),
            vec![Value::U64(7), Value::I64(3)],
        ));
        let key = EntityKey::new_static("Example", 7_u64);

        assert!(!root.has_mutation_context());
        assert_eq!(root.get(&key, "name"), None);
        assert_eq!(root.get_original_version(&key), Some(3));
        assert!(!root.has_mutation_context());

        root.set(key, "name", Value::Text("updated".to_owned()));
        assert!(root.has_mutation_context());
    }

    #[test]
    fn clone_before_first_mutation_materializes_one_shared_ledger() {
        let root = EntityRuntimeState::default();
        let child = root.clone();
        let child_key = EntityKey::new_static("Child", 2_u64);

        child.set(child_key.clone(), "name", Value::Text("updated".to_owned()));

        assert_eq!(
            root.get(&child_key, "name"),
            Some(Value::Text("updated".to_owned()))
        );
        assert!(root.has_mutation_context());
    }

    #[test]
    fn graph_owned_entities_do_not_keep_the_graph_alive() {
        let root = EntityRuntimeState::default();
        let graph_owner = match &root.graph {
            EntityGraphReference::Strong(graph) => Arc::downgrade(graph),
            EntityGraphReference::Weak(_) => unreachable!(),
        };
        let mut builder = EntityGraphBuilder::default();
        builder.install_relation_list(
            "Owner",
            1,
            "children",
            SmartList::from(vec![GraphChild {
                root: EntityRuntimeState::fresh_with_weak_graph(&root),
            }]),
        );
        root.freeze_graph(builder).unwrap();

        drop(root);
        assert!(graph_owner.upgrade().is_none());
    }

    #[test]
    fn cloning_a_graph_owned_entity_promotes_its_graph_anchor() {
        let root = EntityRuntimeState::default();
        let graph_owner = match &root.graph {
            EntityGraphReference::Strong(graph) => Arc::downgrade(graph),
            EntityGraphReference::Weak(_) => unreachable!(),
        };
        let mut builder = EntityGraphBuilder::default();
        builder.install_relation_list(
            "Owner",
            1,
            "children",
            SmartList::from(vec![GraphChild {
                root: EntityRuntimeState::fresh_with_weak_graph(&root),
            }]),
        );
        root.freeze_graph(builder).unwrap();
        let detached = root
            .resolve_relation_list::<GraphChild>("Owner", 1, "children")
            .unwrap()[0]
            .clone();

        drop(root);
        assert!(graph_owner.upgrade().is_some());
        assert!(detached.root.graph.frozen().is_some());
        drop(detached);
        assert!(graph_owner.upgrade().is_none());
    }

    #[test]
    fn relation_handles_distinguish_loaded_empty_and_not_loaded() {
        let root = EntityRuntimeState::default();
        let mut builder = EntityGraphBuilder::default();
        builder.install_relation_list("Owner", 1, "loaded", SmartList::from(vec![7_u64]));
        builder.install_relation_list::<u64>("Owner", 1, "empty", SmartList::empty());
        builder.install_relation_option("Owner", 1, "present", Some(9_u64));
        builder.install_relation_option::<u64>("Owner", 1, "null", None);
        root.freeze_graph(builder).unwrap();

        let loaded = root.relation_list::<u64>("Owner", 1, "loaded");
        assert_eq!(loaded.state(), LoadedRelation::Loaded);
        assert_eq!(loaded.value().map(|list| list.as_slice()), Some(&[7][..]));

        let empty = root.relation_list::<u64>("Owner", 1, "empty");
        assert_eq!(empty.state(), LoadedRelation::Empty);
        assert!(empty.value().is_some_and(SmartList::is_empty));

        let missing = root.relation_list::<u64>("Owner", 1, "missing");
        assert_eq!(missing.state(), LoadedRelation::NotLoaded);
        assert!(missing.value().is_none());

        let present = root.relation_option::<u64>("Owner", 1, "present");
        assert_eq!(present.state(), LoadedRelation::Loaded);
        assert_eq!(present.value(), Some(&9));

        let null = root.relation_option::<u64>("Owner", 1, "null");
        assert_eq!(null.state(), LoadedRelation::Empty);
        assert!(null.value().is_none());

        let absent = root.relation_option::<u64>("Owner", 1, "absent");
        assert_eq!(absent.state(), LoadedRelation::NotLoaded);
        assert!(absent.value().is_none());
    }
}
