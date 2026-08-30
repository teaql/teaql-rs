use std::collections::BTreeSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use teaql_core::{Entity, MutationValues, Value};

use crate::{
    DataServiceError, GraphNode, GraphOperation, ObjectLocation, RuntimeError, UserContext,
};

tokio::task_local! {
    static GRAPH_FIX_TIME: teaql_core::time::Timestamp;
    static GRAPH_FIX_EVIDENCE: Arc<std::sync::Mutex<Vec<crate::FixEvidence>>>;
}

pub(crate) fn current_graph_fix_time() -> teaql_core::time::Timestamp {
    GRAPH_FIX_TIME
        .try_with(|value| *value)
        .unwrap_or_else(|_| teaql_core::time::Timestamp::now())
}

pub(crate) fn record_graph_fix_evidence(evidence: crate::FixEvidence) {
    let _ = GRAPH_FIX_EVIDENCE.try_with(|current| current.lock().unwrap().push(evidence));
}

// ---------------------------------------------------------------------------
// DynGraphSaver — type-erased graph save capability
// ---------------------------------------------------------------------------

/// Object-safe trait for saving a [`GraphNode`] tree to the database.
///
/// A concrete implementation is registered in [`UserContext`] during setup so
/// that [`Audited::save`] can persist entities without exposing the underlying
/// executor type to business code.
pub(crate) trait DynGraphSaver: Send + Sync {
    fn save_graph_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        node: GraphNode,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>>;

    fn save_ledger_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        node: GraphNode,
        root: crate::EntityRuntimeState,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>>;
}

/// Marker struct that implements [`DynGraphSaver`] for a specific executor type `E`.
///
/// `E` is the full executor type (e.g. `SqlDataServiceExecutor<SqliteDialect, …>`).
/// The struct itself is zero-sized; the actual executor is retrieved from
/// [`UserContext`] at call time.
pub(crate) struct GraphSaverFor<E> {
    _marker: PhantomData<fn() -> E>,
}

impl<E> GraphSaverFor<E> {
    pub(crate) fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<E> DynGraphSaver for GraphSaverFor<E>
where
    E: teaql_data_service::QueryExecutor
        + teaql_data_service::MutationExecutor
        + teaql_data_service::TransactionExecutor
        + Send
        + Sync
        + 'static,
    for<'tx> <E as teaql_data_service::TransactionExecutor>::Tx<'tx>: Send + Sync,
{
    fn save_graph_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        node: GraphNode,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            let entity = node.entity.clone();
            let executor = context
                .require_resource::<E>()
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            let tx = teaql_data_service::TransactionExecutor::begin(&*executor)
                .await
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            let result = {
                let eds = crate::EntityDataService::for_executor(context, entity, &tx);
                eds.save_graph_internal(node).await
            };
            match result {
                Ok(saved) => {
                    teaql_data_service::Transaction::commit(tx)
                        .await
                        .map_err(|e| RuntimeError::Graph(e.to_string()))?;
                    Ok(saved)
                }
                Err(error) => {
                    teaql_data_service::Transaction::rollback(tx)
                        .await
                        .map_err(|e| RuntimeError::Graph(e.to_string()))?;
                    Err(match error {
                        DataServiceError::Runtime(r) => r,
                        other => RuntimeError::Graph(other.to_string()),
                    })
                }
            }
        })
    }

    fn save_ledger_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        mut node: GraphNode,
        root: crate::EntityRuntimeState,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            let entity = node.entity.clone();
            let executor = context
                .require_resource::<E>()
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            let tx = teaql_data_service::TransactionExecutor::begin(&*executor)
                .await
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            let descriptor = context.require_entity(&entity)?;
            let id_prop = descriptor.id_property().ok_or_else(|| {
                RuntimeError::Graph(format!("entity {entity} has no id property"))
            })?;
            let current_id = node
                .values
                .get(&id_prop.name)
                .cloned()
                .unwrap_or(Value::I64(0));
            let root_key = crate::EntityKey::new(entity.clone(), current_id);
            let was_new = root.new_keys().contains(&root_key);
            let was_deleted = root.deleted_keys().contains(&root_key);
            let original_version = root.get_original_version(&root_key);
            let result = {
                let eds = crate::EntityDataService::for_executor(context, &entity, &tx);
                eds.execute_ledger_plan_internal(root.clone()).await
            };
            let generated_ids = match result {
                Ok(ids) => {
                    teaql_data_service::Transaction::commit(tx)
                        .await
                        .map_err(|e| RuntimeError::Graph(e.to_string()))?;
                    ids
                }
                Err(error) => {
                    teaql_data_service::Transaction::rollback(tx)
                        .await
                        .map_err(|e| RuntimeError::Graph(e.to_string()))?;
                    return Err(match error {
                        DataServiceError::Runtime(r) => r,
                        other => RuntimeError::Graph(other.to_string()),
                    });
                }
            };

            if let Some(new_id) = generated_ids.get(&root_key) {
                node.values.insert(id_prop.name.clone(), new_id.clone());
            }
            if let Some(changes) = root.current_change_set().changes().get(&root_key) {
                for (field, value) in changes {
                    node.values.insert(field.clone(), value.clone());
                }
            }
            if let Some(version_prop) = descriptor.version_property() {
                let authoritative_version = saved_version(was_new, was_deleted, original_version);
                if let Some(version) = authoritative_version {
                    node.values
                        .insert(version_prop.name.clone(), Value::I64(version));
                }
            }
            root.clear_committed();
            Ok(node)
        })
    }
}

fn saved_version(was_new: bool, was_deleted: bool, original_version: Option<i64>) -> Option<i64> {
    if was_new {
        Some(1)
    } else if was_deleted {
        original_version.map(|version| -(version.abs() + 1))
    } else {
        original_version.map(|version| version + 1)
    }
}

#[cfg(test)]
mod saved_version_tests {
    use super::saved_version;

    #[test]
    fn create_returns_initial_version() {
        assert_eq!(saved_version(true, false, None), Some(1));
    }

    #[test]
    fn update_returns_incremented_version() {
        assert_eq!(saved_version(false, false, Some(7)), Some(8));
    }

    #[test]
    fn delete_returns_next_negative_version() {
        assert_eq!(saved_version(false, true, Some(7)), Some(-8));
    }
}

// ---------------------------------------------------------------------------
// Standalone graph-node extraction (no executor needed)
// ---------------------------------------------------------------------------

/// Convert a typed entity into a [`GraphNode`] tree.
///
/// This only requires metadata (entity descriptors) from the [`UserContext`],
/// **not** the database executor.  It is the standalone equivalent of
/// [`EntityDataService::graph_node_from_entity`].
pub fn graph_node_from_entity<T: Entity>(
    context: &UserContext,
    entity: T,
) -> Result<GraphNode, RuntimeError> {
    let descriptor = T::entity_descriptor();
    let loaded_fields = descriptor
        .properties
        .iter()
        .filter(|property| entity.is_field_loaded(&property.name))
        .map(|property| Value::Text(property.name.clone()))
        .collect::<Vec<_>>();
    let dirty_fields = entity.dirty_fields();
    let original_values = entity.original_values();
    let is_new = entity.is_new();
    let is_deleted = entity.is_marked_as_delete();
    let comment = entity.get_comment();
    let mut node = graph_node_from_values(context, &descriptor.name, entity.into_values())?;
    node.values
        .insert("_loaded_fields".to_owned(), Value::List(loaded_fields));
    node.dirty_fields = dirty_fields;
    node.original_values = original_values.map(Into::into);
    if is_new {
        node.operation = GraphOperation::Create;
    }
    if is_deleted {
        node.operation = GraphOperation::Remove;
        node.relations.clear();
    }
    if let Some(c) = comment {
        node.set_comment(c);
    }
    Ok(node)
}

/// Recursively convert entity mutation values into a [`GraphNode`] tree.
///
/// Relations are resolved via the entity descriptors stored in `context`.
fn graph_node_from_values(
    context: &UserContext,
    entity: &str,
    values: MutationValues,
) -> Result<GraphNode, RuntimeError> {
    let descriptor = context.require_entity(entity)?;
    let mut node = GraphNode::new(entity);

    for (field, value) in values {
        if field == "_comment" {
            if let Value::Text(comment) = value {
                node.set_comment(comment);
            }
            continue;
        }
        if field == "_dirty_fields" {
            if let Value::List(fields) = value {
                let mut dirty = BTreeSet::new();
                for f in fields {
                    if let Value::Text(t) = f {
                        dirty.insert(t);
                    }
                }
                node.dirty_fields = Some(dirty);
            }
            continue;
        }
        if field == "_original_values" {
            if let Value::Object(orig) = value {
                node.original_values = Some(orig.into());
            }
            continue;
        }
        if field == "_is_new" {
            if matches!(value, Value::Bool(true)) {
                node.operation = GraphOperation::Create;
            }
            continue;
        }
        if field == "_is_deleted" {
            if matches!(value, Value::Bool(true)) {
                node.operation = GraphOperation::Remove;
            }
            continue;
        }
        let Some(relation) = descriptor.relation_by_name(&field) else {
            node.values.insert(field, value);
            continue;
        };

        match value {
            Value::Null => {
                node.relations.entry(field).or_default();
            }
            Value::Object(record) => {
                let child =
                    graph_node_from_values(context, &relation.target_entity, record.into())?;
                node.relations.entry(field).or_default().push(child);
            }
            Value::List(values) => {
                let children = node.relations.entry(field.clone()).or_default();
                for value in values {
                    let Value::Object(record) = value else {
                        return Err(RuntimeError::Graph(format!(
                            "relation {}.{} expects object children, got {:?}",
                            entity, field, value
                        )));
                    };
                    children.push(graph_node_from_values(
                        context,
                        &relation.target_entity,
                        record.into(),
                    )?);
                }
            }
            other => {
                return Err(RuntimeError::Graph(format!(
                    "relation {}.{} expects object/list/null, got {:?}",
                    entity, field, other
                )));
            }
        }
    }

    Ok(node)
}

fn merge_relation_mutations_into_root(
    root: &crate::EntityRuntimeState,
    node: &GraphNode,
) -> Result<(), RuntimeError> {
    for children in node.relations.values() {
        for child in children {
            let id = child.values.get("id").cloned().ok_or_else(|| {
                RuntimeError::Graph(format!(
                    "related mutation {} is missing its id",
                    child.entity
                ))
            })?;
            let key = crate::EntityKey::new(child.entity.clone(), id);

            match child.operation {
                GraphOperation::Create => {
                    root.mark_as_new(key.clone());
                    for (field, value) in &child.values {
                        root.set(key.clone(), field, value.clone());
                    }
                }
                GraphOperation::Upsert => {
                    if let Some(fields) = &child.dirty_fields {
                        for field in fields {
                            if let Some(value) = child.values.get(field) {
                                root.set(key.clone(), field, value.clone());
                            }
                        }
                    }
                }
                GraphOperation::Remove => root.mark_as_delete(key.clone()),
                GraphOperation::Reference => {}
            }

            if let Some(version) = child
                .original_values
                .as_ref()
                .and_then(|values| values.get("version"))
                .and_then(Value::try_i64)
            {
                root.set_original_version(key, version);
            }
            merge_relation_mutations_into_root(root, child)?;
        }
    }
    Ok(())
}

fn hydrate_ledger_relations(
    context: &UserContext,
    root: &crate::EntityRuntimeState,
    node: &mut GraphNode,
    visited: &mut BTreeSet<crate::EntityKey>,
) -> Result<(), RuntimeError> {
    let descriptor = context.require_entity(&node.entity)?;
    for relation in &descriptor.relations {
        let Some(local_value) = node.values.get(&relation.local_key).cloned() else {
            continue;
        };
        let existing = node.relations.entry(relation.name.clone()).or_default();
        let existing_keys = existing
            .iter()
            .filter_map(|child| {
                child
                    .values
                    .get("id")
                    .cloned()
                    .map(|id| crate::EntityKey::new(child.entity.clone(), id))
            })
            .collect::<BTreeSet<_>>();
        let mut discovered = Vec::new();
        for (key, changes) in root.current_change_set().changes() {
            if key.entity.as_ref() != relation.target_entity || existing_keys.contains(key) {
                continue;
            }
            let foreign_value = if relation.foreign_key == "id" {
                Some(&key.id)
            } else {
                changes.get(&relation.foreign_key)
            };
            if foreign_value != Some(&local_value) || !visited.insert(key.clone()) {
                continue;
            }
            let mut values: crate::EntityValues = changes.clone().into();
            values
                .entry("id".to_owned())
                .or_insert_with(|| key.id.clone());
            let operation = if root.deleted_keys().contains(key) {
                GraphOperation::Remove
            } else if root.new_keys().contains(key) || root.get_original_version(key).is_none() {
                GraphOperation::Create
            } else {
                GraphOperation::Upsert
            };
            let mut child = GraphNode::new(key.entity.to_string());
            child.values = values;
            child.operation = operation;
            hydrate_ledger_relations(context, root, &mut child, visited)?;
            discovered.push(child);
        }
        existing.extend(discovered);
    }
    Ok(())
}

fn preflight_graph(
    context: &UserContext,
    node: &mut GraphNode,
    location: &ObjectLocation,
    root: Option<&crate::EntityRuntimeState>,
) -> Result<(), RuntimeError> {
    if !matches!(
        node.operation,
        GraphOperation::Remove | GraphOperation::Reference
    ) {
        let before = node.values.clone();
        let status = match node.operation {
            GraphOperation::Create => crate::CheckObjectStatus::Create,
            GraphOperation::Upsert => crate::CheckObjectStatus::Update,
            GraphOperation::Remove | GraphOperation::Reference => unreachable!(),
        };
        crate::mark_entity_status(&mut node.values, status);
        let result = context.check_and_fix_values_at(&node.entity, &mut node.values, location);
        crate::clear_entity_status(&mut node.values);
        result?;

        if let Some(root) = root {
            if let Some(id) = node.values.get("id").cloned() {
                let key = crate::EntityKey::new(node.entity.clone(), id);
                for (field, value) in &node.values {
                    if before.get(field) != Some(value) {
                        root.set(key.clone(), field.clone(), value.clone());
                    }
                }
            }
        }
    }

    for (relation, children) in &mut node.relations {
        for (index, child) in children.iter_mut().enumerate() {
            let child_location = location.clone().member(relation).element(index);
            preflight_graph(context, child, &child_location, root)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// AuditedSaveExt — the `.save(&context)` method on `Audited<T>`
// ---------------------------------------------------------------------------

/// Extension trait that provides the `.save(&context)` method on [`Audited<T>`](teaql_core::Audited).
///
/// # Example
/// ```ignore
/// use teaql_runtime::AuditedSaveExt;
///
/// school.audit_as("创建学校").save(&context).await?;
/// ```
pub trait AuditedSaveExt {
    type Entity;

    fn save<'a>(
        self,
        context: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Entity, RuntimeError>> + Send + 'a>>;
}

impl<T> AuditedSaveExt for teaql_core::Audited<T>
where
    T: Entity + Send + 'static,
{
    type Entity = T;

    fn save<'a>(
        self,
        context: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Entity, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            let entity_name = T::entity_descriptor().name;
            let entity = self.into_entity(); // applies comment onto the entity
            let mut node = graph_node_from_entity(context, entity)?;
            preflight_graph(context, &mut node, &ObjectLocation::root(), None)?;
            let saver = context
                .require_resource::<Arc<dyn DynGraphSaver>>()
                .map_err(|e| {
                    RuntimeError::Graph(format!(
                        "no DynGraphSaver registered — did you call register_executor()? ({})",
                        e
                    ))
                })?;
            let saved = saver.save_graph_dyn(context, node).await?;
            T::from_compact_row(teaql_core::CompactRow::from_map(saved.values.into()))
                .map_err(|e| RuntimeError::Graph(e.to_string()))
        })
    }
}

/// Persist an audited generated entity, including pending ledger changes that
/// may span multiple related entities sharing the same [`EntityRuntimeState`](crate::EntityRuntimeState).
///
/// Generated service crates use this as the implementation behind
/// `entity.audit_as("why").save(&context)`. The audited wrapper is required by the
/// function signature; no unaudited entity write entry point is exposed.
#[doc(hidden)]
pub async fn save_audited_ledger_entity<T>(
    audited: teaql_core::Audited<T>,
    context: &UserContext,
) -> Result<T, RuntimeError>
where
    T: crate::LedgerEntity + Send + 'static,
{
    let evidence = Arc::new(std::sync::Mutex::new(Vec::new()));
    let result = GRAPH_FIX_TIME
        .scope(
            teaql_core::time::Timestamp::now(),
            GRAPH_FIX_EVIDENCE.scope(
                evidence.clone(),
                save_audited_ledger_entity_inner(audited, context),
            ),
        )
        .await;
    context.replace_last_fix_evidence(evidence.lock().unwrap().clone());
    result
}

async fn save_audited_ledger_entity_inner<T>(
    audited: teaql_core::Audited<T>,
    context: &UserContext,
) -> Result<T, RuntimeError>
where
    T: crate::LedgerEntity + Send + 'static,
{
    let entity_name = T::entity_descriptor().name;
    let entity = audited.into_entity();
    let root = entity.entity_runtime_state();
    let mut node = graph_node_from_entity(context, entity)?;
    let saver = context
        .require_resource::<Arc<dyn DynGraphSaver>>()
        .map_err(|e| {
            RuntimeError::Graph(format!(
                "no DynGraphSaver registered — did you call register_executor()? ({e})"
            ))
        })?;

    if let Some(root) = root {
        let root_id = node.values.get("id").cloned().unwrap_or(Value::I64(0));
        let root_key = crate::EntityKey::new(node.entity.clone(), root_id);
        if let Some(changes) = root.current_change_set().changes().get(&root_key) {
            for (field, value) in changes {
                node.values.insert(field.clone(), value.clone());
            }
        }
        let mut visited = BTreeSet::from([root_key]);
        hydrate_ledger_relations(context, &root, &mut node, &mut visited)?;
        preflight_graph(context, &mut node, &ObjectLocation::root(), Some(&root))?;
        merge_relation_mutations_into_root(&root, &node)?;
        let has_ledger_changes = !root.current_change_set().changes().is_empty()
            || !root.deleted_keys().is_empty()
            || !root.new_keys().is_empty();
        if has_ledger_changes {
            let saved = saver.save_ledger_dyn(context, node, root).await?;
            return T::from_compact_row(teaql_core::CompactRow::from_map(saved.values.into()))
                .map_err(|e| RuntimeError::Graph(e.to_string()));
        }
    }

    preflight_graph(context, &mut node, &ObjectLocation::root(), None)?;
    let saved = saver.save_graph_dyn(context, node).await?;
    T::from_compact_row(teaql_core::CompactRow::from_map(saved.values.into()))
        .map_err(|e| RuntimeError::Graph(e.to_string()))
}
