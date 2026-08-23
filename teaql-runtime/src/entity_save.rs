use std::collections::BTreeSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use teaql_core::{Entity, Record, Value};

use crate::{DataServiceError, GraphNode, GraphOperation, RuntimeError, UserContext};

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
        root: crate::EntityRoot,
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
        + Send
        + Sync
        + 'static,
{
    fn save_graph_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        node: GraphNode,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            let entity = node.entity.clone();
            let eds = context
                .entity_data_service::<E>(entity)
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            eds.save_graph_internal(node).await.map_err(|e| match e {
                DataServiceError::Runtime(r) => r,
                other => RuntimeError::Graph(other.to_string()),
            })
        })
    }

    fn save_ledger_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        mut node: GraphNode,
        root: crate::EntityRoot,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            let entity = node.entity.clone();
            let eds = context
                .entity_data_service::<E>(&entity)
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            let generated_ids = eds
                .execute_ledger_plan_internal(root.clone())
                .await
                .map_err(|e| match e {
                    DataServiceError::Runtime(r) => r,
                    other => RuntimeError::Graph(other.to_string()),
                })?;

            let descriptor = context.require_entity(&entity).unwrap();
            if let Some(id_prop) = descriptor.id_property() {
                let current_id = node
                    .values
                    .get(&id_prop.name)
                    .cloned()
                    .unwrap_or(Value::I64(0));
                let root_key = crate::EntityKey::new(entity.clone(), current_id);
                if let Some(new_id) = generated_ids.get(&root_key) {
                    node.values.insert(id_prop.name.clone(), new_id.clone());
                }
                if let Some(changes) = root.current_change_set().changes().get(&root_key) {
                    for (field, value) in changes {
                        node.values.insert(field.clone(), value.clone());
                    }
                }
            }
            root.clear_committed();
            Ok(node)
        })
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
    let dirty_fields = entity.dirty_fields();
    let original_values = entity.original_values();
    let is_deleted = entity.is_marked_as_delete();
    let comment = entity.get_comment();
    let mut node = graph_node_from_record(context, &descriptor.name, entity.into_values().into())?;
    node.dirty_fields = dirty_fields;
    node.original_values = original_values.map(Into::into);
    if is_deleted {
        node.operation = GraphOperation::Remove;
        node.relations.clear();
    }
    if let Some(c) = comment {
        node.set_comment(c);
    }
    Ok(node)
}

/// Recursively convert a [`Record`] into a [`GraphNode`] tree.
///
/// Relations are resolved via the entity descriptors stored in `context`.
fn graph_node_from_record(
    context: &UserContext,
    entity: &str,
    record: Record,
) -> Result<GraphNode, RuntimeError> {
    let descriptor = context.require_entity(entity)?;
    let mut node = GraphNode::new(entity);

    for (field, value) in record {
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
        let Some(relation) = descriptor.relation_by_name(&field) else {
            node.values.insert(field, value);
            continue;
        };

        match value {
            Value::Null => {
                node.relations.entry(field).or_default();
            }
            Value::Object(record) => {
                let child = graph_node_from_record(context, &relation.target_entity, record)?;
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
                    children.push(graph_node_from_record(
                        context,
                        &relation.target_entity,
                        record,
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
            let node = graph_node_from_entity(context, entity)?;
            let saver = context
                .require_resource::<Arc<dyn DynGraphSaver>>()
                .map_err(|e| {
                    RuntimeError::Graph(format!(
                        "no DynGraphSaver registered — did you call register_executor()? ({})",
                        e
                    ))
                })?;
            let saved = saver.save_graph_dyn(context, node).await?;
            T::from_compact_row(teaql_core::CompactRow::from_record(saved.values.into()))
                .map_err(|e| RuntimeError::Graph(e.to_string()))
        })
    }
}

/// Persist an audited generated entity, including pending ledger changes that
/// may span multiple related entities sharing the same [`EntityRoot`](crate::EntityRoot).
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
    let entity_name = T::entity_descriptor().name;
    let entity = audited.into_entity();
    let root = entity.entity_root();
    let node = graph_node_from_entity(context, entity)?;
    let saver = context
        .require_resource::<Arc<dyn DynGraphSaver>>()
        .map_err(|e| {
            RuntimeError::Graph(format!(
                "no DynGraphSaver registered — did you call register_executor()? ({e})"
            ))
        })?;

    if let Some(root) = root {
        let has_ledger_changes = !root.current_change_set().changes().is_empty()
            || !root.deleted_keys().is_empty()
            || !root.new_keys().is_empty();
        if has_ledger_changes {
            let saved = saver.save_ledger_dyn(context, node, root).await?;
            return T::from_compact_row(teaql_core::CompactRow::from_record(saved.values.into()))
                .map_err(|e| RuntimeError::Graph(e.to_string()));
        }
    }

    let saved = saver.save_graph_dyn(context, node).await?;
    T::from_compact_row(teaql_core::CompactRow::from_record(saved.values.into()))
        .map_err(|e| RuntimeError::Graph(e.to_string()))
}
