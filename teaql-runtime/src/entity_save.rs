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
pub trait DynGraphSaver: Send + Sync {
    fn save_graph_dyn<'a>(
        &'a self,
        ctx: &'a UserContext,
        entity: &'a str,
        node: GraphNode,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>>;
}

/// Marker struct that implements [`DynGraphSaver`] for a specific executor type `E`.
///
/// `E` is the full executor type (e.g. `SqlDataServiceExecutor<SqliteDialect, …>`).
/// The struct itself is zero-sized; the actual executor is retrieved from
/// [`UserContext`] at call time.
pub struct GraphSaverFor<E> {
    _marker: PhantomData<fn() -> E>,
}

impl<E> GraphSaverFor<E> {
    pub fn new() -> Self {
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
        ctx: &'a UserContext,
        entity: &'a str,
        node: GraphNode,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            let eds = ctx
                .entity_data_service::<E>(entity)
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            eds.save_graph(node).await.map_err(|e| match e {
                DataServiceError::Runtime(r) => r,
                other => RuntimeError::Graph(other.to_string()),
            })
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
    ctx: &UserContext,
    entity: T,
) -> Result<GraphNode, RuntimeError> {
    let descriptor = T::entity_descriptor();
    let dirty_fields = entity.dirty_fields();
    let original_values = entity.original_values();
    let is_deleted = entity.is_marked_as_delete();
    let comment = entity.get_comment();
    let mut node = graph_node_from_record(ctx, &descriptor.name, entity.into_record())?;
    node.dirty_fields = dirty_fields;
    node.original_values = original_values;
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
/// Relations are resolved via the entity descriptors stored in `ctx`.
fn graph_node_from_record(
    ctx: &UserContext,
    entity: &str,
    record: Record,
) -> Result<GraphNode, RuntimeError> {
    let descriptor = ctx.require_entity(entity)?;
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
                node.original_values = Some(orig);
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
                let child = graph_node_from_record(ctx, &relation.target_entity, record)?;
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
                        ctx,
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
// AuditedSaveExt — the `.save(&ctx)` method on `Audited<T>`
// ---------------------------------------------------------------------------

/// Extension trait that provides the `.save(&ctx)` method on [`Audited<T>`](teaql_core::Audited).
///
/// # Example
/// ```ignore
/// use teaql_runtime::AuditedSaveExt;
///
/// school.audit_as("创建学校").save(&ctx).await?;
/// ```
pub trait AuditedSaveExt {
    fn save<'a>(
        self,
        ctx: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>>;
}

impl<T> AuditedSaveExt for teaql_core::Audited<T>
where
    T: Entity + Send + 'static,
{
    fn save<'a>(
        self,
        ctx: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            let entity_name = T::entity_descriptor().name;
            let entity = self.into_entity(); // applies comment onto the entity
            let node = graph_node_from_entity(ctx, entity)?;
            let saver = ctx
                .require_resource::<Arc<dyn DynGraphSaver>>()
                .map_err(|e| {
                    RuntimeError::Graph(format!(
                        "no DynGraphSaver registered — did you call register_executor()? ({})",
                        e
                    ))
                })?;
            saver.save_graph_dyn(ctx, &entity_name, node).await
        })
    }
}
