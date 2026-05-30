use crate::{Entity, Record, TeaqlEntity};

/// Operation hint for an entity graph node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityGraphOperation {
    /// Upsert: insert if new, update if exists (default).
    Save,
    /// Delete: soft-delete the entity.
    Delete,
}

/// A single node in an annotated entity graph.
///
/// Carries the entity's record data, an optional business-intent comment,
/// and child nodes keyed by relation name.
#[derive(Debug, Clone)]
pub struct EntityGraphNode {
    pub entity_type: String,
    pub record: Record,
    pub comment: Option<String>,
    pub operation: EntityGraphOperation,
    pub children: Vec<(String, EntityGraphNode)>,
}

/// Builder for constructing an annotated entity graph that preserves
/// comment trace chains through the save pipeline.
///
/// # Example
///
/// ```ignore
/// let graph = EntityGraph::new(task)
///     .comment("Create task 'Deploy v2'")
///     .child("task_execution_log_list",
///         EntityGraph::new(log)
///             .comment("Create task 'Deploy v2'"))
///     .build();
/// ```
pub struct EntityGraphBuilder {
    node: EntityGraphNode,
}

impl EntityGraphBuilder {
    /// Set a business-intent comment on this node.
    /// The comment will appear in SQL debug logs and audit trails
    /// as part of the hierarchical trace chain.
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.node.comment = Some(comment.into());
        self
    }

    /// Mark this node for deletion instead of save.
    pub fn delete(mut self) -> Self {
        self.node.operation = EntityGraphOperation::Delete;
        self
    }

    /// Attach a child entity under the given relation name.
    pub fn child(mut self, relation: impl Into<String>, child: EntityGraphBuilder) -> Self {
        self.node.children.push((relation.into(), child.node));
        self
    }

    /// Finalize and produce the `EntityGraph`.
    pub fn build(self) -> EntityGraph {
        EntityGraph { root: self.node }
    }
}

/// An annotated entity graph ready for saving.
///
/// Unlike raw `entity.save()`, this structure preserves business-intent
/// comments at every hop in the graph, producing proper trace chains
/// in SQL logs and audit trails.
pub struct EntityGraph {
    pub root: EntityGraphNode,
}

impl EntityGraph {
    /// Start building from an entity.
    pub fn new<T: Entity + TeaqlEntity>(entity: T) -> EntityGraphBuilder {
        EntityGraphBuilder {
            node: EntityGraphNode {
                entity_type: T::entity_descriptor().name.clone(),
                record: entity.into_record(),
                comment: None,
                operation: EntityGraphOperation::Save,
                children: Vec::new(),
            },
        }
    }
}
