use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use teaql_core::{Record, TraceNode, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphOperation {
    Upsert,
    Create,
    Reference,
    Remove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphMutationKind {
    Create,
    Update,
    Delete,
    Reference,
}

impl GraphMutationKind {
    pub fn for_update(is_update: bool) -> Self {
        match is_update {
            true => Self::Update,
            false => Self::Create,
        }
    }
}

/// A persistent linked-list token for hierarchical trace context.
///
/// Each token holds the trace info for one graph node and an `Arc` pointer
/// to its parent's token. The full trace chain is only materialized when
/// explicitly requested via [`recover_trace_chain()`], giving us zero-cost
/// propagation during the flatten phase.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceScopeToken {
    /// Shared pointer to the parent scope (zero-copy link).
    pub parent: Option<Arc<TraceScopeToken>>,
    /// The trace metadata for this scope level.
    pub track: TraceNode,
    /// The item_index of the PlanItem that created this scope (for debugging).
    pub node_index: u64,
}

impl TraceScopeToken {
    /// Lazily recover the full trace chain by walking the parent pointers.
    /// Only called when an event consumer actually needs the chain.
    pub fn recover_trace_chain(&self) -> Vec<TraceNode> {
        let mut chain = Vec::new();
        let mut current: Option<&TraceScopeToken> = Some(self);
        while let Some(token) = current {
            if !token.track.comment.is_empty() {
                chain.push(token.track.clone());
            }
            current = token.parent.as_deref();
        }
        chain.reverse();
        chain
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphMutationPlanItem {
    pub entity: String,
    pub kind: GraphMutationKind,
    pub values: Record,
    pub update_fields: Vec<String>,
    /// Monotonically increasing index assigned at push time (for debugging).
    pub item_index: u64,
    /// Lazy trace context — only materialized into a Vec<TraceNode> on demand.
    pub scope_token: Option<Arc<TraceScopeToken>>,
    pub old_values: Option<Record>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphMutationBatch {
    pub entity: String,
    pub kind: GraphMutationKind,
    pub update_fields: Vec<String>,
    pub items: Vec<GraphMutationPlanItem>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct GraphMutationPlan {
    pub planned_root: Option<GraphNode>,
    pub items: Vec<GraphMutationPlanItem>,
    pub batches: Vec<GraphMutationBatch>,
    /// Auto-incrementing counter for item_index assignment.
    pub next_item_index: u64,
    /// Keep track of visited nodes to avoid infinite loops and redundant updates
    pub visited_nodes: std::collections::HashSet<(String, String)>,
}

impl GraphMutationPlan {
    pub fn push(
        &mut self,
        entity: impl Into<String>,
        kind: GraphMutationKind,
        values: Record,
        update_fields: Vec<String>,
        scope_token: Option<Arc<TraceScopeToken>>,
        old_values: Option<Record>,
    ) {
        let index = self.next_item_index;
        self.next_item_index += 1;
        self.items.push(GraphMutationPlanItem {
            entity: entity.into(),
            kind,
            values,
            update_fields,
            item_index: index,
            scope_token,
            old_values,
        });
    }

    pub fn rebuild_batches(&mut self) {
        let mut grouped: BTreeMap<
            (String, GraphMutationKind, Vec<String>),
            Vec<GraphMutationPlanItem>,
        > = BTreeMap::new();
        for item in &self.items {
            let update_fields = match item.kind {
                GraphMutationKind::Update => item.update_fields.clone(),
                _ => Vec::new(),
            };
            grouped
                .entry((item.entity.clone(), item.kind, update_fields))
                .or_default()
                .push(item.clone());
        }
        self.batches = grouped
            .into_iter()
            .map(
                |((entity, kind, update_fields), items)| GraphMutationBatch {
                    entity,
                    kind,
                    update_fields,
                    items,
                },
            )
            .collect();
    }

    pub fn grouped_counts(&self) -> BTreeMap<(String, GraphMutationKind), usize> {
        let mut counts = BTreeMap::new();
        for batch in &self.batches {
            *counts
                .entry((batch.entity.clone(), batch.kind))
                .or_insert(0) += batch.items.len();
        }
        counts
    }

    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

pub fn sorted_update_fields(
    values: &Record,
    excluded: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let excluded = excluded.into_iter().collect::<BTreeSet<_>>();
    values
        .keys()
        .filter(|field| !excluded.contains(*field))
        .cloned()
        .collect()
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphNode {
    pub entity: String,
    pub values: Record,
    pub relations: BTreeMap<String, Vec<GraphNode>>,
    pub operation: GraphOperation,
    /// Annotation comment: carries business intent metadata through graph save.
    /// Not persisted to the database — used for observability (SQL logs, audit trails).
    pub comment: Option<String>,
    /// Fields modified via `update_*()` methods (dirty tracking).
    /// `None` = all fields (new entity or no tracking available).
    /// `Some(set)` = only these fields were modified — UPDATE should only include them.
    /// This is the Rust equivalent of Java's `entity.getUpdatedProperties()`.
    pub dirty_fields: Option<BTreeSet<String>>,
    /// L1 Cache snapshot of the entity values exactly as they were loaded from the database.
    /// Used by the Event Engine to eliminate redundant old_value queries during auditing.
    pub original_values: Option<Record>,
}

impl GraphNode {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            values: Record::new(),
            relations: BTreeMap::new(),
            operation: GraphOperation::Upsert,
            comment: None,
            dirty_fields: None,
            original_values: None,
        }
    }

    pub fn operation(mut self, operation: GraphOperation) -> Self {
        self.operation = operation;
        self
    }

    pub fn reference(mut self) -> Self {
        self.operation = GraphOperation::Reference;
        self
    }

    pub fn remove(mut self) -> Self {
        self.operation = GraphOperation::Remove;
        self
    }

    pub fn value(mut self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.values.insert(field.into(), value.into());
        self
    }

    pub fn relation(mut self, name: impl Into<String>, node: GraphNode) -> Self {
        self.relations.entry(name.into()).or_default().push(node);
        self
    }

    pub fn relations(
        mut self,
        name: impl Into<String>,
        nodes: impl IntoIterator<Item = GraphNode>,
    ) -> Self {
        self.relations.entry(name.into()).or_default().extend(nodes);
        self
    }

    pub fn id(&self) -> Option<&Value> {
        self.values.get("id")
    }

    /// Set an annotation comment on this graph node.
    /// The comment propagates through the graph save process for observability.
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Set an annotation comment by mutable reference.
    pub fn set_comment(&mut self, comment: impl Into<String>) {
        self.comment = Some(comment.into());
    }
}

// ---------------------------------------------------------------------------
// Hierarchical Comment Propagation (Scoped Cons List)
// ---------------------------------------------------------------------------

/// A stack-allocated scope node forming a parent-pointer cons list.
///
/// Each node lives on the call stack of the recursive graph save function.
/// Child nodes hold a `&'a` reference to their parent's stack frame,
/// giving us thread-safe, lock-free, zero-overhead hierarchical comment tracking.
#[derive(Debug)]
pub struct ScopedCommentNode<'a> {
    /// Reference to the parent scope (lives on the caller's stack frame)
    pub parent: Option<&'a ScopedCommentNode<'a>>,
    pub track: teaql_core::TraceNode,
}

impl<'a> ScopedCommentNode<'a> {
    pub fn to_trace_chain(&self) -> Vec<teaql_core::TraceNode> {
        let mut chain = Vec::new();
        let mut current: Option<&ScopedCommentNode<'_>> = Some(self);

        while let Some(node) = current {
            if !node.track.comment.is_empty() {
                chain.push(node.track.clone());
            }
            current = node.parent;
        }

        chain.reverse();
        chain
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hierarchical_trace_chain_recovery() {
        let root_trace = TraceNode {
            entity_type: "User".to_string(),
            entity_id: Some(1),
            comment: "Create User".to_string(),
        };

        let child_trace = TraceNode {
            entity_type: "Profile".to_string(),
            entity_id: None,
            comment: "Create Profile".to_string(),
        };

        let empty_comment_trace = TraceNode {
            entity_type: "AuditLog".to_string(),
            entity_id: None,
            comment: "".to_string(),
        };

        // Test ScopedCommentNode
        let root_scope = ScopedCommentNode {
            parent: None,
            track: root_trace.clone(),
        };
        let child_scope = ScopedCommentNode {
            parent: Some(&root_scope),
            track: child_trace.clone(),
        };
        let empty_scope = ScopedCommentNode {
            parent: Some(&child_scope),
            track: empty_comment_trace.clone(),
        };

        let chain = empty_scope.to_trace_chain();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0], root_trace);
        assert_eq!(chain[1], child_trace);

        // Test TraceScopeToken
        let root_token = Arc::new(TraceScopeToken {
            parent: None,
            track: root_trace.clone(),
            node_index: 0,
        });
        let child_token = Arc::new(TraceScopeToken {
            parent: Some(root_token),
            track: child_trace.clone(),
            node_index: 1,
        });
        let empty_token = Arc::new(TraceScopeToken {
            parent: Some(child_token),
            track: empty_comment_trace,
            node_index: 2,
        });

        let chain = empty_token.recover_trace_chain();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0], root_trace);
        assert_eq!(chain[1], child_trace);
    }
}
