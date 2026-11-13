//! Immutable request headers. Parent links are execution-owned, not query JSON
//! or a Context-global stack. Sharing a header never clones a parent's AST.
use std::sync::Arc;

use crate::{QueryIntent, RequestIntentError, RequestKind, TraceKind, TraceNode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOperationKind {
    NamedCount,
    Facet,
    RelationCount,
    RelationSelect,
    Aggregate,
    Pagination,
}

impl QueryOperationKind {
    fn label(self) -> &'static str {
        match self {
            Self::NamedCount => "named-count",
            Self::Facet => "facet",
            Self::RelationCount => "relation-count",
            Self::RelationSelect => "relation-select",
            Self::Aggregate => "aggregate",
            Self::Pagination => "pagination",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct QueryOperation {
    kind: QueryOperationKind,
    method: Option<String>,
    alias: Option<String>,
    relation: Option<String>,
}

impl std::fmt::Debug for QueryOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryOperation")
            .field("kind", &self.kind)
            .field("method", &self.method)
            .field("has_alias", &self.alias.is_some())
            .finish()
    }
}

impl QueryOperation {
    /// Exact generator-supplied native identifier, never reconstructed from SQL.
    pub fn new(kind: QueryOperationKind, method: impl Into<String>) -> Self {
        let method = method.into();
        assert!(
            method
                .bytes()
                .enumerate()
                .all(|(i, c)| c == b'_' || c.is_ascii_alphabetic() || i > 0 && c.is_ascii_digit())
                && !method.is_empty(),
            "QUERY_OPERATION_INVALID: native method must be an ASCII identifier"
        );
        Self {
            kind,
            method: Some(method),
            alias: None,
            relation: None,
        }
    }
    pub fn framework(kind: QueryOperationKind) -> Self {
        Self {
            kind,
            method: None,
            alias: None,
            relation: None,
        }
    }
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }
    pub fn with_relation(mut self, relation: impl Into<String>) -> Self {
        self.relation = Some(relation.into());
        self
    }
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }
    pub fn relation(&self) -> Option<&str> {
        self.relation.as_deref()
    }
    pub fn kind(&self) -> QueryOperationKind {
        self.kind
    }
    pub fn method(&self) -> Option<&str> {
        self.method.as_deref()
    }
    pub fn comment(&self) -> String {
        // Rust Debug uses native string escapes (including \u{...}), unlike
        // JSON's \uXXXX form, which is not a valid Rust string literal.
        let literal = |text: &str| format!("{text:?}");
        if let Some(method) = &self.method {
            let argument = self.alias.as_deref().map(literal).unwrap_or_else(|| {
                if self.kind == QueryOperationKind::RelationSelect {
                    "...".to_owned()
                } else {
                    String::new()
                }
            });
            format!("{method}({argument})")
        } else {
            format!(
                "framework: {}{}{}",
                self.kind.label(),
                self.relation
                    .as_ref()
                    .map(|value| format!(" {value}"))
                    .unwrap_or_default(),
                self.alias
                    .as_deref()
                    .map(|value| format!(" {}", literal(value)))
                    .unwrap_or_default()
            )
        }
    }
}

struct Header {
    parent: Option<QueryRequestScope>,
    name: String,
    comment: String,
    intent: Arc<QueryIntent>,
    operation: Option<QueryOperation>,
}

/// A read-only view of an actual request's identity and direct parent. Bodies
/// remain in their own SelectQuery; a relation shares only this small header.
#[derive(Clone)]
pub struct QueryRequestScope(Arc<Header>);

/// Invocation-local diagnostics, not a writable request-parent API. Equal
/// comments never merge different headers; ids are local serialization indices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRequestTraceNode {
    pub id: usize,
    pub parent_id: Option<usize>,
    pub kind: TraceKind,
    pub name: String,
    pub detail: String,
    pub method: Option<String>,
}

impl QueryRequestTraceNode {
    pub fn from_scopes(scopes: &[QueryRequestScope]) -> Vec<Self> {
        use std::collections::{HashMap, HashSet};
        if scopes.len() < 2 {
            return Vec::new();
        }
        let identity = |scope: &QueryRequestScope| Arc::as_ptr(&scope.0) as usize;
        if scopes.iter().map(identity).collect::<HashSet<_>>().len() < 2 {
            return Vec::new(); // Ordinary traceChain already represents one request.
        }
        let mut indices = HashMap::new();
        let mut nodes = Vec::new();
        for leaf in scopes {
            let mut ancestry = Vec::new();
            let mut current = Some(leaf);
            while let Some(scope) = current {
                ancestry.push(scope);
                current = scope.parent_request();
            }
            for scope in ancestry.into_iter().rev() {
                let key = identity(scope);
                if indices.contains_key(&key) {
                    continue;
                }
                let parent_id = if let Some(parent) = scope.parent_request() {
                    *indices
                        .get(&identity(parent))
                        .expect("parent emitted before child")
                } else {
                    let id = nodes.len();
                    nodes.push(Self {
                        id,
                        parent_id: None,
                        kind: TraceKind::Purpose,
                        name: scope.request_name().into(),
                        detail: scope.purpose().into(),
                        method: None,
                    });
                    id
                };
                let id = nodes.len();
                nodes.push(Self {
                    id,
                    parent_id: Some(parent_id),
                    kind: TraceKind::Comment,
                    name: scope.request_name().into(),
                    detail: scope.comment().into(),
                    method: scope
                        .operation()
                        .and_then(QueryOperation::method)
                        .map(str::to_owned),
                });
                indices.insert(key, id);
            }
        }
        nodes
    }

    /// Common text-log/telemetry shape. Callers must project privacy first.
    pub fn to_json_value(nodes: &[Self]) -> serde_json::Value {
        serde_json::Value::Array(
            nodes
                .iter()
                .map(|node| {
                    let mut value = serde_json::json!({"id":node.id,"parentId":node.parent_id,
                "kind":if node.kind == TraceKind::Purpose {"purpose"} else {"comment"},
                "name":node.name,"detail":node.detail});
                    if let Some(method) = &node.method {
                        value["method"] = method.clone().into();
                    }
                    value
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod request_trace_tree_tests {
    use super::*;

    fn root() -> QueryRequestScope {
        QueryRequestScope::root(
            "School",
            QueryIntent::new("Load schools", "Render the dashboard").unwrap(),
        )
    }

    #[test]
    fn one_request_uses_existing_chain_without_a_duplicate_tree() {
        let root = root();
        assert!(QueryRequestTraceNode::from_scopes(&[]).is_empty());
        assert!(QueryRequestTraceNode::from_scopes(&[root.clone()]).is_empty());
        assert!(QueryRequestTraceNode::from_scopes(&[root.clone(), root]).is_empty());
    }

    #[test]
    fn identical_comments_on_siblings_do_not_invent_ancestry() {
        let root = root();
        let left = root
            .derive_child("Kind", Some("Same local comment"))
            .unwrap();
        let right = root
            .derive_child("Kind", Some("Same local comment"))
            .unwrap();
        let nodes = QueryRequestTraceNode::from_scopes(&[left.clone(), right, root, left]);
        assert_eq!(nodes.len(), 4);
        assert_eq!(nodes[0].kind, TraceKind::Purpose);
        assert_eq!(nodes[0].parent_id, None);
        assert_eq!(nodes[2].parent_id, Some(1));
        assert_eq!(nodes[3].parent_id, Some(1));
        assert_eq!(nodes[2].detail, nodes[3].detail);
        for (index, node) in nodes.iter().enumerate() {
            assert_eq!(node.id, index);
            assert!(node.parent_id.is_none_or(|parent| parent < index));
        }
    }

    #[test]
    fn native_operation_and_local_comment_are_separate_nodes() {
        let root = root();
        let operation = root.derive_operation(
            "Kind",
            QueryOperation::new(QueryOperationKind::Aggregate, "roll_up_to_kind"),
        );
        let child = operation
            .derive_child("Kind", Some("Group visible types"))
            .unwrap();
        let nodes = QueryRequestTraceNode::from_scopes(&[child, root]);
        let json = QueryRequestTraceNode::to_json_value(&nodes);
        assert_eq!(json[0]["parentId"], serde_json::Value::Null);
        assert_eq!(json[2]["method"], "roll_up_to_kind");
        assert_eq!(json[2]["detail"], "roll_up_to_kind()");
        assert_eq!(json[3]["parentId"], 2);
        assert_eq!(json[3]["detail"], "Group visible types");
        assert!(json[3].get("method").is_none());
    }

    #[test]
    fn independent_equal_roots_are_not_merged_by_content() {
        let nodes = QueryRequestTraceNode::from_scopes(&[root(), root()]);
        assert_eq!(nodes.len(), 4);
        assert_eq!(nodes[0].parent_id, None);
        assert_eq!(nodes[2].parent_id, None);
        assert_eq!(nodes[3].parent_id, Some(2));
    }
}

impl std::fmt::Debug for QueryRequestScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryRequestScope")
            .field("name", &self.0.name)
            .field("has_parent", &self.0.parent.is_some())
            .finish()
    }
}
impl PartialEq for QueryRequestScope {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for QueryRequestScope {}

impl QueryRequestScope {
    pub fn root(name: impl Into<String>, intent: QueryIntent) -> Self {
        Self(Arc::new(Header {
            parent: None,
            name: name.into(),
            comment: intent.comment().to_owned(),
            intent: Arc::new(intent),
            operation: None,
        }))
    }
    pub fn parent_request(&self) -> Option<&Self> {
        self.0.parent.as_ref()
    }
    pub fn root_request(&self) -> &Self {
        let mut root = self;
        while let Some(parent) = root.parent_request() {
            root = parent;
        }
        root
    }
    pub fn request_name(&self) -> &str {
        &self.0.name
    }
    pub fn comment(&self) -> &str {
        &self.0.comment
    }
    pub fn purpose(&self) -> &str {
        self.0.intent.purpose()
    }
    pub fn intent(&self) -> &QueryIntent {
        &self.0.intent
    }
    pub fn operation(&self) -> Option<&QueryOperation> {
        self.0.operation.as_ref()
    }

    pub fn derive_operation(&self, name: impl Into<String>, operation: QueryOperation) -> Self {
        Self(Arc::new(Header {
            parent: Some(self.clone()),
            name: name.into(),
            comment: operation.comment(),
            intent: Arc::clone(&self.0.intent),
            operation: Some(operation),
        }))
    }
    pub fn derive_helper(
        &self,
        name: impl Into<String>,
        comment: impl Into<String>,
    ) -> Result<Self, RequestIntentError> {
        let comment = comment.into();
        if comment.trim().is_empty() {
            return Err(RequestIntentError {
                request_kind: RequestKind::Query,
                field: "comment",
            });
        }
        Ok(Self(Arc::new(Header {
            parent: Some(self.clone()),
            name: name.into(),
            comment,
            intent: Arc::clone(&self.0.intent),
            operation: None,
        })))
    }
    pub fn derive_child(
        &self,
        name: impl Into<String>,
        comment: Option<&str>,
    ) -> Result<Self, RequestIntentError> {
        let name = name.into();
        self.derive_helper(
            name.clone(),
            comment
                .map(str::to_owned)
                .unwrap_or_else(|| format!("load {name}")),
        )
    }
    pub fn trace_chain(&self) -> Vec<TraceNode> {
        let mut nodes = Vec::new();
        let mut current = Some(self);
        while let Some(scope) = current {
            nodes.push(TraceNode::typed(
                TraceKind::Comment,
                scope.request_name(),
                None,
                scope.comment(),
            ));
            current = scope.parent_request();
        }
        nodes.push(TraceNode::typed(
            TraceKind::Purpose,
            self.root_request().request_name(),
            None,
            self.purpose(),
        ));
        nodes.reverse();
        nodes
    }
}
