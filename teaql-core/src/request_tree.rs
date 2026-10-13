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
