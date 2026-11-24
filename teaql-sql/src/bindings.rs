//! Per-compilation binding provenance. Never stored in a thread-local or context.
use crate::DatabaseKind;
use teaql_core::{BinaryOp, EntityDescriptor, Expr, Value};
use teaql_data_service::{SqlIntentRedactions, SqlLogContext, SqlParameterLogPolicy as Policy};

pub(crate) type CompilationMetadata =
    std::collections::BTreeMap<String, std::sync::Arc<EntityDescriptor>>;

#[derive(Debug, Clone, Default)]
pub struct SqlBindings {
    values: Vec<Value>,
    policies: Vec<Policy>,
    originals: Vec<(usize, Value)>,
    pub(crate) scope: Option<Policy>,
    pub(crate) metadata: Option<std::sync::Arc<CompilationMetadata>>,
    request_scopes: Vec<teaql_core::QueryRequestScope>,
    untrusted_sql: bool,
}

impl SqlBindings {
    pub fn new() -> Self {
        Self::default()
    }
    pub(crate) fn capture_query_scope(&mut self, query: &teaql_core::SelectQuery) {
        if let Some(scope) = query.request_scope()
            && !self.request_scopes.iter().any(|previous| previous == scope)
        {
            self.request_scopes.push(scope.clone());
        }
    }
    pub fn push(&mut self, value: Value) {
        self.values.push(value);
        self.policies.push(self.scope.unwrap_or_default());
    }
    pub fn push_field(&mut self, entity: &EntityDescriptor, field: &str, value: Value) {
        self.values.push(value);
        self.policies.push(field_policy(entity, field));
    }
    pub(crate) fn push_like_pattern(&mut self, pattern: &str, original: &str) {
        self.originals
            .push((self.values.len(), Value::from(original)));
        self.push(Value::from(pattern));
    }
    /// Rebuild execution-local intent provenance using this invocation's exact
    /// operands and the compiled plan's field policies, never cached raw text.
    pub(crate) fn rebind_log_context(&self, source: &SqlLogContext) -> SqlLogContext {
        let mut context = source.clone();
        let nodes = teaql_core::QueryRequestTraceNode::from_scopes(&self.request_scopes);
        context.request_tree = (!nodes.is_empty()).then(|| nodes.into());
        let original_source = SqlLogContext {
            generated_sql: source.generated_sql,
            parameter_policies: self
                .originals
                .iter()
                .map(|(index, _)| {
                    source
                        .parameter_policies
                        .get(*index)
                        .copied()
                        .unwrap_or_default()
                })
                .collect(),
            ..Default::default()
        };
        let originals = self
            .originals
            .iter()
            .map(|(_, value)| value.clone())
            .collect::<Vec<_>>();
        context.intent_redactions =
            SqlIntentRedactions::from_bindings(&original_source, &originals, "");
        context
    }
    pub fn mark_untrusted_sql(&mut self) {
        self.untrusted_sql = true;
    }
    pub fn log_context(&self, kind: DatabaseKind) -> SqlLogContext {
        self.rebind_log_context(&SqlLogContext {
            database_kind: Some(format!("{kind:?}").to_ascii_lowercase()),
            parameter_policies: self.policies.clone(),
            generated_sql: !self.untrusted_sql,
            ..Default::default()
        })
    }
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
}

impl std::ops::Deref for SqlBindings {
    type Target = [Value];
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

pub(crate) fn field_policy(entity: &EntityDescriptor, field: &str) -> Policy {
    teaql_data_service::SqlIntentRedactions::field_policy(entity, field)
}

/// Validate retained lowering provenance after possible AST rewrites. This
/// compares against an explicitly retained operand; it never infers one by
/// removing wildcard or escape characters from an arbitrary SQL pattern.
pub(crate) fn like_operand(op: BinaryOp, right: &Expr) -> Option<(&str, &str)> {
    let Expr::LikePattern { pattern, original } = right else {
        return None;
    };
    (matches!(op, BinaryOp::Like | BinaryOp::NotLike)
        && (pattern == &format!("%{original}%")
            || pattern == &format!("{original}%")
            || pattern == &format!("%{original}")))
        .then_some((pattern, original))
}

pub(crate) fn combine(left: Option<Policy>, right: Option<Policy>) -> Option<Policy> {
    use Policy::*;
    match (left, right) {
        (None, policy) | (policy, None) => policy,
        (Some(Credential), _) | (_, Some(Credential)) => Some(Credential),
        (Some(Unknown), _) | (_, Some(Unknown)) => Some(Unknown),
        (Some(Masked), _) | (_, Some(Masked)) => Some(Masked),
        _ => Some(Plain),
    }
}

/// References in one scalar expression taint its derived bindings. Logical
/// siblings and subquery entity scopes must not contaminate one another.
pub(crate) fn expression_policy(entity: &EntityDescriptor, expr: &Expr) -> Option<Policy> {
    match expr {
        Expr::Column(name) => Some(field_policy(entity, name)),
        Expr::Function { args, .. } => args
            .iter()
            .fold(None, |p, e| combine(p, expression_policy(entity, e))),
        Expr::Binary { left, right, .. } => combine(
            expression_policy(entity, left),
            expression_policy(entity, right),
        ),
        Expr::Between { expr, lower, upper } => combine(
            expression_policy(entity, expr),
            combine(
                expression_policy(entity, lower),
                expression_policy(entity, upper),
            ),
        ),
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => expression_policy(entity, expr),
        Expr::SubQuery { left, .. } => expression_policy(entity, left),
        Expr::Value(_) | Expr::LikePattern { .. } | Expr::And(_) | Expr::Or(_) | Expr::Not(_) => {
            None
        }
    }
}

#[cfg(test)]
mod request_tree_tests {
    use super::*;
    use teaql_core::{QueryIntent, QueryRequestScope, SelectQuery};

    #[test]
    fn rebind_replaces_cached_request_text_with_current_scope_identity() {
        fn bindings(comment: &str) -> SqlBindings {
            let root = QueryRequestScope::root(
                "School",
                QueryIntent::new(comment, "Render dashboard").unwrap(),
            );
            let child = root.derive_child("Kind", Some(comment)).unwrap();
            let mut bindings = SqlBindings::new();
            bindings.capture_query_scope(&SelectQuery::new("School").with_request_scope(root));
            bindings.capture_query_scope(&SelectQuery::new("Kind").with_request_scope(child));
            bindings
        }
        let source = bindings("OLD-INVOCATION").log_context(DatabaseKind::Sqlite);
        let current = bindings("CURRENT-INVOCATION").rebind_log_context(&source);
        let nodes = current.request_tree.as_deref().unwrap();
        assert_eq!(nodes.len(), 3);
        assert!(nodes.iter().any(|node| node.detail == "CURRENT-INVOCATION"));
        assert!(
            nodes
                .iter()
                .all(|node| !node.detail.contains("OLD-INVOCATION"))
        );
        assert_eq!(nodes[2].parent_id, Some(1));
        assert!(
            source
                .request_tree
                .unwrap()
                .iter()
                .any(|node| node.detail == "OLD-INVOCATION")
        );
    }

    #[test]
    fn ordinary_query_rebinding_drops_any_previous_tree() {
        let source = SqlLogContext {
            request_tree: Some(Vec::<teaql_core::QueryRequestTraceNode>::new().into()),
            ..SqlLogContext::default()
        };
        assert!(
            SqlBindings::new()
                .rebind_log_context(&source)
                .request_tree
                .is_none()
        );
    }
}
