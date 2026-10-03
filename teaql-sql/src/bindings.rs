//! Per-compilation binding provenance. Never stored in a thread-local or context.
use crate::DatabaseKind;
use teaql_core::{EntityDescriptor, Expr, Value};
use teaql_data_service::{SqlLogContext, SqlParameterLogPolicy as Policy};

#[derive(Debug, Clone, Default)]
pub struct SqlBindings {
    values: Vec<Value>,
    policies: Vec<Policy>,
    pub(crate) scope: Option<Policy>,
    untrusted_sql: bool,
}

impl SqlBindings {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn push(&mut self, value: Value) {
        self.values.push(value);
        self.policies.push(self.scope.unwrap_or_default());
    }
    pub fn push_field(&mut self, entity: &EntityDescriptor, field: &str, value: Value) {
        self.values.push(value);
        self.policies.push(field_policy(entity, field));
    }
    pub fn mark_untrusted_sql(&mut self) {
        self.untrusted_sql = true;
    }
    pub fn log_context(&self, kind: DatabaseKind) -> SqlLogContext {
        SqlLogContext {
            database_kind: Some(format!("{kind:?}").to_ascii_lowercase()),
            parameter_policies: self.policies.clone(),
            generated_sql: !self.untrusted_sql,
            ..Default::default()
        }
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
        Expr::Value(_) | Expr::And(_) | Expr::Or(_) | Expr::Not(_) => None,
    }
}
