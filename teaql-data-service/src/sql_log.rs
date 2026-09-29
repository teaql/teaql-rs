/// Compiler-owned provenance, not an opt-out supplied by a remote query caller.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SqlParameterLogPolicy {
    #[default]
    Unknown,
    Plain,
    Masked,
    Credential,
}

/// Statement/cursor completion, not transaction commit or business acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlExecutionOutcome {
    Success,
    Failure,
    Cancelled,
}

impl SqlExecutionOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Compiler/provider-owned log provenance. Pending intent redactions must be
/// removed by the runtime before retaining or delivering the safe projection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SqlLogContext {
    /// Opaque runtime-only state for safe reprojection; never wire data.
    #[doc(hidden)]
    pub projection_state: SqlProjectionState,
    #[doc(hidden)]
    pub intent_redactions: SqlIntentRedactions,
    pub execution_outcome: Option<SqlExecutionOutcome>,
    pub database_kind: Option<String>,
    pub parameter_policies: Vec<SqlParameterLogPolicy>,
    pub generated_sql: bool,
    pub masked_parameters: Vec<bool>,
    pub mode: Option<String>,
    pub omission_reason: Option<String>,
}

/// A runtime owns the private concrete type; provider/workspace code cannot
/// construct that type or interpret its data. Cloning preserves ownership.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct SqlProjectionState(Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>);

impl SqlProjectionState {
    pub fn new<T: std::any::Any + Send + Sync>(state: T) -> Self {
        Self(Some(std::sync::Arc::new(state)))
    }
    pub fn get<T: std::any::Any>(&self) -> Option<&T> {
        self.0.as_ref()?.downcast_ref()
    }
}

impl std::fmt::Debug for SqlProjectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqlProjectionState(<private>)")
    }
}

impl PartialEq for SqlProjectionState {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (None, None) => true,
            (Some(a), Some(b)) => std::sync::Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}
impl Eq for SqlProjectionState {}

/// Execution-local provenance for a derived statement's inherited intent.
/// Raw text is deliberately excluded from Debug; sinks receive an empty value.
#[doc(hidden)]
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SqlIntentRedactions(Vec<(String, bool)>);

impl std::fmt::Debug for SqlIntentRedactions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlIntentRedactions")
            .field("count", &self.0.len())
            .finish()
    }
}

impl SqlIntentRedactions {
    /// Conservative provider fallback; not a wire-level permission or a retained
    /// cache value. Visit only this statement (including SQL subqueries), not
    /// separately executed relation siblings. Iterative to bound the Rust stack.
    pub fn from_unclassified_query(query: &teaql_core::SelectQuery) -> Self {
        use teaql_core::{Expr, Value};
        enum Node<'a> {
            Query(&'a teaql_core::SelectQuery),
            Expr(&'a Expr),
        }
        let mut pending = vec![Node::Query(query)];
        let mut values = Vec::new();
        while let Some(node) = pending.pop() {
            match node {
                Node::Query(query) => {
                    pending.extend(query.filter.iter().map(Node::Expr));
                    pending.extend(query.having.iter().map(Node::Expr));
                    pending.extend(
                        query
                            .expr_projection
                            .iter()
                            .map(|field| Node::Expr(&field.expr)),
                    );
                    pending.extend(
                        query
                            .order_by
                            .iter()
                            .filter_map(|order| order.expr.as_ref())
                            .map(Node::Expr),
                    );
                    if let Some(text) = &query.search_with_text {
                        values.push(Value::Text(text.clone()));
                    }
                }
                Node::Expr(expr) => match expr {
                    Expr::Value(value) => values.push(value.clone()),
                    Expr::Column(_) => {}
                    Expr::Function { args, .. } | Expr::And(args) | Expr::Or(args) => {
                        pending.extend(args.iter().map(Node::Expr));
                    }
                    Expr::Binary { left, right, .. } => {
                        pending.extend([Node::Expr(left), Node::Expr(right)]);
                    }
                    Expr::SubQuery { left, query, .. } => {
                        pending.extend([Node::Expr(left), Node::Query(query)]);
                    }
                    Expr::Between { expr, lower, upper } => {
                        pending.extend([Node::Expr(expr), Node::Expr(lower), Node::Expr(upper)]);
                    }
                    Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::Not(expr) => {
                        pending.push(Node::Expr(expr))
                    }
                },
            }
        }
        Self::from_bindings(&SqlLogContext::default(), &values, "")
    }

    /// Combine execution-local ancestry without exposing its sensitive contents.
    pub fn extend(&mut self, other: &Self) {
        for value in &other.0 {
            if !self.0.contains(value) {
                self.0.push(value.clone());
            }
        }
    }

    /// Add an ordinary mutation ID only to diagnostic free-text redactions.
    /// It does not change the ID's SQL binding policy or execution value.
    pub fn capture_target_id(&mut self, id: &teaql_core::Value) {
        let source = SqlLogContext {
            generated_sql: true,
            parameter_policies: vec![SqlParameterLogPolicy::Masked],
            ..Default::default()
        };
        for (text, _) in Self::from_bindings(&source, std::slice::from_ref(id), "").0 {
            if !self
                .0
                .iter()
                .any(|(existing, forced)| existing == &text && *forced)
            {
                self.0.push((text, true));
            }
        }
    }

    pub fn from_bindings(source: &SqlLogContext, params: &[teaql_core::Value], sql: &str) -> Self {
        let invalid = source.parameter_policies.len() != params.len();
        let statement_credential =
            is_credential_log_name(sql) && (!source.generated_sql || invalid);
        let mut output = source.intent_redactions.clone();
        for (index, value) in params.iter().enumerate() {
            let policy = if invalid {
                SqlParameterLogPolicy::Unknown
            } else {
                source.parameter_policies[index]
            };
            if !statement_credential
                && policy == SqlParameterLogPolicy::Plain
                && !matches!(
                    value,
                    teaql_core::Value::Object(_)
                        | teaql_core::Value::List(_)
                        | teaql_core::Value::Json(_)
                )
            {
                continue;
            }
            let forced = statement_credential
                || matches!(
                    policy,
                    SqlParameterLogPolicy::Unknown | SqlParameterLogPolicy::Credential
                );
            if let teaql_core::Value::Text(text) = value {
                if !text.is_empty() {
                    output.0.push((text.clone(), forced));
                }
                continue;
            }
            // Inspect nested credential keys even when the outer field is plain.
            let mut pending = vec![value];
            let mut strings = Vec::new();
            let mut credential =
                statement_credential || policy == SqlParameterLogPolicy::Credential;
            while let Some(item) = pending.pop() {
                use teaql_core::Value;
                let text = match item {
                    Value::Null | Value::TypedNull(_) => continue,
                    Value::Object(object) => {
                        credential |= object.keys().any(|name| is_credential_log_name(name));
                        pending.extend(object.values());
                        continue;
                    }
                    Value::List(array) => {
                        pending.extend(array);
                        continue;
                    }
                    Value::Json(json) => {
                        let mut json_pending = vec![json];
                        while let Some(item) = json_pending.pop() {
                            if let Some(object) = item.as_object() {
                                credential |=
                                    object.keys().any(|name| is_credential_log_name(name));
                                json_pending.extend(object.values());
                            } else if let Some(array) = item.as_array() {
                                json_pending.extend(array);
                            } else if !item.is_null() {
                                let text = item
                                    .as_str()
                                    .map(str::to_owned)
                                    .unwrap_or_else(|| item.to_string());
                                if !text.is_empty() {
                                    strings.push(text);
                                }
                            }
                        }
                        continue;
                    }
                    Value::Text(text) => text.clone(),
                    Value::Bool(value) => value.to_string(),
                    Value::I64(value) => value.to_string(),
                    Value::U64(value) => value.to_string(),
                    Value::F64(value) => value.to_string(),
                    Value::Decimal(value) => value.to_string(),
                    Value::Date(value) => value.to_string(),
                    Value::Timestamp(value) => value.as_millis().to_string(),
                };
                if !text.is_empty() {
                    strings.push(text);
                }
            }
            let forced = credential || policy == SqlParameterLogPolicy::Unknown;
            if forced || policy == SqlParameterLogPolicy::Masked {
                output
                    .0
                    .extend(strings.into_iter().map(|text| (text, forced)));
            }
        }
        output
    }

    /// Trusted projection SPI, not an application permission to expose values.
    pub fn extend_secrets(&self, allow_plaintext: bool, secrets: &mut Vec<String>) {
        secrets.extend(
            self.0
                .iter()
                .filter(|(_, forced)| *forced || !allow_plaintext)
                .map(|(text, _)| text.clone()),
        );
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

/// Shared conservative credential classifier for compilation and log projection.
pub fn is_credential_log_name(name: &str) -> bool {
    let name: String = name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect();
    [
        "password",
        "passwd",
        "passphrase",
        "privatekey",
        "secret",
        "accesstoken",
        "refreshtoken",
        "idtoken",
        "apikey",
        "authorization",
        "credential",
        "sessiontoken",
        "magiclinktoken",
    ]
    .iter()
    .any(|word| name.contains(word))
}

#[cfg(test)]
mod query_intent_tests {
    use crate::{
        DataServiceCapabilities, DataServiceExecutor, QueryExecutor, QueryRequest, QueryResult,
    };
    use teaql_core::{DataType, EntityDescriptor, Expr, PropertyDescriptor, SelectQuery, Value};

    struct NoIo;
    impl DataServiceExecutor for NoIo {
        type Error = std::io::Error;
        fn capabilities(&self) -> DataServiceCapabilities {
            Default::default()
        }
    }
    impl QueryExecutor for NoIo {
        async fn query(&self, _: QueryRequest) -> Result<QueryResult, Self::Error> {
            panic!("intent discovery must not execute a query")
        }
    }

    #[test]
    fn default_provider_intent_is_unknown_in_debug_and_never_visits_relation_siblings() {
        let nested = EntityDescriptor::new("Child")
            .property(PropertyDescriptor::new("id", DataType::U64).id());
        let query = SelectQuery::new("Root")
            .filter(Expr::and([
                Expr::eq("name", "FILTER-SECRET"),
                Expr::in_subquery(
                    "id",
                    nested,
                    SelectQuery::new("Child").filter(Expr::eq("name", "SUBQUERY-SECRET")),
                    "id",
                ),
                Expr::eq(
                    "payload",
                    Value::object(std::collections::BTreeMap::from([(
                        "password".into(),
                        Value::from("NESTED-CREDENTIAL"),
                    )])),
                ),
            ]))
            .project_expr("value", Expr::value("PROJECTION-SECRET"))
            .having(Expr::eq("name", "HAVING-SECRET"))
            .search_with_text("SEARCH-SECRET")
            .relation_query(
                "siblings",
                SelectQuery::new("Child").filter(Expr::eq("name", "UNRELATED-SIBLING")),
            )
            .limit(10)
            .comment("what: fallback fixture");
        let source = NoIo.query_log_intent(&query);
        for debug in [false, true] {
            let mut secrets = Vec::new();
            source.extend_secrets(debug, &mut secrets);
            for expected in [
                "FILTER-SECRET",
                "SUBQUERY-SECRET",
                "NESTED-CREDENTIAL",
                "PROJECTION-SECRET",
                "HAVING-SECRET",
                "SEARCH-SECRET",
            ] {
                assert!(secrets.iter().any(|value| value == expected), "{expected}");
            }
            assert!(!secrets.iter().any(|value| value == "UNRELATED-SIBLING"));
        }
        assert!(!format!("{source:?}").contains("SECRET"));
    }
}
