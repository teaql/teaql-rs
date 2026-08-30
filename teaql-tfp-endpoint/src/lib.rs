use serde_json::Value as JsonValue;
use std::sync::Arc;
use teaql_data_service::{MutationExecutor, QueryExecutor, QueryRequest};
use teaql_runtime::{
    NoopRuntimeTelemetry, RuntimeAttributeValue, RuntimeOperation, RuntimeTelemetry,
    extract_runtime_context, start_runtime_operation,
};
use thiserror::Error;

pub mod models;
use models::{TfpFacetRequest, TfpMutationQuery, TfpSelectQuery};

#[derive(Clone, Debug)]
pub struct TrustedQueryContext {
    pub tenant_field: String,
    pub tenant_id: teaql_core::Value,
    pub authenticated_user: String,
    pub approved_purpose: String,
    pub allowed_entities: std::collections::BTreeSet<String>,
    /// Per entity mapping from public TFP field names to trusted core field names.
    pub field_mappings:
        std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    pub writable_field_mappings:
        std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    pub allowed_actions: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    pub max_page_size: usize,
}

#[derive(Error, Debug)]
pub enum TfpEndpointError {
    #[error("Failed to parse JSON payload: {0}")]
    ParseError(#[from] serde_json::Error),
    #[error("Failed to translate to core query: {0}")]
    TranslationError(String),
    #[error("Data service error: {0}")]
    ExecutionError(String),
}

impl TfpEndpointError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ParseError(_) => "TFP_INVALID_REQUEST",
            Self::TranslationError(message) if message.contains("audit reason") => {
                "TFP_AUDIT_REASON_REQUIRED"
            }
            Self::TranslationError(message) if message.starts_with("Entity is not allowed") => {
                "TFP_FORBIDDEN_ENTITY"
            }
            Self::TranslationError(message)
                if message.contains("Field is not allowed")
                    || message.contains("not writable")
                    || message.contains("Unknown field")
                    || message.contains("Unknown or forbidden field") =>
            {
                "TFP_FORBIDDEN_FIELD"
            }
            Self::TranslationError(message)
                if message.starts_with('$')
                    || message.starts_with("Unsupported predicate operator")
                    || message.starts_with("Filter must not be empty")
                    || message.starts_with("Predicate for ")
                    || message.starts_with("Facet ")
                    || message.starts_with("A TFP query may")
                    || message.starts_with("A TFP facet")
                    || message.starts_with("Nested facets")
                    || message.starts_with("Duplicate facet")
                    || message.contains("does not accept null") =>
            {
                "TFP_INVALID_REQUEST"
            }
            Self::TranslationError(_) => "TFP_POLICY_VIOLATION",
            Self::ExecutionError(_) => "TFP_EXECUTION_FAILED",
        }
    }
}

/// The core TeaQL Federal Protocol Endpoint processor.
/// This struct is framework-agnostic. Web containers (like Axum or Actix)
/// can handle routing, auth, and IP whitelisting, and simply pass the JSON
/// payload here to get the result.
pub struct TfpEndpoint<Q, M>
where
    Q: QueryExecutor + Send + Sync,
    M: MutationExecutor + Send + Sync,
{
    query_executor: Arc<Q>,
    mutation_executor: Arc<M>,
    telemetry: Arc<dyn RuntimeTelemetry>,
}

impl<Q, M> TfpEndpoint<Q, M>
where
    Q: QueryExecutor + Send + Sync,
    M: MutationExecutor + Send + Sync,
{
    pub fn new(query_executor: Arc<Q>, mutation_executor: Arc<M>) -> Self {
        Self {
            query_executor,
            mutation_executor,
            telemetry: Arc::new(NoopRuntimeTelemetry),
        }
    }

    pub fn with_runtime_telemetry(mut self, telemetry: Arc<dyn RuntimeTelemetry>) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Handles a TFP Query request (usually mapped to /query).
    pub async fn handle_query(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        self.handle_query_with_carrier(trusted, json_payload, &Default::default())
            .await
    }

    pub async fn handle_query_with_carrier(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
        carrier: &std::collections::BTreeMap<String, String>,
    ) -> Result<JsonValue, TfpEndpointError> {
        let propagation = extract_runtime_context(&self.telemetry, carrier);
        propagation
            .run(self.handle_query_observed(trusted, json_payload))
            .await
    }

    async fn handle_query_observed(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        let scope = start_runtime_operation(
            &self.telemetry,
            RuntimeOperation::new("tfp", "server.query").attribute("teaql.tfp.role", "server"),
        );
        let result = scope
            .run(self.handle_query_inner(trusted, json_payload))
            .await;
        match &result {
            Ok(response) => scope.success(std::collections::BTreeMap::from([(
                "teaql.result.cardinality".to_owned(),
                RuntimeAttributeValue::Integer(
                    response
                        .get("data")
                        .and_then(JsonValue::as_array)
                        .map_or(0, Vec::len) as i64,
                ),
            )])),
            Err(error) => scope.failure(tfp_error_type(error)),
        }
        result
    }

    async fn handle_query_inner(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        reject_privileged_input(&json_payload).map_err(TfpEndpointError::TranslationError)?;
        let mut tfp_query: TfpSelectQuery =
            serde_json::from_value(json_payload).map_err(TfpEndpointError::ParseError)?;

        validate_policy(trusted, &tfp_query).map_err(TfpEndpointError::TranslationError)?;
        let mappings = trusted
            .field_mappings
            .get(&tfp_query.entity)
            .ok_or_else(|| {
                TfpEndpointError::TranslationError(format!(
                    "No field policy for entity: {}",
                    tfp_query.entity
                ))
            })?;
        prepare_facets(trusted, &mut tfp_query).map_err(TfpEndpointError::TranslationError)?;
        tfp_query
            .map_fields(mappings)
            .map_err(TfpEndpointError::TranslationError)?;
        let facets = std::mem::take(&mut tfp_query.facets);
        let client_comment = tfp_query
            .generated_comment
            .clone()
            .or(tfp_query.comment_text.clone())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                TfpEndpointError::TranslationError("Query comment is required".into())
            })?;
        let requested_purpose = tfp_query
            .generated_purpose
            .clone()
            .or(tfp_query.purpose_text.clone())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                TfpEndpointError::TranslationError("Query purpose is required".into())
            })?;
        let mut core_query = tfp_query
            .to_core()
            .map_err(TfpEndpointError::TranslationError)?;
        let tenant = teaql_core::Expr::eq(&trusted.tenant_field, trusted.tenant_id.clone());
        core_query.filter = Some(match core_query.filter.take() {
            Some(filter) => teaql_core::Expr::And(vec![tenant, filter]),
            None => tenant,
        });
        let trace = teaql_core::TraceNode {
            entity_type: tfp_query.entity.clone(),
            entity_id: None,
            comment: format!(
                "approved-purpose={}; authenticated-user={}; requested-purpose={}",
                trusted.approved_purpose, trusted.authenticated_user, requested_purpose,
            ),
        };
        core_query.trace_chain.push(trace.clone());

        let outer_query = core_query.clone();
        let request = QueryRequest {
            query: core_query,
            trace_chain: vec![trace],
            comment: Some(client_comment),
            capture_debug_query: true,
            capture_execution_metadata: true,
        };

        let result = self
            .query_executor
            .query(request)
            .await
            .map_err(|e| TfpEndpointError::ExecutionError(e.to_string()))?;

        // Format into a standard response JSON.
        // We'll wrap the rows in a generic data format expected by TeaQL frontend.
        let mut response_obj = serde_json::Map::new();
        let rows_json: Vec<JsonValue> = result
            .rows
            .iter()
            .map(teaql_core::compact_row_to_json_value)
            .collect();

        response_obj.insert("data".to_string(), JsonValue::Array(rows_json));
        let facet_values = self.execute_facets(trusted, &outer_query, facets).await?;
        response_obj.insert("facets".to_string(), JsonValue::Object(facet_values));
        response_obj.insert("resultCode".to_string(), JsonValue::Number(0.into()));
        response_obj.insert("status".to_string(), JsonValue::String("YES".to_string()));
        let trace_json = result
            .metadata
            .trace_chain
            .iter()
            .map(|node| {
                serde_json::json!({
                    "entity": node.entity_type,
                    "comment": node.comment,
                })
            })
            .collect::<Vec<_>>();
        response_obj.insert(
            "execution".to_string(),
            serde_json::json!({
                "backend": result.metadata.backend,
                "resultCount": result.metadata.result_count,
                "trace": trace_json,
                "sqlShape": result.metadata.debug_query.as_deref().map(redact_sql_literals),
            }),
        );

        Ok(JsonValue::Object(response_obj))
    }

    async fn execute_facets(
        &self,
        trusted: &TrustedQueryContext,
        outer_query: &teaql_core::SelectQuery,
        facets: Vec<TfpFacetRequest>,
    ) -> Result<serde_json::Map<String, JsonValue>, TfpEndpointError> {
        let mut output = serde_json::Map::new();
        for facet in facets {
            let mut membership = outer_query.clone();
            membership.projection.clear();
            membership.expr_projection.clear();
            membership.order_by.clear();
            membership.slice = None;
            membership.group_by = vec![facet.relation_name.clone()];
            membership.aggregates = vec![teaql_core::Aggregate::new(
                teaql_core::AggregateFunction::Count,
                "id",
                "__tfpFacetCount",
            )];
            let membership_result = self
                .query_executor
                .query(QueryRequest {
                    query: membership,
                    trace_chain: vec![],
                    comment: Some(format!("TFP facet membership: {}", facet.facet_name)),
                    capture_debug_query: false,
                    capture_execution_metadata: true,
                })
                .await
                .map_err(|error| TfpEndpointError::ExecutionError(error.to_string()))?;
            let mut counts = std::collections::BTreeMap::new();
            for row in &membership_result.rows {
                let Some(id) = row.get(&facet.relation_name).map(value_as_json) else {
                    continue;
                };
                let count = row
                    .get("__tfpFacetCount")
                    .map(value_as_json)
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                counts.insert(json_key(&id), count);
            }

            let aliases = facet
                .query
                .aggregate_items
                .iter()
                .map(|aggregate| aggregate.alias.clone())
                .collect::<Vec<_>>();
            let mut nested = *facet.query;
            nested.aggregate_items.clear();
            nested.group_by_items.clear();
            let mut nested_query = nested
                .to_core()
                .map_err(TfpEndpointError::TranslationError)?;
            let tenant = teaql_core::Expr::eq(&trusted.tenant_field, trusted.tenant_id.clone());
            nested_query.filter = Some(match nested_query.filter.take() {
                Some(filter) => teaql_core::Expr::And(vec![tenant, filter]),
                None => tenant,
            });
            let nested_result = self
                .query_executor
                .query(QueryRequest {
                    query: nested_query,
                    trace_chain: vec![],
                    comment: Some(format!("TFP facet values: {}", facet.facet_name)),
                    capture_debug_query: false,
                    capture_execution_metadata: true,
                })
                .await
                .map_err(|error| TfpEndpointError::ExecutionError(error.to_string()))?;
            let mut values = Vec::new();
            for row in &nested_result.rows {
                let mut value = teaql_core::compact_row_to_json_value(row);
                let id = value.get("id").cloned().unwrap_or(JsonValue::Null);
                let key = json_key(&id);
                if !facet.include_all_facets && !counts.contains_key(&key) {
                    continue;
                }
                let count = counts.get(&key).copied().unwrap_or(0);
                if let Some(object) = value.as_object_mut() {
                    for alias in &aliases {
                        object.insert(alias.clone(), JsonValue::Number(count.into()));
                    }
                }
                values.push(value);
            }
            output.insert(facet.facet_name, JsonValue::Array(values));
        }
        Ok(output)
    }

    /// Handles a TFP Mutation request (usually mapped to /mutate).
    pub async fn handle_mutation(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        self.handle_mutation_with_carrier(trusted, json_payload, &Default::default())
            .await
    }

    pub async fn handle_mutation_with_carrier(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
        carrier: &std::collections::BTreeMap<String, String>,
    ) -> Result<JsonValue, TfpEndpointError> {
        let propagation = extract_runtime_context(&self.telemetry, carrier);
        propagation
            .run(self.handle_mutation_observed(trusted, json_payload))
            .await
    }

    async fn handle_mutation_observed(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        let scope = start_runtime_operation(
            &self.telemetry,
            RuntimeOperation::new("tfp", "server.mutation").attribute("teaql.tfp.role", "server"),
        );
        let result = scope
            .run(self.handle_mutation_inner(trusted, json_payload))
            .await;
        match &result {
            Ok(_) => scope.success(std::collections::BTreeMap::new()),
            Err(error) => scope.failure(tfp_error_type(error)),
        }
        result
    }

    async fn handle_mutation_inner(
        &self,
        trusted: &TrustedQueryContext,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        reject_privileged_input(&json_payload).map_err(TfpEndpointError::TranslationError)?;
        let mut tfp_mutation: TfpMutationQuery =
            serde_json::from_value(json_payload).map_err(TfpEndpointError::ParseError)?;

        validate_mutation_policy(trusted, &tfp_mutation)
            .map_err(TfpEndpointError::TranslationError)?;
        let mappings = trusted
            .writable_field_mappings
            .get(&tfp_mutation.entity)
            .ok_or_else(|| {
                TfpEndpointError::TranslationError(format!(
                    "No writable field policy for entity: {}",
                    tfp_mutation.entity
                ))
            })?;
        tfp_mutation
            .map_writable_fields(mappings)
            .map_err(TfpEndpointError::TranslationError)?;
        if matches!(tfp_mutation.action.as_str(), "Create" | "Update") {
            let tenant_json = value_as_json(&trusted.tenant_id);
            tfp_mutation
                .payload
                .as_object_mut()
                .expect("validated object")
                .insert(trusted.tenant_field.clone(), tenant_json);
        }

        let core_mutation = tfp_mutation
            .to_core()
            .map_err(TfpEndpointError::TranslationError)?;

        let result = self
            .mutation_executor
            .mutate(core_mutation)
            .await
            .map_err(|e| TfpEndpointError::ExecutionError(e.to_string()))?;

        let mut response_obj = serde_json::Map::new();
        response_obj.insert(
            "affectedRows".to_string(),
            JsonValue::Number(result.affected_rows.into()),
        );
        response_obj.insert("resultCode".to_string(), JsonValue::Number(0.into()));
        response_obj.insert("status".to_string(), JsonValue::String("YES".to_string()));

        let mut data_arr = Vec::new();
        if !result.generated_values.is_empty() {
            let generated: teaql_core::Record = result.generated_values.clone().into();
            data_arr.push(teaql_core::record_to_json_value(&generated));
        }
        response_obj.insert("data".to_string(), JsonValue::Array(data_arr));

        Ok(JsonValue::Object(response_obj))
    }
}

fn tfp_error_type(error: &TfpEndpointError) -> &'static str {
    error.code()
}

fn value_as_json(value: &teaql_core::Value) -> JsonValue {
    let record = teaql_core::Record::from([("value".to_owned(), value.clone())]);
    teaql_core::record_to_json_value(&record)["value"].clone()
}

fn json_key(value: &JsonValue) -> String {
    match value {
        JsonValue::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn redact_sql_literals(sql: &str) -> String {
    let mut output = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\'' {
            output.push(ch);
            continue;
        }
        output.push('\'');
        while let Some(value) = chars.next() {
            if value == '\'' {
                if chars.peek() == Some(&'\'') {
                    chars.next();
                    continue;
                }
                break;
            }
        }
        output.push('?');
        output.push('\'');
    }
    output
}

fn reject_privileged_input(payload: &JsonValue) -> Result<(), String> {
    const FORBIDDEN: &[&str] = &[
        "tenant",
        "tenantId",
        "merchant",
        "merchantId",
        "user",
        "userId",
        "permissions",
        "requestPolicy",
        "purposePolicy",
        "trustedContext",
        "hardLimit",
        "hard_limit",
        "hardLimitValue",
        "hard_limit_value",
        "idSetPagination",
        "id_set_pagination",
    ];

    fn reject_at(value: &JsonValue, path: &str, forbidden: &[&str]) -> Result<(), String> {
        match value {
            JsonValue::Object(object) => {
                for (field, child) in object {
                    if forbidden.contains(&field.as_str()) {
                        return Err(format!(
                            "Client cannot provide trusted or server-local field: {path}.{field}"
                        ));
                    }
                    reject_at(child, &format!("{path}.{field}"), forbidden)?;
                }
            }
            JsonValue::Array(values) => {
                for (index, child) in values.iter().enumerate() {
                    reject_at(child, &format!("{path}[{index}]"), forbidden)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    if !payload.is_object() {
        return Err("TFP payload must be an object".into());
    }
    reject_at(payload, "$", FORBIDDEN)
}

fn validate_policy(trusted: &TrustedQueryContext, query: &TfpSelectQuery) -> Result<(), String> {
    if !trusted.allowed_entities.contains(&query.entity) {
        return Err(format!(
            "Entity is not allowed by federation policy: {}",
            query.entity
        ));
    }
    if query.limit_value.unwrap_or(0) > trusted.max_page_size {
        return Err("Page size exceeds federation policy".into());
    }
    let allowed = trusted
        .field_mappings
        .get(&query.entity)
        .ok_or_else(|| format!("No field policy for entity: {}", query.entity))?;
    for field in query
        .order_items
        .iter()
        .map(|value| &value.field)
        .chain(query.select_items.iter())
        .chain(query.group_by_items.iter())
        .chain(query.aggregate_items.iter().map(|value| &value.field))
    {
        if field != "*" && !allowed.contains_key(field) {
            return Err(format!(
                "Field is not allowed by federation policy: {field}"
            ));
        }
    }
    Ok(())
}

fn prepare_facets(trusted: &TrustedQueryContext, query: &mut TfpSelectQuery) -> Result<(), String> {
    if query.facets.len() > 10 {
        return Err("A TFP query may contain at most 10 facets".into());
    }
    let outer_fields = trusted
        .field_mappings
        .get(&query.entity)
        .ok_or_else(|| format!("No field policy for entity: {}", query.entity))?;
    let mut names = std::collections::BTreeSet::new();
    for facet in &mut query.facets {
        if facet.facet_name.is_empty()
            || facet.facet_name.len() > 64
            || !facet
                .facet_name
                .chars()
                .all(|value| value.is_ascii_alphanumeric() || value == '_')
        {
            return Err("Facet name must be 1-64 ASCII letters, digits, or underscores".into());
        }
        if !names.insert(facet.facet_name.clone()) {
            return Err(format!("Duplicate facet name: {}", facet.facet_name));
        }
        facet.relation_name = outer_fields
            .get(&facet.relation_name)
            .ok_or_else(|| {
                format!(
                    "Field is not allowed by federation policy: {}",
                    facet.relation_name
                )
            })?
            .clone();
        if !facet.query.facets.is_empty() {
            return Err("Nested facets are not supported by TFP".into());
        }
        validate_policy(trusted, &facet.query)?;
        let nested_fields = trusted
            .field_mappings
            .get(&facet.query.entity)
            .ok_or_else(|| format!("No field policy for entity: {}", facet.query.entity))?;
        if facet.query.aggregate_items.is_empty()
            || facet
                .query
                .aggregate_items
                .iter()
                .any(|item| !item.function.eq_ignore_ascii_case("count"))
        {
            return Err("A TFP facet requires one or more Count aggregates".into());
        }
        if facet
            .query
            .comment_text
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
            || facet
                .query
                .purpose_text
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
        {
            return Err("Facet query commentText and purposeText are required".into());
        }
        facet.query.map_fields(nested_fields)?;
    }
    Ok(())
}

fn validate_mutation_policy(
    trusted: &TrustedQueryContext,
    mutation: &TfpMutationQuery,
) -> Result<(), String> {
    if !trusted.allowed_entities.contains(&mutation.entity) {
        return Err(format!(
            "Entity is not allowed by federation policy: {}",
            mutation.entity
        ));
    }
    let actions = trusted
        .allowed_actions
        .get(&mutation.entity)
        .ok_or_else(|| format!("No action policy for entity: {}", mutation.entity))?;
    if !actions.contains(&mutation.action) {
        return Err(format!(
            "Action is not allowed by federation policy: {}",
            mutation.action
        ));
    }
    if !mutation.payload.is_object() {
        return Err("Mutation payload must be an object".into());
    }
    if mutation
        .comment
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .is_none()
    {
        return Err("Mutation audit reason is required".into());
    }
    if mutation.payload.get(&trusted.tenant_field).is_some() {
        return Err("Tenant field is server-owned and not allowed".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Mutex;
    use teaql_core::Record;
    use teaql_data_service::{
        DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
        MutationRequest, MutationResult, QueryResult,
    };
    use teaql_runtime::{RuntimeOperation, RuntimeTelemetryScope};

    #[derive(Clone, Default)]
    struct StubExecutor;

    #[derive(Debug)]
    struct StubError;
    impl std::fmt::Display for StubError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("stub error")
        }
    }
    impl std::error::Error for StubError {}

    impl DataServiceExecutor for StubExecutor {
        type Error = StubError;
        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for StubExecutor {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            let rows = if !request.query.group_by.is_empty() {
                vec![teaql_core::CompactRow::from_map(Record::from([
                    ("status_id".into(), teaql_core::Value::I64(1001)),
                    ("__tfpFacetCount".into(), teaql_core::Value::I64(2)),
                ]))]
            } else if request.query.entity == "OrderStatus" {
                vec![
                    teaql_core::CompactRow::from_map(Record::from([
                        ("id".into(), teaql_core::Value::I64(1001)),
                        ("code".into(), teaql_core::Value::Text("NEW".into())),
                    ])),
                    teaql_core::CompactRow::from_map(Record::from([
                        ("id".into(), teaql_core::Value::I64(1002)),
                        ("code".into(), teaql_core::Value::Text("PAID".into())),
                    ])),
                ]
            } else {
                vec![teaql_core::CompactRow::from_map(Record::new())]
            };
            Ok(QueryResult {
                metadata: metadata(DataServiceOperation::Query, Some(rows.len()), None),
                rows,
            })
        }
    }

    impl MutationExecutor for StubExecutor {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            Ok(MutationResult {
                affected_rows: 1,
                generated_values: Record::new().into(),
                persisted_snapshot: None,
                metadata: metadata(DataServiceOperation::Insert, None, Some(1)),
            })
        }
    }

    fn metadata(
        operation: DataServiceOperation,
        result_count: Option<usize>,
        affected_rows: Option<u64>,
    ) -> ExecutionMetadata {
        ExecutionMetadata {
            backend: "stub".into(),
            operation,
            started_at: std::time::SystemTime::now(),
            ended_at: std::time::SystemTime::now(),
            affected_rows,
            result_count,
            trace_chain: Vec::new(),
            comment: None,
            backend_request_id: None,
            parameterized_query: None,
            params: Vec::new(),
            debug_query: None,
        }
    }

    #[derive(Default)]
    struct RecordingTelemetry(Arc<Mutex<Vec<RecordedEvent>>>);

    #[derive(Debug)]
    struct RecordedEvent {
        operation: RuntimeOperation,
        completion: Option<BTreeMap<String, RuntimeAttributeValue>>,
        failure: Option<String>,
    }

    impl RuntimeTelemetry for RecordingTelemetry {
        fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
            let mut events = self.0.lock().expect("events");
            events.push(RecordedEvent {
                operation,
                completion: None,
                failure: None,
            });
            Box::new(RecordingScope {
                events: self.0.clone(),
                index: events.len() - 1,
            })
        }
    }

    struct RecordingScope {
        events: Arc<Mutex<Vec<RecordedEvent>>>,
        index: usize,
    }
    impl RuntimeTelemetryScope for RecordingScope {
        fn success(&mut self, attributes: BTreeMap<String, RuntimeAttributeValue>) {
            self.events.lock().expect("events")[self.index].completion = Some(attributes);
        }
        fn failure(&mut self, error_type: &str) {
            self.events.lock().expect("events")[self.index].failure = Some(error_type.into());
        }
    }

    fn trusted() -> TrustedQueryContext {
        TrustedQueryContext {
            tenant_field: "commerce_platform_id".into(),
            tenant_id: teaql_core::Value::I64(1),
            authenticated_user: "operator-42".into(),
            approved_purpose: "approved-order-search".into(),
            allowed_entities: BTreeSet::from(["CustomerOrder".into(), "OrderStatus".into()]),
            field_mappings: BTreeMap::from([
                (
                    "CustomerOrder".into(),
                    BTreeMap::from([
                        ("id".into(), "id".into()),
                        ("orderNumber".into(), "order_number".into()),
                        ("status".into(), "status_id".into()),
                    ]),
                ),
                (
                    "OrderStatus".into(),
                    BTreeMap::from([("id".into(), "id".into()), ("code".into(), "code".into())]),
                ),
            ]),
            writable_field_mappings: BTreeMap::from([(
                "CustomerOrder".into(),
                BTreeMap::from([("orderNumber".into(), "order_number".into())]),
            )]),
            allowed_actions: BTreeMap::from([(
                "CustomerOrder".into(),
                BTreeSet::from([
                    "Create".into(),
                    "Update".into(),
                    "Delete".into(),
                    "Recover".into(),
                ]),
            )]),
            max_page_size: 100,
        }
    }

    #[test]
    fn client_cannot_override_trusted_context() {
        for field in [
            "tenantId",
            "merchant",
            "user",
            "permissions",
            "requestPolicy",
            "purposePolicy",
            "trustedContext",
        ] {
            assert!(reject_privileged_input(&json!({(field): "attacker"})).is_err());
        }
        assert!(
            reject_privileged_input(&json!({"entity":"CustomerOrder","_purpose":"requested"}))
                .is_ok()
        );
    }

    #[test]
    fn client_cannot_override_hard_limit_at_any_depth() {
        for field in [
            "hardLimit",
            "hard_limit",
            "hardLimitValue",
            "hard_limit_value",
        ] {
            let error = reject_privileged_input(&json!({
                "entity": "CustomerOrder",
                (field): 20_000
            }))
            .unwrap_err();
            assert!(error.contains(field));

            let nested_error = reject_privileged_input(&json!({
                "entity": "CustomerOrder",
                "relations": [{"query": {(field): 20_000}}]
            }))
            .unwrap_err();
            assert!(nested_error.contains(field));
        }
    }

    #[test]
    fn client_cannot_enable_id_set_pagination_at_any_depth() {
        for field in ["idSetPagination", "id_set_pagination"] {
            assert!(reject_privileged_input(&json!({(field): {"maxIds": 5_000_000}})).is_err());
            assert!(
                reject_privileged_input(&json!({
                    "relations": [{"query": {(field): {"namespace": "attacker"}}}]
                }))
                .is_err()
            );
        }
    }

    #[test]
    fn policy_rejects_entity_field_and_page_size() {
        let context = trusted();
        let query = |value| serde_json::from_value::<TfpSelectQuery>(value).unwrap();
        assert!(validate_policy(&context, &query(json!({"entity":"Other"}))).is_err());
        assert!(
            validate_policy(
                &context,
                &query(json!({
                    "entity":"CustomerOrder", "_limit":101
                }))
            )
            .is_err()
        );
        assert!(
            validate_policy(
                &context,
                &query(json!({
                    "entity":"CustomerOrder", "_orderBy":[{"f":"secret","d":"asc"}]
                }))
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn unsupported_predicate_is_an_invalid_request() {
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor));
        let error = endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder",
                    "filterCondition":{"id":{"$wat":7}},
                    "commentText":"exercise invalid operator",
                    "purposeText":"verify stable TFP error classification"
                }),
            )
            .await
            .expect_err("unsupported operator must fail closed");
        assert_eq!(error.code(), "TFP_INVALID_REQUEST");
    }

    #[tokio::test]
    async fn executes_relation_facet_and_returns_count_alias() {
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor));
        let response = endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder",
                    "filterCondition":{"id":{"$gt":0}},
                    "facets":[{
                        "facetName":"statusFacet",
                        "relationName":"status",
                        "includeAllFacets":true,
                        "query":{
                            "entity":"OrderStatus",
                            "selectItems":["id","code"],
                            "aggregateItems":[{"function":"Count","field":"id","alias":"orderCount"}],
                            "commentText":"load status facet",
                            "purposeText":"render order filters"
                        }
                    }],
                    "limitValue":10,
                    "commentText":"load orders",
                    "purposeText":"render order list"
                }),
            )
            .await
            .expect("facet query");
        let facet = response["facets"]["statusFacet"]
            .as_array()
            .expect("facet array");
        assert_eq!(facet.len(), 2);
        assert_eq!(facet[0]["orderCount"], 2);
        assert_eq!(facet[1]["orderCount"], 0);
    }

    #[test]
    fn sql_shape_redacts_string_literals() {
        let shape = redact_sql_literals(
            "select * from t where email = 'private-address' and name = 'private-name'",
        );
        assert!(!shape.contains('@'));
        assert!(!shape.contains("Brien"));
        assert_eq!(shape, "select * from t where email = '?' and name = '?'");
    }

    #[tokio::test]
    async fn records_tfp_server_query_mutation_and_failure_lifecycles() {
        let telemetry = Arc::new(RecordingTelemetry::default());
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor))
            .with_runtime_telemetry(telemetry.clone());

        let response = endpoint
            .handle_query(
                &trusted(),
                json!({
                    "entity":"CustomerOrder", "_comment":"generated query",
                    "_purpose":"requested purpose", "_limit":10
                }),
            )
            .await
            .expect("query response");
        assert_eq!(response["data"].as_array().map(Vec::len), Some(1));
        endpoint
            .handle_mutation(
                &trusted(),
                json!({
                    "entity":"CustomerOrder", "action":"Create",
                    "payload":{"orderNumber":"O-1"},
                    "comment":"create order"
                }),
            )
            .await
            .expect("mutation response");
        let error = endpoint
            .handle_query(&trusted(), json!({"entity":"Other"}))
            .await
            .expect_err("policy failure");
        assert!(matches!(error, TfpEndpointError::TranslationError(_)));

        let events = telemetry.0.lock().expect("events");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].operation.family, "tfp");
        assert_eq!(events[0].operation.name, "server.query");
        assert_eq!(
            events[0].operation.attributes["teaql.tfp.role"],
            "server".into()
        );
        assert_eq!(
            events[0].completion.as_ref().unwrap()["teaql.result.cardinality"],
            1usize.into()
        );
        assert_eq!(events[1].operation.name, "server.mutation");
        assert!(events[1].completion.is_some());
        assert_eq!(events[2].failure.as_deref(), Some("TFP_FORBIDDEN_ENTITY"));
    }

    #[tokio::test]
    async fn mutation_requires_trusted_entity_action_fields_and_audit_reason() {
        let endpoint = TfpEndpoint::new(Arc::new(StubExecutor), Arc::new(StubExecutor));
        for payload in [
            json!({"entity":"Other","action":"Create","payload":{},"comment":"x"}),
            json!({"entity":"CustomerOrder","action":"Publish","payload":{},"comment":"x"}),
            json!({"entity":"CustomerOrder","action":"Create","payload":{"secret":"x"},"comment":"x"}),
            json!({"entity":"CustomerOrder","action":"Create","payload":{"orderNumber":"x"},"comment":" "}),
            json!({"entity":"CustomerOrder","action":"Create","payload":{"commerce_platform_id":99},"comment":"x"}),
        ] {
            assert!(endpoint.handle_mutation(&trusted(), payload).await.is_err());
        }
    }
}
