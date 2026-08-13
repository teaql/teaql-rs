use serde_json::Value as JsonValue;
use std::sync::Arc;
use teaql_data_service::{MutationExecutor, QueryExecutor, QueryRequest};
use thiserror::Error;

pub mod models;
use models::{TfpMutationQuery, TfpSelectQuery};

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
        }
    }

    /// Handles a TFP Query request (usually mapped to /query).
    pub async fn handle_query(
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
        tfp_query
            .map_fields(mappings)
            .map_err(TfpEndpointError::TranslationError)?;
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

        let request = QueryRequest {
            query: core_query,
            trace_chain: vec![trace],
            comment: Some(client_comment),
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
            .map(teaql_core::record_to_json_value)
            .collect();

        response_obj.insert("data".to_string(), JsonValue::Array(rows_json));
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

    /// Handles a TFP Mutation request (usually mapped to /mutate).
    pub async fn handle_mutation(
        &self,
        json_payload: JsonValue,
    ) -> Result<JsonValue, TfpEndpointError> {
        let tfp_mutation: TfpMutationQuery =
            serde_json::from_value(json_payload).map_err(TfpEndpointError::ParseError)?;

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
            data_arr.push(teaql_core::record_to_json_value(&result.generated_values));
        }
        response_obj.insert("data".to_string(), JsonValue::Array(data_arr));

        Ok(JsonValue::Object(response_obj))
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    fn trusted() -> TrustedQueryContext {
        TrustedQueryContext {
            tenant_field: "commerce_platform_id".into(),
            tenant_id: teaql_core::Value::I64(1),
            authenticated_user: "operator-42".into(),
            approved_purpose: "approved-order-search".into(),
            allowed_entities: BTreeSet::from(["CustomerOrder".into()]),
            field_mappings: BTreeMap::from([(
                "CustomerOrder".into(),
                BTreeMap::from([
                    ("id".into(), "id".into()),
                    ("orderNumber".into(), "order_number".into()),
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

    #[test]
    fn sql_shape_redacts_string_literals() {
        let shape = redact_sql_literals(
            "select * from t where email = 'private-address' and name = 'private-name'",
        );
        assert!(!shape.contains('@'));
        assert!(!shape.contains("Brien"));
        assert_eq!(shape, "select * from t where email = '?' and name = '?'");
    }
}
