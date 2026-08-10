use std::sync::Arc;
use serde_json::Value as JsonValue;
use teaql_data_service::{MutationExecutor, QueryExecutor, QueryRequest};
use thiserror::Error;

pub mod models;
use models::{TfpMutationQuery, TfpSelectQuery};

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
    pub async fn handle_query(&self, json_payload: JsonValue) -> Result<JsonValue, TfpEndpointError> {
        let tfp_query: TfpSelectQuery = serde_json::from_value(json_payload)
            .map_err(TfpEndpointError::ParseError)?;

        let core_query = tfp_query.to_core();

        let request = QueryRequest {
            query: core_query,
            trace_chain: vec![],
            comment: tfp_query.comment_text,
        };

        let result = self.query_executor
            .query(request)
            .await
            .map_err(|e| TfpEndpointError::ExecutionError(e.to_string()))?;

        // Format into a standard response JSON.
        // We'll wrap the rows in a generic data format expected by TeaQL frontend.
        let mut response_obj = serde_json::Map::new();
        let rows_json: Vec<JsonValue> = result.rows.iter().map(teaql_core::record_to_json_value).collect();
        
        response_obj.insert("data".to_string(), JsonValue::Array(rows_json));
        response_obj.insert("resultCode".to_string(), JsonValue::Number(0.into()));
        response_obj.insert("status".to_string(), JsonValue::String("YES".to_string()));
        
        Ok(JsonValue::Object(response_obj))
    }

    /// Handles a TFP Mutation request (usually mapped to /mutate).
    pub async fn handle_mutation(&self, json_payload: JsonValue) -> Result<JsonValue, TfpEndpointError> {
        let tfp_mutation: TfpMutationQuery = serde_json::from_value(json_payload)
            .map_err(TfpEndpointError::ParseError)?;

        let core_mutation = tfp_mutation.to_core()
            .map_err(TfpEndpointError::TranslationError)?;

        let result = self.mutation_executor
            .mutate(core_mutation)
            .await
            .map_err(|e| TfpEndpointError::ExecutionError(e.to_string()))?;

        let mut response_obj = serde_json::Map::new();
        response_obj.insert("affectedRows".to_string(), JsonValue::Number(result.affected_rows.into()));
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
