use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde_json::{Value as JsonValue, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use teaql_core::{Record, Value};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
    GuardedMutationExecutor, GuardedMutationRequest, MutationExecutor, MutationRequest,
    MutationResult, QueryExecutor, QueryRequest, QueryResult,
};
use teaql_tfp_endpoint::{
    TfpEndpoint, TfpEndpointError, TrustedEntityVisibility, TrustedQueryContext,
};

#[derive(Clone, Default)]
struct StubExecutor;

#[derive(Debug)]
struct StubError;
impl std::fmt::Display for StubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("stub error")
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
        if !request.query.group_by.is_empty() {
            let mut row = Record::new();
            row.insert("status".into(), Value::I64(1001));
            row.insert("__tfpFacetCount".into(), Value::I64(1));
            return Ok(QueryResult {
                rows: vec![teaql_core::CompactRow::from_map(row)],
                metadata: metadata(DataServiceOperation::Query, Some(1), None, request.comment),
            });
        }
        if request.query.entity == "OrderStatus" {
            let rows = [(1001, "NEW", "New"), (1002, "PAID", "Paid")]
                .into_iter()
                .map(|(id, code, label)| {
                    let mut row = Record::new();
                    row.insert("id".into(), Value::I64(id));
                    row.insert("code".into(), Value::Text(code.into()));
                    row.insert("label".into(), Value::Text(label.into()));
                    teaql_core::CompactRow::from_map(row)
                })
                .collect();
            return Ok(QueryResult {
                rows,
                metadata: metadata(DataServiceOperation::Query, Some(2), None, request.comment),
            });
        }
        let mut row = Record::new();
        row.insert("id".into(), Value::I64(7));
        row.insert("status".into(), Value::Text("NEW".into()));
        row.insert("orderNumber".into(), Value::Text("ORD-007".into()));
        row.insert("reviewed".into(), Value::Bool(true));
        Ok(QueryResult {
            rows: vec![teaql_core::CompactRow::from_map(row)],
            metadata: metadata(DataServiceOperation::Query, Some(1), None, request.comment),
        })
    }
}
impl MutationExecutor for StubExecutor {
    async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
        let operation = match request {
            MutationRequest::Insert(_) => DataServiceOperation::Insert,
            MutationRequest::Update(_) => DataServiceOperation::Update,
            MutationRequest::Delete(_) => DataServiceOperation::Delete,
            MutationRequest::Recover(_) => DataServiceOperation::Recover,
            MutationRequest::Batch(_) => DataServiceOperation::Update,
        };
        let mut generated = Record::new();
        generated.insert("id".into(), Value::I64(42));
        Ok(MutationResult {
            affected_rows: 1,
            generated_values: generated.into(),
            persisted_snapshot: None,
            metadata: metadata(operation, None, Some(1), None),
        })
    }
}

impl GuardedMutationExecutor for StubExecutor {
    async fn mutate_guarded(
        &self,
        request: GuardedMutationRequest,
    ) -> Result<MutationResult, Self::Error> {
        self.mutate(request.mutation).await
    }
}
fn metadata(
    operation: DataServiceOperation,
    result_count: Option<usize>,
    affected_rows: Option<u64>,
    comment: Option<String>,
) -> ExecutionMetadata {
    ExecutionMetadata {
        backend: "tfp-conformance-stub".into(),
        operation,
        started_at: std::time::SystemTime::now(),
        ended_at: std::time::SystemTime::now(),
        affected_rows,
        result_count,
        trace_chain: vec![],
        comment,
        backend_request_id: None,
        parameterized_query: None,
        params: vec![],
        debug_query: None,
    }
}

type Endpoint = TfpEndpoint<StubExecutor, StubExecutor>;
#[derive(Clone)]
struct AppState {
    endpoint: Arc<Endpoint>,
    trusted: Arc<TrustedQueryContext>,
}

async fn query(
    State(state): State<AppState>,
    Json(payload): Json<JsonValue>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    state
        .endpoint
        .handle_query(&state.trusted, payload)
        .await
        .map(Json)
        .map_err(error)
}
async fn mutate(
    State(state): State<AppState>,
    Json(payload): Json<JsonValue>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    state
        .endpoint
        .handle_mutation(&state.trusted, payload)
        .await
        .map(Json)
        .map_err(error)
}
fn error(value: TfpEndpointError) -> (StatusCode, Json<JsonValue>) {
    (
        status_for_error(&value),
        Json(json!({"code": value.code(), "message": value.public_message()})),
    )
}

fn status_for_error(value: &TfpEndpointError) -> StatusCode {
    match value.code() {
        "TFP_INVALID_REQUEST"
        | "TFP_AUDIT_REASON_REQUIRED"
        | "WIRE_UNKNOWN_FIELD"
        | "WIRE_FIELD_COLLISION" => StatusCode::BAD_REQUEST,
        "TFP_FORBIDDEN_ENTITY" | "TFP_FORBIDDEN_FIELD" | "TFP_POLICY_VIOLATION" => {
            StatusCode::FORBIDDEN
        }
        "TFP_MUTATION_TARGET_UNAVAILABLE" => StatusCode::CONFLICT,
        "TFP_EXECUTION_FAILED" => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
fn trusted() -> TrustedQueryContext {
    let fields = BTreeMap::from([
        ("id".into(), "id".into()),
        ("status".into(), "status".into()),
        ("orderNumber".into(), "order_number".into()),
        ("reviewed".into(), "reviewed".into()),
    ]);
    TrustedQueryContext {
        tenant_field: "tenant_id".into(),
        tenant_id: Value::I64(1),
        entity_visibility: BTreeMap::from([
            (
                "CustomerOrder".into(),
                TrustedEntityVisibility::Versioned {
                    field: "version".into(),
                },
            ),
            ("OrderStatus".into(), TrustedEntityVisibility::Unversioned),
        ]),
        authenticated_user: "conformance-agent".into(),
        approved_purpose: "tfp-conformance".into(),
        allowed_entities: BTreeSet::from(["CustomerOrder".into(), "OrderStatus".into()]),
        field_mappings: BTreeMap::from([
            ("CustomerOrder".into(), fields),
            (
                "OrderStatus".into(),
                BTreeMap::from([
                    ("id".into(), "id".into()),
                    ("code".into(), "code".into()),
                    ("label".into(), "label".into()),
                ]),
            ),
        ]),
        writable_field_mappings: BTreeMap::from([(
            "CustomerOrder".into(),
            BTreeMap::from([("status".into(), "status".into())]),
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
        wire_metadata: BTreeMap::new(),
        max_page_size: 100,
        max_offset: 10_000,
    }
}

#[tokio::main]
async fn main() {
    let address = std::env::var("TEAQL_TFP_LISTEN").unwrap_or_else(|_| "127.0.0.1:19091".into());
    let state = AppState {
        endpoint: Arc::new(TfpEndpoint::new(
            Arc::new(StubExecutor),
            Arc::new(StubExecutor),
        )),
        trusted: Arc::new(trusted()),
    };
    let app = Router::new()
        .route("/query", post(query))
        .route("/mutate", post(mutate))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect("bind TFP conformance server");
    println!("TFP conformance server listening on {address}");
    axum::serve(listener, app)
        .await
        .expect("serve TFP conformance server");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_current_tfp_error_codes_to_http_status_classes() {
        let parse_error = serde_json::from_str::<JsonValue>("{")
            .expect_err("invalid JSON should construct a parse error");
        let cases = [
            (
                TfpEndpointError::ParseError(parse_error),
                "TFP_INVALID_REQUEST",
                StatusCode::BAD_REQUEST,
            ),
            (
                TfpEndpointError::TranslationError("audit reason is required".into()),
                "TFP_AUDIT_REASON_REQUIRED",
                StatusCode::BAD_REQUEST,
            ),
            (
                TfpEndpointError::TranslationError("Entity is not allowed: Secret".into()),
                "TFP_FORBIDDEN_ENTITY",
                StatusCode::FORBIDDEN,
            ),
            (
                TfpEndpointError::TranslationError("Field is not allowed: secret".into()),
                "TFP_FORBIDDEN_FIELD",
                StatusCode::FORBIDDEN,
            ),
            (
                TfpEndpointError::TranslationError("tenant policy denied request".into()),
                "TFP_POLICY_VIOLATION",
                StatusCode::FORBIDDEN,
            ),
            (
                TfpEndpointError::ExecutionError("sensitive provider detail".into()),
                "TFP_EXECUTION_FAILED",
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                TfpEndpointError::MutationTargetUnavailable,
                "TFP_MUTATION_TARGET_UNAVAILABLE",
                StatusCode::CONFLICT,
            ),
            (
                TfpEndpointError::WireInput("unknownField".into()),
                "WIRE_UNKNOWN_FIELD",
                StatusCode::BAD_REQUEST,
            ),
            (
                TfpEndpointError::WireCollision("field alias collision".into()),
                "WIRE_FIELD_COLLISION",
                StatusCode::BAD_REQUEST,
            ),
        ];

        for (error, code, expected_status) in cases {
            assert_eq!(error.code(), code);
            assert_eq!(status_for_error(&error), expected_status, "code={code}");
        }
    }

    #[test]
    fn execution_error_response_is_generic_and_server_classified() {
        let (status, Json(body)) = error(TfpEndpointError::ExecutionError(
            "UNIQUE constraint failed: tenant_id=42".into(),
        ));

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["code"], "TFP_EXECUTION_FAILED");
        assert_eq!(body["message"], "Data service execution failed");
        assert!(!body.to_string().contains("tenant_id"));
        assert!(!body.to_string().contains("42"));
    }
}
