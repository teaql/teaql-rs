use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde_json::{json, Value as JsonValue};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use teaql_core::{Record, Value};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
};
use teaql_tfp_endpoint::{TfpEndpoint, TfpEndpointError, TrustedQueryContext};

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
        let mut row = Record::new();
        row.insert("id".into(), Value::I64(7));
        row.insert("status".into(), Value::Text("NEW".into()));
        Ok(QueryResult {
            rows: vec![teaql_core::CompactRow::from_record(row)],
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
            generated_values: generated,
            persisted_record: None,
            metadata: metadata(operation, None, Some(1), None),
        })
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
        StatusCode::BAD_REQUEST,
        Json(json!({"code": value.code(), "message": value.to_string()})),
    )
}
fn trusted() -> TrustedQueryContext {
    let fields = BTreeMap::from([
        ("id".into(), "id".into()),
        ("status".into(), "status".into()),
        ("orderNumber".into(), "order_number".into()),
    ]);
    TrustedQueryContext {
        tenant_field: "tenant_id".into(),
        tenant_id: Value::I64(1),
        authenticated_user: "conformance-agent".into(),
        approved_purpose: "tfp-conformance".into(),
        allowed_entities: BTreeSet::from(["CustomerOrder".into()]),
        field_mappings: BTreeMap::from([("CustomerOrder".into(), fields)]),
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
        max_page_size: 100,
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
