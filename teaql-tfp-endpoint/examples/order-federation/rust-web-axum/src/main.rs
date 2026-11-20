use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use order_federation_service_core::runtime::LocalSchemaProvider;
use order_federation_service_core::{service_runtime, AuditedSave, ServiceRuntimeConfig, Q};
use serde_json::{json, Value as JsonValue};
use std::{collections::BTreeMap, sync::Arc};
use teaql_core::{Entity, Value};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
use teaql_runtime::UserContext;
use teaql_sql::SqlDataServiceExecutor;
use teaql_tfp_endpoint::{
    TfpEndpoint, TfpEndpointError, TrustedEntityVisibility, TrustedQueryContext,
};

const ENTITY: &str = "FederatedOrder";

type Executor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, LocalSchemaProvider>;
type Endpoint = TfpEndpoint<Executor, Executor>;
type Reply = Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)>;

#[derive(Clone)]
struct AppState {
    endpoint: Arc<Endpoint>,
    trusted: Arc<TrustedQueryContext>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::var("TEAQL_FEDERATION_DATABASE")?;
    let database_url = format!("sqlite:file:{database}");
    let context = service_runtime(ServiceRuntimeConfig { database_url }).await?;
    context.ensure_schema().await?;
    let primary = Q::merchants_minimal()
        .with_id_is(1)
        .limit(1)
        .comment("Read the model's primary merchant")
        .purpose("Resolve the order operator's merchant")
        .execute_for_one(&context)
        .await?
        .ok_or("Model did not provision a primary merchant")?;
    let existing = Q::federated_orders_minimal()
        .with_id_is(1)
        .limit(1)
        .comment("Check for an existing delivery order")
        .purpose("Keep the local order data across restarts")
        .execute_for_one(&context)
        .await?;
    if existing.is_none() {
        let mut other = Q::merchants()
            .comment("Prepare an independent merchant")
            .purpose("Prove another merchant's order remains private")
            .new_entity(&context)?;
        other.update_name("Independent Retailer");
        let other = other
            .audit_as("Register the second merchant")
            .save(&context)
            .await?;
        create_order(&context, primary.id(), "FEDERATION-042").await?;
        create_order(&context, other.id(), "FEDERATION-OTHER").await?;
    }
    let transport = context
        .require_resource::<SqliteMutationExecutor>()?
        .clone();
    let provider = Arc::new(Executor::new(SqliteDialect, transport, LocalSchemaProvider));
    let state = AppState {
        endpoint: Arc::new(TfpEndpoint::new(provider.clone(), provider)),
        trusted: Arc::new(trusted(primary.id())),
    };
    let app = Router::new()
        .route("/query", post(query))
        .route("/mutate", post(mutate))
        .layer(axum::extract::DefaultBodyLimit::max(8192))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    println!("FEDERATION_READY=http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn create_order(
    context: &UserContext,
    merchant: u64,
    reference: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut order = Q::federated_orders()
        .comment("Prepare an order for dispatch")
        .purpose("Create controlled dispatch data for the merchant")
        .new_entity(context)?;
    order
        .update_merchant_id(merchant)
        .update_order_number(reference)
        .update_fulfillment_status("ready");
    order
        .audit_as("Register a merchant dispatch order")
        .save(context)
        .await?;
    Ok(())
}

fn trusted(merchant: u64) -> TrustedQueryContext {
    let readable = [
        ("id".into(), "id".into()),
        ("orderNumber".into(), "order_number".into()),
        ("fulfillmentStatus".into(), "fulfillment_status".into()),
        ("version".into(), "version".into()),
    ]
    .into();
    TrustedQueryContext {
        tenant_field: "merchant".into(),
        tenant_id: Value::U64(merchant),
        entity_visibility: [(
            ENTITY.into(),
            TrustedEntityVisibility::Versioned {
                field: "version".into(),
            },
        )]
        .into(),
        authenticated_user: "dispatch-desk".into(),
        approved_purpose: "review-dispatch".into(),
        allowed_entities: [ENTITY.into()].into(),
        field_mappings: [(ENTITY.into(), readable)].into(),
        writable_field_mappings: [(
            ENTITY.into(),
            [("fulfillmentStatus".into(), "fulfillment_status".into())].into(),
        )]
        .into(),
        allowed_actions: [(ENTITY.into(), ["Update".into()].into())].into(),
        wire_metadata: BTreeMap::new(),
        max_page_size: 10,
        max_offset: 20,
    }
}

async fn query(State(state): State<AppState>, Json(payload): Json<JsonValue>) -> Reply {
    state
        .endpoint
        .handle_query(&state.trusted, payload)
        .await
        .map(Json)
        .map_err(reply_error)
}

async fn mutate(State(state): State<AppState>, Json(payload): Json<JsonValue>) -> Reply {
    state
        .endpoint
        .handle_mutation(&state.trusted, payload)
        .await
        .map(Json)
        .map_err(reply_error)
}

fn reply_error(error: TfpEndpointError) -> (StatusCode, Json<JsonValue>) {
    if std::env::var_os("TEAQL_FEDERATION_FIXTURE_DIAGNOSTIC").is_some() {
        // This switch is used by the verifier with its own synthetic SQLite database.
        if let Some(detail) = error.internal_diagnostic() {
            eprintln!("Order federation fixture provider diagnostic: {detail}");
        }
    }
    let status = match error.code() {
        "TFP_FORBIDDEN_ENTITY" | "TFP_FORBIDDEN_FIELD" | "TFP_POLICY_VIOLATION" => {
            StatusCode::FORBIDDEN
        }
        "TFP_MUTATION_TARGET_UNAVAILABLE" => StatusCode::CONFLICT,
        "TFP_EXECUTION_FAILED" => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::BAD_REQUEST,
    };
    (status, Json(json!({"code": error.code()})))
}
