use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use supplier_order_sync_service_core::{
    service_runtime, AuditedSave, ServiceRuntimeConfig, SupplierOrder, E, Q,
};
use teaql_core::Entity;
use teaql_http_reqwest::{HttpServiceProfile, ReqwestHttpRequestService};
use teaql_runtime::{
    HttpCall, HttpCancellation, HttpFailure, HttpMethod, HttpResponse, HttpRetryMode, UserContext,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::time::timeout;

type AppResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const PROFILE: &str = "supplier-orders";
const QUERY: &str = "probe=PRIVATE_QUERY_SYNC";
const FIXTURE_BUDGET: Duration = Duration::from_secs(8);

struct Scenario {
    name: &'static str,
    attempts: u8,
    status: &'static str,
    retry: HttpRetryMode,
}

impl Scenario {
    fn reference(&self, round: &str) -> String {
        format!("SYNC-{}-{round}", self.name)
    }
}

const SCENARIOS: [Scenario; 5] = [
    Scenario {
        name: "direct",
        attempts: 1,
        status: "acknowledged",
        retry: HttpRetryMode::None,
    },
    Scenario {
        name: "retry",
        attempts: 2,
        status: "dispatched",
        retry: HttpRetryMode::ConfirmedIdempotent,
    },
    Scenario {
        name: "failed",
        attempts: 2,
        status: "pending",
        retry: HttpRetryMode::ConfirmedIdempotent,
    },
    Scenario {
        name: "cancel",
        attempts: 1,
        status: "pending",
        retry: HttpRetryMode::ConfirmedIdempotent,
    },
    Scenario {
        name: "mismatch",
        attempts: 1,
        status: "pending",
        retry: HttpRetryMode::None,
    },
];

async fn serve(
    listener: TcpListener,
    round: String,
    arrived: Arc<Notify>,
) -> AppResult<Vec<Value>> {
    let mut requests = Vec::new();
    for scenario in SCENARIOS.iter() {
        let reference = scenario.reference(&round);
        for attempt in 1..=scenario.attempts {
            let (stream, peer) = timeout(FIXTURE_BUDGET, listener.accept()).await??;
            assert!(peer.ip().is_loopback());
            let mut request = BufReader::new(stream);
            let mut line = String::new();
            timeout(FIXTURE_BUDGET, request.read_line(&mut line)).await??;
            assert_eq!(
                line.trim_end(),
                format!("GET /api/orders/{reference}?{QUERY} HTTP/1.1")
            );
            let mut traceparent = None;
            loop {
                line.clear();
                timeout(FIXTURE_BUDGET, request.read_line(&mut line)).await??;
                if line == "\r\n" {
                    break;
                }
                assert!(!line.is_empty());
                if let Some((key, value)) = line.split_once(':') {
                    if key.eq_ignore_ascii_case("traceparent") {
                        traceparent = Some(value.trim().to_owned());
                    }
                }
            }
            requests.push(
                json!({"case": scenario.name, "attempt": attempt, "traceparent": traceparent}),
            );
            if scenario.name == "cancel" {
                arrived.notify_one();
                let mut byte = [0; 1];
                assert_eq!(
                    timeout(FIXTURE_BUDGET, request.read(&mut byte)).await??,
                    0,
                    "The client must release the cancelled request"
                );
                continue;
            }
            let status_line =
                if scenario.name == "failed" || (scenario.name == "retry" && attempt == 1) {
                    "503 Service Unavailable"
                } else {
                    "200 OK"
                };
            let reply_reference = if scenario.name == "mismatch" {
                "OTHER-ORDER"
            } else {
                &reference
            };
            let proposed_status = if scenario.name == "mismatch" || status_line.starts_with("503") {
                "dispatched"
            } else {
                scenario.status
            };
            let body = json!({"reference": reply_reference, "status": proposed_status}).to_string();
            let response = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            request.get_mut().write_all(response.as_bytes()).await?;
            request.get_mut().shutdown().await?;
        }
    }
    assert_eq!(requests.len(), 7);
    Ok(requests)
}

async fn apply_reply(
    context: &UserContext,
    mut order: SupplierOrder,
    reply: HttpResponse,
) -> AppResult<&'static str> {
    if reply.status() != 200 {
        return Ok("unavailable");
    }
    let payload = reply.json_value()?;
    let reference = E::supplier_order(&order)
        .get_external_reference()
        .eval()
        .ok_or("The order needs a supplier reference")?;
    if payload["reference"] != reference {
        return Ok("wrong-order");
    }
    let Some(status @ ("acknowledged" | "dispatched")) = payload["status"].as_str() else {
        return Ok("invalid-status");
    };
    order.update_sync_status(status);
    order
        .audit_as("Apply the verified supplier status")
        .save(context)
        .await?;
    Ok("applied")
}

async fn reconcile(context: &UserContext, round: &str, arrived: &Notify) -> AppResult<Value> {
    let supplier = Q::suppliers_minimal()
        .with_id_is(1)
        .limit(1)
        .comment("Read the supplier installed by the model")
        .purpose("Link procurement orders to the configured supplier")
        .execute_for_one(context)
        .await?
        .ok_or("Supplier was not found")?;
    let mut result = BTreeMap::new();
    let mut outcomes = BTreeMap::new();
    for scenario in SCENARIOS.iter() {
        let reference = scenario.reference(round);
        let mut order = Q::supplier_orders()
            .comment("Create one procurement order awaiting supplier status")
            .purpose("Retain local pending status until the response is verified")
            .new_entity(context)?;
        order
            .update_supplier_id(supplier.id())
            .update_external_reference(reference.as_str())
            .update_sync_status("pending");
        let order = order
            .audit_as("Record a pending procurement order")
            .save(context)
            .await?;
        let requested_reference = E::supplier_order(&order)
            .get_external_reference()
            .eval()
            .ok_or("The order needs a supplier reference")?;
        let call = HttpCall::get(
            PROFILE,
            format!("orders/{requested_reference}?{QUERY}"),
            "Read dispatch status from the named supplier",
            format!(
                "Reconcile the procurement order after the {} request",
                scenario.name
            ),
        )?
        .with_retry_mode(scenario.retry);
        let http = context.http_requests()?;
        let order_id = order.id();
        let response = if scenario.name == "cancel" {
            let cancellation = HttpCancellation::new();
            let interrupt = async {
                let result = timeout(FIXTURE_BUDGET, arrived.notified()).await;
                cancellation.cancel();
                result
            };
            let (res, delivered) = tokio::join!(
                http.execute_cancellable(context, &call, &cancellation),
                interrupt
            );
            delivered?;
            res
        } else {
            http.execute(context, &call).await
        };
        let outcome = match response {
            Ok(reply) => {
                assert_eq!(reply.attempts(), scenario.attempts);
                apply_reply(context, order, reply).await?
            }
            Err(HttpFailure::Cancelled) => "cancelled",
            Err(error) => return Err(error.into()),
        };
        outcomes.insert(scenario.name, outcome);
        let stored = Q::supplier_orders()
            .with_id_is(order_id)
            .limit(1)
            .comment("Read the order after its HTTP reconciliation")
            .purpose("Check the persisted supplier result")
            .execute_for_one(context)
            .await?
            .ok_or("The reconciled order was not found")?;
        assert_eq!(
            E::supplier_order(&stored)
                .get_sync_status()
                .eval()
                .as_deref(),
            Some(scenario.status)
        );
        assert_eq!(
            E::supplier_order(&stored)
                .get_external_reference()
                .eval()
                .as_deref(),
            Some(reference.as_str())
        );
        assert_eq!(
            stored.version(),
            if scenario.status == "pending" { 1 } else { 2 }
        );
        result.insert(scenario.name, stored.id());
    }
    assert_eq!(
        outcomes,
        BTreeMap::from([
            ("direct", "applied"),
            ("retry", "applied"),
            ("failed", "unavailable"),
            ("cancel", "cancelled"),
            ("mismatch", "wrong-order"),
        ])
    );
    Ok(json!({"orders": result, "outcomes": outcomes}))
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let round = std::env::var("TEAQL_SHOWCASE_ROUND")?;
    let database_url = format!(
        "sqlite:file:{}",
        std::env::var("TEAQL_SUPPLIER_SYNC_DATABASE")?
    );
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await?;
    let base = url::Url::parse(&format!("http://{}/api/", listener.local_addr()?))?;
    let profile = HttpServiceProfile::new(PROFILE, base)?
        .budgets(
            Duration::from_secs(2),
            Duration::from_secs(6),
            Duration::from_secs(7),
        )?
        .retry(
            2,
            Duration::from_millis(5),
            BTreeSet::from([HttpMethod::Get]),
        )?;
    let context = service_runtime(ServiceRuntimeConfig { database_url })
        .await?
        .with_http_request_service(Arc::new(ReqwestHttpRequestService::new(vec![profile])?));
    context.ensure_schema().await?;
    let arrived = Arc::new(Notify::new());
    let fixture = tokio::spawn(serve(listener, round.clone(), arrived.clone()));
    let reconciled = reconcile(&context, &round, &arrived).await;
    if reconciled.is_err() {
        fixture.abort();
        let _ = fixture.await;
        return reconciled.map(|_| ());
    }
    let requests = fixture.await??;
    context.runtime_telemetry().flush();
    context.runtime_telemetry().shutdown();
    let reconciled = reconciled?;
    println!(
        "SUPPLIER_ORDER_SYNC={}",
        json!({
            "round": round, "orders": reconciled["orders"], "outcomes": reconciled["outcomes"],
            "requests": requests.len(), "cancelled": true,
            "wireRequests": requests
        })
    );
    Ok(())
}
