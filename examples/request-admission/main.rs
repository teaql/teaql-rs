//! Request-owned intent admission through generated Q, audited save and SQLite.
//! Metadata callbacks count completed provider observations, not provider entry.
use checker_fix_advanced_core::{AuditedSave, E, LedgerEntity, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use futures_util::{FutureExt, StreamExt};
use serde_json::{Value as Json, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, SelectQuery, Value};
use teaql_runtime::{
    FixedBusinessClock, MutationDecision, MutationPlan, MutationPolicy, MutationPolicyIdentity,
    RequestPolicy, RuntimeAttributeValue, RuntimeError, RuntimeOperation, RuntimeTelemetry,
    RuntimeTelemetryScope, SafeAuditEvent, SafeAuditEventSink, UserContext,
};

static SQL: AtomicUsize = AtomicUsize::new(0);
fn observe(_: &str) {
    SQL.fetch_add(1, Ordering::SeqCst);
}
#[derive(Default)]
struct Counters {
    query_policy: AtomicUsize,
    mutation_policy: AtomicUsize,
    governance: AtomicUsize,
    provider_metadata: AtomicUsize,
    audits: AtomicUsize,
    provider_boundary: AtomicUsize,
}
struct Telemetry(Arc<Counters>);
struct Scope;
impl RuntimeTelemetryScope for Scope {
    fn success(&mut self, _: std::collections::BTreeMap<String, RuntimeAttributeValue>) {}
    fn failure(&mut self, _: &str) {}
}
impl RuntimeTelemetry for Telemetry {
    fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
        if operation.family == "provider" {
            self.0.provider_boundary.fetch_add(1, Ordering::SeqCst);
        }
        Box::new(Scope)
    }
}
struct Policy(Arc<Counters>);
impl RequestPolicy for Policy {
    fn enforce_select(&self, _: &UserContext, _: &mut SelectQuery) -> Result<(), RuntimeError> {
        self.0.query_policy.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct MutationReview(Arc<Counters>);
impl MutationPolicy for MutationReview {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("request-admission", "1", "local-example")
    }
    fn review(&self, _: &UserContext, _: &MutationPlan) -> MutationDecision {
        self.0.mutation_policy.fetch_add(1, Ordering::SeqCst);
        MutationDecision::allow()
    }
}
struct Audit(Arc<Counters>);
impl SafeAuditEventSink for Audit {
    fn on_safe_event(&self, _: &UserContext, _: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.0.audits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn snapshot(c: &Counters, ctx: &UserContext, pool: &Arc<Mutex<rusqlite::Connection>>) -> Vec<u64> {
    vec![
        c.query_policy.load(Ordering::SeqCst) as u64,
        c.mutation_policy.load(Ordering::SeqCst) as u64,
        c.governance.load(Ordering::SeqCst) as u64,
        c.provider_metadata.load(Ordering::SeqCst) as u64,
        c.audits.load(Ordering::SeqCst) as u64,
        SQL.load(Ordering::SeqCst) as u64,
        ctx.sql_logs().len() as u64,
        pool.lock().unwrap().total_changes(),
        c.provider_boundary.load(Ordering::SeqCst) as u64,
    ]
}
fn effects(before: &[u64], after: &[u64]) -> Json {
    let names = [
        "queryPolicy",
        "mutationPolicy",
        "governanceResolution",
        "providerMetadata",
        "audits",
        "sqlStatements",
        "sqlLogs",
        "physicalChanges",
        "providerBoundaryStarts",
    ];
    json!(
        names
            .into_iter()
            .zip(before.iter().zip(after).map(|(a, b)| b - a))
            .collect::<std::collections::BTreeMap<_, _>>()
    )
}
fn panic_text(value: Box<dyn std::any::Any + Send>) -> String {
    value
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| value.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_default()
}
async fn query(
    ctx: &UserContext,
    id: u64,
    comment: Option<&str>,
    purpose: &str,
    mode: &str,
    child_selection: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = Q::purchase_orders().with_id_is(id).limit(1).stream(1);
    let request = if child_selection {
        request.select_tenant_with(Q::tenants_minimal().limit(1).comment("valid child comment"))
    } else {
        request
    };
    let request = if mode == "page-count" {
        request.count()
    } else {
        request
    };
    let request = if let Some(comment) = comment {
        request.comment(comment)
    } else {
        request
    };
    let request = request.purpose(purpose);
    match mode {
        "list" | "page-count" => {
            let _ = request.execute_for_list(ctx).await?;
        }
        "one" => {
            let _ = request.execute_for_one(ctx).await?;
        }
        "exact-count" => {
            let _ = request.execute_for_count(ctx).await?;
        }
        "stream" => {
            let mut rows = request.execute_for_stream(ctx).await?;
            while let Some(row) = rows.next().await {
                let _ = row?;
            }
        }
        _ => panic!("unknown query mode"),
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::var("TEAQL_REQUEST_ADMISSION_DB")?;
    let round: usize = std::env::var("TEAQL_REQUEST_ADMISSION_ROUND")?.parse()?;
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(database)?));
    let counts = Arc::new(Counters::default());
    let governance = counts.clone();
    let mut ctx = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1)
        .with_request_policy(Policy(counts.clone()))
        .with_runtime_telemetry(Arc::new(Telemetry(counts.clone())))
        .with_custom_event_sink(Audit(counts.clone()))
        .with_mutation_policy_registry(
            move |_: &str| -> Option<Arc<dyn teaql_runtime::MutationPolicy>> {
                governance.governance.fetch_add(1, Ordering::SeqCst);
                Some(Arc::new(MutationReview(governance.clone())))
            },
        );
    ctx.set_business_clock(FixedBusinessClock::new(
        Utc.with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
            .single()
            .unwrap(),
    ));
    ctx.ensure_schema().await?;
    let capture = counts.clone();
    let executor = ctx
        .require_resource::<checker_fix_advanced_core::runtime::DataServiceExecutor>()?
        .clone()
        .with_query_metadata_observer(Arc::new(move |_| {
            capture.provider_metadata.fetch_add(1, Ordering::SeqCst);
        }));
    ctx.register_executor(executor);
    pool.lock()
        .unwrap()
        .trace_v2(
            rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
            Some(|event: rusqlite::trace::TraceEvent<'_>| {
                if let rusqlite::trace::TraceEvent::Stmt(statement, sql) = event {
                    if sql.starts_with("--") {
                        observe(sql);
                    } else {
                        observe(&statement.expanded_sql().expect("expanded SQLite trace SQL"));
                    }
                }
            }),
        )
        .unwrap();
    let prior: i64 =
        pool.lock()
            .unwrap()
            .query_row("SELECT count(*) FROM purchase_order_data", [], |r| r.get(0))?;
    assert_eq!(
        prior,
        ((round - 1) * 2) as i64,
        "reuse the database without reset"
    );
    let mut total = 0;
    for logging in [false, true] {
        if logging {
            ctx.enable_all_sql_log();
        } else {
            ctx.disable_sql_log();
        }
        let mut owner = Q::purchase_orders()
            .comment("Prepare request intent fixture")
            .purpose("Verify request admission before side effects")
            .new_entity(&ctx);
        owner.update_contact_phone("5550801");
        owner.update_internal_note(Value::Null);
        let mut owner = owner
            .audit_as("Create the intent test owner")
            .save(&ctx)
            .await?;
        let mut child = Q::order_items()
            .comment("Prepare an annotated child")
            .purpose("A child reason cannot authorize the root")
            .new_entity(&ctx);
        child.update_purchase_order_id(owner.id());
        child.update_product_name(format!("admission child {round} {logging}"));
        child.update_unit_price(Value::Decimal("1.00".parse()?));
        let mut child = child
            .audit_as("Create the independent child")
            .save(&ctx)
            .await?;
        child.update_product_name(format!("approved child {round} {logging}"));
        let child = child.audit_as("Valid local child reason").into_entity();
        owner.update_contact_phone("5550802");
        owner.include_pending_mutations_from(&child)?;
        let identity = (owner.id(), owner.version(), child.id(), child.version());
        let blank = [
            ("empty", ""),
            ("ascii-whitespace", " \t\r\n"),
            ("NEL", "\u{85}"),
            ("NBSP", "\u{a0}"),
            ("em-space", "\u{2003}"),
        ];
        for action in ["query-comment", "query-purpose", "mutation-comment"] {
            let kinds: Vec<_> = if action == "query-comment" {
                std::iter::once(("missing", None))
                    .chain(blank.iter().map(|(k, v)| (*k, Some(*v))))
                    .collect()
            } else {
                blank.iter().map(|(k, v)| (*k, Some(*v))).collect()
            };
            let modes: &[&str] = if action == "mutation-comment" {
                &["audited-save"]
            } else {
                &["list", "one", "exact-count", "page-count", "stream"]
            };
            for (kind, invalid) in kinds {
                for mode in modes {
                    ctx.clear_sql_logs();
                    let before = snapshot(&counts, &ctx, &pool);
                    let outcome = std::panic::AssertUnwindSafe(async {
                        if action == "mutation-comment" {
                            let _ = owner.clone().audit_as(invalid.unwrap()).save(&ctx).await?;
                            Ok::<(), Box<dyn std::error::Error>>(())
                        } else {
                            let comment = if action == "query-comment" {
                                invalid
                            } else {
                                Some("PRIVATE-INTENT-CANARY")
                            };
                            let purpose = if action == "query-purpose" {
                                invalid.unwrap()
                            } else {
                                "PRIVATE-INTENT-CANARY"
                            };
                            query(&ctx, owner.id(), comment, purpose, mode, true).await
                        }
                    })
                    .catch_unwind()
                    .await;
                    let (boundary, message) = match outcome {
                        Err(value) => ("panic", panic_text(value)),
                        Ok(Err(error)) => ("result", error.to_string()),
                        Ok(Ok(())) => ("accepted", String::new()),
                    };
                    let after = snapshot(&counts, &ctx, &pool);
                    let (code, field, request_kind) = if action == "query-purpose" {
                        ("QUERY_PURPOSE_REQUIRED", "purpose", "Query")
                    } else {
                        (
                            "REQUEST_COMMENT_REQUIRED",
                            "comment",
                            if action == "mutation-comment" {
                                "Mutation"
                            } else {
                                "Query"
                            },
                        )
                    };
                    let expected = format!(
                        "{code}: {request_kind} Request requires a non-blank {field}; supply it at the request entry point"
                    );
                    let unchanged =
                        identity == (owner.id(), owner.version(), child.id(), child.version());
                    let passed = before == after
                        && message == expected
                        && unchanged
                        && !message.contains("PRIVATE-INTENT-CANARY");
                    println!(
                        "RUST_REQUEST_ADMISSION_CASE={}",
                        json!({"round":round,"logging":logging,
                    "action":action,"mode":mode,"inputKind":kind,"boundary":boundary,"code":code,
                    "field":field,"requestKind":request_kind,"passed":passed,"effects":effects(&before,&after),
                    "identitiesUnchanged":unchanged,"childReasonRetained":child.get_comment().as_deref()==Some("Valid local child reason"),
                    "privateValuesNotEchoed":!message.contains("PRIVATE-INTENT-CANARY")})
                    );
                    assert!(
                        passed,
                        "intent admission failed for {action}/{mode}/{kind}: {message}"
                    );
                    total += 1;
                }
            }
        }
        ctx.clear_sql_logs();
        let before = snapshot(&counts, &ctx, &pool);
        let saved = owner
            .audit_as("Approve the root and annotated child")
            .save(&ctx)
            .await?;
        let values = Q::purchase_orders()
            .with_id_is(saved.id())
            .select_order_item_list_with(Q::order_items().limit(5))
            .limit(1)
            .comment("Verify the saved graph")
            .purpose("Calibrate actual execution and loaded E")
            .execute_for_list(&ctx)
            .await?;
        let value = values.first().ok_or("missing saved owner")?;
        let e_before = SQL.load(Ordering::SeqCst);
        assert_eq!(
            E::purchase_order(value)
                .get_contact_phone()
                .eval()
                .as_deref(),
            Some("5550802")
        );
        assert_eq!(
            E::purchase_order(value)
                .get_order_item_list()
                .first()
                .get_product_name()
                .eval()
                .as_deref(),
            Some(format!("approved child {round} {logging}").as_str())
        );
        assert_eq!(SQL.load(Ordering::SeqCst), e_before);
        for mode in ["list", "one", "exact-count", "page-count", "stream"] {
            // Streams currently support scalar roots only. Invalid requests above
            // still carry a child selection: intent must reject before that limit.
            query(
                &ctx,
                saved.id(),
                Some("Calibrate a valid request"),
                "Verify all terminal APIs",
                mode,
                mode != "stream",
            )
            .await?;
        }
        let after = snapshot(&counts, &ctx, &pool);
        let calibrated = after[0] > before[0]
            && after[1] > before[1]
            && after[2] > before[2]
            && after[3] > before[3]
            && after[4] > before[4]
            && after[5] > before[5]
            && after[7] > before[7]
            && after[8] > before[8]
            && (after[6] > before[6]) == logging;
        println!(
            "RUST_REQUEST_ADMISSION_CONTROL={}",
            json!({"round":round,"logging":logging,"passed":calibrated,
            "owner":saved.id(),"child":child.id(),"eProviderReads":0,"effects":effects(&before,&after)})
        );
        assert!(
            calibrated,
            "each observed boundary must be calibrated by a real valid save/query"
        );
    }
    assert_eq!(total, 120);
    println!(
        "RUST_REQUEST_ADMISSION_RESULT={}",
        json!({"round":round,"cases":total,"controls":2,"passed":true})
    );
    Ok(())
}
