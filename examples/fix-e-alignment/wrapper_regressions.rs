//! Small, application-owned Q/E and real SQLite probes for generated wrappers.
//! The runner supplies two freshly generated libraries; neither is hand edited.
use checker_fix_advanced_core::{AuditedSave, E, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use futures_util::{FutureExt, StreamExt};
use serde_json::{Value as Json, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, SelectQuery, Value};
use teaql_data_service::{
    ExecutionMetadata, QueryRequest, SqlExecutionOutcome, StreamQueryExecutor,
};
use teaql_runtime::FixedBusinessClock;

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);
fn observe(_: &str) {
    STATEMENTS.fetch_add(1, Ordering::SeqCst);
}
fn count() -> usize {
    STATEMENTS.load(Ordering::SeqCst)
}
fn record(cases: &mut Vec<Json>, name: &str, passed: bool, details: Json) {
    let value = json!({"name":name,"passed":passed,"details":details});
    println!("RUST_WRAPPER_CASE={value}");
    cases.push(value);
}
fn panic_text(value: Box<dyn std::any::Any + Send>) -> String {
    value
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| value.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_default()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let round: usize = std::env::var("TEAQL_WRAPPER_ROUND")?.parse()?;
    let database = std::env::var("TEAQL_WRAPPER_DB")?;
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(&database)?));
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    context.set_business_clock(FixedBusinessClock::new(
        Utc.with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
            .single()
            .unwrap(),
    ));
    context.ensure_schema().await?;
    let prior: i64 =
        pool.lock()
            .unwrap()
            .query_row("SELECT count(*) FROM purchase_order_data", [], |r| r.get(0))?;
    assert_eq!(
        prior,
        ((round - 1) * 2) as i64,
        "rounds reuse the unreset business database"
    );
    let mut other = Q::tenants()
        .comment("Prepare an alternate relation target")
        .purpose("Verify FK mutation invalidates an old relation view")
        .new_entity(&context)
        .expect("reserve example identity");
    other.update_name(format!("Other root {round}"));
    let other = other
        .audit_as("Create the independent alternate root")
        .save(&context)
        .await?;
    let mut owner = Q::purchase_orders()
        .comment("Prepare one wrapper fixture")
        .purpose("Verify relation views and stream observation")
        .new_entity(&context)
        .expect("reserve example identity");
    owner.update_contact_phone("5550880");
    owner.update_internal_note(Value::Null);
    let owner = owner
        .audit_as("Create the generated wrapper fixture")
        .save(&context)
        .await?;
    let id = owner.id();
    assert!(id > 0);
    let mut second = Q::purchase_orders()
        .comment("Prepare a second ordinary relation target")
        .purpose("Verify FK invalidation without bypassing context-root immutability")
        .new_entity(&context)
        .expect("reserve example identity");
    second.update_contact_phone("5550880");
    second.update_internal_note(Value::Null);
    let second = second
        .audit_as("Create the second wrapper fixture")
        .save(&context)
        .await?;
    let mut item = Q::order_items()
        .comment("Prepare an ordinary mutable FK")
        .purpose("Verify a loaded target cannot survive retargeting")
        .new_entity(&context)
        .expect("reserve example identity");
    item.update_purchase_order_id(id);
    item.update_product_name(format!("Wrapper item {round}"));
    item.update_unit_price(Value::Decimal("1.00".parse()?));
    let item = item
        .audit_as("Create the wrapper relation child")
        .save(&context)
        .await?;
    context.enable_all_sql_log();
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
    let mut cases = Vec::new();

    let filtered = Q::tenants()
        .with_id_is(1_u64)
        .select_purchase_order_list_with(
            Q::purchase_orders()
                .with_id_is(id)
                .select_tenant_with(Q::tenants_minimal().with_id_is(other.id()).limit(1))
                .limit(1),
        )
        .limit(1)
        .comment("Load the real root and a filtered edge to that same identity")
        .purpose("Do not replace filtered detail with a shared full object")
        .execute_for_list(&context)
        .await?;
    assert_eq!(filtered.len(), 1);
    let root = filtered.first().unwrap();
    let before = count();
    assert!(E::tenant(root).get_name().eval().is_some());
    assert_eq!(
        E::tenant(root)
            .get_purchase_order_list()
            .first()
            .get_tenant_id()
            .eval(),
        Some(1)
    );
    let error = E::tenant(root)
        .get_purchase_order_list()
        .first()
        .get_tenant()
        .get_name()
        .try_eval()
        .unwrap_err()
        .to_wire();
    let no_io = count() == before;
    record(
        &mut cases,
        "filtered-edge-before-shared-identity",
        no_io
            && error.instance_path.ends_with("/tenant")
            && error.attempted_instance_path.ends_with("/tenant/name"),
        json!({"owner":id,"target":1,"targetNameOnOuterRootLoaded":true,"fkLoaded":true,"instancePath":error.instance_path,"attemptedPath":error.attempted_instance_path,"eStatements":count()-before}),
    );

    let sparse = Q::tenants()
        .with_id_is(1_u64)
        .select_purchase_order_list_with(
            Q::purchase_orders()
                .with_id_is(id)
                .select_tenant_with(Q::tenants_minimal().limit(1))
                .limit(1),
        )
        .limit(1)
        .comment("Keep a sparse edge beside a full shared root")
        .purpose("Preserve request-local projection boundaries")
        .execute_for_list(&context)
        .await?;
    let root = sparse.first().unwrap();
    let before = count();
    assert!(E::tenant(root).get_name().eval().is_some());
    let error = E::tenant(root)
        .get_purchase_order_list()
        .first()
        .get_tenant()
        .get_name()
        .try_eval()
        .unwrap_err()
        .to_wire();
    record(
        &mut cases,
        "sparse-edge-before-shared-identity",
        count() == before && error.instance_path.ends_with("/name"),
        json!({"owner":id,"outerNameLoaded":true,"instancePath":error.instance_path,"eStatements":count()-before}),
    );

    let complete = Q::order_items()
        .with_id_is(item.id())
        .select_purchase_order_with(Q::purchase_orders().limit(1))
        .limit(1)
        .comment("Load a complete old relation target")
        .purpose("Invalidate old views after a pending FK change")
        .execute_for_list(&context)
        .await?;
    let mut changed = complete.first().unwrap().clone();
    let before = count();
    assert!(
        E::order_item(&changed)
            .get_purchase_order()
            .get_contact_phone()
            .eval()
            .is_some()
    );
    changed.update_purchase_order_id(second.id());
    assert_eq!(
        E::order_item(&changed).get_purchase_order_id().eval(),
        Some(second.id())
    );
    let error = E::order_item(&changed)
        .get_purchase_order()
        .get_contact_phone()
        .try_eval()
        .unwrap_err()
        .to_wire();
    record(
        &mut cases,
        "changed-fk-invalidates-edge",
        count() == before && error.instance_path == "/purchaseOrder",
        json!({"owner":item.id(),"newTarget":second.id(),"instancePath":error.instance_path,"eStatements":count()-before}),
    );

    let metadata: Arc<Mutex<Vec<ExecutionMetadata>>> = Arc::default();
    let capture = metadata.clone();
    let wrapper = context
        .require_resource::<checker_fix_advanced_core::runtime::DataServiceExecutor>()?
        .clone()
        .with_query_metadata_observer(Arc::new(move |m| capture.lock().unwrap().push(m.clone())));
    context.register_executor(wrapper);
    metadata.lock().unwrap().clear();
    context.clear_sql_logs();
    let mut stream = Q::purchase_orders()
        .with_id_is(id)
        .limit(1)
        .stream(1)
        .comment("Observe a completed generated stream")
        .purpose("Preserve caller and trusted completion observers")
        .execute_for_stream(&context)
        .await?;
    let row = stream.next().await.ok_or("missing streamed owner")??;
    assert_eq!(
        E::purchase_order(&row)
            .get_contact_phone()
            .eval()
            .as_deref(),
        Some("5550880")
    );
    assert!(stream.next().await.is_none());
    drop(stream);
    let observed = metadata.lock().unwrap().clone();
    let logs = context.sql_logs();
    record(
        &mut cases,
        "completed-stream-observers",
        observed.len() == 1
            && logs.len() == 1
            && observed[0].result_count == Some(1)
            && observed[0].sql_log.execution_outcome == Some(SqlExecutionOutcome::Success),
        json!({"trusted":observed.len(),"callerLogs":logs.len(),"rows":1,"outcome":format!("{:?}",observed.first().and_then(|m| m.sql_log.execution_outcome))}),
    );

    metadata.lock().unwrap().clear();
    context.clear_sql_logs();
    let before = count();
    let stream = Q::purchase_orders()
        .with_id_is(id)
        .limit(1)
        .stream(1)
        .comment("Prepare but do not poll a generated stream")
        .purpose("Do not manufacture an execution event")
        .execute_for_stream(&context)
        .await?;
    drop(stream);
    let observed = metadata.lock().unwrap().len();
    let logs = context.sql_logs().len();
    record(
        &mut cases,
        "unpolled-stream-is-unrecorded",
        observed == 0 && logs == 0 && count() == before,
        json!({"trusted":observed,"callerLogs":logs,"statements":count()-before}),
    );

    metadata.lock().unwrap().clear();
    context.clear_sql_logs();
    let mut stream = Q::purchase_orders()
        .limit(2)
        .stream(1)
        .comment("Drop a consumed but unfinished stream")
        .purpose("Retain a truthful cancellation outcome")
        .execute_for_stream(&context)
        .await?;
    assert!(
        stream
            .next()
            .await
            .ok_or("missing cancellation control row")?
            .is_ok()
    );
    drop(stream);
    let observed = metadata.lock().unwrap().clone();
    let logs = context.sql_logs();
    record(
        &mut cases,
        "cancelled-stream-observers",
        observed.len() == 1
            && logs.len() == 1
            && observed[0].sql_log.execution_outcome == Some(SqlExecutionOutcome::Cancelled),
        json!({"trusted":observed.len(),"callerLogs":logs.len(),"outcome":format!("{:?}",observed.first().and_then(|m| m.sql_log.execution_outcome))}),
    );

    let panics = Arc::new(AtomicUsize::new(0));
    let called = panics.clone();
    let wrapper = context
        .require_resource::<checker_fix_advanced_core::runtime::DataServiceExecutor>()?
        .clone()
        .with_query_metadata_observer(Arc::new(move |_| {
            called.fetch_add(1, Ordering::SeqCst);
            panic!("intentional trusted observer failure");
        }));
    context.register_executor(wrapper);
    context.clear_sql_logs();
    let mut stream = Q::purchase_orders()
        .with_id_is(id)
        .limit(1)
        .stream(1)
        .comment("Isolate a trusted observer failure")
        .purpose("Still forward caller diagnostics and actual rows")
        .execute_for_stream(&context)
        .await?;
    assert!(
        stream
            .next()
            .await
            .ok_or("missing panic control row")?
            .is_ok()
    );
    assert!(stream.next().await.is_none());
    drop(stream);
    let logs = context.sql_logs();
    record(
        &mut cases,
        "trusted-panic-does-not-drop-upstream",
        panics.load(Ordering::SeqCst) == 1 && logs.len() == 1,
        json!({"trusted":panics.load(Ordering::SeqCst),"callerLogs":logs.len()}),
    );

    for (kind, blank) in [
        ("empty", ""),
        ("ascii-whitespace", " \t\r\n"),
        ("NEL", "\u{85}"),
        ("NBSP", "\u{a0}"),
        ("em-space", "\u{2003}"),
    ] {
        let before = count();
        context.clear_sql_logs();
        let result = std::panic::catch_unwind(|| {
            Q::purchase_orders()
                .limit(1)
                .comment("PRIVATE-WRAPPER-CANARY")
                .purpose(blank)
        });
        let message = result.err().map(panic_text).unwrap_or_default();
        record(
            &mut cases,
            &format!("blank-purpose-{kind}"),
            message
                == "QUERY_PURPOSE_REQUIRED: Query Request requires a non-blank purpose; supply it at the request entry point"
                && count() == before
                && context.sql_logs().is_empty(),
            json!({"code":"QUERY_PURPOSE_REQUIRED","statements":count()-before,"privateCanaryExposed":message.contains("PRIVATE-WRAPPER-CANARY")}),
        );
    }
    for (kind, value) in [
        ("missing", None),
        ("empty", Some("")),
        ("ascii-whitespace", Some(" \t\r\n")),
        ("NEL", Some("\u{85}")),
        ("NBSP", Some("\u{a0}")),
        ("em-space", Some("\u{2003}")),
    ] {
        let request = Q::purchase_orders()
            .with_id_is(id)
            .select_tenant_with(Q::tenants_minimal().comment("valid child comment").limit(1))
            .limit(1);
        let request = if let Some(value) = value {
            request.comment(value)
        } else {
            request
        };
        let before = count();
        context.clear_sql_logs();
        let result = std::panic::AssertUnwindSafe(async {
            request
                .purpose("PRIVATE-WRAPPER-CANARY")
                .execute_for_list(&context)
                .await
        })
        .catch_unwind()
        .await;
        let (boundary, message) = match result {
            Err(value) => ("panic", panic_text(value)),
            Ok(Err(error)) => ("result", error.to_string()),
            Ok(Ok(_)) => ("accepted", String::new()),
        };
        let diagnostic = message
            == "REQUEST_COMMENT_REQUIRED: Query Request requires a non-blank comment; supply it at the request entry point";
        record(
            &mut cases,
            &format!("blank-comment-{kind}"),
            diagnostic
                && count() == before
                && context.sql_logs().is_empty()
                && !message.contains("PRIVATE-WRAPPER-CANARY"),
            json!({"diagnosticMatches":diagnostic,"boundary":boundary,"code":"REQUEST_COMMENT_REQUIRED","field":"comment","requestKind":"Query","privateCanaryExposed":message.contains("PRIVATE-WRAPPER-CANARY"),"statements":count()-before,"sqlLogs":context.sql_logs().len()}),
        );
    }

    let search_pool = Arc::new(Mutex::new(rusqlite::Connection::open(format!(
        "{database}.search.sqlite"
    ))?));
    let mut search_context = search_core::service_runtime_from_pool(search_pool.clone())
        .await?
        .with_active_root("Platform", 1);
    search_context.set_business_clock(FixedBusinessClock::new(
        Utc.with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
            .single()
            .unwrap(),
    ));
    search_context.ensure_schema().await?;
    let trusted: Arc<Mutex<Vec<ExecutionMetadata>>> = Arc::default();
    let caller: Arc<Mutex<Vec<ExecutionMetadata>>> = Arc::default();
    let captured = trusted.clone();
    let wrapper = search_context
        .require_resource::<search_core::runtime::DataServiceExecutor>()?
        .clone()
        .with_query_metadata_observer(Arc::new(move |m| captured.lock().unwrap().push(m.clone())));
    let captured = caller.clone();
    let request = QueryRequest::from_query(
        SelectQuery::new("Document")
            .comment("Observe search metadata with a SQL fallback")
            .purpose("Do not lose the caller observer when search is not configured")
            .limit(1),
    )?;
    let mut stream = StreamQueryExecutor::query_stream_observed(
        &wrapper,
        request,
        1,
        Arc::new(move |m| captured.lock().unwrap().push(m)),
    );
    while let Some(chunk) = stream.next().await {
        chunk?;
    }
    drop(stream);
    let tc = trusted.lock().unwrap().len();
    let cc = caller.lock().unwrap().len();
    record(
        &mut cases,
        "search-sql-fallback-retains-both-observers",
        tc == 1 && cc == 1,
        json!({"trusted":tc,"caller":cc}),
    );

    trusted.lock().unwrap().clear();
    caller.lock().unwrap().clear();
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let provider = teaql_provider_meilisearch::MeilisearchProvider::new(
        format!("http://{}", listener.local_addr()?),
        None,
    );
    let wrapper = wrapper.with_meilisearch(provider);
    let captured = caller.clone();
    let request = QueryRequest::from_query(
        SelectQuery::new("Document")
            .comment("Reject an unsupported search stream")
            .purpose("Return an error without inventing an execution or network request")
            .limit(1),
    )?;
    let mut stream = StreamQueryExecutor::query_stream_observed(
        &wrapper,
        request,
        1,
        Arc::new(move |m| captured.lock().unwrap().push(m)),
    );
    let error = stream
        .next()
        .await
        .ok_or("missing unsupported search result")?
        .unwrap_err()
        .to_string();
    assert!(stream.next().await.is_none());
    drop(stream);
    let no_network = matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock);
    let tc = trusted.lock().unwrap().len();
    let cc = caller.lock().unwrap().len();
    record(
        &mut cases,
        "unsupported-search-does-not-invent-execution",
        error.contains("Meilisearch stream queries not supported")
            && tc == 0
            && cc == 0
            && no_network,
        json!({"trusted":tc,"caller":cc,"networkRequests":if no_network {0}else{1},"unsupported":true}),
    );

    let failed: Vec<_> = cases
        .iter()
        .filter(|v| v["passed"] != true)
        .map(|v| v["name"].clone())
        .collect();
    println!(
        "RUST_WRAPPER_RESULT={}",
        json!({"round":round,"owner":id,"cases":cases.len(),"failed":failed,"passed":failed.is_empty()})
    );
    assert!(
        failed.is_empty(),
        "generated wrapper regressions: {failed:?}"
    );
    Ok(())
}
