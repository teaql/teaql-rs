//! A discarded new child is not a save participant; persisted deletions still are.
use checker_fix_advanced_core::{AuditedSave, PurchaseOrder, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, Value};
use teaql_runtime::{FixedBusinessClock, LedgerEntity, RuntimeError, UserContext};

static SQL: AtomicUsize = AtomicUsize::new(0);
fn trace(_: &str) {
    SQL.fetch_add(1, Ordering::SeqCst);
}
fn child(context: &UserContext) -> PurchaseOrder {
    let mut value = Q::purchase_orders()
        .comment("Prepare optional child work")
        .purpose("Verify cancelled child omission at the normal audited save boundary")
        .new_entity(context)
        .expect("reserve example identity");
    value.update_contact_phone("5550166");
    value.update_internal_note(Value::Null);
    value
}
fn failure<T>(result: &Result<T, RuntimeError>) -> (&'static str, Vec<serde_json::Value>) {
    match result {
        Ok(_) => ("accepted", vec![]),
        Err(RuntimeError::Check(results)) => (
            "check",
            results
                .iter()
                .map(|v| {
                    serde_json::json!({
            "entity":v.entity_type,"path":v.location.instance_path(),"rule":v.rule.wire_id()})
                })
                .collect(),
        ),
        Err(_) => ("other_error", vec![]),
    }
}
pub async fn verify() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(Mutex::new(match std::env::var("TEAQL_FIX_E_DB") {
        Ok(path) => rusqlite::Connection::open(format!("{path}.cancelled-root.sqlite"))?,
        Err(_) => rusqlite::Connection::open_in_memory()?,
    }));
    let mut owner = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    let mut missing = service_runtime_from_pool(pool.clone()).await?;
    let now = Utc.with_ymd_and_hms(2032, 5, 6, 7, 8, 9).single().unwrap();
    owner.set_business_clock(FixedBusinessClock::new(now));
    missing.set_business_clock(FixedBusinessClock::new(now));
    owner.ensure_schema().await?;
    pool.lock()
        .unwrap()
        .trace_v2(
            rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
            Some(|event: rusqlite::trace::TraceEvent<'_>| {
                if let rusqlite::trace::TraceEvent::Stmt(statement, sql) = event {
                    if sql.starts_with("--") {
                        trace(sql);
                    } else {
                        trace(&statement.expanded_sql().expect("expanded SQLite trace SQL"));
                    }
                }
            }),
        )
        .unwrap();
    let mut cases = Vec::new();
    let initial_children: i64 =
        pool.lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM purchase_order_data", [], |r| r.get(0))?;
    for (name, cancel) in [("cancel-new-child", true), ("keep-new-child", false)] {
        let rows = Q::tenants()
            .with_id_is(1_u64)
            .limit(1)
            .comment("Load the existing graph owner")
            .purpose("Verify omission without manufacturing a new contextual owner")
            .execute_for_list(&owner)
            .await?;
        let mut parent = rows.first().ok_or("bootstrap owner missing")?.clone();
        let mut pending = child(&owner);
        if cancel {
            pending.mark_for_deletion();
        }
        parent.include_pending_mutations_from(&pending)?;
        let new_name = format!("{name}-{}-{initial_children}", parent.id());
        parent.update_name(new_name.clone());
        let before = SQL.load(Ordering::SeqCst);
        let result = parent
            .audit_as("Save only live graph participants")
            .save(&missing)
            .await;
        let statements = SQL.load(Ordering::SeqCst) - before;
        let (kind, violations) = failure(&result);
        let (stored_name, count): (String, i64) = pool.lock().unwrap().query_row(
            "SELECT name,(SELECT COUNT(*) FROM purchase_order_data) FROM tenant_data WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let passed = if cancel {
            result.is_ok() && stored_name == new_name && statements > 0 && count == initial_children
        } else {
            kind == "check"
                && statements == 0
                && stored_name != new_name
                && count == initial_children
        };
        cases.push(serde_json::json!({"case":name,"passed":passed,"errorKind":kind,
            "violations":violations,"saveStatements":statements,"storedName":stored_name,"childRows":count}));
    }
    let mut cancelled = child(&owner);
    cancelled.mark_for_deletion();
    let before = SQL.load(Ordering::SeqCst);
    let result = cancelled
        .audit_as("Reject a cancelled new root instead of inventing a persisted return value")
        .save(&owner)
        .await;
    let statements = SQL.load(Ordering::SeqCst) - before;
    cases.push(serde_json::json!({"case":"cancel-new-root","passed":result.as_ref().err().is_some_and(|e|e.to_string().contains("cancel"))&&statements==0,
        "saveStatements":statements,"error":result.err().map(|e|e.to_string())}));
    let mut persisted = child(&owner)
        .audit_as("Prepare a real existing child deletion control")
        .save(&owner)
        .await?;
    let id = persisted.id();
    persisted.mark_for_deletion();
    let rows = Q::tenants()
        .with_id_is(1_u64)
        .limit(1)
        .comment("Load the owner for the existing deletion control")
        .purpose("Verify an existing deleted child cannot skip contextual ownership")
        .execute_for_list(&owner)
        .await?;
    let mut parent = rows.first().ok_or("owner missing")?.clone();
    parent.include_pending_mutations_from(&persisted)?;
    parent.update_name("Existing child must reject");
    let before = SQL.load(Ordering::SeqCst);
    let result = parent
        .audit_as("Reject an existing child deletion without its context")
        .save(&missing)
        .await;
    let statements = SQL.load(Ordering::SeqCst) - before;
    let (kind, violations) = failure(&result);
    let version: i64 = pool.lock().unwrap().query_row(
        "SELECT version FROM purchase_order_data WHERE id=?1",
        [i64::try_from(id)?],
        |r| r.get(0),
    )?;
    cases.push(serde_json::json!({"case":"delete-existing-child","passed":kind=="check"&&statements==0&&version==1,
        "errorKind":kind,"violations":violations,"saveStatements":statements,"childVersion":version}));
    pool.lock()
        .unwrap()
        .trace_v2(
            rusqlite::trace::TraceEventCodes::empty(),
            None::<fn(rusqlite::trace::TraceEvent<'_>)>,
        )
        .unwrap();
    let passed = cases.iter().all(|c| c["passed"] == true);
    println!(
        "RUST_CANCELLED_CONTEXT_ROOT={}",
        serde_json::json!({"passed":passed,"cases":cases,"initialChildren":initial_children})
    );
    assert!(passed, "cancelled context-bound graph observations failed");
    Ok(())
}
