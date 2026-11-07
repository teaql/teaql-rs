//! TC-REQ-06: generated nested Q/E on two real threads, one Context and SQLite.
//! Policy only pauses requests. No request, trace node or SQL result is fabricated.
use checker_fix_advanced_core::{AuditedSave, E, PurchaseOrder, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use serde_json::{Value as Json, json};
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use teaql_core::{Entity, QueryRequestScope, SelectQuery, TraceKind, TraceNode, Value};
use teaql_runtime::{
    FixedBusinessClock, RequestPolicy, RuntimeError, SafeAuditEvent, SafeAuditEventSink,
    SqlLogOperation, UserContext,
};

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);
fn observe(_: &str) {
    STATEMENTS.fetch_add(1, Ordering::SeqCst);
}
fn count() -> usize {
    STATEMENTS.load(Ordering::SeqCst)
}
fn nodes(source: &[TraceNode]) -> Json {
    json!(source.iter().map(|node| json!({"kind":format!("{:?}",node.kind),"name":node.entity_type,"comment":node.comment})).collect::<Vec<_>>())
}
#[derive(Default)]
struct State {
    released: bool,
    scopes: Vec<QueryRequestScope>,
    observations: Vec<Json>,
}
#[derive(Default)]
struct Gate {
    state: Mutex<State>,
    changed: Condvar,
    context: AtomicUsize,
    audits: AtomicUsize,
}
impl Gate {
    fn wait_for(&self, expected: usize) {
        let (state, timeout) = self
            .changed
            .wait_timeout_while(
                self.state.lock().unwrap(),
                Duration::from_secs(10),
                |state| state.scopes.len() < expected,
            )
            .unwrap();
        assert!(
            !timeout.timed_out() && state.scopes.len() >= expected,
            "bounded request overlap checkpoint"
        );
    }
    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct Policy(Arc<Gate>);
impl RequestPolicy for Policy {
    fn enforce_select(
        &self,
        context: &UserContext,
        query: &mut SelectQuery,
    ) -> Result<(), RuntimeError> {
        if query.entity != "PurchaseOrder"
            || !matches!(query.comment.as_deref(), Some("alpha root" | "beta root"))
        {
            return Ok(());
        }
        assert_eq!(
            context as *const UserContext as usize,
            self.0.context.load(Ordering::SeqCst)
        );
        let scope = query
            .request_scope()
            .expect("runtime owns the root request before policy");
        assert!(scope.parent_request().is_none());
        let mut state = self.0.state.lock().unwrap();
        state.scopes.push(scope.clone());
        state.observations.push(json!({"entity":query.entity,"comment":query.comment,"purpose":query.purpose,
            "sameContext":true,"thread":format!("{:?}",std::thread::current().id()),"request":nodes(&scope.trace_chain())}));
        self.0.changed.notify_all();
        let (state, timeout) = self
            .0
            .changed
            .wait_timeout_while(state, Duration::from_secs(10), |state| !state.released)
            .unwrap();
        assert!(
            !timeout.timed_out() && state.released,
            "controller must release both live requests"
        );
        Ok(())
    }
}
struct Audit(Arc<Gate>);
impl SafeAuditEventSink for Audit {
    fn on_safe_event(&self, _: &UserContext, _: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.0.audits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
async fn seed(context: &UserContext, label: &str) -> Result<PurchaseOrder, RuntimeError> {
    let mut parent = Q::purchase_orders()
        .comment("Prepare an isolated query owner")
        .purpose("Seed the concurrent Q example")
        .new_entity(context).expect("reserve example identity");
    parent.update_contact_phone(label);
    parent.update_internal_note(Value::Null);
    let parent = parent
        .audit_as("Create an independent query owner")
        .save(context)
        .await?;
    let mut child = Q::order_items()
        .comment("Prepare an isolated query child")
        .purpose("Seed the bounded nested Q example")
        .new_entity(context).expect("reserve example identity");
    child.update_purchase_order_id(parent.id());
    child.update_product_name(format!("{label} child"));
    child.update_unit_price(Value::Decimal("1.00".parse().unwrap()));
    child
        .audit_as("Create the query owner's child")
        .save(context)
        .await?;
    Ok(parent)
}
async fn load(context: &UserContext, id: u64, label: &str) -> Result<PurchaseOrder, RuntimeError> {
    let mut rows = Q::purchase_orders()
        .with_id_is(id)
        .select_order_item_list_with(
            Q::order_items()
                .limit(10)
                .comment(format!("{label} children")),
        )
        .limit(1)
        .comment(format!("{label} root"))
        .purpose(format!("{label} purpose"))
        .execute_for_list(context)
        .await?;
    assert_eq!(rows.len(), 1);
    Ok(rows.data.pop().unwrap())
}
fn loaded(row: &PurchaseOrder, expected: &str) {
    let before = count();
    assert_eq!(
        E::purchase_order(row).get_contact_phone().eval().as_deref(),
        Some(expected)
    );
    let child = E::purchase_order(row)
        .get_order_item_list()
        .first()
        .eval()
        .unwrap();
    assert_eq!(
        E::order_item(child).get_product_name().eval().as_deref(),
        Some(format!("{expected} child").as_str())
    );
    assert_eq!(count(), before, "loaded E cannot access the provider");
}
pub async fn verify() -> Result<(), Box<dyn std::error::Error>> {
    let database = format!(
        "{}.concurrent-query.sqlite",
        std::env::var("TEAQL_FIX_E_DB")?
    );
    let round: usize = std::env::var("TEAQL_MIXED_GRAPH_ROUND")?.parse()?;
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(database)?));
    let gate = Arc::new(Gate::default());
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1)
        .with_custom_event_sink(Audit(gate.clone()));
    context.set_business_clock(FixedBusinessClock::new(
        Utc.with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
            .single()
            .unwrap(),
    ));
    context.ensure_schema().await?;
    let alpha = seed(&context, "alpha").await?;
    let beta = seed(&context, "beta").await?;
    context.enable_all_sql_log();
    context.clear_sql_logs();
    gate.audits.store(0, Ordering::SeqCst);
    pool.lock().unwrap().trace(Some(observe));
    gate.context
        .store(&context as *const UserContext as usize, Ordering::SeqCst);
    context.set_request_policy(Policy(gate.clone()));
    let original_trace_id = context.trace_id().to_owned();
    let alpha_id = alpha.id();
    let beta_id = beta.id();
    let (left, right, checkpoints) = std::thread::scope(|threads| {
        let _release = Release(gate.clone());
        let current = &context;
        let left = threads.spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(load(current, alpha_id, "alpha"))
                .map_err(|error| error.to_string())
        });
        gate.wait_for(1);
        let right = threads.spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(load(current, beta_id, "beta"))
                .map_err(|error| error.to_string())
        });
        gate.wait_for(2);
        assert!(
            !left.is_finished() && !right.is_finished(),
            "both generated query futures are live"
        );
        assert_eq!(context.trace_id(), original_trace_id);
        let state = gate.state.lock().unwrap();
        assert_eq!(state.scopes.len(), 2);
        assert_ne!(
            state.scopes[0].root_request(),
            state.scopes[1].root_request(),
            "one Context must not share the request root"
        );
        let checkpoints = state.observations.clone();
        drop(state);
        gate.release();
        (
            left.join().expect("first worker must complete").unwrap(),
            right.join().expect("second worker must complete").unwrap(),
            checkpoints,
        )
    });
    loaded(&left, "alpha");
    loaded(&right, "beta");
    let after = load(&context, alpha.id(), "after").await?;
    loaded(&after, "alpha");
    assert_eq!(context.trace_id(), original_trace_id);
    assert_eq!(gate.audits.load(Ordering::SeqCst), 0);
    let logs = context.sql_logs();
    assert_eq!(logs.len(), 6);
    assert!(count() >= 6);
    let events = logs.iter().map(|entry| {
        assert_eq!(entry.operation, SqlLogOperation::Select);
        json!({"comment":entry.comment,"purpose":entry.purpose,"request":nodes(&entry.trace_chain),"path":nodes(&entry.trace_path),
            "operation":format!("{:?}",entry.operation),"resultCount":entry.result_count})
    }).collect::<Vec<_>>();
    let result = json!({"round":round,"overlapEntered":checkpoints.len(),"checkpoints":checkpoints,"distinctRequestRoots":true,
        "sameContext":true,"contextTraceIdentifierUnchanged":context.trace_id()==original_trace_id,"generatedQAndE":true,
        "database":"fixture.sqlite.concurrent-query.sqlite","alphaId":alpha.id(),"betaId":beta.id(),"events":events,
        "eProviderReads":0,"queryAuditEvents":gate.audits.load(Ordering::SeqCst)});
    println!("RUST_CONCURRENT_QUERY_OBSERVATION={result}");
    for label in ["alpha", "beta", "after"] {
        let own = logs
            .iter()
            .filter(|entry| entry.purpose.as_deref() == Some(format!("{label} purpose").as_str()))
            .collect::<Vec<_>>();
        assert_eq!(own.len(), 2);
        let mut roots = 0;
        let mut children = 0;
        for entry in own {
            assert_eq!(
                entry.comment.as_deref(),
                Some(format!("{label} root").as_str())
            );
            assert_eq!(entry.result_count, Some(1));
            assert!(
                entry
                    .trace_chain
                    .iter()
                    .any(|node| node.comment == format!("{label} root"))
            );
            let relations = entry
                .trace_path
                .iter()
                .filter(|node| node.kind == TraceKind::Relation)
                .count();
            assert!(relations <= 1);
            if relations == 1 {
                children += 1;
            } else {
                roots += 1;
            }
            assert_eq!(
                relations == 1,
                entry
                    .trace_chain
                    .iter()
                    .any(|node| node.comment == format!("{label} children"))
            );
            assert_eq!(entry.trace_path.last().unwrap().kind, TraceKind::Sql);
            assert_eq!(
                entry.trace_path[entry.trace_path.len() - 2].kind,
                TraceKind::Provider
            );
            let text = format!("{:?}{:?}", entry.trace_chain, entry.trace_path);
            for other in ["alpha", "beta", "after"] {
                if other != label {
                    for suffix in [" root", " children", " purpose"] {
                        assert!(!text.contains(&format!("{other}{suffix}")));
                    }
                }
            }
        }
        assert_eq!((roots, children), (1, 1));
    }
    let mut result = result;
    result["passed"] = json!(true);
    println!("RUST_CONCURRENT_QUERY={result}");
    Ok(())
}
