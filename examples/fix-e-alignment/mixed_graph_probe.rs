//! ML-09/ML-11 through generated Q/E/save and explicit ledger composition.
//! SQL observes storage and installs fault-injection triggers; it never seeds
//! business data. AFTER triggers prove two writes actually became visible
//! inside the failed transaction, rather than merely counting attempted calls.
use checker_fix_advanced_core::{
    AuditedSave, E, OrderItem, PurchaseOrder, Q, service_runtime_from_pool,
};
use chrono::{TimeZone, Utc};
use rusqlite::functions::FunctionFlags;
use serde_json::{Value as Json, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use teaql_core::{Entity, Value};
use teaql_runtime::{
    EntityKey, FixedBusinessClock, LedgerEntity, MutationDecision, MutationPlan, MutationPolicy,
    MutationPolicyIdentity, RawAuditEventKind, RuntimeError, SafeAuditEvent, SafeAuditEventSink,
    UserContext,
};

#[derive(Clone, Default)]
struct Evidence {
    plans: Arc<Mutex<Vec<MutationPlan>>>,
    audits: Arc<Mutex<Vec<SafeAuditEvent>>>,
    writes: Arc<Mutex<Vec<Json>>>,
    arm: Arc<Mutex<(bool, usize)>>,
    reject_commit: Arc<AtomicBool>,
    commits: Arc<Mutex<Vec<Json>>>,
}
impl Evidence {
    fn begin(&self, fail_after: usize) {
        self.plans.lock().unwrap().clear();
        self.audits.lock().unwrap().clear();
        self.writes.lock().unwrap().clear();
        self.commits.lock().unwrap().clear();
        self.reject_commit.store(false, Ordering::SeqCst);
        *self.arm.lock().unwrap() = (true, fail_after);
    }
    fn stop(&self) {
        self.arm.lock().unwrap().0 = false;
        self.reject_commit.store(false, Ordering::SeqCst);
    }
}
impl SafeAuditEventSink for Evidence {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.audits.lock().unwrap().push(event.clone());
        Ok(())
    }
}
struct Policy(Evidence);
impl MutationPolicy for Policy {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("mixed-graph-example", "1", "create-update-delete-atomicity")
    }
    fn review(&self, _: &UserContext, plan: &MutationPlan) -> MutationDecision {
        self.0.plans.lock().unwrap().push(plan.clone());
        MutationDecision::allow()
    }
}
fn parent(context: &UserContext) -> PurchaseOrder {
    let mut value = Q::purchase_orders()
        .comment("Prepare a mixed mutation owner")
        .purpose("Verify one graph save commits create update and deletion")
        .new_entity(context).expect("reserve example identity");
    value.update_contact_phone("5550680");
    value.update_internal_note(Value::Null);
    value
}
fn child(context: &UserContext, owner: u64, name: &str) -> OrderItem {
    let mut value = Q::order_items()
        .comment("Prepare a mixed mutation child")
        .purpose("Verify explicit composition and retryable graph mutations")
        .new_entity(context).expect("reserve example identity");
    value.update_purchase_order_id(owner);
    value.update_product_name(name);
    value.update_unit_price(Value::Decimal("10.00".parse().unwrap()));
    value
}
async fn load_parent(context: &UserContext, id: u64) -> Result<PurchaseOrder, RuntimeError> {
    Ok(Q::purchase_orders()
        .with_id_is(id)
        .limit(1)
        .comment("Load the complete mixed owner")
        .purpose("Verify optimistic versions and authoritative mutation readback")
        .execute_for_list(context)
        .await?
        .first()
        .unwrap()
        .clone())
}
async fn load_child(context: &UserContext, id: u64) -> Result<OrderItem, RuntimeError> {
    Ok(Q::order_items()
        .with_id_is(id)
        .limit(1)
        .comment("Load the complete old child")
        .purpose("Verify mark for deletion joins the ordinary save boundary")
        .execute_for_list(context)
        .await?
        .first()
        .unwrap()
        .clone())
}
fn snapshot_sql(owner: u64, old: u64, new: u64) -> String {
    format!("SELECT json_object('orderVersion',(SELECT version FROM purchase_order_data WHERE id={owner}),
        'oldVersion',(SELECT version FROM order_item_data WHERE id={old}),
        'newCount',(SELECT count(*) FROM order_item_data WHERE id={new}))")
}
fn snapshot(pool: &Arc<Mutex<rusqlite::Connection>>, owner: u64, old: u64, new: u64) -> Json {
    let value: String = pool
        .lock()
        .unwrap()
        .query_row(&snapshot_sql(owner, old, new), [], |r| r.get(0))
        .unwrap();
    serde_json::from_str(&value).unwrap()
}
fn install_triggers(pool: &Arc<Mutex<rusqlite::Connection>>, owner: u64, old: u64, new: u64) {
    let state = snapshot_sql(owner, old, new);
    let trigger = |name: &str, event: &str, table: &str, condition: &str, kind: &str| {
        format!(
        "DROP TRIGGER IF EXISTS {name}; CREATE TRIGGER {name} AFTER {event} ON {table}
         WHEN {condition} BEGIN SELECT CASE WHEN core236_mixed_probe('{kind}',NEW.id,NEW.version,({state}))=1
         THEN RAISE(ABORT,'core236 mixed graph intentional second-write failure') END; END;"
    )
    };
    let sql = trigger(
        "core236_mixed_owner",
        "UPDATE",
        "purchase_order_data",
        &format!("NEW.id={owner}"),
        "Update",
    ) + &trigger(
        "core236_mixed_insert",
        "INSERT",
        "order_item_data",
        &format!("NEW.id={new}"),
        "Create",
    ) + &trigger(
        "core236_mixed_delete",
        "UPDATE",
        "order_item_data",
        &format!("NEW.id={old} AND NEW.version<0"),
        "Delete",
    );
    pool.lock().unwrap().execute_batch(&sql).unwrap();
}
fn plan(e: &Evidence, owner: u64, old: u64, new: u64) -> Vec<Json> {
    let plans = e.plans.lock().unwrap();
    assert_eq!(
        plans.len(),
        1,
        "review one complete plan, not individual commands"
    );
    let operations = plans[0].operations.iter().map(|v| json!({"kind":format!("{:?}",v.kind),
        "entity":v.entity.entity.to_string(),"id":v.entity.id.try_u64().unwrap(),"originalVersion":v.original_version})).collect::<Vec<_>>();
    let actual = operations
        .iter()
        .map(|v| {
            (
                v["kind"].as_str().unwrap().to_owned(),
                v["entity"].as_str().unwrap().to_owned(),
                v["id"].as_u64().unwrap(),
            )
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(operations.len(), 3);
    assert_eq!(
        actual,
        BTreeSet::from([
            ("Update".to_owned(), "PurchaseOrder".to_owned(), owner),
            ("Delete".to_owned(), "OrderItem".to_owned(), old),
            ("Create".to_owned(), "OrderItem".to_owned(), new),
        ])
    );
    operations
}
fn audit(e: &Evidence, owner: u64, old: u64, new: u64) -> Vec<Json> {
    let audits = e.audits.lock().unwrap();
    assert_eq!(audits.len(), 3);
    let result = audits
        .iter()
        .map(|v| {
            let kind = match v.kind {
                RawAuditEventKind::Created => "Create",
                RawAuditEventKind::Updated => "Update",
                RawAuditEventKind::Deleted => "Delete",
                _ => panic!("unexpected audit kind"),
            };
            json!({"kind":kind,"entity":v.entity,"id":v.entity_id.unwrap()})
        })
        .collect::<Vec<_>>();
    let actual = result
        .iter()
        .map(|v| {
            (
                v["kind"].as_str().unwrap().to_owned(),
                v["entity"].as_str().unwrap().to_owned(),
                v["id"].as_u64().unwrap(),
            )
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual,
        BTreeSet::from([
            ("Update".to_owned(), "PurchaseOrder".to_owned(), owner),
            ("Delete".to_owned(), "OrderItem".to_owned(), old),
            ("Create".to_owned(), "OrderItem".to_owned(), new)
        ])
    );
    result
}

pub async fn verify() -> Result<(), Box<dyn std::error::Error>> {
    let database = format!("{}.mixed-graph.sqlite", std::env::var("TEAQL_FIX_E_DB")?);
    let round: usize = std::env::var("TEAQL_MIXED_GRAPH_ROUND")?.parse()?;
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(database)?));
    let evidence = Evidence::default();
    let committing = evidence.clone();
    pool.lock().unwrap().commit_hook(Some(move || {
        if !committing.arm.lock().unwrap().0 {
            return false;
        }
        let reject = committing.reject_commit.load(Ordering::SeqCst);
        committing.commits.lock().unwrap().push(json!({
            "writes":committing.writes.lock().unwrap().len(),
            "audits":committing.audits.lock().unwrap().len(),"rejected":reject
        }));
        reject
    }))?;
    let observed = evidence.clone();
    pool.lock().unwrap().create_scalar_function("core236_mixed_probe", 4, FunctionFlags::SQLITE_UTF8, move |args| {
        let (enabled, fail_after) = *observed.arm.lock().unwrap(); if !enabled {return Ok(0i64);}
        let kind: String = args.get(0)?; let id: i64 = args.get(1)?; let version: i64 = args.get(2)?; let state: String = args.get(3)?;
        let mut writes = observed.writes.lock().unwrap();
        writes.push(json!({"kind":kind,"id":id,"version":version,"visible":serde_json::from_str::<Json>(&state).unwrap()}));
        Ok(i64::from(fail_after > 0 && writes.len() == fail_after))
    })?;
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    context.set_business_clock(FixedBusinessClock::new(
        Utc.with_ymd_and_hms(2032, 6, 7, 8, 9, 10).single().unwrap(),
    ));
    context.ensure_schema().await?;
    context.ensure_schema().await?;
    let count: i64 =
        pool.lock()
            .unwrap()
            .query_row("SELECT count(*) FROM purchase_order_data", [], |r| r.get(0))?;
    assert_eq!(
        count,
        if round == 1 { 0 } else { 3 },
        "round two reuses its unreset database"
    );
    context.set_custom_event_sink(evidence.clone());
    let policy: Arc<dyn MutationPolicy> = Arc::new(Policy(evidence.clone()));
    context.set_mutation_policy_registry(move |_: &str| Some(policy.clone()));
    for scenario in ["commit", "rollback-retry", "commit-failure-retry"] {
        let fail = scenario != "commit";
        let fail_commit = scenario == "commit-failure-retry";
        evidence.stop();
        let seeded = parent(&context)
            .audit_as("Create the independent mixed owner")
            .save(&context)
            .await?;
        let seeded_child = child(&context, seeded.id(), "old child")
            .audit_as("Create the old mixed child")
            .save(&context)
            .await?;
        let owner_id = seeded.id();
        let old_id = seeded_child.id();
        let mut owner = load_parent(&context, owner_id).await?;
        let mut old = load_child(&context, old_id).await?;
        let new = child(&context, owner_id, "new child");
        let new_id = new.id();
        assert!(new_id > 0 && new_id != old_id);
        old.mark_for_deletion();
        owner.update_contact_phone("5550681");
        // These exact methods are documented by model-aware create Assist.
        owner.include_pending_mutations_from(&old)?;
        owner.include_pending_mutations_from(&new)?;
        let pending = owner.entity_runtime_state().unwrap();
        let root_key = EntityKey::new_static("PurchaseOrder", owner_id);
        let old_key = EntityKey::new_static("OrderItem", old_id);
        let new_key = EntityKey::new_static("OrderItem", new_id);
        assert!(pending.new_keys().contains(&new_key) && pending.deleted_keys().contains(&old_key));
        let original = snapshot(&pool, owner_id, old_id, new_id);
        assert_eq!(
            original,
            json!({"orderVersion":1,"oldVersion":1,"newCount":0})
        );
        install_triggers(&pool, owner_id, old_id, new_id);
        let mut failure = Json::Null;
        if fail {
            evidence.begin(if fail_commit { 0 } else { 2 });
            evidence.reject_commit.store(fail_commit, Ordering::SeqCst);
            // Clone retains the same pending ledger for retry; composition was
            // already explicit and is not inferred from cloning or Context.
            let result = owner
                .clone()
                .audit_as("Attempt the mixed graph before injected failure")
                .save(&context)
                .await;
            let error = result
                .err()
                .expect("the armed SQLite statement or commit hook must reject this save")
                .to_string();
            if !fail_commit {
                assert!(
                    error.contains("core236 mixed graph intentional second-write failure"),
                    "{error}"
                );
            }
            let operations = plan(&evidence, owner_id, old_id, new_id);
            let writes = evidence.writes.lock().unwrap().clone();
            assert_eq!(writes.len(), if fail_commit { 3 } else { 2 });
            let commits = evidence.commits.lock().unwrap().clone();
            assert_eq!(
                commits,
                if fail_commit {
                    vec![json!({"writes":3,"audits":0,"rejected":true})]
                } else {
                    vec![]
                }
            );
            assert_ne!(writes[0]["visible"], original);
            assert_ne!(writes[1]["visible"], original);
            assert_eq!(snapshot(&pool, owner_id, old_id, new_id), original);
            assert!(
                evidence.audits.lock().unwrap().is_empty(),
                "rollback must publish no committed audit"
            );
            assert_eq!(pending.get_original_version(&root_key), Some(1));
            assert_eq!(pending.get_original_version(&old_key), Some(1));
            assert!(
                pending
                    .changed_field_names(&root_key)
                    .contains("contact_phone")
            );
            assert!(
                pending.new_keys().contains(&new_key) && pending.deleted_keys().contains(&old_key)
            );
            failure = json!({"operations":operations,"actualWritesBeforeFailure":writes,"storedAfterRollback":original,
                "stage":if fail_commit {"commit"} else {"statement"},"error":error,"commitCallbacks":commits,
                "committedAudits":0,"pendingCreate":true,"pendingDelete":true,"pendingUpdate":true,"originalVersionsRetained":true});
        }
        evidence.begin(0);
        let saved = owner
            .audit_as("Commit the mixed graph with exactly one audited save")
            .save(&context)
            .await?;
        let operations = plan(&evidence, owner_id, old_id, new_id);
        let writes = evidence.writes.lock().unwrap().clone();
        assert_eq!(writes.len(), 3);
        let audits = audit(&evidence, owner_id, old_id, new_id);
        let commits = evidence.commits.lock().unwrap().clone();
        assert_eq!(
            commits,
            vec![json!({"writes":3,"audits":0,"rejected":false})]
        );
        assert_eq!(
            snapshot(&pool, owner_id, old_id, new_id),
            json!({"orderVersion":2,"oldVersion":-2,"newCount":1})
        );
        let loaded = load_parent(&context, owner_id).await?;
        let loaded_new = load_child(&context, new_id).await?;
        assert_eq!(
            E::purchase_order(&loaded)
                .get_contact_phone()
                .eval()
                .as_deref(),
            Some("5550681")
        );
        assert_eq!(E::purchase_order(&loaded).get_version().eval(), Some(2));
        assert_eq!(
            E::order_item(&loaded_new)
                .get_product_name()
                .eval()
                .as_deref(),
            Some("new child")
        );
        assert_eq!(E::order_item(&loaded_new).get_version().eval(), Some(1));
        assert_eq!(
            E::order_item(&loaded_new).get_purchase_order_id().eval(),
            Some(owner_id)
        );
        assert!(pending.new_keys().is_empty() && pending.deleted_keys().is_empty());
        assert!(pending.changed_field_names(&root_key).is_empty());
        evidence.begin(0);
        let saved = saved
            .audit_as("Repeat an unchanged successful mixed graph")
            .save(&context)
            .await?;
        assert_eq!(saved.id(), owner_id);
        assert!(
            evidence.plans.lock().unwrap().is_empty()
                && evidence.audits.lock().unwrap().is_empty()
                && evidence.writes.lock().unwrap().is_empty()
                && evidence.commits.lock().unwrap().is_empty()
        );
        evidence.stop();
        println!(
            "RUST_MIXED_GRAPH_CASE={}",
            json!({"passed":true,"round":round,"case":scenario,
            "ownerId":owner_id,"oldChildId":old_id,"newChildId":new_id,"plan":operations,"writes":writes,"audits":audits,
            "commitCallbacks":commits,"failure":failure,"unchangedSaveWrites":0,"unchangedSavePolicyReviews":0,"unchangedSaveAudits":0})
        );
    }
    Ok(())
}
