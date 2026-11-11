//! Application-owned Q/E/save probe; the evaluated model and Assist are retained.
#[path = "rust-integral-generated.rs"] mod exact;
use scalar_type_conformance_service_core::{AuditedSave, E, Q, ScalarTypeProbe, service_runtime_from_pool};
use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
use teaql_core::{Entity, Value};
use teaql_runtime::{EntityKey, LedgerEntity, MutationDecision, MutationPlan, MutationPolicy, MutationPolicyIdentity, RuntimeError, SafeAuditEvent, SafeAuditEventSink, UserContext};

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);
fn observe(_: &str) { STATEMENTS.fetch_add(1, Ordering::SeqCst); }
#[derive(Clone, Default)]
struct Evidence { plans: Arc<Mutex<Vec<MutationPlan>>>, audits: Arc<Mutex<Vec<SafeAuditEvent>>> }
impl SafeAuditEventSink for Evidence {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.audits.lock().unwrap().push(event.clone()); Ok(())
    }
}
struct Policy(Evidence);
impl MutationPolicy for Policy {
    fn identity(&self) -> MutationPolicyIdentity { MutationPolicyIdentity::new("restoration-probe", "1", "known-baseline") }
    fn review(&self, _: &UserContext, plan: &MutationPlan) -> MutationDecision {
        self.0.plans.lock().unwrap().push(plan.clone()); MutationDecision::allow()
    }
}
async fn load(context: &UserContext, id: u64) -> ScalarTypeProbe {
    Q::scalar_type_probes().with_id_is(id)
        .select_type_platform_with(Q::type_platforms_minimal().select_name()).limit(1)
        .comment("load the restoration graph").purpose("verify explicit loaded relation retention")
        .execute_for_one(context).await.unwrap().unwrap()
}

#[tokio::test]
async fn generated_known_baseline_restore() {
    let round = std::env::var("TEAQL_PROBE_ROUND").unwrap();
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(std::env::var("TEAQL_REVERT_DB").unwrap()).unwrap()));
    let mut context = service_runtime_from_pool(pool.clone()).await.unwrap(); context.ensure_schema().await.unwrap();
    pool.lock().unwrap().trace(Some(observe));
    let evidence = Evidence::default(); context.set_custom_event_sink(evidence.clone());
    let policy: Arc<dyn MutationPolicy> = Arc::new(Policy(evidence.clone()));
    context.set_mutation_policy_registry(move |_: &str| Some(policy.clone()));
    let name = format!("loaded revert {round}");
    let mut created = exact::prepared(&context, &name); created.update_optional_string(Value::Null);
    let saved = created.audit_as("persist the restoration fixture").save(&context).await.unwrap(); let id = saved.id();
    let pending = exact::prepared(&context, "independent pending root");
    let pending_root = LedgerEntity::entity_runtime_state(&pending).unwrap();
    let pending_before = pending_root.current_change_set();
    let mut value = load(&context, id).await;
    value.update_required_string("intermediate"); value.update_required_string(name.clone());
    assert_eq!(E::scalar_type_probe(&value).get_required_string().eval(), Some(name.clone()));
    let before = STATEMENTS.load(Ordering::SeqCst); let plans = evidence.plans.lock().unwrap().len(); let audits = evidence.audits.lock().unwrap().len();
    let value = value.audit_as("save after restoring the known string").save(&context).await.unwrap();
    let after_save = STATEMENTS.load(Ordering::SeqCst);
    let physical: (String, i64) = pool.lock().unwrap().query_row("SELECT required_string,version FROM scalar_type_probe_data WHERE id=?1", [id as i64], |row| Ok((row.get(0)?,row.get(1)?))).unwrap();
    assert_eq!(physical.0, name); assert_eq!(physical.1, 1, "restored value must not bump optimistic version");
    assert_eq!(after_save, before, "restoration reached provider statements");
    assert_eq!(evidence.plans.lock().unwrap().len(), plans); assert_eq!(evidence.audits.lock().unwrap().len(), audits);
    assert_eq!(E::scalar_type_probe(&value).get_version().eval(), Some(1));
    assert_eq!(E::scalar_type_probe(&value).get_type_platform().get_name().eval().as_deref(), Some("Scalar Type Conformance"));

    let mut value = load(&context,id).await; value.update_optional_string("intermediate"); value.update_optional_string(Value::Null);
    let before = STATEMENTS.load(Ordering::SeqCst);
    let value = value.audit_as("save after restoring the known null").save(&context).await.unwrap();
    assert_eq!(STATEMENTS.load(Ordering::SeqCst),before); assert_eq!(E::scalar_type_probe(&value).get_version().eval(),Some(1));
    assert!(value.is_field_loaded("optional_string"));
    assert_eq!(E::scalar_type_probe(&value).get_optional_string().try_eval().unwrap(),None);

    let mut sparse = Q::scalar_type_probes_minimal().with_id_is(id).select_required_string().limit(1)
        .comment("load a clean partial restoration view").purpose("require completeness only for actual mutations").execute_for_one(&context).await.unwrap().unwrap();
    sparse.update_required_string("intermediate"); sparse.update_required_string(name.clone());
    let before = STATEMENTS.load(Ordering::SeqCst);
    let sparse = sparse.audit_as("save an unchanged partial object").save(&context).await.unwrap();
    assert_eq!(STATEMENTS.load(Ordering::SeqCst),before); assert!(E::scalar_type_probe(&sparse).get_required_long().try_eval().is_err());

    let mut bad = Q::scalar_type_probes_minimal().with_id_is(id).select_required_string().limit(1)
        .comment("load an incomplete edited view").purpose("retain the partial mutation rejection boundary").execute_for_one(&context).await.unwrap().unwrap();
    bad.update_optional_string(Value::Null);
    let before = STATEMENTS.load(Ordering::SeqCst);
    assert!(matches!(bad.audit_as("reject an incomplete explicit null edit").save(&context).await,Err(RuntimeError::Check(_))));
    assert_eq!(STATEMENTS.load(Ordering::SeqCst),before);

    let malformed = load(&context,id).await;
    LedgerEntity::entity_runtime_state(&malformed).unwrap().set(EntityKey::new_static("ScalarTypeProbe",id),"requiredString",name.clone());
    let before = STATEMENTS.load(Ordering::SeqCst); assert!(malformed.audit_as("reject a native-member ledger alias").save(&context).await.is_err());
    assert_eq!(STATEMENTS.load(Ordering::SeqCst),before);

    let mut changed = load(&context,id).await; changed.update_required_string("intermediate");changed.update_required_string(name.clone());changed.update_optional_string("real edit");
    let changed = changed.audit_as("save only the real remaining edit").save(&context).await.unwrap();
    assert_eq!(E::scalar_type_probe(&changed).get_version().eval(),Some(2));
    assert_eq!(E::scalar_type_probe(&changed).get_required_string().eval(),Some(name));
    assert_eq!(E::scalar_type_probe(&changed).get_optional_string().eval(),Some(Some("real edit".to_owned())));
    assert_eq!(evidence.plans.lock().unwrap().len(),plans+1); assert_eq!(evidence.audits.lock().unwrap().len(),audits+1);
    let plan = evidence.plans.lock().unwrap().last().unwrap().clone();assert_eq!(plan.operations.len(),1);
    assert!(plan.operations[0].changed_values.contains_key("optional_string")); assert!(!plan.operations[0].changed_values.contains_key("required_string"));
    assert_eq!(pending_root.current_change_set(),pending_before);
    let root_version: i64 = pool.lock().unwrap().query_row("SELECT version FROM type_platform_data WHERE id=1",[],|row|row.get(0)).unwrap();assert_eq!(root_version,1);
    println!("RUST_LOADED_REVERT={}",serde_json::json!({"passed":true,"round":round,"id":id,"finalVersion":2,
        "knownValueRestored":true,"knownNullRestored":true,"zeroProviderOnNoop":true,"loadedRelationRetained":true,
        "unchangedPartialAccepted":true,"modifiedPartialRejected":true,"rawAliasRejected":true,"remainingEditSaved":true,"unrelatedPendingRetained":true,
        "completeExampleInventoryAccepted":false,"internalArtifactAccepted":false}));
}
