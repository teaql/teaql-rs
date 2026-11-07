//! Context-root ownership is a save boundary, not a tenant-specific policy.
//! Business setup uses generated Q/E/audited Save; SQL only observes storage.
use checker_fix_advanced_core::{AuditedSave, E, PurchaseOrder, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use teaql_core::{Entity, Value};
use teaql_runtime::{
    EntityKey, FixedBusinessClock, LedgerEntity, MutationDecision, MutationPlan, MutationPolicy,
    MutationPolicyIdentity, RuntimeError, SafeAuditEvent, SafeAuditEventSink, UserContext,
};

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);
fn observe(_: &str) {
    STATEMENTS.fetch_add(1, Ordering::SeqCst);
}
#[derive(Clone, Default)]
struct Evidence {
    reviews: Arc<AtomicUsize>,
    audits: Arc<AtomicUsize>,
}
impl SafeAuditEventSink for Evidence {
    fn on_safe_event(&self, _: &UserContext, _: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.audits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Policy(Evidence);
impl MutationPolicy for Policy {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("context-root-probe", "1", "save-admission")
    }
    fn review(&self, _: &UserContext, _: &MutationPlan) -> MutationDecision {
        self.0.reviews.fetch_add(1, Ordering::SeqCst);
        MutationDecision::allow()
    }
}
fn instrument(context: &mut UserContext, evidence: &Evidence) {
    context.set_business_clock(FixedBusinessClock::new(
        Utc.with_ymd_and_hms(2032, 5, 6, 7, 8, 9).single().unwrap(),
    ));
    context.set_custom_event_sink(evidence.clone());
    let policy: Arc<dyn MutationPolicy> = Arc::new(Policy(evidence.clone()));
    context.set_mutation_policy_registry(move |_: &str| Some(policy.clone()));
}
fn parent(context: &UserContext) -> PurchaseOrder {
    let mut value = Q::purchase_orders()
        .comment("Prepare a context-root boundary fixture")
        .purpose("Verify original ownership before persistence or audit effects")
        .new_entity(context).expect("reserve example identity");
    value.update_contact_phone("5550188");
    value.update_internal_note(Value::Null);
    value
}
fn storage(
    pool: &Arc<Mutex<rusqlite::Connection>>,
    id: u64,
) -> Result<serde_json::Value, rusqlite::Error> {
    pool.lock().unwrap().query_row(
        "SELECT tenant, version, contact_phone, create_time, update_time FROM purchase_order_data WHERE id=?1",
        [i64::try_from(id).map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?], |row| Ok(serde_json::json!({"root":row.get::<_, Option<i64>>(0)?,
            "version":row.get::<_, i64>(1)?, "phone":row.get::<_, String>(2)?,
            "created":row.get::<_, Option<i64>>(3)?, "updated":row.get::<_, Option<i64>>(4)?})))
}
async fn rejected<T>(
    future: impl Future<Output = Result<T, RuntimeError>>,
    name: &str,
    evidence: &Evidence,
) -> serde_json::Value {
    let sql = STATEMENTS.load(Ordering::SeqCst);
    let reviews = evidence.reviews.load(Ordering::SeqCst);
    let audits = evidence.audits.load(Ordering::SeqCst);
    let (kind, error, violations) = match future.await {
        Err(RuntimeError::Check(results)) => ("check", None, results),
        Err(error) => ("other_error", Some(error.to_string()), vec![]),
        Ok(_) => ("accepted", None, vec![]),
    };
    let sql = STATEMENTS.load(Ordering::SeqCst) - sql;
    let reviews = evidence.reviews.load(Ordering::SeqCst) - reviews;
    let audits = evidence.audits.load(Ordering::SeqCst) - audits;
    let root_violation = violations.iter().any(|v| {
        v.entity_type.as_deref() == Some("PurchaseOrder")
            && v.location.instance_path() == "/tenant"
            && matches!(
                v.rule,
                teaql_runtime::CheckRule::ContextRootMissing
                    | teaql_runtime::CheckRule::ContextRootMismatch
            )
    });
    let views: Vec<_> = violations
        .iter()
        .map(|v| {
            serde_json::json!({"entity":v.entity_type,
        "path":v.location.instance_path(), "rule":v.rule.wire_id()})
        })
        .collect();
    serde_json::json!({"case":name,"errorKind":kind,"error":error,"violations":views,
        "saveStatements":sql,"policyReviews":reviews,"auditEvents":audits,
        "passed":kind=="check"&&root_violation&&sql==0&&reviews==0&&audits==0})
}

pub async fn verify() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(Mutex::new(match std::env::var("TEAQL_FIX_E_DB") {
        Ok(file) => rusqlite::Connection::open(format!("{file}.context-root.sqlite"))?,
        Err(_) => rusqlite::Connection::open_in_memory()?,
    }));
    let evidence = Evidence::default();
    let mut owner = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    instrument(&mut owner, &evidence);
    owner.ensure_schema().await?;
    // The second root really exists, so a transferred FK cannot fail merely because of an FK constraint.
    let mut other = Q::tenants()
        .comment("Prepare an independent domain root")
        .purpose("Calibrate cross-context ownership rejection")
        .new_entity(&owner);
    other.update_name("Independent root");
    let other = other
        .audit_as("Create the other real root")
        .save(&owner)
        .await?;
    let mut wrong = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", other.id());
    let mut missing = service_runtime_from_pool(pool.clone()).await?;
    let mut wrong_type = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Organization", 1);
    for context in [&mut wrong, &mut missing, &mut wrong_type] {
        instrument(context, &evidence);
    }
    pool.lock().unwrap().trace(Some(observe));
    let mut cases = Vec::new();
    let before_sql = STATEMENTS.load(Ordering::SeqCst);
    let before_reviews = evidence.reviews.load(Ordering::SeqCst);
    let before_audits = evidence.audits.load(Ordering::SeqCst);
    let good = parent(&owner)
        .audit_as("Calibrate the normal save path")
        .save(&owner)
        .await?;
    cases.push(serde_json::json!({"case":"valid-create","id":good.id(),"passed":E::purchase_order(&good).get_tenant_id().eval()==Some(1)
        &&STATEMENTS.load(Ordering::SeqCst)>before_sql&&evidence.reviews.load(Ordering::SeqCst)>before_reviews
        &&evidence.audits.load(Ordering::SeqCst)>before_audits}));
    for (name, context) in [
        ("create-missing-root", &missing),
        ("create-wrong-root-type", &wrong_type),
    ] {
        cases.push(
            rejected(
                parent(context)
                    .audit_as("Reject absent or incompatible root context")
                    .save(context),
                name,
                &evidence,
            )
            .await,
        );
    }
    for name in [
        "update-wrong-context",
        "delete-wrong-context",
        "delete-missing-root",
        "delete-wrong-root-type",
        "update-context-and-payload",
        "update-payload-only",
        "update-cleared-root",
        "delete-context-and-payload",
    ] {
        let saved = parent(&owner)
            .audit_as("Create an independently owned mutation fixture")
            .save(&owner)
            .await?;
        let rows = Q::purchase_orders()
            .with_id_is(saved.id())
            .limit(1)
            .comment("Load the authoritative owner snapshot")
            .purpose("Compare current and original root bindings")
            .execute_for_list(&owner)
            .await?;
        let mut value = rows.first().ok_or("owner fixture missing")?.clone();
        assert_eq!(E::purchase_order(&value).get_tenant_id().eval(), Some(1));
        let before = storage(&pool, value.id())?;
        let id = value.id();
        if name.contains("payload") || name == "update-cleared-root" {
            value.entity_runtime_state().unwrap().set(
                EntityKey::new_static("PurchaseOrder", id),
                "tenant",
                if name == "update-cleared-root" {
                    0u64
                } else {
                    other.id()
                },
            );
        }
        if name.starts_with("delete") {
            value.mark_for_deletion();
        } else {
            value.update_contact_phone("5550199");
        }
        let context = match name {
            "delete-missing-root" => &missing,
            "delete-wrong-root-type" => &wrong_type,
            "update-payload-only" | "update-cleared-root" => &owner,
            _ => &wrong,
        };
        let mut result = rejected(
            value
                .audit_as("Reject a mutation outside its original root")
                .save(context),
            name,
            &evidence,
        )
        .await;
        let after = storage(&pool, id)?;
        result["id"] = serde_json::json!(id);
        let unchanged = before == after;
        result["storageBefore"] = before;
        result["storageAfter"] = after;
        result["storageUnchanged"] = serde_json::json!(unchanged);
        result["passed"] = serde_json::json!(result["passed"] == true && unchanged);
        cases.push(result);
    }
    let mut good = good;
    good.update_contact_phone("5550177");
    let mut good = good
        .audit_as("Calibrate a valid same-root update")
        .save(&owner)
        .await?;
    cases.push(serde_json::json!({"case":"valid-update","id":good.id(),"passed":E::purchase_order(&good).get_version().eval()==Some(2)
        &&E::purchase_order(&good).get_contact_phone().eval().as_deref()==Some("5550177")}));
    good.mark_for_deletion();
    let good = good
        .audit_as("Calibrate a valid same-root deletion")
        .save(&owner)
        .await?;
    cases.push(serde_json::json!({"case":"valid-delete","id":good.id(),"passed":E::purchase_order(&good).get_version().eval()==Some(-3)}));
    pool.lock().unwrap().trace(None);
    let passed = cases.iter().all(|case| case["passed"] == true);
    println!(
        "RUST_CONTEXT_ROOT_BOUNDARY={}",
        serde_json::json!({"passed":passed,"cases":cases,
        "otherRootExists":true,"otherRootId":other.id(),"generatedLibraryUnmodified":true,
        "recoveryViaGraphSaveSupported":false})
    );
    assert!(
        passed,
        "context-root save boundary failed; see structured evidence"
    );
    Ok(())
}
