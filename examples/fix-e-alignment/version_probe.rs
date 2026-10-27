//! Generated Q/E/Save at the exact last representable optimistic version.
use checker_fix_advanced_core::{AuditedSave, E, Q};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, Value};
use teaql_runtime::{
    EntityKey, JsonFieldNamingProfile, LedgerEntity, MutationDecision, MutationPlan,
    MutationPolicy, MutationPolicyIdentity, RuntimeError, SafeAuditEvent, SafeAuditEventSink,
    UserContext,
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
        MutationPolicyIdentity::new("version-boundary", "1", "checked-i64")
    }
    fn review(&self, _: &UserContext, _: &MutationPlan) -> MutationDecision {
        self.0.reviews.fetch_add(1, Ordering::SeqCst);
        MutationDecision::allow()
    }
}

pub async fn verify(
    context: &mut UserContext,
    pool: Arc<Mutex<rusqlite::Connection>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let evidence = Evidence::default();
    context.set_custom_event_sink(evidence.clone());
    let policy: Arc<dyn MutationPolicy> = Arc::new(Policy(evidence.clone()));
    context.set_mutation_policy_registry(move |_: &str| Some(policy.clone()));
    pool.lock().unwrap().trace(Some(observe));
    let mut cases = Vec::new();
    for (case, phone, initial) in [
        ("last-representable-update", "5550950", i64::MAX - 1),
        ("loaded-exhausted-update", "5550951", i64::MAX),
    ] {
        let mut order = Q::purchase_orders()
            .comment("Create a synthetic version boundary fixture")
            .purpose("Verify exact optimistic lifecycle limits")
            .new_entity(context);
        order.update_contact_phone(phone.to_owned());
        order.update_internal_note(Value::Null);
        let order = order
            .audit_as("Create version boundary fixture")
            .save(context)
            .await?;
        let id = order.id();
        // Test-only physical setup; all business mutation under test uses Save.
        pool.lock().unwrap().execute(
            "UPDATE purchase_order_data SET version=?1 WHERE id=?2",
            rusqlite::params![initial, i64::try_from(id)?],
        )?;
        let loaded = Q::purchase_orders()
            .with_id_is(id)
            .order_by_id_asc()
            .limit(1)
            .comment("Load the exact version boundary through generated Q")
            .purpose("Keep loaded metadata and ledger baselines authoritative")
            .execute_for_list(context)
            .await?;
        let mut order = loaded
            .first()
            .ok_or("version boundary row was not loaded")?
            .clone();
        assert_eq!(
            E::purchase_order(&order).get_version().eval(),
            Some(initial)
        );
        let mut valid_statements = None;
        if initial == i64::MAX - 1 {
            order.update_internal_note("last permitted update".to_owned());
            let before = STATEMENTS.load(Ordering::SeqCst);
            order = order
                .audit_as("Advance to the last representable live version")
                .save(context)
                .await?;
            valid_statements = Some(STATEMENTS.load(Ordering::SeqCst) - before);
            assert!(valid_statements.unwrap() > 0);
        }
        assert_eq!(
            E::purchase_order(&order).get_version().eval(),
            Some(i64::MAX)
        );
        let key = EntityKey::new_static("PurchaseOrder", id);
        order.update_internal_note("must remain pending".to_owned());
        let pending = order.entity_runtime_state().unwrap();
        let before_statements = STATEMENTS.load(Ordering::SeqCst);
        let before_reviews = evidence.reviews.load(Ordering::SeqCst);
        let before_audits = evidence.audits.load(Ordering::SeqCst);
        let error = order
            .audit_as("Reject exhausted optimistic version before any execution")
            .save(context)
            .await
            .unwrap_err();
        let RuntimeError::Check(checks) = error else {
            panic!("expected structured exhausted-version rejection, got {error}");
        };
        assert_eq!(checks.len(), 1);
        let wire = checks[0].to_wire(JsonFieldNamingProfile::CamelCase);
        assert_eq!(wire.rule_id, "version_exhausted");
        assert_eq!(wire.instance_path, "/version");
        assert_eq!(wire.entity_type.as_deref(), Some("PurchaseOrder"));
        let statements = STATEMENTS.load(Ordering::SeqCst) - before_statements;
        let reviews = evidence.reviews.load(Ordering::SeqCst) - before_reviews;
        let audits = evidence.audits.load(Ordering::SeqCst) - before_audits;
        assert_eq!((statements, reviews, audits), (0, 0, 0));
        assert_eq!(pending.get_original_version(&key), Some(i64::MAX));
        assert_eq!(
            pending
                .current_change_set()
                .changes()
                .get(&key)
                .unwrap()
                .get("internal_note"),
            Some(&Value::from("must remain pending"))
        );
        let (stored_version, stored_note): (i64, Option<String>) = pool.lock().unwrap().query_row(
            "SELECT version,internal_note FROM purchase_order_data WHERE id=?1",
            [i64::try_from(id)?],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(stored_version, i64::MAX);
        assert_eq!(
            stored_note.as_deref(),
            (initial == i64::MAX - 1).then_some("last permitted update")
        );
        cases.push(serde_json::json!({"case":case,"id":id,"version":stored_version.to_string(),"originalVersion":pending.get_original_version(&key).unwrap().to_string(),
            "storedNote":stored_note,"validUpdateStatements":valid_statements,"rejectionRule":wire.rule_id,"rejectionPath":wire.instance_path,
            "rejectionStatements":statements,"rejectionPolicyReviews":reviews,"rejectionAudits":audits,"pendingIntentPreserved":true}));
    }
    pool.lock().unwrap().trace(None);
    println!(
        "RUST_VERSION_BOUNDARIES={}",
        serde_json::json!({"language":"rust","passed":true,"cases":cases})
    );
    Ok(())
}
