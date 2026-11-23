//! CHK-03: keep the context-root FK loaded and isolate one untouched missing
//! non-root field. All business writes use generated Q/E/audited-save APIs.
use checker_fix_advanced_core::{AuditedSave, E, PurchaseOrder, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use serde_json::{Value as Json, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, Value};
use teaql_runtime::{
    FixedBusinessClock, MutationDecision, MutationPlan, MutationPolicy, MutationPolicyIdentity,
    RuntimeError, SafeAuditEvent, SafeAuditEventSink, UserContext,
};

static STATEMENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());
fn observe(sql: &str) {
    STATEMENTS.lock().unwrap().push(sql.to_owned());
}
#[derive(Clone, Default)]
struct Effects {
    policies: Arc<AtomicUsize>,
    audits: Arc<AtomicUsize>,
}
impl SafeAuditEventSink for Effects {
    fn on_safe_event(&self, _: &UserContext, _: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.audits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl MutationPolicy for Effects {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("completeness-probe", "1", "controlled-save")
    }
    fn review(&self, _: &UserContext, _: &MutationPlan) -> MutationDecision {
        self.policies.fetch_add(1, Ordering::SeqCst);
        MutationDecision::allow()
    }
}
impl Effects {
    fn snapshot(&self) -> [usize; 3] {
        [
            STATEMENTS.lock().unwrap().len(),
            self.policies.load(Ordering::SeqCst),
            self.audits.load(Ordering::SeqCst),
        ]
    }
    fn delta(&self, before: [usize; 3]) -> [usize; 3] {
        let now = self.snapshot();
        [now[0] - before[0], now[1] - before[1], now[2] - before[2]]
    }
}
fn emit(round: usize, name: &str, mut facts: Json) {
    facts["round"] = json!(round);
    facts["case"] = json!(name);
    facts["passed"] = json!(true);
    println!("RUST_NONROOT_COMPLETENESS_CASE={facts}");
}
async fn full(context: &UserContext, id: u64) -> PurchaseOrder {
    Q::purchase_orders()
        .with_id_is(id)
        .limit(1)
        .comment("Reload complete owner after a rejected partial mutation")
        .purpose("Verify exact values and versions without inspecting generated source")
        .execute_for_list(context)
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone()
}
fn state(value: &PurchaseOrder) -> Json {
    json!({"tenantId":E::purchase_order(value).get_tenant_id().eval(),"version":E::purchase_order(value).get_version().eval(),
        "contact_phone":value.is_field_loaded("contact_phone"),"internal_note":value.is_field_loaded("internal_note"),
        "create_time":value.is_field_loaded("create_time"),"update_time":value.is_field_loaded("update_time")})
}
pub async fn verify() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::var("TEAQL_COMPLETENESS_DB")?;
    let round: usize = std::env::var("TEAQL_COMPLETENESS_ROUND")?.parse()?;
    let instant = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .unwrap();
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(path)?));
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    context.set_business_clock(FixedBusinessClock::new(instant));
    context.ensure_schema().await?;
    let effects = Effects::default();
    context.set_custom_event_sink(effects.clone());
    let policy: Arc<dyn MutationPolicy> = Arc::new(effects.clone());
    context.set_mutation_policy_registry(move |_: &str| Some(policy.clone()));
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
    let mut owner = Q::purchase_orders()
        .comment("Prepare a complete owner for isolated field projections")
        .purpose("Test missing non-root fields rather than the context-root guard")
        .new_entity(&context)
        .expect("reserve example identity");
    owner.update_contact_phone("5550950");
    owner.update_internal_note(Value::Null);
    let before = effects.snapshot();
    let owner = owner
        .audit_as("Create the complete field-completeness control")
        .save(&context)
        .await?;
    let calibration = effects.delta(before);
    assert!(calibration.iter().all(|count| *count > 0));
    emit(
        round,
        "calibrated-valid-create",
        json!({"id":owner.id(),"effects":calibration,"version":E::purchase_order(&owner).get_version().eval()}),
    );
    for missing in ["internal_note", "contact_phone", "create_time"] {
        let mut query = Q::purchase_orders_minimal()
            .with_id_is(owner.id())
            .limit(1)
            .select_tenant_with(
                Q::tenants_minimal()
                    .limit(1)
                    .comment("Load only root identity, not root name"),
            );
        if missing != "contact_phone" {
            query = query.select_contact_phone();
        }
        if missing != "internal_note" {
            query = query.select_internal_note();
        }
        if missing != "create_time" {
            query = query.select_create_time();
        }
        query = query.select_update_time();
        let rows = query
            .comment("Load exactly one missing non-root field")
            .purpose("Prevent context-root guard from masking field-completeness behavior")
            .execute_for_list(&context)
            .await?;
        let mut partial = rows.first().unwrap().clone();
        let before_state = state(&partial);
        assert_eq!(E::purchase_order(&partial).get_tenant_id().eval(), Some(1));
        for field in [
            "contact_phone",
            "internal_note",
            "create_time",
            "update_time",
        ] {
            assert_eq!(
                partial.is_field_loaded(field),
                field != missing,
                "unexpected projection for {field}"
            );
        }
        let missing_error = match missing {
            "internal_note" => E::purchase_order(&partial)
                .get_internal_note()
                .try_eval()
                .unwrap_err()
                .to_wire(),
            "contact_phone" => E::purchase_order(&partial)
                .get_contact_phone()
                .try_eval()
                .unwrap_err()
                .to_wire(),
            "create_time" => E::purchase_order(&partial)
                .get_create_time()
                .try_eval()
                .unwrap_err()
                .to_wire(),
            _ => unreachable!(),
        };
        if missing == "contact_phone" {
            partial.update_internal_note("Rejected partial edit");
        } else {
            partial.update_contact_phone("5550959");
        }
        assert!(
            !partial.is_field_loaded(missing),
            "the mutation must not fill the field being tested"
        );
        let before = effects.snapshot();
        let result = partial
            .audit_as("Reject mutation with one untouched non-root field NotLoaded")
            .save(&context)
            .await;
        let delta = effects.delta(before);
        let violations = match result {
            Err(RuntimeError::Check(results)) => results,
            _ => panic!("partial non-root field was not rejected by Checker: {missing}"),
        };
        assert!(
            violations
                .iter()
                .any(|v| v.rule.wire_id() == "invalid_type" && v.location.model_path() == missing)
        );
        assert_eq!(
            delta,
            [0, 0, 0],
            "partial save reached SQL, customer policy or audit"
        );
        let persisted = full(&context, owner.id()).await;
        assert_eq!(
            E::purchase_order(&persisted)
                .get_contact_phone()
                .eval()
                .as_deref(),
            Some("5550950")
        );
        assert_eq!(
            E::purchase_order(&persisted).get_internal_note().eval(),
            None
        );
        assert_eq!(E::purchase_order(&persisted).get_version().eval(), Some(1));
        emit(
            round,
            missing,
            json!({"id":owner.id(),"beforeState":before_state,"missingExpression":missing_error,
            "violations":violations.iter().map(|v| v.to_wire(teaql_runtime::JsonFieldNamingProfile::CamelCase)).collect::<Vec<_>>(),
            "effects":delta,"persistedVersion":1,"persistedPhone":"5550950","persistedNote":null}),
        );
    }
    let rows = Q::purchase_orders()
        .with_id_is(owner.id())
        .limit(1)
        .select_tenant_with(
            Q::tenants_minimal()
                .limit(1)
                .comment("Retain an intentionally partial unmodified root"),
        )
        .comment("Load a complete owner with an untouched partial relation")
        .purpose("Reject only modified incomplete entities, not every object in a graph")
        .execute_for_list(&context)
        .await?;
    let mut complete = rows.first().unwrap().clone();
    assert_eq!(E::purchase_order(&complete).get_tenant_id().eval(), Some(1));
    let relation_missing = E::purchase_order(&complete)
        .get_tenant()
        .get_name()
        .try_eval()
        .unwrap_err()
        .to_wire();
    for field in [
        "contact_phone",
        "internal_note",
        "create_time",
        "update_time",
    ] {
        assert!(complete.is_field_loaded(field));
    }
    complete.update_contact_phone(format!("555095{round}"));
    let before = effects.snapshot();
    let saved = complete
        .audit_as("Save the complete owner without modifying its partial root detail")
        .save(&context)
        .await?;
    let delta = effects.delta(before);
    assert!(delta.iter().all(|count| *count > 0));
    assert_eq!(E::purchase_order(&saved).get_version().eval(), Some(2));
    let persisted = full(&context, owner.id()).await;
    assert_eq!(E::purchase_order(&persisted).get_version().eval(), Some(2));
    assert_eq!(
        E::purchase_order(&persisted).get_contact_phone().eval(),
        Some(format!("555095{round}"))
    );
    emit(
        round,
        "untouched-partial-target-accepted",
        json!({"id":owner.id(),"effects":delta,"relationMissing":relation_missing,
        "version":2,"phone":format!("555095{round}"),"completeOwnerState":state(&persisted)}),
    );
    pool.lock()
        .unwrap()
        .trace_v2(
            rusqlite::trace::TraceEventCodes::empty(),
            None::<fn(rusqlite::trace::TraceEvent<'_>)>,
        )
        .unwrap();
    println!(
        "RUST_NONROOT_COMPLETENESS_RESULT={}",
        json!({"language":"rust","round":round,"cases":5,"passed":true,
        "rootFkLoadedInAllNegativeCases":true,"otherUnmodifiedPartialRelationAllowed":true,"publicPublication":false})
    );
    Ok(())
}
