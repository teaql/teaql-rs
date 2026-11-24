//! Explicit graph composition below untouched sparse ancestors; generated Q/E/save only.
use checker_fix_advanced_core::{
    AuditedSave, E, LedgerEntity, Q, Tenant, generated_json_field_naming_profile,
    service_runtime_from_pool,
};
use chrono::{TimeZone, Utc};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, Value};
use teaql_runtime::{
    AtomicCounterIdGenerator, FixedBusinessClock, InternalIdGenerator, JsonFieldNamingProfile,
    MutationDecision, MutationPlan, MutationPolicy, MutationPolicyIdentity, RuntimeError,
    SafeAuditEvent, SafeAuditEventSink, UserContext,
};

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);
fn observe(_: &str) {
    STATEMENTS.fetch_add(1, Ordering::SeqCst);
}
fn count() -> usize {
    STATEMENTS.load(Ordering::SeqCst)
}
async fn graph(
    context: &UserContext,
    order_id: u64,
    sparse_leaf: bool,
) -> Result<Tenant, Box<dyn std::error::Error>> {
    let leaf = if sparse_leaf {
        Q::order_items_minimal()
    } else {
        Q::order_items()
    };
    let mut rows = Q::tenants_minimal()
        .with_id_is(1_u64)
        .select_purchase_order_list_with(
            Q::purchase_orders_minimal()
                .with_id_is(order_id)
                .select_order_item_list_with(
                    leaf.order_by_id_asc()
                        .limit(10)
                        .comment("Load explicitly selected child records"),
                )
                .order_by_id_asc()
                .limit(10)
                .comment("Load the parent identity and its child list"),
        )
        .limit(1)
        .comment("Load two sparse ancestors above an explicitly selected leaf")
        .purpose("Only the modified entity must be complete before saving the graph")
        .execute_for_list(context)
        .await?;
    Ok(rows.data.pop().ok_or("The selected root must exist")?)
}

pub async fn verify() -> Result<(), Box<dyn std::error::Error>> {
    let connection = match std::env::var("TEAQL_FIX_E_DB") {
        Ok(file) => rusqlite::Connection::open(format!("{file}.partial-ancestors.sqlite"))?,
        Err(_) => rusqlite::Connection::open_in_memory()?,
    };
    let pool = Arc::new(Mutex::new(connection));
    let first = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .unwrap();
    let later = first + chrono::Duration::days(1);
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    context.set_business_clock(FixedBusinessClock::new(first));
    context.ensure_schema().await?;
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
    let mut parent = Q::purchase_orders()
        .comment("Prepare the complete parent control")
        .purpose("Exercise partial graph traversal against real SQLite")
        .new_entity(&context)
        .expect("reserve example identity");
    parent.update_contact_phone("5550600");
    parent.update_internal_note(Value::Null);
    let parent = parent
        .audit_as("Create a parent for the partial graph fixture")
        .save(&context)
        .await?;
    let mut item = Q::order_items()
        .comment("Prepare the complete leaf control")
        .purpose("Persist the fixture only through generated audited saves")
        .new_entity(&context)
        .expect("reserve example identity");
    item.update_purchase_order_id(parent.id());
    item.update_product_name("Partial graph original");
    item.update_unit_price(Value::Decimal("10.00".parse()?));
    let item = item
        .audit_as("Create the complete partial-ancestor fixture")
        .save(&context)
        .await?;
    assert_eq!(E::purchase_order(&parent).get_version().eval(), Some(1));
    assert_eq!(E::order_item(&item).get_version().eval(), Some(1));

    let root = graph(&context, parent.id(), false).await?;
    let owner = E::tenant(&root)
        .get_purchase_order_list()
        .first()
        .eval()
        .unwrap();
    let leaf = E::purchase_order(owner)
        .get_order_item_list()
        .first()
        .eval()
        .unwrap();
    assert_eq!(leaf.id(), item.id());
    let before = count();
    let root_state = root.loaded_state_snapshot().unwrap();
    let parent_state = owner.loaded_state_snapshot().unwrap();
    assert!(E::tenant(&root).get_name().try_eval().is_err());
    assert!(
        E::purchase_order(owner)
            .get_contact_phone()
            .try_eval()
            .is_err()
    );
    let json: serde_json::Value = serde_json::from_str(
        &context.encode_entity_json(&root, generated_json_field_naming_profile())?,
    )?;
    assert!(json.get("name").is_none());
    assert!(json["purchaseOrderList"][0].get("contactPhone").is_none());
    assert_eq!(count(), before);
    assert!(Arc::ptr_eq(
        &root_state,
        &root.loaded_state_snapshot().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &parent_state,
        &owner.loaded_state_snapshot().unwrap()
    ));
    let guard_statements = count() - before;
    let mut cases = vec!["partial-ancestor-guards-and-JSON-no-I/O"];

    // A RelationHandle is immutable. Take an owned edit view and explicitly
    // include only its intent; never mutate the shared immutable relation list.
    let mut leaf = leaf.clone();
    let created = E::order_item(&leaf).get_create_time().eval().unwrap();
    leaf.update_product_name("Partial graph updated");
    root.include_pending_mutations_from(&leaf)?;
    context.set_business_clock(FixedBusinessClock::new(later));
    let before = count();
    let root = root
        .audit_as("Save a complete descendant through untouched sparse ancestors")
        .save(&context)
        .await?;
    let save_statements = count() - before;
    assert!(
        save_statements > 0,
        "The actual provider observer must be calibrated"
    );
    // The successful save returns an authoritative root readback. Changed
    // descendants invalidate its old relation view rather than expose stale data.
    let before = count();
    assert_eq!(
        E::tenant(&root).get_name().try_eval()?.as_deref(),
        Some("Tenant Name")
    );
    assert!(
        E::tenant(&root)
            .get_purchase_order_list()
            .try_eval()
            .is_err()
    );
    assert_eq!(E::tenant(&root).get_version().eval(), Some(1));
    assert_eq!(count(), before);
    let parents = Q::purchase_orders()
        .with_id_is(parent.id())
        .limit(1)
        .comment("Read back the unchanged parent")
        .purpose("Distinguish authoritative readback from mutation of ancestors")
        .execute_for_list(&context)
        .await?;
    let unchanged_parent = parents.first().unwrap();
    assert_eq!(
        E::purchase_order(unchanged_parent).get_version().eval(),
        Some(1)
    );
    assert_eq!(
        E::purchase_order(unchanged_parent)
            .get_contact_phone()
            .eval()
            .as_deref(),
        Some("5550600")
    );
    assert_eq!(
        E::purchase_order(unchanged_parent)
            .get_create_time()
            .eval()
            .unwrap()
            .as_millis(),
        first.timestamp_millis()
    );
    assert_eq!(
        E::purchase_order(unchanged_parent)
            .get_update_time()
            .eval()
            .unwrap()
            .as_millis(),
        first.timestamp_millis()
    );
    let stored = Q::order_items()
        .with_id_is(item.id())
        .limit(1)
        .comment("Read back the modified complete leaf")
        .purpose("Verify actual graph persistence")
        .execute_for_list(&context)
        .await?;
    let stored = stored.first().unwrap();
    assert_eq!(
        E::order_item(stored).get_product_name().eval().as_deref(),
        Some("Partial graph updated")
    );
    assert_eq!(E::order_item(stored).get_version().eval(), Some(2));
    assert_eq!(
        E::order_item(stored).get_create_time().eval(),
        Some(created)
    );
    assert_eq!(
        E::order_item(stored)
            .get_update_time()
            .eval()
            .unwrap()
            .as_millis(),
        later.timestamp_millis()
    );
    cases.push("complete-leaf-saved-through-two-untouched-partial-ancestors");

    let root = graph(&context, parent.id(), true).await?;
    let owner = E::tenant(&root)
        .get_purchase_order_list()
        .first()
        .eval()
        .unwrap();
    let mut sparse = E::purchase_order(owner)
        .get_order_item_list()
        .first()
        .eval()
        .unwrap()
        .clone();
    sparse.update_product_name("Rejected partial leaf");
    root.include_pending_mutations_from(&sparse)?;
    let state = root.entity_runtime_state().unwrap();
    let intent = state.current_change_set();
    assert!(!intent.changes().is_empty());
    let mut rejections = Vec::new();
    let mut rejection_statements = Vec::new();
    for _ in 0..2 {
        let before = count();
        // Save consumes its input. The retained view shares this operation's
        // ledger so both failure attempts can prove that intent survives.
        let error = root
            .clone()
            .audit_as("Reject an incomplete modified descendant")
            .save(&context)
            .await
            .expect_err("An incomplete modified leaf must be rejected");
        rejection_statements.push(count() - before);
        assert_eq!(count(), before, "Reject before any provider statements");
        assert_eq!(state.current_change_set(), intent);
        assert_eq!(
            E::order_item(&sparse).get_product_name().eval().as_deref(),
            Some("Rejected partial leaf")
        );
        assert_eq!(E::order_item(&sparse).get_version().eval(), Some(2));
        let RuntimeError::Check(violations) = error else {
            return Err(error.into());
        };
        assert!(!violations.is_empty());
        for violation in &violations {
            assert_eq!(violation.entity_type.as_deref(), Some("OrderItem"));
            assert_eq!(
                violation.location.instance_path(),
                "/purchaseOrderList/0/orderItemList/0/tenant"
            );
        }
        rejections.push(
            violations
                .iter()
                .map(|v| v.to_wire(JsonFieldNamingProfile::CamelCase))
                .collect::<Vec<_>>(),
        );
    }
    let stored = Q::order_items()
        .with_id_is(item.id())
        .limit(1)
        .comment("Read after both rejected save attempts")
        .purpose("Verify no partial writes or version bump")
        .execute_for_list(&context)
        .await?;
    assert_eq!(
        E::order_item(stored.first().unwrap())
            .get_product_name()
            .eval()
            .as_deref(),
        Some("Partial graph updated")
    );
    assert_eq!(
        E::order_item(stored.first().unwrap()).get_version().eval(),
        Some(2)
    );
    cases.push("partial-leaf-rejected-twice-before-provider-with-intent-retained");
    println!(
        "RUST_PARTIAL_ANCESTORS_RESULT={}",
        serde_json::json!({
            "passed":true,"cases":cases,"orderId":parent.id(),"leafId":item.id(),
            "ancestorVersions":[1,1],"leafVersion":2,"guardStatements":guard_statements,
            "saveStatements":save_statements,"rejectionStatements":rejection_statements,
            "rejections":rejections,"intentRetained":true,"generatedImplementationsModified":false,
            "returnedRootAuthoritativelyReadBack":true,"returnedReverseRelationInvalidated":true
        })
    );
    Ok(())
}

#[derive(Clone, Default)]
struct ManagedEvidence {
    ids: Arc<AtomicUsize>,
    policy_lookups: Arc<AtomicUsize>,
    reviews: Arc<AtomicUsize>,
    audits: Arc<AtomicUsize>,
}
impl SafeAuditEventSink for ManagedEvidence {
    fn on_safe_event(&self, _: &UserContext, _: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.audits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct ManagedPolicy(ManagedEvidence);
impl MutationPolicy for ManagedPolicy {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("partial-ancestor-root-probe", "1", "save-admission")
    }
    fn review(&self, _: &UserContext, _: &MutationPlan) -> MutationDecision {
        self.0.reviews.fetch_add(1, Ordering::SeqCst);
        MutationDecision::allow()
    }
}
#[derive(Clone)]
struct ManagedIds(ManagedEvidence, Arc<AtomicCounterIdGenerator>);
impl InternalIdGenerator for ManagedIds {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        self.0.ids.fetch_add(1, Ordering::SeqCst);
        self.1.generate_id(entity)
    }
    fn ensure_floor(&self, entity: &str, floor: u64) -> Result<(), RuntimeError> {
        self.1.ensure_floor(entity, floor)
    }
}
fn managed_instrument(context: &mut UserContext, evidence: &ManagedEvidence) {
    context.set_business_clock(FixedBusinessClock::new(
        Utc.with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
            .single()
            .unwrap(),
    ));
    context.set_custom_event_sink(evidence.clone());
    let captured = evidence.clone();
    let policy: Arc<dyn MutationPolicy> = Arc::new(ManagedPolicy(evidence.clone()));
    context.set_mutation_policy_registry(move |_: &str| {
        captured.policy_lookups.fetch_add(1, Ordering::SeqCst);
        Some(policy.clone())
    });
}
fn managed_counts(evidence: &ManagedEvidence) -> [usize; 5] {
    [
        count(),
        evidence.ids.load(Ordering::SeqCst),
        evidence.policy_lookups.load(Ordering::SeqCst),
        evidence.reviews.load(Ordering::SeqCst),
        evidence.audits.load(Ordering::SeqCst),
    ]
}
fn managed_effects(before: [usize; 5], evidence: &ManagedEvidence) -> serde_json::Value {
    let now = managed_counts(evidence);
    serde_json::json!({"sqlStatements":now[0]-before[0],"idAllocations":now[1]-before[1],
        "policyLookups":now[2]-before[2],"policyReviews":now[3]-before[3],"auditEvents":now[4]-before[4]})
}
fn managed_storage(
    pool: &Arc<Mutex<rusqlite::Connection>>,
) -> Result<serde_json::Value, rusqlite::Error> {
    let connection = pool.lock().unwrap();
    let mut parents = connection
        .prepare("SELECT id,version,tenant,contact_phone FROM purchase_order_data ORDER BY id")?;
    let parents = parents
        .query_map([], |r| {
            Ok(serde_json::json!([
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?
            ]))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut children = connection.prepare(
        "SELECT id,version,tenant,purchase_order,product_name FROM order_item_data ORDER BY id",
    )?;
    let children = children
        .query_map([], |r| {
            Ok(serde_json::json!([
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, String>(4)?
            ]))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(serde_json::json!({"parents":parents,"children":children}))
}

pub async fn verify_managed_root() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(Mutex::new(match std::env::var("TEAQL_FIX_E_DB") {
        Ok(file) => rusqlite::Connection::open(format!("{file}.partial-ancestor-root.sqlite"))?,
        Err(_) => rusqlite::Connection::open_in_memory()?,
    }));
    let evidence = ManagedEvidence::default();
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    managed_instrument(&mut context, &evidence);
    context.ensure_schema().await?;
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
    let mut other = Q::tenants()
        .comment("Prepare a second real root")
        .purpose("Reject incorrect ownership rather than a missing foreign key")
        .new_entity(&context)
        .expect("reserve example identity");
    other.update_name("Independent partial root");
    let other = other
        .audit_as("Create the other partial-ancestor root")
        .save(&context)
        .await?;
    let mut wrong = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", other.id());
    managed_instrument(&mut wrong, &evidence);
    let mut parent = Q::purchase_orders()
        .comment("Prepare a complete graph parent")
        .purpose("Test managed ownership beneath two sparse ancestors")
        .new_entity(&context)
        .expect("reserve example identity");
    parent.update_contact_phone("5550987");
    parent.update_internal_note(Value::Null);
    let parent = parent
        .audit_as("Create the managed-root parent control")
        .save(&context)
        .await?;
    let mut item = Q::order_items()
        .comment("Prepare the complete graph child")
        .purpose("Exercise generated mutation and loaded expressions")
        .new_entity(&context)
        .expect("reserve example identity");
    item.update_purchase_order_id(parent.id());
    item.update_product_name("Partial root original");
    item.update_unit_price(Value::Decimal("1".parse()?));
    let item = item
        .audit_as("Create the managed-root child control")
        .save(&context)
        .await?;
    // Swap the public ID SPI only after creating real fixtures. Its positive
    // calibration cannot affect their persisted IDs or the default ID space.
    let ids = ManagedIds(
        evidence.clone(),
        Arc::new(AtomicCounterIdGenerator::new(5000)),
    );
    context.set_internal_id_generator(ids.clone());
    wrong.set_internal_id_generator(ids);
    assert_eq!(context.next_id("ObserverCalibration")?, 5001);
    let root = graph(&context, parent.id(), false).await?;
    let parent_view = E::tenant(&root)
        .get_purchase_order_list()
        .first()
        .eval()
        .unwrap();
    let mut leaf = E::purchase_order(parent_view)
        .get_order_item_list()
        .first()
        .eval()
        .unwrap()
        .clone();
    assert_eq!(leaf.id(), item.id());
    let root_state = root.loaded_state_snapshot().unwrap();
    let parent_state = parent_view.loaded_state_snapshot().unwrap();
    assert!(E::tenant(&root).get_name().try_eval().is_err());
    assert!(
        E::purchase_order(parent_view)
            .get_contact_phone()
            .try_eval()
            .is_err()
    );
    let calibration = managed_counts(&evidence);
    assert!(calibration.iter().all(|v| *v > 0));
    leaf.update_product_name("Partial root accepted retry");
    root.include_pending_mutations_from(&leaf)?;
    let state = root.entity_runtime_state().unwrap();
    let intent = state.current_change_set();
    assert!(!intent.changes().is_empty());
    let original = managed_storage(&pool)?;
    let mut denials = Vec::new();
    for attempt in 1..=2 {
        let before = managed_counts(&evidence);
        let error = root
            .clone()
            .audit_as("Reject wrong ownership below two sparse ancestors")
            .save(&wrong)
            .await
            .expect_err("The wrong root must reject before provider entry");
        let effects = managed_effects(before, &evidence);
        assert_eq!(
            effects,
            serde_json::json!({"sqlStatements":0,"idAllocations":0,"policyLookups":0,"policyReviews":0,"auditEvents":0})
        );
        let RuntimeError::Check(violations) = error else {
            return Err(error.into());
        };
        assert!(!violations.is_empty());
        for violation in &violations {
            assert_eq!(violation.entity_type.as_deref(), Some("OrderItem"));
            assert!(matches!(
                violation.rule,
                teaql_runtime::CheckRule::ContextRootMismatch
            ));
            assert_eq!(
                violation.message.as_deref(),
                Some(
                    "The tenant is invalid.\nLocation: the 1st purchase order list → the 1st order item list"
                )
            );
            assert_eq!(
                violation.location.instance_path(),
                "/purchaseOrderList/0/orderItemList/0/tenant"
            );
        }
        assert_eq!(managed_storage(&pool)?, original);
        assert_eq!(state.current_change_set(), intent);
        assert!(Arc::ptr_eq(
            &root_state,
            &root.loaded_state_snapshot().unwrap()
        ));
        assert!(Arc::ptr_eq(
            &parent_state,
            &parent_view.loaded_state_snapshot().unwrap()
        ));
        assert_eq!(
            E::order_item(&leaf).get_product_name().eval().as_deref(),
            Some("Partial root accepted retry")
        );
        assert_eq!(E::order_item(&leaf).get_version().eval(), Some(1));
        assert!(wrong.last_fix_evidence().is_empty());
        denials.push(serde_json::json!({"attempt":attempt,"effects":effects,"pendingIntentRetained":true,
            "storageUnchanged":true,"sparseStatesUnchanged":true,"fixEvidenceAbsent":true,
            "violations":violations.iter().map(|v|v.to_wire(JsonFieldNamingProfile::CamelCase)).collect::<Vec<_>>()}));
    }
    let before = managed_counts(&evidence);
    let accepted = root
        .clone()
        .audit_as("Retry the same graph with its correct root")
        .save(&context)
        .await?;
    let effects = managed_effects(before, &evidence);
    assert!(effects["sqlStatements"].as_u64().unwrap() > 0);
    assert!(effects["policyReviews"].as_u64().unwrap() > 0);
    assert_eq!(effects["auditEvents"], 1);
    assert!(state.current_change_set().changes().is_empty());
    assert_eq!(E::tenant(&accepted).get_version().eval(), Some(1));
    assert_eq!(
        E::tenant(&accepted).get_name().eval().as_deref(),
        Some("Tenant Name")
    );
    assert!(
        E::tenant(&accepted)
            .get_purchase_order_list()
            .try_eval()
            .is_err()
    );
    let stored = Q::order_items()
        .with_id_is(item.id())
        .limit(1)
        .comment("Read the authorized graph update")
        .purpose("Verify actual child persistence after root rejection and retry")
        .execute_for_list(&context)
        .await?;
    let stored = stored.first().ok_or("The persisted child is missing")?;
    assert_eq!(
        E::order_item(stored).get_product_name().eval().as_deref(),
        Some("Partial root accepted retry")
    );
    assert_eq!(E::order_item(stored).get_version().eval(), Some(2));
    let rows = managed_storage(&pool)?;
    assert_eq!(rows["parents"], original["parents"]);
    pool.lock()
        .unwrap()
        .trace_v2(
            rusqlite::trace::TraceEventCodes::empty(),
            None::<fn(rusqlite::trace::TraceEvent<'_>)>,
        )
        .unwrap();
    println!(
        "RUST_PARTIAL_ANCESTOR_ROOT_RESULT={}",
        serde_json::json!({"passed":true,"orderId":parent.id(),"leafId":item.id(),
        "otherRootId":other.id(),"denials":denials,"acceptedEffects":effects,"ancestorVersions":[1,1],"leafVersion":2,
        "positiveCalibration":true,"sameGraphRetry":true,"returnedRootAuthoritativelyReadBack":true,"returnedReverseRelationInvalidated":true,
        "generatedImplementationsModified":false})
    );
    Ok(())
}
