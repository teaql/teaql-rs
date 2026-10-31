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
use teaql_runtime::{FixedBusinessClock, JsonFieldNamingProfile, RuntimeError, UserContext};

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
        .with_id_is(1)
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
    pool.lock().unwrap().trace(Some(observe));
    let mut parent = Q::purchase_orders()
        .comment("Prepare the complete parent control")
        .purpose("Exercise partial graph traversal against real SQLite")
        .new_entity(&context);
    parent.update_contact_phone("5550600");
    parent.update_internal_note(Value::Null);
    let parent = parent
        .audit_as("Create a parent for the partial graph fixture")
        .save(&context)
        .await?;
    let mut item = Q::order_items()
        .comment("Prepare the complete leaf control")
        .purpose("Persist the fixture only through generated audited saves")
        .new_entity(&context);
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
