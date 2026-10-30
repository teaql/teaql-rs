//! Stable business time must survive sparse save deltas; no-op saves stay no-op.
use checker_fix_advanced_core::{AuditedSave, E, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, Value};
use teaql_runtime::FixedBusinessClock;

static SQL: AtomicUsize = AtomicUsize::new(0);
fn observe(_: &str) {
    SQL.fetch_add(1, Ordering::SeqCst);
}
fn statements() -> usize {
    SQL.load(Ordering::SeqCst)
}

pub async fn verify() -> Result<(), Box<dyn std::error::Error>> {
    let connection = match std::env::var("TEAQL_FIX_E_DB") {
        Ok(path) => rusqlite::Connection::open(format!("{path}.clock-updates.sqlite"))?,
        Err(_) => rusqlite::Connection::open_in_memory()?,
    };
    let pool = Arc::new(Mutex::new(connection));
    let first = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .unwrap();
    let later = first + chrono::Duration::hours(1);
    let mut context = service_runtime_from_pool(pool.clone())
        .await?
        .with_active_root("Tenant", 1);
    context.set_business_clock(FixedBusinessClock::new(first));
    context.ensure_schema().await?;
    pool.lock().unwrap().trace(Some(observe));
    let mut owner = Q::purchase_orders()
        .comment("Prepare a stable-clock owner")
        .purpose("Verify managed time without manufacturing mutation intent")
        .new_entity(&context);
    owner.update_contact_phone("5550100");
    owner.update_internal_note(Value::Null);
    let before = statements();
    let mut owner = owner
        .audit_as("Create the clock regression owner")
        .save(&context)
        .await?;
    let create_sql = statements() - before;
    let mut item = Q::order_items()
        .comment("Prepare a stable-clock child")
        .purpose("Exercise valid updates under the same frozen business time")
        .new_entity(&context);
    item.update_purchase_order_id(owner.id());
    item.update_product_name("A");
    item.update_unit_price(Value::Decimal("1.23".parse()?));
    let mut item = item
        .audit_as("Create the valid clock regression item")
        .save(&context)
        .await?;

    owner.update_contact_phone("5550101");
    let before = statements();
    let mut owner = owner
        .audit_as("Update the owner without advancing time")
        .save(&context)
        .await?;
    let owner_sql = statements() - before;
    item.update_product_name("B");
    let before = statements();
    let item = item
        .audit_as("Update the item without advancing time")
        .save(&context)
        .await?;
    let item_sql = statements() - before;
    let stable = E::purchase_order(&owner).get_version().eval() == Some(2)
        && E::order_item(&item).get_version().eval() == Some(2)
        && E::purchase_order(&owner)
            .get_update_time()
            .eval()
            .unwrap()
            .as_millis()
            == first.timestamp_millis()
        && E::order_item(&item)
            .get_update_time()
            .eval()
            .unwrap()
            .as_millis()
            == first.timestamp_millis();

    let before = statements();
    owner = owner
        .audit_as("Save the unchanged owner without writing")
        .save(&context)
        .await?;
    let noop_owner_sql = statements() - before;
    owner.update_contact_phone("temporary");
    owner.update_contact_phone("5550101");
    let before = statements();
    let owner = owner
        .audit_as("Restore the original owner value without writing")
        .save(&context)
        .await?;
    let restored_sql = statements() - before;
    let before = statements();
    let item = item
        .audit_as("Save the unchanged child without writing")
        .save(&context)
        .await?;
    let noop_item_sql = statements() - before;
    let unchanged = E::purchase_order(&owner).get_version().eval() == Some(2)
        && E::order_item(&item).get_version().eval() == Some(2);

    context.set_business_clock(FixedBusinessClock::new(later));
    let mut owner = owner;
    owner.update_contact_phone("5550102");
    let before = statements();
    let owner = owner
        .audit_as("Observe the next injected business instant")
        .save(&context)
        .await?;
    let advanced_sql = statements() - before;
    let owners = Q::purchase_orders()
        .with_id_is(owner.id())
        .limit(1)
        .comment("Read the final owner clock fixture")
        .purpose("Verify real persisted values using generated Q and E")
        .execute_for_list(&context)
        .await?;
    let items = Q::order_items()
        .with_id_is(item.id())
        .limit(1)
        .comment("Read the unchanged child clock fixture")
        .purpose("Check independent child versions and timestamps")
        .execute_for_list(&context)
        .await?;
    let owner = owners.first().unwrap();
    let item = items.first().unwrap();
    let advanced = E::purchase_order(owner)
        .get_update_time()
        .eval()
        .unwrap()
        .as_millis()
        == later.timestamp_millis()
        && E::purchase_order(owner)
            .get_create_time()
            .eval()
            .unwrap()
            .as_millis()
            == first.timestamp_millis()
        && E::purchase_order(owner).get_version().eval() == Some(3)
        && E::order_item(item).get_version().eval() == Some(2)
        && E::order_item(item)
            .get_update_time()
            .eval()
            .unwrap()
            .as_millis()
            == first.timestamp_millis();
    let passed = stable
        && unchanged
        && advanced
        && create_sql > 0
        && owner_sql > 0
        && item_sql > 0
        && advanced_sql > 0
        && noop_owner_sql == 0
        && restored_sql == 0
        && noop_item_sql == 0;
    println!(
        "RUST_GENERATED_CLOCK_UPDATES={}",
        serde_json::json!({"passed":passed,
        "ownerId":owner.id(),"childId":item.id(),"firstMillis":first.timestamp_millis(),"laterMillis":later.timestamp_millis(),
        "stableUpdatePassed":stable,"unchangedVersionsPassed":unchanged,"advancedReadbackPassed":advanced,
        "createSQLStatements":create_sql,"ownerUpdateSQLStatements":owner_sql,"childUpdateSQLStatements":item_sql,
        "unchangedOwnerSQLStatements":noop_owner_sql,"restoredOwnerSQLStatements":restored_sql,
        "unchangedChildSQLStatements":noop_item_sql,"advancedUpdateSQLStatements":advanced_sql})
    );
    pool.lock().unwrap().trace(None);
    assert!(
        passed,
        "business time and no-op mutation boundaries must remain consistent"
    );
    Ok(())
}
