//! Application-owned regression against an unmodified locally generated library.
//! Build wiring is provided by conformance's source-only native gate.
use checker_fix_advanced_core::{AuditedSave, E, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use std::sync::{Arc, Mutex};
use teaql_core::{Entity, Value};
use teaql_runtime::FixedBusinessClock;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let first = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .unwrap();
    let second = first + chrono::Duration::hours(25);
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open_in_memory()?));
    let mut context = service_runtime_from_pool(pool)
        .await?
        .with_active_root("Tenant", 1);
    context.set_business_clock(FixedBusinessClock::new(first));
    context.ensure_schema().await?;

    let mut order = Q::purchase_orders()
        .comment("Prepare the synthetic clock fixture")
        .purpose("Verify generated audited saves and expression diagnostics")
        .new_entity(&context);
    order.update_contact_phone("5550100");
    order.update_internal_note(Value::Null);
    let saved = order
        .audit_as("Create the synthetic Fix order")
        .save(&context)
        .await?;
    let id = saved.id();
    assert_eq!(
        E::purchase_order(&saved)
            .get_create_time()
            .eval()
            .unwrap()
            .as_millis(),
        first.timestamp_millis()
    );
    assert_eq!(
        E::purchase_order(&saved)
            .get_update_time()
            .eval()
            .unwrap()
            .as_millis(),
        first.timestamp_millis()
    );
    assert_eq!(E::purchase_order(&saved).get_tenant_id().eval(), Some(1));
    assert!(
        E::purchase_order(&saved)
            .get_internal_note()
            .eval()
            .is_none()
    );

    let partials = Q::purchase_orders_minimal()
        .with_id_is(id)
        .limit(1)
        .comment("Load only the identity projection")
        .purpose("Verify complete E diagnostics without implicit loading")
        .execute_for_list(&context)
        .await?;
    let partial = partials.first().expect("saved order must exist");
    let error = E::purchase_order(partial)
        .get_tenant()
        .get_name()
        .try_eval()
        .unwrap_err();
    let wire = error.to_wire();
    assert_eq!(wire.instance_path, "/tenant");
    assert_eq!(wire.attempted_instance_path, "/tenant/name");
    assert_eq!(
        wire.expression_with_broken_point.as_deref(),
        Some("E::purchase_order(<root>).get_tenant()<broken>.get_name().try_eval()")
    );
    assert_eq!(
        wire.expression_with_broken_point
            .as_ref()
            .unwrap()
            .matches("<broken>")
            .count(),
        1
    );
    let repeated_error = E::purchase_order(partial)
        .get_tenant()
        .get_purchase_order_list()
        .first()
        .get_tenant()
        .get_name()
        .try_eval()
        .unwrap_err()
        .to_wire();
    assert_eq!(repeated_error.instance_path, "/tenant");
    assert_eq!(
        repeated_error.attempted_instance_path,
        "/tenant/purchaseOrderList/0/tenant/name"
    );
    assert_eq!(
        repeated_error.expression_with_broken_point.as_deref(),
        Some(
            "E::purchase_order(<root>).get_tenant()<broken>.get_purchase_order_list().first().get_tenant().get_name().try_eval()"
        )
    );
    let list_error = E::purchase_order(partial)
        .get_order_item_list()
        .get(2)
        .get_product_name()
        .try_eval()
        .unwrap_err()
        .to_wire();
    assert_eq!(list_error.instance_path, "/orderItemList");
    assert_eq!(
        list_error.attempted_instance_path,
        "/orderItemList/2/productName"
    );
    assert!(
        list_error
            .original_expression
            .as_ref()
            .unwrap()
            .contains(".get(2).get_product_name().try_eval()")
    );
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        E::purchase_order(partial)
            .get_contact_phone()
            .or_if_null("PRIVATE-FALLBACK".to_owned())
    }))
    .expect_err("NotLoaded must not return a fallback");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("get_contact_phone()<broken>.or_if_null(<fallback>)"));
    assert!(!message.contains("PRIVATE-FALLBACK"));

    context.set_business_clock(FixedBusinessClock::new(second));
    let rows = Q::purchase_orders()
        .with_id_is(id)
        .limit(1)
        .comment("Reload the complete order before changing it")
        .purpose("Verify update Fix preserves the original create time")
        .execute_for_list(&context)
        .await?;
    let mut loaded = rows.first().expect("saved order must exist").clone();
    loaded.update_contact_phone("5550101");
    loaded
        .audit_as("Update the synthetic Fix order")
        .save(&context)
        .await?;
    let rows = Q::purchase_orders()
        .with_id_is(id)
        .limit(1)
        .comment("Verify the persisted update")
        .purpose("Check Business Clock create and update values using E")
        .execute_for_list(&context)
        .await?;
    let persisted = rows.first().unwrap();
    assert_eq!(
        E::purchase_order(persisted)
            .get_create_time()
            .eval()
            .unwrap()
            .as_millis(),
        first.timestamp_millis()
    );
    assert_eq!(
        E::purchase_order(persisted)
            .get_update_time()
            .eval()
            .unwrap()
            .as_millis(),
        second.timestamp_millis()
    );
    assert_eq!(
        E::purchase_order(persisted)
            .get_contact_phone()
            .eval()
            .as_deref(),
        Some("5550101")
    );
    println!(
        "FIX_E_EXAMPLE={}",
        serde_json::json!({"language":"rust", "passed":true,
        "normalGeneratedSave":true, "createTimePreserved":true, "updateTimeAdvanced":true,
        "declaredContextRootInjected":true, "diagnostics":[wire,repeated_error,list_error]})
    );
    Ok(())
}
