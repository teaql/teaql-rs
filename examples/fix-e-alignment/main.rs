//! Application-owned regression against an unmodified locally generated library.
//! Run `node examples/fix-e-alignment/verify-local.cjs` from this repository.
use checker_fix_advanced_core::{AuditedSave, E, Q, service_runtime_from_pool};
use chrono::{TimeZone, Utc};
use std::sync::{Arc, Mutex};
use teaql_core::{Entity, Value};
use teaql_runtime::FixedBusinessClock;
mod cancelled_root_probe;
mod checker_pipeline_probe;
mod checker_range_probe;
mod checker_type_probe;
mod checker_wire_probe;
mod clock_update_probe;
mod context_root_probe;
mod concurrent_query_probe;
mod loading_probe;
mod mixed_graph_probe;
mod nonroot_completeness_probe;
mod ownership_probe;
mod paged_ownership;
mod partial_ancestor_probe;
mod tombstone_probe;
mod version_probe;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let first = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .unwrap();
    let second = first + chrono::Duration::hours(25);
    let connection = match std::env::var("TEAQL_FIX_E_DB") {
        Ok(path) => rusqlite::Connection::open(path)?,
        Err(_) => rusqlite::Connection::open_in_memory()?,
    };
    let pool = Arc::new(Mutex::new(connection));
    let mut context = service_runtime_from_pool(pool.clone())
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
    let mut human_messages = vec![error.to_string()];
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
        .unwrap_err();
    human_messages.push(repeated_error.to_string());
    let repeated_error = repeated_error.to_wire();
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
        .unwrap_err();
    human_messages.push(list_error.to_string());
    let list_error = list_error.to_wire();
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
    assert!(
        !message.contains("diagnostic:"),
        "panic is the human view, not a JSON envelope"
    );
    assert!(
        !message.contains("\"instancePath\""),
        "structured diagnostics remain on try_eval/to_wire"
    );
    assert_eq!(
        message.matches("<broken>").count(),
        1,
        "human panic has one positional breakpoint"
    );
    let panic_human = message.to_owned();

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
    // Exact methods come from current purchase_order.order_item_list Assist.
    let aggregates = Q::purchase_orders_minimal()
        .with_id_is(id)
        .limit(1)
        .sum_unit_price_of_order_items_as(
            "emptyItemSum",
            Q::order_items_minimal()
                .limit(10)
                .comment("sum order item prices"),
        )
        .count_order_items_as("emptyItemCount")
        .comment("load order with its selected total")
        .purpose("verify empty SUM through generated Q and E")
        .execute_for_list(&context)
        .await?;
    let aggregate = aggregates.first().expect("aggregate owner remains visible");
    assert_eq!(E::purchase_order(aggregate).get_id().eval(), Some(id));
    let json = aggregate.clone().into_json();
    assert!(json.as_object().unwrap().contains_key("emptyItemSum"));
    assert!(json["emptyItemSum"].is_null());
    assert_eq!(json["emptyItemCount"], serde_json::json!(0));
    println!("GENERATED_EMPTY_SUM_PASS language=rust present=true value=null");
    loading_probe::verify(&context, id).await?;
    ownership_probe::verify(&context).await?;
    tombstone_probe::verify(&mut context, pool.clone()).await?;
    version_probe::verify(&mut context, pool).await?;
    checker_type_probe::verify().await?;
    checker_range_probe::verify().await?;
    clock_update_probe::verify().await?;
    context_root_probe::verify().await?;
    cancelled_root_probe::verify().await?;
    partial_ancestor_probe::verify().await?;
    partial_ancestor_probe::verify_managed_root().await?;
    checker_pipeline_probe::verify().await?;
    nonroot_completeness_probe::verify().await?;
    checker_wire_probe::verify().await?;
    mixed_graph_probe::verify().await?;
    concurrent_query_probe::verify().await?;
    println!(
        "FIX_E_EXAMPLE={}",
        serde_json::json!({"language":"rust", "passed":true,
        "normalGeneratedSave":true, "createTimePreserved":true, "updateTimeAdvanced":true,
        "declaredContextRootInjected":true, "diagnostics":[wire,repeated_error,list_error],
        "humanMessages":human_messages,"panicHuman":panic_human})
    );
    Ok(())
}
