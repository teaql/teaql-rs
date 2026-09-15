use order_management_service_core::teaql_core::Entity as _;
use order_management_service_core::{
    request_support::AuditedSave as _, service_runtime, ServiceRuntimeConfig, E, Q,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("TEAQL_SAVE_LOAD_STATE_DATABASE")?;
    let context = service_runtime(ServiceRuntimeConfig { database_url }).await?;
    context.ensure_schema().await?;

    let order_id = Q::order_lines()
        .order_by_id_desc()
        .limit(1)
        .comment("what: find a seeded child for the forward-FK Save probe")
        .purpose("why: select a stable parent on repeated acceptance runs")
        .execute_for_one(&context)
        .await?
        .expect("nested graph probe must seed an OrderLine")
        .customer_order_id();
    let mut order = Q::customer_orders()
        .with_id_is(order_id)
        .select_customer_with(Q::customers().select_self_fields().limit(1))
        .limit(1)
        .comment("what: load a complete Order and its current Customer")
        .purpose("why: detect stale forward relation after changing the FK")
        .execute_for_one(&context)
        .await?
        .expect("seeded parent Order must exist");
    let old_customer_id = E::customer_order(&order)
        .get_customer()
        .eval()
        .expect("the relation must be loaded")
        .id();
    assert_eq!(old_customer_id, order.customer_id());

    let mut replacement = Q::customers()
        .comment("what: create a replacement Customer for the order")
        .purpose("why: exercise a real FK mutation, not a scalar-only Save")
        .new_entity(&context);
    let replacement_id = replacement.id();
    replacement
        .update_name("Forward FK replacement customer")
        .update_email(format!("forward-fk-{replacement_id}@example.test"))
        .update_commerce_platform_id(order.commerce_platform_id());
    replacement
        .audit_as("Create replacement Customer for forward-FK Save test")
        .save(&context)
        .await?;

    order.update_customer_id(replacement_id);
    let saved = order
        .audit_as("Reassign Order to replacement Customer")
        .save(&context)
        .await?;
    assert_eq!(saved.customer_id(), replacement_id);
    // An unchanged loaded relation may be retained, but a changed FK must not
    // expose the old object. NotLoaded remains a deliberate fail-fast state.
    let panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let stale_relation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        E::customer_order(&saved)
            .get_customer()
            .eval()
            .map(|customer| customer.id())
    }));
    std::panic::set_hook(panic_hook);
    let panic_payload =
        stale_relation.expect_err("a changed FK must invalidate the old loaded Customer relation");
    let diagnostic = panic_payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic_payload.downcast_ref::<&str>().copied())
        .expect("NotLoaded must produce a textual diagnostic");
    assert!(
        diagnostic.contains("customer") && diagnostic.contains("select_customer"),
        "expected a Customer NotLoaded diagnostic, got: {diagnostic}"
    );
    println!("SAVE_FORWARD_FK_NOT_LOADED_EXPECTED order_id={order_id}");

    let reloaded = Q::customer_orders()
        .with_id_is(order_id)
        .select_customer_with(Q::customers().select_self_fields().limit(1))
        .limit(1)
        .comment("what: reload Order and replacement Customer from storage")
        .purpose("why: prove the persisted FK and hydrated graph agree")
        .execute_for_one(&context)
        .await?
        .expect("updated Order must persist");
    assert_eq!(reloaded.customer_id(), replacement_id);
    assert_eq!(
        E::customer_order(&reloaded)
            .get_customer()
            .eval()
            .expect("replacement Customer must hydrate")
            .id(),
        replacement_id
    );
    println!(
        "SAVE_FORWARD_FK_PASS order_id={order_id} old_customer_id={old_customer_id} new_customer_id={replacement_id}"
    );
    Ok(())
}
