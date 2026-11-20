use serde_json::{json, Value};
use shipping_order_logging_core::{
    request_support::AuditedSave as _, service_runtime, teaql_core::Entity as _,
    ServiceRuntimeConfig, Q,
};
use std::sync::{Arc, Mutex};
use teaql_runtime::{
    LedgerEntity as _, RuntimeError, SafeAuditEvent, SafeAuditEventSink, UserContext,
};

const CUSTOMER_REFERENCE: &str = "PRIVATE-CARRIER-REFERENCE-5821";
const AMENDMENT: &str = "Keep upright for the carrier";

#[derive(Clone, Default)]
struct ShippingAudit(Arc<Mutex<Vec<Value>>>);

impl SafeAuditEventSink for ShippingAudit {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        let fields: Vec<_> = event
            .fields
            .iter()
            .map(|field| json!({"name":field.name,"value":field.value,"masked":field.masked}))
            .collect();
        let reasons: Vec<_> = event
            .trace_chain
            .iter()
            .map(|node| node.comment.as_str())
            .collect();
        self.0.lock().expect("shipping audit lock").push(json!({
            "entity":event.entity,"id":event.entity_id,"fields":fields,"reasons":reasons,
        }));
        Ok(())
    }
}

async fn lookup(
    context: &UserContext,
) -> Result<shipping_order_logging_core::ShippingOrder, Box<dyn std::error::Error>> {
    Ok(Q::shipping_orders()
        .with_customer_reference_is(CUSTOMER_REFERENCE)
        .limit(1)
        .select_shipping_item_list()
        .comment("Find the carrier shipment")
        .purpose("Read the shipping order and its loading lines")
        .execute_for_one(context)
        .await?
        .ok_or("The shipping order is missing")?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().collect();
    assert_eq!(
        arguments.len(),
        3,
        "Usage: shipping-order-logging DATABASE SAFE_AUDIT_JSON"
    );
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: arguments[1].clone(),
    })
    .await?;
    context.enable_select_sql_log();
    context.enable_mutation_sql_log();
    let audit = ShippingAudit::default();
    context.set_custom_event_sink(audit.clone());
    context.ensure_schema().await?;
    let warehouse = Q::warehouses()
        .limit(1)
        .comment("Read the dispatch warehouse")
        .purpose("Associate the shipping order with its warehouse")
        .execute_for_one(&context)
        .await?
        .ok_or("The warehouse is missing")?;
    audit.0.lock().expect("shipping audit lock").clear();

    let mut order = Q::shipping_orders()
        .comment("Prepare a shipping order")
        .purpose("Record a carrier shipment")
        .new_entity(&context)?;
    order.update_warehouse_id(warehouse.id());
    order.update_reference("SHIP-001");
    order.update_customer_reference(CUSTOMER_REFERENCE);
    order.update_instructions("Await carrier");
    let mut item = Q::shipping_items()
        .comment("Prepare one returnable case")
        .purpose("Add the carrier load to the order")
        .new_entity(&context)?;
    item.update_shipping_order_id(order.id());
    item.update_sku("RETURNABLE-CASE");
    item.update_quantity(2_i64);
    order.include_pending_mutations_from(&item)?;
    let order_id = order.id();
    order
        .audit_as("Accept the carrier shipment")
        .save(&context)
        .await?;

    let mut loaded = lookup(&context).await?;
    let relation = loaded.shipping_item_list();
    let items = relation
        .value()
        .ok_or("The order's carrier load was not selected")?;
    assert_eq!(loaded.reference(), "SHIP-001");
    assert_eq!(loaded.customer_reference(), CUSTOMER_REFERENCE);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].quantity(), 2);
    loaded.update_instructions(AMENDMENT);
    let mut updated = loaded
        .audit_as("Change handling instructions")
        .save(&context)
        .await?;
    let committed = audit.0.lock().expect("shipping audit lock").len();

    // Inject a real SQLite constraint error into the generated save.
    let conn = rusqlite::Connection::open(&arguments[1])?;
    conn.execute_batch("CREATE TRIGGER reject_carrier_override BEFORE UPDATE OF instructions ON shipping_order_data
      WHEN NEW.instructions = 'Declined override'
      BEGIN SELECT RAISE(ABORT, 'carrier override rejected'); END")?;
    updated.update_instructions("Declined override");
    let rejected = updated
        .audit_as("Reject a carrier override")
        .save(&context)
        .await;
    conn.execute_batch("DROP TRIGGER reject_carrier_override")?;
    let error = rejected.expect_err("The forbidden carrier override was saved");
    assert!(
        error.to_string().contains("carrier override rejected"),
        "{error}"
    );

    let saved = lookup(&context).await?;
    assert_eq!(saved.id(), order_id);
    assert_eq!(saved.customer_reference(), CUSTOMER_REFERENCE);
    assert_eq!(saved.instructions(), AMENDMENT);
    assert_eq!(saved.version(), 2);
    assert_eq!(committed, 3);
    let audits = audit.0.lock().expect("shipping audit lock");
    assert_eq!(audits.len(), committed);
    let safe = serde_json::to_string(&*audits)?;
    assert!(!safe.contains(CUSTOMER_REFERENCE) && safe.contains("Change handling instructions"));
    std::fs::write(&arguments[2], safe + "\n")?;
    let relation = saved.shipping_item_list();
    let items = relation
        .value()
        .ok_or("The committed order's carrier load was not selected")?;
    println!(
        "SHIPPING_LOG_RESULT={}",
        json!({
            "orderId":saved.id(),"itemId":items[0].id(),"version":saved.version(),"committedAudits":committed,"rejected":true,
        })
    );
    Ok(())
}
