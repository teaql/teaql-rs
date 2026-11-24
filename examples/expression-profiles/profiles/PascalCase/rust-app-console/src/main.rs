use dispatch_document_core::{
    request_support::AuditedSave as _, runtime::generated_wire_field_mappings, service_runtime,
    ServiceRuntimeConfig, E, Q,
};
use serde_json::{json, Map, Value};
use teaql_core::Entity as _;

const REFERENCE: &str = "DOCK-042";
const NOTE: &str = "Lift-gate pickup after 14:00";
const DOCK: &str = "North Receiving Terminal";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    assert!(args.len() == 4 && matches!(args[3].as_str(), "first" | "restart"));
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: args[1].clone(),
    })
    .await?;
    context.ensure_schema().await?;
    if args[3] == "first" {
        let mut order = Q::dispatch_orders()
            .comment("Prepare the receiving order")
            .purpose("Set its loading instruction")
            .new_entity(&context)?;
        order
            .update_distribution_center_id(1_u64)
            .update_order_reference(REFERENCE)
            .update_handling_note(NOTE);
        order
            .audit_as("Register the dispatch at the receiving terminal")
            .save(&context)
            .await?;
    }
    let child = Q::dispatch_orders()
        .with_order_reference_is(REFERENCE)
        .limit(1)
        .comment("Select the dispatch order");
    let root = Q::distribution_centers()
        .with_id_is(1_u64)
        .select_dispatch_order_list_with(child)
        .limit(1)
        .comment("Read the dispatch desk and its order")
        .purpose("Check the complete loading instruction")
        .execute_for_one(&context)
        .await?
        .ok_or("The dispatch desk is missing")?;
    let desk = E::distribution_center(&root).get_dock_name().try_eval()?;
    let order = E::distribution_center(&root)
        .get_dispatch_order_list()
        .first();
    let reference = order.get_order_reference().try_eval()?;
    let note = E::distribution_center(&root)
        .get_dispatch_order_list()
        .first()
        .get_handling_note()
        .try_eval()?;
    assert_eq!(desk.as_deref(), Some(DOCK));
    assert_eq!(reference.as_deref(), Some(REFERENCE));
    assert_eq!(note.as_deref(), Some(NOTE));

    let orders = Q::dispatch_orders()
        .with_order_reference_is(REFERENCE)
        .limit(2)
        .comment("Read back the dispatch record")
        .purpose("Project the dispatch document through the generated wire names")
        .execute_for_list(&context)
        .await?;
    assert_eq!(orders.len(), 1);
    assert_eq!((orders[0].id(), orders[0].version()), (1, 1));
    let fields = generated_wire_field_mappings();
    let fields = fields
        .get("DispatchOrder")
        .ok_or("Missing model wire mapping")?;
    let wire_reference = fields
        .get("order_reference")
        .ok_or("Missing reference field")?;
    let wire_note = fields.get("handling_note").ok_or("Missing note field")?;
    let document = Map::from_iter([
        (wire_reference.clone(), json!(reference)),
        (wire_note.clone(), json!(note)),
    ]);
    let reloaded: Map<String, Value> = serde_json::from_slice(&serde_json::to_vec(&document)?)?;
    assert_eq!(reloaded[wire_reference], REFERENCE);
    assert_eq!(reloaded[wire_note], NOTE);

    let sparse = Q::distribution_centers_minimal()
        .with_id_is(1_u64)
        .select_dock_name()
        .limit(1)
        .comment("Read the desk without its dispatch orders")
        .purpose("Verify the modeled preload instruction")
        .execute_for_one(&context)
        .await?
        .ok_or("The sparse dispatch desk is missing")?;
    assert_eq!(
        E::distribution_center(&sparse).get_dock_name().try_eval()?,
        desk
    );
    let missing = E::distribution_center(&sparse)
        .get_dispatch_order_list()
        .first()
        .get_order_reference()
        .try_eval()
        .expect_err("A dispatch was returned without loading the list");
    println!(
        "DISPATCH_DOCUMENT_PROFILE={}",
        json!({
            "profile": args[2], "phase": args[3], "reference": REFERENCE, "handlingNote": NOTE,
            "id": orders[0].id(), "version": orders[0].version(), "dock": desk,
            "document": document, "diagnostic": missing.to_wire(), "human": missing.to_string(),
        })
    );
    Ok(())
}
