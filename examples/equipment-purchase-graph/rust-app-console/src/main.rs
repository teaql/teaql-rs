use equipment_procurement_core::{
    request_support::AuditedSave as _, service_runtime, teaql_core::Entity as _, PurchaseOrder,
    ServiceRuntimeConfig, TeaqlRuntime, E, Q,
};
use serde_json::{json, Value};
use teaql_runtime::LedgerEntity as _;

type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

const APPROVED: &str = "PROC-042";
const BLOCKED: &str = "PROC-043";
const KEYBOARD: &str = "USB-KEY";
const MOUSE: &str = "USB-MOUSE";
const REJECTED_AFTER_WRITES: &str = "unapproved dock rejected after order and first line";

async fn seed_catalog(context: &impl TeaqlRuntime, desk_id: u64) -> AppResult<()> {
    for (sku, label) in [(KEYBOARD, "Keyboard"), (MOUSE, "Mouse")] {
        let mut product = Q::catalog_products()
            .comment("Register a catalog product")
            .purpose("Provide orderable equipment for workstation setup")
            .new_entity(context)?;
        product
            .update_procurement_desk_id(desk_id)
            .update_sku(sku)
            .update_label(label);
        product
            .audit_as("Approve the equipment for workplace procurement")
            .save(context)
            .await?;
    }
    Ok(())
}

async fn catalog_id(context: &impl TeaqlRuntime, sku: &str) -> AppResult<u64> {
    let product = Q::catalog_products()
        .with_sku_is(sku)
        .limit(1)
        .comment("Find the approved catalog item by SKU")
        .purpose("Keep each purchase line linked to a shared catalog identity")
        .execute_for_one(context)
        .await?
        .ok_or("An approved product is missing")?;
    assert_eq!(product.version(), 1);
    Ok(product.id())
}

async fn assemble_order(
    context: &impl TeaqlRuntime,
    desk_id: u64,
    reference: &str,
    lines: &[(&str, u64, i64)],
) -> AppResult<(PurchaseOrder, Vec<u64>)> {
    let mut order = Q::purchase_orders()
        .comment("Prepare an equipment purchase order")
        .purpose("Order workstation equipment as one graph")
        .new_entity(context)?;
    order
        .update_procurement_desk_id(desk_id)
        .update_reference(reference);
    let mut line_ids = Vec::new();
    for (code, product_id, quantity) in lines {
        let mut line = Q::purchase_lines()
            .comment("Prepare one equipment purchase line")
            .purpose("Add a catalog item to the same purchase order")
            .new_entity(context)?;
        line.update_purchase_order_id(order.id())
            .update_catalog_product_id(*product_id)
            .update_line_code(*code)
            .update_quantity(*quantity);
        order.include_pending_mutations_from(&line)?;
        if *code == "desk-keyboard" {
            // Import the same pending line twice and verify it is persisted only once.
            order.include_pending_mutations_from(&line)?;
        }
        line_ids.push(line.id());
    }
    Ok((order, line_ids))
}

async fn read_approved(
    context: &impl TeaqlRuntime,
    keyboard_id: u64,
    mouse_id: u64,
) -> AppResult<Value> {
    let items = Q::purchase_lines()
        .select_catalog_product()
        .order_by_id_asc()
        .limit(4)
        .comment("Load purchase lines and their catalog product");
    let order = Q::purchase_orders()
        .with_reference_is(APPROVED)
        .select_purchase_line_list_with(items)
        .limit(1)
        .comment("Load the approved purchase order and its complete graph")
        .purpose("Match equipment and quantities to the approved request")
        .execute_for_one(context)
        .await?
        .ok_or("The approved purchase order is missing")?;
    let order_expr = E::purchase_order(&order);
    assert_eq!(order_expr.get_reference().eval().as_deref(), Some(APPROVED));
    assert_eq!(order.version(), 1);
    assert_eq!(order_expr.get_purchase_line_list().size().eval(), Some(3));

    let mut lines = Vec::new();
    for (index, (code, product_id, label, quantity)) in [
        ("desk-keyboard", keyboard_id, "Keyboard", 2),
        ("desk-mouse", mouse_id, "Mouse", 2),
        ("spare-keyboard", keyboard_id, "Keyboard", 1),
    ]
    .into_iter()
    .enumerate()
    {
        let line = order_expr.get_purchase_line_list().get(index);
        let line_id = line.get_id().eval().ok_or("Missing purchase line ID")?;
        assert_eq!(line.get_line_code().eval().as_deref(), Some(code));
        assert_eq!(line.get_quantity().eval(), Some(quantity));
        assert_eq!(line.get_version().eval(), Some(1));
        assert_eq!(line.get_purchase_order_id().eval(), Some(order.id()));
        assert_eq!(line.get_catalog_product_id().eval(), Some(product_id));
        let product = line.get_catalog_product();
        assert_eq!(product.get_id().eval(), Some(product_id));
        assert_eq!(product.get_label().eval().as_deref(), Some(label));
        lines.push(json!({"id": line_id, "code": code, "productId": product_id, "label": label, "quantity": quantity}));
    }
    Ok(json!({"id": order.id(), "reference": APPROVED, "version": order.version(), "lines": lines}))
}

async fn prove_rollback(
    context: &impl TeaqlRuntime,
    order_id: u64,
    line_ids: &[u64],
) -> AppResult<()> {
    let order = Q::purchase_orders_minimal()
        .with_id_is(order_id)
        .limit(1)
        .comment("Look for the rejected order identity")
        .purpose("Verify that the database rolled back the purchase order")
        .execute_for_one(context)
        .await?;
    assert!(order.is_none(), "The rejected order persisted");
    for id in line_ids {
        let line = Q::purchase_lines_minimal()
            .with_id_is(*id)
            .limit(1)
            .comment("Look for a rejected purchase line identity")
            .purpose("Verify that the database rolled back every purchase line")
            .execute_for_one(context)
            .await?;
        assert!(line.is_none(), "A rejected purchase line persisted");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() == 3 && matches!(args[2].as_str(), "create" | "reject" | "restart"),
        "Usage: equipment-procurement-core-workspace <sqlite-file> <create|reject|restart>"
    );
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: args[1].clone(),
    })
    .await?;
    context.ensure_schema().await?;
    let desk = Q::procurement_desks_minimal()
        .with_id_is(1_u64)
        .select_name()
        .limit(1)
        .comment("Find the operations procurement desk")
        .purpose("Identify the department placing the purchase")
        .execute_for_one(&context)
        .await?
        .ok_or("The procurement desk is missing")?;
    assert_eq!(desk.name(), "Operations Procurement");
    if args[2] == "create" {
        seed_catalog(&context, desk.id()).await?;
    }
    let keyboard = catalog_id(&context, KEYBOARD).await?;
    let mouse = catalog_id(&context, MOUSE).await?;
    assert_ne!(keyboard, mouse);
    let mut committed = Value::Null;
    let mut rejected = Value::Null;
    match args[2].as_str() {
        "create" => {
            let lines = [
                ("desk-keyboard", keyboard, 2),
                ("desk-mouse", mouse, 2),
                ("spare-keyboard", keyboard, 1),
            ];
            let (order, line_ids) = assemble_order(&context, desk.id(), APPROVED, &lines).await?;
            let saved = order
                .audit_as("Approve equipment for two workstations")
                .save(&context)
                .await?;
            committed = json!({"id": saved.id(), "version": saved.version(), "lineIds": line_ids});
        }
        "reject" => {
            let lines = [
                ("replacement-keyboard", keyboard, 1),
                ("unapproved-dock", mouse, 1),
            ];
            let (order, line_ids) = assemble_order(&context, desk.id(), BLOCKED, &lines).await?;
            let order_id = order.id();
            let error = order
                .audit_as("Submit a follow-up equipment purchase")
                .save(&context)
                .await
                .expect_err("The unapproved dock purchase committed");
            assert!(error.to_string().contains(REJECTED_AFTER_WRITES), "{error}");
            prove_rollback(&context, order_id, &line_ids).await?;
            rejected = json!({"id": order_id, "reference": BLOCKED, "lineIds": line_ids, "driverError": error.to_string()});
        }
        _ => {}
    }
    let graph = read_approved(&context, keyboard, mouse).await?;
    println!(
        "EQUIPMENT_PURCHASE_GRAPH={}",
        json!({
            "phase": args[2], "graph": graph, "committed": committed, "rejected": rejected
        })
    );
    Ok(())
}
