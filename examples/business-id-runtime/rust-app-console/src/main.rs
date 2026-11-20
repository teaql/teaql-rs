use chrono::{TimeZone as _, Utc};
use purchase_order_numbering_core::{
    service_runtime_from_env, teaql_core::Entity as _, AuditedSave as _, PurchaseOrder, Q,
};
use serde_json::json;
use teaql_runtime::{FixedBusinessClock, RuntimeError, UserContext};

const REJECTED_SUPPLIER: &str = "Unaudited Supplier";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let round = std::env::var("TEAQL_PURCHASE_NUMBERING_ROUND")?;
    let mut context = service_runtime_from_env().await?;
    context.set_business_clock(FixedBusinessClock::new(
        Utc.with_ymd_and_hms(2026, 10, 1, 8, 30, 0).unwrap(),
    ));
    context.ensure_schema().await?;

    let first = create_order(&context, &format!("Northwind-{round}")).await?;
    let second = create_order(&context, &format!("Contoso-{round}")).await?;
    let (first_id, second_id) = (first.id(), second.id());
    let (first_number, second_number) = (first.order_number(), second.order_number());
    assert_ne!(first_id, second_id);
    assert_ne!(first_number, second_number);

    let retry = first
        .audit_as("Retry recording Northwind's purchase order")
        .save(&context)
        .await?;
    assert_eq!(retry.id(), first_id);
    assert_eq!(retry.order_number(), first_number);

    for order in [&retry, &second] {
        let found = Q::purchase_orders()
            .with_order_number_is(order.order_number())
            .limit(2)
            .comment("Read a purchase order by its assigned number")
            .purpose("Verify the procurement record and allocation")
            .execute_for_one(&context)
            .await?
            .expect("Typed order-number query must find the saved order");
        assert_eq!(found.id(), order.id());
        assert_eq!(found.order_number(), order.order_number());
        assert_eq!(found.supplier(), order.supplier());
    }

    let rejected = prepare_order(&context, REJECTED_SUPPLIER)?;
    let result = rejected.audit_as(" ").save(&context).await;
    let rejection = match result {
        Err(RuntimeError::RequestIntent(error)) if error.code() == "REQUEST_COMMENT_REQUIRED" => {
            error.code()
        }
        _ => return Err("Expected missing mutation comment rejection".into()),
    };
    let absent = Q::purchase_orders()
        .with_supplier_is(REJECTED_SUPPLIER)
        .limit(2)
        .comment("Look for the unaudited purchase order")
        .purpose("Prove the rejected operation stored no order")
        .execute_for_one(&context)
        .await?;
    assert!(absent.is_none());

    println!(
        "PURCHASE_ORDER_NUMBERING={}",
        json!({
            "round": round, "firstId": first_id, "secondId": second_id,
            "retryStable": true, "rejection": rejection, "typedReadback": true,
        })
    );
    Ok(())
}

fn prepare_order(context: &UserContext, supplier: &str) -> Result<PurchaseOrder, RuntimeError> {
    let mut order = Q::purchase_orders()
        .comment("Initialize the purchase order")
        .purpose("Record a procurement commitment")
        .new_entity(context)?;
    order.update_platform_id(1_u64);
    order.update_supplier(supplier);
    Ok(order)
}

async fn create_order(
    context: &UserContext,
    supplier: &str,
) -> Result<PurchaseOrder, RuntimeError> {
    prepare_order(context, supplier)?
        .audit_as("Record the approved purchase order")
        .save(context)
        .await
}
