use order_edit_reference_core::{
    service_runtime, AuditedSave, OrderItem, SalesOrder, ServiceRuntimeConfig, E, Q,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use teaql_core::Entity;
use teaql_runtime::{
    ContextBoundReferenceRuntime, DeploymentProfile, ExternalEntityReference, ReferenceDocumentScope,
    ReferenceIdentity, ReferenceKey, RoundTripReferenceError, StaticReferenceKeyProvider,
    TrustedReferencePrincipal, UserContext,
};

type AppResult<T> = Result<T, Box<dyn std::error::Error>>;
const PURPOSE: &str = "edit-order";

fn editor(actor: &str, merchant_id: u64) -> Result<TrustedReferencePrincipal, RoundTripReferenceError> {
    TrustedReferencePrincipal::new("oidc", actor, "Merchant", merchant_id)
}

fn scope(order: &SalesOrder, purpose: &str, order_id: u64) -> Result<ReferenceDocumentScope, RoundTripReferenceError> {
    ReferenceDocumentScope::new(
        format!("editor-{}", order.id()), purpose, "SalesOrder", order_id, order.version(),
    )
}

async fn load_line(context: &UserContext, id: u64) -> AppResult<OrderItem> {
    let line = Q::order_items()
        .with_id_is(id)
        .limit(1)
        .comment("Read the order item and its current version")
        .purpose("Verify the authenticated line belongs to the order being edited")
        .execute_for_one(context)
        .await?
        .ok_or("The order line was not found")?;
    Ok(line)
}

async fn amend(
    context: &UserContext,
    order: &SalesOrder,
    reference: &ExternalEntityReference,
    quantity: i64,
) -> AppResult<()> {
    let resolved = context.resolve_reference(reference, "OrderItem", &scope(order, PURPOSE, order.id())?)?;
    let mut line = load_line(context, resolved.identity.id).await?;
    if E::order_item(&line).get_sales_order_id().eval() != Some(order.id()) {
        return Err("The line is outside this order".into());
    }
    if line.version() != resolved.identity.version {
        return Err("The edit page is stale".into());
    }
    line.update_quantity(quantity);
    line.audit_as("Accept the edited tea quantity").save(context).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let database_url = format!("sqlite:file:{}", std::env::var("TEAQL_REFERENCE_DATABASE")?);
    let round = std::env::var("TEAQL_SHOWCASE_ROUND")?;
    let access = Arc::new(AtomicBool::new(true));
    let can_edit = Arc::clone(&access);
    let keys = Arc::new(StaticReferenceKeyProvider::new(
        "fixture", [ReferenceKey::new("fixture", [0x27; 32])?],
    )?);
    let policy = Arc::new(move |principal: &TrustedReferencePrincipal, scope: &ReferenceDocumentScope,
                                identity: &ReferenceIdentity| {
        if !can_edit.load(Ordering::SeqCst) || principal.subject != "alice"
            || principal.domain_root_type != "Merchant" || principal.domain_root_id != 1
            || scope.aggregate_type != "SalesOrder" || scope.purpose != PURPOSE
            || identity.entity_type != "OrderItem"
        {
            return Err(RoundTripReferenceError::authorization_required());
        }
        Ok(())
    });
    let reference_runtime = Arc::new(ContextBoundReferenceRuntime::governed(
        DeploymentProfile::Test, "order-editor", "local", keys, policy,
    )?);
    let context = service_runtime(ServiceRuntimeConfig { database_url }).await?
        .with_round_trip_reference_runtime(reference_runtime.clone())
        .with_trusted_reference_principal(editor("alice", 1)?);
    context.ensure_schema().await?;

    let merchant = Q::merchants_minimal()
        .with_id_is(1)
        .limit(1)
        .comment("Read the initialized merchant")
        .purpose("Place the order under the trusted merchant")
        .execute_for_one(&context)
        .await?
        .ok_or("Merchant was not found")?;
    let order_number = format!("EDIT-042-{round}");
    let mut order = Q::sales_orders()
        .comment("Prepare the merchant order")
        .purpose("Create a real order for the editing page")
        .new_entity(&context)?;
    order.update_merchant_id(merchant.id()).update_order_number(order_number.as_str());
    let order = order.audit_as("Open the merchant order").save(&context).await?;
    let mut line = Q::order_items()
        .comment("Prepare the tea line")
        .purpose("Record the original quantity")
        .new_entity(&context)?;
    line.update_sales_order_id(order.id()).update_sku("TEA-001").update_quantity(2_i64);
    let line = line.audit_as("Record the initial order line").save(&context).await?;

    let stored = load_line(&context, line.id()).await?;
    let scope_for_edit = scope(&order, PURPOSE, order.id())?;
    let identity = ReferenceIdentity::new("OrderItem", stored.id(), stored.version())?;
    let reference = context.reference_for(identity.clone(), &scope_for_edit, Duration::from_secs(900))?;
    let wire = context.serialize_reference(&reference)?;
    let returned = context.deserialize_reference(&wire)?;
    assert_eq!(context.resolve_reference(&returned, "OrderItem", &scope_for_edit)?.identity, identity);
    assert_eq!(identity.version, 1);

    let ExternalEntityReference::Governed(token) = &reference else {
        return Err("The workspace requires an opaque governed reference".into());
    };
    assert!(token.starts_with("tqr1.fixture."));
    let midpoint = token.len() / 2;
    let mut tampered = token.clone();
    tampered.replace_range(midpoint..midpoint + 1, if token.as_bytes()[midpoint] == b'A' { "B" } else { "A" });
    assert!(context.resolve_reference(&ExternalEntityReference::Governed(tampered), "OrderItem", &scope_for_edit).is_err());
    assert!(context.resolve_reference(&returned, "OrderItem", &scope(&order, "view-order", order.id())?).is_err());
    assert!(context.resolve_reference(&returned, "OrderItem", &scope(&order, PURPOSE, order.id() + 1)?).is_err());
    for (actor, merchant_id) in [("bob", 1), ("alice", 2)] {
        let other = UserContext::new().with_round_trip_reference_runtime(reference_runtime.clone())
            .with_trusted_reference_principal(editor(actor, merchant_id)?);
        assert!(other.resolve_reference(&returned, "OrderItem", &scope_for_edit).is_err());
    }
    access.store(false, Ordering::SeqCst);
    let rejected = amend(&context, &order, &returned, 99).await.unwrap_err();
    assert_eq!(rejected.downcast_ref::<RoundTripReferenceError>().unwrap().code(),
        "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED");
    access.store(true, Ordering::SeqCst);
    amend(&context, &order, &returned, 4).await?;
    assert_eq!(amend(&context, &order, &returned, 99).await.unwrap_err().to_string(), "The edit page is stale");

    let updated = load_line(&context, line.id()).await?;
    let expression = E::order_item(&updated);
    assert_eq!(expression.clone().get_sales_order_id().eval(), Some(order.id()));
    assert_eq!(expression.clone().get_quantity().eval(), Some(4));
    assert_eq!(expression.clone().get_version().eval(), Some(2));
    assert_eq!(expression.get_sku().eval().as_deref(), Some("TEA-001"));
    println!("ORDER_REFERENCE={}", serde_json::json!({
        "round": round, "orderId": order.id(), "orderNumber": order_number, "lineId": line.id(),
        "sku": "TEA-001", "version": 2, "quantity": 4, "denialCount": 7
    }));
    Ok(())
}
