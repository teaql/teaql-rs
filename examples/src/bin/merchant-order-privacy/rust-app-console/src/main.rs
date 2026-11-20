use merchant_order_privacy_service_core::{
    service_runtime, AuditedSave, CustomerOrder, ServiceRuntimeConfig, E, Q,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use teaql_core::Entity;
use teaql_runtime::{AeadEntityReferenceCodec, UserContext};

type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

async fn place_order(
    context: &UserContext,
    merchant: u64,
    reference: &str,
    email: &str,
) -> AppResult<CustomerOrder> {
    let mut profile = Q::customer_profiles()
        .comment("Prepare the customer's delivery profile")
        .purpose("Associate a customer with one merchant")
        .new_entity(context)?;
    profile.update_merchant_id(merchant).update_email(email);
    let profile = profile
        .audit_as("Record the customer's delivery profile")
        .save(context)
        .await?;
    let mut order = Q::customer_orders()
        .comment("Prepare an order for dispatch")
        .purpose("Record the merchant's order and delivery profile")
        .new_entity(context)?;
    order
        .update_merchant_id(merchant)
        .update_customer_profile_id(profile.id())
        .update_order_number(reference)
        .update_fulfillment_status("ready");
    Ok(order
        .audit_as("Queue a merchant order for dispatch")
        .save(context)
        .await?)
}

async fn lookup(
    context: &UserContext,
    trusted_merchant: u64,
    id: u64,
    reference: &str,
    email: &str,
) -> AppResult<Option<CustomerOrder>> {
    Ok(Q::customer_orders()
        .with_id_is(id)
        .with_order_number_is(reference)
        .with_merchant_matching(Q::merchants_minimal().with_id_is(trusted_merchant))
        .with_customer_profile_matching(Q::customer_profiles_minimal().with_email_is(email))
        .select_customer_profile()
        .limit(1)
        .comment("Load the scoped order and delivery profile")
        .purpose("Review a dispatch order for the authenticated merchant")
        .execute_for_one(context)
        .await?)
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let round = std::env::var("TEAQL_SHOWCASE_ROUND")?;
    let database_url = format!(
        "sqlite:file:{}",
        std::env::var("TEAQL_ORDER_PRIVACY_DATABASE")?
    );
    // Both accounts and the key belong to this disposable fixture.
    let codec = AeadEntityReferenceCodec::new(1, [(1, vec![0x27; 32])])?;
    let mut context = service_runtime(ServiceRuntimeConfig { database_url })
        .await?
        .with_entity_reference_codec(Arc::new(codec));
    context.enable_select_sql_log();
    context.enable_mutation_sql_log();
    context.ensure_schema().await?;
    let home = Q::merchants_minimal()
        .with_id_is(1)
        .limit(1)
        .comment("Read the merchant provisioned by the model")
        .purpose("Set the trusted dispatch scope")
        .execute_for_one(&context)
        .await?
        .ok_or("Merchant not initialized")?;
    let mut other = Q::merchants()
        .comment("Prepare an independent merchant for the access check")
        .purpose("Provide a second merchant with its own dispatch order")
        .new_entity(&context)?;
    other.update_name(format!("Independent retailer {round}").as_str());
    let other = other
        .audit_as("Register a second merchant")
        .save(&context)
        .await?;
    let order_number = format!("SHIP-PRIVATE-{round}");
    let email = format!("sealed-{round}@example.test");
    let foreign_number = format!("SHIP-OTHER-{round}");
    let foreign_email = format!("foreign-{round}@example.test");
    let order = place_order(&context, home.id(), &order_number, &email).await?;
    let foreign = place_order(&context, other.id(), &foreign_number, &foreign_email).await?;
    let purpose = format!("view-dispatch:{}", home.id());
    let token = context.encode_entity_reference(
        "CustomerOrder",
        order.id(),
        order.version(),
        &purpose,
        Duration::from_secs(300),
    )?;
    assert!(token.starts_with("tqr1."));
    assert!(context
        .decode_entity_reference(&token, "CustomerOrder", "edit-dispatch")
        .is_err());
    assert!(context
        .decode_entity_reference(
            &token,
            "CustomerOrder",
            &format!("view-dispatch:{}", other.id())
        )
        .is_err());
    let claims = context.decode_entity_reference(&token, "CustomerOrder", &purpose)?;
    let loaded = lookup(&context, home.id(), claims.id, &order_number, &email)
        .await?
        .ok_or("The authenticated dispatch order was not found")?;
    assert_eq!(loaded.version(), claims.version);
    let profile = E::customer_order(&loaded)
        .get_customer_profile()
        .eval()
        .ok_or("Customer profile was not loaded")?;
    assert_eq!(profile.email(), email);
    assert!(lookup(
        &context,
        home.id(),
        foreign.id(),
        &foreign_number,
        &foreign_email
    )
    .await?
    .is_none());
    println!(
        "ORDER_PRIVACY={}",
        json!({"round": round, "authorizedOrder": loaded.id(),
        "foreignOrder": foreign.id(), "merchant": home.id(), "foreignMerchant": other.id(),
        "purposeDenied": true, "foreignDenied": true, "version": loaded.version()})
    );
    Ok(())
}
