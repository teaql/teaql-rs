use chrono::NaiveDate;
use order_management_service_core::{
    request_support::AuditedSave as _, service_runtime, Q, ServiceRuntimeConfig,
};
use order_management_service_core::teaql_core::Entity as _;
use rust_decimal::Decimal;
use std::path::PathBuf;

const PURPOSE: &str = "Operate the local order-management quick start";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let database = root.join(".local/order.db");
    if !database.exists() {
        println!("[database] {} was not found; TeaQL will create it", database.display());
    }
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: database.to_string_lossy().into_owned(),
    }).await?;
    println!("[schema] ensured 7 generated entity tables");

    let platforms = Q::commerce_platforms()
        .with_name_is("Northwind Demo")
        .comment("Check whether deterministic quick-start data exists")
        .purpose("Initialize the local order-management example")
        .execute_for_list(&context).await?;
    let platform_id = if let Some(platform) = platforms.first() {
        platform.id()
    } else {
        let request = Q::commerce_platforms()
            .comment("Construct the quick-start platform")
            .purpose(PURPOSE);
        let mut platform = request.new_entity(&context);
        platform.update_name("Northwind Demo");
        let platform_id = platform.id();
        platform.audit_as("Create quick-start commerce platform").save(&context).await?;

        platform_id
    };

    let seeded_orders = Q::customer_orders()
        .with_order_number_is("WEB-2026-001")
        .comment("Check whether the deterministic order exists")
        .purpose("Initialize the local order-management example")
        .execute_for_list(&context).await?;
    if seeded_orders.is_empty() {
        let request = Q::customers().comment("Construct the quick-start customer").purpose(PURPOSE);
        let mut customer = request.new_entity(&context);
        customer.update_name("Acme Retail")
            .update_email("masked-in-quick-start")
            .update_commerce_platform_id(platform_id);
        let customer_id = customer.id();
        customer.audit_as("Create masked quick-start customer").save(&context).await?;

        let request = Q::customer_orders().comment("Construct the quick-start order").purpose(PURPOSE);
        let mut order = request.new_entity(&context);
        order.update_order_number("WEB-2026-001")
            .update_order_date(NaiveDate::from_ymd_opt(2026, 8, 12).unwrap())
            .update_total_amount(Decimal::new(12995, 2))
            .update_status_to_pending()
            .update_customer_id(customer_id)
            .update_commerce_platform_id(platform_id);
        order.audit_as("Create deterministic quick-start order").save(&context).await?;
        println!("[seed] inserted deterministic customer and order");
    } else {
        println!("[seed] deterministic data already exists; no duplicate rows added");
    }

    let orders = Q::customer_orders()
        .with_order_number_containing("WEB-")
        .order_by_id_asc()
        .comment("List WEB orders for the terminal quick start")
        .purpose("Show the operator a deterministic order list")
        .execute_for_list(&context).await?;
    println!("[query] matched {} order(s)", orders.len());
    for order in orders.iter() {
        println!("  {}  {}  {}", order.order_number(), order.order_date(), order.total_amount());
    }

    let existing = Q::order_search_presets()
        .with_request_id_is("quick-start-pending-orders")
        .comment("Check idempotent quick-start preset")
        .purpose("Persist the operator's reusable search")
        .execute_for_one(&context).await?;
    if let Some(preset) = existing {
        println!("[mutation] preset #{} already exists", preset.id());
    } else {
        let request = Q::order_search_presets().comment("Construct a reusable search").purpose(PURPOSE);
        let mut preset = request.new_entity(&context);
        preset.update_name("Pending web orders")
            .update_filter_json("{\"order_number\":\"WEB-\"}")
            .update_request_id("quick-start-pending-orders")
            .update_owner_user_id("quick-start-user")
            .update_commerce_platform_id(platform_id);
        let preset_id = preset.id();
        preset.audit_as("Save idempotent quick-start search preset").save(&context).await?;
        println!("[mutation] saved preset #{}", preset_id);
    }
    Ok(())
}
