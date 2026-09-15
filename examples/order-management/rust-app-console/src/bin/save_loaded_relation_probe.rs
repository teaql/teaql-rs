use chrono::NaiveDate;
use order_management_service_core::teaql_core::Entity as _;
use order_management_service_core::{
    request_support::AuditedSave as _, service_runtime, DataServiceExecutor, ServiceRuntimeConfig, Q,
};
use rust_decimal::Decimal;
use teaql_runtime::{LedgerEntity as _, LoadedRelation};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("TEAQL_SAVE_LOAD_STATE_DATABASE")?;
    let context = service_runtime(ServiceRuntimeConfig { database_url }).await?;
    context.ensure_schema().await?;

    // Select a root that actually has children. Earlier acceptance runs also
    // create child-free Orders, so "latest Order" is not a stable fixture.
    let seeded_order_id = Q::order_lines()
        .order_by_id_desc()
        .limit(1)
        .comment("what: find a seeded child for the relation-state probe")
        .purpose("why: select its parent instead of a later child-free Order")
        .execute_for_one(&context)
        .await?
        .expect("nested-graph probe must seed an OrderLine first")
        .customer_order_id();

    // The nested-graph acceptance probe seeds this SQLite file first. This
    // request loads every root scalar and an unchanged reverse relation list.
    let mut order = Q::customer_orders()
        .with_id_is(seeded_order_id)
        .select_order_line_list_with(Q::order_lines().select_self_fields().limit(10))
        .limit(1)
        .comment("what: load a complete Order with its child relation snapshot")
        .purpose("why: compare loaded relation state before and after audited Save")
        .execute_for_one(&context)
        .await?
        .expect("nested-graph probe must seed an Order before this check");
    let order_id = order.id();
    let version_before = order.version();
    let relation_before = order.order_line_list();
    assert_eq!(relation_before.state(), LoadedRelation::Loaded);
    let skus_before = relation_before
        .value()
        .expect("loaded relation must have a list")
        .iter()
        .map(|line| line.sku().to_string())
        .collect::<Vec<_>>();
    assert!(!skus_before.is_empty());

    let expected_amount = order.total_amount() + Decimal::new(100, 2);
    order.update_total_amount(expected_amount);
    let saved = order
        .audit_as("change only Order amount; retain unchanged loaded child view")
        .save(&context)
        .await?;
    assert_eq!(saved.id(), order_id);
    assert_eq!(saved.version(), version_before + 1);
    assert_eq!(saved.total_amount(), expected_amount);
    let relation_after = saved.order_line_list();
    assert_eq!(
        relation_after.state(),
        LoadedRelation::Loaded,
        "Save of an unrelated scalar must not silently turn a loaded relation into NotLoaded"
    );
    let skus_after = relation_after
        .value()
        .expect("unchanged loaded relation must retain its list")
        .iter()
        .map(|line| line.sku().to_string())
        .collect::<Vec<_>>();
    assert_eq!(skus_after, skus_before);
    println!(
        "SAVE_LOADED_RELATION_PASS order_id={order_id} version={} children={}",
        saved.version(),
        skus_after.len()
    );

    let mut transaction_order = Q::customer_orders()
        .with_id_is(order_id)
        .select_order_line_list_with(Q::order_lines().select_self_fields().limit(10))
        .comment("what: load the Order relation before a transaction-scoped Save")
        .purpose("why: verify transaction Save preserves an unchanged loaded view")
        .execute_for_one(&context)
        .await?
        .expect("saved Order must still exist");
    assert_eq!(transaction_order.order_line_list().state(), LoadedRelation::Loaded);
    let transaction_version_before = transaction_order.version();
    let transaction_amount = transaction_order.total_amount() + Decimal::new(100, 2);
    transaction_order.update_total_amount(transaction_amount);
    let transaction_saved = context
        .execute_in_transaction::<DataServiceExecutor, _, _>(|scope| {
            Box::pin(async move {
                scope
                    .save_audited(transaction_order.audit_as("change Order amount in one transaction"))
                    .await
            })
        })
        .await?;
    assert_eq!(transaction_saved.id(), order_id);
    assert_eq!(transaction_saved.version(), transaction_version_before + 1);
    assert_eq!(transaction_saved.total_amount(), transaction_amount);
    let transaction_relation = transaction_saved.order_line_list();
    assert_eq!(transaction_relation.state(), LoadedRelation::Loaded);
    let transaction_skus = transaction_relation
        .value()
        .expect("unchanged transaction relation must retain its list")
        .iter()
        .map(|line| line.sku().to_string())
        .collect::<Vec<_>>();
    assert_eq!(transaction_skus, skus_before);
    println!("TRANSACTION_SAVE_LOADED_RELATION_PASS order_id={order_id}");

    let parent = Q::customer_orders()
        .with_id_is(order_id)
        .select_order_line_list_with(Q::order_lines().select_self_fields().limit(10))
        .comment("what: reload a full Order and child list for stale-view guard")
        .purpose("why: prove changing a child invalidates Save's old loaded relation")
        .execute_for_one(&context)
        .await?
        .expect("saved Order must still exist");
    assert_eq!(parent.order_line_list().state(), LoadedRelation::Loaded);
    let child_id = parent
        .order_line_list()
        .value()
        .expect("loaded child list")
        .iter()
        .next()
        .expect("at least one child")
        .id();
    let mut child = Q::order_lines()
        .with_id_is(child_id)
        .comment("what: load the first child for an audited graph mutation")
        .purpose("why: modify one child inside the parent's save boundary")
        .execute_for_one(&context)
        .await?
        .expect("child must exist");
    child.update_quantity(child.quantity() + 1);
    parent.include_pending_mutations_from(&child)?;
    let changed = parent
        .audit_as("change child quantity; do not expose an obsolete loaded relation")
        .save(&context)
        .await?;
    assert_eq!(
        changed.order_line_list().state(),
        LoadedRelation::NotLoaded,
        "a changed child graph must invalidate its prior loaded snapshot"
    );
    println!("SAVE_CHANGED_RELATION_INVALIDATED order_id={order_id} child_id={child_id}");

    let mut empty_order = Q::customer_orders()
        .comment("what: create a root without OrderLine children")
        .purpose("why: exercise loaded Empty rather than Loaded or NotLoaded")
        .new_entity(&context);
    let empty_id = empty_order.id();
    empty_order
        .update_order_number(format!("EMPTY-REL-{empty_id}"))
        .update_order_date(NaiveDate::from_ymd_opt(2026, 9, 15).unwrap())
        .update_total_amount(Decimal::new(500, 2))
        .update_status_to_pending()
        .update_customer_id(saved.customer_id())
        .update_commerce_platform_id(saved.commerce_platform_id());
    empty_order
        .audit_as("create a relation-free Order for state validation")
        .save(&context)
        .await?;
    let mut empty_loaded = Q::customer_orders()
        .with_id_is(empty_id)
        .select_order_line_list_with(Q::order_lines().select_self_fields().limit(10))
        .comment("what: load an Order whose child relation is empty")
        .purpose("why: preserve the Empty relation state through scalar Save")
        .execute_for_one(&context)
        .await?
        .expect("empty Order must persist");
    assert_eq!(
        empty_loaded.order_line_list().state(),
        LoadedRelation::Empty
    );
    empty_loaded.update_total_amount(Decimal::new(600, 2));
    let empty_saved = empty_loaded
        .audit_as("scalar-only update of an Order with empty child list")
        .save(&context)
        .await?;
    assert_eq!(empty_saved.order_line_list().state(), LoadedRelation::Empty);
    println!("SAVE_EMPTY_RELATION_PASS order_id={empty_id}");

    let mut product = Q::products()
        .comment("what: create a Product with an explicit null image URL")
        .purpose("why: verify typed Save preserves loaded null scalar state")
        .new_entity(&context);
    let product_id = product.id();
    product
        .update_name("Scalar state probe Product")
        .update_sku(format!("SCALAR-STATE-{product_id}"))
        .update_commerce_platform_id(saved.commerce_platform_id())
        .update_image_url(order_management_service_core::teaql_core::Value::Null);
    let created_product = product
        .audit_as("create a Product with null image URL")
        .save(&context)
        .await?;
    assert_eq!(created_product.id(), product_id);
    assert_eq!(created_product.version(), 1);
    assert!(created_product.is_loaded("image_url"));
    assert!(matches!(
        created_product.eval_image_url(),
        order_management_service_core::teaql_core::eval::EvalResult::Null
    ));

    let mut loaded_product = Q::products()
        .with_id_is(product_id)
        .comment("what: load the complete Product including its null image URL")
        .purpose("why: change a fully loaded scalar without losing null semantics")
        .execute_for_one(&context)
        .await?
        .expect("created Product must persist");
    assert!(matches!(
        loaded_product.eval_image_url(),
        order_management_service_core::teaql_core::eval::EvalResult::Null
    ));
    loaded_product.update_image_url("https://example.test/scalar-state.png");
    let saved_product = loaded_product
        .audit_as("set Product image URL from null to a concrete value")
        .save(&context)
        .await?;
    assert_eq!(saved_product.id(), product_id);
    assert_eq!(saved_product.version(), 2);
    assert!(saved_product.is_loaded("image_url"));
    assert!(matches!(
        saved_product.eval_image_url(),
        order_management_service_core::teaql_core::eval::EvalResult::Value(Some(ref url))
            if url == "https://example.test/scalar-state.png"
    ));

    let mut transaction_product = Q::products()
        .with_id_is(product_id)
        .comment("what: reload the complete Product before a transaction-scoped null update")
        .purpose("why: verify explicit transaction Save returns authoritative null state")
        .execute_for_one(&context)
        .await?
        .expect("updated Product must persist");
    transaction_product.update_image_url(order_management_service_core::teaql_core::Value::Null);
    let transaction_product = context
        .execute_in_transaction::<DataServiceExecutor, _, _>(|scope| {
            Box::pin(async move {
                scope
                    .save_audited(transaction_product.audit_as("clear Product image URL in transaction"))
                    .await
            })
        })
        .await?;
    assert_eq!(transaction_product.id(), product_id);
    assert_eq!(transaction_product.version(), 3);
    assert!(transaction_product.is_loaded("image_url"));
    assert!(matches!(
        transaction_product.eval_image_url(),
        order_management_service_core::teaql_core::eval::EvalResult::Null
    ));

    let mut partial_product = Q::products_minimal()
        .with_id_is(product_id)
        .select_name()
        .comment("what: deliberately load only Product identity and name")
        .purpose("why: prove NotLoaded scalars cannot be silently saved")
        .execute_for_one(&context)
        .await?
        .expect("Product must exist in a minimal projection");
    assert!(!partial_product.is_loaded("image_url"));
    assert!(matches!(
        partial_product.eval_image_url(),
        order_management_service_core::teaql_core::eval::EvalResult::NotLoaded { .. }
    ));
    partial_product.update_name("Must not persist from an incomplete Product");
    let incomplete_error = partial_product
        .audit_as("reject an incomplete Product update")
        .save(&context)
        .await
        .expect_err("checker must reject an update from a NotLoaded scalar projection");
    assert!(
        incomplete_error.to_string().contains("fully loaded"),
        "{incomplete_error}"
    );
    let persisted_product = Q::products()
        .with_id_is(product_id)
        .comment("what: reload Product after rejected incomplete Save")
        .purpose("why: prove the checker rejected the mutation before database write")
        .execute_for_one(&context)
        .await?
        .expect("rejected Save must not delete Product");
    assert_eq!(persisted_product.name(), "Scalar state probe Product");
    assert_eq!(persisted_product.version(), 3);
    assert!(matches!(
        persisted_product.eval_image_url(),
        order_management_service_core::teaql_core::eval::EvalResult::Null
    ));
    println!("SAVE_SCALAR_STATE_PASS product_id={product_id}");
    Ok(())
}
