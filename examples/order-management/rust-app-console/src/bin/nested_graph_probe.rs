use chrono::NaiveDate;
use order_management_service_core::teaql_core::Entity as _;
use order_management_service_core::{
    request_support::AuditedSave as _, service_runtime, ServiceRuntimeConfig, Q,
};
use rust_decimal::Decimal;
use teaql_runtime::LedgerEntity as _;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("TEAQL_NESTED_PROBE_DATABASE")?;
    let database_path = database_url
        .strip_prefix("sqlite:file:")
        .ok_or_else(|| std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "nested graph probe requires a sqlite:file: URL",
        ))?
        .to_owned();
    let context = service_runtime(ServiceRuntimeConfig { database_url }).await?;
    context.ensure_schema().await?;

    let platforms = Q::commerce_platforms()
        .comment("what: find the probe platform")
        .purpose("why: attach the nested mutation graph")
        .execute_for_list(&context)
        .await?;
    let platform_id = if let Some(platform) = platforms.first() {
        platform.id()
    } else {
        let mut platform = Q::commerce_platforms()
            .comment("what: create the probe platform")
            .purpose("why: attach the nested mutation graph")
            .new_entity(&context);
        platform.update_name("Nested probe platform");
        let id = platform.id();
        platform
            .audit_as("Initialize nested probe platform")
            .save(&context)
            .await?;
        id
    };

    let mut customer = Q::customers()
        .comment("what: construct a probe customer")
        .purpose("why: satisfy the order relation")
        .new_entity(&context);
    customer
        .update_name("Nested probe customer")
        .update_email("nested@example.test")
        .update_commerce_platform_id(platform_id);
    let customer_id = customer.id();
    customer
        .audit_as("Create nested probe customer")
        .save(&context)
        .await?;

    let mut product = Q::products()
        .comment("what: construct a probe product")
        .purpose("why: satisfy the order line relation")
        .new_entity(&context);
    product
        .update_name("Nested probe product")
        .update_sku("NESTED-001")
        .update_commerce_platform_id(platform_id);
    let product_id = product.id();
    product
        .audit_as("Create nested probe product")
        .save(&context)
        .await?;

    let mut order = Q::customer_orders()
        .comment("what: construct a probe order")
        .purpose("why: serve as the graph save boundary")
        .new_entity(&context);
    order
        .update_order_number("NESTED-PROBE-001")
        .update_order_date(NaiveDate::from_ymd_opt(2026, 9, 15).unwrap())
        .update_total_amount(Decimal::new(1250, 2))
        .update_status_to_pending()
        .update_customer_id(customer_id)
        .update_commerce_platform_id(platform_id);
    let order_id = order.id();
    order
        .audit_as("Create parent probe order")
        .save(&context)
        .await?;

    let order = Q::customer_orders()
        .with_id_is(order_id)
        .comment("what: reload the full parent order")
        .purpose("why: validate nested graph save")
        .execute_for_one(&context)
        .await?
        .expect("saved parent order should be available");
    let mut line = Q::order_lines()
        .comment("what: construct a deliberately incomplete child")
        .purpose("why: validate nested checker path")
        .new_entity(&context);
    line.update_customer_order_id(order_id)
        .update_product_id(product_id)
        .update_product_name("Nested probe product")
        .update_quantity(1)
        .update_commerce_platform_id(platform_id);
    let line_id = line.id();
    order.include_pending_mutations_from(&line)?;

    match order
        .audit_as("Probe missing child SKU")
        .save(&context)
        .await
    {
        Ok(_) => panic!("nested save unexpectedly accepted a child missing sku"),
        Err(error) => {
            let diagnostic = format!("{error:?}");
            assert!(
                diagnostic.contains("Required")
                    && diagnostic.contains("order_line_list")
                    && diagnostic.contains("sku"),
                "expected the nested child checker path, got {diagnostic}"
            );
            println!("NEGATIVE: {diagnostic}");
        }
    }
    let before = Q::order_lines()
        .with_id_is(line_id)
        .comment("what: check rejected child did not persist")
        .purpose("why: prove validation ran before database write")
        .execute_for_list(&context)
        .await?;
    assert!(
        before.is_empty(),
        "rejected child was written to the database"
    );

    line.update_sku("NESTED-001");
    let order = Q::customer_orders()
        .with_id_is(order_id)
        .comment("what: reload the parent after the rejected save")
        .purpose("why: retry nested graph composition without cloning")
        .execute_for_one(&context)
        .await?
        .expect("parent order should remain available");
    order.include_pending_mutations_from(&line)?;
    order
        .audit_as("Save complete nested child")
        .save(&context)
        .await?;
    let after = Q::order_lines()
        .with_id_is(line_id)
        .comment("what: check nested child persisted")
        .purpose("why: prove graph save performs the child write")
        .execute_for_list(&context)
        .await?;
    assert_eq!(after.len(), 1, "complete child did not persist");
    println!(
        "POSITIVE: order_id={order_id}, line_id={line_id}, rows={}",
        after.len()
    );

    let mut removable = Q::order_lines()
        .comment("what: construct a removable probe child")
        .purpose("why: exercise mixed graph save")
        .new_entity(&context);
    removable
        .update_customer_order_id(order_id)
        .update_product_id(product_id)
        .update_product_name("Removable probe product")
        .update_sku("REMOVABLE-001")
        .update_quantity(1)
        .update_commerce_platform_id(platform_id);
    let removable_id = removable.id();
    removable
        .audit_as("Create removable probe child")
        .save(&context)
        .await?;

    let mut parent = Q::customer_orders()
        .with_id_is(order_id)
        .comment("what: load the full parent before a mixed graph save")
        .purpose("why: validate its complete business state")
        .execute_for_one(&context)
        .await?
        .expect("parent must still exist");
    let mut existing = Q::order_lines()
        .with_id_is(line_id)
        .comment("what: load the child to update")
        .purpose("why: include its original version in the graph")
        .execute_for_one(&context)
        .await?
        .expect("existing child must still exist");
    let mut stale = Q::order_lines()
        .with_id_is(line_id)
        .comment("what: independently load a stale child snapshot")
        .purpose("why: prove optimistic conflict after mixed graph save")
        .execute_for_one(&context)
        .await?
        .expect("stale child snapshot must exist");
    let mut stale_for_attachment = Q::order_lines()
        .with_id_is(line_id)
        .comment("what: independently load a second stale child snapshot")
        .purpose("why: verify generated graph attachment rejects mixed versions")
        .execute_for_one(&context)
        .await?
        .expect("attachment probe child snapshot must exist");
    let mut removable = Q::order_lines()
        .with_id_is(removable_id)
        .comment("what: load the child to mark for deletion")
        .purpose("why: include its original version in the graph")
        .execute_for_one(&context)
        .await?
        .expect("removable child must still exist");
    let mut added = Q::order_lines()
        .comment("what: construct a new child in the mixed graph")
        .purpose("why: validate create update and delete in one save")
        .new_entity(&context);
    added
        .update_customer_order_id(order_id)
        .update_product_id(product_id)
        .update_product_name("New mixed-graph child")
        .update_sku("MIXED-001")
        .update_quantity(3)
        .update_commerce_platform_id(platform_id);
    let added_id = added.id();
    let parent_version_before = parent.version();
    let existing_version_before = existing.version();
    let removable_version_before = removable.version();

    parent.update_total_amount(Decimal::new(3750, 2));
    existing.update_quantity(2);
    removable.mark_for_deletion();
    parent.include_pending_mutations_from(&existing)?;
    parent.include_pending_mutations_from(&removable)?;
    parent.include_pending_mutations_from(&added)?;
    parent
        .audit_as("Update order, modify a child, delete a child, and add a child")
        .save(&context)
        .await?;

    let parent_after = Q::customer_orders()
        .with_id_is(order_id)
        .comment("what: verify the parent after mixed graph save")
        .purpose("why: retain mutation evidence")
        .execute_for_one(&context)
        .await?
        .expect("updated parent must exist");
    let existing_after = Q::order_lines()
        .with_id_is(line_id)
        .comment("what: verify the updated child")
        .purpose("why: retain mutation evidence")
        .execute_for_one(&context)
        .await?
        .expect("updated child must exist");
    let added_after = Q::order_lines()
        .with_id_is(added_id)
        .comment("what: verify the new child")
        .purpose("why: retain mutation evidence")
        .execute_for_list(&context)
        .await?;
    let removed_after = Q::order_lines()
        .with_id_is(removable_id)
        .comment("what: verify soft-deleted child is absent from normal queries")
        .purpose("why: retain mutation evidence")
        .execute_for_list(&context)
        .await?;
    assert_eq!(parent_after.total_amount(), Decimal::new(3750, 2));
    assert_eq!(parent_after.version(), parent_version_before + 1);
    assert_eq!(existing_after.quantity(), 2);
    assert_eq!(existing_after.version(), existing_version_before + 1);
    assert_eq!(added_after.len(), 1);
    assert!(removed_after.is_empty(), "deleted child remained in normal results");
    // A conformance-only direct read distinguishes soft deletion from physical deletion.
    let database = rusqlite::Connection::open(database_path)?;
    let deleted_version: i64 = database.query_row(
        "SELECT version FROM order_line_data WHERE id = ?1",
        [i64::try_from(removable_id)?],
        |row| row.get(0),
    )?;
    assert_eq!(
        deleted_version,
        -(removable_version_before + 1),
        "delete intent did not leave a versioned tombstone"
    );
    stale.update_quantity(99);
    match stale
        .audit_as("Probe a stale child update after mixed graph save")
        .save(&context)
        .await
    {
        Ok(_) => panic!("stale child update unexpectedly succeeded"),
        Err(error) => {
            let diagnostic = format!("{error:?}");
            assert!(
                diagnostic.contains("OptimisticLockConflict")
                    && diagnostic.contains(&format!("id: \"{line_id}\"")),
                "expected optimistic conflict, got {diagnostic}"
            );
            println!("STALE: {diagnostic}");
        }
    }
    let post_conflict: (i64, i64) = database.query_row(
        "SELECT version, quantity FROM order_line_data WHERE id = ?1",
        [i64::try_from(line_id)?],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(post_conflict, (existing_version_before + 1, 2));

    // The generated recursive attachment method has a void return type. It must
    // retain a conflict in the receiving ledger so save cannot silently rebase
    // old mutation intent onto the newly loaded optimistic version.
    let mut current_for_attachment = existing_after;
    current_for_attachment.update_quantity(2);
    stale_for_attachment.update_quantity(99);
    let current_state = current_for_attachment
        .entity_runtime_state()
        .expect("generated child must expose its runtime state");
    stale_for_attachment.attach_runtime_state_recursive(current_state);
    match stale_for_attachment
        .audit_as("Probe generated attachment with conflicting child versions")
        .save(&context)
        .await
    {
        Ok(_) => panic!("generated attachment silently accepted conflicting versions"),
        Err(error) => {
            let diagnostic = format!("{error:?}");
            assert!(
                diagnostic.contains("generated entity graph attachment failed before save")
                    && diagnostic.contains(&format!("OrderLine#{line_id}"))
                    && diagnostic.contains(&format!("version {}", existing_version_before + 1))
                    && diagnostic.contains(&format!("version {existing_version_before}")),
                "expected a version-specific generated attachment error, got {diagnostic}"
            );
            println!("HIDDEN_CONFLICT: {diagnostic}");
        }
    }
    let post_attachment: (i64, i64) = database.query_row(
        "SELECT version, quantity FROM order_line_data WHERE id = ?1",
        [i64::try_from(line_id)?],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(post_attachment, (existing_version_before + 1, 2));

    let parent = Q::customer_orders()
        .with_id_is(order_id)
        .comment("what: reload the parent for cancelled new-child intent")
        .purpose("why: prove an unsaved deletion causes no database delete")
        .execute_for_one(&context)
        .await?
        .expect("parent must remain available");
    let mut cancelled = Q::order_lines()
        .comment("what: construct a child that is cancelled before save")
        .purpose("why: validate ledger-only deletion intent")
        .new_entity(&context);
    cancelled
        .update_customer_order_id(order_id)
        .update_product_id(product_id)
        .update_product_name("Cancelled child")
        .update_sku("CANCELLED-001")
        .update_quantity(1)
        .update_commerce_platform_id(platform_id);
    let cancelled_id = cancelled.id();
    cancelled.mark_for_deletion();
    parent.include_pending_mutations_from(&cancelled)?;
    parent
        .audit_as("Cancel a new child within the parent mutation graph")
        .save(&context)
        .await?;
    let cancelled_rows: i64 = database.query_row(
        "SELECT COUNT(*) FROM order_line_data WHERE id = ?1",
        [i64::try_from(cancelled_id)?],
        |row| row.get(0),
    )?;
    assert_eq!(cancelled_rows, 0, "cancelled new child reached the database");
    println!("CANCELLED: new_child_id={cancelled_id}, database_rows=0");
    println!(
        "MIXED: parent_id={order_id}, updated_child={line_id}, deleted_child={removable_id}, added_child={added_id}"
    );
    Ok(())
}
