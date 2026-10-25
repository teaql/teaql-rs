//! Independent new roots and deferred saves from one generated batch query.
use checker_fix_advanced_core::{AuditedSave, E, Q};
use teaql_core::{Entity, Value};
use teaql_runtime::UserContext;

pub async fn verify(context: &UserContext) -> Result<(), Box<dyn std::error::Error>> {
    let before = Q::purchase_orders()
        .limit(20)
        .comment("Inspect the initial ownership fixture")
        .purpose("Verify independent roots never share pending mutations")
        .execute_for_list(context)
        .await?;
    let mut first = Q::purchase_orders()
        .comment("Prepare independent new owner A")
        .purpose("Verify Context does not own mutation ledgers")
        .new_entity(context);
    let mut second = Q::purchase_orders()
        .comment("Prepare independent new owner B")
        .purpose("Verify Context does not own mutation ledgers")
        .new_entity(context);
    first.update_contact_phone("5550300");
    first.update_internal_note(Value::Null);
    second.update_contact_phone("5550400");
    second.update_internal_note(Value::Null);
    let saved_a = first
        .audit_as("Save only independent new owner A")
        .save(context)
        .await?;
    let initial = Q::purchase_orders()
        .limit(20)
        .comment("Inspect A's save before saving B")
        .purpose("Verify the unsaved new owner did not leak into persistence")
        .execute_for_list(context)
        .await?;
    assert_eq!(initial.len(), before.len() + 1);
    let saved_b = second
        .audit_as("Explicitly save independent new owner B")
        .save(context)
        .await?;
    let batch = Q::purchase_orders()
        .limit(20)
        .comment("Load independent owners in one batch")
        .purpose("Verify independently saveable list roots retain separate mutation boundaries")
        .execute_for_list(context)
        .await?;
    assert_eq!(batch.len(), before.len() + 2);
    let mut owner_a = batch
        .iter()
        .find(|row| row.id() == saved_a.id())
        .unwrap()
        .clone();
    let mut owner_b = batch
        .iter()
        .find(|row| row.id() == saved_b.id())
        .unwrap()
        .clone();
    owner_a.update_contact_phone("5550301");
    owner_b.update_contact_phone("5550401");
    owner_a
        .audit_as("Save only A from a multi-root query result")
        .save(context)
        .await?;
    let unchanged = Q::purchase_orders()
        .with_id_is(saved_b.id())
        .limit(1)
        .comment("Inspect B after saving A")
        .purpose("Verify A neither persists nor commits B")
        .execute_for_list(context)
        .await?;
    assert_eq!(unchanged.len(), 1);
    assert_eq!(
        E::purchase_order(unchanged.first().unwrap())
            .get_contact_phone()
            .eval()
            .as_deref(),
        Some("5550400")
    );
    assert_eq!(
        E::purchase_order(unchanged.first().unwrap())
            .get_version()
            .eval(),
        Some(1)
    );
    owner_b
        .audit_as("Save B after A without discarding its pending mutation")
        .save(context)
        .await?;
    let changed = Q::purchase_orders()
        .with_id_is(saved_b.id())
        .limit(1)
        .comment("Inspect the later independent B save")
        .purpose("Verify B's mutation survived A's save")
        .execute_for_list(context)
        .await?;
    assert_eq!(changed.len(), 1);
    assert_eq!(
        E::purchase_order(changed.first().unwrap())
            .get_contact_phone()
            .eval()
            .as_deref(),
        Some("5550401")
    );
    assert_eq!(
        E::purchase_order(changed.first().unwrap())
            .get_version()
            .eval(),
        Some(2)
    );
    crate::paged_ownership::verify(context, saved_a.id(), saved_b.id()).await?;
    println!(
        "CORE_MUTATION_OWNERSHIP={}",
        serde_json::json!({"language": "rust", "passed": true,
        "independentNewRoots": true, "batchRootIsolation": true,
        "siblingVersionUnchanged": true, "siblingPendingMutationRetained": true})
    );
    Ok(())
}
