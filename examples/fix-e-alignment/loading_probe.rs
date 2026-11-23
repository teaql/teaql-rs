//! Real generated Q/E/save loading-state boundary checks, not hand-built rows.
use checker_fix_advanced_core::{AuditedSave, E, Q};
use teaql_core::Entity;
use teaql_runtime::{RuntimeError, UserContext};

pub async fn verify(context: &UserContext, id: u64) -> Result<(), Box<dyn std::error::Error>> {
    let rows = Q::purchase_orders()
        .with_id_is(id)
        .select_tenant_with(
            Q::tenants_minimal()
                .with_id_is(2_u64)
                .limit(1)
                .comment("Load only a different tenant's detail"),
        )
        .limit(1)
        .comment("Keep the selected FK when target detail is filtered out")
        .purpose("Verify filtered target detail is NotLoaded rather than loaded null")
        .execute_for_list(context)
        .await?;
    assert_eq!(rows.len(), 1);
    let mut owner = rows.first().unwrap().clone();
    assert_eq!(E::purchase_order(&owner).get_tenant_id().eval(), Some(1));
    let error = E::purchase_order(&owner)
        .get_tenant()
        .get_name()
        .try_eval()
        .expect_err("A filtered target must be NotLoaded, not loaded null");
    let detail = error.to_wire();
    assert_eq!(detail.attempted_instance_path, "/tenant/name");
    owner.update_contact_phone("5550102");
    owner
        .audit_as("Change only the complete owner, not its partial target")
        .save(context)
        .await?;
    let complete = Q::purchase_orders()
        .with_id_is(id)
        .limit(1)
        .comment("Reload the successful owner update")
        .purpose("Verify untouched partial targets do not prevent saving the owner")
        .execute_for_list(context)
        .await?;
    assert_eq!(complete.len(), 1);
    assert_eq!(
        E::purchase_order(complete.first().unwrap())
            .get_contact_phone()
            .eval()
            .as_deref(),
        Some("5550102")
    );
    let version = E::purchase_order(complete.first().unwrap())
        .get_version()
        .eval();
    let partials = Q::purchase_orders_minimal()
        .with_id_is(id)
        .limit(1)
        .comment("Load an identity-only owner")
        .purpose("Reject mutation of an incomplete owner before persistence")
        .execute_for_list(context)
        .await?;
    assert_eq!(partials.len(), 1);
    let mut partial = partials.first().unwrap().clone();
    partial.update_contact_phone("5550103");
    let error = partial
        .audit_as("Attempt to modify an incomplete owner")
        .save(context)
        .await
        .expect_err("Incomplete modified owner must be rejected");
    assert!(matches!(error, RuntimeError::Check(ref results) if !results.is_empty()));
    let unchanged = Q::purchase_orders()
        .with_id_is(id)
        .limit(1)
        .comment("Reload after the rejected incomplete mutation")
        .purpose("Verify persisted values and optimistic version remain unchanged")
        .execute_for_list(context)
        .await?;
    assert_eq!(unchanged.len(), 1);
    assert_eq!(
        E::purchase_order(unchanged.first().unwrap())
            .get_contact_phone()
            .eval()
            .as_deref(),
        Some("5550102")
    );
    assert_eq!(
        E::purchase_order(unchanged.first().unwrap())
            .get_version()
            .eval(),
        version
    );
    println!(
        "CORE_LOAD_EXAMPLE={}",
        serde_json::json!({"language": "rust", "passed": true, "foreignKeyPreserved": true,
            "filteredDetailNotLoaded": detail, "untouchedPartialTargetSave": true,
            "modifiedPartialOwnerRejected": true, "rejectedVersionUnchanged": true})
    );
    Ok(())
}
