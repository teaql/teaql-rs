//! Required end-to-end acceptance: save only a subtype, then query its parent.
//! Unlike the composition fixture, this must not materialize a parent manually.
use inheritance_matching_service_core::teaql_core::Entity as _;
use inheritance_matching_service_core::{
    AuditedSave as _, E, Q, ServiceRuntimeConfig, service_runtime,
};
mod observability;
mod rollback;

// Application-owned generic helper: the native entity type survives the same
// public runtime save boundary used by concrete generated audited wrappers.
async fn save_generic<T>(
    entity: T,
    context: &teaql_runtime::UserContext,
    comment: &str,
) -> Result<T, teaql_runtime::RuntimeError>
where
    T: inheritance_matching_service_core::teaql_core::Entity + Send + 'static,
{
    teaql_runtime::AuditedSaveExt::save(entity.audit_as(comment), context).await
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut context = service_runtime(ServiceRuntimeConfig::from_env()?).await?;
    context.ensure_schema().await?;
    if std::env::args().nth(1).as_deref() == Some("--schema-only") {
        return Ok(());
    }
    let events = observability::RecordedEvents::default();
    context.set_custom_event_sink(events.clone());
    let mut academy = Q::academies()
        .comment("Create one inherited object without a manual parent")
        .purpose("Verify the natural inheritance lifecycle before declaring query parity")
        .new_entity(&context);
    academy.update_platform_id(1_u64);
    academy.update_school_type_to_primary();
    academy.update_name("Natural inheritance academy");
    academy.update_student_capacity(20_i64);
    academy.update_campus_code("NORTH");
    let saved = save_generic(
        academy,
        &context,
        "Create the natural inherited query acceptance object",
    )
    .await?;
    let id = saved.id();
    let child = Q::academies_minimal()
        .with_id_is(id)
        .select_name()
        .limit(1)
        .comment("Confirm the inherited child really persisted")
        .purpose("Distinguish missing parent storage from a failed child save")
        .execute_for_one(&context)
        .await?
        .ok_or("child was not persisted")?;
    assert_eq!(child.runtime_type(), "Academy");
    assert_eq!(
        E::academy(&child).get_name().eval().as_deref(),
        Some("Natural inheritance academy")
    );
    let parents = Q::schools_minimal()
        .with_id_is(id)
        .select_name()
        .matching_any_of([Q::academies_minimal().with_id_is(id).into()])
        .limit(10)
        .comment("Find the saved child through its parent request")
        .purpose("Require natural inheritance semantics rather than a manually joined fixture")
        .execute_for_list(&context)
        .await?;
    println!(
        "NATURAL_INHERITANCE_QUERY={}",
        serde_json::json!({"childId":id,"childPresent":true,
        "childVersion":child.version(),"expectedParents":1,"actualParents":parents.len(),"manualParentSeed":false})
    );
    if parents.len() != 1 {
        return Err("NATURAL_PARENT_MISSING: saving the subtype did not make it queryable through its parent".into());
    }
    assert_eq!(parents[0].runtime_type(), "Academy");
    assert_eq!(
        E::school(&parents[0]).get_name().eval().as_deref(),
        Some("Natural inheritance academy")
    );
    let unchanged = saved
        .audit_as("Verify unchanged inherited save retains its version")
        .save(&context)
        .await?;
    assert_eq!(unchanged.version(), 1);
    let mut partial = Q::academies_minimal()
        .with_id_is(id)
        .select_name()
        .limit(1)
        .comment("Load an intentionally incomplete inherited object")
        .purpose("Require complete mutation targets, not partial DTO writes")
        .execute_for_one(&context)
        .await?
        .ok_or("partial child missing")?;
    partial.update_name("Incomplete write must be rejected");
    assert!(
        partial
            .audit_as("Reject incomplete inherited mutation")
            .save(&context)
            .await
            .is_err()
    );

    let mut current = Q::academies()
        .with_id_is(id)
        .limit(1)
        .comment("Load the complete child for an audited update")
        .purpose("Validate inherited and own fields together")
        .execute_for_one(&context)
        .await?
        .ok_or("complete child missing")?;
    let mut stale = Q::academies()
        .with_id_is(id)
        .limit(1)
        .comment("Load an independent old version of the child")
        .purpose("Verify optimistic conflict isolation between query ledgers")
        .execute_for_one(&context)
        .await?
        .ok_or("stale fixture missing")?;
    current.update_name("Updated inherited academy");
    current.update_campus_code("SOUTH");
    let updated = current
        .audit_as("Update inherited name and own campus atomically")
        .save(&context)
        .await?;
    assert_eq!(updated.version(), 2);
    stale.update_campus_code("STALE");
    assert!(
        stale
            .audit_as("Reject an independently loaded stale subtype")
            .save(&context)
            .await
            .is_err()
    );

    let parent = Q::schools()
        .with_id_is(id)
        .limit(1)
        .comment("Load the inherited object through its parent view")
        .purpose("Verify one authoritative parent value and version")
        .execute_for_one(&context)
        .await?
        .ok_or("updated parent missing")?;
    assert_eq!(parent.version(), 2);
    assert_eq!(parent.runtime_type(), "Academy");
    let mut parent = parent
        .audit_as("Preserve parent-view subtype identity through a no-op save")
        .save(&context)
        .await?;
    assert_eq!(parent.version(), 2);
    assert_eq!(parent.runtime_type(), "Academy");
    assert_eq!(
        E::school(&parent).get_name().eval().as_deref(),
        Some("Updated inherited academy")
    );
    parent.update_name("Parent edited academy");
    let parent = parent
        .audit_as("Edit the shared inherited field through School")
        .save(&context)
        .await?;
    assert_eq!(parent.version(), 3);
    assert_eq!(parent.runtime_type(), "Academy");
    let mut child = Q::academies()
        .with_id_is(id)
        .limit(1)
        .comment("Reload the subtype after its parent view changed")
        .purpose("Reject divergent parent and child copies")
        .execute_for_one(&context)
        .await?
        .ok_or("parent-edited child missing")?;
    assert_eq!(child.version(), 3);
    assert_eq!(
        E::academy(&child).get_name().eval().as_deref(),
        Some("Parent edited academy")
    );
    assert_eq!(
        child.clone().into_values().get("campus_code"),
        Some(&inheritance_matching_service_core::teaql_core::Value::from(
            "SOUTH"
        ))
    );
    child.mark_for_deletion();
    let deleted = child
        .audit_as("Mark the inherited object for deletion and save")
        .save(&context)
        .await?;
    assert_eq!(deleted.version(), -4);
    assert!(
        Q::academies()
            .with_id_is(id)
            .limit(1)
            .comment("Check normal subtype visibility after deletion")
            .purpose("Hide the shared root tombstone from subtype queries")
            .execute_for_one(&context)
            .await?
            .is_none()
    );
    assert!(
        Q::schools()
            .with_id_is(id)
            .limit(1)
            .comment("Check normal parent visibility after subtype deletion")
            .purpose("Hide the same logical object through every parent view")
            .execute_for_one(&context)
            .await?
            .is_none()
    );
    let mut tombstone = Q::academies()
        .deleted_rows_only()
        .with_id_is(id)
        .limit(1)
        .comment("Read the retained subtype tombstone explicitly")
        .purpose("Prove soft deletion rather than physical row removal")
        .execute_for_one(&context)
        .await?
        .ok_or("deleted child missing")?;
    assert_eq!(tombstone.version(), -4);
    assert_eq!(
        E::academy(&tombstone).get_name().eval().as_deref(),
        Some("Parent edited academy")
    );
    let mut stale_deleted = Q::academies()
        .deleted_rows_only()
        .with_id_is(id)
        .limit(1)
        .comment("Read an independent tombstone version before recovery")
        .purpose("Reject stale recovery without sharing a mutation ledger")
        .execute_for_one(&context)
        .await?
        .ok_or("stale tombstone missing")?;
    let mut partial_recovery = Q::academies_minimal()
        .deleted_rows_only()
        .with_id_is(id)
        .select_name()
        .limit(1)
        .comment("Load an intentionally incomplete tombstone")
        .purpose("Require complete actual recovery targets rather than implicit loading")
        .execute_for_one(&context)
        .await?
        .ok_or("partial tombstone missing")?;
    partial_recovery.mark_to_recover();
    let before_rejected = events.len();
    assert!(
        partial_recovery
            .audit_as("Reject incomplete recovery")
            .save(&context)
            .await
            .is_err()
    );
    assert_eq!(
        events.len(),
        before_rejected,
        "failed recovery must not publish a committed audit"
    );
    tombstone.mark_to_recover();
    let recovered = tombstone
        .audit_as("Recover without inventing a scalar edit")
        .save(&context)
        .await?;
    assert_eq!(recovered.version(), 5);
    assert_eq!(recovered.runtime_type(), "Academy");
    let mut visible = Q::academies()
        .with_id_is(id)
        .limit(1)
        .comment("Read the recovered child through its ordinary request")
        .purpose("Prove recovery restored visibility and retained inherited values")
        .execute_for_one(&context)
        .await?
        .ok_or("recovered child invisible")?;
    assert_eq!(visible.version(), 5);
    stale_deleted.mark_to_recover();
    assert!(
        stale_deleted
            .audit_as("Reject recovery from the obsolete tombstone version")
            .save(&context)
            .await
            .is_err()
    );
    visible.mark_for_deletion();
    let redeleted = visible
        .audit_as("Delete before testing recovery with edits")
        .save(&context)
        .await?;
    assert_eq!(redeleted.version(), -6);
    let mut edited_recovery = Q::academies()
        .deleted_rows_only()
        .with_id_is(id)
        .limit(1)
        .comment("Load a complete tombstone for recovery and correction")
        .purpose("Persist inherited and child edits with one recovery version")
        .execute_for_one(&context)
        .await?
        .ok_or("second tombstone missing")?;
    edited_recovery.update_name("Corrected recovered academy");
    edited_recovery.update_campus_code("WEST");
    edited_recovery.mark_to_recover();
    let recovered = edited_recovery
        .audit_as("Recover and correct both declaring tables atomically")
        .save(&context)
        .await?;
    assert_eq!(recovered.version(), 7);
    assert_eq!(
        E::academy(&recovered).get_name().eval().as_deref(),
        Some("Corrected recovered academy")
    );
    assert_eq!(
        recovered.clone().into_values().get("campus_code"),
        Some(&inheritance_matching_service_core::teaql_core::Value::from(
            "WEST"
        ))
    );
    let parent = Q::schools()
        .with_id_is(id)
        .limit(1)
        .comment("Read the parent after recovery with field changes")
        .purpose("Require one shared version and corrected ancestor storage")
        .execute_for_one(&context)
        .await?
        .ok_or("recovered parent invisible")?;
    assert_eq!(parent.version(), 7);
    assert_eq!(parent.runtime_type(), "Academy");
    assert_eq!(
        E::school(&parent).get_name().eval().as_deref(),
        Some("Corrected recovered academy")
    );
    let mut live = Q::academies()
        .with_id_is(id)
        .limit(1)
        .comment("Load a live row for a negative recovery test")
        .purpose("Reject recovery of a non-tombstone before persistence")
        .execute_for_one(&context)
        .await?
        .ok_or("live child missing")?;
    live.mark_to_recover();
    let before_rejected = events.len();
    assert!(
        live.audit_as("Reject recovery of an already live row")
            .save(&context)
            .await
            .is_err()
    );
    assert_eq!(events.len(), before_rejected);
    events.verify_recoveries(id);
    let rollback_verified =
        std::env::var("TEAQL_INHERITANCE_ROLLBACK_TEST").as_deref() == Ok("true");
    if rollback_verified {
        rollback::verify(&context, id, &events).await?;
    }
    println!(
        "NATURAL_INHERITANCE_LIFECYCLE={}",
        serde_json::json!({"passed":true,"id":id,
        "generatedQAndE":true,"auditedMutation":true,"manualParentSeed":false,
        "caseCount":if rollback_verified {15} else {13},"versions":if rollback_verified {vec![1,2,3,-4,5,-6,7,8]} else {vec![1,2,3,-4,5,-6,7]},"noopVersion":1,"genericSavePreservesType":true,
        "partialWriteRejected":true,"staleWriteRejected":true,"deletedVersion":-4,
        "parentAndChildHidden":true,"retainedDeletedRow":true,
        "typedViewSubtypeMetadata":true,"parentNoopSubtypeRetained":true,
        "recoveryWithoutScalarEdit":true,"recoveryWithEdits":true,"staleRecoveryRejected":true,
        "partialRecoveryRejected":true,"liveRecoveryRejected":true,"recoveryAuditLineage":true,
        "mixedGraphRollback":rollback_verified,"retrySameGraph":rollback_verified})
    );
    Ok(())
}
