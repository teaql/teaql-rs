//! Required end-to-end acceptance: save only a subtype, then query its parent.
//! Unlike the composition fixture, this must not materialize a parent manually.
use inheritance_matching_service_core::teaql_core::Entity as _;
use inheritance_matching_service_core::{
    AuditedSave as _, E, Q, ServiceRuntimeConfig, service_runtime,
};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = service_runtime(ServiceRuntimeConfig::from_env()?).await?;
    context.ensure_schema().await?;
    let mut academy = Q::academies()
        .comment("Create one inherited object without a manual parent")
        .purpose("Verify the natural inheritance lifecycle before declaring query parity")
        .new_entity(&context);
    academy.update_platform_id(1_u64);
    academy.update_school_type_to_primary();
    academy.update_name("Natural inheritance academy");
    academy.update_student_capacity(20_i64);
    academy.update_campus_code("NORTH");
    let saved = academy
        .audit_as("Create the natural inherited query acceptance object")
        .save(&context)
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

    let mut parent = Q::schools()
        .with_id_is(id)
        .limit(1)
        .comment("Load the inherited object through its parent view")
        .purpose("Verify one authoritative parent value and version")
        .execute_for_one(&context)
        .await?
        .ok_or("updated parent missing")?;
    assert_eq!(parent.version(), 2);
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
    let tombstone = Q::academies()
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
    println!(
        "NATURAL_INHERITANCE_LIFECYCLE={}",
        serde_json::json!({"passed":true,"id":id,
        "generatedQAndE":true,"auditedMutation":true,"manualParentSeed":false,
        "caseCount":8,"versions":[1,2,3,-4],"noopVersion":1,
        "partialWriteRejected":true,"staleWriteRejected":true,"deletedVersion":-4,
        "parentAndChildHidden":true,"retainedDeletedRow":true})
    );
    Ok(())
}
