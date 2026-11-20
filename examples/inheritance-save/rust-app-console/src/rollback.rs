use crate::observability::RecordedEvents;
use inheritance_matching_service_core::teaql_core::Entity as _;
use inheritance_matching_service_core::{AuditedSave as _, E, Q};
use teaql_runtime::{LedgerEntity as _, UserContext};

/// The verifier installs an owned reject-only SQL trigger. All application
/// mutation and readback below use generated APIs, never direct SQL.
pub async fn verify(
    context: &UserContext,
    id: u64,
    events: &RecordedEvents,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut root = Q::platforms()
        .with_id_is(1_u64)
        .select_school_list_with(Q::schools().with_id_is(id).limit(1))
        .limit(1)
        .comment("Load the complete graph for transactional failure injection")
        .purpose("Prove two logical entities share one audited save transaction")
        .execute_for_one(context)
        .await?
        .ok_or("graph root missing")?;
    assert_eq!(E::platform(&root).get_school_list().size().eval(), Some(1));
    let root_version = root.version();
    let root_name = E::platform(&root)
        .get_name()
        .eval()
        .ok_or("root name missing")?;
    let mut child = root
        .school_list()
        .value()
        .ok_or("school list not loaded")?
        .iter()
        .next()
        .ok_or("graph child missing")?
        .clone();
    let child_name = E::school(&child)
        .get_name()
        .eval()
        .ok_or("child name missing")?;
    assert_eq!(child.version(), 7);
    root.update_name(format!("Graph root {id}"));
    child.update_name("ROLLBACK_REJECT");
    child.set_comment("Correct the selected child in the shared graph");
    root.include_pending_mutations_from(&child)?;
    let retry = root.clone();
    let before = events.len();
    let failure = root
        .audit_as("Exercise rollback after an earlier logical participant wrote")
        .save(context)
        .await;
    let error = failure.expect_err("the owned late-write fault must be reached");
    assert!(
        error.to_string().contains("owned late school fault"),
        "expected a fault after the earlier root write, got {error}"
    );
    assert_eq!(
        events.len(),
        before,
        "failed graph must not emit committed audit events"
    );
    let persisted_root = Q::platforms()
        .with_id_is(1_u64)
        .limit(1)
        .comment("Read the root after late child failure")
        .purpose("Verify the earlier root write was rolled back")
        .execute_for_one(context)
        .await?
        .ok_or("rolled-back root missing")?;
    let persisted_child = Q::schools()
        .with_id_is(id)
        .limit(1)
        .comment("Read the child after injected failure")
        .purpose("Verify no object or optimistic version partly committed")
        .execute_for_one(context)
        .await?
        .ok_or("rolled-back child missing")?;
    assert_eq!(persisted_root.version(), root_version);
    assert_eq!(
        E::platform(&persisted_root).get_name().eval(),
        Some(root_name)
    );
    assert_eq!(persisted_child.version(), 7);
    assert_eq!(
        E::school(&persisted_child).get_name().eval(),
        Some(child_name)
    );
    assert!(
        child
            .dirty_fields()
            .is_some_and(|fields| fields.contains("name")),
        "failed save preserves caller intent for retry"
    );
    child.update_name(format!("Graph child {id}"));
    retry.include_pending_mutations_from(&child)?;
    let saved = retry
        .audit_as("Retry the same graph after correcting the rejected child")
        .save(context)
        .await?;
    assert_eq!(saved.version(), root_version + 1);
    let persisted_child = Q::schools()
        .with_id_is(id)
        .limit(1)
        .comment("Read the child after retrying the same graph")
        .purpose("Verify preserved ledger intent commits exactly once")
        .execute_for_one(context)
        .await?
        .ok_or("retried child missing")?;
    assert_eq!(persisted_child.version(), 8);
    assert_eq!(
        E::school(&persisted_child).get_name().eval(),
        Some(format!("Graph child {id}"))
    );
    assert_eq!(
        events.len(),
        before + 2,
        "retry commits one audit event per logical participant"
    );
    println!(
        "MIXED_GRAPH_ROLLBACK={}",
        serde_json::json!({"passed":true,"id":id,"rootVersionBefore":root_version,"rootVersionAfter":saved.version(),"childVersionAfter":8,"failedAudits":0,"retryAudits":2,"generatedQAndE":true,"retrySameGraph":true})
    );
    Ok(())
}
