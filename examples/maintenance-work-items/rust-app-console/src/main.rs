use maintenance_work_items_core::{
    request_support::AuditedSave as _, service_runtime, teaql_core::Entity as _,
    ServiceRuntimeConfig, TeaqlRuntime, WorkItem, E, Q,
};
use serde_json::json;
use teaql_runtime::RuntimeError;

type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

const TICKET: &str = "LIFT-042";
const DRAFT: &str = "Inspect the loading lift brake";
const UPDATED: &str = "Replace the loading lift brake pads";
const INSPECTION: &str = "The brake pads are below the service limit";

async fn current(context: &impl TeaqlRuntime) -> AppResult<Option<WorkItem>> {
    Ok(Q::work_items()
        .with_ticket_is(TICKET)
        .limit(1)
        .comment("Read the loading lift ticket by its reference")
        .purpose("Check the current maintenance work item")
        .execute_for_one(context)
        .await?)
}

async fn create(context: &impl TeaqlRuntime) -> AppResult<WorkItem> {
    let board = Q::maintenance_boards_minimal()
        .with_id_is(1_u64)
        .limit(1)
        .comment("Find the maintenance board initialized by the model")
        .purpose("File a loading lift inspection")
        .execute_for_one(context)
        .await?
        .ok_or("The maintenance board is missing")?;
    let mut item = Q::work_items()
        .comment("Prepare a loading lift inspection ticket")
        .purpose("Put the inspection on the maintenance board")
        .new_entity(context)?;
    item.update_maintenance_board_id(board.id())
        .update_ticket(TICKET)
        .update_title(DRAFT)
        .update_priority(2_i64);
    item.audit_as("File the loading lift brake inspection")
        .save(context)
        .await?;
    let read = current(context)
        .await?
        .ok_or("The new work item is missing")?;
    assert_eq!(read.version(), 1);
    assert_eq!(E::work_item(&read).get_priority().eval(), Some(2));
    assert_eq!(E::work_item(&read).get_inspection_note().eval(), None);
    Ok(read)
}

async fn amend(context: &impl TeaqlRuntime) -> AppResult<(WorkItem, String)> {
    let mut editor = current(context).await?.ok_or("The inspection is missing")?;
    let mut stale = current(context)
        .await?
        .ok_or("The stale control is missing")?;
    assert_eq!((stale.id(), stale.version()), (editor.id(), 1));
    editor
        .update_priority(1_i64)
        .update_title(UPDATED)
        .update_inspection_note(Some(INSPECTION.to_owned()));
    let saved = editor
        .audit_as("Escalate the ticket after inspecting the brake pads")
        .save(context)
        .await?;
    assert_eq!(saved.version(), 2);
    stale.update_title("Close the work item without inspecting the lift");
    let error = stale
        .audit_as("Reject an update from the old inspection view")
        .save(context)
        .await
        .expect_err("A stale client overwrote the inspector's update");
    assert!(
        matches!(&error, RuntimeError::OptimisticLockConflict { entity, id }
        if entity == "WorkItem" && id == &saved.id().to_string()),
        "{error}"
    );

    let read = Q::work_items()
        .with_ticket_is(TICKET)
        .with_priority_is(1_i64)
        .with_title_is(UPDATED)
        .limit(1)
        .comment("Find the urgent brake repair ticket by title and priority")
        .purpose("Verify that stale editing preserved the inspector's decision")
        .execute_for_one(context)
        .await?
        .ok_or("The updated work item did not match the maintenance queue")?;
    assert_eq!((read.id(), read.version()), (saved.id(), 2));
    assert_eq!(
        E::work_item(&read).get_inspection_note().eval().as_deref(),
        Some(INSPECTION)
    );
    Ok((read, error.to_string()))
}

async fn tombstone(context: &impl TeaqlRuntime) -> AppResult<WorkItem> {
    assert!(
        current(context).await?.is_none(),
        "Closed ticket is still active"
    );
    let closed = Q::work_items()
        .with_ticket_is(TICKET)
        .deleted_rows_only()
        .limit(1)
        .comment("Read the closed brake repair ticket")
        .purpose("Verify that closing retained the maintenance record")
        .execute_for_one(context)
        .await?
        .ok_or("The soft-deleted ticket is missing")?;
    assert_eq!(closed.version(), -3);
    Ok(closed)
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() == 3,
        "Usage: maintenance-work-items-core-workspace <sqlite-file> <create|amend|close|restart>"
    );
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: args[1].clone(),
    })
    .await?;
    context.ensure_schema().await?;
    let (item, conflict) = match args[2].as_str() {
        "create" => (create(&context).await?, None),
        "amend" => {
            let (item, error) = amend(&context).await?;
            (item, Some(error))
        }
        "close" => {
            let mut item = current(&context)
                .await?
                .ok_or("No repair ticket to close")?;
            item.mark_for_deletion();
            item.audit_as("Close the work item once the brake pads are replaced")
                .save(&context)
                .await?;
            (tombstone(&context).await?, None)
        }
        "restart" => (tombstone(&context).await?, None),
        _ => return Err("Unknown maintenance phase".into()),
    };
    let item_expr = E::work_item(&item);
    println!(
        "MAINTENANCE_WORK_ITEM={}",
        json!({
            "phase": args[2], "id": item.id(), "version": item.version(),
            "ticket": item_expr.get_ticket().eval(), "title": item_expr.get_title().eval(),
            "priority": item_expr.get_priority().eval(),
            "inspection": item_expr.get_inspection_note().eval(), "conflict": conflict
        })
    );
    Ok(())
}
