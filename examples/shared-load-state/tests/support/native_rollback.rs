//! Native-only authoritative readback failure after real DML in isolated SQLite.
use school_management_service_core::{AuditedSave, E, Q};
use std::sync::Arc;
use teaql_core::Entity;
use teaql_runtime::UserContext;

pub async fn verify(
    context: &UserContext,
    database: &str,
    name: &str,
    sibling: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let rows = Q::schools()
        .with_name_in([name, sibling])
        .select_self_fields()
        .order_by_id_asc()
        .limit(2)
        .comment("what: load native-only rollback controls")
        .purpose("why: preserve independent siblings through readback failure")
        .execute_for_list(context)
        .await?;
    assert_eq!(rows.len(), 2);
    let snapshot = rows[0].loaded_state_snapshot().unwrap();
    assert!(Arc::ptr_eq(
        &snapshot,
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    let original_version = rows[0].version();
    let sibling_version = rows[1].version();
    let mut pending = rows[0].clone();
    let next_name = format!("{name} native-retry");
    pending.update_name(next_name.as_str());
    assert!(Arc::ptr_eq(
        &snapshot,
        &pending.loaded_state_snapshot().unwrap()
    ));
    // Only fault injection uses direct SQL; all business writes use generated mutation.
    let control = rusqlite::Connection::open(database)?;
    control.execute_batch(
        "CREATE TRIGGER controlled_native_readback AFTER UPDATE OF name ON school_data
        WHEN NEW.name LIKE '% native-retry'
        BEGIN UPDATE school_data SET established_date='not-a-date' WHERE id=NEW.id; END",
    )?;
    let failure = pending
        .clone()
        .audit_as("rollback an invalid native authoritative readback")
        .save(context)
        .await;
    control.execute_batch("DROP TRIGGER controlled_native_readback")?;
    let error = failure.expect_err("invalid native date readback must reject and roll back");
    assert!(
        error.to_string().to_lowercase().contains("date"),
        "wrong failure: {error}"
    );
    assert_eq!(pending.version(), original_version);
    assert!(pending.dirty_fields().unwrap().contains("name"));
    assert!(Arc::ptr_eq(
        &snapshot,
        &pending.loaded_state_snapshot().unwrap()
    ));
    assert_eq!(rows[1].version(), sibling_version);
    assert!(rows[1].dirty_fields().is_none());
    assert!(Arc::ptr_eq(
        &snapshot,
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    let stored = Q::schools()
        .with_name_in([name, sibling])
        .select_self_fields()
        .order_by_id_asc()
        .limit(2)
        .comment("what: read native rows after the failed authoritative readback")
        .purpose("why: prove the native transaction was rolled back")
        .execute_for_list(context)
        .await?;
    assert_eq!(stored.len(), 2);
    assert_eq!(
        E::school(&stored[0]).get_name().eval().as_deref(),
        Some(name)
    );
    assert_eq!(stored[0].version(), original_version);
    assert_eq!(
        E::school(&stored[0]).get_established_date().eval(),
        Some("1995-09-01".parse()?)
    );
    assert_eq!(stored[1].version(), sibling_version);
    let mut saved = pending
        .audit_as("retry the unchanged native mutation intent")
        .save(context)
        .await?;
    assert_eq!(saved.version(), original_version + 1);
    assert_eq!(
        E::school(&saved).get_name().eval().as_deref(),
        Some(next_name.as_str())
    );
    assert!(saved.dirty_fields().is_none());
    assert!(Arc::ptr_eq(
        &snapshot,
        &saved.loaded_state_snapshot().unwrap()
    ));
    // Restore the source name for the independent materialization control.
    saved.update_name(name);
    saved
        .audit_as("restore the native rollback fixture name")
        .save(context)
        .await?;
    println!(
        "PASS generated Rust LF11 native readback rollback retains loaded state and retry intent"
    );
    Ok(())
}
