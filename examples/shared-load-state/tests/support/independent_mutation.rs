//! Shared availability must not turn two root edits into one save transaction.
use school_management_service_core::{AuditedSave, E, Q};
use std::sync::Arc;
use teaql_core::Entity;
use teaql_runtime::UserContext;

pub fn verify<'a>(
    context: &'a UserContext,
    first_id: u64,
    first_name: &'a str,
    second_name: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error>>> + 'a>>
{
    // The wide generated test already exercises large async save/graph frames.
    // Keep construction of this additional control out of its caller's frame.
    Box::pin(async move {
        let rows = Q::schools()
            .with_name_in([first_name, second_name])
            .order_by_id_asc()
            .limit(2)
            .comment("what: load two roots with shared field availability")
            .purpose("why: saving one root must not consume another root's pending intent")
            .execute_for_list(context)
            .await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id(), first_id);
        let mut first = rows[0].clone();
        let mut second = rows[1].clone();
        let state = first.loaded_state_snapshot().unwrap();
        assert!(Arc::ptr_eq(
            &state,
            &second.loaded_state_snapshot().unwrap()
        ));
        let first_version = first.version();
        let second_version = second.version();
        let first_pending = format!("{first_name} independent ledger probe");
        let second_pending = format!("{second_name} abandoned ledger probe");
        first.update_name(first_pending.as_str());
        second.update_name(second_pending.as_str());
        assert!(Arc::ptr_eq(&state, &first.loaded_state_snapshot().unwrap()));
        assert!(Arc::ptr_eq(
            &state,
            &second.loaded_state_snapshot().unwrap()
        ));
        let mut saved = first
            .audit_as("save only the first root while another root remains dirty")
            .save(context)
            .await?;
        let stored = Q::schools()
            .with_name_in([first_pending.as_str(), second_name])
            .order_by_id_asc()
            .limit(2)
            .comment("what: inspect both roots after saving only the first")
            .purpose("why: detect accidental persistence of an independent pending mutation")
            .execute_for_list(context)
            .await?;
        assert_eq!(stored.len(), 2);
        assert_eq!(
            E::school(&stored[0]).get_name().eval().as_deref(),
            Some(first_pending.as_str())
        );
        assert_eq!(stored[0].version(), first_version + 1);
        assert_eq!(
            E::school(&stored[1]).get_name().eval().as_deref(),
            Some(second_name)
        );
        assert_eq!(stored[1].version(), second_version);
        assert_eq!(
            E::school(&second).get_name().eval().as_deref(),
            Some(second_pending.as_str())
        );
        assert_eq!(second.version(), second_version);
        assert!(second.dirty_fields().unwrap().contains("name"));
        assert!(saved.dirty_fields().is_none());
        assert!(Arc::ptr_eq(
            &state,
            &second.loaded_state_snapshot().unwrap()
        ));
        // Drop the second pending operation; do not save it just to clean the test.
        saved.update_name(first_name);
        saved
            .audit_as("restore the independently saved test root")
            .save(context)
            .await?;
        println!("PASS generated Rust saving one dirty root leaves another dirty root unpersisted");
        Ok(())
    })
}
