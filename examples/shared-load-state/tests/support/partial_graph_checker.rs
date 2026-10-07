//! A referenced object that is actually modified must itself be complete.
use school_management_service_core::{AuditedSave, Q};
use std::sync::Arc;
use teaql_core::Entity;
use teaql_runtime::UserContext;

pub fn verify<'a>(
    context: &'a UserContext,
    name: &'a str,
    observed: &'a crate::observed_executor::ObservedExecutor<
        school_management_service_core::ServiceRuntimeExecutor,
    >,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn std::error::Error>>> + 'a>>
{
    Box::pin(async move {
        let mut row = Q::schools()
            .with_name_in([name])
            .select_platform_with(Q::platforms_minimal().select_name())
            .limit(1)
            .comment("what: load a full School with partial Platform detail")
            .purpose("why: Checker must reject mutation of a partial referenced object")
            .execute_for_one(context)
            .await?
            .unwrap();
        assert!(row.is_field_loaded("address"));
        let platform = school_management_service_core::E::school(&row)
            .get_platform()
            .eval()
            .unwrap()
            .clone();
        assert!(platform.is_field_loaded("name"));
        assert!(!platform.is_field_loaded("base_url"));
        let untouched_state = platform.loaded_state_snapshot().unwrap();
        row.update_name("Complete parent control");
        row.audit_as("save complete parent without modifying partial Platform")
            .save(context)
            .await?;
        assert!(Arc::ptr_eq(
            &untouched_state,
            &platform.loaded_state_snapshot().unwrap()
        ));
        assert!(!platform.is_field_loaded("base_url"));
        assert!(!platform.is_field_loaded("update_time"));
        let mut restored = Q::schools()
            .with_name_in(["Complete parent control"])
            .limit(1)
            .comment("what: read saved complete parent")
            .purpose("why: verify untouched partial reference does not block persistence")
            .execute_for_one(context)
            .await?
            .unwrap();
        restored.update_name(name);
        restored
            .audit_as("restore parent after modified-only Checker control")
            .save(context)
            .await?;
        println!("PASS generated Rust complete parent saves with untouched partial reference");
        let mut pending = platform.clone();
        pending.update_name("Partial child control");
        let child_state = pending.loaded_state_snapshot().unwrap();
        let before = observed.counts();
        let error = pending
            .clone()
            .audit_as("reject mutation of the incomplete referenced object")
            .save(context)
            .await
            .expect_err("mutation must reject the partial referenced object");
        assert!(
            matches!(error, teaql_runtime::RuntimeError::Check(_)),
            "wrong rejection: {error}"
        );
        assert_eq!(before, observed.counts());
        assert!(Arc::ptr_eq(
            &child_state,
            &platform.loaded_state_snapshot().unwrap()
        ));
        assert!(!platform.is_field_loaded("base_url"));
        assert!(Arc::ptr_eq(
            &child_state,
            &pending.loaded_state_snapshot().unwrap()
        ));
        println!(
            "PASS generated Rust modified partial referenced object rejects before provider entry without widening state"
        );
        Ok(())
    })
}
