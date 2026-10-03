//! Loaded cross-type privacy through generated Q/E and audited save only.
use super::{AuditCapture, Observation, Outcome};
use teaql_runtime::UserContext;
use trace_chain_service_core::teaql_core::Entity as _;
use trace_chain_service_core::{AuditedSave as _, E, LedgerEntity as _, Q};

fn assert_private(context: &UserContext, capture: &AuditCapture, secrets: &[&str]) {
    let logs = context.sql_logs();
    assert!(!logs.is_empty(), "real SQL diagnostics required");
    let audits = capture.events();
    assert_eq!(audits.len(), 2, "root and child committed facts");
    for secret in secrets {
        for log in &logs {
            let intent = format!(
                "{:?} {:?} {:?} {:?}",
                log.comment, log.purpose, log.audit_reason, log.trace_path
            );
            assert!(
                !intent.contains(secret),
                "cross-type SQL intent leaked loaded sibling value"
            );
        }
        for event in &audits {
            assert!(
                !format!("{:?}", event.trace_chain).contains(secret),
                "cross-type committed audit leaked loaded sibling value"
            );
        }
    }
}

pub async fn loaded_graph_privacy(
    context: &UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    let platform = Q::platforms()
        .limit(1)
        .comment("reuse privacy fixture root")
        .purpose("attach a generated graph")
        .execute_for_one(context)
        .await?
        .ok_or("seeded platform required")?;
    let mut root = Q::customer_orders()
        .comment("prepare privacy root")
        .purpose("exercise generated graph persistence")
        .new_entity(context);
    root.update_platform_id(platform.id());
    root.update_order_number("TRACE-GRAPH-PRIVACY");
    root.update_description("privacy seed");
    let root_id = root.id();
    let mut child = Q::order_items()
        .comment("prepare private child")
        .purpose("capture a known private loaded value")
        .new_entity(context);
    child.update_customer_order_id(root_id);
    let mut old = format!("RUST-PRIVATE-OLD-{root_id}");
    child.update_name(old.clone());
    let child_id = child.id();
    root.include_pending_mutations_from(&child)?;
    root.audit_as("seed private graph").save(context).await?;

    let mut root = Q::customer_orders()
        .with_id_is(root_id)
        .limit(1)
        .comment("load privacy root")
        .purpose("edit the complete loaded aggregate")
        .execute_for_one(context)
        .await?
        .ok_or("root required")?;
    for round in 0..2 {
        let mut child = Q::order_items()
            .with_id_is(child_id)
            .limit(1)
            .comment("load privacy child")
            .purpose("retain original private scalar")
            .execute_for_one(context)
            .await?
            .ok_or("child required")?;
        assert_eq!(
            E::order_item(&child).get_name().eval().as_deref(),
            Some(old.as_str())
        );
        let next = format!("RUST-PRIVATE-NEW-{root_id}-{round}");
        root.update_description(format!("privacy revision {round}"));
        child.update_name(next.clone());
        root.include_pending_mutations_from(&child)?;
        capture.clear();
        observation.clear();
        context.clear_sql_logs();
        root = root
            .audit_as(format!("page 1 replace {old} with {next}"))
            .save(context)
            .await?;
        assert_private(context, capture, &[&old, &next]);
        assert!(
            context.sql_logs().iter().any(|entry| entry
                .audit_reason
                .as_deref()
                .is_some_and(|s| s.contains("page 1"))),
            "public business intent must remain"
        );
        let persisted = Q::order_items()
            .with_id_is(child_id)
            .limit(1)
            .comment("check persisted private value")
            .purpose("masking cannot change stored data")
            .execute_for_one(context)
            .await?
            .ok_or("saved child required")?;
        assert_eq!(
            E::order_item(&persisted).get_name().eval().as_deref(),
            Some(next.as_str())
        );
        old = next;
    }
    let mut child = Q::order_items()
        .with_id_is(child_id)
        .limit(1)
        .comment("load child for deletion")
        .purpose("retain deleted row privacy provenance")
        .execute_for_one(context)
        .await?
        .ok_or("child required")?;
    child.mark_for_deletion();
    root.update_description("delete private child");
    root.include_pending_mutations_from(&child)?;
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    root.audit_as(format!("remove {old}")).save(context).await?;
    assert_private(context, capture, &[&old]);
    assert!(
        Q::order_items()
            .with_id_is(child_id)
            .limit(1)
            .comment("verify deleted child")
            .purpose("normal queries exclude deleted rows")
            .execute_for_one(context)
            .await?
            .is_none()
    );
    context.clear_sql_logs();
    Q::customer_orders()
        .with_id_is(root_id)
        .limit(1)
        .comment(old.clone())
        .purpose("independent request must not inherit private graph values")
        .execute_for_one(context)
        .await?;
    assert_eq!(context.sql_logs()[0].comment.as_deref(), Some(old.as_str()));
    println!("TC-REQ-16 LOADED GRAPH PRIVACY PASSED root={root_id} child={child_id}");
    Ok(())
}
