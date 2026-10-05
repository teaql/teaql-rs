//! Loaded cross-type privacy through generated Q/E and audited save only.
use super::{AuditCapture, Observation, Outcome};
use teaql_runtime::{RawAuditEventKind, UserContext};
use trace_chain_service_core::teaql_core::{Entity as _, TraceKind, TraceNode};
use trace_chain_service_core::{AuditedSave as _, E, LedgerEntity as _, Q};

pub(super) fn assert_private(context: &UserContext, capture: &AuditCapture, secrets: &[&str]) {
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

fn lineage(nodes: &[TraceNode]) -> Vec<(&str, Option<u64>, &str)> {
    nodes
        .iter()
        .filter(|node| node.kind == TraceKind::AuditReason)
        .map(|node| {
            (
                node.entity_type.as_str(),
                node.entity_id,
                node.comment.as_str(),
            )
        })
        .collect()
}

fn assert_complete_private_chain(
    context: &UserContext,
    capture: &AuditCapture,
    observation: &Observation,
    root_id: u64,
    child_id: u64,
    reason: &str,
    safe_reason: &str,
    child_kind: RawAuditEventKind,
) {
    use teaql_data_service::DataServiceOperation;
    let expected = vec![("CustomerOrder", Some(root_id), reason)];
    let safe = vec![("CustomerOrder", Some(root_id), safe_reason)];
    let commands = observation.commands();
    let metadata = observation.metadata();
    let audits = capture.events();
    assert_eq!(commands.len(), 2, "root and child commands");
    // The saver also loads authoritative graph state for validation and the
    // returned root. Account for those executions rather than hiding them.
    let state_queries: Vec<_> = metadata
        .iter()
        .filter(|statement| {
            !statement
                .trace_chain
                .iter()
                .any(|node| node.kind == TraceKind::Entity)
        })
        .collect();
    assert!(
        !state_queries.is_empty(),
        "actual graph-state queries are observed"
    );
    for statement in &state_queries {
        assert_eq!(statement.operation, DataServiceOperation::Query);
        assert_eq!(statement.result_count, Some(1));
        assert!(statement.trace_chain.iter().any(|node| {
            node.kind == TraceKind::Purpose
                && node.comment == "runtime: load state for an audited graph mutation"
        }));
    }
    assert_eq!(
        metadata.len(),
        state_queries.len() + 4,
        "all graph-state queries, two writes and two readbacks"
    );
    for request in &commands {
        assert_eq!(
            request.comment(),
            reason,
            "trusted owned intent remains raw"
        );
        assert_eq!(
            lineage(request.trace_chain()),
            expected,
            "complete raw command root chain"
        );
    }
    for statement in &metadata {
        assert_eq!(statement.comment.as_deref(), Some(reason));
        assert_eq!(
            lineage(&statement.trace_chain),
            expected,
            "complete raw physical SQL root chain"
        );
    }
    let logs = context.sql_logs();
    assert_eq!(
        logs.len(),
        metadata.len(),
        "every actual statement reaches the safe SQL sink"
    );
    for log in &logs {
        assert_eq!(
            log.audit_reason.as_deref(),
            Some(safe_reason),
            "safe SQL keeps masked root intent"
        );
        assert_eq!(log.trace_path.len(), 4, "safe SQL route is retained");
        assert_eq!(log.trace_path[0].kind, TraceKind::Operation);
        assert_eq!(log.trace_path[0].entity_type, "CustomerOrder");
        assert_eq!(log.trace_path[2].kind, TraceKind::Provider);
        assert_eq!(log.trace_path[2].entity_type, "sqlite");
        assert_eq!(log.trace_path[3].kind, TraceKind::Sql);
    }
    assert_eq!(audits.len(), 2);
    for (entity, id, kind) in [
        ("CustomerOrder", root_id, RawAuditEventKind::Updated),
        ("OrderItem", child_id, child_kind),
    ] {
        let matching: Vec<_> = audits
            .iter()
            .filter(|event| event.entity == entity && event.entity_id == Some(id))
            .collect();
        assert_eq!(
            matching.len(),
            1,
            "one safe committed audit per typed target"
        );
        let event = matching[0];
        assert_eq!(event.kind, kind);
        assert_eq!(
            event.trace_chain.len(),
            1,
            "safe audit cannot drop or duplicate root"
        );
        assert_eq!(
            lineage(&event.trace_chain),
            safe,
            "complete safe committed root chain"
        );
        let physical: Vec<_> = metadata
            .iter()
            .filter(|statement| {
                statement.trace_chain.iter().any(|node| {
                    node.kind == TraceKind::Entity
                        && node.entity_type == entity
                        && node.entity_id == Some(id)
                })
            })
            .collect();
        assert_eq!(physical.len(), 2, "write/readback retain typed target");
        assert_ne!(physical[0].operation, DataServiceOperation::Query);
        assert_eq!(physical[0].affected_rows, Some(1));
        assert_eq!(physical[1].operation, DataServiceOperation::Query);
        assert_eq!(physical[1].result_count, Some(1));
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
        // Numeric prose can collide with a generated target ID and is then
        // correctly redacted. Use nonnumeric text as the public-prose control;
        // the School bootstrap probe independently requires target-ID masking.
        let reason = format!("first page replace {old} with {next}");
        root = root.audit_as(reason.clone()).save(context).await?;
        assert_private(context, capture, &[&old, &next]);
        assert_complete_private_chain(
            context,
            capture,
            observation,
            root_id,
            child_id,
            &reason,
            "first page replace [REDACTED] with [REDACTED]",
            RawAuditEventKind::Updated,
        );
        assert!(
            context.sql_logs().iter().any(|entry| entry
                .audit_reason
                .as_deref()
                .is_some_and(|s| s.contains("first page"))),
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
    let reason = format!("remove {old}");
    root.audit_as(reason.clone()).save(context).await?;
    assert_private(context, capture, &[&old]);
    assert_complete_private_chain(
        context,
        capture,
        observation,
        root_id,
        child_id,
        &reason,
        "remove [REDACTED]",
        RawAuditEventKind::Deleted,
    );
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
    println!(
        "TC-REQ-16 COMPLETE PRIVATE LINEAGE PASSED raw commands/SQL, safe SQL route and committed audit"
    );
    Ok(())
}
