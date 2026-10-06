//! Actual generated Q/E/save proof for successful persisted-result SELECTs.
//! Observation is passive: no request reasons or SQL trace frames are injected.
use super::{
    AuditCapture, ExpectedItem, Observation, Outcome, assert_audit_graph, assert_execution_lineage,
};
use teaql_data_service::{DataServiceOperation, SqlExecutionOutcome};
use teaql_runtime::{RawAuditEventKind, SqlLogOperation, UserContext};
use trace_chain_service_core::teaql_core::{Entity as _, TraceKind};
use trace_chain_service_core::{AuditedSave as _, E, LedgerEntity as _, Q};

fn verify_physical_pairs(
    context: &UserContext,
    capture: &AuditCapture,
    observation: &Observation,
    expected: &[ExpectedItem],
) {
    assert_audit_graph(&capture.events(), expected);
    assert_execution_lineage(observation, expected);
    let metadata = observation.metadata();
    let reads: Vec<_> = metadata
        .iter()
        .filter(|statement| {
            statement.operation == DataServiceOperation::Query
                && statement.trace_chain.iter().any(|node| {
                    node.kind == TraceKind::Purpose
                        && node.comment == "verify the persisted mutation result"
                })
        })
        .collect();
    assert_eq!(
        reads.len(),
        expected.len(),
        "one real readback per changed row"
    );
    let logs = context.sql_logs();
    let read_logs: Vec<_> = logs
        .iter()
        .filter(|entry| entry.purpose.as_deref() == Some("verify the persisted mutation result"))
        .collect();
    assert_eq!(
        read_logs.len(),
        expected.len(),
        "readbacks reach the safe sink exactly once"
    );
    for item in expected {
        let physical: Vec<_> = metadata
            .iter()
            .filter(|statement| {
                statement.trace_chain.iter().any(|node| {
                    node.kind == TraceKind::Entity
                        && node.entity_type == item.entity
                        && node.entity_id == Some(item.id)
                })
            })
            .collect();
        assert_eq!(
            physical.len(),
            2,
            "write then readback for each typed identity"
        );
        assert_ne!(physical[0].operation, DataServiceOperation::Query);
        assert_eq!(physical[0].affected_rows, Some(1));
        assert_eq!(physical[1].operation, DataServiceOperation::Query);
        assert_eq!(physical[1].result_count, Some(1));
        for statement in &physical {
            assert_eq!(
                statement.sql_log.execution_outcome,
                Some(SqlExecutionOutcome::Success)
            );
            assert_eq!(statement.comment.as_deref(), Some(item.reasons[0].2));
            let actual: Vec<_> = statement
                .trace_chain
                .iter()
                .filter(|node| node.kind == TraceKind::AuditReason)
                .map(|node| {
                    (
                        node.entity_type.as_str(),
                        node.entity_id.unwrap(),
                        node.comment.as_str(),
                    )
                })
                .collect();
            assert_eq!(
                actual, item.reasons,
                "readback preserves the complete local branch"
            );
        }
        let safe = read_logs
            .iter()
            .find(|entry| Some(entry.sql.as_str()) == physical[1].parameterized_query.as_deref())
            .unwrap();
        assert_eq!(safe.operation, SqlLogOperation::Select);
        assert_eq!(safe.comment.as_deref(), Some(item.reasons[0].2));
        assert_eq!(
            safe.audit_reason.as_deref(),
            Some(item.reasons[0].2),
            "derived readback inherits request intent; physical metadata retains the full branch lineage above"
        );
        assert_eq!(
            safe.trace_path.first().unwrap().entity_type,
            "CustomerOrder"
        );
        assert_eq!(safe.trace_path.last().unwrap().entity_type, "select");
        assert!(
            safe.trace_path
                .iter()
                .any(|node| node.kind == TraceKind::Request && node.entity_type == "CustomerOrder"),
            "readback query keeps the originating request root"
        );
        assert!(safe.trace_path.iter().all(|node| !matches!(
            node.kind,
            TraceKind::Comment | TraceKind::Purpose | TraceKind::AuditReason
        )));
        // Export fixed fixture intent and safe projections, never raw binds.
        println!(
            "READBACK_OBSERVED {}",
            serde_json::json!({
                "entity": item.entity, "id": item.id,
                "write_operation": format!("{:?}", physical[0].operation),
                "read_operation": "Query", "affected_rows": physical[0].affected_rows,
                "result_count": physical[1].result_count,
                "write_sql": physical[0].parameterized_query,
                "read_sql": physical[1].parameterized_query,
                "write_outcome": format!("{:?}", physical[0].sql_log.execution_outcome.unwrap()),
                "read_outcome": format!("{:?}", physical[1].sql_log.execution_outcome.unwrap()),
                "comment": physical[1].comment,
                "lineage": physical[1].trace_chain.iter()
                    .filter(|node| node.kind == TraceKind::AuditReason)
                    .map(|node| serde_json::json!([node.entity_type, node.entity_id, node.comment]))
                    .collect::<Vec<_>>(),
                "safe_comment": safe.comment, "safe_purpose": safe.purpose,
                "safe_audit_reason": safe.audit_reason,
                "safe_path": safe.trace_path.iter()
                    .map(|node| serde_json::json!([format!("{:?}", node.kind), node.entity_type]))
                    .collect::<Vec<_>>(),
                "write_count": observation.commands().len(),
                "readback_count": reads.len(), "audit_count": capture.events().len(),
            })
        );
    }
}

pub async fn successful_graph_readback(
    context: &UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    let platform = Q::platforms()
        .limit(1)
        .comment("what: reuse the readback fixture root")
        .purpose("why: attach a generated graph without bypassing bootstrap")
        .execute_for_one(context)
        .await?
        .ok_or("seeded Platform required")?;
    let mut order = Q::customer_orders()
        .comment("what: prepare successful graph readback")
        .purpose("why: witness the exact persisted-result SQL for a graph")
        .new_entity(context);
    order.update_platform_id(platform.id());
    order.update_order_number("TRACE-SUCCESS-READBACK");
    order.update_description("Successful readback fixture");
    let order_id = order.id();
    let mut child = Q::order_items()
        .comment("what: prepare successful child readback")
        .purpose("why: check inherited root and additional local business reason")
        .new_entity(context);
    child.update_customer_order_id(order_id);
    child.update_name("Successful readback item");
    let child_id = child.id();
    let child = child
        .audit_as("authorize successful item readback")
        .into_entity();
    order.include_pending_mutations_from(&child)?;
    let root_reason = "create successful readback graph";
    let expected = [
        ExpectedItem {
            entity: "CustomerOrder",
            id: order_id,
            kind: RawAuditEventKind::Created,
            reasons: vec![("CustomerOrder", order_id, root_reason)],
        },
        ExpectedItem {
            entity: "OrderItem",
            id: child_id,
            kind: RawAuditEventKind::Created,
            reasons: vec![
                ("CustomerOrder", order_id, root_reason),
                ("OrderItem", child_id, "authorize successful item readback"),
            ],
        },
    ];
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    let saved = order.audit_as(root_reason).save(context).await?;
    verify_physical_pairs(context, capture, observation, &expected);
    assert_eq!(E::customer_order(&saved).get_version().eval(), Some(1));
    assert_eq!(
        E::customer_order(&saved)
            .get_description()
            .eval()
            .as_deref(),
        Some("Successful readback fixture")
    );

    let mut order = Q::customer_orders()
        .with_id_is(order_id)
        .limit(1)
        .comment("what: fully load successful readback root")
        .purpose("why: verify an optimistic update from the original complete state")
        .execute_for_one(context)
        .await?
        .ok_or("readback order must exist")?;
    let mut child = Q::order_items()
        .with_id_is(child_id)
        .limit(1)
        .comment("what: fully load successful readback child")
        .purpose("why: update a complete independently loaded editing target")
        .execute_for_one(context)
        .await?
        .ok_or("readback child must exist")?;
    assert_eq!(E::order_item(&child).get_version().eval(), Some(1));
    order.update_description("Updated successful readback fixture");
    child.update_name("Updated successful readback item");
    let child = child
        .audit_as("revise successful item readback")
        .into_entity();
    order.include_pending_mutations_from(&child)?;
    let root_reason = "update successful readback graph";
    let expected = [
        ExpectedItem {
            entity: "CustomerOrder",
            id: order_id,
            kind: RawAuditEventKind::Updated,
            reasons: vec![("CustomerOrder", order_id, root_reason)],
        },
        ExpectedItem {
            entity: "OrderItem",
            id: child_id,
            kind: RawAuditEventKind::Updated,
            reasons: vec![
                ("CustomerOrder", order_id, root_reason),
                ("OrderItem", child_id, "revise successful item readback"),
            ],
        },
    ];
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    let saved = order.audit_as(root_reason).save(context).await?;
    verify_physical_pairs(context, capture, observation, &expected);
    assert_eq!(E::customer_order(&saved).get_version().eval(), Some(2));
    assert_eq!(
        E::customer_order(&saved)
            .get_description()
            .eval()
            .as_deref(),
        Some("Updated successful readback fixture")
    );
    let child = Q::order_items()
        .with_id_is(child_id)
        .limit(1)
        .comment("what: reload the updated successful readback child")
        .purpose("why: independently verify persistence through generated Q and E")
        .execute_for_one(context)
        .await?
        .ok_or("updated child must exist")?;
    assert_eq!(
        E::order_item(&child).get_customer_order_id().eval(),
        Some(order_id)
    );
    assert_eq!(E::order_item(&child).get_version().eval(), Some(2));
    assert_eq!(
        E::order_item(&child).get_name().eval().as_deref(),
        Some("Updated successful readback item")
    );
    println!(
        "TC-REQ-10 SUCCESSFUL READBACK PASSED root={order_id} child={child_id}; four real writes, four readbacks, four committed audits"
    );
    Ok(())
}
