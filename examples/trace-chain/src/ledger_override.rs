//! Inspect real planner-owned ledger chains before BEGIN, then compare each
//! emitted command/SQL/audit. Never supply a trace chain to execution.
use std::sync::{Arc, Mutex};

use super::{
    AuditCapture, ExpectedItem, Observation, Outcome, assert_audit_graph, assert_execution_lineage,
};
use teaql_runtime::{EntityKey, RawAuditEventKind, UserContext};
use trace_chain_service_core::teaql_core::{Entity as _, TraceKind};
use trace_chain_service_core::{AuditedSave as _, E, LedgerEntity as _, OrderItem, Q};

async fn load_item(context: &UserContext, id: u64) -> Outcome<OrderItem> {
    Q::order_items()
        .with_id_is(id)
        .limit(1)
        .comment("what: reload the complete ledger item")
        .purpose("why: verify persisted state and preserve optimistic version")
        .execute_for_one(context)
        .await?
        .ok_or_else(|| "saved item required".into())
}

pub async fn ledger_override(
    context: &mut UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    let platform = Q::platforms()
        .limit(1)
        .comment("what: reuse the seeded ledger fixture root")
        .purpose("why: build a generated mutation graph")
        .execute_for_one(context)
        .await?
        .ok_or("seeded platform required")?;
    for logging in [false, true] {
        if logging {
            context.enable_all_sql_log();
        } else {
            context.disable_sql_log();
        }
        let mut order = Q::customer_orders()
            .comment("what: create the ledger precedence root")
            .purpose("why: test complete chains instead of appended fallback")
            .new_entity(context);
        order.update_platform_id(platform.id());
        order.update_order_number("LEDGER-OVERRIDE");
        order.update_description("Ledger precedence create");
        let root_id = order.id();
        let mut specific = Q::order_items()
            .comment("what: create the annotated ledger item")
            .purpose("why: produce a complete runtime-owned chain")
            .new_entity(context);
        specific.update_customer_order_id(root_id);
        specific.update_name("Special item create");
        let specific_id = specific.id();
        let mut inherited = Q::order_items()
            .comment("what: create the unannotated ledger sibling")
            .purpose("why: retain the root reason without sibling contamination")
            .new_entity(context);
        inherited.update_customer_order_id(root_id);
        inherited.update_name("Ordinary item create");
        let inherited_id = inherited.id();

        for (phase, root_reason, child_reason) in [
            (0, "create precedence graph", "accept special branch"),
            (1, "revise precedence graph", "revise special branch"),
            (2, "retire precedence branch", "retire special branch"),
        ] {
            if phase > 0 {
                specific = load_item(context, specific_id).await?;
                inherited = load_item(context, inherited_id).await?;
                assert_eq!(E::order_item(&specific).get_version().eval(), Some(phase));
                assert_eq!(E::order_item(&inherited).get_version().eval(), Some(phase));
                order.update_description(format!("Ledger precedence revision {phase}"));
                inherited.update_name(format!("Ordinary item revision {phase}"));
                if phase == 2 {
                    specific.mark_for_deletion();
                } else {
                    specific.update_name("Special item revision");
                }
            }
            specific = specific.audit_as(child_reason).into_entity();
            order.include_pending_mutations_from(&specific)?;
            order.include_pending_mutations_from(&inherited)?;
            let state = order.entity_runtime_state().ok_or("root state required")?;
            let keys = [
                EntityKey::new("CustomerOrder", root_id),
                EntityKey::new("OrderItem", specific_id),
                EntityKey::new("OrderItem", inherited_id),
            ];
            for key in &keys {
                assert!(
                    state.get_trace_chain(key).is_empty(),
                    "no stale planned scope"
                );
            }
            let observed_scopes = Arc::new(Mutex::new(Vec::new()));
            let planned = observed_scopes.clone();
            let planned_state = state.clone();
            observation.observe_next_begin(move || {
                *planned.lock().unwrap() = keys
                    .iter()
                    .map(|key| planned_state.get_trace_chain(key))
                    .collect();
            });
            capture.clear();
            observation.clear();
            context.clear_sql_logs();
            order = order.audit_as(root_reason).save(context).await?;
            let root = ("CustomerOrder", root_id, root_reason);
            let expected = [
                ExpectedItem {
                    entity: "CustomerOrder",
                    id: root_id,
                    kind: if phase == 0 {
                        RawAuditEventKind::Created
                    } else {
                        RawAuditEventKind::Updated
                    },
                    reasons: vec![root],
                },
                ExpectedItem {
                    entity: "OrderItem",
                    id: specific_id,
                    kind: match phase {
                        0 => RawAuditEventKind::Created,
                        1 => RawAuditEventKind::Updated,
                        _ => RawAuditEventKind::Deleted,
                    },
                    reasons: vec![root, ("OrderItem", specific_id, child_reason)],
                },
                ExpectedItem {
                    entity: "OrderItem",
                    id: inherited_id,
                    kind: if phase == 0 {
                        RawAuditEventKind::Created
                    } else {
                        RawAuditEventKind::Updated
                    },
                    reasons: vec![root],
                },
            ];
            {
                let scopes = observed_scopes.lock().unwrap();
                assert_eq!(
                    scopes.len(),
                    expected.len(),
                    "observed actual pre-BEGIN ledger"
                );
                for (scope, item) in scopes.iter().zip(&expected) {
                    let actual: Vec<_> = scope
                        .iter()
                        .map(|node| {
                            assert_eq!(node.kind, TraceKind::AuditReason);
                            (
                                node.entity_type.as_str(),
                                node.entity_id.unwrap(),
                                node.comment.as_str(),
                            )
                        })
                        .collect();
                    assert_eq!(actual, item.reasons, "complete runtime ledger scope");
                }
            }
            assert_execution_lineage(observation, &expected);
            assert_audit_graph(&capture.events(), &expected);
            for key in &[
                EntityKey::new("CustomerOrder", root_id),
                EntityKey::new("OrderItem", specific_id),
                EntityKey::new("OrderItem", inherited_id),
            ] {
                assert!(
                    state.get_trace_chain(key).is_empty(),
                    "committed scope is cleared"
                );
            }
            let logs = context.sql_logs();
            if logging {
                let writes: Vec<_> = logs
                    .iter()
                    .filter(|log| log.operation.is_mutation())
                    .collect();
                assert_eq!(writes.len(), 3);
                for log in writes {
                    assert_eq!(log.comment.as_deref(), Some(root_reason));
                    let reasons: Vec<_> = log
                        .trace_path
                        .iter()
                        .filter(|node| node.kind == TraceKind::AuditReason)
                        .collect();
                    assert!(
                        reasons.is_empty(),
                        "intent is projected separately from SQL path"
                    );
                }
            } else {
                assert!(
                    logs.is_empty(),
                    "diagnostic switch cannot disable lineage evidence"
                );
            }
            let loaded = Q::customer_orders()
                .with_id_is(root_id)
                .limit(1)
                .comment("what: verify the committed root")
                .purpose("why: trace acceptance must also preserve actual data")
                .execute_for_one(context)
                .await?
                .ok_or("saved root required")?;
            assert_eq!(
                E::customer_order(&loaded).get_version().eval(),
                Some(phase + 1)
            );
            let sibling = load_item(context, inherited_id).await?;
            assert_eq!(
                E::order_item(&sibling).get_customer_order_id().eval(),
                Some(root_id)
            );
            assert_eq!(
                E::order_item(&sibling).get_version().eval(),
                Some(phase + 1)
            );
            assert_eq!(
                E::order_item(&sibling).get_name().eval(),
                Some(if phase == 0 {
                    "Ordinary item create".to_owned()
                } else {
                    format!("Ordinary item revision {phase}")
                })
            );
            if phase == 2 {
                assert!(
                    Q::order_items()
                        .with_id_is(specific_id)
                        .limit(1)
                        .comment("what: verify deleted branch is absent")
                        .purpose("why: soft deletion must reach persistence")
                        .execute_for_one(context)
                        .await?
                        .is_none()
                );
            } else {
                let child = load_item(context, specific_id).await?;
                assert_eq!(
                    E::order_item(&child).get_customer_order_id().eval(),
                    Some(root_id)
                );
                assert_eq!(E::order_item(&child).get_version().eval(), Some(phase + 1));
                assert_eq!(
                    E::order_item(&child).get_name().eval().as_deref(),
                    Some(if phase == 0 {
                        "Special item create"
                    } else {
                        "Special item revision"
                    })
                );
            }
        }
        println!(
            "TC-MUT-10 GENERATED LEDGER PRECEDENCE PASSED logging={logging}; planner-owned complete chain replaces fallback for insert/update/delete, sibling stays root-only"
        );
    }
    context.enable_all_sql_log();
    Ok(())
}
