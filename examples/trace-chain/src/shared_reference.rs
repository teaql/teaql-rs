//! Real generated Q/E/save acceptance for two mutable roots sharing one
//! immutable identity-graph reference. No generated object setters or entity
//! clones are used to compose the mutations.
use super::{
    AuditCapture, ExpectedItem, Observation, Outcome, assert_audit_graph, assert_execution_lineage,
};
use teaql_runtime::{EntityKey, EntityRuntimeState, RawAuditEventKind, UserContext};
use trace_chain_service_core::teaql_core::Entity as _;
use trace_chain_service_core::{AuditedSave as _, E, LedgerEntity as _, Q};

fn assert_read_only(state: &EntityRuntimeState) {
    assert!(state.current_change_set().changes().is_empty());
    assert!(state.new_keys().is_empty());
    assert!(state.deleted_keys().is_empty());
    assert_eq!(state.get_comment(), None);
}

pub async fn shared_reference_graphs(
    context: &UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    // The earlier normative/concurrency cases guarantee at least two complete
    // roots. Bound the query rather than resetting or replacing retained data.
    let mut rows = Q::customer_orders()
        .order_by_id_asc()
        .limit(2)
        .select_platform_with(Q::platforms().limit(1))
        .comment("what: load two orders sharing one immutable Platform")
        .purpose("why: verify read-only reference and mutation-ledger isolation")
        .execute_for_list(context)
        .await?;
    assert_eq!(rows.data.len(), 2, "two independently editable roots");
    let mut right = rows.data.pop().ok_or("second order must be loaded")?;
    let mut left = rows.data.pop().ok_or("first order must be loaded")?;
    let left_id = left.id();
    let right_id = right.id();
    let left_version = E::customer_order(&left).get_version().eval().unwrap();
    let right_version = E::customer_order(&right).get_version().eval().unwrap();
    let left_description = format!("Shared snapshot, independent graph A revision {left_version}");
    let right_description =
        format!("Shared snapshot, independent graph B revision {right_version}");
    let left_platform = E::customer_order(&left).get_platform().eval().unwrap();
    let right_platform = E::customer_order(&right).get_platform().eval().unwrap();
    assert!(
        std::ptr::eq(left_platform, right_platform),
        "this must exercise one shared snapshot, not two equal-ID objects"
    );
    let platform_id = left_platform.id();
    let platform_state = left_platform.entity_runtime_state().unwrap();
    let platform_snapshot = platform_state.original_snapshot().unwrap();
    let platform_key = EntityKey::new("Platform", platform_id);
    let platform_version = platform_state.get_original_version(&platform_key);
    let left_state = left.entity_runtime_state().unwrap();
    let right_state = right.entity_runtime_state().unwrap();
    assert_ne!(left_state, right_state, "root ledgers remain independent");
    assert_ne!(
        left_state, platform_state,
        "reference does not own this save"
    );
    assert_ne!(
        right_state, platform_state,
        "reference does not own this save"
    );
    assert_read_only(&platform_state);

    let mut expected = Vec::new();
    for (order, root_reason, child_reason, description, name) in [
        (
            &mut left,
            "revise shared-reference graph A",
            "append item in shared-reference graph A",
            left_description.as_str(),
            "Shared reference item A",
        ),
        (
            &mut right,
            "revise shared-reference graph B",
            "append item in shared-reference graph B",
            right_description.as_str(),
            "Shared reference item B",
        ),
    ] {
        order.update_description(description);
        let root_id = order.id();
        let mut child = Q::order_items()
            .comment("what: prepare a child for one shared-reference graph")
            .purpose("why: verify pending intent is composed only into its owner")
            .new_entity(context);
        child.update_customer_order_id(root_id);
        child.update_name(name);
        let child_id = child.id();
        let child = child.audit_as(child_reason).into_entity();
        order.include_pending_mutations_from(&child)?;
        let state = order.entity_runtime_state().unwrap();
        let keys: Vec<_> = state
            .current_change_set()
            .changes()
            .keys()
            .cloned()
            .collect();
        assert_eq!(
            keys,
            vec![
                EntityKey::new("CustomerOrder", root_id),
                EntityKey::new("OrderItem", child_id),
            ],
            "composition must not import the other root or the read-only Platform"
        );
        let root = ("CustomerOrder", root_id, root_reason);
        expected.extend([
            ExpectedItem {
                entity: "CustomerOrder",
                id: root_id,
                kind: RawAuditEventKind::Updated,
                reasons: vec![root],
            },
            ExpectedItem {
                entity: "OrderItem",
                id: child_id,
                kind: RawAuditEventKind::Created,
                reasons: vec![root, ("OrderItem", child_id, child_reason)],
            },
        ]);
    }
    assert_read_only(&platform_state);
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    observation.probe_concurrent_begins(true);
    let (left, right) = tokio::join!(
        left.audit_as("revise shared-reference graph A")
            .save(context),
        right
            .audit_as("revise shared-reference graph B")
            .save(context),
    );
    observation.probe_concurrent_begins(false);
    left?;
    right?;
    assert_audit_graph(&capture.events(), &expected);
    assert_execution_lineage(observation, &expected);
    assert_eq!(
        context
            .sql_logs()
            .iter()
            .filter(|entry| entry.operation.is_mutation())
            .count(),
        4,
        "only two roots and their explicitly composed children are written"
    );
    assert_read_only(&platform_state);
    assert_eq!(platform_state.original_snapshot(), Some(platform_snapshot));
    assert_eq!(
        platform_state.get_original_version(&platform_key),
        platform_version
    );
    assert!(platform_state.get_trace_chain(&platform_key).is_empty());
    assert!(
        left_state
            .get_trace_chain(&EntityKey::new("CustomerOrder", right_id))
            .is_empty()
    );
    assert!(
        right_state
            .get_trace_chain(&EntityKey::new("CustomerOrder", left_id))
            .is_empty()
    );

    for (id, old_version, description, child_id, child_name) in [
        (
            left_id,
            left_version,
            left_description.as_str(),
            expected[1].id,
            "Shared reference item A",
        ),
        (
            right_id,
            right_version,
            right_description.as_str(),
            expected[3].id,
            "Shared reference item B",
        ),
    ] {
        let order = Q::customer_orders()
            .with_id_is(id)
            .limit(1)
            .select_platform_with(Q::platforms().limit(1))
            .comment("what: reload an independently committed shared-reference order")
            .purpose("why: verify actual root version and reference identity")
            .execute_for_one(context)
            .await?
            .ok_or("committed order must exist")?;
        assert_eq!(
            E::customer_order(&order)
                .get_description()
                .eval()
                .as_deref(),
            Some(description)
        );
        assert_eq!(
            E::customer_order(&order).get_version().eval(),
            Some(old_version + 1)
        );
        assert_eq!(
            E::customer_order(&order).get_platform_id().eval(),
            Some(platform_id)
        );
        assert_eq!(
            E::customer_order(&order)
                .get_platform()
                .eval()
                .unwrap()
                .entity_runtime_state()
                .unwrap()
                .get_original_version(&platform_key),
            platform_version
        );
        let item = Q::order_items()
            .with_id_is(child_id)
            .limit(1)
            .comment("what: reload the child belonging to one shared-reference graph")
            .purpose("why: verify the committed child value and modeled owner")
            .execute_for_one(context)
            .await?
            .ok_or("committed child must exist")?;
        assert_eq!(
            E::order_item(&item).get_customer_order_id().eval(),
            Some(id)
        );
        assert_eq!(
            E::order_item(&item).get_name().eval().as_deref(),
            Some(child_name)
        );
    }
    println!(
        "TC-MUT-12 SHARED READONLY PASSED roots={left_id},{right_id} platform={platform_id}; pointer-shared snapshot, separate ledgers, overlapping saves"
    );
    Ok(())
}
