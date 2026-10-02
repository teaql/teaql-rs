use super::{
    AuditCapture, ExpectedItem, Observation, Outcome, assert_audit_graph, assert_execution_lineage,
};
use teaql_runtime::{RawAuditEventKind, UserContext};
use trace_chain_service_core::teaql_core::{Entity as _, TraceKind};
use trace_chain_service_core::{AuditedSave as _, CustomerOrder, LedgerEntity as _, Q};

pub async fn query_three_relations(
    context: &UserContext,
    observation: &Observation,
    attempt_id: u64,
) -> Outcome<()> {
    context.clear_sql_logs();
    observation.clear();
    let attempt = Q::payment_attempts()
        .with_id_is(attempt_id)
        .limit(1)
        .select_payment_with(
            Q::payments_minimal().limit(1).select_customer_order_with(
                Q::customer_orders_minimal()
                    .limit(1)
                    .select_platform_with(Q::platforms_minimal().limit(1)),
            ),
        )
        .comment("what: load three modeled relations")
        .purpose("why: verify generated relation trace propagation")
        .execute_for_one(context)
        .await?;
    assert!(attempt.is_some(), "root attempt must exist");
    let logs = context.sql_logs();
    assert_eq!(
        logs.len(),
        4,
        "root query and three actual relation queries"
    );
    let expected = ["payment", "customer_order", "platform"];
    for (depth, log) in logs.iter().enumerate() {
        assert_eq!(
            log.comment.as_deref(),
            Some("what: load three modeled relations")
        );
        assert_eq!(
            log.purpose.as_deref(),
            Some("why: verify generated relation trace propagation")
        );
        assert_eq!(
            log.trace_path.first().unwrap().entity_type,
            "PaymentAttempt"
        );
        let actual: Vec<_> = log
            .trace_path
            .iter()
            .filter(|node| node.kind == TraceKind::Relation)
            .map(|node| node.entity_type.as_str())
            .collect();
        assert_eq!(
            actual,
            expected[..depth],
            "actual generated relation route at level {depth}"
        );
        assert_eq!(log.result_count, Some(1));
    }
    let metadata = observation.metadata();
    assert_eq!(
        metadata.len(),
        4,
        "actual root and three physical relation query metadata records"
    );
    for (depth, statement) in metadata.iter().enumerate() {
        assert_eq!(
            statement.operation,
            teaql_data_service::DataServiceOperation::Query
        );
        assert_eq!(
            statement.comment.as_deref(),
            Some("what: load three modeled relations")
        );
        let relation_route: Vec<_> = statement
            .trace_chain
            .iter()
            .filter(|node| node.kind == TraceKind::Relation)
            .map(|node| node.entity_type.as_str())
            .collect();
        assert_eq!(
            relation_route,
            expected[..depth],
            "physical relation metadata at level {depth}"
        );
        let purpose = statement
            .trace_chain
            .iter()
            .rfind(|node| node.kind == TraceKind::Purpose);
        assert_eq!(
            purpose.map(|node| node.comment.as_str()),
            Some("why: verify generated relation trace propagation")
        );
        assert_eq!(statement.result_count, Some(1));
    }
    println!("TC-SQL-07 PASSED generated relation path: payment -> customer_order -> platform");
    Ok(())
}

fn prepare_graph(
    context: &UserContext,
    platform_id: u64,
    lane: &'static str,
) -> Outcome<(CustomerOrder, Vec<ExpectedItem>)> {
    let (root_reason, payment_reason, shipment_reason) = match lane {
        "A" => ("submit graph A", "authorize graph A", "dispatch graph A"),
        "B" => ("submit graph B", "authorize graph B", "dispatch graph B"),
        _ => unreachable!(),
    };
    let mut order = Q::customer_orders()
        .comment("what: prepare an independent concurrent root")
        .purpose("why: verify operation-local graph ownership")
        .new_entity(context);
    order.update_platform_id(platform_id);
    order.update_order_number(format!("TRACE-CONCURRENT-{lane}"));
    order.update_description("Concurrent draft");
    let root_id = order.id();
    let mut payment = Q::payments()
        .comment("what: prepare an independent payment branch")
        .purpose("why: verify operation-local branch ownership")
        .new_entity(context);
    payment.update_customer_order_id(root_id);
    payment.update_reference_code(format!("TRACE-CONCURRENT-PAYMENT-{lane}"));
    let payment_id = payment.id();
    let payment = payment.audit_as(payment_reason).into_entity();
    let mut attempt = Q::payment_attempts()
        .comment("what: prepare an independent grandchild")
        .purpose("why: verify inherited branch ancestry")
        .new_entity(context);
    attempt.update_payment_id(payment_id);
    attempt.update_reference_code(format!("TRACE-CONCURRENT-ATTEMPT-{lane}"));
    let attempt_id = attempt.id();
    let mut shipment = Q::shipments()
        .comment("what: prepare an independent sibling")
        .purpose("why: verify concurrent sibling isolation")
        .new_entity(context);
    shipment.update_customer_order_id(root_id);
    shipment.update_reference_code(format!("TRACE-CONCURRENT-SHIPMENT-{lane}"));
    let shipment_id = shipment.id();
    let shipment = shipment.audit_as(shipment_reason).into_entity();
    payment.include_pending_mutations_from(&attempt)?;
    order.include_pending_mutations_from(&payment)?;
    order.include_pending_mutations_from(&shipment)?;
    let root = ("CustomerOrder", root_id, root_reason);
    let child = ("Payment", payment_id, payment_reason);
    Ok((
        order.audit_as(root_reason).into_entity(),
        vec![
            ExpectedItem {
                entity: "CustomerOrder",
                id: root_id,
                kind: RawAuditEventKind::Created,
                reasons: vec![root],
            },
            ExpectedItem {
                entity: "Payment",
                id: payment_id,
                kind: RawAuditEventKind::Created,
                reasons: vec![root, child],
            },
            ExpectedItem {
                entity: "PaymentAttempt",
                id: attempt_id,
                kind: RawAuditEventKind::Created,
                reasons: vec![root, child],
            },
            ExpectedItem {
                entity: "Shipment",
                id: shipment_id,
                kind: RawAuditEventKind::Created,
                reasons: vec![root, ("Shipment", shipment_id, shipment_reason)],
            },
        ],
    ))
}

pub async fn concurrent_graphs(
    context: &UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    let platform = Q::platforms()
        .limit(1)
        .comment("what: load the concurrent fixture's shared domain root")
        .purpose("why: compose two independent graphs on one Context")
        .execute_for_one(context)
        .await?
        .ok_or("seeded platform must exist")?;
    // ID assignment is deliberately completed before the overlapping saves.
    // This proves graph concurrency, not concurrent raw synchronous allocation.
    let (left, mut expected) = prepare_graph(context, platform.id(), "A")?;
    let (right, right_expected) = prepare_graph(context, platform.id(), "B")?;
    expected.extend(right_expected);
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    observation.probe_concurrent_begins(true);
    let (left, right) = tokio::join!(
        left.audit_as("submit graph A").save(context),
        right.audit_as("submit graph B").save(context),
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
        8
    );
    println!(
        "TC-MUT-12 PASSED overlapping generated graph saves on one Context; IDs prepared before save"
    );
    Ok(())
}
