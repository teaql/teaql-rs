//! teaql-rs #239 / TC-MUT-09: real prepared batches, not injected trace frames.
use super::{
    AuditCapture, ExpectedItem, Observation, Outcome, assert_audit_graph, assert_execution_lineage,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use teaql_runtime::{
    RawAuditEventKind, RuntimeAttributeValue, RuntimeOperation, RuntimeTelemetry,
    RuntimeTelemetryScope, UserContext,
};
use trace_chain_service_core::teaql_core::{Entity as _, TraceKind};
use trace_chain_service_core::{AuditedSave as _, E, LedgerEntity as _, Q};

#[derive(Clone, Default)]
struct BatchWitness(Arc<Mutex<Vec<(RuntimeOperation, bool)>>>);

struct WitnessScope {
    operation: RuntimeOperation,
    witness: BatchWitness,
}

impl RuntimeTelemetry for BatchWitness {
    fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
        Box::new(WitnessScope {
            operation,
            witness: self.clone(),
        })
    }
}

impl RuntimeTelemetryScope for WitnessScope {
    fn success(&mut self, _attributes: BTreeMap<String, RuntimeAttributeValue>) {
        self.witness
            .0
            .lock()
            .unwrap()
            .push((self.operation.clone(), true));
    }

    fn failure(&mut self, _error_type: &str) {
        self.witness
            .0
            .lock()
            .unwrap()
            .push((self.operation.clone(), false));
    }
}

impl BatchWitness {
    fn clear(&self) {
        self.0.lock().unwrap().clear();
    }

    fn assert_one_prepared_batch(&self, operation: &str) {
        let events = self.0.lock().unwrap();
        let batches: Vec<_> = events
            .iter()
            .filter(|(event, _)| {
                event.family == "mutation" && event.name == format!("OrderItem.batch_{operation}")
            })
            .collect();
        assert_eq!(
            batches.len(),
            1,
            "both same-type items must enter one prepared batch: {events:#?}"
        );
        assert!(
            batches[0].1,
            "the prepared batch must complete successfully"
        );
        assert!(
            !events.iter().any(|(event, _)| {
                event.family == "mutation" && event.name == format!("OrderItem.{operation}")
            }),
            "individual EntityDataService operations must not substitute for the batch"
        );
        println!(
            "BATCH WITNESS OrderItem.batch_{operation}: one completed prepared batch, two physical statements"
        );
    }
}

fn assert_two_statement_shapes(
    observation: &Observation,
    operation: teaql_data_service::DataServiceOperation,
) {
    let statements: Vec<_> = observation
        .metadata()
        .into_iter()
        .filter(|statement| {
            statement.operation == operation
                && statement
                    .trace_chain
                    .iter()
                    .any(|node| node.kind == TraceKind::Entity && node.entity_type == "OrderItem")
        })
        .collect();
    assert_eq!(statements.len(), 2, "two physical OrderItem statements");
    let sql = statements[0]
        .parameterized_query
        .as_deref()
        .expect("actual provider SQL");
    assert_eq!(
        statements[1].parameterized_query.as_deref(),
        Some(sql),
        "same statement shape"
    );
    assert_ne!(
        statements[0].params, statements[1].params,
        "separate item binds"
    );
    assert!(
        statements
            .iter()
            .all(|statement| statement.affected_rows == Some(1))
    );
}

fn expected_items(
    order_id: u64,
    ids: [u64; 2],
    kind: RawAuditEventKind,
    root_reason: &'static str,
    reasons: [&'static str; 2],
) -> Vec<ExpectedItem> {
    let root = ("CustomerOrder", order_id, root_reason);
    let mut expected = vec![ExpectedItem {
        entity: "CustomerOrder",
        id: order_id,
        kind,
        reasons: vec![root],
    }];
    for (id, reason) in ids.into_iter().zip(reasons) {
        expected.push(ExpectedItem {
            entity: "OrderItem",
            id,
            kind,
            reasons: vec![root, ("OrderItem", id, reason)],
        });
    }
    expected
}

fn assert_safe_root_reason(context: &UserContext, root_reason: &str) {
    let actual: Vec<_> = context
        .sql_logs()
        .into_iter()
        .filter(|log| {
            log.operation.is_mutation()
                && log
                    .trace_path
                    .iter()
                    .any(|node| node.kind == TraceKind::Entity && node.entity_type == "OrderItem")
        })
        .map(|log| {
            log.audit_reason
                .expect("safe SQL retains the request-owned root reason")
        })
        .collect();
    assert_eq!(
        actual,
        [root_reason, root_reason],
        "both physical statements retain the batch request intent; command, metadata and committed audit checks retain distinct local reasons"
    );
}

pub async fn same_type_batch(
    context: &mut UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    let witness = BatchWitness::default();
    let previous_telemetry = context.runtime_telemetry().clone();
    context.set_runtime_telemetry(Arc::new(witness.clone()));
    let platform = Q::platforms()
        .limit(1)
        .comment("what: reuse the batch fixture domain root")
        .purpose("why: persist two same-type children in one graph")
        .execute_for_one(context)
        .await?
        .ok_or("platform must be seeded")?;
    let mut order = Q::customer_orders()
        .comment("what: prepare a same-type batch root")
        .purpose("why: observe per-item lineage through grouping")
        .new_entity(context);
    order.update_platform_id(platform.id());
    order.update_order_number("TRACE-SAME-TYPE-BATCH");
    order.update_description("Same-type batch draft");
    let order_id = order.id();
    let create_reasons = ["add first batched item", "add second batched item"];
    let mut first = Q::order_items()
        .comment("what: prepare the first batched item")
        .purpose("why: retain a distinct local create reason")
        .new_entity(context);
    first.update_customer_order_id(order_id);
    first.update_name("First batch item");
    let first_id = first.id();
    let first = first.audit_as(create_reasons[0]).into_entity();
    let mut second = Q::order_items()
        .comment("what: prepare the second batched item")
        .purpose("why: retain another local create reason")
        .new_entity(context);
    second.update_customer_order_id(order_id);
    second.update_name("Second batch item");
    let second_id = second.id();
    let second = second.audit_as(create_reasons[1]).into_entity();
    // Reverse inclusion order: the planner's item index, not call order, must
    // keep each trace paired with its typed entity identity.
    order.include_pending_mutations_from(&second)?;
    order.include_pending_mutations_from(&first)?;
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    witness.clear();
    order.audit_as("create batched items").save(context).await?;
    let ids = [first_id, second_id];
    let expected = expected_items(
        order_id,
        ids,
        RawAuditEventKind::Created,
        "create batched items",
        create_reasons,
    );
    assert_audit_graph(&capture.events(), &expected);
    assert_execution_lineage(observation, &expected);
    witness.assert_one_prepared_batch("insert");
    assert_two_statement_shapes(
        observation,
        teaql_data_service::DataServiceOperation::Insert,
    );
    assert_safe_root_reason(context, "create batched items");

    // Fully load each editing target through the current generated Q API.
    let mut order = Q::customer_orders()
        .with_id_is(order_id)
        .limit(1)
        .comment("what: load the complete batch root for modification")
        .purpose("why: validate complete business state before saving")
        .execute_for_one(context)
        .await?
        .ok_or("batch root must exist")?;
    let mut first = Q::order_items()
        .with_id_is(first_id)
        .limit(1)
        .comment("what: load the complete first batched item")
        .purpose("why: modify the first item with its own reason")
        .execute_for_one(context)
        .await?
        .ok_or("first item must exist")?;
    let mut second = Q::order_items()
        .with_id_is(second_id)
        .limit(1)
        .comment("what: load the complete second batched item")
        .purpose("why: modify the second item with its own reason")
        .execute_for_one(context)
        .await?
        .ok_or("second item must exist")?;
    let versions = [
        E::order_item(&first).get_version().eval().unwrap(),
        E::order_item(&second).get_version().eval().unwrap(),
    ];
    let update_reasons = ["revise first batched item", "revise second batched item"];
    order.update_description("Same-type batch submitted");
    first.update_name("First revised batch item");
    second.update_name("Second revised batch item");
    let first = first.audit_as(update_reasons[0]).into_entity();
    let second = second.audit_as(update_reasons[1]).into_entity();
    order.include_pending_mutations_from(&second)?;
    order.include_pending_mutations_from(&first)?;
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    witness.clear();
    order.audit_as("update batched items").save(context).await?;
    let expected = expected_items(
        order_id,
        ids,
        RawAuditEventKind::Updated,
        "update batched items",
        update_reasons,
    );
    assert_audit_graph(&capture.events(), &expected);
    assert_execution_lineage(observation, &expected);
    witness.assert_one_prepared_batch("update");
    assert_two_statement_shapes(
        observation,
        teaql_data_service::DataServiceOperation::Update,
    );
    assert_safe_root_reason(context, "update batched items");

    let order = Q::customer_orders()
        .with_id_is(order_id)
        .limit(1)
        .select_order_item_list_with(Q::order_items().limit(2).order_by_name_asc())
        .comment("what: reload the committed batched item graph")
        .purpose("why: verify both updates and optimistic versions with Q and E")
        .execute_for_one(context)
        .await?
        .ok_or("committed batch root must exist")?;
    assert_eq!(
        E::customer_order(&order)
            .get_description()
            .eval()
            .as_deref(),
        Some("Same-type batch submitted")
    );
    let relation = order.order_item_list();
    let items = relation.value().ok_or("items must be loaded")?;
    assert_eq!(items.len(), 2);
    for ((item, id), (name, version)) in items.iter().zip(ids).zip(
        ["First revised batch item", "Second revised batch item"]
            .into_iter()
            .zip(versions),
    ) {
        assert_eq!(E::order_item(item).get_id().eval(), Some(id));
        assert_eq!(E::order_item(item).get_name().eval().as_deref(), Some(name));
        assert_eq!(E::order_item(item).get_version().eval(), Some(version + 1));
    }
    context.set_runtime_telemetry(previous_telemetry);
    println!(
        "TC-MUT-09 PASSED generated same-type insert/update batches: item IDs {ids:?}, distinct reasons at command/SQL/audit, Q/E readback"
    );
    Ok(())
}
