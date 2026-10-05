//! teaql-rs #239; generated APIs discovered through retained current Assist.
//! Expected chains are test assertions only, never inputs to the runtime.
use std::sync::{Arc, Mutex};
mod batching;
mod checker_overlap;
mod database_ids;
mod failure;
mod graph_privacy;
mod graph_privacy_failure;
mod ledger_override;
mod observation;
mod paging;
mod readback_transport;
mod scenarios;
mod shared_reference;
mod streaming;
mod successful_readback;
use observation::{Observation, Observed};
use teaql_runtime::{
    RawAuditEventKind, RuntimeError, SafeAuditEvent, SafeAuditEventSink, SqlLogOperation,
    UserContext,
};
use trace_chain_service_core::teaql_core::{Entity as _, TraceKind};
use trace_chain_service_core::{
    AuditedSave as _, E, LedgerEntity as _, Q, ServiceRuntimeConfig, ServiceRuntimeExecutor,
    service_runtime,
};

type Outcome<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Default)]
struct AuditCapture(Arc<Mutex<Vec<SafeAuditEvent>>>);

impl SafeAuditEventSink for AuditCapture {
    fn on_safe_event(
        &self,
        _context: &UserContext,
        event: &SafeAuditEvent,
    ) -> Result<(), RuntimeError> {
        self.0
            .lock()
            .expect("audit capture lock")
            .push(event.clone());
        Ok(())
    }
}

impl AuditCapture {
    fn clear(&self) {
        self.0.lock().expect("audit capture lock").clear();
    }

    fn events(&self) -> Vec<SafeAuditEvent> {
        self.0.lock().expect("audit capture lock").clone()
    }
}

#[derive(Debug)]
struct ExpectedItem {
    entity: &'static str,
    id: u64,
    kind: RawAuditEventKind,
    reasons: Vec<(&'static str, u64, &'static str)>,
}

fn assert_audit_graph(events: &[SafeAuditEvent], expected: &[ExpectedItem]) {
    assert_eq!(
        events.len(),
        expected.len(),
        "one committed event per changed entity: {events:#?}"
    );
    for item in expected {
        let matches: Vec<_> = events
            .iter()
            .filter(|event| {
                event.entity == item.entity
                    && event.kind == item.kind
                    && event.entity_id == Some(item.id)
            })
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "typed entity identity {item:?}: {events:#?}"
        );
        let event = matches[0];
        assert_eq!(event.kind, item.kind, "operation for {item:?}");
        let actual: Vec<_> = event
            .trace_chain
            .iter()
            .map(|node| {
                assert_eq!(node.kind, TraceKind::AuditReason, "typed lineage {item:?}");
                (
                    node.entity_type.as_str(),
                    node.entity_id.expect("assigned ID at audit sink"),
                    node.comment.as_str(),
                )
            })
            .collect();
        assert_eq!(actual, item.reasons, "root-to-leaf lineage for {item:?}");
        println!(
            "AUDIT {:?} {}#{} {actual:?}",
            event.kind, item.entity, item.id
        );
    }
}

fn assert_execution_lineage(observation: &Observation, expected: &[ExpectedItem]) {
    use teaql_data_service::{DataServiceOperation, MutationCommand};
    let commands = observation.commands();
    assert_eq!(
        commands.len(),
        expected.len(),
        "actual emitted command count"
    );
    let metadata: Vec<_> = observation
        .metadata()
        .into_iter()
        .filter(|statement| statement.operation != DataServiceOperation::Query)
        .collect();
    assert_eq!(
        metadata.len(),
        expected.len(),
        "actual physical metadata count"
    );
    for item in expected {
        let commands: Vec<_> = commands
            .iter()
            .filter(|request| {
                let (entity, id) = match &request.command {
                    MutationCommand::Insert(command) => (
                        &command.entity,
                        command.values.get("id").and_then(|id| id.try_u64()),
                    ),
                    MutationCommand::Update(command) => (&command.entity, command.id.try_u64()),
                    MutationCommand::Delete(command) => (&command.entity, command.id.try_u64()),
                    MutationCommand::Recover(command) => (&command.entity, command.id.try_u64()),
                    MutationCommand::Batch(_) => unreachable!("observer flattens containers"),
                };
                entity == item.entity && id == Some(item.id)
            })
            .collect();
        assert_eq!(commands.len(), 1, "emitted command identifies {item:?}");
        let request = commands[0];
        assert_eq!(
            request
                .trace_chain()
                .iter()
                .filter(|node| node.kind == TraceKind::AuditReason)
                .count(),
            item.reasons.len(),
            "complete ledger chain must replace, not append to, fallback: {item:?}"
        );
        assert_eq!(
            request.comment(),
            item.reasons[0].2,
            "explicit owned root intent"
        );
        let actual: Vec<_> = request
            .trace_chain()
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
        assert_eq!(actual, item.reasons, "emitted command lineage for {item:?}");
        let statements: Vec<_> = metadata
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
            statements.len(),
            1,
            "physical SQL metadata identifies {item:?}: {metadata:#?}"
        );
        let actual: Vec<_> = statements[0]
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
            "physical SQL metadata lineage for {item:?}"
        );
        println!(
            "COMMAND + SQL METADATA {}#{} {actual:?}",
            item.entity, item.id
        );
    }
}

async fn normative_graph(
    context: &UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    let platform = Q::platforms()
        .limit(1)
        .comment("what: reuse the seeded trace fixture root")
        .purpose("why: attach the normative order graph")
        .execute_for_one(context)
        .await?
        .ok_or("platform must be seeded")?;

    // TC-MUT-08 is a deliberate fixture condition, not an accidental property
    // of fresh databases. Reserve unused IDs through the generated allocator
    // until both independent type sequences reach the same number. Discarded
    // entities are never saved; gaps do not imply abandoned business rows.
    let new_order = || {
        Q::customer_orders()
            .comment("what: create the normative order")
            .purpose("why: prepare the mixed mutation fixture")
            .new_entity(context)
    };
    let new_payment = || {
        Q::payments()
            .comment("what: create a pending payment")
            .purpose("why: prepare a nested mutation branch")
            .new_entity(context)
    };
    let mut order = new_order();
    let mut payment = new_payment();
    for _ in 0..512 {
        match order.id().cmp(&payment.id()) {
            std::cmp::Ordering::Equal => break,
            std::cmp::Ordering::Less => order = new_order(),
            std::cmp::Ordering::Greater => payment = new_payment(),
        }
    }
    assert_eq!(
        order.id(),
        payment.id(),
        "bounded fixture must align type-specific IDs"
    );
    order.update_platform_id(platform.id());
    order.update_order_number("TRACE-ORDER-001");
    order.update_description("Draft order");
    let order_id = order.id();
    order
        .audit_as("initialize normative fixture")
        .save(context)
        .await?;

    let mut available = Q::order_items()
        .comment("what: create an available item")
        .purpose("why: prepare an existing child update")
        .new_entity(context);
    available.update_customer_order_id(order_id);
    available.update_name("Available item");
    let available_id = available.id();
    available
        .audit_as("initialize normative fixture")
        .save(context)
        .await?;

    let mut removable = Q::order_items()
        .comment("what: create an unavailable item")
        .purpose("why: prepare an existing child deletion")
        .new_entity(context);
    removable.update_customer_order_id(order_id);
    removable.update_name("Unavailable item");
    let removable_id = removable.id();
    removable
        .audit_as("initialize normative fixture")
        .save(context)
        .await?;

    payment.update_customer_order_id(order_id);
    payment.update_reference_code("TRACE-PAYMENT-001");
    let payment_id = payment.id();
    payment
        .audit_as("initialize normative fixture")
        .save(context)
        .await?;

    // Every editing target is independently fully loaded, then explicitly composed.
    let mut order = Q::customer_orders()
        .with_id_is(order_id)
        .limit(1)
        .comment("what: fully load the order for submission")
        .purpose("why: validate the whole editing target")
        .execute_for_one(context)
        .await?
        .ok_or("order must exist")?;
    let mut available = Q::order_items()
        .with_id_is(available_id)
        .limit(1)
        .comment("what: fully load the available item")
        .purpose("why: prepare a child update")
        .execute_for_one(context)
        .await?
        .ok_or("available item must exist")?;
    let mut removable = Q::order_items()
        .with_id_is(removable_id)
        .limit(1)
        .comment("what: fully load the unavailable item")
        .purpose("why: prepare a versioned child deletion")
        .execute_for_one(context)
        .await?
        .ok_or("unavailable item must exist")?;
    let mut payment = Q::payments()
        .with_id_is(payment_id)
        .limit(1)
        .comment("what: fully load the pending payment")
        .purpose("why: prepare the authorization branch")
        .execute_for_one(context)
        .await?
        .ok_or("payment must exist")?;

    order.update_description("Submitted order");
    available.update_name("Confirmed item");
    payment.update_reference_code("TRACE-PAYMENT-AUTHORIZED");
    let payment = payment.audit_as("authorize payment").into_entity();
    removable.mark_for_deletion();
    let removable = removable.audit_as("remove unavailable item").into_entity();

    let mut attempt = Q::payment_attempts()
        .comment("what: prepare a payment attempt")
        .purpose("why: compose a new grandchild without an extra save")
        .new_entity(context);
    attempt.update_payment_id(payment_id);
    attempt.update_reference_code("TRACE-ATTEMPT-001");
    let attempt_id = attempt.id();

    let mut shipment = Q::shipments()
        .comment("what: prepare a shipment")
        .purpose("why: compose an independent sibling branch")
        .new_entity(context);
    shipment.update_customer_order_id(order_id);
    shipment.update_reference_code("TRACE-SHIPMENT-001");
    let shipment_id = shipment.id();
    let shipment = shipment.audit_as("dispatch shipment").into_entity();

    payment.include_pending_mutations_from(&attempt)?;
    order.include_pending_mutations_from(&payment)?;
    order.include_pending_mutations_from(&available)?;
    order.include_pending_mutations_from(&shipment)?;
    order.include_pending_mutations_from(&removable)?;

    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    // Only the audited root is saved; all six outcomes belong to this transaction.
    order.audit_as("submit order").save(context).await?;

    let root_reason = ("CustomerOrder", order_id, "submit order");
    let expected = [
        ExpectedItem {
            entity: "CustomerOrder",
            id: order_id,
            kind: RawAuditEventKind::Updated,
            reasons: vec![root_reason],
        },
        ExpectedItem {
            entity: "OrderItem",
            id: available_id,
            kind: RawAuditEventKind::Updated,
            reasons: vec![root_reason],
        },
        ExpectedItem {
            entity: "Payment",
            id: payment_id,
            kind: RawAuditEventKind::Updated,
            reasons: vec![root_reason, ("Payment", payment_id, "authorize payment")],
        },
        ExpectedItem {
            entity: "PaymentAttempt",
            id: attempt_id,
            kind: RawAuditEventKind::Created,
            reasons: vec![root_reason, ("Payment", payment_id, "authorize payment")],
        },
        ExpectedItem {
            entity: "Shipment",
            id: shipment_id,
            kind: RawAuditEventKind::Created,
            reasons: vec![root_reason, ("Shipment", shipment_id, "dispatch shipment")],
        },
        ExpectedItem {
            entity: "OrderItem",
            id: removable_id,
            kind: RawAuditEventKind::Deleted,
            reasons: vec![
                root_reason,
                ("OrderItem", removable_id, "remove unavailable item"),
            ],
        },
    ];
    assert_audit_graph(&capture.events(), &expected);
    assert_execution_lineage(observation, &expected);
    assert_eq!(
        order_id, payment_id,
        "same numeric identity must be exercised in the saved graph"
    );
    println!(
        "TC-MUT-08 GENERATED SAME ID PASSED order={order_id} payment={payment_id}; distinct command/SQL/audit targets"
    );
    let mutation_logs: Vec<_> = context
        .sql_logs()
        .into_iter()
        .filter(|log| log.operation.is_mutation())
        .collect();
    assert_eq!(
        mutation_logs.len(),
        6,
        "one physical write per mutation item"
    );
    for log in &mutation_logs {
        let node = log
            .trace_path
            .iter()
            .find(|node| node.kind == TraceKind::Entity)
            .expect("canonical SQL path has a typed entity frame");
        // The existing v1 canonical path intentionally omits IDs when rebuilt.
        // Assigned identity is asserted above at command and physical metadata
        // boundaries, not invented for this diagnostic path projection.
        assert_eq!(node.entity_id, None, "canonical v1 rebuild contract");
        let item = expected
            .iter()
            .find(|item| {
                item.entity == node.entity_type
                    && match item.kind {
                        RawAuditEventKind::Created => log.operation == SqlLogOperation::Insert,
                        RawAuditEventKind::Updated => log.operation == SqlLogOperation::Update,
                        RawAuditEventKind::Deleted => log.operation == SqlLogOperation::Delete,
                        _ => false,
                    }
            })
            .expect("canonical SQL path identifies the actual entity type and operation");
        assert_eq!(
            log.audit_reason.as_deref(),
            Some(root_reason.2),
            "SQL intent belongs to the root request; local reasons remain in the independently checked lineage"
        );
        assert_eq!(log.trace_path.first().unwrap().entity_type, "CustomerOrder");
        assert_eq!(log.trace_path.last().unwrap().kind, TraceKind::Sql);
        assert_eq!(log.affected_rows, Some(1));
        let operation = match item.kind {
            RawAuditEventKind::Created => SqlLogOperation::Insert,
            RawAuditEventKind::Updated => SqlLogOperation::Update,
            RawAuditEventKind::Deleted => SqlLogOperation::Delete,
            _ => unreachable!(),
        };
        assert_eq!(log.operation, operation);
    }

    let order = Q::customer_orders()
        .with_id_is(order_id)
        .limit(1)
        .select_order_item_list_with(Q::order_items_minimal().limit(10))
        .select_payment_list_with(
            Q::payments_minimal()
                .limit(10)
                .select_payment_attempt_list_with(Q::payment_attempts_minimal().limit(10)),
        )
        .select_shipment_list_with(Q::shipments_minimal().limit(10))
        .comment("what: query the committed normative graph")
        .purpose("why: verify Q loading and E traversal after mixed save")
        .execute_for_one(context)
        .await?
        .ok_or("committed order must exist")?;
    assert_eq!(
        E::customer_order(&order)
            .get_description()
            .eval()
            .as_deref(),
        Some("Submitted order")
    );
    assert_eq!(
        E::customer_order(&order)
            .get_order_item_list()
            .size()
            .eval(),
        Some(1)
    );
    assert_eq!(
        E::customer_order(&order).get_payment_list().size().eval(),
        Some(1)
    );
    assert_eq!(
        E::customer_order(&order).get_shipment_list().size().eval(),
        Some(1)
    );
    let payments = order.payment_list();
    let payment = payments
        .value()
        .ok_or("payment list must be loaded")?
        .first()
        .ok_or("payment must be loaded")?;
    assert_eq!(
        payment
            .payment_attempt_list()
            .value()
            .ok_or("attempt list must be loaded")?
            .len(),
        1
    );
    assert_eq!(
        E::customer_order(&order)
            .get_order_item_list()
            .first()
            .get_id()
            .eval(),
        Some(available_id)
    );
    println!(
        "TC-MUT-15 PASSED order={order_id} payment={payment_id} attempt={attempt_id} shipment={shipment_id}"
    );
    scenarios::query_three_relations(context, observation, attempt_id).await?;

    // An unannotated child's deletion inherits only the root responsibility.
    // The committed safe event must nevertheless identify the deleted child.
    let mut available = Q::order_items()
        .with_id_is(available_id)
        .limit(1)
        .comment("what: load the child for inherited-reason deletion")
        .purpose("why: distinguish target identity from responsibility lineage")
        .execute_for_one(context)
        .await?
        .ok_or("available item must exist before deletion")?;
    available.mark_for_deletion();
    let order = Q::customer_orders()
        .with_id_is(order_id)
        .limit(1)
        .comment("what: load the clean parent for inherited-reason deletion")
        .purpose("why: compose only the fully loaded child's pending mutation")
        .execute_for_one(context)
        .await?
        .ok_or("parent must exist before child deletion")?;
    order.include_pending_mutations_from(&available)?;
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    order
        .audit_as("delete inherited child")
        .save(context)
        .await?;
    let deleted = [ExpectedItem {
        entity: "OrderItem",
        id: available_id,
        kind: RawAuditEventKind::Deleted,
        reasons: vec![("CustomerOrder", order_id, "delete inherited child")],
    }];
    assert_audit_graph(&capture.events(), &deleted);
    assert_execution_lineage(observation, &deleted);
    assert!(
        Q::order_items()
            .with_id_is(available_id)
            .limit(1)
            .comment("what: verify inherited-reason deletion")
            .purpose("why: ensure the identified child is no longer visible")
            .execute_for_one(context)
            .await?
            .is_none()
    );
    println!(
        "TC-MUT-15 SAFE TARGET PASSED unannotated child deletion retains independent typed identity"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Outcome<()> {
    let database_url = std::env::var("TEAQL_TRACE_CHAIN_DATABASE")?;
    if std::env::var("TEAQL_TRACE_CHAIN_SCENARIO").as_deref() == Ok("loaded-privacy-rollback") {
        return graph_privacy_failure::rollback_and_retry(database_url).await;
    }
    let capture = AuditCapture::default();
    let observation = Observation::default();
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: database_url.clone(),
    })
    .await?
    .with_custom_event_sink(capture.clone());
    // Framework-owned SPI observer: wrap the exact generated executor rather
    // than substitute another provider or create fabricated execution metadata.
    let executor = context
        .require_resource::<ServiceRuntimeExecutor>()?
        .clone()
        .with_query_metadata_observer(observation.query_metadata_observer());
    // Generated Q keeps its original typed executor binding, while save uses
    // the operation-owned delegating wrapper. Both observe the same provider.
    context.insert_resource(executor.clone());
    context.register_executor(Observed::new(executor, observation.clone()));
    context.ensure_schema().await?;
    let initial_bootstrap_metadata = observation.metadata();
    let initial_bootstrap_commands = observation.commands();
    let initial_bootstrap_audits = capture.events();
    observation.clear();
    capture.clear();
    context.clear_sql_logs();
    context.ensure_schema().await?;
    let repeated = observation.metadata();
    assert!(
        !initial_bootstrap_metadata.is_empty(),
        "observe real generated bootstrap SQL"
    );
    assert!(!repeated.is_empty(), "observe repeated bootstrap lookups");
    assert!(observation.commands().is_empty(), "no repeated seed writes");
    assert!(
        capture.events().iter().all(|event| !matches!(
            event.kind,
            RawAuditEventKind::Created
                | RawAuditEventKind::Updated
                | RawAuditEventKind::Deleted
                | RawAuditEventKind::Recovered
        )),
        "no repeated committed data mutation"
    );
    let first_lookup = &initial_bootstrap_metadata[0];
    for fact in &initial_bootstrap_metadata {
        assert!(
            fact.comment
                .as_deref()
                .is_some_and(|comment| !comment.trim().is_empty()),
            "all bootstrap statements own nonblank intent"
        );
    }
    for fact in &repeated {
        assert_eq!(
            fact.operation,
            teaql_data_service::DataServiceOperation::Query
        );
        assert_eq!(
            fact.comment, first_lookup.comment,
            "generated bootstrap lookup intent is stable"
        );
        assert!(
            fact.trace_chain
                .iter()
                .any(|node| node.kind == TraceKind::Purpose && !node.comment.trim().is_empty())
        );
    }
    let data_audits: Vec<_> = initial_bootstrap_audits
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                RawAuditEventKind::Created | RawAuditEventKind::Updated
            )
        })
        .collect();
    assert_eq!(initial_bootstrap_commands.len(), data_audits.len());
    for event in data_audits {
        let identity = event
            .bootstrap_audit
            .as_ref()
            .expect("generated bootstrap audit attribution");
        assert_eq!(identity.actor, "teaql-generated-bootstrap");
        assert_eq!(identity.category, "runtime-bootstrap");
        assert!(!identity.reason.trim().is_empty());
        assert_eq!(event.trace_chain.len(), 1);
        assert_eq!(event.trace_chain[0].kind, TraceKind::AuditReason);
        assert_eq!(event.trace_chain[0].entity_type, "Platform");
        assert_eq!(event.trace_chain[0].entity_id, Some(1));
    }
    for command in &initial_bootstrap_commands {
        assert!(!command.comment().trim().is_empty());
        let reasons: Vec<_> = command
            .trace_chain()
            .iter()
            .filter(|node| node.kind == TraceKind::AuditReason)
            .collect();
        assert_eq!(reasons.len(), 1);
        assert_eq!(reasons[0].comment, command.comment());
        assert_eq!(reasons[0].entity_type, "Platform");
        assert_eq!(reasons[0].entity_id, Some(1));
    }
    println!(
        "TC-REQ-09 RUST GENERATED BOOTSTRAP PASSED first_writes={} repeat_writes=0 comment={}",
        initial_bootstrap_commands.len(),
        first_lookup.comment.as_deref().unwrap()
    );
    observation.clear();
    capture.clear();
    context.clear_sql_logs();
    if std::env::var("TEAQL_TRACE_CHAIN_SCENARIO").as_deref() == Ok("checker-overlap") {
        return checker_overlap::checker_overlap(&mut context, &capture, &observation).await;
    }
    if std::env::var("TEAQL_TRACE_CHAIN_SCENARIO").as_deref() == Ok("ledger-override") {
        return ledger_override::ledger_override(&mut context, &capture, &observation).await;
    }
    graph_privacy::loaded_graph_privacy(&context, &capture, &observation).await?;
    normative_graph(&context, &capture, &observation).await?;
    batching::same_type_batch(&mut context, &capture, &observation).await?;
    scenarios::concurrent_graphs(&context, &capture, &observation).await?;
    // The following failure fixture deliberately installs a broken allocator
    // for this Context's final use; normal allocation probes must precede it.
    shared_reference::shared_reference_graphs(&context, &capture, &observation).await?;
    checker_overlap::checker_overlap(&mut context, &capture, &observation).await?;
    ledger_override::ledger_override(&mut context, &capture, &observation).await?;
    successful_readback::successful_graph_readback(&context, &capture, &observation).await?;
    streaming::scalar_streams(&context, &capture, &observation).await?;
    paging::paged_graph(&context, &capture, &observation).await?;
    failure::failed_graph(&mut context, &capture, &observation).await?;
    failure::failed_readback(database_url.clone(), &capture, &observation).await?;
    graph_privacy_failure::rollback_and_retry(database_url.clone()).await?;
    database_ids::generated_database_ids(database_url).await
}
