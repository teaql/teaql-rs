//! Controlled faulty ID-provider setup; all business work still uses generated
//! Q/Mutation APIs. SQLite, not the test observer, produces the constraint failure.
use super::{AuditCapture, Observation, Outcome};
use teaql_data_service::{DataServiceOperation, MutationCommand, SqlExecutionOutcome};
use teaql_runtime::{InternalIdGenerator, RuntimeError, SqlLogOperation, UserContext};
use trace_chain_service_core::teaql_core::{Entity as _, TraceKind};
use trace_chain_service_core::{AuditedSave as _, E, LedgerEntity as _, Q};

struct DuplicateId(u64);

impl InternalIdGenerator for DuplicateId {
    fn generate_id(&self, _entity: &str) -> Result<u64, RuntimeError> {
        Ok(self.0)
    }
}

pub async fn failed_graph(
    context: &mut UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    let platform = Q::platforms()
        .limit(1)
        .comment("what: reuse the failure fixture domain root")
        .purpose("why: test real SQLite rollback after graph planning")
        .execute_for_one(context)
        .await?
        .ok_or("seeded platform must exist")?;
    let payment = Q::payments()
        .limit(1)
        .comment("what: locate an existing payment identity")
        .purpose("why: inject a faulty allocator, not fabricated trace metadata")
        .execute_for_one(context)
        .await?
        .ok_or("normative fixture must have a payment")?;
    let duplicate_payment_id = payment.id();
    let original_reference = E::payment(&payment).get_reference_code().eval();
    let mut order = Q::customer_orders()
        .comment("what: prepare a new root for a failing graph")
        .purpose("why: ensure an earlier successful INSERT is rolled back")
        .new_entity(context);
    order.update_platform_id(platform.id());
    order.update_order_number("TRACE-ROLLBACK-001");
    order.update_description("A draft that must not be committed");
    let order_id = order.id();

    // Test-owned SPI fault: deliberately return an already persisted Payment ID.
    // This is installed last, after schema and normal allocation, and belongs
    // only to this short-lived example Context. No SQL or trace is injected.
    context.set_internal_id_generator(DuplicateId(duplicate_payment_id));
    let mut duplicate = Q::payments()
        .comment("what: prepare a payment with a deliberately broken allocator")
        .purpose("why: force a real provider failure after the root INSERT")
        .new_entity(context);
    duplicate.update_customer_order_id(order_id);
    duplicate.update_reference_code("TRACE-MUST-NOT-COMMIT");
    assert_eq!(duplicate.id(), duplicate_payment_id);
    let duplicate = duplicate
        .audit_as("authorize failing payment")
        .into_entity();
    order.include_pending_mutations_from(&duplicate)?;

    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    let result = order.audit_as("submit failing graph").save(context).await;
    let error = match result {
        Ok(_) => {
            return Err("faulty allocator must cause a real SQLite primary-key failure".into());
        }
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("UNIQUE constraint failed"),
        "actual provider error: {error}"
    );
    assert!(
        capture.events().is_empty(),
        "rolled-back graph must not emit committed audit events"
    );

    let commands = observation.commands();
    assert_eq!(
        commands.len(),
        2,
        "root succeeded, then Payment was attempted"
    );
    for (index, request) in commands.iter().enumerate() {
        assert_eq!(request.comment(), "submit failing graph");
        let MutationCommand::Insert(command) = &request.command else {
            panic!("failing fixture must emit INSERTs only");
        };
        assert_eq!(
            command.entity,
            if index == 0 {
                "CustomerOrder"
            } else {
                "Payment"
            }
        );
        let reasons: Vec<_> = command
            .trace_chain
            .iter()
            .filter(|node| node.kind == TraceKind::AuditReason)
            .map(|node| {
                (
                    node.entity_type.as_str(),
                    node.entity_id,
                    node.comment.as_str(),
                )
            })
            .collect();
        let mut expected = vec![("CustomerOrder", Some(order_id), "submit failing graph")];
        if index == 1 {
            expected.push((
                "Payment",
                Some(duplicate_payment_id),
                "authorize failing payment",
            ));
        }
        assert_eq!(reasons, expected);
    }
    let metadata: Vec<_> = observation
        .metadata()
        .into_iter()
        .filter(|statement| statement.operation == DataServiceOperation::Insert)
        .collect();
    assert_eq!(
        metadata.len(),
        2,
        "successful and failing physical statement metadata"
    );
    for (index, statement) in metadata.iter().enumerate() {
        let expected = if index == 0 {
            vec![("CustomerOrder", Some(order_id), "submit failing graph")]
        } else {
            vec![
                ("CustomerOrder", Some(order_id), "submit failing graph"),
                (
                    "Payment",
                    Some(duplicate_payment_id),
                    "authorize failing payment",
                ),
            ]
        };
        let reasons: Vec<_> = statement
            .trace_chain
            .iter()
            .filter(|node| node.kind == TraceKind::AuditReason)
            .map(|node| {
                (
                    node.entity_type.as_str(),
                    node.entity_id,
                    node.comment.as_str(),
                )
            })
            .collect();
        assert_eq!(
            reasons, expected,
            "physical failure metadata must retain complete branch ancestry"
        );
    }
    let logs = context.sql_logs();
    let failed = logs
        .iter()
        .find(|entry| {
            entry.operation == SqlLogOperation::Insert
                && entry.log_context.execution_outcome == Some(SqlExecutionOutcome::Failure)
        })
        .expect("actual SQLite failure must reach safe SQL sink");
    assert_eq!(
        failed.audit_reason.as_deref(),
        Some("submit failing graph"),
        "failed physical SQL retains the request root; branch ancestry is checked separately above"
    );
    assert_eq!(
        failed.trace_path.first().unwrap().entity_type,
        "CustomerOrder"
    );
    assert!(
        failed
            .trace_path
            .iter()
            .any(|node| node.kind == TraceKind::Entity && node.entity_type == "Payment")
    );
    assert!(
        logs.iter()
            .any(|entry| entry.operation == SqlLogOperation::Insert
                && entry.log_context.execution_outcome == Some(SqlExecutionOutcome::Success)),
        "root write was executed before the failing child"
    );

    let missing = Q::customer_orders()
        .with_id_is(order_id)
        .limit(1)
        .comment("what: inspect the failed root")
        .purpose("why: prove the successful root INSERT was rolled back")
        .execute_for_one(context)
        .await?;
    assert!(missing.is_none(), "no failed graph root may persist");
    let unchanged = Q::payments()
        .with_id_is(duplicate_payment_id)
        .limit(1)
        .comment("what: inspect the existing payment after rollback")
        .purpose("why: prove the failed INSERT did not change the existing row")
        .execute_for_one(context)
        .await?
        .ok_or("existing payment must survive")?;
    assert_eq!(
        E::payment(&unchanged).get_reference_code().eval(),
        original_reference
    );
    println!(
        "TC-MUT-13 PASSED real SQLite failure retains branch lineage; no committed audit; root rolled back"
    );
    Ok(())
}

pub async fn failed_readback(
    database_url: String,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    use super::{Observed, readback_transport::ReadbackFault};
    use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
    use teaql_sql::SqlDataServiceExecutor;
    use trace_chain_service_core::{LocalSchemaProvider, ServiceRuntimeConfig, service_runtime};

    let mut context = service_runtime(ServiceRuntimeConfig { database_url })
        .await?
        .with_custom_event_sink(capture.clone());
    context.ensure_schema().await?;
    let transport = context
        .require_resource::<SqliteMutationExecutor>()?
        .clone();
    // Keep generated model metadata, the standard compiler, the real SQLite
    // transaction and its lease. The fault is below SQL compilation and does
    // not create, remove or replace command/diagnostic trace frames.
    let executor = SqlDataServiceExecutor::new(
        SqliteDialect,
        ReadbackFault::new(transport),
        LocalSchemaProvider,
    );
    context.register_executor(Observed::new(executor, observation.clone()));
    let platform = Q::platforms()
        .limit(1)
        .comment("what: load the readback failure fixture root")
        .purpose("why: prepare a generated save with a transport fault")
        .execute_for_one(&context)
        .await?
        .ok_or("platform must exist")?;
    let mut order = Q::customer_orders()
        .comment("what: create an order for the readback failure probe")
        .purpose("why: verify successful write metadata survives readback failure")
        .new_entity(&context);
    order.update_platform_id(platform.id());
    order.update_order_number("TRACE-READBACK-001");
    order.update_description("Readback failure draft");
    let order_id = order.id();
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    let error = match order
        .audit_as("submit readback failure graph")
        .save(&context)
        .await
    {
        Ok(_) => return Err("readback fault must fail the generated save".into()),
        Err(error) => error,
    };
    assert!(error.to_string().contains("INJECTED_READBACK_FAILURE"));
    assert!(
        capture.events().is_empty(),
        "readback failure rolls back; audit is commit-bound"
    );
    let metadata = observation.metadata();
    let write = metadata
        .iter()
        .find(|statement| statement.operation == DataServiceOperation::Insert)
        .expect("successful physical INSERT must not disappear");
    assert_eq!(
        write.sql_log.execution_outcome,
        Some(SqlExecutionOutcome::Success)
    );
    assert_eq!(write.affected_rows, Some(1));
    let failed_read = metadata
        .iter()
        .find(|statement| {
            statement.operation == DataServiceOperation::Query
                && statement.sql_log.execution_outcome == Some(SqlExecutionOutcome::Failure)
        })
        .expect("readback has a separate failed statement record");
    for statement in [write, failed_read] {
        let reasons: Vec<_> = statement
            .trace_chain
            .iter()
            .filter(|node| node.kind == TraceKind::AuditReason)
            .map(|node| {
                (
                    node.entity_type.as_str(),
                    node.entity_id,
                    node.comment.as_str(),
                )
            })
            .collect();
        assert_eq!(
            reasons,
            [(
                "CustomerOrder",
                Some(order_id),
                "submit readback failure graph"
            )]
        );
    }
    let logs = context.sql_logs();
    let write = logs
        .iter()
        .find(|log| log.operation == SqlLogOperation::Insert)
        .expect("safe successful write record");
    assert_eq!(
        write.log_context.execution_outcome,
        Some(SqlExecutionOutcome::Success)
    );
    assert_eq!(
        write.audit_reason.as_deref(),
        Some("submit readback failure graph")
    );
    let read = logs
        .iter()
        .find(|log| {
            log.operation == SqlLogOperation::Select
                && log.log_context.execution_outcome == Some(SqlExecutionOutcome::Failure)
        })
        .expect("safe failed readback record");
    assert_eq!(
        read.comment.as_deref(),
        Some("submit readback failure graph")
    );
    assert_eq!(
        read.purpose.as_deref(),
        Some("verify the persisted mutation result")
    );
    let absent = Q::customer_orders()
        .with_id_is(order_id)
        .limit(1)
        .comment("what: inspect the readback-failed root")
        .purpose("why: verify rollback after a physically successful write")
        .execute_for_one(&context)
        .await?;
    assert!(absent.is_none());
    println!(
        "TC-MUT-14 PASSED real write success and injected readback failure remain separate; no committed audit"
    );
    Ok(())
}
