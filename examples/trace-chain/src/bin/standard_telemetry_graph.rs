//! Generated graph mutation, independent audit and loaded Q/E; no SDK setup.
use std::sync::{Arc, Mutex};
use teaql_runtime::{
    RawAuditEventKind, RuntimeError, SafeAuditEvent, SafeAuditEventSink, UserContext,
};
use trace_chain_service_core::teaql_core::{Entity as _, TraceKind};
use trace_chain_service_core::{
    AuditedSave as _, E, LedgerEntity as _, Q, ServiceRuntimeConfig, service_runtime,
};

#[derive(Clone, Default)]
struct AuditCapture(Arc<Mutex<Vec<SafeAuditEvent>>>);
impl SafeAuditEventSink for AuditCapture {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.0.lock().unwrap().push(event.clone());
        Ok(())
    }
}
impl AuditCapture {
    fn committed(&self) -> Vec<SafeAuditEvent> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    RawAuditEventKind::Created
                        | RawAuditEventKind::Updated
                        | RawAuditEventKind::Deleted
                        | RawAuditEventKind::Recovered
                )
            })
            .cloned()
            .collect()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "teaql-standard-graph-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&directory)?;
    let audit = AuditCapture::default();
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: format!("sqlite://{}?mode=rwc", directory.join("graph.db").display()),
    })
    .await?
    .with_custom_event_sink(audit.clone());
    context.disable_sql_log();
    context.ensure_schema().await?;
    context.ensure_schema().await?;
    assert_eq!(
        audit.committed().len(),
        1,
        "repeat bootstrap must not repeat audit"
    );
    audit.0.lock().unwrap().clear();

    let mut order = Q::customer_orders()
        .comment("prepare standard graph order")
        .purpose("prepare governed graph fixture")
        .new_entity(&context);
    order.update_platform_id(1);
    order.update_order_number("STANDARD-GRAPH-ORDER");
    order.update_description("governed graph fixture");
    let order_id = order.id();
    let mut payment = Q::payments()
        .comment("prepare standard graph payment")
        .purpose("prepare governed graph fixture")
        .new_entity(&context);
    payment.update_customer_order_id(order_id);
    payment.update_reference_code("STANDARD-GRAPH-PAYMENT");
    let payment_id = payment.id();
    let payment = payment
        .audit_as("authorize standard graph payment")
        .into_entity();
    let mut attempt = Q::payment_attempts()
        .comment("prepare standard graph attempt")
        .purpose("prepare governed graph fixture")
        .new_entity(&context);
    attempt.update_payment_id(payment_id);
    attempt.update_reference_code("STANDARD-GRAPH-ATTEMPT");
    let attempt_id = attempt.id();
    payment.include_pending_mutations_from(&attempt)?;
    order.include_pending_mutations_from(&payment)?;
    let saved = order
        .audit_as("submit standard order graph")
        .save(&context)
        .await?;
    assert_eq!(saved.id(), order_id);
    assert_eq!(
        E::customer_order(&saved)
            .get_order_number()
            .eval()
            .as_deref(),
        Some("STANDARD-GRAPH-ORDER")
    );
    let committed = audit.committed();
    assert_eq!(committed.len(), 3);
    for (entity, id, reasons) in [
        (
            "CustomerOrder",
            order_id,
            vec!["submit standard order graph"],
        ),
        (
            "Payment",
            payment_id,
            vec![
                "submit standard order graph",
                "authorize standard graph payment",
            ],
        ),
        (
            "PaymentAttempt",
            attempt_id,
            vec![
                "submit standard order graph",
                "authorize standard graph payment",
            ],
        ),
    ] {
        let event = committed
            .iter()
            .find(|event| event.entity == entity && event.entity_id == Some(id))
            .expect("independent committed target");
        assert!(matches!(event.kind, RawAuditEventKind::Created));
        assert_eq!(
            event
                .trace_chain
                .iter()
                .filter(|node| node.kind == TraceKind::AuditReason)
                .map(|node| node.comment.as_str())
                .collect::<Vec<_>>(),
            reasons
        );
    }
    let reloaded = Q::payment_attempts()
        .with_id_is(attempt_id)
        .limit(1)
        .select_payment_with(
            Q::payments()
                .limit(1)
                .select_customer_order_with(
                    Q::customer_orders()
                        .limit(1)
                        .comment("load standard graph order"),
                )
                .comment("load standard graph payment"),
        )
        .comment("reload standard mutation graph")
        .purpose("verify standard graph persistence")
        .execute_for_one(&context)
        .await?
        .expect("saved attempt");
    assert_eq!(
        E::payment_attempt(&reloaded)
            .get_reference_code()
            .eval()
            .as_deref(),
        Some("STANDARD-GRAPH-ATTEMPT")
    );
    assert_eq!(
        E::payment_attempt(&reloaded)
            .get_payment()
            .get_reference_code()
            .eval()
            .as_deref(),
        Some("STANDARD-GRAPH-PAYMENT")
    );
    assert_eq!(
        E::payment_attempt(&reloaded)
            .get_payment()
            .get_customer_order()
            .get_order_number()
            .eval()
            .as_deref(),
        Some("STANDARD-GRAPH-ORDER")
    );
    assert_eq!(
        audit.committed().len(),
        3,
        "Q/E readback must not create business audit"
    );
    assert!(context.sql_logs().is_empty());
    if std::env::args().any(|arg| arg == "--queue-pressure") {
        for _ in 0..1500 {
            let row = Q::payment_attempts()
                .with_id_is(attempt_id)
                .limit(1)
                .comment("probe bounded telemetry queue")
                .purpose("verify queue pressure preserves typed reads")
                .execute_for_one(&context)
                .await?
                .expect("pressure read");
            assert_eq!(
                E::payment_attempt(&row)
                    .get_reference_code()
                    .eval()
                    .as_deref(),
                Some("STANDARD-GRAPH-ATTEMPT")
            );
        }
        assert_eq!(audit.committed().len(), 3);
        println!("RUST_QUEUE_PRESSURE READY queries=1500 committedAudits=3");
    }
    context.runtime_telemetry().flush();
    context.runtime_telemetry().shutdown();
    if std::env::args().any(|arg| arg == "--post-shutdown") {
        let row = Q::payment_attempts()
            .with_id_is(attempt_id)
            .limit(1)
            .comment("read after owned telemetry shutdown")
            .purpose("verify business continues independently of export")
            .execute_for_one(&context)
            .await?
            .expect("post-shutdown read");
        assert_eq!(
            E::payment_attempt(&row)
                .get_reference_code()
                .eval()
                .as_deref(),
            Some("STANDARD-GRAPH-ATTEMPT")
        );
        assert_eq!(audit.committed().len(), 3);
        assert!(context.sql_logs().is_empty());
        println!("RUST_POST_SHUTDOWN_READ PASSED rows=1 committedAudits=3");
    }
    println!(
        "RUST_STANDARD_GRAPH PASSED savedEntities=3 committedAudits=3 inheritedGrandchild=true loadedDepth=2 ordinarySqlLogs=0"
    );
    Ok(())
}
