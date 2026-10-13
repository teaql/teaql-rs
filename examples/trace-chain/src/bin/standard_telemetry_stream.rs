//! Generated bounded Q/E streams; no application-owned SDK or instrumentation.
use futures_util::{FutureExt, StreamExt};
use std::sync::{Arc, Mutex};
use teaql_runtime::{
    RawAuditEventKind, RuntimeError, SafeAuditEvent, SafeAuditEventSink, UserContext,
};
use trace_chain_service_core::teaql_core::Entity as _;
use trace_chain_service_core::{AuditedSave as _, E, Q, ServiceRuntimeConfig, service_runtime};

#[derive(Clone, Default)]
struct AuditCapture(Arc<Mutex<Vec<SafeAuditEvent>>>);
impl SafeAuditEventSink for AuditCapture {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.0.lock().unwrap().push(event.clone());
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "teaql-standard-stream-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&directory)?;
    let audit = AuditCapture::default();
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: format!(
            "sqlite://{}?mode=rwc",
            directory.join("stream.db").display()
        ),
    })
    .await?
    .with_custom_event_sink(audit.clone());
    context.disable_sql_log();
    context.ensure_schema().await?;
    context.ensure_schema().await?;
    for index in 0..3 {
        let mut order = Q::customer_orders()
            .comment("prepare standard stream fixture")
            .purpose("prepare bounded stream records")
            .new_entity(&context);
        order.update_platform_id(1);
        order.update_order_number(format!("STREAM-{index}"));
        order.update_description("bounded stream fixture");
        order
            .audit_as("seed standard stream fixture")
            .save(&context)
            .await?;
    }
    let committed = || {
        audit
            .0
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
            .count()
    };
    assert_eq!(committed(), 4);
    let before = committed();
    let mut complete = Q::customer_orders_minimal()
        .select_order_number()
        .order_by_id_asc()
        .limit(3)
        .stream(1)
        .comment("consume complete standard stream")
        .purpose("verify complete stream root lifetime")
        .execute_for_stream(&context)
        .await?;
    let mut cancelled = Q::customer_orders_minimal()
        .select_description()
        .order_by_id_asc()
        .limit(3)
        .stream(1)
        .comment("close partial standard stream")
        .purpose("verify cancelled stream root lifetime")
        .execute_for_stream(&context)
        .await?;
    let partial = cancelled.next().await.unwrap()?;
    assert_eq!(
        E::customer_order(&partial)
            .get_description()
            .eval()
            .as_deref(),
        Some("bounded stream fixture")
    );
    assert!(
        complete.next().now_or_never().is_none(),
        "second stream waits on SQLite lease"
    );
    let other = Q::platforms_minimal()
        .with_id_is(1)
        .limit(1)
        .comment("run independent query between stream polls")
        .purpose("verify stream context isolation")
        .execute_for_list(&context);
    futures_util::pin_mut!(other);
    assert!(
        other.as_mut().now_or_never().is_none(),
        "ordinary query waits on SQLite lease"
    );
    drop(cancelled);
    // Drop restores the stream's own context and releases its cursor before
    // the already-polled independent query is resumed.
    assert_eq!(other.await?.len(), 1);
    let first = complete.next().await.unwrap()?;
    assert_eq!(
        E::customer_order(&first)
            .get_order_number()
            .eval()
            .as_deref(),
        Some("STREAM-0")
    );
    let mut delivered = 1;
    while let Some(row) = complete.next().await {
        row?;
        delivered += 1;
    }
    assert_eq!(delivered, 3);
    drop(complete);
    assert_eq!(
        committed(),
        before,
        "stream telemetry must not produce committed business audit"
    );
    assert!(context.sql_logs().is_empty());
    context.runtime_telemetry().flush();
    context.runtime_telemetry().shutdown();
    println!(
        "RUST_STANDARD_STREAM PASSED completeRows=3 cancelledRows=1 independentRows=1 committedAudits=4 ordinarySqlLogs=0"
    );
    Ok(())
}
