//! Generated Q/E and bootstrap; no application-owned OpenTelemetry SDK setup.
use std::sync::{Arc, Mutex};
use teaql_runtime::{
    RawAuditEventKind, RuntimeError, SafeAuditEvent, SafeAuditEventSink, UserContext,
};
use trace_chain_service_core::{E, Q, ServiceRuntimeConfig, service_runtime};

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
        "teaql-rust-standard-telemetry-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&directory)?;
    let audit = AuditCapture::default();
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: format!(
            "sqlite://{}?mode=rwc",
            directory.join("example.db").display()
        ),
    })
    .await?
    .with_custom_event_sink(audit.clone());
    context.disable_sql_log();
    context.ensure_schema().await?;
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
    assert_eq!(
        committed(),
        1,
        "Generated root bootstrap must produce one committed mutation"
    );
    context.ensure_schema().await?;
    assert_eq!(
        committed(),
        1,
        "Repeated bootstrap must not repeat writes or committed audit"
    );
    for _ in 0..2 {
        let rows = Q::platforms_minimal()
            .with_id_is(1)
            .order_by_id_asc()
            .limit(1)
            .comment("load seeded platform")
            .purpose("verify automatic standard telemetry")
            .execute_for_list(&context)
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(E::platform(&rows[0]).get_id().eval(), Some(1));
    }
    assert!(
        context.sql_logs().is_empty(),
        "Ordinary logging remains independently disabled"
    );
    context.runtime_telemetry().flush();
    context.runtime_telemetry().shutdown();
    println!(
        "RUST_STANDARD_TELEMETRY_EXAMPLE PASSED ensures=2 committedSeeds=1 queries=2 rows=1 ordinarySqlLogs=0"
    );
    Ok(())
}
