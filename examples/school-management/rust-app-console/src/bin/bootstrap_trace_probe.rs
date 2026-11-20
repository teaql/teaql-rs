//! Observe generated bootstrap; expected trace frames are never runtime inputs.
use school_management_service_core::{
    Q, ServiceRuntimeConfig,
    request_support::AuditedSave as _,
    service_runtime,
    teaql_core::{Entity as _, TraceKind},
};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use teaql_runtime::{
    RawAuditEventKind, RuntimeError, SafeAuditEvent, SafeAuditEventSink, SqlLogOperation,
    UserContext,
};

#[derive(Clone)]
struct Evidence {
    database: PathBuf,
    events: Arc<Mutex<Vec<SafeAuditEvent>>>,
}

impl SafeAuditEventSink for Evidence {
    fn on_safe_event(
        &self,
        _context: &UserContext,
        event: &SafeAuditEvent,
    ) -> Result<(), RuntimeError> {
        if !matches!(
            event.kind,
            RawAuditEventKind::Created | RawAuditEventKind::Updated
        ) {
            return Ok(());
        }
        assert_eq!(event.trace_chain.len(), 1, "assigned bootstrap lineage");
        let node = &event.trace_chain[0];
        let table = match event.entity.as_str() {
            "Platform" => "platform_data",
            "SchoolType" => "school_type_data",
            other => panic!("unexpected School fixture entity: {other}"),
        };
        let connection = rusqlite::Connection::open_with_flags(
            &self.database,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        connection
            .busy_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let (actual_version, actual_name): (i64, String) = connection
            .query_row(
                &format!("SELECT version, name FROM {table} WHERE id = ?"),
                [i64::try_from(node.entity_id.expect("assigned ID")).unwrap()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        if let Some(identity) = &event.bootstrap_audit {
            assert_eq!(
                Some(actual_version),
                identity.resulting_version,
                "bootstrap audit must follow commit visible to an independent connection"
            );
        } else {
            // Ordinary update audits contain changed fields, not an implicit
            // version change. Prove this app's name mutation is committed.
            assert_eq!(actual_name, "Drifted Primary");
            assert!(actual_version >= 2);
        }
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

impl Evidence {
    fn clear(&self, context: &UserContext) {
        self.events.lock().unwrap().clear();
        context.clear_sql_logs();
    }
    fn verify(&self, context: &UserContext, writes: usize, reads: usize, logging: bool) {
        let events = self.events.lock().unwrap();
        assert_eq!(events.len(), writes, "committed audits");
        let mut identities: Vec<_> = events
            .iter()
            .map(|event| {
                (
                    event.entity.as_str(),
                    event.trace_chain[0].entity_id.unwrap(),
                )
            })
            .collect();
        identities.sort();
        let expected_identities = match writes {
            3 => vec![("Platform", 1), ("SchoolType", 1001), ("SchoolType", 1002)],
            1 => vec![("SchoolType", 1001)],
            0 => vec![],
            _ => panic!("unexpected bootstrap write count"),
        };
        assert_eq!(
            identities, expected_identities,
            "fixed root and constant IDs"
        );
        let mut expected_reasons = Vec::new();
        for event in events.iter() {
            let identity = event
                .bootstrap_audit
                .as_ref()
                .expect("bootstrap attribution retained in safe event");
            assert_eq!(identity.actor, "teaql-generated-bootstrap");
            assert_eq!(identity.category, "runtime-bootstrap");
            let node = &event.trace_chain[0];
            assert_eq!(node.kind, TraceKind::AuditReason);
            assert_eq!(node.entity_type, event.entity);
            assert!(node.entity_id.is_some() && !node.comment.trim().is_empty());
            assert_eq!(identity.reason, node.comment);
            expected_reasons.push((event.entity.clone(), node.comment.clone()));
        }
        let logs = context.sql_logs();
        if !logging {
            assert!(logs.is_empty());
            return;
        }
        assert_eq!(
            logs.len(),
            writes + reads,
            "physical writes, lookups and readbacks: {:?}",
            logs.iter()
                .map(|log| (&log.operation, &log.comment, &log.purpose))
                .collect::<Vec<_>>()
        );
        let mut actual_reasons = Vec::new();
        for log in logs {
            let select = log.operation == SqlLogOperation::Select;
            assert_eq!(
                log.trace_path
                    .iter()
                    .map(|node| node.kind)
                    .collect::<Vec<_>>(),
                vec![
                    TraceKind::Operation,
                    if select {
                        TraceKind::Request
                    } else {
                        TraceKind::Entity
                    },
                    TraceKind::Provider,
                    TraceKind::Sql
                ]
            );
            if select {
                let comment = log.comment.as_deref().unwrap_or_default();
                assert!(
                    !comment.contains("1001") && !comment.contains("1002"),
                    "derived bootstrap read leaked target ID in intent: {comment}"
                );
                assert!(
                    log.comment
                        .as_ref()
                        .is_some_and(|value| !value.trim().is_empty())
                );
                assert!(
                    log.purpose
                        .as_ref()
                        .is_some_and(|value| !value.trim().is_empty())
                );
            } else {
                actual_reasons.push((
                    log.trace_path[1].entity_type.clone(),
                    log.audit_reason.expect("mutation intent"),
                ));
            }
        }
        actual_reasons.sort();
        expected_reasons.sort();
        assert_eq!(
            actual_reasons, expected_reasons,
            "SQL and committed audit reasons agree"
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = PathBuf::from(std::env::var("TEAQL_SCHOOL_BOOTSTRAP_DB")?);
    let logging = std::env::var("TEAQL_SCHOOL_BOOTSTRAP_LOGGING")? == "on";
    let evidence = Evidence {
        database: database.clone(),
        events: Default::default(),
    };
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: database.to_string_lossy().into_owned(),
    })
    .await?
    .with_custom_event_sink(evidence.clone())
    .with_user_identifier("school-example-user");
    if !logging {
        context.disable_sql_log();
    }
    context.ensure_schema().await?;
    let fresh = evidence.events.lock().unwrap().len() == 3;
    evidence.verify(
        &context,
        if fresh { 3 } else { 0 },
        // Every create has one bootstrap lookup, provider readback and
        // runtime strong-entity readback; these are actual physical queries.
        if fresh { 9 } else { 3 },
        logging,
    );
    evidence.clear(&context);
    context.ensure_schema().await?;
    evidence.verify(&context, 0, 3, logging);
    let mut primary = Q::school_types()
        .with_id_is(1001)
        .limit(1)
        .comment("load Primary for audited drift")
        .purpose("verify bootstrap reconciliation")
        .execute_for_one(&context)
        .await?
        .expect("seeded constant");
    let original_version = primary.version();
    primary.update_name("Drifted Primary");
    primary
        .audit_as("simulate constant drift")
        .save(&context)
        .await?;
    assert!(
        evidence
            .events
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .bootstrap_audit
            .is_none(),
        "ordinary save inherited bootstrap attribution"
    );
    evidence.clear(&context);
    context.ensure_schema().await?;
    // Reconciliation also loads the current row before the update.
    evidence.verify(&context, 1, 6, logging);
    let restored = Q::school_types()
        .with_id_is(1001)
        .limit(1)
        .comment("verify corrected constant")
        .purpose("verify persisted reconciliation")
        .execute_for_one(&context)
        .await?
        .unwrap();
    assert_eq!(restored.name(), "Primary");
    assert_eq!(restored.version(), original_version + 2);
    evidence.clear(&context);
    context.ensure_schema().await?;
    evidence.verify(&context, 0, 3, logging);
    assert_eq!(context.user_identifier(), Some("school-example-user"));
    println!(
        "PASS Rust generated bootstrap trace logging={logging} fresh={fresh} originalVersion={original_version}"
    );
    Ok(())
}
