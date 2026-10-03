//! A loaded private graph fails only after both real writes, then retries.
use super::{
    AuditCapture, Observation, Observed, Outcome, graph_privacy::assert_private,
    readback_transport::ReadbackFault,
};
use teaql_data_service::{DataServiceOperation, SqlExecutionOutcome};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
use teaql_runtime::UserContext;
use teaql_sql::SqlDataServiceExecutor;
use trace_chain_service_core::teaql_core::Entity as _;
use trace_chain_service_core::{
    AuditedSave as _, CustomerOrder, E, LedgerEntity as _, LocalSchemaProvider, Q,
    ServiceRuntimeConfig, service_runtime,
};

async fn load_pending(
    context: &UserContext,
    root_id: u64,
    child_id: u64,
    name: &str,
) -> Outcome<CustomerOrder> {
    let mut root = Q::customer_orders()
        .with_id_is(root_id)
        .limit(1)
        .comment("reload rollback privacy root")
        .purpose("prepare a complete versioned graph")
        .execute_for_one(context)
        .await?
        .ok_or("root required")?;
    let mut child = Q::order_items()
        .with_id_is(child_id)
        .limit(1)
        .comment("reload rollback privacy child")
        .purpose("retain old values for safe diagnostics")
        .execute_for_one(context)
        .await?
        .ok_or("child required")?;
    root.update_description("private graph after retry");
    child.update_name(name.to_owned());
    root.include_pending_mutations_from(&child)?;
    Ok(root)
}

pub async fn rollback_and_retry(database_url: String) -> Outcome<()> {
    let capture = AuditCapture::default();
    let observation = Observation::default();
    let mut context = service_runtime(ServiceRuntimeConfig { database_url })
        .await?
        .with_custom_event_sink(capture.clone());
    context.ensure_schema().await?;
    let platform = Q::platforms()
        .limit(1)
        .comment("reuse rollback privacy platform")
        .purpose("attach the fixture through generated APIs")
        .execute_for_one(&context)
        .await?
        .ok_or("platform required")?;
    let mut root = Q::customer_orders()
        .comment("prepare rollback root")
        .purpose("verify private graph rollback")
        .new_entity(&context);
    root.update_platform_id(platform.id());
    root.update_order_number("TRACE-PRIVACY-ROLLBACK");
    root.update_description("private graph before retry");
    let root_id = root.id();
    let old = format!("PRIVATE-ROLLBACK-OLD-{root_id}");
    let next = format!("PRIVATE-ROLLBACK-NEW-{root_id}");
    let mut child = Q::order_items()
        .comment("prepare rollback child")
        .purpose("persist a masked original value")
        .new_entity(&context);
    child.update_customer_order_id(root_id);
    child.update_name(old.clone());
    let child_id = child.id();
    root.include_pending_mutations_from(&child)?;
    root.audit_as("seed rollback privacy graph")
        .save(&context)
        .await?;

    let transport = context
        .require_resource::<SqliteMutationExecutor>()?
        .clone();
    // Root write and its real readback succeed. Child write succeeds; only its
    // readback fails. This must roll back both rows and discard all audit facts.
    context.register_executor(Observed::new(
        SqlDataServiceExecutor::new(
            SqliteDialect,
            ReadbackFault::after_writes(transport, 2),
            LocalSchemaProvider,
        ),
        observation.clone(),
    ));
    let pending = load_pending(&context, root_id, child_id, &next).await?;
    let reason = format!("page 1 replace {old} with {next}");
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    let failed = pending.audit_as(reason.clone()).save(&context).await;
    let error = match failed {
        Ok(_) => return Err("second readback must fail".into()),
        Err(error) => error,
    };
    assert!(error.to_string().contains("INJECTED_READBACK_FAILURE"));
    assert!(
        capture.events().is_empty(),
        "no committed audit after failed graph"
    );
    let commands = observation.commands();
    assert_eq!(commands.len(), 2);
    assert!(
        commands.iter().all(|request| request.comment() == reason),
        "privacy projection must not rewrite trusted request intent"
    );
    let metadata = observation.metadata();
    let writes: Vec<_> = metadata
        .iter()
        .filter(|m| m.operation == DataServiceOperation::Update)
        .collect();
    assert_eq!(writes.len(), 2, "both real writes precede failure");
    assert!(writes.iter().all(|m| m.affected_rows == Some(1)
        && m.sql_log.execution_outcome == Some(SqlExecutionOutcome::Success)));
    assert_eq!(
        metadata
            .iter()
            .filter(|m| m.operation == DataServiceOperation::Query
                && m.sql_log.execution_outcome == Some(SqlExecutionOutcome::Failure))
            .count(),
        1
    );
    let logs = context.sql_logs();
    assert!(
        logs.iter()
            .any(|log| log.log_context.execution_outcome == Some(SqlExecutionOutcome::Failure))
    );
    for log in &logs {
        let diagnostic = format!("{log:?}");
        assert!(
            !diagnostic.contains(&old),
            "failure diagnostics leaked old sibling value"
        );
        assert!(
            !diagnostic.contains(&next),
            "failure diagnostics leaked pending sibling value"
        );
    }
    let failed_log = logs
        .iter()
        .find(|log| log.log_context.execution_outcome == Some(SqlExecutionOutcome::Failure))
        .unwrap();
    assert!(
        failed_log
            .audit_reason
            .as_deref()
            .is_some_and(|s| s.starts_with("page 1"))
    );
    assert_eq!(
        failed_log.trace_path.first().unwrap().entity_type,
        "CustomerOrder"
    );
    assert_eq!(failed_log.trace_path.last().unwrap().entity_type, "select");
    let stored_root = Q::customer_orders()
        .with_id_is(root_id)
        .limit(1)
        .comment("check rolled-back root")
        .purpose("verify persisted value and version")
        .execute_for_one(&context)
        .await?
        .ok_or("persisted root required")?;
    assert_eq!(
        E::customer_order(&stored_root)
            .get_description()
            .eval()
            .as_deref(),
        Some("private graph before retry")
    );
    assert_eq!(
        E::customer_order(&stored_root).get_version().eval(),
        Some(1)
    );
    let stored_child = Q::order_items()
        .with_id_is(child_id)
        .limit(1)
        .comment("check rolled-back child")
        .purpose("verify private original value survived")
        .execute_for_one(&context)
        .await?
        .ok_or("persisted child required")?;
    assert_eq!(
        E::order_item(&stored_child).get_name().eval().as_deref(),
        Some(old.as_str())
    );
    assert_eq!(E::order_item(&stored_child).get_version().eval(), Some(1));

    // Public save consumes the wrapper. Retry reloads complete objects and
    // reapplies the business input; no entity clone or shared ledger shortcut.
    let pending = load_pending(&context, root_id, child_id, &next).await?;
    capture.clear();
    observation.clear();
    context.clear_sql_logs();
    let saved = pending.audit_as(reason).save(&context).await?;
    assert_private(&context, &capture, &[&old, &next]);
    assert_eq!(
        E::customer_order(&saved)
            .get_description()
            .eval()
            .as_deref(),
        Some("private graph after retry")
    );
    assert_eq!(E::customer_order(&saved).get_version().eval(), Some(2));
    let child = Q::order_items()
        .with_id_is(child_id)
        .limit(1)
        .comment("check retried child")
        .purpose("confirm one committed version increment")
        .execute_for_one(&context)
        .await?
        .ok_or("retried child required")?;
    assert_eq!(
        E::order_item(&child).get_name().eval().as_deref(),
        Some(next.as_str())
    );
    assert_eq!(E::order_item(&child).get_version().eval(), Some(2));
    context.clear_sql_logs();
    Q::customer_orders()
        .with_id_is(root_id)
        .limit(1)
        .comment(old.clone())
        .purpose("independent request after rollback and retry")
        .execute_for_one(&context)
        .await?;
    assert_eq!(context.sql_logs()[0].comment.as_deref(), Some(old.as_str()));
    println!("TC-REQ-16 GRAPH PRIVACY ROLLBACK PASSED root={root_id} child={child_id}");
    Ok(())
}
