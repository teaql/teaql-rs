//! Runtime-owned SQLite streaming diagnostic fixture; no generated source edits.
use futures_util::StreamExt;
use rusqlite::Connection;
use teaql_core::{
    DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, SelectQuery, TraceKind,
    TraceNode,
};
use teaql_data_service::MutationRequest;
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt as _};
use teaql_runtime::{InMemoryMetadataStore, PurposedSelectQuery, UserContext};
type Executor =
    teaql_sql::SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, InMemoryMetadataStore>;

async fn context(seed: bool) -> UserContext {
    let metadata = InMemoryMetadataStore::new().with_entity(
        EntityDescriptor::new("Customer")
            .table_name("customers")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text))
            .property(PropertyDescriptor::new("address", DataType::Text))
            .property(PropertyDescriptor::new("password", DataType::Text))
            .audit_mask_fields(vec!["name".into()]),
    );
    let transport = SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
    let mut context = UserContext::new().with_metadata(metadata.clone());
    context.use_sqlite_provider(transport.clone());
    context.register_executor(Executor::new(SqliteDialect, transport, metadata));
    if seed {
        context.ensure_schema().await.unwrap();
        context
            .execute_in_transaction::<Executor, _, _>(|scope| {
                Box::pin(async move {
                    for id in 1_u64..=3 {
                        let mut command = InsertCommand::new("Customer")
                            .value("id", id)
                            .value("version", 1_i64)
                            .value("name", "Riverside")
                            .value("address", "1 Runtime Road")
                            .value("password", "PASSWORD-CANARY");
                        command.trace_chain.push(TraceNode::typed(
                            TraceKind::AuditReason,
                            "Customer",
                            Some(id),
                            "seed cursor fixture",
                        ));
                        scope
                            .mutate(
                                teaql_data_service::MutationCommand::Insert(command)
                                    .request("verify safe provider diagnostics")
                                    .unwrap(),
                            )
                            .await?;
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
    context.clear_sql_logs();
    context
}

fn query(empty: bool) -> PurposedSelectQuery {
    PurposedSelectQuery::new(
        SelectQuery::new("Customer")
            .limit(10)
            .stream(1)
            .filter(Expr::and([
                Expr::eq("name", "Riverside"),
                Expr::eq("address", "1 Runtime Road"),
                Expr::eq("password", "PASSWORD-CANARY"),
                Expr::gt("id", if empty { 100_u64 } else { 0 }),
            ]))
            .comment("read masked stream"),
        "verify cursor diagnostics",
    )
}

fn check(context: &UserContext, rows: usize, outcome: teaql_data_service::SqlExecutionOutcome) {
    let logs = context.sql_logs();
    assert_eq!(
        logs.len(),
        1,
        "missing or duplicate terminal SQL diagnostic"
    );
    let log = &logs[0];
    assert_eq!(log.log_context.execution_outcome, Some(outcome));
    assert_eq!(log.result_count, Some(rows));
    assert_eq!(log.comment.as_deref(), Some("read masked stream"));
    assert_eq!(log.purpose.as_deref(), Some("verify cursor diagnostics"));
    // Intent has dedicated fields; canonical trace omits duplicate intent nodes.
    assert!(
        log.trace_path
            .iter()
            .any(|node| { node.kind == TraceKind::Request && node.entity_type == "Customer" })
    );
    assert!(
        log.trace_path
            .iter()
            .any(|node| node.kind == TraceKind::Provider)
    );
    assert!(
        log.trace_path
            .iter()
            .any(|node| node.kind == TraceKind::Sql)
    );
    assert!(
        !log.trace_path
            .iter()
            .any(|node| node.comment == "second relation trace")
    );
    let retained = format!("{log:?}");
    assert!(!retained.contains("Riverside"));
    assert!(!retained.contains("PASSWORD-CANARY"));
    assert!(log.debug_sql.contains("Ri*****de"));
    assert!(log.debug_sql.contains("1 Runtime Road"));
    assert!(!log.debug_sql.contains("Riverside"));
    assert!(!log.debug_sql.contains("PASSWORD-CANARY"));
    assert!(log.debug_sql.contains("NOT REPLAYABLE"));
}

#[tokio::test]
async fn complete_stream_logs_once() {
    let context = context(true).await;
    let service = context.entity_data_service::<Executor>("Customer").unwrap();
    let query = query(false);
    let mut stream = service.fetch_stream(&query).await.unwrap();
    let mut count = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        for row in &chunk.rows {
            assert_eq!(row.get("name"), Some(&teaql_core::Value::from("Riverside")));
            assert_eq!(
                row.get("password"),
                Some(&teaql_core::Value::from("PASSWORD-CANARY"))
            );
        }
        count += chunk.rows.len();
    }
    drop(stream);
    assert_eq!(count, 3);
    check(
        &context,
        3,
        teaql_data_service::SqlExecutionOutcome::Success,
    );
}

#[tokio::test]
async fn dropped_stream_releases_cursor_and_logs_only_delivered_rows() {
    let context = context(true).await;
    let service = context.entity_data_service::<Executor>("Customer").unwrap();
    let query = query(false);
    let mut stream = service.fetch_stream(&query).await.unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().rows.len(), 1);
    drop(stream);
    check(
        &context,
        1,
        teaql_data_service::SqlExecutionOutcome::Cancelled,
    );
    assert_eq!(service.fetch_all(&query).await.unwrap().len(), 3);
}

#[tokio::test]
async fn empty_stream_has_successful_zero_delivery() {
    let context = context(true).await;
    let service = context.entity_data_service::<Executor>("Customer").unwrap();
    let query = query(true);
    let mut stream = service.fetch_stream(&query).await.unwrap();
    assert!(stream.next().await.is_none());
    drop(stream);
    check(
        &context,
        0,
        teaql_data_service::SqlExecutionOutcome::Success,
    );
}

#[tokio::test]
async fn provider_failure_still_logs_safe_sql() {
    let context = context(false).await;
    let service = context.entity_data_service::<Executor>("Customer").unwrap();
    let query = query(false);
    let mut stream = service.fetch_stream(&query).await.unwrap();
    assert!(stream.next().await.unwrap().is_err());
    drop(stream);
    check(
        &context,
        0,
        teaql_data_service::SqlExecutionOutcome::Failure,
    );
}

#[tokio::test]
async fn disabled_logging_does_not_prevent_cursor_release() {
    let mut context = context(true).await;
    context.disable_sql_log();
    let service = context.entity_data_service::<Executor>("Customer").unwrap();
    let query = query(false);
    let mut stream = service.fetch_stream(&query).await.unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().rows.len(), 1);
    drop(stream);
    assert_eq!(service.fetch_all(&query).await.unwrap().len(), 3);
    assert!(context.sql_logs().is_empty());
}

#[tokio::test]
async fn streams_keep_independent_intent_snapshots_in_one_context() {
    let context = context(true).await;
    let service = context.entity_data_service::<Executor>("Customer").unwrap();
    let first_query = query(false);
    let mut second_selection = SelectQuery::new("Customer")
        .limit(1)
        .stream(1)
        .comment("second cursor");
    second_selection.trace_chain.push(TraceNode::typed(
        TraceKind::Relation,
        "Customer",
        None,
        "second relation trace",
    ));
    let second_query = PurposedSelectQuery::new(second_selection, "independent second purpose");
    // Construct both before consuming either; creation of the second must not
    // replace the intent retained by the first. SQLite cursors are consumed serially.
    let mut first = service.fetch_stream(&first_query).await.unwrap();
    let mut second = service.fetch_stream(&second_query).await.unwrap();
    first.next().await.unwrap().unwrap();
    drop(first);
    check(
        &context,
        1,
        teaql_data_service::SqlExecutionOutcome::Cancelled,
    );
    while let Some(chunk) = second.next().await {
        chunk.unwrap();
    }
    drop(second);
    let logs = context.sql_logs();
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[1].comment.as_deref(), Some("second cursor"));
    assert!(logs[1].trace_path.iter().any(|node| {
        node.kind == TraceKind::Relation && node.comment == "second relation trace"
    }));
    assert_eq!(
        logs[1].purpose.as_deref(),
        Some("independent second purpose")
    );
    assert!(
        !logs[1]
            .trace_path
            .iter()
            .any(|node| node.comment == "verify cursor diagnostics")
    );
    assert_eq!(
        logs[1].log_context.execution_outcome,
        Some(teaql_data_service::SqlExecutionOutcome::Success)
    );
    assert_eq!(logs[1].result_count, Some(1));
}

#[tokio::test]
async fn ordinary_query_failure_has_safe_diagnostic_and_unknown_count() {
    let context = context(false).await;
    let service = context.entity_data_service::<Executor>("Customer").unwrap();
    let error = service.fetch_all(&query(false)).await.unwrap_err();
    assert!(error.to_string().contains("no such table"));
    let logs = context.sql_logs();
    assert_eq!(logs.len(), 1, "failed query lost its SQL diagnostic");
    assert_eq!(
        logs[0].log_context.execution_outcome,
        Some(teaql_data_service::SqlExecutionOutcome::Failure)
    );
    assert_eq!(logs[0].result_count, None);
    assert!(logs[0].debug_sql.contains("Ri*****de"));
    assert!(logs[0].debug_sql.contains("1 Runtime Road"));
    assert!(!format!("{:?}", logs[0]).contains("PASSWORD-CANARY"));
}

#[tokio::test]
async fn transaction_query_failure_has_safe_diagnostic() {
    let context = context(false).await;
    let result = context
        .execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                let query = query(false).into_query();
                scope
                    .query(teaql_data_service::QueryRequest {
                        trace_chain: query.trace_chain.clone(),
                        intent: teaql_core::QueryIntent::from_optional(
                            query.comment.as_deref(),
                            query.purpose.as_deref(),
                        )
                        .unwrap(),
                        query,
                        capture_debug_query: true,
                        capture_execution_metadata: true,
                    })
                    .await?;
                Ok(())
            })
        })
        .await;
    assert!(result.is_err());
    let logs = context.sql_logs();
    assert_eq!(
        logs.len(),
        1,
        "failed transaction query lost its SQL diagnostic"
    );
    assert_eq!(
        logs[0].log_context.execution_outcome,
        Some(teaql_data_service::SqlExecutionOutcome::Failure)
    );
    assert_eq!(logs[0].result_count, None);
    assert!(logs[0].debug_sql.contains("Ri*****de"));
    assert!(!format!("{:?}", logs[0]).contains("PASSWORD-CANARY"));
}

#[tokio::test]
async fn transaction_buffered_stream_counts_only_delivered_chunks() {
    let context = context(true).await;
    context
        .execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                let service = scope.entity_data_service("Customer").unwrap();
                let query = query(false);
                let mut stream = service.fetch_stream(&query).await.unwrap();
                assert_eq!(stream.next().await.unwrap().unwrap().rows.len(), 1);
                drop(stream);
                Ok(())
            })
        })
        .await
        .unwrap();
    check(
        &context,
        1,
        teaql_data_service::SqlExecutionOutcome::Cancelled,
    );
}

#[tokio::test]
async fn transaction_buffered_stream_failure_logs_once() {
    let context = context(false).await;
    context
        .execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                let service = scope.entity_data_service("Customer").unwrap();
                let query = query(false);
                let mut stream = service.fetch_stream(&query).await.unwrap();
                assert!(stream.next().await.unwrap().is_err());
                drop(stream);
                Ok(())
            })
        })
        .await
        .unwrap();
    check(
        &context,
        0,
        teaql_data_service::SqlExecutionOutcome::Failure,
    );
}

fn insert_customer(id: u64) -> MutationRequest {
    let mut command = InsertCommand::new("Customer")
        .value("id", id)
        .value("version", 1_i64)
        .value("name", "Riverside")
        .value("address", "1 Runtime Road")
        .value("password", "PASSWORD-CANARY");
    command.trace_chain.push(TraceNode::typed(
        TraceKind::AuditReason,
        "Customer",
        Some(id),
        "test audited write",
    ));
    teaql_data_service::MutationCommand::Insert(command)
        .request("verify safe provider diagnostics")
        .unwrap()
}

#[tokio::test]
async fn transaction_duplicate_write_failure_keeps_safe_sql_and_unknown_count() {
    let context = context(true).await;
    let result = context
        .execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                scope.mutate(insert_customer(1)).await?;
                Ok(())
            })
        })
        .await;
    assert!(result.unwrap_err().to_string().contains("UNIQUE"));
    let logs = context.sql_logs();
    assert_eq!(logs.len(), 1, "failed write lost its SQL diagnostic");
    let log = &logs[0];
    assert_eq!(log.affected_rows, None);
    assert_eq!(
        log.log_context.execution_outcome,
        Some(teaql_data_service::SqlExecutionOutcome::Failure)
    );
    assert_eq!(log.audit_reason.as_deref(), Some("test audited write"));
    assert!(log.debug_sql.contains("Ri*****de"));
    assert!(log.debug_sql.contains("1 Runtime Road"));
    assert!(!format!("{log:?}").contains("PASSWORD-CANARY"));
    assert_eq!(
        context
            .entity_data_service::<Executor>("Customer")
            .unwrap()
            .fetch_all(&query(false))
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn partial_batch_keeps_statement_success_even_when_transaction_rolls_back() {
    let context = context(true).await;
    let result = context
        .execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                scope
                    .mutate(
                        teaql_data_service::MutationCommand::Batch(vec![
                            insert_customer(4),
                            insert_customer(1),
                            insert_customer(5),
                        ])
                        .request("verify safe provider diagnostics")
                        .unwrap(),
                    )
                    .await?;
                Ok(())
            })
        })
        .await;
    assert!(result.is_err());
    let logs = context.sql_logs();
    let writes: Vec<_> = logs
        .iter()
        .filter(|log| log.operation == teaql_runtime::SqlLogOperation::Insert)
        .collect();
    assert_eq!(
        writes.len(),
        2,
        "retain actual writes, never invent unexecuted third member"
    );
    assert_eq!(writes[0].affected_rows, Some(1));
    assert_eq!(
        writes[0].log_context.execution_outcome,
        Some(teaql_data_service::SqlExecutionOutcome::Success)
    );
    assert_eq!(writes[1].affected_rows, None);
    assert_eq!(
        writes[1].log_context.execution_outcome,
        Some(teaql_data_service::SqlExecutionOutcome::Failure)
    );
    assert!(!format!("{logs:?}").contains("PASSWORD-CANARY"));
    assert_eq!(
        context
            .entity_data_service::<Executor>("Customer")
            .unwrap()
            .fetch_all(&query(false))
            .await
            .unwrap()
            .len(),
        3,
        "first write was rolled back"
    );
}

#[tokio::test]
async fn sqlite_trigger_missing_readback_keeps_both_sql_successes() {
    let context = context(true).await;
    // Infrastructure-only fault injection: the statement succeeds, but a trigger
    // removes its row before the framework's post-write snapshot can be read.
    context.require_resource::<Executor>().unwrap().transport.connection().lock().unwrap()
        .execute_batch("CREATE TRIGGER remove_new_customer AFTER INSERT ON customers BEGIN DELETE FROM customers WHERE id = NEW.id; END;").unwrap();
    let result = context
        .execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                let mut request = insert_customer(4);
                if let teaql_data_service::MutationCommand::Insert(command) = &mut request.command {
                    command.trace_chain[0].comment =
                        "test audited write Riverside PASSWORD-CANARY".into();
                }
                scope.mutate(request).await?;
                Ok(())
            })
        })
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("could not be read back")
    );
    let logs = context.sql_logs();
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[0].operation, teaql_runtime::SqlLogOperation::Insert);
    assert_eq!(logs[0].affected_rows, Some(1));
    assert_eq!(logs[1].operation, teaql_runtime::SqlLogOperation::Select);
    assert_eq!(logs[1].result_count, Some(0));
    for log in &logs {
        assert_eq!(
            log.audit_reason.as_deref(),
            Some("test audited write [REDACTED] [REDACTED]")
        );
        assert!(log.log_context.intent_redactions.is_empty());
        assert_eq!(
            log.log_context.execution_outcome,
            Some(teaql_data_service::SqlExecutionOutcome::Success)
        );
        assert!(!format!("{log:?}").contains("Riverside"));
        assert!(!format!("{log:?}").contains("PASSWORD-CANARY"));
    }
    assert_eq!(
        context
            .entity_data_service::<Executor>("Customer")
            .unwrap()
            .fetch_all(&query(false))
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn disabled_mutation_log_still_returns_original_failure() {
    let mut context = context(true).await;
    context.disable_sql_log();
    let result = context
        .execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                scope.mutate(insert_customer(1)).await?;
                Ok(())
            })
        })
        .await;
    assert!(result.unwrap_err().to_string().contains("UNIQUE"));
    assert!(context.sql_logs().is_empty());
}
