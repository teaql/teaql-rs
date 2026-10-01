//! Real SQLite mask contract, retained in the controlled examples gate.
use rusqlite::Connection;
use teaql_core::{
    DataType, DeleteCommand, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor,
    SelectQuery, UpdateCommand, Value,
};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt as _};
use teaql_runtime::{InMemoryMetadataStore, UserContext};

fn entity() -> EntityDescriptor {
    EntityDescriptor::new("Order")
        .table_name("orders")
        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new("name", DataType::Text))
}

#[tokio::test]
async fn sqlite_compiler_to_context_logs_preserves_field_masking_across_crud_and_batch() {
    use teaql_data_service::QueryRequest;
    type Executor = teaql_sql::SqlDataServiceExecutor<
        SqliteDialect,
        SqliteMutationExecutor,
        InMemoryMetadataStore,
    >;
    let descriptor = entity()
        .audit_mask_fields(vec!["name".into()])
        .property(PropertyDescriptor::new("status", DataType::Text))
        .property(PropertyDescriptor::new("password", DataType::Text));
    let metadata = InMemoryMetadataStore::new().with_entity(descriptor);
    let transport = SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
    let executor = Executor::new(SqliteDialect, transport.clone(), metadata.clone());
    let mut context = UserContext::new().with_metadata(metadata);
    context.use_sqlite_provider(transport.clone());
    context.register_executor(executor);
    context.ensure_schema().await.unwrap();

    fn request(query: SelectQuery) -> QueryRequest {
        let mut query = query.limit(10).comment("what: inspect masked CRUD fixture");
        query.trace_chain.push(teaql_core::TraceNode::typed(
            teaql_core::TraceKind::Purpose,
            "Order",
            None,
            "why: verify execution values differ from log projection",
        ));
        QueryRequest {
            trace_chain: query.trace_chain.clone(),
            intent: teaql_core::QueryIntent::new(
                "what: inspect masked CRUD fixture",
                "why: verify execution values differ from log projection",
            )
            .unwrap(),
            query,
            capture_debug_query: true,
            capture_execution_metadata: true,
        }
    }
    context
        .execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                let mut inserts = Vec::new();
                for id in [1_u64, 2] {
                    let mut command = InsertCommand::new("Order")
                        .value("id", id)
                        .value("version", 1_i64)
                        .value("name", "Riverside")
                        .value("status", "ACTIVE")
                        .value("password", "PASSWORD-CANARY");
                    command.trace_chain.push(teaql_core::TraceNode::typed(
                        teaql_core::TraceKind::AuditReason,
                        "Order",
                        Some(id),
                        "create masking fixture",
                    ));
                    inserts.push(
                        teaql_data_service::MutationCommand::Insert(command)
                            .request("create masking fixture")
                            .unwrap(),
                    );
                }
                scope
                    .mutate(
                        teaql_data_service::MutationCommand::Batch(inserts)
                            .request("create masking fixture")
                            .unwrap(),
                    )
                    .await?;
                for _ in 0..2 {
                    let rows = scope
                        .query(request(
                            SelectQuery::new("Order").filter(Expr::eq("name", "Riverside")),
                        ))
                        .await?;
                    assert_eq!(rows.rows.len(), 2);
                    assert_eq!(rows.rows[0].get("name"), Some(&Value::from("Riverside")));
                    assert_eq!(
                        rows.rows[0].get("password"),
                        Some(&Value::from("PASSWORD-CANARY"))
                    );
                }
                scope
                    .mutate(
                        teaql_data_service::MutationCommand::Update(
                            UpdateCommand::new("Order", 1_u64)
                                .expected_version(1)
                                .value("name", "Lakeside"),
                        )
                        .request("create masking fixture")
                        .unwrap(),
                    )
                    .await?;
                scope
                    .mutate(
                        teaql_data_service::MutationCommand::Delete(
                            DeleteCommand::new("Order", 1_u64).expected_version(2),
                        )
                        .request("create masking fixture")
                        .unwrap(),
                    )
                    .await?;
                scope
                    .mutate(
                        teaql_data_service::MutationCommand::Recover(
                            teaql_core::RecoverCommand::new("Order", 1_u64, -3),
                        )
                        .request("create masking fixture")
                        .unwrap(),
                    )
                    .await?;
                let rows = scope
                    .query(request(
                        SelectQuery::new("Order").filter(Expr::eq("id", 1_u64)),
                    ))
                    .await?;
                assert_eq!(rows.rows[0].get("name"), Some(&Value::from("Lakeside")));
                assert_eq!(rows.rows[0].get("version"), Some(&Value::I64(4)));
                Ok(())
            })
        })
        .await
        .unwrap();
    let logs = context.sql_logs();
    assert_eq!(
        logs.len(),
        8,
        "batch must emit two independent statement logs"
    );
    for log in &logs {
        assert!(
            log.log_context.omission_reason.is_none(),
            "{:?}",
            log.log_context
        );
        assert!(!log.debug_sql.contains('?'));
        assert!(!format!("{log:?}").contains("PASSWORD-CANARY"));
        assert!(!format!("{log:?}").contains("Riverside"));
        assert!(!format!("{log:?}").contains("Lakeside"));
    }
    assert!(logs[0].debug_sql.contains("'Ri*****de' /* masked */"));
    assert!(logs[0].debug_sql.contains("'ACTIVE'"));
    assert!(logs[0].debug_sql.contains("'[REDACTED]' /* masked */"));
    assert!(logs[4].debug_sql.contains("'La****de' /* masked */"));
    assert_eq!(logs[2].result_count, Some(2));
    assert_eq!(
        logs[0].audit_reason.as_deref(),
        Some("create masking fixture")
    );
    assert_eq!(
        logs[2].purpose.as_deref(),
        Some("why: verify execution values differ from log projection")
    );
}
