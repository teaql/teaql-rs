//! Real request -> SQLite transaction -> Context log checks (teaql-rs #239).
//! Tests never insert expected trace nodes into requests or provider metadata.
use std::sync::Arc;

use teaql_core::{
    DataType, DeleteCommand, EntityDescriptor, InsertCommand, PropertyDescriptor, RecoverCommand,
    SelectQuery, TraceKind,
};
use teaql_data_service::{DataServiceOperation, MutationCommand, QueryRequest, SchemaProvider};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, SqlLogOperation, UserContext};
use teaql_sql::SqlDataServiceExecutor;

#[derive(Clone)]
struct Schema(Arc<EntityDescriptor>);

impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        (self.0.name == name).then(|| self.0.clone())
    }
}

type Executor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;

#[test]
fn native_recover_request_preserves_operation_and_owned_reason_at_safe_log_sink() {
    futures_executor::block_on(async {
        let descriptor = EntityDescriptor::new("RecoveryProbe")
            .table_name("recovery_probe")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text));
        let transport = SqliteMutationExecutor::from_connection(
            rusqlite::Connection::open_in_memory().unwrap(),
        );
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()));
        context.use_sqlite_provider(transport.clone());
        context.ensure_schema().await.unwrap();
        context.insert_resource(Executor::new(
            SqliteDialect,
            transport,
            Schema(Arc::new(descriptor)),
        ));

        let result = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move {
                    transaction
                        .mutate(
                            MutationCommand::Insert(
                                InsertCommand::new("RecoveryProbe")
                                    .value("id", 7_i64)
                                    .value("version", 1_i64)
                                    .value("name", "private recovery fixture"),
                            )
                            .request("seed recovery fixture")?,
                        )
                        .await?;
                    transaction
                        .mutate(
                            MutationCommand::Delete(
                                DeleteCommand::new("RecoveryProbe", 7_i64).expected_version(1),
                            )
                            .request("archive recovery fixture")?,
                        )
                        .await?;
                    transaction
                        .mutate(
                            MutationCommand::Recover(RecoverCommand::new(
                                "RecoveryProbe",
                                7_i64,
                                -2,
                            ))
                            .request("restore archived object")?,
                        )
                        .await
                })
            })
            .await
            .unwrap();

        assert_eq!(result.affected_rows, 1);
        assert_eq!(result.metadata.operation, DataServiceOperation::Recover);
        let persisted = result.persisted_snapshot.unwrap();
        assert_eq!(
            persisted.get("version").and_then(|value| value.try_i64()),
            Some(3)
        );
        let logs = context.sql_logs();
        let recovery = logs
            .iter()
            .find(|entry| entry.operation == SqlLogOperation::Recover)
            .expect("Recover must not be relabeled Update in the Context log");
        assert_eq!(
            recovery.audit_reason.as_deref(),
            Some("restore archived object")
        );
        assert_eq!(
            recovery.trace_path.first().unwrap().entity_type,
            "RecoveryProbe"
        );
        assert_eq!(recovery.trace_path.last().unwrap().entity_type, "recover");
        assert!(recovery.trace_path.iter().all(|node| !matches!(
            node.kind,
            TraceKind::Comment | TraceKind::Purpose | TraceKind::AuditReason
        )));
        assert!(!format!("{logs:?}").contains("private recovery fixture"));

        let queried = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move {
                    transaction
                        .query(QueryRequest::from_query(
                            SelectQuery::new("RecoveryProbe")
                                .limit(1)
                                .comment("load restored object")
                                .purpose("verify recovered version"),
                        )?)
                        .await
                })
            })
            .await
            .unwrap();
        assert_eq!(queried.rows.len(), 1);
        let query_logs = context.sql_logs();
        let query = query_logs
            .iter()
            .find(|entry| entry.purpose.as_deref() == Some("verify recovered version"))
            .expect("native Query Request must retain its owned purpose");
        assert_eq!(query.comment.as_deref(), Some("load restored object"));
        assert_eq!(
            query.trace_path.first().unwrap().entity_type,
            "RecoveryProbe"
        );
        assert_eq!(query.trace_path.last().unwrap().entity_type, "select");
    });
}
