//! Ordinary typed mutations and signed apply use the same physical target.
use futures_executor::block_on;
use rusqlite::Connection;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use teaql_core::{DataType, EntityDescriptor, InsertCommand, PropertyDescriptor};
use teaql_data_service::{MutationCommand, MutationExecutor, SchemaProvider};
use teaql_provider_sqlite::committed_events::{CommittedEventTarget, OutboxRecord};
use teaql_provider_sqlite::committed_security::SqliteCommittedReceiver;
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, UserContext};
use teaql_sql::{
    SqlDataServiceExecutor, SqlDialect, SqlTransaction, SqlTransactionTransport, SqlTransport,
};
const MODEL: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
#[derive(Clone)]
struct Schema(Arc<EntityDescriptor>);
impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        (name == "school").then(|| self.0.clone())
    }
}
fn entity() -> EntityDescriptor {
    EntityDescriptor::new("school")
        .table_name("school_data")
        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new("name", DataType::Text).not_null())
}
fn insert() -> InsertCommand {
    InsertCommand::new("school")
        .value("id", 99_u64)
        .value("version", 1_i64)
        .value("name", "ordinary write")
}
async fn fixture() -> (SqliteMutationExecutor, UserContext, SqliteCommittedReceiver) {
    let executor = SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
    let mut context =
        UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(entity()));
    context.insert_resource(CommittedEventTarget::new("readonly-source", MODEL).unwrap());
    context.use_sqlite_provider(executor.clone());
    context.ensure_schema().await.unwrap();
    let receiver = SqliteCommittedReceiver::new(
        executor.clone(),
        "readonly-source",
        "readonly-target",
        MODEL,
        HashMap::from([("current".into(), KEY.to_vec())]),
    )
    .unwrap();
    receiver.ensure_schema(&context).unwrap();
    (executor, context, receiver)
}
fn count(executor: &SqliteMutationExecutor) -> i64 {
    executor
        .connection()
        .lock()
        .unwrap()
        .query_row("SELECT count(*) FROM school_data", [], |r| r.get(0))
        .unwrap()
}
#[test]
fn direct_and_batch_mutations_cannot_write_to_signed_receiver_target() {
    block_on(async {
        for batch in [false, true] {
            let (executor, _, _) = fixture().await;
            let provider = SqlDataServiceExecutor::new(
                SqliteDialect,
                executor.clone(),
                Schema(Arc::new(entity())),
            );
            let command = MutationCommand::Insert(insert())
                .request("verify target default read-only")
                .unwrap();
            let command = if batch {
                MutationCommand::Batch(vec![command])
                    .request("verify target default read-only")
                    .unwrap()
            } else {
                command
            };
            let error = provider
                .mutate(command)
                .await
                .expect_err("analytical target ordinary mutation must be read-only");
            assert!(error.to_string().contains("READ_ONLY_TARGET"), "{error}");
            assert_eq!(count(&executor), 0);
        }
    });
}
#[test]
fn transaction_mutation_cannot_bypass_read_only_target() {
    block_on(async {
        let (executor, _, _) = fixture().await;
        let tx = executor.begin_sql().await.unwrap();
        let query = SqliteDialect.compile_insert(&entity(), &insert()).unwrap();
        let error = tx
            .execute_sql(&query)
            .await
            .expect_err("analytical target transaction mutation must be read-only");
        assert!(error.to_string().contains("READ_ONLY_TARGET"), "{error}");
        tx.rollback_sql().await.unwrap();
        assert_eq!(count(&executor), 0);
    });
}
#[test]
fn signed_replication_and_an_independent_normal_source_remain_writable() {
    block_on(async {
        let (executor, context, receiver) = fixture().await;
        let text=json!({"protocol_version":1,"change_set_id":"fact","source_runtime_id":"readonly-source","source_sequence":"1","source_model_hash":MODEL,"source_transaction_id":"tx","committed_at":"2026-10-22T00:00:00Z","audit":{"comment":"receive school","purpose":"verify read-only separation"},"mutations":[{"entity_type":"school","entity_id":"7","operation":"create","base_version":null,"committed_version":"1","fields":{"name":"replicated"},"trace_chain":[]}]}).to_string();
        let record = OutboxRecord {
            source_runtime_id: "readonly-source".into(),
            source_sequence: 1,
            change_set_id: "fact".into(),
            model_hash: MODEL.into(),
            payload_sha256: format!("{:x}", Sha256::digest(text.as_bytes())),
            payload_json: text,
        };
        receiver
            .receive(
                &context,
                &SqliteCommittedReceiver::sign("current", "readonly-target", KEY, record).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(count(&executor), 1);
        let source = SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        source.connection().lock().unwrap().execute_batch("CREATE TABLE school_data(id INTEGER PRIMARY KEY,version INTEGER NOT NULL,name TEXT NOT NULL)").unwrap();
        let provider =
            SqlDataServiceExecutor::new(SqliteDialect, source.clone(), Schema(Arc::new(entity())));
        provider
            .mutate(
                MutationCommand::Insert(insert())
                    .request("save independent normal source")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(count(&source), 1);
        assert_eq!(count(&executor), 1);
        println!("CMR15_READ_ONLY_SOURCE_PASS language=rust direct=1 batch=1 transaction=1 source_isolated=1 replication=1");
    });
}
