use std::collections::BTreeMap;

use rusqlite::Connection;
use teaql_core::{CompactRow, Entity, TeaqlEntity as _, Value};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_provider_sqlite::committed_events::CommittedEventTarget;
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{
    CommittedEventSource, EntityKey, InMemoryMetadataStore, LedgerEntity, UserContext,
    save_audited_ledger_entity,
};
use teaql_sql::SqlDataServiceExecutor;

#[teaql_entity]
#[derive(Debug, TeaqlEntity)]
#[teaql(entity = "ExampleOrder", table = "committed_example_order")]
struct ExampleOrder {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
}

fn new_order(id: u64, name: &str) -> ExampleOrder {
    let mut order = ExampleOrder::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(id)),
        ("version".into(), Value::I64(0)),
        ("name".into(), Value::Text(name.into())),
    ])))
    .expect("decode example order");
    order.mark_as_new();
    order.entity_runtime_state().expect("new order ledger").set(
        EntityKey::new("ExampleOrder", id),
        "name",
        name,
    );
    order
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let transport = SqliteMutationExecutor::from_connection(Connection::open_in_memory()?);
    let metadata = InMemoryMetadataStore::new().with_entity(ExampleOrder::entity_descriptor());
    let service = SqlDataServiceExecutor::new(SqliteDialect, transport.clone(), metadata.clone());
    let mut context = UserContext::new()
        .with_metadata(metadata)
        .with_committed_event_source(CommittedEventSource::new(
            "example-rust-source",
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )?);
    context.use_sqlite_provider(transport.clone());
    context.register_executor(service);
    context.ensure_schema().await?;
    transport.connection().lock().unwrap().execute_batch(
        "CREATE TRIGGER committed_example_default AFTER INSERT ON committed_example_order \
         BEGIN UPDATE committed_example_order SET name = 'database accepted' WHERE id = NEW.id; END",
    )?;

    let order = save_audited_ledger_entity(
        new_order(71, "pre-save").audit_as("create example order"),
        &context,
    )
    .await?;
    assert_eq!(order.name, "database accepted");
    let outbox = transport.committed_outbox_after("example-rust-source", 0, 10)?;
    assert_eq!(outbox.len(), 1);
    let first: serde_json::Value = serde_json::from_str(&outbox[0].payload_json)?;
    assert_eq!(first["mutations"][0]["fields"]["name"], "database accepted");
    assert_eq!(first["audit"]["comment"], "create example order");

    let target_transport = SqliteMutationExecutor::from_connection(Connection::open_in_memory()?);
    let target_metadata =
        InMemoryMetadataStore::new().with_entity(ExampleOrder::entity_descriptor());
    let target_service = SqlDataServiceExecutor::new(
        SqliteDialect,
        target_transport.clone(),
        target_metadata.clone(),
    );
    let mut target_context = UserContext::new().with_metadata(target_metadata);
    target_context.insert_resource(CommittedEventTarget::new(
        "example-rust-source",
        "sha256:0000000000000000000000000000000000000000000000000000000000000000",
    )?);
    target_context.use_sqlite_provider(target_transport.clone());
    target_context.register_executor(target_service);
    target_context.ensure_schema().await?;
    let delivered = transport
        .publish_committed_batch(
            "example-rust-source",
            "example-analytics",
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            10,
            |record| {
                let target = &target_transport;
                let target_context = &target_context;
                async move {
                    target
                        .apply_model_checked_committed_set(target_context, &record)
                        .await
                }
            },
        )
        .await?;
    assert_eq!(delivered.acknowledged, 1);
    assert_eq!(delivered.status.backlog, 0);
    let replicated_name: String = target_transport.connection().lock().unwrap().query_row(
        "SELECT name FROM committed_example_order WHERE id = 71",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(replicated_name, "database accepted");

    transport
        .connection()
        .lock()
        .unwrap()
        .execute_batch("DROP TABLE teaql_event_outbox")?;
    assert!(
        save_audited_ledger_entity(
            new_order(72, "must roll back").audit_as("reject broken outbox"),
            &context,
        )
        .await
        .is_err()
    );
    let count: i64 = transport.connection().lock().unwrap().query_row(
        "SELECT COUNT(*) FROM committed_example_order WHERE id = 72",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(count, 0);

    println!("COMMITTED_EVENTS_PASS authoritative=database accepted outbox=1 target=1 rollback=1");
    Ok(())
}
