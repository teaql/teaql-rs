use teaql_core::{Entity, Expr, SmartList};
use teaql_examples::{Order, sqlite_context};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
use teaql_runtime::{AuditedSaveExt, EntityKey, PurposedSelectQuery};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = rusqlite::Connection::open_in_memory()?;
    let executor = SqliteMutationExecutor::from_connection(connection);
    let context = sqlite_context(executor);
    context.ensure_schema().await?;
    let data_service = context.entity_data_service::<teaql_sql::SqlDataServiceExecutor<
        SqliteDialect,
        SqliteMutationExecutor,
        teaql_runtime::InMemoryMetadataStore,
    >>("Order")?;

    Order {
        root: Default::default(),
        id: 1,
        version: 1,
        name: "draft".to_owned(),
        lines: SmartList::default(),
    }
    .audit_as("Create the example order")
    .save(&context)
    .await?;

    let query = PurposedSelectQuery::new(
        data_service
            .select()
            .project("id")
            .project("version")
            .project("name")
            .filter(Expr::eq("id", 1_u64))
            .comment("Read the order from the SQLite CRUD calibration"),
        "Check persisted business state at each CRUD boundary",
    );
    let draft = data_service
        .fetch_entities::<Order>(&query)
        .await?
        .into_iter()
        .next()
        .ok_or("The draft order was not created")?;
    assert_eq!((draft.name.as_str(), draft.version), ("draft", 1));
    let key = EntityKey::new("Order", draft.id);
    draft.root.set(key.clone(), "name", "submitted");
    draft
        .audit_as("Submit the example order")
        .save(&context)
        .await?;

    let submitted = data_service
        .fetch_entities::<Order>(&query)
        .await?
        .into_iter()
        .next()
        .ok_or("The submitted order could not be reloaded")?;
    assert_eq!((submitted.name.as_str(), submitted.version), ("submitted", 2));
    submitted.root.mark_as_delete(key);
    submitted
        .audit_as("Delete the example order")
        .save(&context)
        .await?;

    let orders = data_service.fetch_entities::<Order>(&query).await?;
    assert!(orders.is_empty(), "The soft-deleted order is still visible");
    println!("PASS native schema and CRUD calibration: draft v1, submitted v2, deleted row hidden");
    println!("schema+crud example rows: {orders:?}");
    Ok(())
}
