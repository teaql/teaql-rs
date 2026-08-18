use teaql_core::{Entity, Expr, SmartList};
use teaql_examples::{Order, reset_sqlite_schema, sqlite_context};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
use teaql_runtime::{AuditedSaveExt, EntityKey, PurposedSelectQuery};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = rusqlite::Connection::open_in_memory()?;
    let executor = SqliteMutationExecutor::from_connection(connection);
    reset_sqlite_schema(&executor).await?;

    let context = sqlite_context(executor);
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

    Order {
        root: Default::default(),
        id: 1,
        version: 1,
        name: "submitted".to_owned(),
        lines: SmartList::default(),
    }
    .audit_as("Submit the example order")
    .save(&context)
    .await?;

    let deleted = Order {
        root: Default::default(),
        id: 1,
        version: 2,
        name: "submitted".to_owned(),
        lines: SmartList::default(),
    };
    deleted.root.mark_as_delete(EntityKey::new("Order", 1_u64));
    deleted
        .audit_as("Delete the example order")
        .save(&context)
        .await?;

    let query = PurposedSelectQuery::new(
        data_service
            .select()
            .project("id")
            .project("version")
            .project("name")
            .filter(Expr::eq("id", 1_u64)),
        "Inspect the soft-deleted example order",
    );
    let orders = data_service.fetch_entities::<Order>(&query).await?;

    println!("schema+crud example rows: {orders:?}");
    Ok(())
}
