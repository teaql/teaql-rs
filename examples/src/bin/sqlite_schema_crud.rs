use teaql_core::{DeleteCommand, Expr, RecoverCommand, UpdateCommand};
use teaql_examples::{Order, reset_sqlite_schema, sqlite_context};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection = rusqlite::Connection::open_in_memory()?;
    let executor = SqliteMutationExecutor::from_connection(connection);
    reset_sqlite_schema(&executor).await?;

    let ctx = sqlite_context(executor);
    let data_service = ctx.entity_data_service::<teaql_sql::SqlDataServiceExecutor<
        SqliteDialect,
        SqliteMutationExecutor,
        teaql_runtime::InMemoryMetadataStore,
    >>("Order")?;

    data_service
        .insert(
            &data_service
                .insert_command()
                .value("id", 1_u64)
                .value("version", 1_i64)
                .value("name", "draft"),
        )
        .await?;
    data_service
        .update(
            &UpdateCommand::new("Order", 1_u64)
                .expected_version(1)
                .value("name", "submitted"),
        )
        .await?;
    data_service
        .delete(&DeleteCommand::new("Order", 1_u64).expected_version(2))
        .await?;
    data_service
        .recover(&RecoverCommand::new("Order", 1_u64, -3))
        .await?;

    let orders = data_service
        .fetch_entities::<Order>(
            &data_service
                .select()
                .project("id")
                .project("version")
                .project("name")
                .filter(Expr::eq("id", 1_u64)),
        )
        .await?;

    println!("schema+crud example rows: {orders:?}");
    Ok(())
}
