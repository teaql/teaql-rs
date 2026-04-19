use teaql_core::{DeleteCommand, Expr, RecoverCommand, UpdateCommand};
use teaql_dialect_sqlite::SqliteDialect;
use teaql_examples::{Order, SqliteSyncExecutor, reset_sqlite_schema, sqlite_context};
use teaql_runtime::sqlx_support::SqliteMutationExecutor;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let executor = SqliteMutationExecutor::new(pool.clone());
    reset_sqlite_schema(&pool, &executor).await?;

    let ctx = sqlite_context(executor);
    let repo = ctx.resolve_repository::<SqliteDialect, SqliteSyncExecutor>("Order")?;

    repo.insert(
        &repo
            .insert_command()
            .value("id", 1_u64)
            .value("version", 1_i64)
            .value("name", "draft"),
    )?;
    repo.update(
        &UpdateCommand::new("Order", 1_u64)
            .expected_version(1)
            .value("name", "submitted"),
    )?;
    repo.delete(&DeleteCommand::new("Order", 1_u64).expected_version(2))?;
    repo.recover(&RecoverCommand::new("Order", 1_u64, -3))?;

    let orders = repo.fetch_entities::<Order>(
        &repo
            .select()
            .project("id")
            .project("version")
            .project("name")
            .filter(Expr::eq("id", 1_u64)),
    )?;

    println!("schema+crud example rows: {orders:?}");
    Ok(())
}
