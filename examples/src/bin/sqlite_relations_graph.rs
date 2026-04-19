use teaql_core::{SelectQuery, SmartList};
use teaql_dialect_sqlite::SqliteDialect;
use teaql_examples::{
    Order, OrderLine, Product, SqliteSyncExecutor, reset_sqlite_schema, sqlite_context,
};
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

    repo.save_entity_graph_create(Order {
        id: 1,
        version: 1,
        name: "graph-order".to_owned(),
        lines: SmartList::from(vec![
            OrderLine {
                id: 10,
                order_id: 0,
                name: "first-line".to_owned(),
                product_id: 100,
                product: Some(Product {
                    id: 100,
                    name: "keyboard".to_owned(),
                }),
            },
            OrderLine {
                id: 11,
                order_id: 0,
                name: "second-line".to_owned(),
                product_id: 101,
                product: Some(Product {
                    id: 101,
                    name: "mouse".to_owned(),
                }),
            },
        ]),
    })?;

    let orders =
        repo.fetch_enhanced_entities::<Order>(&SelectQuery::new("Order").order_asc("id"))?;

    println!("relation+graph example rows: {orders:?}");
    Ok(())
}
