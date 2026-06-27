use teaql_core::{SelectQuery, SmartList};
use teaql_examples::{Order, OrderLine, Product, reset_sqlite_schema, sqlite_context};
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
        .save_entity_graph(Order {
            root: Default::default(),
            id: 1,
            version: 1,
            name: "graph-order".to_owned(),
            lines: SmartList::from(vec![
                OrderLine {
                    root: Default::default(),
                    id: 10,
                    order_id: 0,
                    name: "first-line".to_owned(),
                    product_id: 100,
                    product: Some(Product {
                        root: Default::default(),
                        id: 100,
                        name: "keyboard".to_owned(),
                    }),
                },
                OrderLine {
                    root: Default::default(),
                    id: 11,
                    order_id: 0,
                    name: "second-line".to_owned(),
                    product_id: 101,
                    product: Some(Product {
                        root: Default::default(),
                        id: 101,
                        name: "mouse".to_owned(),
                    }),
                },
            ]),
        })
        .await?;

    let orders = data_service
        .fetch_enhanced_entities::<Order>(&SelectQuery::new("Order").order_asc("id"))
        .await?;

    println!("relation+graph example rows: {orders:?}");
    Ok(())
}
