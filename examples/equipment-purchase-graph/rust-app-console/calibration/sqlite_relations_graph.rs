use teaql_core::{Entity, SelectQuery, SmartList};
use teaql_examples::{Order, OrderLine, Product, sqlite_context};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
use teaql_runtime::{AuditedSaveExt, PurposedSelectQuery};

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

    for (id, name) in [(100, "keyboard"), (101, "mouse")] {
        Product {
            root: Default::default(),
            id,
            name: name.to_owned(),
        }
        .audit_as("Seed the product before creating an order line that references it")
        .save(&context)
        .await?;
    }

    Order {
        root: Default::default(),
        id: 1,
        version: 1,
        name: "graph-order".to_owned(),
        lines: SmartList::from(vec![
            OrderLine {
                root: Default::default(),
                id: 10,
                order_id: 1,
                name: "first-line".to_owned(),
                product_id: 100,
                product: None,
            },
            OrderLine {
                root: Default::default(),
                id: 11,
                order_id: 1,
                name: "second-line".to_owned(),
                product_id: 101,
                product: None,
            },
        ]),
    }
    .audit_as("Create the example order graph")
    .save(&context)
    .await?;

    let query = PurposedSelectQuery::new(
        SelectQuery::new("Order")
            .project("name")
            .relation_query("lines", SelectQuery::new("OrderLine").relation("product"))
            .order_asc("id"),
        "Display the saved example order",
    );
    let orders = data_service
        .fetch_enhanced_entities::<Order>(&query)
        .await?;

    println!("relation+graph example rows: {orders:?}");

    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].name, "graph-order");
    assert_eq!(orders[0].lines.len(), 2);
    assert_eq!(
        orders[0].lines[0].product.as_ref().unwrap().name,
        "keyboard"
    );
    assert_eq!(orders[0].lines[1].product.as_ref().unwrap().name, "mouse");

    Ok(())
}
