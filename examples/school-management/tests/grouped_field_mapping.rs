//! Read an existing legacy schema through logical names, including grouped rows.
//! DDL is fixture setup only; all data writes use audited Context mutation requests.
use std::sync::{Arc, Mutex};

use teaql_core::{
    CompactRow, DataType, EntityDescriptor, Expr, InsertCommand, OrderBy, PropertyDescriptor,
    RelationDescriptor, SelectQuery, Value,
};
use teaql_data_service::MutationCommand;
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt as _};
use teaql_runtime::{InMemoryMetadataStore, PurposedSelectQuery, UserContext};
use teaql_sql::{SqlDataServiceExecutor, SqlDialect};

type Executor =
    SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, InMemoryMetadataStore>;
type Pool = Arc<Mutex<rusqlite::Connection>>;

fn descriptors() -> (EntityDescriptor, EntityDescriptor) {
    let common = |name, table| {
        EntityDescriptor::new(name)
            .table_name(table)
            .property(
                PropertyDescriptor::new("id", DataType::I64)
                    .column_name("Legacy Id")
                    .id(),
            )
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .column_name("Legacy Revision")
                    .version(),
            )
    };
    let kind = common("LegacyKind", "legacy_kind")
        .property(PropertyDescriptor::new("name", DataType::Text).column_name("Legacy Label"));
    let record = common("LegacyRecord", "legacy_record")
        .property(PropertyDescriptor::new("kind_id", DataType::I64).column_name("Legacy Kind Key"))
        .property(
            PropertyDescriptor::new("display_name", DataType::Text)
                .column_name("Legacy Display Text"),
        )
        .property(PropertyDescriptor::new("quantity", DataType::I64).column_name("Legacy Quantity"))
        .relation(
            RelationDescriptor::new("kind", "LegacyKind")
                .local_key("kind_id")
                .foreign_key("id"),
        );
    (kind, record)
}

async fn setup() -> (UserContext, Pool, Vec<(String, String)>) {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            r#"
        CREATE TABLE legacy_kind (
            "Legacy Id" INTEGER PRIMARY KEY, "Legacy Revision" INTEGER,
            "Legacy Label" TEXT);
        CREATE TABLE legacy_record (
            "Legacy Id" INTEGER PRIMARY KEY, "Legacy Revision" INTEGER,
            "Legacy Kind Key" INTEGER, "Legacy Display Text" TEXT,
            "Legacy Quantity" INTEGER);
    "#,
        )
        .unwrap();
    let pool = Arc::new(Mutex::new(connection));
    let original_schema = schema(&pool);
    let (kind, record) = descriptors();
    let metadata = InMemoryMetadataStore::new()
        .with_entity(kind)
        .with_entity(record);
    let transport = SqliteMutationExecutor::new(pool.clone());
    let mut context = UserContext::new().with_metadata(metadata.clone());
    context.use_sqlite_provider(transport.clone());
    context.register_executor(Executor::new(SqliteDialect, transport, metadata));
    // The customer's tables already exist. Do not create a replacement schema
    // or rename columns to match Rust storage keys.
    context
        .execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                for command in [
                    InsertCommand::new("LegacyKind")
                        .value("id", 1001_i64)
                        .value("name", "Primary"),
                    InsertCommand::new("LegacyKind")
                        .value("id", 1002_i64)
                        .value("name", "Secondary"),
                    InsertCommand::new("LegacyRecord")
                        .value("id", 1_i64)
                        .value("kind_id", 1001_i64)
                        .value("display_name", "A")
                        .value("quantity", 10_i64),
                    InsertCommand::new("LegacyRecord")
                        .value("id", 2_i64)
                        .value("kind_id", 1001_i64)
                        .value("display_name", "A")
                        .value("quantity", 20_i64),
                    InsertCommand::new("LegacyRecord")
                        .value("id", 3_i64)
                        .value("kind_id", 1002_i64)
                        .value("display_name", "B")
                        .value("quantity", 40_i64),
                ] {
                    scope
                        .mutate(
                            MutationCommand::Insert(command.value("version", 1_i64))
                                .request("Seed legacy mapping example through Context")?,
                        )
                        .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    (context, pool, original_schema)
}

fn schema(pool: &Pool) -> Vec<(String, String)> {
    let connection = pool.lock().unwrap();
    let mut statement = connection.prepare("SELECT name, sql FROM sqlite_master WHERE type='table' AND name LIKE 'legacy_%' ORDER BY name").unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

async fn rows(context: &UserContext, query: SelectQuery) -> Vec<CompactRow> {
    let query = PurposedSelectQuery::new(
        query.limit(10).comment("Read legacy mapping example"),
        "Verify logical names survive SQL grouping without schema changes",
    );
    context
        .entity_data_service::<Executor>("LegacyRecord")
        .unwrap()
        .fetch_all(&query)
        .await
        .unwrap()
}

#[tokio::test]
async fn grouped_legacy_foreign_keys_preserve_logical_names_and_aliases() {
    let (context, pool, original) = setup().await;
    let result = rows(
        &context,
        SelectQuery::new("LegacyRecord")
            .group_by("kind_id")
            .project_expr("kindKey", Expr::column("kind_id"))
            .count("n")
            .sum("quantity", "total")
            .order_by(OrderBy::asc("kind_id")),
    )
    .await;
    assert_eq!(result.len(), 2);
    for (row, id, count, total) in [(&result[0], 1001, 2, 30), (&result[1], 1002, 1, 40)] {
        assert_eq!(row.get("kind_id").and_then(Value::try_i64), Some(id));
        assert_eq!(row.get("kindKey").and_then(Value::try_i64), Some(id));
        assert_eq!(row.get("n").and_then(Value::try_i64), Some(count));
        assert_eq!(row.get("total").and_then(Value::try_i64), Some(total));
        assert!(!row.contains_key("Legacy Kind Key"));
    }
    assert_eq!(schema(&pool), original);
}

#[tokio::test]
async fn grouped_legacy_relations_load_details_using_the_logical_foreign_key() {
    let (context, pool, original) = setup().await;
    let query = SelectQuery::new("LegacyRecord")
        .group_by("kind_id")
        .count("n")
        .order_by(OrderBy::asc("kind_id"))
        .object_group_by(
            "kind",
            "kind_id",
            SelectQuery::new("LegacyKind").project("name").limit(10),
        );
    let result = rows(&context, query).await;
    assert_eq!(result.len(), 2);
    for (row, id, label) in [
        (&result[0], 1001, "Primary"),
        (&result[1], 1002, "Secondary"),
    ] {
        assert_eq!(row.get("kind_id").and_then(Value::try_i64), Some(id));
        let Some(Value::Object(kind)) = row.get("kind") else {
            panic!("grouped kind details missing: {row:?}");
        };
        assert_eq!(kind.get("id").and_then(Value::try_i64), Some(id));
        assert_eq!(kind.get("name"), Some(&Value::from(label)));
    }
    assert_eq!(schema(&pool), original);
}

#[tokio::test]
async fn grouping_without_aggregates_returns_only_logical_dimensions() {
    let (context, pool, original) = setup().await;
    let result = rows(
        &context,
        SelectQuery::new("LegacyRecord")
            .project("id")
            .project("version")
            .project("display_name")
            .project("quantity")
            .group_by("display_name")
            .order_by(OrderBy::asc("display_name")),
    )
    .await;
    assert_eq!(result.len(), 2);
    for (row, name) in [(&result[0], "A"), (&result[1], "B")] {
        assert_eq!(row.get("display_name"), Some(&Value::from(name)));
        assert_eq!(
            row.len(),
            1,
            "ordinary entity projections cannot select a random member of a group"
        );
    }
    assert_eq!(schema(&pool), original);
}

#[tokio::test]
async fn grouped_legacy_smart_list_keeps_explicit_relation_enhancements() {
    let (context, pool, original) = setup().await;
    let query = PurposedSelectQuery::new(
        SelectQuery::new("LegacyRecord")
            .group_by("kind_id")
            .count("n")
            .order_by(OrderBy::asc("kind_id"))
            .limit(10)
            .object_group_by(
                "kind",
                "kind_id",
                SelectQuery::new("LegacyKind").project("name").limit(10),
            )
            .comment("Load legacy grouped SmartList details"),
        "Keep row and list execution semantics aligned",
    );
    let result = context
        .entity_data_service::<Executor>("LegacyRecord")
        .unwrap()
        .fetch_smart_list(&query)
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    for (row, id, label) in [
        (&result[0], 1001, "Primary"),
        (&result[1], 1002, "Secondary"),
    ] {
        assert_eq!(row.get("kind_id").and_then(Value::try_i64), Some(id));
        let Some(Value::Object(kind)) = row.get("kind") else {
            panic!("SmartList lost requested grouped details: {row:?}");
        };
        assert_eq!(kind.get("name"), Some(&Value::from(label)));
    }
    assert_eq!(schema(&pool), original);
}

#[test]
fn mapping_metadata_does_not_expose_legacy_columns_as_logical_api_fields() {
    let (_, record) = descriptors();
    assert_eq!(
        record.property_by_name("kind_id").unwrap().column_name,
        "Legacy Kind Key"
    );
    assert!(record.property_by_name("Legacy Kind Key").is_none());
    let bad = SelectQuery::new("LegacyRecord")
        .group_by("Legacy Kind Key")
        .count("n");
    assert!(SqliteDialect.compile_select(&record, &bad).is_err());
}
