//! Real SQLite relation diagnostics: provenance belongs to one query tree, not UserContext.
use rusqlite::Connection;
use teaql_core::{
    CompactRow, DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor,
    RelationAggregate, RelationDescriptor, SelectQuery, TraceKind, TraceNode, Value,
};
use teaql_data_service::{MutationRequest, SqlExecutionOutcome};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt as _};
use teaql_runtime::{InMemoryMetadataStore, PurposedSelectQuery, UserContext};

type Executor =
    teaql_sql::SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, InMemoryMetadataStore>;
const BUSINESS: &str = "Riverside";
const CREDENTIAL: &str = "RELATION-PASSWORD-CANARY";

// Runtime-owned typed adapters exercise compact/identity-graph hydration without
// inspecting or modifying a generated library.
macro_rules! graph_entity {
    ($name:ident, $table:literal) => {
        #[derive(Clone)]
        struct $name {
            row: CompactRow,
            state: teaql_runtime::EntityRuntimeState,
        }
        impl teaql_core::TeaqlEntity for $name {
            const ENTITY_NAME: &'static str = stringify!($name);
            fn entity_descriptor() -> EntityDescriptor {
                descriptor(stringify!($name), $table)
            }
        }
        impl teaql_core::Entity for $name {
            fn from_compact_row(row: CompactRow) -> Result<Self, teaql_core::EntityError> {
                Ok(Self {
                    row,
                    state: Default::default(),
                })
            }
            fn into_values(self) -> teaql_core::MutationValues {
                self.row.into_map().into()
            }
            fn on_loaded(&mut self, context: &dyn std::any::Any) {
                self.state = context
                    .downcast_ref::<teaql_runtime::EntityRuntimeState>()
                    .unwrap()
                    .clone();
            }
        }
        impl teaql_core::IdentifiableEntity for $name {
            fn id_value(&self) -> Value {
                self.row.get("id").unwrap().clone()
            }
        }
    };
}
graph_entity!(Customer, "customers");
graph_entity!(Order, "orders");
graph_entity!(Item, "items");

fn descriptor(name: &str, table: &str) -> EntityDescriptor {
    EntityDescriptor::new(name)
        .table_name(table)
        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
}

async fn fixture() -> (UserContext, SqliteMutationExecutor) {
    let metadata = InMemoryMetadataStore::new()
        .with_entity(
            descriptor("Customer", "customers")
                .property(PropertyDescriptor::new("name", DataType::Text))
                .property(PropertyDescriptor::new("password", DataType::Text))
                .audit_mask_fields(vec!["name".into()])
                .relation(
                    RelationDescriptor::new("orders", "Order")
                        .local_key("id")
                        .foreign_key("customer_id")
                        .many(),
                ),
        )
        .with_entity(
            descriptor("Order", "orders")
                .property(PropertyDescriptor::new("customer_id", DataType::U64))
                .relation(
                    RelationDescriptor::new("customer", "Customer")
                        .local_key("customer_id")
                        .foreign_key("id"),
                )
                .relation(
                    RelationDescriptor::new("items", "Item")
                        .local_key("id")
                        .foreign_key("order_id")
                        .many(),
                ),
        )
        .with_entity(
            descriptor("Item", "items")
                .property(PropertyDescriptor::new("order_id", DataType::U64)),
        );
    let transport = SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
    let mut context = UserContext::new().with_metadata(metadata.clone());
    let mut decoders = teaql_runtime::InMemoryEntityGraphDecoderRegistry::default();
    decoders.register::<Customer>();
    decoders.register::<Order>();
    decoders.register::<Item>();
    context.set_entity_graph_decoder_registry(decoders);
    context.use_sqlite_provider(transport.clone());
    context.register_executor(Executor::new(SqliteDialect, transport.clone(), metadata));
    context.ensure_schema().await.unwrap();
    context
        .execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                for (name, fields) in [
                    (
                        "Customer",
                        vec![
                            ("name", Value::from(BUSINESS)),
                            ("password", Value::from(CREDENTIAL)),
                        ],
                    ),
                    ("Order", vec![("customer_id", Value::U64(1))]),
                    ("Item", vec![("order_id", Value::U64(1))]),
                ] {
                    let mut command = InsertCommand::new(name)
                        .value("id", 1_u64)
                        .value("version", 1_i64);
                    for (field, value) in fields {
                        command = command.value(field, value);
                    }
                    command.trace_chain.push(TraceNode::typed(
                        TraceKind::AuditReason,
                        name,
                        Some(1),
                        "seed relation mask fixture",
                    ));
                    scope.mutate(MutationRequest::Insert(command)).await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    context.clear_sql_logs();
    (context, transport)
}

fn query(shape: &str) -> PurposedSelectQuery {
    let flat = shape.starts_with("flat_");
    let shape = shape.strip_prefix("flat_").unwrap_or(shape);
    let mut child = SelectQuery::new("Order").projects(["id", "version", "customer_id"]);
    if shape != "batch" {
        child = child.limit(2);
    }
    if shape == "window" || (flat && shape == "nested") {
        child = child.top_n_probe_parent_threshold(0);
    }
    if shape == "probe" {
        child = child.top_n_probe_parent_threshold(32);
    }
    if shape == "nested" {
        child = child.relation_query("items", SelectQuery::new("Item").limit(2));
    }
    if shape == "forward" {
        child = child.relation_query("customer", SelectQuery::new("Customer").limit(2));
    }
    let mut root = SelectQuery::new("Customer")
        .projects(["id", "version", "name", "password"])
        .filter(Expr::and([
            Expr::eq("name", BUSINESS),
            Expr::eq("password", CREDENTIAL),
        ]))
        .limit(10)
        .comment(format!("what: load graph for {BUSINESS} {CREDENTIAL}"));
    if shape != "aggregate" {
        root = root.relation_query("orders", child);
    }
    PurposedSelectQuery::new(root, format!("why: inspect {BUSINESS} {CREDENTIAL}"))
}

async fn execute<E>(
    repo: &teaql_runtime::EntityDataService<'_, E>,
    shape: &str,
) -> Result<Vec<CompactRow>, teaql_runtime::DataServiceError<E::Error>>
where
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync,
{
    if shape.starts_with("flat_") {
        let entities = if shape == "flat_nested" {
            repo.fetch_enhanced_entities_with_relation_aggregates_owned::<Customer>(
                query(shape),
                &[],
            )
            .await?
        } else {
            repo.fetch_enhanced_entities::<Customer>(&query(shape))
                .await?
        };
        let mut rows = Vec::new();
        for entity in entities {
            let orders = entity
                .state
                .resolve_relation_list::<Order>("Customer", 1, "orders")
                .unwrap();
            assert_eq!(orders.len(), 1, "typed graph must really be hydrated");
            assert_eq!(
                orders[0].row.get("customer_id").and_then(Value::try_u64),
                Some(1)
            );
            if shape == "flat_nested" {
                assert_eq!(
                    entity
                        .state
                        .resolve_relation_list::<Item>("Order", 1, "items")
                        .unwrap()
                        .len(),
                    1
                );
            }
            rows.push(entity.row);
        }
        return Ok(rows);
    }
    if shape == "aggregate" {
        let list = repo
            .fetch_smart_list_with_relation_aggregates(
                &query(shape),
                &[RelationAggregate::new(
                    "orders",
                    "order_count",
                    SelectQuery::new("Order").count("value"),
                    true,
                )],
            )
            .await?;
        Ok(list.into_iter().collect())
    } else {
        repo.fetch_all(&query(shape)).await
    }
}

async fn run(shape: &str, transaction: bool, failure: bool) {
    let (context, transport) = fixture().await;
    let repository = context.entity_data_service::<Executor>("Customer").unwrap();
    let failing_table = if shape.ends_with("nested") {
        "items"
    } else {
        "orders"
    };
    if failure {
        transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch(&format!(
                "ALTER TABLE {failing_table} RENAME TO unavailable_relation"
            ))
            .unwrap();
    }
    let result = if transaction {
        context
            .execute_in_transaction::<Executor, _, _>(|scope| {
                Box::pin(async move {
                    // Return the query result separately: this fixture tests statement diagnostics,
                    // not whether a successful parent SQL implies a committed business operation.
                    Ok(
                        execute(&scope.entity_data_service("Customer").unwrap(), shape)
                            .await
                            .map_err(|error| error.to_string()),
                    )
                })
            })
            .await
            .unwrap()
    } else {
        execute(&repository, shape)
            .await
            .map_err(|error| error.to_string())
    };
    assert_eq!(result.is_err(), failure, "{shape}: {result:?}");
    if let Ok(rows) = &result {
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("name"),
            Some(&Value::from(BUSINESS)),
            "driver values must remain original"
        );
        if shape != "aggregate" && !shape.starts_with("flat_") {
            assert!(
                matches!(rows[0].get("orders"), Some(Value::List(items)) if items.len() == 1),
                "rows: {rows:?}"
            );
        }
    }
    let logs = context.sql_logs();
    // The current row-based loader fetches descendants both within the child
    // request and while attaching the relation plan. Retain that existing four-
    // statement behavior here; changing query planning is not this mask fix.
    let expected = if shape == "flat_nested" {
        3
    } else if (shape == "nested" || shape == "forward") && !failure {
        4
    } else if shape == "nested" {
        3
    } else {
        2
    };
    assert_eq!(
        logs.len(),
        expected,
        "one log per executed statement: {logs:#?}"
    );
    for (index, entry) in logs.iter().enumerate() {
        let retained = format!("{entry:?}");
        assert!(
            !retained.contains(BUSINESS),
            "{shape} tx={transaction} failure={failure}: sensitive intent in statement {index}: {retained}"
        );
        assert!(
            !retained.contains(CREDENTIAL),
            "credentials must remain masked, including explicit debug"
        );
        assert!(
            entry
                .comment
                .as_deref()
                .is_some_and(|s| s.contains("what: load graph"))
        );
        assert!(
            entry
                .purpose
                .as_deref()
                .is_some_and(|s| s.contains("why: inspect"))
        );
        assert!(!entry.debug_sql.is_empty());
        assert_eq!(
            entry.log_context.execution_outcome,
            Some(if failure && index + 1 == expected {
                SqlExecutionOutcome::Failure
            } else {
                SqlExecutionOutcome::Success
            })
        );
    }
    // Reusing the same context/repository must not inherit the earlier redaction list.
    context.clear_sql_logs();
    let ordinary = PurposedSelectQuery::new(
        SelectQuery::new("Customer")
            .limit(1)
            .comment(format!("what: independent {BUSINESS}")),
        "why: query-local provenance",
    );
    repository.fetch_all(&ordinary).await.unwrap();
    assert!(
        context.sql_logs()[0]
            .comment
            .as_deref()
            .unwrap()
            .contains(BUSINESS)
    );
}

#[test]
fn relation_file_sinks() {
    use std::process::Command;
    let directory = std::env::temp_dir().join(format!(
        "teaql-rust-relation-mask-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    for (debug, cached) in [(false, false), (true, false), (false, true), (true, true)] {
        let ordinary = directory.join(format!("ordinary-{debug}-cached-{cached}.log"));
        let sensitive = directory.join(format!("explicit-{debug}-cached-{cached}.log"));
        let expected_sql = if cached { "FROM orders" } else { "FROM items" };
        let expected_purpose = if cached {
            "why: cached parent"
        } else {
            "why: inspect"
        };
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "relation_file_fixture",
                "--nocapture",
            ])
            .env("TEAQL_LOG_ENDPOINT", &ordinary)
            .env("TEAQL_SQL_DEBUG_ENDPOINT", &sensitive)
            .env("TEAQL_SQL_LOG", "_full_with_payload")
            .env("TEAQL_LOG_FORMAT", "debug")
            .env(
                "TEAQL_RELATION_CACHE_FIXTURE",
                if cached { "yes" } else { "no" },
            )
            .env_remove("TEAQL_TRACE_MODE")
            .env_remove("TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS");
        if debug {
            command.env(
                "TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS",
                "I_UNDERSTAND_SENSITIVE_DATA_MAY_BE_WRITTEN_TO_DISK",
            );
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let ordinary = std::fs::read_to_string(ordinary).unwrap();
        assert!(!ordinary.contains(BUSINESS));
        assert!(!ordinary.contains(CREDENTIAL));
        assert!(ordinary.contains("what: load graph"));
        assert!(ordinary.contains(expected_purpose));
        assert!(
            ordinary.contains(expected_sql),
            "failed descendant SQL must remain visible"
        );
        if debug {
            let sensitive = std::fs::read_to_string(sensitive).unwrap();
            assert!(!sensitive.contains(CREDENTIAL));
            assert!(sensitive.contains(BUSINESS));
            assert!(sensitive.contains(expected_sql));
            assert!(sensitive.contains("EXPLICIT OPT-IN"));
            let failed_child = sensitive
                .lines()
                .find(|line| {
                    line.contains("execution_outcome: Some(Failure)") && line.contains(expected_sql)
                })
                .expect("actual failed child must be logged");
            assert!(
                failed_child.contains(&format!("{expected_purpose} {BUSINESS} [REDACTED]")),
                "debug child intent must use current field classification"
            );
        } else {
            assert!(
                !sensitive.exists(),
                "endpoint alone must not enable a sensitive sink"
            );
            assert!(ordinary.contains("NOT REPLAYABLE"));
        }
    }
    println!("relation file evidence retained at {}", directory.display());
}

#[tokio::test]
#[ignore = "executed four times by relation_file_sinks with isolated environment and file sinks"]
async fn relation_file_fixture() {
    if std::env::var("TEAQL_RELATION_CACHE_FIXTURE").as_deref() == Ok("yes") {
        cached_parent(true, true).await;
        return;
    }
    let (context, transport) = fixture().await;
    transport
        .connection()
        .lock()
        .unwrap()
        .execute_batch("ALTER TABLE items RENAME TO unavailable_relation")
        .unwrap();
    context
        .execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                assert!(
                    execute(
                        &scope.entity_data_service("Customer").unwrap(),
                        "flat_nested"
                    )
                    .await
                    .is_err()
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(context.sql_logs().len(), 3);
}

async fn cached_parent(transaction: bool, failure: bool) {
    let (mut context, transport) = fixture().await;
    context.insert_resource(teaql_runtime::InMemoryAggregationCache::with_namespace(
        "mask-parent",
    ));
    let query = PurposedSelectQuery::new(
        query("batch").into_query().enable_aggregation_cache(),
        format!("why: cached parent {BUSINESS} {CREDENTIAL}"),
    );
    let repo = context.entity_data_service::<Executor>("Customer").unwrap();
    assert_eq!(repo.fetch_all(&query).await.unwrap().len(), 1);
    assert_eq!(context.sql_logs().len(), 2);
    context.clear_sql_logs();
    if failure {
        transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch("ALTER TABLE orders RENAME TO unavailable_orders")
            .unwrap();
    }
    let result = if transaction {
        context
            .execute_in_transaction::<Executor, _, _>(|scope| {
                Box::pin(async move {
                    Ok(scope
                        .entity_data_service("Customer")
                        .unwrap()
                        .fetch_all(&query)
                        .await
                        .map_err(|e| e.to_string()))
                })
            })
            .await
            .unwrap()
    } else {
        repo.fetch_all(&query).await.map_err(|e| e.to_string())
    };
    assert_eq!(result.is_err(), failure);
    let logs = context.sql_logs();
    assert_eq!(
        logs.len(),
        1,
        "cached parent must not be executed or logged as SQL again"
    );
    let child = &logs[0];
    assert!(child.sql.contains("FROM orders"));
    let retained = format!("{child:?}");
    assert!(
        !retained.contains(BUSINESS),
        "cache lost masked ancestry: {retained}"
    );
    assert!(!retained.contains(CREDENTIAL));
    assert!(
        child
            .purpose
            .as_deref()
            .unwrap()
            .contains("why: cached parent")
    );
    assert!(
        child
            .comment
            .as_deref()
            .unwrap()
            .contains("what: load graph")
    );
}

#[tokio::test]
async fn cached_parent_success() {
    cached_parent(false, false).await;
}
#[tokio::test]
async fn cached_parent_failure() {
    cached_parent(false, true).await;
}
#[tokio::test]
async fn cached_parent_tx_success() {
    cached_parent(true, false).await;
}
#[tokio::test]
async fn cached_parent_tx_failure() {
    cached_parent(true, true).await;
}

#[tokio::test]
async fn retained_id_set_hit_preserves_binding_sources_for_typed_relations() {
    let (context, _) = fixture().await;
    let query = PurposedSelectQuery::new(
        query("flat_nested")
            .into_query()
            .optimize_pagination_with_id_set_config("mask-ids", 60, 100),
        format!("why: retained ID set {BUSINESS} {CREDENTIAL}"),
    );
    let repo = context.entity_data_service::<Executor>("Customer").unwrap();
    for expected_statements in [4, 3] {
        context.clear_sql_logs();
        let entities = repo
            .fetch_enhanced_entities::<Customer>(&query)
            .await
            .unwrap();
        assert_eq!(entities.total_count, Some(1));
        assert_eq!(entities.len(), 1);
        assert_eq!(
            entities[0]
                .state
                .resolve_relation_list::<Item>("Order", 1, "items")
                .unwrap()
                .len(),
            1
        );
        let logs = context.sql_logs();
        assert_eq!(logs.len(), expected_statements);
        for log in logs {
            let text = format!("{log:?}");
            assert!(!text.contains(BUSINESS));
            assert!(!text.contains(CREDENTIAL));
            assert!(
                log.purpose
                    .as_deref()
                    .unwrap()
                    .contains("why: retained ID set")
            );
        }
    }
}

macro_rules! cases {
    ($($name:ident => ($shape:literal, $tx:literal, $failure:literal)),* $(,)?) => {$ (
        #[tokio::test] async fn $name() { run($shape, $tx, $failure).await; }
    )*};
}
cases! {
    flat_batch => ("flat_batch", false, false), flat_batch_failure => ("flat_batch", false, true),
    flat_batch_tx => ("flat_batch", true, false), flat_batch_tx_failure => ("flat_batch", true, true),
    flat_nested => ("flat_nested", false, false), flat_nested_failure => ("flat_nested", false, true),
    flat_nested_tx => ("flat_nested", true, false), flat_nested_tx_failure => ("flat_nested", true, true),
    batch => ("batch", false, false), batch_failure => ("batch", false, true),
    batch_tx => ("batch", true, false), batch_tx_failure => ("batch", true, true),
    probe => ("probe", false, false), probe_failure => ("probe", false, true),
    probe_tx => ("probe", true, false), probe_tx_failure => ("probe", true, true),
    window => ("window", false, false), window_failure => ("window", false, true),
    window_tx => ("window", true, false), window_tx_failure => ("window", true, true),
    nested => ("nested", false, false), nested_failure => ("nested", false, true),
    nested_tx => ("nested", true, false), nested_tx_failure => ("nested", true, true),
    forward => ("forward", false, false), forward_failure => ("forward", false, true),
    forward_tx => ("forward", true, false), forward_tx_failure => ("forward", true, true),
    aggregate => ("aggregate", false, false), aggregate_failure => ("aggregate", false, true),
    aggregate_tx => ("aggregate", true, false), aggregate_tx_failure => ("aggregate", true, true),
}
