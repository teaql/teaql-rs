//! Executor SPI qualification. Not an application-facing alternate save API.
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use teaql_core::dynamic_fields::{
    DynamicFieldDefinitions, DynamicFieldMutation, DynamicFieldSelection, DynamicFieldState,
    DynamicFieldValues,
};
use teaql_core::{DataType, EntityDescriptor, MutationIntent, QueryIntent, Value};
use teaql_data_service::dynamic_fields::DynamicFieldWrite;
use teaql_data_service::{QueryExecutor, SchemaProvider, Transaction, TransactionExecutor};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::dynamic_fields::{DatabaseDynamicFieldsProvider, DynamicFieldsProvider};
use teaql_runtime::{InMemoryMetadataStore, UserContext};
use teaql_sql::{SqlDataServiceExecutor, SqlTransport};

#[derive(Clone)]
struct Schema;
impl SchemaProvider for Schema {
    fn get_entity(&self, _: &str) -> Option<Arc<EntityDescriptor>> {
        None
    }
}
type Executor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;
fn executor() -> Executor {
    SqlDataServiceExecutor::new(
        SqliteDialect,
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open_in_memory().unwrap()),
        Schema,
    )
}
fn definitions(
    owner: &str,
    fields: impl IntoIterator<Item = (String, DataType)>,
) -> Arc<DynamicFieldDefinitions> {
    DynamicFieldDefinitions::new(owner, "test-v1", fields).unwrap()
}
fn write(
    definitions: Arc<DynamicFieldDefinitions>,
    namespace: &str,
    id: u64,
    values: impl IntoIterator<Item = (String, DynamicFieldMutation)>,
) -> DynamicFieldWrite {
    DynamicFieldWrite {
        definitions,
        namespace: namespace.into(),
        owner_id: id,
        changes: values.into_iter().collect(),
    }
}
fn query_intent() -> QueryIntent {
    QueryIntent::new("read typed extensions", "verify storage invariants").unwrap()
}
fn mutation_intent() -> MutationIntent {
    MutationIntent::new("framework graph persistence regression").unwrap()
}

#[test]
fn context_schema_initializes_storage_and_ambient_store_cannot_write() {
    futures_executor::block_on(async {
        let executor = executor();
        let mut context = UserContext::new().with_metadata(InMemoryMetadataStore::new());
        context.use_sqlite_provider(executor.transport.clone());
        context.register_executor(executor);
        let definitions = definitions("School", [("note".into(), DataType::Text)]);
        let provider = Arc::new(
            DatabaseDynamicFieldsProvider::<Executor>::new("extension-demo", [definitions.clone()])
                .unwrap(),
        );
        context.set_dynamic_fields_provider(provider.clone());
        context.ensure_schema().await.unwrap();
        context.ensure_schema().await.unwrap();
        let executor = context.require_resource::<Executor>().unwrap();
        let store = QueryExecutor::dynamic_field_store(executor).unwrap();
        assert!(!store.is_transaction_bound());
        let writes = [write(
            definitions,
            "extension-demo",
            1,
            [(
                "note".into(),
                DynamicFieldMutation::Set("PRIVATE-CANARY".into()),
            )],
        )];
        assert_eq!(
            store
                .apply_writes(&writes, &mutation_intent())
                .await
                .unwrap_err()
                .code,
            "DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED"
        );
        let loaded = provider
            .load_values(
                &context,
                "School",
                &[1, 2],
                &DynamicFieldSelection::All,
                &query_intent(),
            )
            .await
            .unwrap();
        assert_eq!(loaded.rows.len(), 2);
        assert!(loaded.rows.values().all(HashMap::is_empty));
        assert!(!format!("{writes:?}").contains("PRIVATE-CANARY"));
    });
}

#[test]
fn native_transaction_owns_dynamic_commit_rollback_and_typed_identity() {
    futures_executor::block_on(async {
        let executor = executor();
        let platform = definitions("Platform", [("note".into(), DataType::Text)]);
        let definitions = definitions("School", [("note".into(), DataType::Text)]);
        let store = QueryExecutor::dynamic_field_store(&executor).unwrap();
        store.ensure_schema().await.unwrap();
        executor
            .transport
            .connection()
            .lock()
            .unwrap()
            .execute_batch("CREATE TABLE native_probe(id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        let tx = executor.begin().await.unwrap();
        let transaction = QueryExecutor::dynamic_field_store(&tx).unwrap();
        assert!(transaction.is_transaction_bound());
        assert_eq!(store.binding_key(), transaction.binding_key());
        assert!(store.source_identity().is_some());
        assert_eq!(store.source_identity(), transaction.source_identity());
        let cloned = executor.transport.clone();
        assert_eq!(
            store.source_identity(),
            SqlTransport::dynamic_field_store(&cloned)
                .unwrap()
                .source_identity()
        );
        let separately_constructed = SqliteMutationExecutor::new(executor.transport.connection());
        assert_ne!(
            store.source_identity(),
            SqlTransport::dynamic_field_store(&separately_constructed)
                .unwrap()
                .source_identity()
        );
        assert_ne!(
            SqlTransport::dynamic_field_store(&separately_constructed)
                .unwrap()
                .binding_key(),
            transaction.binding_key()
        );
        let writes = [
            write(
                definitions.clone(),
                "first",
                1,
                [("note".into(), DynamicFieldMutation::Set("school".into()))],
            ),
            write(
                platform.clone(),
                "first",
                1,
                [("note".into(), DynamicFieldMutation::Set("platform".into()))],
            ),
            write(
                definitions.clone(),
                "second",
                1,
                [(
                    "note".into(),
                    DynamicFieldMutation::Set("other namespace".into()),
                )],
            ),
        ];
        transaction.validate_writes(&writes).unwrap();
        transaction
            .apply_writes(&writes, &mutation_intent())
            .await
            .unwrap();
        // Probe native DML on the transaction-owned transport, not ambient SQL.
        tx.transport
            .execute_sql(&teaql_sql::CompiledQuery {
                sql: "INSERT INTO native_probe(id,name) VALUES(1,?)".into(),
                params: vec!["native".into()],
                comment: Some("native transaction rollback probe".into()),
                log_context: Default::default(),
            })
            .await
            .unwrap();
        let result = transaction
            .load_values(
                "first",
                &definitions,
                &[1],
                &DynamicFieldSelection::All,
                &query_intent(),
            )
            .await
            .unwrap();
        assert_eq!(result[&1]["note"], Value::Text("school".into()));
        tx.rollback().await.unwrap();
        assert_eq!(
            executor
                .transport
                .connection()
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM native_probe", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(
            store
                .load_values(
                    "first",
                    &definitions,
                    &[1],
                    &DynamicFieldSelection::All,
                    &query_intent()
                )
                .await
                .unwrap()[&1]
                .is_empty()
        );

        let tx = executor.begin().await.unwrap();
        QueryExecutor::dynamic_field_store(&tx)
            .unwrap()
            .apply_writes(&writes, &mutation_intent())
            .await
            .unwrap();
        tx.commit().await.unwrap();
        for (namespace, definitions, expected) in [
            ("first", definitions.clone(), "school"),
            ("first", platform, "platform"),
            ("second", definitions.clone(), "other namespace"),
        ] {
            assert_eq!(
                store
                    .load_values(
                        namespace,
                        &definitions,
                        &[1],
                        &DynamicFieldSelection::All,
                        &query_intent()
                    )
                    .await
                    .unwrap()[&1]["note"],
                Value::Text(expected.into())
            );
        }
        let newer =
            DynamicFieldDefinitions::new("School", "test-v2", [("note".into(), DataType::Text)])
                .unwrap();
        assert_eq!(
            store
                .load_values(
                    "first",
                    &newer,
                    &[1],
                    &DynamicFieldSelection::All,
                    &query_intent()
                )
                .await
                .unwrap_err()
                .code,
            "DYNAMIC_FIELD_DEFINITION_MISMATCH"
        );
    });
}

#[test]
fn all_scalar_types_roundtrip_exactly_and_json_null_is_not_sql_null() {
    futures_executor::block_on(async {
        let executor = executor();
        let values: BTreeMap<String, (DataType, Value)> = [
            ("bool", DataType::Bool, Value::Bool(false)),
            ("signed", DataType::I64, Value::I64(i64::MIN)),
            ("unsigned", DataType::U64, Value::U64(u64::MAX)),
            (
                "float",
                DataType::F64,
                Value::F64(f64::from_bits(0x3fd5_5555_5555_5555)),
            ),
            ("negative_zero", DataType::F64, Value::F64(-0.0)),
            (
                "decimal",
                DataType::Decimal,
                Value::Decimal("12345678901234567890.12345678".parse().unwrap()),
            ),
            (
                "text",
                DataType::Text,
                Value::Text("'); DROP TABLE native_probe; -- 🌍".into()),
            ),
            ("empty", DataType::LargeText, Value::Text(String::new())),
            (
                "json",
                DataType::Json,
                Value::Json(serde_json::json!({"code":"é", "count":42})),
            ),
            (
                "json_null",
                DataType::Json,
                Value::Json(serde_json::Value::Null),
            ),
            (
                "date",
                DataType::Date,
                Value::Date("1995-09-01".parse().unwrap()),
            ),
            (
                "time",
                DataType::Timestamp,
                Value::Timestamp(teaql_core::time::Timestamp(1_750_000_000_123)),
            ),
            ("sql_null", DataType::Text, Value::Null),
        ]
        .into_iter()
        .map(|(code, data_type, value)| (code.into(), (data_type, value)))
        .collect();
        let definitions = definitions(
            "School",
            values
                .iter()
                .map(|(code, (data_type, _))| (code.clone(), *data_type)),
        );
        let store = QueryExecutor::dynamic_field_store(&executor).unwrap();
        store.ensure_schema().await.unwrap();
        let tx = executor.begin().await.unwrap();
        let writes = [write(
            definitions.clone(),
            "types",
            u64::MAX,
            values
                .iter()
                .map(|(code, (_, value))| (code.clone(), DynamicFieldMutation::Set(value.clone()))),
        )];
        QueryExecutor::dynamic_field_store(&tx)
            .unwrap()
            .apply_writes(&writes, &mutation_intent())
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let result = store
            .load_values(
                "types",
                &definitions,
                &[u64::MAX],
                &DynamicFieldSelection::All,
                &query_intent(),
            )
            .await
            .unwrap();
        for (code, (_, expected)) in &values {
            assert_eq!(&result[&u64::MAX][code], expected, "{code}");
            if let Value::F64(expected) = expected {
                let Value::F64(actual) = result[&u64::MAX][code] else {
                    panic!("float tag lost")
                };
                assert_eq!(expected.to_bits(), actual.to_bits());
            }
        }
        let fields =
            DynamicFieldValues::from_values(definitions, result[&u64::MAX].clone()).unwrap();
        assert_eq!(
            fields.field("json_null").unwrap().state(),
            DynamicFieldState::Value
        );
        assert_eq!(
            fields.field("sql_null").unwrap().state(),
            DynamicFieldState::Null
        );
    });
}

#[test]
fn null_delete_and_missing_remain_distinct_and_nonfinite_rejects_before_any_write() {
    futures_executor::block_on(async {
        let executor = executor();
        let definitions = definitions(
            "School",
            [
                ("note".into(), DataType::Text),
                ("amount".into(), DataType::F64),
            ],
        );
        let store = QueryExecutor::dynamic_field_store(&executor).unwrap();
        store.ensure_schema().await.unwrap();
        let tx = executor.begin().await.unwrap();
        let invalid = [
            write(
                definitions.clone(),
                "states",
                1,
                [(
                    "note".into(),
                    DynamicFieldMutation::Set("must-not-write".into()),
                )],
            ),
            write(
                definitions.clone(),
                "states",
                2,
                [(
                    "amount".into(),
                    DynamicFieldMutation::Set(Value::F64(f64::NAN)),
                )],
            ),
        ];
        let transaction = QueryExecutor::dynamic_field_store(&tx).unwrap();
        assert_eq!(
            transaction
                .apply_writes(&invalid, &mutation_intent())
                .await
                .unwrap_err()
                .code,
            "DYNAMIC_FIELD_NONFINITE_NUMBER"
        );
        assert!(
            transaction
                .load_values(
                    "states",
                    &definitions,
                    &[1, 2],
                    &DynamicFieldSelection::All,
                    &query_intent()
                )
                .await
                .unwrap()
                .values()
                .all(HashMap::is_empty)
        );
        let writes = [
            write(
                definitions.clone(),
                "states",
                1,
                [("note".into(), DynamicFieldMutation::Set(Value::Null))],
            ),
            write(
                definitions.clone(),
                "states",
                2,
                [("note".into(), DynamicFieldMutation::Set("second".into()))],
            ),
        ];
        transaction
            .apply_writes(&writes, &mutation_intent())
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let rows = store
            .load_values(
                "states",
                &definitions,
                &[1, 2, 3, 1],
                &DynamicFieldSelection::All,
                &query_intent(),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        let fields = DynamicFieldValues::from_batch(
            definitions.clone(),
            (1..=3).map(|id| rows[&id].clone()),
        )
        .unwrap();
        assert_eq!(
            fields[0].field("note").unwrap().state(),
            DynamicFieldState::Null
        );
        assert_eq!(
            fields[2].field("note").unwrap().state(),
            DynamicFieldState::NotLoaded
        );
        assert!(Arc::ptr_eq(
            fields[0].selected_codes(),
            fields[1].selected_codes()
        ));
        assert!(!Arc::ptr_eq(
            fields[1].selected_codes(),
            fields[2].selected_codes()
        ));
        let tx = executor.begin().await.unwrap();
        QueryExecutor::dynamic_field_store(&tx)
            .unwrap()
            .apply_writes(
                &[write(
                    definitions.clone(),
                    "states",
                    1,
                    [("note".into(), DynamicFieldMutation::Delete)],
                )],
                &mutation_intent(),
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            store
                .load_values(
                    "states",
                    &definitions,
                    &[1],
                    &DynamicFieldSelection::All,
                    &query_intent()
                )
                .await
                .unwrap()[&1]
                .is_empty()
        );
    });
}

#[test]
fn dropped_transaction_rolls_back_and_large_id_batch_is_bounded() {
    futures_executor::block_on(async {
        let executor = executor();
        let definitions = definitions("School", [("note".into(), DataType::Text)]);
        let store = QueryExecutor::dynamic_field_store(&executor).unwrap();
        store.ensure_schema().await.unwrap();
        {
            let tx = executor.begin().await.unwrap();
            QueryExecutor::dynamic_field_store(&tx)
                .unwrap()
                .apply_writes(
                    &[write(
                        definitions.clone(),
                        "large",
                        1,
                        [("note".into(), DynamicFieldMutation::Set("abandoned".into()))],
                    )],
                    &mutation_intent(),
                )
                .await
                .unwrap();
        }
        let rows = store
            .load_values(
                "large",
                &definitions,
                &(1..=1200).collect::<Vec<_>>(),
                &DynamicFieldSelection::All,
                &query_intent(),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1200);
        assert!(rows.values().all(HashMap::is_empty));
    });
}
