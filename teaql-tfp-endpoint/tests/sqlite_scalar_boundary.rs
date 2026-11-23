use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::json;
use teaql_core::{DataType, EntityDescriptor, InsertCommand, PropertyDescriptor};
use teaql_data_service::SchemaProvider;
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
use teaql_runtime::InMemoryMetadataStore;
use teaql_sql::{SqlDataServiceExecutor, SqlDialect};
use teaql_tfp_endpoint::{
    TfpEndpoint, TrustedEntityVisibility, TrustedQueryContext,
    wire_metadata_from_generated_with_model,
};

#[derive(Clone)]
struct Schema(Arc<EntityDescriptor>);

impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        (self.0.name == name).then(|| self.0.clone())
    }
}

fn scalar_order() -> EntityDescriptor {
    EntityDescriptor::new("ScalarOrder")
        .table_name("scalar_order_data")
        .property(PropertyDescriptor::new("id", DataType::I64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new("commerce_platform_id", DataType::I64).not_null())
        .property(PropertyDescriptor::new("amount", DataType::Decimal).not_null())
        .property(PropertyDescriptor::new("required_long", DataType::I64).not_null())
        .property(PropertyDescriptor::new("order_number", DataType::Text).not_null())
}

fn trusted(entity: &EntityDescriptor) -> TrustedQueryContext {
    let canonical = BTreeMap::from([
        ("id".into(), "id".into()),
        ("amount".into(), "amount".into()),
        ("required_long".into(), "required_long".into()),
        ("order_number".into(), "order_number".into()),
    ]);
    let model = InMemoryMetadataStore::new().with_entity(entity.clone());
    let wire_metadata = wire_metadata_from_generated_with_model(
        BTreeMap::from([(
            "ScalarOrder".into(),
            BTreeMap::from([
                ("id".into(), "id".into()),
                ("amount".into(), "amount".into()),
                ("required_long".into(), "requiredLong".into()),
                ("order_number".into(), "orderNumber".into()),
            ]),
        )]),
        BTreeMap::new(),
        &model,
    )
    .expect("generated model field types");
    TrustedQueryContext {
        tenant_field: "commerce_platform_id".into(),
        tenant_id: teaql_core::Value::I64(1),
        entity_visibility: BTreeMap::from([(
            "ScalarOrder".into(),
            TrustedEntityVisibility::Versioned {
                field: "version".into(),
            },
        )]),
        authenticated_user: "scalar-verifier".into(),
        approved_purpose: "typed-scalar-query".into(),
        allowed_entities: BTreeSet::from(["ScalarOrder".into()]),
        field_mappings: BTreeMap::from([("ScalarOrder".into(), canonical)]),
        writable_field_mappings: BTreeMap::new(),
        allowed_actions: BTreeMap::new(),
        wire_metadata,
        max_page_size: 10,
        max_offset: 100,
    }
}

#[tokio::test]
async fn tfp_scalar_strings_use_model_types_in_real_sqlite_query() {
    let entity = Arc::new(scalar_order());
    let transport = SqliteMutationExecutor::from_connection(
        rusqlite::Connection::open_in_memory().expect("in-memory sqlite"),
    );
    transport
        .connection()
        .lock()
        .expect("sqlite connection")
        .execute_batch(
            "CREATE TABLE scalar_order_data (\
                id INTEGER PRIMARY KEY NOT NULL, \
                version INTEGER NOT NULL, \
                commerce_platform_id INTEGER NOT NULL, \
                amount TEXT NOT NULL, \
                required_long INTEGER NOT NULL, \
                order_number TEXT NOT NULL\
            )",
        )
        .expect("create scalar table");
    let amount = teaql_core::Decimal::from_str_exact("123456789012.3456789").unwrap();
    let insert = InsertCommand::new("ScalarOrder")
        .value("id", 1_i64)
        .value("version", 1_i64)
        .value("commerce_platform_id", 1_i64)
        .value("amount", teaql_core::Value::Decimal(amount))
        .value("required_long", 9007199254740993_i64)
        .value("order_number", "123.45");
    transport
        .execute(
            &SqliteDialect
                .compile_insert(&entity, &insert)
                .expect("compile insert"),
        )
        .expect("seed scalar row");
    let executor = Arc::new(SqlDataServiceExecutor::new(
        SqliteDialect,
        transport,
        Schema(entity.clone()),
    ));
    let endpoint = TfpEndpoint::new(executor.clone(), executor);
    let trusted = trusted(&entity);
    let query = json!({
        "entity": "ScalarOrder",
        "filterCondition": {"$and": [
            {"amount": {"$eq": "123456789012.3456789"}},
            {"requiredLong": {"$eq": "9007199254740993"}},
            {"orderNumber": {"$eq": "123.45"}}
        ]},
        "selectItems": ["id", "amount", "requiredLong", "orderNumber"],
        "limitValue": 1,
        "commentText": "load one scalar order",
        "purposeText": "verify model-typed TFP predicates against SQLite"
    });
    let result = endpoint
        .handle_query(&trusted, query)
        .await
        .expect("typed query");
    let rows = result["data"].as_array().expect("response rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["amount"], "123456789012.3456789");
    assert_eq!(rows[0]["requiredLong"], 9007199254740993_i64);
    assert_eq!(rows[0]["orderNumber"], "123.45");

    for invalid in [
        json!({"amount": {"$eq": 3.5}}),
        json!({"amount": {"$eq": "not-a-decimal"}}),
        json!({"requiredLong": {"$eq": "9223372036854775808"}}),
    ] {
        let error = endpoint
            .handle_query(
                &trusted,
                json!({
                    "entity": "ScalarOrder",
                    "filterCondition": invalid,
                    "limitValue": 1,
                    "commentText": "reject malformed scalar input",
                    "purposeText": "prove type safety at TFP ingress"
                }),
            )
            .await
            .expect_err("invalid scalar predicate");
        assert_eq!(error.code(), "TFP_INVALID_REQUEST");
    }
}
