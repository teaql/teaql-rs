use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::json;
use teaql_core::{DataType, EntityDescriptor, InsertCommand, PropertyDescriptor};
use teaql_data_service::SchemaProvider;
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
use teaql_sql::{SqlDataServiceExecutor, SqlDialect};
use teaql_tfp_endpoint::{TfpEndpoint, TfpEndpointError, TrustedQueryContext, WireEntityMetadata};

#[derive(Clone)]
struct Schema(Arc<EntityDescriptor>);

impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        (self.0.name == name).then(|| self.0.clone())
    }
}

fn customer_order_entity() -> EntityDescriptor {
    EntityDescriptor::new("CustomerOrder")
        .table_name("customer_order_data")
        .property(PropertyDescriptor::new("id", DataType::I64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new("commerce_platform_id", DataType::I64).not_null())
        .property(PropertyDescriptor::new("order_number", DataType::Text).not_null())
}

fn trusted_tenant_one() -> TrustedQueryContext {
    let canonical_policy = BTreeMap::from([
        ("id".into(), "id".into()),
        ("version".into(), "version".into()),
        ("order_number".into(), "order_number".into()),
    ]);
    let wire_metadata = WireEntityMetadata::new(
        BTreeMap::from([
            ("id".into(), "id".into()),
            ("version".into(), "version".into()),
            ("order_number".into(), "orderNumber".into()),
        ]),
        BTreeMap::new(),
    )
    .expect("wire metadata");

    TrustedQueryContext {
        tenant_field: "commerce_platform_id".into(),
        tenant_id: teaql_core::Value::I64(1),
        active_version_fields: BTreeMap::from([("CustomerOrder".into(), "version".into())]),
        authenticated_user: "tenant-one-operator".into(),
        approved_purpose: "tenant-boundary-regression".into(),
        allowed_entities: BTreeSet::from(["CustomerOrder".into()]),
        field_mappings: BTreeMap::from([("CustomerOrder".into(), canonical_policy)]),
        writable_field_mappings: BTreeMap::from([(
            "CustomerOrder".into(),
            BTreeMap::from([("order_number".into(), "order_number".into())]),
        )]),
        allowed_actions: BTreeMap::from([(
            "CustomerOrder".into(),
            BTreeSet::from(["Update".into(), "Delete".into(), "Recover".into()]),
        )]),
        wire_metadata: BTreeMap::from([("CustomerOrder".into(), wire_metadata)]),
        max_page_size: 100,
    }
}

#[tokio::test]
async fn tfp_json_enforces_tenant_boundary_in_real_sqlite_statement() {
    let entity = Arc::new(customer_order_entity());
    let transport = SqliteMutationExecutor::from_connection(
        rusqlite::Connection::open_in_memory().expect("in-memory sqlite"),
    );
    transport
        .connection()
        .lock()
        .expect("sqlite connection")
        .execute_batch(
            "CREATE TABLE customer_order_data (\
                id INTEGER PRIMARY KEY NOT NULL, \
                version INTEGER NOT NULL, \
                commerce_platform_id INTEGER NOT NULL, \
                order_number TEXT NOT NULL\
            )",
        )
        .expect("create customer_order_data");

    for (id, version, tenant_id, order_number) in [
        (1_i64, 1_i64, 1_i64, "TENANT-ONE-ORDER"),
        (2_i64, 1_i64, 2_i64, "TENANT-TWO-ORDER"),
        (3_i64, -2_i64, 2_i64, "TENANT-TWO-DELETED"),
        (4_i64, -2_i64, 1_i64, "TENANT-ONE-DELETED"),
    ] {
        let insert = InsertCommand::new("CustomerOrder")
            .value("id", id)
            .value("version", version)
            .value("commerce_platform_id", tenant_id)
            .value("order_number", order_number);
        transport
            .execute(
                &SqliteDialect
                    .compile_insert(&entity, &insert)
                    .expect("compile seed insert"),
            )
            .expect("seed order");
    }

    let executor = Arc::new(SqlDataServiceExecutor::new(
        SqliteDialect,
        transport.clone(),
        Schema(entity),
    ));
    let endpoint = TfpEndpoint::new(executor.clone(), executor);
    let trusted = trusted_tenant_one();

    let query = json!({
        "entity": "CustomerOrder",
        "selectItems": ["id", "version", "orderNumber"],
        "_limit": 10,
        "_comment": "what: read orders visible to tenant one",
        "_purpose": "why: verify the trusted tenant boundary"
    });
    let initial = endpoint
        .handle_query(&trusted, query.clone())
        .await
        .expect("tenant-scoped query");
    let rows = initial["data"].as_array().expect("query rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["order_number"], "TENANT-ONE-ORDER");

    let cross_tenant_error = endpoint
        .handle_mutation(
            &trusted,
            json!({
                "entity": "CustomerOrder",
                "action": "Update",
                "id": 2,
                "expectedVersion": 1,
                "payload": {"orderNumber": "STOLEN"},
                "comment": "attempt cross-tenant update"
            }),
        )
        .await
        .expect_err("cross-tenant mutation must not match a row");
    assert!(matches!(
        cross_tenant_error,
        TfpEndpointError::MutationTargetUnavailable
    ));
    assert_eq!(cross_tenant_error.code(), "TFP_MUTATION_TARGET_UNAVAILABLE");

    let own_update = endpoint
        .handle_mutation(
            &trusted,
            json!({
                "entity": "CustomerOrder",
                "action": "Update",
                "id": 1,
                "expectedVersion": 1,
                "payload": {"orderNumber": "TENANT-ONE-UPDATED"},
                "comment": "update the tenant-owned order"
            }),
        )
        .await
        .expect("same-tenant update");
    assert_eq!(own_update["affectedRows"], 1);

    let after = endpoint
        .handle_query(&trusted, query.clone())
        .await
        .expect("query after update");
    let rows = after["data"].as_array().expect("query rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["version"], 2);
    assert_eq!(rows[0]["order_number"], "TENANT-ONE-UPDATED");

    let cross_tenant_delete = endpoint
        .handle_mutation(
            &trusted,
            json!({
                "entity": "CustomerOrder",
                "action": "Delete",
                "id": 2,
                "expectedVersion": 1,
                "payload": {},
                "comment": "attempt cross-tenant delete"
            }),
        )
        .await
        .expect_err("cross-tenant delete must not match a row");
    assert_eq!(
        cross_tenant_delete.code(),
        "TFP_MUTATION_TARGET_UNAVAILABLE"
    );

    let own_delete = endpoint
        .handle_mutation(
            &trusted,
            json!({
                "entity": "CustomerOrder",
                "action": "Delete",
                "id": 1,
                "expectedVersion": 2,
                "payload": {},
                "comment": "soft-delete the tenant-owned order"
            }),
        )
        .await
        .expect("same-tenant delete");
    assert_eq!(own_delete["affectedRows"], 1);
    let after_delete = endpoint
        .handle_query(&trusted, query.clone())
        .await
        .expect("query after soft delete");
    assert!(
        after_delete["data"]
            .as_array()
            .expect("query rows")
            .is_empty(),
        "soft-deleted rows must be absent from ordinary TFP queries"
    );

    let cross_tenant_recover = endpoint
        .handle_mutation(
            &trusted,
            json!({
                "entity": "CustomerOrder",
                "action": "Recover",
                "id": 3,
                "expectedVersion": -2,
                "payload": {},
                "comment": "attempt cross-tenant recovery"
            }),
        )
        .await
        .expect_err("cross-tenant recovery must not match a row");
    assert_eq!(
        cross_tenant_recover.code(),
        "TFP_MUTATION_TARGET_UNAVAILABLE"
    );

    let own_recover = endpoint
        .handle_mutation(
            &trusted,
            json!({
                "entity": "CustomerOrder",
                "action": "Recover",
                "id": 4,
                "expectedVersion": -2,
                "payload": {},
                "comment": "recover the tenant-owned order"
            }),
        )
        .await
        .expect("same-tenant recovery");
    assert_eq!(own_recover["affectedRows"], 1);
    let after_recover = endpoint
        .handle_query(&trusted, query)
        .await
        .expect("query after recovery");
    let rows = after_recover["data"].as_array().expect("query rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], 4);
    assert_eq!(rows[0]["version"], 3);
    assert_eq!(rows[0]["order_number"], "TENANT-ONE-DELETED");

    let connection = transport.connection();
    let connection = connection.lock().expect("sqlite connection");
    let tenant_two: (i64, i64, String) = connection
        .query_row(
            "SELECT commerce_platform_id, version, order_number \
             FROM customer_order_data WHERE id = 2",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("tenant two row");
    assert_eq!(tenant_two, (2, 1, "TENANT-TWO-ORDER".into()));
    let versions = [1_i64, 2, 3, 4]
        .into_iter()
        .map(|id| {
            connection
                .query_row(
                    "SELECT version FROM customer_order_data WHERE id = ?1",
                    [id],
                    |row| row.get::<_, i64>(0),
                )
                .expect("order version")
        })
        .collect::<Vec<_>>();
    assert_eq!(versions, vec![-3, 1, -2, 3]);
}
