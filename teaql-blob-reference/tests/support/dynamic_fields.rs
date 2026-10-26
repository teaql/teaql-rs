//! Real PostgreSQL extension storage and the generated-entity save bridge.
use super::{Business, Fixture};
use std::collections::BTreeMap;
use std::sync::Arc;
use teaql_core::dynamic_fields::{
    DynamicFieldDefinitions, DynamicFieldMutation, DynamicFieldSelection, DynamicFieldState,
};
use teaql_core::{
    Audited, BlobRef, DataType, Entity, Expr, MutationIntent, QueryIntent, SelectQuery,
    TeaqlEntity as _, Value,
};
use teaql_data_service::dynamic_fields::{DynamicFieldStore, DynamicFieldWrite};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_provider_postgres::PgMutationExecutor;
use teaql_runtime::dynamic_fields::{DatabaseDynamicFieldsProvider, DynamicFieldsProvider};
use teaql_runtime::{AuditedSaveExt, InMemoryMetadataStore, PurposedSelectQuery, UserContext};
use teaql_sql::{SqlTransaction, SqlTransactionTransport, SqlTransport};

#[teaql_entity]
#[derive(Clone, Debug, TeaqlEntity)]
#[teaql(entity = "Asset", table = "r5_assets", indexed_layout)]
struct DynamicAsset {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    document: Option<BlobRef>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, Value>,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}
impl DynamicAsset {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "r5-dynamic-asset-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("document", 2)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("document", "document", "document"),
    ];
}

fn definitions() -> Arc<DynamicFieldDefinitions> {
    DynamicFieldDefinitions::new(
        "Asset",
        "r5-extension-v1",
        [
            ("note".into(), DataType::Text),
            ("nil".into(), DataType::Text),
            ("bits".into(), DataType::F64),
            ("extra".into(), DataType::I64),
        ],
    )
    .unwrap()
}

async fn graph_context(f: &Fixture) -> UserContext {
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(DynamicAsset::entity_descriptor()));
    context.register_executor(f.business());
    let provider = Arc::new(
        DatabaseDynamicFieldsProvider::<Business>::new("r5-dynamic", [definitions()]).unwrap(),
    );
    provider.ensure_schema(&context).await.unwrap();
    context.set_dynamic_fields_provider(provider);
    context
}

async fn load_asset(context: &UserContext) -> DynamicAsset {
    let selection = DynamicFieldSelection::fields([
        ("note".into(), DataType::Text),
        ("extra".into(), DataType::I64),
    ])
    .unwrap();
    let query = SelectQuery::new("Asset")
        .projects(["id", "version", "document"])
        .filter(Expr::eq("id", 42_u64))
        .comment("load Asset extensions on the registered database")
        .select_dynamic_fields(selection);
    let request = PurposedSelectQuery::new(query, "test persistent dynamic save");
    let mut rows = context
        .entity_data_service::<Business>("Asset")
        .unwrap()
        .fetch_enhanced_entities::<DynamicAsset>(&request)
        .await
        .unwrap()
        .data;
    assert_eq!(rows.len(), 1);
    rows.remove(0)
}

#[tokio::test]
async fn generated_asset_save_persists_dynamic_field_on_native_postgres_owner() {
    let f = Fixture::new(false).await;
    f.write(f.root.as_ref(), &f.target(42, "document"), None)
        .await;
    let context = graph_context(&f).await;
    let mut loaded = load_asset(&context).await;
    assert_eq!(
        loaded
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .state(),
        DynamicFieldState::NotLoaded
    );
    loaded
        .update_dynamic_field("note", Value::from("first"))
        .unwrap();
    let saved = Audited::new(loaded, "persist Asset extension")
        .save(&context)
        .await
        .unwrap();
    assert_eq!(
        saved
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .value(),
        Some(&Value::from("first"))
    );
    let independent = f.admin.get().await.unwrap();
    let persisted: String = independent
        .query_one(
            &format!(
                "SELECT payload FROM \"{}\".teaql_dynamic_field_storage_v1
                 WHERE namespace='r5-dynamic' AND owner_type='Asset' AND owner_id='42' AND code='note'",
                f.schema
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(persisted, "\"first\"");
    println!("R5_PG_DYNAMIC_GENERATED_SAVE_READBACK_PASS");
    f.finish().await;
}

#[tokio::test]
async fn generated_dynamic_saves_repeat_rollback_and_retry_on_one_native_owner() {
    let f = Fixture::new(false).await;
    f.write(f.root.as_ref(), &f.target(42, "document"), None)
        .await;
    let context = graph_context(&f).await;
    let mut loaded = load_asset(&context).await;
    loaded
        .update_dynamic_field("note", Value::from("first"))
        .unwrap();
    let saved = context
        .execute_in_transaction::<Business, _, _>(move |scope| {
            Box::pin(async move {
                let mut first = scope
                    .save_audited(Audited::new(loaded, "first extension save"))
                    .await?;
                first
                    .update_dynamic_field("note", Value::from("second"))
                    .unwrap();
                scope
                    .save_audited(Audited::new(first, "second extension save"))
                    .await
            })
        })
        .await
        .unwrap();
    assert_eq!(
        saved
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .value(),
        Some(&Value::from("second"))
    );
    let mut retry = load_asset(&context).await;
    assert_eq!(
        retry
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .value(),
        Some(&Value::from("second"))
    );
    retry
        .update_dynamic_field("note", Value::from("third"))
        .unwrap();
    let retry = retry.clone();
    let result = context
        .execute_in_transaction::<Business, (), _>(move |scope| {
            Box::pin(async move {
                scope
                    .save_audited(Audited::new(retry.clone(), "rolled back extension save"))
                    .await?;
                Err(teaql_runtime::RuntimeError::Graph(
                    "abort extension save".into(),
                ))
            })
        })
        .await;
    assert!(result.is_err());
    let after_rollback = load_asset(&context).await;
    assert_eq!(
        after_rollback
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .value(),
        Some(&Value::from("second"))
    );
    let mut final_edit = after_rollback;
    final_edit
        .update_dynamic_field("note", Value::from("third"))
        .unwrap();
    Audited::new(final_edit, "retry extension save")
        .save(&context)
        .await
        .unwrap();
    let independent = f.admin.get().await.unwrap();
    let persisted: String = independent
        .query_one(
            &format!(
                "SELECT payload FROM \"{}\".teaql_dynamic_field_storage_v1
                 WHERE namespace='r5-dynamic' AND owner_type='Asset' AND owner_id='42' AND code='note'",
                f.schema
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(persisted, "\"third\"");
    println!("R5_PG_DYNAMIC_REPEATED_SAVE_ROLLBACK_RETRY_PASS");
    f.finish().await;
}

#[tokio::test]
async fn native_postgres_extension_store_uses_physical_transaction_and_exact_values() {
    let f = Fixture::new(false).await;
    let ambient = f.root.dynamic_field_store().unwrap();
    assert!(!ambient.is_transaction_bound());
    ambient.ensure_schema().await.unwrap();
    let definitions =
        definitions().with_storage_binding("r5-dynamic", ambient.source_identity().unwrap());
    let write = DynamicFieldWrite {
        namespace: "r5-dynamic".into(),
        definitions: definitions.clone(),
        owner_id: 42,
        changes: BTreeMap::from([
            (
                "note".into(),
                DynamicFieldMutation::Set(Value::from("first")),
            ),
            ("nil".into(), DynamicFieldMutation::Set(Value::Null)),
            ("bits".into(), DynamicFieldMutation::Set(Value::F64(-0.0))),
        ]),
    };
    assert_eq!(
        ambient
            .validate_writes(std::slice::from_ref(&write))
            .unwrap_err()
            .code,
        "DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED"
    );
    let intent = MutationIntent::from_optional(Some("persist extensions on one owner")).unwrap();
    let query = QueryIntent::new("inspect extension storage", "verify the physical row").unwrap();
    let selection = DynamicFieldSelection::All;
    let transaction = f.root.begin_sql().await.unwrap();
    assert!(transaction.is_transaction_bound());
    assert_eq!(transaction.binding_key(), ambient.binding_key());
    assert_eq!(transaction.source_identity(), ambient.source_identity());
    transaction.apply_writes(&[write], &intent).await.unwrap();
    let pending = transaction
        .load_values("r5-dynamic", &definitions, &[42], &selection, &query)
        .await
        .unwrap();
    assert_eq!(pending[&42]["note"], Value::from("first"));
    assert_eq!(pending[&42]["nil"], Value::Null);
    let Value::F64(bits) = pending[&42]["bits"] else {
        panic!("stored f64 required")
    };
    assert_eq!(bits.to_bits(), (-0.0_f64).to_bits());
    assert!(
        ambient
            .load_values("r5-dynamic", &definitions, &[42], &selection, &query)
            .await
            .unwrap()[&42]
            .is_empty()
    );
    SqlTransaction::commit_sql(transaction).await.unwrap();
    let committed = ambient
        .load_values("r5-dynamic", &definitions, &[42], &selection, &query)
        .await
        .unwrap();
    assert_eq!(committed[&42], pending[&42]);

    let rollback = f.root.begin_sql().await.unwrap();
    rollback
        .apply_writes(
            &[DynamicFieldWrite {
                namespace: "r5-dynamic".into(),
                definitions: definitions.clone(),
                owner_id: 42,
                changes: BTreeMap::from([
                    ("note".into(), DynamicFieldMutation::Delete),
                    ("bits".into(), DynamicFieldMutation::Set(Value::F64(1.25))),
                ]),
            }],
            &intent,
        )
        .await
        .unwrap();
    assert!(
        !rollback
            .load_values("r5-dynamic", &definitions, &[42], &selection, &query)
            .await
            .unwrap()[&42]
            .contains_key("note")
    );
    SqlTransaction::rollback_sql(rollback).await.unwrap();
    assert_eq!(
        ambient
            .load_values("r5-dynamic", &definitions, &[42], &selection, &query)
            .await
            .unwrap()[&42],
        committed[&42]
    );

    let unrelated = PgMutationExecutor::new(f.root.pool());
    assert_eq!(
        unrelated
            .load_values("r5-dynamic", &definitions, &[42], &selection, &query)
            .await
            .unwrap_err()
            .code,
        "DYNAMIC_FIELD_STORAGE_PROVENANCE_MISMATCH"
    );
    let wrong_owner = unrelated.begin_sql().await.unwrap();
    assert_ne!(wrong_owner.binding_key(), ambient.binding_key());
    assert_eq!(
        wrong_owner
            .validate_writes(&[DynamicFieldWrite {
                namespace: "r5-dynamic".into(),
                definitions: definitions.clone(),
                owner_id: 42,
                changes: BTreeMap::from([("note".into(), DynamicFieldMutation::Delete)]),
            }])
            .unwrap_err()
            .code,
        "DYNAMIC_FIELD_STORAGE_PROVENANCE_MISMATCH"
    );
    SqlTransaction::rollback_sql(wrong_owner).await.unwrap();
    println!("R5_PG_DYNAMIC_PHYSICAL_OWNER_ISOLATION_EXACT_VALUES_PASS");
    f.finish().await;
}
