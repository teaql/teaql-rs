//! Real PostgreSQL extension storage and the generated-entity save bridge.
use super::{Business, Fixture};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use teaql_core::dynamic_fields::{
    DynamicFieldDefinitions, DynamicFieldMutation, DynamicFieldSelection, DynamicFieldState,
};
use teaql_core::{
    Audited, BlobRef, DataType, Entity, Expr, MutationIntent, QueryIntent, SelectQuery,
    TeaqlEntity as _, Value,
};
use teaql_data_service::dynamic_fields::{DynamicFieldStore, DynamicFieldWrite};
use teaql_data_service::infrastructure_sql::PhysicalTransactionOutcome;
use teaql_data_service::{Transaction, TransactionExecutor};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_provider_postgres::{PgMutationExecutor, PostgresDialect};
use teaql_runtime::dynamic_fields::{DatabaseDynamicFieldsProvider, DynamicFieldsProvider};
use teaql_runtime::{
    AuditedSaveExt, InMemoryMetadataStore, PurposedSelectQuery, RawAuditEventKind, SafeAuditEvent,
    SafeAuditEventSink, UserContext, save_audited_ledger_entity_with_executor,
    save_audited_ledger_entity_with_native_executor,
};
use teaql_sql::{SqlDataServiceExecutor, SqlTransaction, SqlTransactionTransport, SqlTransport};

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

struct CapturedSafeAudits(Arc<Mutex<Vec<SafeAuditEvent>>>);

impl SafeAuditEventSink for CapturedSafeAudits {
    fn on_safe_event(
        &self,
        _context: &UserContext,
        event: &SafeAuditEvent,
    ) -> Result<(), teaql_runtime::RuntimeError> {
        self.0.lock().unwrap().push(event.clone());
        Ok(())
    }
}

struct RejectCommittedAudit;

impl SafeAuditEventSink for RejectCommittedAudit {
    fn on_safe_event(
        &self,
        _context: &UserContext,
        _event: &SafeAuditEvent,
    ) -> Result<(), teaql_runtime::RuntimeError> {
        Err(teaql_runtime::RuntimeError::Graph(
            "deliberate audit sink failure".into(),
        ))
    }
}

#[tokio::test]
async fn raw_native_executor_delivers_dynamic_audit_only_after_physical_commit() {
    let f = Fixture::new(false).await;
    f.write(f.root.as_ref(), &f.target(42, "document"), None)
        .await;
    let audits = Arc::new(Mutex::new(Vec::new()));
    let mut context = graph_context(&f).await;
    context.set_custom_event_sink(CapturedSafeAudits(audits.clone()));
    let context = Arc::new(context);
    let mut loaded = load_asset(&context).await;
    loaded
        .update_dynamic_field("note", Value::from("committed"))
        .unwrap();
    let executor = f.business();
    let transaction = TransactionExecutor::begin(&executor).await.unwrap();
    assert!(
        save_audited_ledger_entity_with_executor(
            Audited::new(loaded.clone(), "legacy raw native save must reject"),
            &context,
            &transaction,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("NATIVE_RAW_EXECUTOR_REQUIRES_COMMIT_AUDIT_OWNER")
    );
    let mut saved = save_audited_ledger_entity_with_native_executor(
        Audited::new(loaded, "native raw extension save"),
        context.clone(),
        &transaction,
    )
    .await
    .unwrap();
    assert_eq!(
        saved
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .value(),
        Some(&Value::from("committed"))
    );
    saved
        .update_dynamic_field("note", Value::from("second"))
        .unwrap();
    save_audited_ledger_entity_with_native_executor(
        Audited::new(saved, "second native raw extension save"),
        context.clone(),
        &transaction,
    )
    .await
    .unwrap();
    assert!(audits.lock().unwrap().is_empty());
    assert!(context.has_unfinished_view_transaction());
    let independent = f.admin.get().await.unwrap();
    let before: i64 = independent
        .query_one(
            &format!(
                "SELECT count(*) FROM \"{}\".teaql_dynamic_field_storage_v1 WHERE owner_id='42'",
                f.schema
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, 0);
    Transaction::commit(transaction).await.unwrap();
    assert!(!context.has_unfinished_view_transaction());
    let delivered = audits.lock().unwrap().clone();
    assert_eq!(delivered.len(), 2);
    for event in &delivered {
        assert_eq!(event.kind, RawAuditEventKind::Updated);
        assert_eq!(event.entity, "Asset");
        assert_eq!(event.entity_id, Some(42));
    }

    let mut rollback = load_asset(&context).await;
    rollback
        .update_dynamic_field("note", Value::from("rolled back"))
        .unwrap();
    let transaction = TransactionExecutor::begin(&executor).await.unwrap();
    save_audited_ledger_entity_with_native_executor(
        Audited::new(rollback, "rollback native raw extension save"),
        context.clone(),
        &transaction,
    )
    .await
    .unwrap();
    assert_eq!(audits.lock().unwrap().len(), 2);
    Transaction::rollback(transaction).await.unwrap();
    assert_eq!(audits.lock().unwrap().len(), 2);
    assert!(!context.has_unfinished_view_transaction());
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
    assert_eq!(persisted, "\"second\"");
    println!("R5_NATIVE_RAW_DYNAMIC_AUDIT_PHYSICAL_COMMIT_GATE_PASS");
    f.finish().await;
}

#[tokio::test]
async fn raw_native_save_error_vetoes_commit_even_if_caller_catches_it() {
    let f = Fixture::new(false).await;
    f.write(f.root.as_ref(), &f.target(42, "document"), None)
        .await;
    let context = Arc::new(graph_context(&f).await);
    let mut loaded = load_asset(&context).await;
    loaded
        .update_dynamic_field("note", Value::from("must roll back"))
        .unwrap();
    let unrelated = SqlDataServiceExecutor::new(
        PostgresDialect,
        PgMutationExecutor::new(f.root.pool()),
        InMemoryMetadataStore::new().with_entity(DynamicAsset::entity_descriptor()),
    );
    let transaction = TransactionExecutor::begin(&unrelated).await.unwrap();
    let error = save_audited_ledger_entity_with_native_executor(
        Audited::new(loaded, "reject unrelated native dynamic storage"),
        context.clone(),
        &transaction,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED")
    );
    assert!(Transaction::commit(transaction).await.is_err());
    assert!(!context.has_unfinished_view_transaction());
    let independent = f.admin.get().await.unwrap();
    let stored: i64 = independent
        .query_one(
            &format!(
                "SELECT count(*) FROM \"{}\".teaql_dynamic_field_storage_v1 WHERE owner_id='42'",
                f.schema
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, 0);
    println!("R5_NATIVE_RAW_CAUGHT_ERROR_COMMIT_VETO_PASS");
    f.finish().await;
}

#[tokio::test]
async fn raw_native_audit_notification_failure_preserves_committed_sql_truth() {
    let f = Fixture::new(false).await;
    f.write(f.root.as_ref(), &f.target(42, "document"), None)
        .await;
    let mut context = graph_context(&f).await;
    context.set_custom_event_sink(RejectCommittedAudit);
    let context = Arc::new(context);
    let mut loaded = load_asset(&context).await;
    loaded
        .update_dynamic_field("note", Value::from("committed despite audit failure"))
        .unwrap();
    let executor = f.business();
    let transaction = TransactionExecutor::begin(&executor).await.unwrap();
    let owner = Transaction::physical_identity(&transaction).unwrap();
    save_audited_ledger_entity_with_native_executor(
        Audited::new(loaded, "native raw audit failure after commit"),
        context.clone(),
        &transaction,
    )
    .await
    .unwrap();
    assert_eq!(owner.outcome(), PhysicalTransactionOutcome::Active);
    assert!(Transaction::commit(transaction).await.is_err());
    assert_eq!(owner.outcome(), PhysicalTransactionOutcome::Committed);
    assert!(!context.has_unfinished_view_transaction());
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
    assert_eq!(persisted, "\"committed despite audit failure\"");
    println!("R5_NATIVE_RAW_AUDIT_AFTER_COMMIT_SQL_TRUTH_PASS");
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
