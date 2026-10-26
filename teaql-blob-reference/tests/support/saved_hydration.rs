//! Actual SQL row/version provenance through the macro/runtime save hydration bridge.
use super::Fixture;
use std::sync::Arc;
use teaql_core::TeaqlEntity as _;
use teaql_core::{
    Audited, BlobFieldSpec, BlobRef, CompactRow, DataType, Entity, EntityDescriptor,
    EntitySnapshot, Value,
};
use teaql_data_service::infrastructure_sql::*;
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_runtime::{
    AuditedSaveExt, EntityKey, EntityRuntimeState, InMemoryMetadataStore, InternalIdGenerator,
    LedgerEntity, MutationDecision, MutationPlan, MutationPolicy, MutationPolicyIdentity,
    RuntimeError, UserContext,
};

#[teaql_entity]
#[derive(Clone, Debug, TeaqlEntity)]
#[teaql(entity = "Asset", table = "r5_assets")]
struct SavedAsset {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    document: Option<BlobRef>,
}
// Journal and typed readback fixtures use Key-shaped text without claiming
// automatic Blob binding. The modeled Blob test below installs the real spec.
fn transport_descriptor() -> EntityDescriptor {
    let mut descriptor = SavedAsset::entity_descriptor();
    descriptor
        .properties
        .iter_mut()
        .find(|property| property.name == "document")
        .unwrap()
        .data_type = DataType::Text;
    descriptor
}
fn key() -> EntityKey {
    EntityKey::new_static("Asset", 42_u64)
}
fn blob(name: &str) -> BlobRef {
    BlobRef::new(format!("blob_saved_{name}")).unwrap()
}
async fn row(sql: &dyn InfrastructureSql) -> CompactRow {
    let rows = sql
        .fetch(
            &InfrastructureStatement::new(
                "SELECT id,version,document FROM r5_assets WHERE id=42",
                vec![],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    rows.into_iter().next().unwrap()
}

#[tokio::test]
async fn owned_native_save_decodes_and_acknowledges_before_physical_commit() {
    let f = Fixture::new(true).await;
    f.write(
        f.root.as_ref(),
        &f.target(42, "document"),
        Some(&blob("original")),
    )
    .await;
    let loaded = SavedAsset::from_compact_row(row(f.root.as_ref()).await).unwrap();
    let root = loaded.entity_runtime_state().unwrap();
    root.set(key(), "document", Value::BlobRef(blob("owned")));
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(transport_descriptor()));
    context.register_executor(f.business());
    let saved = Audited::new(loaded, "owned native save")
        .save(&context)
        .await
        .unwrap();
    assert_eq!(saved.version, 2);
    assert_eq!(saved.entity_runtime_state(), Some(root.clone()));
    assert!(!root.has_pending_submission());
    assert_eq!(root.get_original_version(&key()), Some(2));
    let actual = row(f.root.as_ref()).await;
    assert_eq!(actual.get("version"), Some(&Value::I64(2)));
    assert_eq!(
        actual.get("document"),
        Some(&Value::Text(blob("owned").as_str().into()))
    );
    println!("R5_OWNED_NATIVE_TYPED_COMMIT_PASS");
    f.finish().await;
}

#[tokio::test]
async fn modeled_blob_save_without_binding_participant_leaves_real_sql_and_intent_unchanged() {
    let f = Fixture::new(true).await;
    f.write(
        f.root.as_ref(),
        &f.target(42, "document"),
        Some(&blob("original")),
    )
    .await;
    let loaded = SavedAsset::from_compact_row(row(f.root.as_ref()).await).unwrap();
    let root = loaded.entity_runtime_state().unwrap();
    root.set(key(), "document", Value::BlobRef(blob("unbound")));
    let mut descriptor = SavedAsset::entity_descriptor();
    let spec = BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap();
    let field = descriptor
        .properties
        .iter_mut()
        .find(|property| property.name == "document")
        .unwrap();
    *field = field.clone().blob_field_spec(spec);
    let mut context =
        UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(descriptor));
    context.register_executor(f.business());

    let error = Audited::new(loaded, "modeled Blob write requires binding")
        .save(&context)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RuntimeError::Capability {
            code: "BLOB_CAPABILITY_MISSING",
            ..
        }
    ));
    let actual = row(f.root.as_ref()).await;
    assert_eq!(actual.get("version"), Some(&Value::I64(1)));
    assert_eq!(
        actual.get("document"),
        Some(&Value::Text(blob("original").as_str().into()))
    );
    assert_eq!(
        root.get(&key(), "document"),
        Some(Value::BlobRef(blob("unbound")))
    );
    assert!(!root.has_pending_submission());
    f.finish().await;
}

struct EditAfterOwnedReview(EntityRuntimeState);
impl MutationPolicy for EditAfterOwnedReview {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("owned-review-late-edit", "1", "sha256:owned-late-edit")
    }
    fn review(&self, _context: &UserContext, plan: &MutationPlan) -> MutationDecision {
        assert_eq!(plan.request_key, "Asset.saveGraph");
        assert_eq!(plan.operations.len(), 1);
        assert_eq!(
            plan.operations[0].changed_values.get("document"),
            Some(&Value::BlobRef(blob("submitted")))
        );
        self.0.set(key(), "document", Value::BlobRef(blob("late")));
        MutationDecision::allow()
    }
}

#[tokio::test]
async fn owned_native_review_freezes_sql_intent_and_keeps_late_edit() {
    let f = Fixture::new(true).await;
    f.write(
        f.root.as_ref(),
        &f.target(42, "document"),
        Some(&blob("original")),
    )
    .await;
    let loaded = SavedAsset::from_compact_row(row(f.root.as_ref()).await).unwrap();
    let root = loaded.entity_runtime_state().unwrap();
    root.set(key(), "document", Value::BlobRef(blob("submitted")));
    let policy: Arc<dyn MutationPolicy> = Arc::new(EditAfterOwnedReview(root.clone()));
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(transport_descriptor()))
        .with_mutation_policy_registry(move |request_key: &str| {
            (request_key == "Asset.saveGraph").then(|| policy.clone())
        });
    context.register_executor(f.business());
    let saved = Audited::new(loaded, "owned reviewed save")
        .save(&context)
        .await
        .unwrap();
    assert_eq!(saved.version, 2);
    assert_eq!(saved.document, Some(blob("submitted")));
    assert_eq!(root.get_original_version(&key()), Some(2));
    assert_eq!(
        root.get(&key(), "document"),
        Some(Value::BlobRef(blob("late")))
    );
    let actual = row(f.root.as_ref()).await;
    assert_eq!(actual.get("version"), Some(&Value::I64(2)));
    assert_eq!(
        actual.get("document"),
        Some(&Value::Text(blob("submitted").as_str().into()))
    );
    println!("R5_OWNED_NATIVE_REVIEW_FROZEN_LATE_EDIT_PASS");
    f.finish().await;
}

#[tokio::test]
async fn owned_native_new_row_acknowledges_insert_origin() {
    let f = Fixture::new(true).await;
    let new =
        SavedAsset::from_compact_row(CompactRow::from_map(std::collections::BTreeMap::from([
            ("id".into(), Value::U64(42)),
            ("version".into(), Value::I64(0)),
            ("document".into(), Value::Null),
        ])))
        .unwrap();
    let root = new.entity_runtime_state().unwrap();
    root.mark_as_new(key());
    root.set(key(), "document", Value::BlobRef(blob("new_owned")));
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(transport_descriptor()));
    context.register_executor(f.business());
    let saved = Audited::new(new, "owned new Asset save")
        .save(&context)
        .await
        .unwrap();
    assert_eq!(saved.id, 42);
    assert_eq!(saved.version, 1);
    assert_eq!(saved.entity_runtime_state(), Some(root.clone()));
    assert!(!root.is_new(&key()));
    assert!(!root.has_pending_submission());
    assert_eq!(root.get_original_version(&key()), Some(1));
    assert_eq!(f.count("r5_assets").await, 1);
    let actual = row(f.root.as_ref()).await;
    assert_eq!(actual.get("version"), Some(&Value::I64(1)));
    assert_eq!(
        actual.get("document"),
        Some(&Value::Text(blob("new_owned").as_str().into()))
    );
    println!("R5_OWNED_NATIVE_INSERT_ORIGIN_PASS");
    f.finish().await;
}

struct AllocateAssetId;
impl InternalIdGenerator for AllocateAssetId {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        assert_eq!(entity, "Asset");
        Ok(42)
    }
}

#[tokio::test]
async fn owned_native_allocated_id_aliases_original_and_returned_handles() {
    let f = Fixture::new(true).await;
    let temporary = EntityKey::new_static("Asset", 0_u64);
    let new =
        SavedAsset::from_compact_row(CompactRow::from_map(std::collections::BTreeMap::from([
            ("id".into(), Value::U64(0)),
            ("version".into(), Value::I64(0)),
            ("document".into(), Value::Null),
        ])))
        .unwrap();
    let root = new.entity_runtime_state().unwrap();
    root.mark_as_new(temporary.clone());
    root.set(
        temporary.clone(),
        "document",
        Value::BlobRef(blob("allocated")),
    );
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(transport_descriptor()))
        .with_internal_id_generator(AllocateAssetId);
    context.register_executor(f.business());
    let saved = Audited::new(new, "owned allocated Asset save")
        .save(&context)
        .await
        .unwrap();
    assert_eq!(saved.id, 42);
    assert_eq!(saved.version, 1);
    assert_eq!(saved.entity_runtime_state(), Some(root.clone()));
    assert!(!root.is_new(&temporary));
    assert!(!root.is_new(&key()));
    assert_eq!(root.get_original_version(&temporary), Some(1));
    assert_eq!(root.get_original_version(&key()), Some(1));
    root.set(key(), "document", Value::BlobRef(blob("later")));
    assert_eq!(
        root.get(&temporary, "document"),
        Some(Value::BlobRef(blob("later")))
    );
    let actual = row(f.root.as_ref()).await;
    assert_eq!(actual.get("id"), Some(&Value::I64(42)));
    assert_eq!(actual.get("version"), Some(&Value::I64(1)));
    assert_eq!(
        actual.get("document"),
        Some(&Value::Text(blob("allocated").as_str().into()))
    );
    println!("R5_OWNED_NATIVE_ALLOCATED_ALIAS_PASS");
    f.finish().await;
}

#[tokio::test]
async fn owned_invalid_canonical_readback_rolls_back_retryable_intent() {
    let f = Fixture::new(true).await;
    f.write(
        f.root.as_ref(),
        &f.target(42, "document"),
        Some(&blob("original")),
    )
    .await;
    f.admin
        .get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "CREATE FUNCTION \"{0}\".corrupt_owned_readback() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.document='invalid_blob_reference'; RETURN NEW; END $$; CREATE TRIGGER corrupt_owned_readback BEFORE UPDATE ON \"{0}\".r5_assets FOR EACH ROW EXECUTE FUNCTION \"{0}\".corrupt_owned_readback()",
            f.schema
        ))
        .await
        .unwrap();
    let loaded = SavedAsset::from_compact_row(row(f.root.as_ref()).await).unwrap();
    let root = loaded.entity_runtime_state().unwrap();
    root.set(key(), "document", Value::BlobRef(blob("submitted")));
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(transport_descriptor()));
    context.register_executor(f.business());
    let error = Audited::new(loaded, "owned corrupted authoritative readback")
        .save(&context)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("BLOB_REFERENCE_INVALID"));
    assert!(!root.has_pending_submission());
    assert_eq!(root.get_original_version(&key()), Some(1));
    assert_eq!(
        root.get(&key(), "document"),
        Some(Value::BlobRef(blob("submitted")))
    );
    let actual = row(f.root.as_ref()).await;
    assert_eq!(actual.get("version"), Some(&Value::I64(1)));
    assert_eq!(
        actual.get("document"),
        Some(&Value::Text(blob("original").as_str().into()))
    );
    println!("R5_OWNED_NATIVE_READBACK_ROLLBACK_PASS");
    f.finish().await;
}

#[tokio::test]
async fn outer_transaction_repeats_native_save_on_one_owner_and_preserves_late_edit() {
    let f = Fixture::new(true).await;
    f.write(
        f.root.as_ref(),
        &f.target(42, "document"),
        Some(&blob("original")),
    )
    .await;
    let loaded = SavedAsset::from_compact_row(row(f.root.as_ref()).await).unwrap();
    let root = loaded.entity_runtime_state().unwrap();
    root.set(key(), "document", Value::BlobRef(blob("first")));
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(transport_descriptor()));
    context.insert_resource(f.business());
    let ledger = root.clone();
    let saved = context
        .execute_in_transaction::<super::support::Business, _, _>(move |scope| {
            Box::pin(async move {
                let first = scope
                    .save_audited(Audited::new(loaded, "first outer native save"))
                    .await?;
                assert_eq!(first.version, 2);
                assert_eq!(first.entity_runtime_state(), Some(ledger.clone()));
                assert!(ledger.has_pending_submission());
                ledger.set(key(), "document", Value::BlobRef(blob("second")));
                let second = scope
                    .save_audited(Audited::new(first, "second outer native save"))
                    .await?;
                assert_eq!(second.version, 3);
                ledger.set(key(), "document", Value::BlobRef(blob("late")));
                Ok(second)
            })
        })
        .await
        .unwrap();
    assert_eq!(saved.entity_runtime_state(), Some(root.clone()));
    assert!(!root.has_pending_submission());
    assert_eq!(root.get_original_version(&key()), Some(3));
    assert_eq!(
        root.get(&key(), "document"),
        Some(Value::BlobRef(blob("late")))
    );
    let actual = row(f.root.as_ref()).await;
    assert_eq!(actual.get("version"), Some(&Value::I64(3)));
    assert_eq!(
        actual.get("document"),
        Some(&Value::Text(blob("second").as_str().into()))
    );
    println!("R5_OUTER_NATIVE_REPEAT_LATE_EDIT_PASS");
    f.finish().await;
}

#[tokio::test]
async fn outer_transaction_rollback_restores_original_and_retryable_intent() {
    let f = Fixture::new(true).await;
    f.write(
        f.root.as_ref(),
        &f.target(42, "document"),
        Some(&blob("original")),
    )
    .await;
    let loaded = SavedAsset::from_compact_row(row(f.root.as_ref()).await).unwrap();
    let root = loaded.entity_runtime_state().unwrap();
    root.set(key(), "document", Value::BlobRef(blob("submitted")));
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(transport_descriptor()));
    context.insert_resource(f.business());
    let result = context
        .execute_in_transaction::<super::support::Business, _, _>(move |scope| {
            Box::pin(async move {
                let saved = scope
                    .save_audited(Audited::new(loaded, "outer rollback save"))
                    .await?;
                assert_eq!(saved.version, 2);
                Err::<(), _>(teaql_runtime::RuntimeError::Graph(
                    "abort transaction".into(),
                ))
            })
        })
        .await;
    assert!(result.is_err());
    assert!(!root.has_pending_submission());
    assert_eq!(root.get_original_version(&key()), Some(1));
    assert_eq!(
        root.get(&key(), "document"),
        Some(Value::BlobRef(blob("submitted")))
    );
    let actual = row(f.root.as_ref()).await;
    assert_eq!(actual.get("version"), Some(&Value::I64(1)));
    assert_eq!(
        actual.get("document"),
        Some(&Value::Text(blob("original").as_str().into()))
    );
    println!("R5_OUTER_NATIVE_ROLLBACK_RETRY_PASS");
    f.finish().await;
}

struct DenyAsset;
impl MutationPolicy for DenyAsset {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("deny-asset", "1", "sha256:deny-asset")
    }
    fn review(&self, _context: &UserContext, plan: &MutationPlan) -> MutationDecision {
        assert_eq!(plan.request_key, "Asset.saveGraph");
        MutationDecision::deny("ASSET-DENIED", "asset save denied", ["Asset.document"])
    }
}

#[tokio::test]
async fn outer_policy_denial_restores_draft_and_commits_no_save_write() {
    let f = Fixture::new(true).await;
    f.write(
        f.root.as_ref(),
        &f.target(42, "document"),
        Some(&blob("original")),
    )
    .await;
    let loaded = SavedAsset::from_compact_row(row(f.root.as_ref()).await).unwrap();
    let root = loaded.entity_runtime_state().unwrap();
    root.set(key(), "document", Value::BlobRef(blob("denied")));
    let policy: Arc<dyn MutationPolicy> = Arc::new(DenyAsset);
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(transport_descriptor()))
        .with_mutation_policy_registry(move |request_key: &str| {
            (request_key == "Asset.saveGraph").then(|| policy.clone())
        });
    context.insert_resource(f.business());
    context
        .execute_in_transaction::<super::support::Business, _, _>(move |scope| {
            Box::pin(async move {
                let error = scope
                    .save_audited(Audited::new(loaded, "denied outer save"))
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("ASSET-DENIED"));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(!root.has_pending_submission());
    assert_eq!(root.get_original_version(&key()), Some(1));
    assert_eq!(
        root.get(&key(), "document"),
        Some(Value::BlobRef(blob("denied")))
    );
    let actual = row(f.root.as_ref()).await;
    assert_eq!(actual.get("version"), Some(&Value::I64(1)));
    assert_eq!(
        actual.get("document"),
        Some(&Value::Text(blob("original").as_str().into()))
    );
    println!("R5_OUTER_POLICY_DENIAL_NO_WRITE_PASS");
    f.finish().await;
}

#[tokio::test]
async fn outer_caught_invalid_canonical_readback_vetoes_physical_commit() {
    let f = Fixture::new(true).await;
    f.write(
        f.root.as_ref(),
        &f.target(42, "document"),
        Some(&blob("original")),
    )
    .await;
    f.admin
        .get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "CREATE FUNCTION \"{0}\".corrupt_asset_readback() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.document='invalid_blob_reference'; RETURN NEW; END $$; CREATE TRIGGER corrupt_asset_readback BEFORE UPDATE ON \"{0}\".r5_assets FOR EACH ROW EXECUTE FUNCTION \"{0}\".corrupt_asset_readback()",
            f.schema
        ))
        .await
        .unwrap();
    let loaded = SavedAsset::from_compact_row(row(f.root.as_ref()).await).unwrap();
    let root = loaded.entity_runtime_state().unwrap();
    root.set(key(), "document", Value::BlobRef(blob("submitted")));
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(transport_descriptor()));
    context.insert_resource(f.business());
    let error = context
        .execute_in_transaction::<super::support::Business, _, _>(move |scope| {
            Box::pin(async move {
                let save_error = scope
                    .save_audited(Audited::new(loaded, "corrupted authoritative readback"))
                    .await
                    .unwrap_err();
                assert!(save_error.to_string().contains("BLOB_REFERENCE_INVALID"));
                Ok(())
            })
        })
        .await
        .unwrap_err();
    assert!(
        matches!(error, teaql_runtime::RuntimeError::Transaction(_)),
        "{error}"
    );
    assert!(!root.has_pending_submission());
    assert_eq!(root.get_original_version(&key()), Some(1));
    assert_eq!(
        root.get(&key(), "document"),
        Some(Value::BlobRef(blob("submitted")))
    );
    let actual = row(f.root.as_ref()).await;
    assert_eq!(actual.get("version"), Some(&Value::I64(1)));
    assert_eq!(
        actual.get("document"),
        Some(&Value::Text(blob("original").as_str().into()))
    );
    println!("R5_OUTER_CAUGHT_READBACK_VETO_PASS");
    f.finish().await;
}

#[tokio::test]
async fn saved_readback_alias_uses_pretransaction_version_and_original_values_after_real_rollback()
{
    let f = Fixture::new(true).await;
    f.write(
        f.root.as_ref(),
        &f.target(42, "document"),
        Some(&blob("original")),
    )
    .await;
    let loaded = SavedAsset::from_compact_row(row(f.root.as_ref()).await).unwrap();
    let root = loaded.entity_runtime_state().unwrap();
    root.set(key(), "document", Value::BlobRef(blob("submitted")));
    let tx = f.root.begin().await.unwrap();
    let identity = tx.transaction_identity().unwrap();
    let submission = root.begin_submission(&identity).unwrap();
    assert_eq!(
        tx.execute(
            &InfrastructureStatement::new(
                "UPDATE r5_assets SET document=$1,version=2 WHERE id=42 AND version=1",
                vec![Value::Text(blob("submitted").as_str().into())]
            )
            .unwrap()
        )
        .await
        .unwrap(),
        1
    );
    let readback = row(tx.as_ref()).await;
    submission
        .acknowledge([(key(), EntitySnapshot::from(readback.clone().into_map()))])
        .unwrap();
    let context = teaql_runtime::SavedEntityContext::new(&root, false);
    let saved = SavedAsset::from_compact_row_after_save(readback, &context, false).unwrap();
    let state = saved.entity_runtime_state().unwrap();
    assert_eq!(state, root);
    assert!(state.has_pending_submission());
    assert_eq!(state.get_original_version(&key()), Some(2));
    state.set(key(), "document", Value::BlobRef(blob("late")));
    tx.rollback().await.unwrap();
    assert_eq!(state.get_original_version(&key()), Some(1));
    assert_eq!(root.get_original_version(&key()), Some(1));
    assert_eq!(
        state.original_snapshot().unwrap().get("document"),
        Some(&Value::Text(blob("original").as_str().into()))
    );
    assert_eq!(
        state.get(&key(), "document"),
        Some(Value::BlobRef(blob("late")))
    );
    assert!(!state.requires_reload_after_completion());
    let actual = f
        .admin
        .get()
        .await
        .unwrap()
        .query_one(
            &format!(
                "SELECT version,document FROM \"{}\".r5_assets WHERE id=42",
                f.schema
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(actual.get::<_, i64>("version"), 1);
    assert_eq!(
        actual.get::<_, String>("document"),
        blob("original").as_str()
    );
    println!("R5_SAVED_HYDRATION_REAL_ROLLBACK_ALIAS_BASELINE_PASS");
    f.finish().await;
}

#[tokio::test]
async fn new_saved_readback_has_no_loaded_authority_after_rollback_or_actual_ack_loss() {
    for unknown in [false, true] {
        let mut proxy = super::support::proxy::ProtocolFault::new().await;
        let f = Fixture::with_port(true, proxy.port, 1).await;
        let original =
            SavedAsset::from_compact_row(CompactRow::from_map(std::collections::BTreeMap::from([
                ("id".into(), Value::U64(42)),
                ("version".into(), Value::I64(0)),
                ("document".into(), Value::Null),
            ])))
            .unwrap();
        let root = original.entity_runtime_state().unwrap();
        root.mark_as_new(key());
        root.set(key(), "document", Value::BlobRef(blob("new")));
        let tx = f.root.begin().await.unwrap();
        let identity = tx.transaction_identity().unwrap();
        let submission = root.begin_submission(&identity).unwrap();
        f.write(tx.as_ref(), &f.target(42, "document"), Some(&blob("new")))
            .await;
        let readback = row(tx.as_ref()).await;
        submission
            .acknowledge([(key(), EntitySnapshot::from(readback.clone().into_map()))])
            .unwrap();
        let context = teaql_runtime::SavedEntityContext::new(&root, false);
        let saved = SavedAsset::from_compact_row_after_save(readback, &context, false).unwrap();
        let alias = saved.clone().entity_runtime_state().unwrap();
        assert_eq!(alias, root);
        if unknown {
            proxy.drop_commit_ack();
            assert_eq!(
                tx.commit().await.unwrap_err(),
                InfrastructureSqlError::TransactionUnknown
            );
            proxy.intercepted().await;
        } else {
            tx.rollback().await.unwrap();
        }
        assert!(alias.is_new(&key()));
        assert_eq!(alias.get_original_version(&key()), None);
        assert_eq!(alias.original_snapshot(), None);
        assert_eq!(alias.requires_reload_after_completion(), unknown);
        assert_eq!(f.count("r5_assets").await, i64::from(unknown));
        if unknown {
            let fresh = SavedAsset::from_compact_row(row(f.root.as_ref()).await)
                .unwrap()
                .entity_runtime_state()
                .unwrap();
            assert_ne!(fresh, alias);
            assert!(!fresh.is_new(&key()));
            assert_eq!(fresh.get_original_version(&key()), Some(1));
            assert!(!fresh.requires_reload_after_completion());
        }
        println!("R5_SAVED_HYDRATION_NEW_ROW_ORIGIN_PASS unknown={unknown}");
        f.finish().await;
        drop(proxy);
    }
}
