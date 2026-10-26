//! Actual GraphSaverFor entrypoint with transaction-owned Blob bindings.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use teaql_core::{
    Audited, BlobFieldSpec, BlobRef, CompactRow, DataType, Entity, EntityDescriptor, EntityError,
    MutationValues, PropertyDescriptor, TeaqlEntity, Value,
};
use teaql_runtime::{
    AuditedSaveExt, BlobMutationParticipantResource, InMemoryMetadataStore, InternalIdGenerator,
    LedgerEntity, RuntimeError, UserContext, save_audited_ledger_entity_with_native_executor,
};

impl LedgerEntity for Asset {
    fn entity_runtime_state(&self) -> Option<teaql_runtime::EntityRuntimeState> {
        None
    }
}

#[derive(Clone)]
struct Asset {
    id: u64,
    version: i64,
    document: Option<BlobRef>,
    is_new: bool,
    deleted: bool,
    dirty: Option<BTreeSet<String>>,
    comment: Option<String>,
}

impl TeaqlEntity for Asset {
    const ENTITY_NAME: &'static str = "Asset";

    fn entity_descriptor() -> EntityDescriptor {
        EntityDescriptor::new("Asset")
            .table_name("r5_assets")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("document", DataType::BlobRef).blob_field_spec(
                    BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap(),
                ),
            )
    }
}

impl Entity for Asset {
    fn from_compact_row(row: CompactRow) -> Result<Self, EntityError> {
        let id = row
            .get("id")
            .and_then(Value::try_u64)
            .ok_or_else(|| EntityError::new("Asset", "missing id"))?;
        let version = row
            .get("version")
            .and_then(Value::try_i64)
            .ok_or_else(|| EntityError::new("Asset", "missing version"))?;
        let document = match row.get("document") {
            None | Some(Value::Null) => None,
            Some(Value::BlobRef(value)) => Some(value.clone()),
            Some(Value::Text(value)) => Some(
                BlobRef::new(value.clone())
                    .map_err(|_| EntityError::new("Asset", "invalid Blob key"))?,
            ),
            _ => return Err(EntityError::new("Asset", "invalid Blob value")),
        };
        Ok(Self {
            id,
            version,
            document,
            is_new: false,
            deleted: false,
            dirty: None,
            comment: None,
        })
    }

    fn into_values(self) -> MutationValues {
        BTreeMap::from([
            ("id".into(), Value::U64(self.id)),
            ("version".into(), Value::I64(self.version)),
            (
                "document".into(),
                self.document.map_or(Value::Null, Value::BlobRef),
            ),
        ])
        .into()
    }

    fn is_new(&self) -> bool {
        self.is_new
    }

    fn is_marked_as_delete(&self) -> bool {
        self.deleted
    }

    fn dirty_fields(&self) -> Option<BTreeSet<String>> {
        self.dirty.clone()
    }

    fn get_comment(&self) -> Option<String> {
        self.comment.clone()
    }

    fn set_comment(&mut self, comment: String) {
        self.comment = Some(comment);
    }
}

struct Id42;
impl InternalIdGenerator for Id42 {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        assert_eq!(entity, "Asset");
        Ok(42)
    }
}

#[tokio::test]
async fn audited_graph_create_replace_delete_commits_business_row_and_blob_binding_together() {
    let f = Fixture::new(true).await;
    let descriptor = Asset::entity_descriptor();
    let spec = descriptor
        .property_by_name("document")
        .unwrap()
        .blob_field_spec
        .as_ref()
        .unwrap();
    let policies = Arc::new(BlobPolicies::default());
    let policy = policies.resolve(spec).unwrap();
    let target = f.target(42, "document");
    let first = f
        .upload("graph_first", target.clone(), policy.clone(), 0)
        .await;
    let second = f.upload("graph_second", target.clone(), policy, 0).await;
    let participant = BlobMutationParticipant::new(
        f.catalog.clone(),
        policies,
        Arc::new(BindAuthorization),
        Arc::new(BindClock),
    );
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor))
        .with_internal_id_generator(Id42);
    context.insert_resource(BindPrincipal(Arc::new(std::sync::atomic::AtomicBool::new(
        true,
    ))));
    context.insert_resource(BlobMutationParticipantResource(Arc::new(participant)));
    context.register_executor(f.business());

    let created = Audited::new(
        Asset {
            id: 0,
            version: 0,
            document: Some(first.clone()),
            is_new: true,
            deleted: false,
            dirty: None,
            comment: None,
        },
        "create asset with Blob",
    )
    .save(&context)
    .await
    .unwrap();
    assert_eq!((created.id, created.version), (42, 1));
    assert_eq!(created.document, Some(first.clone()));
    assert_eq!(f.count("r5_assets").await, 1);
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(first)
    );

    let replaced = Audited::new(
        Asset {
            document: Some(second.clone()),
            dirty: Some(BTreeSet::from(["document".into()])),
            ..created
        },
        "replace asset Blob",
    )
    .save(&context)
    .await
    .unwrap();
    assert_eq!(replaced.version, 2);
    assert_eq!(replaced.document, Some(second.clone()));
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(second)
    );

    Audited::new(
        Asset {
            deleted: true,
            ..replaced
        },
        "delete asset and release Blob",
    )
    .save(&context)
    .await
    .unwrap();
    assert_eq!(f.count("r5_assets").await, 1);
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT version AS value FROM r5_assets WHERE id=42"
        )
        .await,
        -3
    );
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        None
    );
    f.finish().await;
}

#[tokio::test]
async fn raw_native_graph_without_ledger_applies_blob_before_caller_commit() {
    use teaql_data_service::{Transaction, TransactionExecutor};

    let f = Fixture::new(true).await;
    let descriptor = Asset::entity_descriptor();
    let policy = BlobPolicies::default()
        .resolve(
            descriptor
                .property_by_name("document")
                .unwrap()
                .blob_field_spec
                .as_ref()
                .unwrap(),
        )
        .unwrap();
    let target = f.target(42, "document");
    let reference = f.upload("raw_graph_blob", target.clone(), policy, 0).await;
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor))
        .with_internal_id_generator(Id42);
    context.insert_resource(BindPrincipal(Arc::new(std::sync::atomic::AtomicBool::new(
        true,
    ))));
    context.insert_resource(BlobMutationParticipantResource(Arc::new(
        BlobMutationParticipant::new(
            f.catalog.clone(),
            Arc::new(BlobPolicies::default()),
            Arc::new(BindAuthorization),
            Arc::new(BindClock),
        ),
    )));
    let business = f.business();
    let tx = TransactionExecutor::begin(&business).await.unwrap();
    let saved = save_audited_ledger_entity_with_native_executor(
        Audited::new(
            Asset {
                id: 0,
                version: 0,
                document: Some(reference.clone()),
                is_new: true,
                deleted: false,
                dirty: None,
                comment: None,
            },
            "raw graph Blob create",
        ),
        Arc::new(context),
        &tx,
    )
    .await
    .unwrap();
    assert_eq!((saved.id, saved.version), (42, 1));
    assert_eq!(f.count("r5_assets").await, 0);
    assert!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .is_none()
    );
    Transaction::commit(tx).await.unwrap();
    assert_eq!(f.count("r5_assets").await, 1);
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(reference)
    );
    f.finish().await;
}

#[derive(Clone)]
struct MultiFieldAsset {
    id: u64,
    version: i64,
    document: Option<BlobRef>,
    preview: Option<BlobRef>,
    preview_loaded: bool,
    dirty: BTreeSet<String>,
    comment: Option<String>,
}

impl MultiFieldAsset {
    fn partial(id: u64, version: i64, document: Option<BlobRef>) -> Self {
        Self {
            id,
            version,
            document,
            preview: None,
            preview_loaded: false,
            dirty: BTreeSet::from(["document".into()]),
            comment: None,
        }
    }
}

impl TeaqlEntity for MultiFieldAsset {
    const ENTITY_NAME: &'static str = "Asset";

    fn entity_descriptor() -> EntityDescriptor {
        EntityDescriptor::new("Asset")
            .table_name("r5_assets")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("document", DataType::BlobRef).blob_field_spec(
                    BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap(),
                ),
            )
            .property(
                PropertyDescriptor::new("preview", DataType::BlobRef).blob_field_spec(
                    BlobFieldSpec::new("Asset", "preview", ["pdf"], true, "default").unwrap(),
                ),
            )
    }
}

impl Entity for MultiFieldAsset {
    fn from_compact_row(row: CompactRow) -> Result<Self, EntityError> {
        fn blob(row: &CompactRow, field: &str) -> Result<Option<BlobRef>, EntityError> {
            match row.get(field) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::BlobRef(value)) => Ok(Some(value.clone())),
                Some(Value::Text(value)) => BlobRef::new(value.clone())
                    .map(Some)
                    .map_err(|_| EntityError::new("Asset", "invalid Blob key")),
                _ => Err(EntityError::new("Asset", "invalid Blob value")),
            }
        }
        Ok(Self {
            id: row
                .get("id")
                .and_then(Value::try_u64)
                .ok_or_else(|| EntityError::new("Asset", "missing id"))?,
            version: row
                .get("version")
                .and_then(Value::try_i64)
                .ok_or_else(|| EntityError::new("Asset", "missing version"))?,
            document: blob(&row, "document")?,
            preview: blob(&row, "preview")?,
            preview_loaded: true,
            dirty: BTreeSet::new(),
            comment: None,
        })
    }

    fn into_values(self) -> MutationValues {
        BTreeMap::from([
            ("id".into(), Value::U64(self.id)),
            ("version".into(), Value::I64(self.version)),
            (
                "document".into(),
                self.document.map_or(Value::Null, Value::BlobRef),
            ),
            (
                "preview".into(),
                self.preview.map_or(Value::Null, Value::BlobRef),
            ),
        ])
        .into()
    }

    fn is_field_loaded(&self, field: &str) -> bool {
        field != "preview" || self.preview_loaded
    }

    fn dirty_fields(&self) -> Option<BTreeSet<String>> {
        Some(self.dirty.clone())
    }

    fn get_comment(&self) -> Option<String> {
        self.comment.clone()
    }

    fn set_comment(&mut self, comment: String) {
        self.comment = Some(comment);
    }
}

fn multi_field_context(f: &Fixture) -> UserContext {
    let mut context = UserContext::new().with_metadata(
        InMemoryMetadataStore::new().with_entity(MultiFieldAsset::entity_descriptor()),
    );
    context.insert_resource(BindPrincipal(Arc::new(std::sync::atomic::AtomicBool::new(
        true,
    ))));
    context.insert_resource(BlobMutationParticipantResource(Arc::new(
        BlobMutationParticipant::new(
            f.catalog.clone(),
            Arc::new(BlobPolicies::default()),
            Arc::new(BindAuthorization),
            Arc::new(BindClock),
        ),
    )));
    context.register_executor(f.business());
    context
}

#[tokio::test]
async fn native_graph_partial_update_keeps_unloaded_shared_blob_until_last_clear() {
    let f = Fixture::new(true).await;
    let policies = BlobPolicies::default();
    let descriptor = MultiFieldAsset::entity_descriptor();
    let document_policy = policies
        .resolve(
            descriptor
                .property_by_name("document")
                .unwrap()
                .blob_field_spec
                .as_ref()
                .unwrap(),
        )
        .unwrap();
    let preview_policy = policies
        .resolve(
            descriptor
                .property_by_name("preview")
                .unwrap()
                .blob_field_spec
                .as_ref()
                .unwrap(),
        )
        .unwrap();
    let document = f.target(42, "document");
    let preview = f.target(42, "preview");
    let shared = f
        .upload("graph_shared", document.clone(), document_policy.clone(), 0)
        .await;
    let replacement = f
        .upload(
            "graph_replacement",
            document.clone(),
            document_policy.clone(),
            0,
        )
        .await;
    f.bind(
        document.clone(),
        None,
        Some(shared.clone()),
        document_policy,
        100,
    )
    .await;
    f.bind(
        preview.clone(),
        None,
        Some(shared.clone()),
        preview_policy,
        100,
    )
    .await;
    let preview_before = f
        .catalog
        .binding(&f.scope(), &preview)
        .await
        .unwrap()
        .unwrap();
    let context = multi_field_context(&f);
    let saved = Audited::new(
        MultiFieldAsset::partial(42, 2, Some(replacement.clone())),
        "replace only loaded document field",
    )
    .save(&context)
    .await
    .unwrap();
    assert_eq!(saved.version, 3);
    assert_eq!(saved.document, Some(replacement.clone()));
    assert_eq!(saved.preview, Some(shared.clone()));
    let preview_after = f
        .catalog
        .binding(&f.scope(), &preview)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(preview_after.reference, Some(shared.clone()));
    assert_eq!(preview_after.version, preview_before.version);
    assert_eq!(
        f.catalog
            .find(&f.scope(), &shared)
            .await
            .unwrap()
            .unwrap()
            .unreferenced_at(),
        None
    );
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT CASE WHEN (SELECT xmin::text FROM r5_assets WHERE id=42)=(SELECT xmin::text FROM teaql_blob_binding WHERE entity_id=42 AND field_name='document') THEN 1 ELSE 0 END AS value"
        )
        .await,
        1
    );
    let cleared = Audited::new(
        MultiFieldAsset {
            id: 42,
            version: 3,
            document: Some(replacement),
            preview: None,
            preview_loaded: true,
            dirty: BTreeSet::from(["preview".into()]),
            comment: None,
        },
        "clear last shared preview reference",
    )
    .save(&context)
    .await
    .unwrap();
    assert_eq!(cleared.version, 4);
    assert_eq!(cleared.preview, None);
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &preview)
            .await
            .unwrap()
            .unwrap()
            .reference,
        None
    );
    assert!(
        f.catalog
            .find(&f.scope(), &shared)
            .await
            .unwrap()
            .unwrap()
            .unreferenced_at()
            .is_some()
    );
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT CASE WHEN (SELECT xmin::text FROM r5_assets WHERE id=42)=(SELECT xmin::text FROM teaql_blob_binding WHERE entity_id=42 AND field_name='preview') THEN 1 ELSE 0 END AS value"
        )
        .await,
        1
    );
    println!("R5_RUST_NATIVE_GRAPH_SHARED_UNLOADED_PASS");
    f.finish().await;
}

#[tokio::test]
async fn native_graph_concurrent_replacement_has_one_winner_and_fresh_retry() {
    let f = Fixture::new(true).await;
    let policy = BlobPolicies::default()
        .resolve(
            MultiFieldAsset::entity_descriptor()
                .property_by_name("document")
                .unwrap()
                .blob_field_spec
                .as_ref()
                .unwrap(),
        )
        .unwrap();
    let target = f.target(42, "document");
    let old = f
        .upload("graph_race_old", target.clone(), policy.clone(), 0)
        .await;
    let first = f
        .upload("graph_race_first", target.clone(), policy.clone(), 0)
        .await;
    let second = f
        .upload("graph_race_second", target.clone(), policy.clone(), 0)
        .await;
    f.bind(target.clone(), None, Some(old), policy, 100).await;
    let context = multi_field_context(&f);
    let (left, right) = tokio::join!(
        Audited::new(
            MultiFieldAsset::partial(42, 1, Some(first.clone())),
            "first concurrent Blob replacement"
        )
        .save(&context),
        Audited::new(
            MultiFieldAsset::partial(42, 1, Some(second.clone())),
            "second concurrent Blob replacement"
        )
        .save(&context)
    );
    assert_ne!(left.is_ok(), right.is_ok());
    let (winner, loser) = if left.is_ok() {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(winner)
    );
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT version AS value FROM r5_assets WHERE id=42"
        )
        .await,
        2
    );
    let retried = Audited::new(
        MultiFieldAsset::partial(42, 2, Some(loser.clone())),
        "retry rejected concurrent Blob replacement from current version",
    )
    .save(&context)
    .await
    .unwrap();
    assert_eq!(retried.version, 3);
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(loser)
    );
    println!("R5_RUST_NATIVE_GRAPH_CONCURRENT_RETRY_PASS");
    f.finish().await;
}
