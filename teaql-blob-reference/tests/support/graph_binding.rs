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
