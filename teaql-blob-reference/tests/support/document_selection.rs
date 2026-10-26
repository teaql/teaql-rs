//! tqd2/tqs1 selection acceptance on the same real PostgreSQL owner as business DML.
use super::*;
use chrono::DateTime;
use std::time::Duration;
use teaql_core::{BlobFieldSpec, BlobRef, DataType, EntityDescriptor, PropertyDescriptor, Value};
use teaql_runtime::{
    BlobSelectionClaim, BlobSelectionTarget, BlobSelectionTargetKind, BoundDocumentSnapshot,
    DocumentBlobBindingSnapshot, DocumentEntitySnapshot, DocumentMutationKind,
    DocumentRelationCompleteness, DocumentRelationSnapshot, DocumentSnapshot, EntityKey,
    InMemoryMetadataStore, MutationOperation, MutationOperationKind, MutationPlan,
    ReferenceDocumentScope, ReferenceIdentity, RoundTripReferenceError, TrustedReferencePrincipal,
    UserContext,
};
use teaql_secure_document_reference::{
    ContextBoundReferenceRuntime, DeploymentProfile, ReferenceKey, StaticReferenceKeyProvider,
};

fn context() -> UserContext {
    context_for("actor-a")
}

fn context_for(subject: &str) -> UserContext {
    let provider = ContextBoundReferenceRuntime::governed(
        DeploymentProfile::Test,
        "asset-service",
        "test",
        Arc::new(
            StaticReferenceKeyProvider::new("k1", [ReferenceKey::new("k1", [0x42; 32]).unwrap()])
                .unwrap(),
        ),
        Arc::new(
            |_: &TrustedReferencePrincipal,
             _: &ReferenceDocumentScope,
             _: &ReferenceIdentity|
             -> Result<(), RoundTripReferenceError> { Ok(()) },
        ),
    )
    .unwrap()
    .with_clock(|| DateTime::from_timestamp(1_800_000_000, 0).unwrap())
    .with_nonce_source(|| [0x33; 12]);
    let spec = BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap();
    let descriptor = EntityDescriptor::new("Asset")
        .table_name("r5_assets")
        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new("document", DataType::BlobRef).blob_field_spec(spec));
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor))
        .with_round_trip_reference_runtime(Arc::new(provider))
        .with_trusted_reference_principal(
            TrustedReferencePrincipal::new("oidc", subject, "Tenant", 7).unwrap(),
        );
    context.insert_resource(BindPrincipal(Arc::new(std::sync::atomic::AtomicBool::new(
        true,
    ))));
    context
}

fn participant(f: &Fixture) -> BlobMutationParticipant {
    BlobMutationParticipant::new(
        f.catalog.clone(),
        Arc::new(BlobPolicies::default()),
        Arc::new(BindAuthorization),
        Arc::new(BindClock),
    )
}

fn bound_document(old: Option<BlobRef>, version: u64) -> BoundDocumentSnapshot {
    let scope = ReferenceDocumentScope::new("asset-42-v1", "edit-asset", "Asset", 42, 1).unwrap();
    let snapshot = DocumentSnapshot::new(
        "asset-model-v1",
        "ASSET-42",
        scope,
        ReferenceIdentity::new("Asset", 42, 1).unwrap(),
        vec![
            DocumentEntitySnapshot::new(
                "asset:0",
                ReferenceIdentity::new("Asset", 42, 1).unwrap(),
                ["document"],
                ["document"],
                [
                    DocumentMutationKind::Update,
                    DocumentMutationKind::RemoveChild,
                ],
            )
            .unwrap(),
        ],
        vec![
            DocumentRelationSnapshot::new(
                "assets",
                ["asset:0"],
                DocumentRelationCompleteness::Complete,
                true,
                [DocumentMutationKind::RemoveChild],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    BoundDocumentSnapshot::new(
        snapshot,
        vec![DocumentBlobBindingSnapshot {
            row_key: "asset:0".into(),
            canonical_field: "document".into(),
            entity_type: "Asset".into(),
            reference: old,
            binding_version: version,
            nullable: true,
            writable: true,
        }],
    )
    .unwrap()
}

fn existing_target(scope: ReferenceDocumentScope, command: &str) -> BlobSelectionTarget {
    BlobSelectionTarget {
        scope,
        target_entity: "Asset".into(),
        canonical_field: "document".into(),
        kind: BlobSelectionTargetKind::ExistingRow,
        target_key: "asset:0".into(),
        command_id: command.into(),
    }
}

fn update(command: &str, next: BlobRef) -> MutationPlan {
    MutationPlan {
        execution_id: command.into(),
        request_key: "Asset.save".into(),
        root_entity_type: "Asset".into(),
        audit_reason: Some("replace selected document".into()),
        operations: vec![MutationOperation {
            kind: MutationOperationKind::Update,
            entity: EntityKey::new("Asset", 42_u64),
            original_version: Some(1),
            changed_values: BTreeMap::from([("document".into(), Value::BlobRef(next))]),
        }],
    }
}

fn request<'a>(
    token: &'a str,
    aggregate_type: &'a str,
    purpose: &'a str,
    command_id: &'a str,
    selections: &'a [DocumentBlobSelectionInput<'a>],
) -> DocumentBlobMutationRequest<'a> {
    DocumentBlobMutationRequest {
        document_token: token,
        aggregate_type,
        purpose,
        command_id,
        selections,
    }
}

#[tokio::test]
async fn selected_replacement_requires_current_binding_and_commits_with_business_row() {
    let f = Fixture::new(true).await;
    let target = f.target(42, "document");
    let spec = BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap();
    let policy = BlobPolicies::default().resolve(&spec).unwrap();
    let old = f
        .upload("document_old", target.clone(), policy.clone(), 0)
        .await;
    let next = f
        .upload("document_next", target.clone(), policy.clone(), 0)
        .await;
    let stale_next = f
        .upload("document_stale", target.clone(), policy.clone(), 0)
        .await;
    f.bind(target.clone(), None, Some(old.clone()), policy, 100)
        .await;
    let version = f
        .catalog
        .binding(&f.scope(), &target)
        .await
        .unwrap()
        .unwrap()
        .version;
    let context = context();
    let participant = participant(&f);
    let document = bound_document(Some(old.clone()), u64::try_from(version).unwrap());
    let scope = document.snapshot.scope.clone();
    let token = context
        .issue_bound_document_snapshot(document, Duration::from_secs(300))
        .unwrap();
    let selected = existing_target(scope.clone(), "replace-1");
    let selection = context
        .issue_blob_selection(
            &token,
            BlobSelectionClaim {
                target: selected.clone(),
                reference: next.clone(),
            },
            Duration::from_secs(120),
        )
        .unwrap();
    let input = [DocumentBlobSelectionInput {
        token: &selection,
        target: selected.clone(),
    }];
    let approved = update("replace-1", next.clone());
    assert_eq!(
        participant
            .plan_document_selections(
                &context,
                &approved,
                request(&token, "Asset", "edit-asset", "replace-1", &[]),
            )
            .err()
            .unwrap()
            .code,
        BlobErrorCode::BindDenied,
        "a raw BlobRef in an approved plan cannot replace a missing tqs1"
    );
    let bob = context_for("bob");
    let bob_document = bound_document(Some(old.clone()), u64::try_from(version).unwrap());
    let bob_scope = bob_document.snapshot.scope.clone();
    let bob_token = bob
        .issue_bound_document_snapshot(bob_document, Duration::from_secs(300))
        .unwrap();
    let bob_target = existing_target(bob_scope, "replace-1");
    let bob_selection = bob
        .issue_blob_selection(
            &bob_token,
            BlobSelectionClaim {
                target: bob_target.clone(),
                reference: next.clone(),
            },
            Duration::from_secs(120),
        )
        .unwrap();
    assert_eq!(
        participant
            .plan_document_selections(
                &bob,
                &approved,
                request(
                    &bob_token,
                    "Asset",
                    "edit-asset",
                    "replace-1",
                    &[DocumentBlobSelectionInput {
                        token: &bob_selection,
                        target: bob_target
                    }],
                ),
            )
            .err()
            .unwrap()
            .code,
        BlobErrorCode::BindDenied,
        "a document Actor must explicitly map to the current Blob identity"
    );
    assert_eq!(
        participant
            .plan_document_selections(
                &context,
                &approved,
                request(&token, "Asset", "edit-asset", "different-command", &input),
            )
            .err()
            .unwrap()
            .code,
        BlobErrorCode::BindDenied
    );

    let granted = context.get_resource::<BindPrincipal>().unwrap().0.clone();
    let tx = f.root.begin().await.unwrap();
    let plan = participant
        .plan_document_selections(
            &context,
            &approved,
            request(&token, "Asset", "edit-asset", "replace-1", &input),
        )
        .unwrap()
        .unwrap();
    let prepared = participant
        .prepare(&context, tx.as_ref(), plan, &BTreeMap::new())
        .await
        .unwrap();
    f.write(tx.as_ref(), &target, Some(&next)).await;
    granted.store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        participant
            .apply(&context, tx.as_ref(), prepared)
            .await
            .err()
            .unwrap()
            .code,
        BlobErrorCode::BindDenied
    );
    granted.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(tx.commit().await.is_err());
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(old.clone())
    );
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT version AS value FROM r5_assets WHERE id=42"
        )
        .await,
        1
    );

    let tx = f.root.begin().await.unwrap();
    let plan = participant
        .plan_document_selections(
            &context,
            &approved,
            request(&token, "Asset", "edit-asset", "replace-1", &input),
        )
        .unwrap()
        .unwrap();
    let prepared = participant
        .prepare(&context, tx.as_ref(), plan, &BTreeMap::new())
        .await
        .unwrap();
    f.write(tx.as_ref(), &target, Some(&next)).await;
    participant
        .apply(&context, tx.as_ref(), prepared)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(next.clone())
    );
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT version AS value FROM r5_assets WHERE id=42"
        )
        .await,
        2
    );

    let stale_target = existing_target(scope, "stale-1");
    let stale_selection = context
        .issue_blob_selection(
            &token,
            BlobSelectionClaim {
                target: stale_target.clone(),
                reference: stale_next.clone(),
            },
            Duration::from_secs(120),
        )
        .unwrap();
    let tx = f.root.begin().await.unwrap();
    let stale_plan = participant
        .plan_document_selections(
            &context,
            &update("stale-1", stale_next),
            request(
                &token,
                "Asset",
                "edit-asset",
                "stale-1",
                &[DocumentBlobSelectionInput {
                    token: &stale_selection,
                    target: stale_target,
                }],
            ),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        participant
            .prepare(&context, tx.as_ref(), stale_plan, &BTreeMap::new())
            .await
            .err()
            .unwrap()
            .code,
        BlobErrorCode::IdempotencyConflict
    );
    assert!(tx.commit().await.is_err());
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(next.clone())
    );
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT version AS value FROM r5_assets WHERE id=42"
        )
        .await,
        2
    );

    let absent = BlobRef::new("blob_not_uploaded").unwrap();
    let current_binding = f
        .catalog
        .binding(&f.scope(), &target)
        .await
        .unwrap()
        .unwrap();
    let current_document =
        bound_document(Some(next), u64::try_from(current_binding.version).unwrap());
    let missing_target = existing_target(current_document.snapshot.scope.clone(), "missing-1");
    let current_token = context
        .issue_bound_document_snapshot(current_document, Duration::from_secs(300))
        .unwrap();
    let missing_selection = context
        .issue_blob_selection(
            &current_token,
            BlobSelectionClaim {
                target: missing_target.clone(),
                reference: absent.clone(),
            },
            Duration::from_secs(120),
        )
        .unwrap();
    let tx = f.root.begin().await.unwrap();
    let missing_plan = participant
        .plan_document_selections(
            &context,
            &update("missing-1", absent),
            request(
                &current_token,
                "Asset",
                "edit-asset",
                "missing-1",
                &[DocumentBlobSelectionInput {
                    token: &missing_selection,
                    target: missing_target,
                }],
            ),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        participant
            .prepare(&context, tx.as_ref(), missing_plan, &BTreeMap::new())
            .await
            .err()
            .unwrap()
            .code,
        BlobErrorCode::BindDenied
    );
    assert!(tx.commit().await.is_err());
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap(),
        current_binding
    );
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT version AS value FROM r5_assets WHERE id=42"
        )
        .await,
        2
    );
    f.finish().await;
}

#[tokio::test]
async fn selected_new_client_key_requires_resolved_id_before_business_dml() {
    let f = Fixture::new(true).await;
    let final_target = f.target(77, "document");
    let spec = BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap();
    let policy = BlobPolicies::default().resolve(&spec).unwrap();
    let selected_key = f
        .upload("document_client", final_target.clone(), policy, 0)
        .await;
    let context = context();
    let participant = participant(&f);
    let scope = ReferenceDocumentScope::new("folder-7-v1", "edit-folder", "Folder", 7, 1).unwrap();
    let document = BoundDocumentSnapshot::new(
        DocumentSnapshot::new(
            "folder-model-v1",
            "FOLDER-7",
            scope.clone(),
            ReferenceIdentity::new("Folder", 7, 1).unwrap(),
            vec![],
            vec![
                DocumentRelationSnapshot::new(
                    "assets",
                    Vec::<String>::new(),
                    DocumentRelationCompleteness::Complete,
                    true,
                    [DocumentMutationKind::CreateChild],
                )
                .unwrap(),
            ],
        )
        .unwrap(),
        vec![],
    )
    .unwrap();
    let token = context
        .issue_bound_document_snapshot(document, Duration::from_secs(300))
        .unwrap();
    let target = BlobSelectionTarget {
        scope,
        target_entity: "Asset".into(),
        canonical_field: "document".into(),
        kind: BlobSelectionTargetKind::NewClientKey,
        target_key: "client-row-7".into(),
        command_id: "create-1".into(),
    };
    let selection = context
        .issue_blob_selection(
            &token,
            BlobSelectionClaim {
                target: target.clone(),
                reference: selected_key.clone(),
            },
            Duration::from_secs(120),
        )
        .unwrap();
    let temporary = EntityKey::new("Asset", Value::Text("client-row-7".into()));
    let approved = MutationPlan {
        execution_id: "create-1".into(),
        request_key: "Folder.save".into(),
        root_entity_type: "Folder".into(),
        audit_reason: Some("create selected document".into()),
        operations: vec![MutationOperation {
            kind: MutationOperationKind::Create,
            entity: temporary.clone(),
            original_version: None,
            changed_values: BTreeMap::from([(
                "document".into(),
                Value::BlobRef(selected_key.clone()),
            )]),
        }],
    };
    let input = [DocumentBlobSelectionInput {
        token: &selection,
        target: target.clone(),
    }];
    let tx = f.root.begin().await.unwrap();
    let plan = participant
        .plan_document_selections(
            &context,
            &approved,
            request(&token, "Folder", "edit-folder", "create-1", &input),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        participant
            .prepare(&context, tx.as_ref(), plan, &BTreeMap::new())
            .await
            .err()
            .unwrap()
            .code,
        BlobErrorCode::TransactionRequired
    );
    assert!(tx.commit().await.is_err());
    assert_eq!(f.count("r5_assets").await, 0);

    let tx = f.root.begin().await.unwrap();
    let plan = participant
        .plan_document_selections(
            &context,
            &approved,
            request(&token, "Folder", "edit-folder", "create-1", &input),
        )
        .unwrap()
        .unwrap();
    let prepared = participant
        .prepare(
            &context,
            tx.as_ref(),
            plan,
            &BTreeMap::from([(temporary, Value::U64(77))]),
        )
        .await
        .unwrap();
    f.write(tx.as_ref(), &final_target, Some(&selected_key))
        .await;
    participant
        .apply(&context, tx.as_ref(), prepared)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &final_target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(selected_key)
    );
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT version AS value FROM r5_assets WHERE id=77"
        )
        .await,
        1
    );
    f.finish().await;
}
