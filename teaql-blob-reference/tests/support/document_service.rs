//! Application-owned document open/accept, selection and receipt on real PostgreSQL.
use super::*;
use async_trait::async_trait;
use serde_json::{Value as JsonValue, json};
use std::sync::atomic::{AtomicBool, Ordering};
use teaql_provider_postgres::PgMutationExecutor;
use teaql_runtime::{
    AcceptedContextualDocument, ContextBoundDocumentService, ContextualDocument,
    DocumentAcceptRequest, DocumentOpenRequest, DocumentRoundTripError, MutationDecision,
    MutationPolicy, MutationPolicyIdentity, SubmittedContextualDocument,
    VerifiedBoundDocumentSnapshot, ViewAuthorization, ViewEntityTarget, ViewErrorCode,
    ViewFieldDecision, ViewIdentity, ViewInput, ViewOptions, ViewPath, ViewPreparationService,
};

const BUSINESS_ID: &str = "ASSET-42";
const PURPOSE: &str = "edit-asset";
const MODEL: &str = "asset-pg-document-v1";

struct AssetDocumentPolicy;

impl MutationPolicy for AssetDocumentPolicy {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("asset-document", "1", MODEL)
    }

    fn review(&self, _: &UserContext, plan: &MutationPlan) -> MutationDecision {
        let allowed = plan.request_key == "Asset.acceptDocument"
            && plan.root_entity_type == "Asset"
            && plan.audit_reason.is_some()
            && plan.operations.len() == 1
            && plan.operations[0].kind == MutationOperationKind::Update
            && plan.operations[0].entity == EntityKey::new("Asset", 42_u64)
            && plan.operations[0].original_version.is_some()
            && plan.operations[0].changed_values.len() == 1
            && matches!(
                plan.operations[0].changed_values.get("document"),
                Some(Value::BlobRef(_))
            );
        if allowed {
            MutationDecision::allow()
        } else {
            MutationDecision::deny(
                "ASSET_DOCUMENT_DENIED",
                "Invalid document mutation",
                ["document"],
            )
        }
    }
}

#[derive(Clone)]
struct PostgresAssetDocumentService {
    root: Arc<PgMutationExecutor>,
    catalog: PostgresBlobCatalog,
    mutation: BlobMutationParticipant,
    scope: BlobScope,
    fail_receipt: Arc<AtomicBool>,
}

impl PostgresAssetDocumentService {
    fn new(f: &Fixture) -> Self {
        Self {
            root: f.root.clone(),
            catalog: f.catalog.clone(),
            mutation: participant(f),
            scope: f.scope(),
            fail_receipt: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn require_access(
        &self,
        sql: &dyn InfrastructureSql,
        principal: &TrustedReferencePrincipal,
    ) -> Result<(), DocumentRoundTripError> {
        let rows = sql
            .fetch(
                &InfrastructureStatement::new(
                    "SELECT can_edit FROM r5_document_permission WHERE subject=$1 AND asset_id=$2 FOR SHARE",
                    vec![Value::Text(principal.subject.clone()), Value::U64(42)],
                )
                .unwrap(),
            )
            .await
            .map_err(|_| DocumentRoundTripError::configuration())?;
        if principal.subject != "actor-a"
            || rows.len() != 1
            || rows[0].get("can_edit").and_then(Value::try_i64) != Some(1)
        {
            return Err(DocumentRoundTripError::authorization_required());
        }
        Ok(())
    }

    async fn current_row(
        &self,
        sql: &dyn InfrastructureSql,
    ) -> Result<(i64, BlobBinding), DocumentRoundTripError> {
        let rows = sql
            .fetch(
                &InfrastructureStatement::new(
                    "SELECT version,document FROM r5_assets WHERE id=$1 FOR UPDATE",
                    vec![Value::U64(42)],
                )
                .unwrap(),
            )
            .await
            .map_err(|_| DocumentRoundTripError::configuration())?;
        if rows.len() != 1 {
            return Err(DocumentRoundTripError::not_found());
        }
        let version = rows[0]
            .get("version")
            .and_then(Value::try_i64)
            .ok_or_else(DocumentRoundTripError::projection_violation)?;
        let binding = self
            .catalog
            .binding_for_update(
                sql,
                &self.scope,
                &BlobFieldTarget::new("Asset", Some(42), "document").unwrap(),
            )
            .await
            .map_err(|_| DocumentRoundTripError::configuration())?;
        let row_key = match rows[0].get("document") {
            Some(Value::Text(value)) => Some(value.as_str()),
            Some(Value::Null) | None => None,
            _ => return Err(DocumentRoundTripError::projection_violation()),
        };
        if row_key != binding.reference.as_ref().map(BlobRef::as_str) {
            return Err(DocumentRoundTripError::projection_violation());
        }
        Ok((version, binding))
    }

    fn render(
        &self,
        context: &UserContext,
        version: i64,
        binding: &BlobBinding,
        lifetime: Duration,
    ) -> Result<ContextualDocument, DocumentRoundTripError> {
        let scope = ReferenceDocumentScope::new(
            format!("asset-42-v{version}"),
            PURPOSE,
            "Asset",
            42,
            version,
        )?;
        let snapshot = DocumentSnapshot::new(
            MODEL,
            BUSINESS_ID,
            scope,
            ReferenceIdentity::new("Asset", 42, version)?,
            vec![DocumentEntitySnapshot::new(
                "asset:0",
                ReferenceIdentity::new("Asset", 42, version)?,
                ["document"],
                ["document"],
                [DocumentMutationKind::Update],
            )?],
            vec![],
        )?;
        let document = BoundDocumentSnapshot::new(
            snapshot,
            vec![DocumentBlobBindingSnapshot {
                row_key: "asset:0".into(),
                canonical_field: "document".into(),
                entity_type: "Asset".into(),
                reference: binding.reference.clone(),
                binding_version: u64::try_from(binding.version)
                    .map_err(|_| DocumentRoundTripError::projection_violation())?,
                nullable: true,
                writable: true,
            }],
        )?;
        Ok(ContextualDocument {
            business_id: BUSINESS_ID.into(),
            document_token: context.issue_bound_document_snapshot(document, lifetime)?,
            body: json!({"assetId":42}),
        })
    }

    async fn accept_on_owner(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        request: &DocumentAcceptRequest,
        verified: &BoundDocumentSnapshot,
        fingerprint: &str,
        sql: &dyn InfrastructureSql,
    ) -> Result<AcceptedContextualDocument, DocumentRoundTripError> {
        self.require_access(sql, principal).await?;
        let receipt_rows = sql
            .fetch(
                &InfrastructureStatement::new(
                    "SELECT fingerprint,previous_version,new_version,result_token FROM r5_document_command WHERE scope=$1 AND actor=$2 AND command_id=$3 FOR UPDATE",
                    vec![
                        Value::Text(self.scope.boundary().into()),
                        Value::Text(principal.subject.clone()),
                        Value::Text(request.submitted.command_id.clone()),
                    ],
                )
                .unwrap(),
            )
            .await
            .map_err(|_| DocumentRoundTripError::configuration())?;
        if let Some(receipt) = receipt_rows.first() {
            if receipt.get("fingerprint") != Some(&Value::Text(fingerprint.into())) {
                return Err(DocumentRoundTripError::validation_failed());
            }
            let previous = receipt
                .get("previous_version")
                .and_then(Value::try_i64)
                .ok_or_else(DocumentRoundTripError::projection_violation)?;
            let next = receipt
                .get("new_version")
                .and_then(Value::try_i64)
                .ok_or_else(DocumentRoundTripError::projection_violation)?;
            let token = match receipt.get("result_token") {
                Some(Value::Text(token)) => token.clone(),
                _ => return Err(DocumentRoundTripError::projection_violation()),
            };
            return Ok(AcceptedContextualDocument {
                document: ContextualDocument {
                    business_id: BUSINESS_ID.into(),
                    document_token: token,
                    body: json!({"assetId":42}),
                },
                previous_revision: previous,
                new_revision: next,
                changed_entities: vec![ReferenceIdentity::new("Asset", 42, next)?],
            });
        }

        let (version, _) = self.current_row(sql).await?;
        if version != verified.snapshot.aggregate.version {
            return Err(DocumentRoundTripError::revision_conflict());
        }
        let expected = existing_target(
            verified.snapshot.scope.clone(),
            &request.submitted.command_id,
        );
        let selection_token = request.submitted.body["selection"]
            .as_str()
            .ok_or_else(DocumentRoundTripError::projection_violation)?;
        let selected = context.consume_blob_selection(
            &request.submitted.document_token,
            selection_token,
            &expected,
        )?;
        let approved = MutationPlan {
            execution_id: request.submitted.command_id.clone(),
            request_key: "Asset.acceptDocument".into(),
            root_entity_type: "Asset".into(),
            audit_reason: Some("replace selected document".into()),
            operations: vec![MutationOperation {
                kind: MutationOperationKind::Update,
                entity: EntityKey::new("Asset", 42_u64),
                original_version: Some(version),
                changed_values: BTreeMap::from([(
                    "document".into(),
                    Value::BlobRef(selected.claim.reference.clone()),
                )]),
            }],
        };
        if !AssetDocumentPolicy.review(context, &approved).is_allowed() {
            return Err(DocumentRoundTripError::validation_failed());
        }
        let input = [DocumentBlobSelectionInput {
            token: selection_token,
            target: expected,
        }];
        let plan = self
            .mutation
            .plan_document_selections(
                context,
                &approved,
                super::request(
                    &request.submitted.document_token,
                    "Asset",
                    PURPOSE,
                    &request.submitted.command_id,
                    &input,
                ),
            )
            .map_err(|_| DocumentRoundTripError::validation_failed())?
            .ok_or_else(DocumentRoundTripError::validation_failed)?;
        let prepared = self
            .mutation
            .prepare(context, sql, plan, &BTreeMap::new())
            .await
            .map_err(|_| DocumentRoundTripError::validation_failed())?;
        let changed = sql
            .execute(
                &InfrastructureStatement::new(
                    "UPDATE r5_assets SET document=$1,version=version+1 WHERE id=$2 AND version=$3",
                    vec![
                        Value::Text(selected.claim.reference.as_str().into()),
                        Value::U64(42),
                        Value::I64(version),
                    ],
                )
                .unwrap(),
            )
            .await
            .map_err(|_| DocumentRoundTripError::validation_failed())?;
        if changed != 1 {
            return Err(DocumentRoundTripError::revision_conflict());
        }
        self.mutation
            .apply(context, sql, prepared)
            .await
            .map_err(|_| DocumentRoundTripError::validation_failed())?;
        let (next_version, next_binding) = self.current_row(sql).await?;
        let result = self.render(
            context,
            next_version,
            &next_binding,
            Duration::from_secs(300),
        )?;
        sql.execute(
            &InfrastructureStatement::new(
                "INSERT INTO r5_document_command(scope,actor,command_id,fingerprint,previous_version,new_version,result_token) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                vec![
                    Value::Text(self.scope.boundary().into()),
                    Value::Text(principal.subject.clone()),
                    Value::Text(request.submitted.command_id.clone()),
                    Value::Text(fingerprint.into()),
                    Value::I64(version),
                    Value::I64(if self.fail_receipt.load(Ordering::SeqCst) { -1 } else { next_version }),
                    Value::Text(result.document_token.clone()),
                ],
            )
            .unwrap(),
        )
        .await
        .map_err(|_| DocumentRoundTripError::validation_failed())?;
        Ok(AcceptedContextualDocument {
            document: result,
            previous_revision: version,
            new_revision: next_version,
            changed_entities: vec![ReferenceIdentity::new("Asset", 42, next_version)?],
        })
    }
}

#[async_trait]
impl ContextBoundDocumentService for PostgresAssetDocumentService {
    fn supports_view_preparation(&self) -> bool {
        true
    }

    async fn validate_prepared_bound_document(
        &self,
        _context: &UserContext,
        principal: &TrustedReferencePrincipal,
        verified: &VerifiedBoundDocumentSnapshot,
    ) -> Result<(), DocumentRoundTripError> {
        if verified.document.snapshot.model_fingerprint != MODEL
            || verified.document.snapshot.business_id != BUSINESS_ID
            || verified.document.bindings.len() != 1
        {
            return Err(DocumentRoundTripError::projection_violation());
        }
        let sql = self
            .root
            .begin()
            .await
            .map_err(|_| DocumentRoundTripError::configuration())?;
        let result = async {
            self.require_access(sql.as_ref(), principal).await?;
            let (version, binding) = self.current_row(sql.as_ref()).await?;
            let issued = &verified.document.bindings[0];
            if version != verified.document.snapshot.aggregate.version
                || issued.row_key != "asset:0"
                || issued.canonical_field != "document"
                || issued.reference != binding.reference
                || i64::try_from(issued.binding_version).ok() != Some(binding.version)
            {
                return Err(DocumentRoundTripError::revision_conflict());
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                sql.commit()
                    .await
                    .map_err(|_| DocumentRoundTripError::configuration())?;
                Ok(())
            }
            Err(error) => {
                let _ = sql.rollback().await;
                Err(error)
            }
        }
    }

    async fn open_document(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        request: DocumentOpenRequest,
    ) -> Result<ContextualDocument, DocumentRoundTripError> {
        if request.aggregate_type != "Asset"
            || request.purpose != PURPOSE
            || request.business_id != BUSINESS_ID
        {
            return Err(DocumentRoundTripError::scope_mismatch());
        }
        let sql = self
            .root
            .begin()
            .await
            .map_err(|_| DocumentRoundTripError::configuration())?;
        let result = async {
            self.require_access(sql.as_ref(), principal).await?;
            let (version, binding) = self.current_row(sql.as_ref()).await?;
            self.render(context, version, &binding, request.lifetime)
        }
        .await;
        match result {
            Ok(document) => {
                sql.commit()
                    .await
                    .map_err(|_| DocumentRoundTripError::configuration())?;
                Ok(document)
            }
            Err(error) => {
                let _ = sql.rollback().await;
                Err(error)
            }
        }
    }

    async fn accept_document(
        &self,
        context: &UserContext,
        principal: &TrustedReferencePrincipal,
        request: DocumentAcceptRequest,
    ) -> Result<AcceptedContextualDocument, DocumentRoundTripError> {
        if request.expected_aggregate_type != "Asset"
            || request.purpose != PURPOSE
            || request.submitted.business_id != BUSINESS_ID
        {
            return Err(DocumentRoundTripError::scope_mismatch());
        }
        let Some(body) = request.submitted.body.as_object() else {
            return Err(DocumentRoundTripError::projection_violation());
        };
        if body.len() != 2
            || body.get("assetId").and_then(JsonValue::as_u64) != Some(42)
            || body.get("selection").and_then(JsonValue::as_str).is_none()
        {
            return Err(DocumentRoundTripError::projection_violation());
        }
        let verified = context.consume_bound_document_snapshot(
            &request.submitted.document_token,
            "Asset",
            PURPOSE,
        )?;
        if verified.document.snapshot.model_fingerprint != MODEL
            || verified.document.snapshot.business_id != BUSINESS_ID
        {
            return Err(DocumentRoundTripError::projection_violation());
        }
        let fingerprint = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&json!({
                    "aggregate":"Asset",
                    "purpose":PURPOSE,
                    "businessId":request.submitted.business_id,
                    "documentToken":request.submitted.document_token,
                    "commandId":request.submitted.command_id,
                    "body":request.submitted.body,
                }))
                .map_err(|_| DocumentRoundTripError::projection_violation())?
            )
        );
        let sql = self
            .root
            .begin()
            .await
            .map_err(|_| DocumentRoundTripError::configuration())?;
        let result = self
            .accept_on_owner(
                context,
                principal,
                &request,
                &verified.document,
                &fingerprint,
                sql.as_ref(),
            )
            .await;
        match result {
            Ok(accepted) => {
                sql.commit()
                    .await
                    .map_err(|_| DocumentRoundTripError::configuration())?;
                Ok(accepted)
            }
            Err(error) => {
                let _ = sql.rollback().await;
                Err(error)
            }
        }
    }
}

struct AssetDocumentPresentation;

impl ViewAuthorization for AssetDocumentPresentation {
    fn identity(&self, context: &UserContext) -> Result<ViewIdentity, ViewErrorCode> {
        let principal = context
            .trusted_reference_principal()
            .map_err(|_| ViewErrorCode::AccessDenied)?;
        ViewIdentity::new(
            &principal.authentication_realm,
            &principal.subject,
            format!(
                "{}:{}",
                principal.domain_root_type, principal.domain_root_id
            ),
        )
    }

    fn can_read_entity(
        &self,
        _: &UserContext,
        _: &ViewEntityTarget,
        _: &ViewPath,
    ) -> Result<bool, ViewErrorCode> {
        Ok(false)
    }

    fn field(
        &self,
        _: &UserContext,
        _: &ViewEntityTarget,
        _: &str,
        _: &ViewPath,
    ) -> Result<ViewFieldDecision, ViewErrorCode> {
        Ok(ViewFieldDecision::Hidden)
    }
}

#[tokio::test]
async fn context_document_open_accept_selection_and_receipt_use_one_postgres_owner() {
    let f = Fixture::new(true).await;
    execute(f.root.as_ref(), "CREATE TABLE r5_document_permission(subject TEXT NOT NULL,asset_id BIGINT NOT NULL,can_edit BIGINT NOT NULL,PRIMARY KEY(subject,asset_id))", vec![]).await;
    execute(
        f.root.as_ref(),
        "INSERT INTO r5_document_permission(subject,asset_id,can_edit) VALUES ($1,$2,$3)",
        vec![Value::Text("actor-a".into()), Value::U64(42), Value::I64(1)],
    )
    .await;
    execute(f.root.as_ref(), "CREATE TABLE r5_document_command(scope TEXT NOT NULL,actor TEXT NOT NULL,command_id TEXT NOT NULL,fingerprint TEXT NOT NULL,previous_version BIGINT NOT NULL,new_version BIGINT NOT NULL CHECK(new_version>0),result_token TEXT NOT NULL,PRIMARY KEY(scope,actor,command_id))", vec![]).await;
    let target = f.target(42, "document");
    let spec = BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap();
    let policy = BlobPolicies::default().resolve(&spec).unwrap();
    let old = f
        .upload("service_old", target.clone(), policy.clone(), 0)
        .await;
    let next = f
        .upload("service_next", target.clone(), policy.clone(), 0)
        .await;
    f.bind(target.clone(), None, Some(old.clone()), policy, 100)
        .await;
    let service = Arc::new(PostgresAssetDocumentService::new(&f));
    let mut context = context();
    context.set_context_bound_document_service(service.clone());
    ViewPreparationService::new(Arc::new(AssetDocumentPresentation))
        .install_into(&mut context)
        .unwrap();
    let view = context
        .open_document_for_view(
            DocumentOpenRequest::new("Asset", BUSINESS_ID, PURPOSE, Duration::from_secs(300))
                .unwrap(),
        )
        .await
        .unwrap();
    let options = ViewOptions::governed();
    let prepared = context
        .prepare_view(ViewInput::Document(&view), &options)
        .await
        .unwrap();
    assert!(
        prepared.document_credentials().unwrap()["documentToken"]
            .as_str()
            .unwrap()
            .starts_with("tqd2.")
    );
    assert!(!prepared.value().to_string().contains(old.as_str()));
    let opened = context
        .open_document(
            DocumentOpenRequest::new("Asset", BUSINESS_ID, PURPOSE, Duration::from_secs(300))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(opened.body, json!({"assetId":42}));
    let verified = context
        .consume_bound_document_snapshot(&opened.document_token, "Asset", PURPOSE)
        .unwrap();
    assert_eq!(verified.document.bindings[0].reference, Some(old.clone()));
    let selected = existing_target(verified.document.snapshot.scope.clone(), "service-1");
    let selection = context
        .issue_blob_selection(
            &opened.document_token,
            BlobSelectionClaim {
                target: selected,
                reference: next.clone(),
            },
            Duration::from_secs(120),
        )
        .unwrap();
    let submit = DocumentAcceptRequest::new(
        "Asset",
        PURPOSE,
        SubmittedContextualDocument {
            business_id: BUSINESS_ID.into(),
            document_token: opened.document_token.clone(),
            command_id: "service-1".into(),
            body: json!({"assetId":42,"selection":selection}),
        },
    )
    .unwrap();

    execute(
        f.root.as_ref(),
        "UPDATE r5_document_permission SET can_edit=0 WHERE subject=$1 AND asset_id=$2",
        vec![Value::Text("actor-a".into()), Value::U64(42)],
    )
    .await;
    assert_eq!(
        context
            .accept_document(submit.clone())
            .await
            .err()
            .unwrap()
            .code(),
        "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED"
    );
    execute(
        f.root.as_ref(),
        "UPDATE r5_document_permission SET can_edit=1 WHERE subject=$1 AND asset_id=$2",
        vec![Value::Text("actor-a".into()), Value::U64(42)],
    )
    .await;
    let mut raw_key = submit.clone();
    raw_key.submitted.body["selection"] = json!(next.as_str());
    assert_eq!(
        context.accept_document(raw_key).await.err().unwrap().code(),
        "ROUND_TRIP_REFERENCE_INVALID"
    );

    service.fail_receipt.store(true, Ordering::SeqCst);
    assert_eq!(
        context
            .accept_document(submit.clone())
            .await
            .err()
            .unwrap()
            .code(),
        "DOCUMENT_VALIDATION_FAILED"
    );
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT version AS value FROM r5_assets WHERE id=42"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT count(*) AS value FROM r5_document_command"
        )
        .await,
        0
    );
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(old)
    );
    service.fail_receipt.store(false, Ordering::SeqCst);
    let accepted = context.accept_document(submit.clone()).await.unwrap();
    assert_eq!((accepted.previous_revision, accepted.new_revision), (1, 2));
    assert_eq!(accepted.document.body, json!({"assetId":42}));
    let accepted_snapshot = context
        .consume_bound_document_snapshot(&accepted.document.document_token, "Asset", PURPOSE)
        .unwrap();
    assert_eq!(
        accepted_snapshot.document.bindings[0].reference,
        Some(next.clone())
    );
    assert_eq!(
        context
            .reuse_prepared_view(&prepared, &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::PreparationFailed,
        "the old view must not survive a committed binding replacement"
    );
    let current_view = context
        .open_document_for_view(
            DocumentOpenRequest::new("Asset", BUSINESS_ID, PURPOSE, Duration::from_secs(300))
                .unwrap(),
        )
        .await
        .unwrap();
    let current_prepared = context
        .prepare_view(ViewInput::Document(&current_view), &options)
        .await
        .unwrap();
    context
        .reuse_prepared_view(&current_prepared, &options)
        .await
        .unwrap();
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT version AS value FROM r5_assets WHERE id=42"
        )
        .await,
        2
    );
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT count(*) AS value FROM r5_document_command"
        )
        .await,
        1
    );
    let oracle = f
        .admin
        .get()
        .await
        .unwrap()
        .query_one(
            &format!(
                "SELECT a.xmin::text AS business_xmin,b.xmin::text AS binding_xmin,c.xmin::text AS receipt_xmin FROM \"{0}\".r5_assets a JOIN \"{0}\".teaql_blob_binding b ON b.entity_id=a.id AND b.entity_type='Asset' AND b.field_name='document' AND b.scope_realm='fixture' AND b.scope_boundary='tenant-a' JOIN \"{0}\".r5_document_command c ON c.scope='tenant-a' AND c.actor='actor-a' AND c.command_id='service-1' WHERE a.id=42",
                f.schema
            ),
            &[],
        )
        .await
        .unwrap();
    let business_xmin: String = oracle.get("business_xmin");
    assert_eq!(business_xmin, oracle.get::<_, String>("binding_xmin"));
    assert_eq!(business_xmin, oracle.get::<_, String>("receipt_xmin"));
    let replay = context.accept_document(submit.clone()).await.unwrap();
    assert_eq!(replay, accepted);
    assert_eq!(
        scalar(
            f.root.as_ref(),
            "SELECT version AS value FROM r5_assets WHERE id=42"
        )
        .await,
        2
    );
    let stale_selection = context
        .issue_blob_selection(
            &opened.document_token,
            BlobSelectionClaim {
                target: existing_target(verified.document.snapshot.scope.clone(), "service-2"),
                reference: next.clone(),
            },
            Duration::from_secs(120),
        )
        .unwrap();
    let stale = DocumentAcceptRequest::new(
        "Asset",
        PURPOSE,
        SubmittedContextualDocument {
            business_id: BUSINESS_ID.into(),
            document_token: opened.document_token.clone(),
            command_id: "service-2".into(),
            body: json!({"assetId":42,"selection":stale_selection}),
        },
    )
    .unwrap();
    assert_eq!(
        context.accept_document(stale).await.err().unwrap().code(),
        "DOCUMENT_REVISION_CONFLICT"
    );
    let different = f
        .upload(
            "service_other",
            target.clone(),
            BlobPolicies::default().resolve(&spec).unwrap(),
            0,
        )
        .await;
    let different_selection = context
        .issue_blob_selection(
            &opened.document_token,
            BlobSelectionClaim {
                target: existing_target(verified.document.snapshot.scope, "service-1"),
                reference: different,
            },
            Duration::from_secs(120),
        )
        .unwrap();
    let mut conflict = submit;
    conflict.submitted.body["selection"] = json!(different_selection);
    assert_eq!(
        context
            .accept_document(conflict)
            .await
            .err()
            .unwrap()
            .code(),
        "DOCUMENT_VALIDATION_FAILED"
    );
    assert_eq!(
        f.catalog
            .binding(&f.scope(), &target)
            .await
            .unwrap()
            .unwrap()
            .reference,
        Some(next)
    );
    execute(
        f.root.as_ref(),
        "UPDATE r5_document_permission SET can_edit=0 WHERE subject=$1 AND asset_id=$2",
        vec![Value::Text("actor-a".into()), Value::U64(42)],
    )
    .await;
    assert_eq!(
        context
            .reuse_prepared_view(&current_prepared, &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::AccessDenied,
        "reuse must check the application's current database permission"
    );
    f.finish().await;
}
