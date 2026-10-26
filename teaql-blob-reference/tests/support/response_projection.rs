use super::*;
use futures_util::stream;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, sync::atomic::AtomicBool, time::Duration};
use teaql_blob_store_file::{FileBlobHttpGateway, FileBlobStore, SystemBlobGatewayClock};
use teaql_core::{BlobFieldSpec, BlobRef, DataType, EntityDescriptor, PropertyDescriptor, Value};
use teaql_runtime::{
    AuthorizedResponseGraph, CurrentDocumentViewValidation, DocumentContext,
    DocumentViewProjection, InMemoryMetadataStore, ReferenceDocumentScope,
    TrustedReferencePrincipal, UserContext, ViewAuthorization, ViewDocumentContributor, ViewEntity,
    ViewEntityTarget, ViewErrorCode, ViewField, ViewFieldDecision, ViewIdentity, ViewInput,
    ViewOptions, ViewPath, ViewPreparationService,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{net::TcpListener, sync::oneshot};

async fn get(address: std::net::SocketAddr, url: &str, range: Option<&str>) -> (Vec<u8>, Vec<u8>) {
    let path = url.strip_prefix(&format!("http://{address}")).unwrap();
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let range = range.map_or(String::new(), |value| format!("Range: {value}\r\n"));
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\n{range}Connection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut result = Vec::new();
    stream.read_to_end(&mut result).await.unwrap();
    let split = result
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .unwrap()
        + 4;
    (result[..split].to_vec(), result[split..].to_vec())
}

struct ReadState {
    allowed: AtomicBool,
    show_preview: AtomicBool,
}
struct ReadAuth(Arc<ReadState>);
impl BlobReadAuthorization for ReadAuth {
    fn identity(&self, _: &UserContext) -> BlobResult<BlobUploadIdentity> {
        BlobUploadIdentity::new(BlobScope::new("fixture", "tenant-a")?, "actor-a")
    }
    fn authorize_read(
        &self,
        _: &UserContext,
        _: &BlobFieldTarget,
        _: &BlobRef,
        _: Option<&teaql_runtime::DocumentContext>,
    ) -> BlobResult<bool> {
        Ok(self.0.allowed.load(Ordering::SeqCst))
    }
}
struct ViewAuth(Arc<ReadState>);
impl ViewAuthorization for ViewAuth {
    fn identity(&self, _: &UserContext) -> Result<ViewIdentity, ViewErrorCode> {
        ViewIdentity::new("fixture", "actor-a", "tenant-a")
    }
    fn can_read_entity(
        &self,
        _: &UserContext,
        _: &ViewEntityTarget,
        _: &ViewPath,
    ) -> Result<bool, ViewErrorCode> {
        Ok(self.0.allowed.load(Ordering::SeqCst))
    }
    fn field(
        &self,
        _: &UserContext,
        _: &ViewEntityTarget,
        field: &str,
        _: &ViewPath,
    ) -> Result<ViewFieldDecision, ViewErrorCode> {
        if field == "preview" && !self.0.show_preview.load(Ordering::SeqCst) {
            Ok(ViewFieldDecision::Hidden)
        } else {
            Ok(ViewFieldDecision::value())
        }
    }
}
struct AssetView {
    document: Value,
    preview: Value,
}
struct DocumentIssuer(chrono::DateTime<chrono::Utc>);
struct DocumentValidation;
#[async_trait::async_trait]
impl CurrentDocumentViewValidation for DocumentValidation {
    async fn validate(&self, _: &UserContext, _: &DocumentContext) -> Result<(), ViewErrorCode> {
        Ok(())
    }
}
#[async_trait::async_trait]
impl ViewDocumentContributor for DocumentIssuer {
    async fn prepare(
        &self,
        _: &UserContext,
        graph: &AuthorizedResponseGraph,
        _: &ViewOptions,
    ) -> Result<DocumentViewProjection, ViewErrorCode> {
        Ok(DocumentViewProjection {
            graph: graph.clone(),
            context: DocumentContext {
                principal: TrustedReferencePrincipal::new("oidc", "actor-a", "Tenant", 7).unwrap(),
                scope: ReferenceDocumentScope::new("view-doc", "edit", "Asset", 42, 1).unwrap(),
                expires_at: self.0,
            },
            credentials: serde_json::json!({"fixture":"opaque"}),
            validation: Arc::new(DocumentValidation),
        })
    }
}
impl ViewEntity for AssetView {
    fn target(&self) -> ViewEntityTarget {
        ViewEntityTarget {
            entity: "Asset".into(),
            id: Some(42),
            revision: Some(1),
        }
    }
    fn loaded_fields(&self) -> Vec<ViewField> {
        vec![
            ViewField::new("document", "document"),
            ViewField::new("preview", "preview"),
        ]
    }
    fn read_field(&self, field: &str) -> Result<ViewInput<'_>, ViewErrorCode> {
        match field {
            "document" => Ok(ViewInput::Value(&self.document)),
            "preview" => Ok(ViewInput::Value(&self.preview)),
            _ => Err(ViewErrorCode::ResultUnsupported),
        }
    }
}

#[tokio::test]
async fn actual_postgres_bound_view_uses_file_gateway_and_rechecks_current_binding() {
    let fixture = Fixture::new(true).await;
    let temp = tempfile::tempdir().unwrap();
    let files = FileBlobStore::new(temp.path().join("files"), 2_000_000, 2).unwrap();
    files.initialize().await.unwrap();
    let bytes = fs::read(std::env::var("TEAQL_BLOB_RUST_PDF").unwrap()).unwrap();
    let uploaded = files.create_upload("visible").await.unwrap();
    let stored = files
        .write(
            &uploaded,
            Box::pin(stream::iter([Ok(Bytes::from(bytes.clone()))])),
            2_000_000,
        )
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = format!("http://{address}/blob/read");
    let gateway = Arc::new(
        FileBlobHttpGateway::new(
            files.clone(),
            &endpoint,
            "one",
            BTreeMap::from([("one".into(), vec![7; 32])]),
            Arc::new(SystemBlobGatewayClock),
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let (stop_send, stop_recv) = oneshot::channel();
    let serving = gateway.clone();
    let worker = tokio::spawn(async move {
        serving
            .serve(listener, async {
                let _ = stop_recv.await;
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.validate_ready().await.is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    let mut profile = BlobPolicyProfile::named("default");
    profile.overrides.response_shape = Some(ResponseShape::Descriptor);
    profile.overrides.cache_policy = Some(CachePolicy::PrivateMaxAge(20));
    let policies = Arc::new(
        BlobPolicies::new(
            PolicyLimits::default(),
            profile,
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let document_spec = BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap();
    let preview_spec = BlobFieldSpec::new("Asset", "preview", ["pdf"], true, "default").unwrap();
    let policy = policies.resolve(&document_spec).unwrap();
    let target = fixture.target(42, "document");
    let mut draft = fixture.draft("projected", target.clone(), policy.clone());
    draft.storage_location = stored.location().into();
    let key = draft.reference.clone();
    fixture.catalog.begin_upload(draft, 100).await.unwrap();
    fixture
        .catalog
        .complete_verified(
            &fixture.scope(),
            &key,
            VerifiedContent {
                content_type: "application/pdf".into(),
                size_bytes: bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(&bytes)),
                content_version: stored.content_version().into(),
            },
            100,
        )
        .await
        .unwrap();
    fixture
        .bind(target.clone(), None, Some(key.clone()), policy.clone(), 101)
        .await;
    let state = Arc::new(ReadState {
        allowed: AtomicBool::new(true),
        show_preview: AtomicBool::new(false),
    });
    let contributor = Arc::new(BlobResponseContributor::new(
        fixture.catalog.clone(),
        gateway.clone(),
        policies,
        Arc::new(ReadAuth(state.clone())),
        Arc::new(SystemBlobUploadClock),
    ));
    let descriptor = EntityDescriptor::new("Asset")
        .property(
            PropertyDescriptor::new("document", DataType::BlobRef).blob_field_spec(document_spec),
        )
        .property(
            PropertyDescriptor::new("preview", DataType::BlobRef).blob_field_spec(preview_spec),
        );
    let mut context =
        UserContext::new().with_metadata(InMemoryMetadataStore::new().with_entity(descriptor));
    ViewPreparationService::new(Arc::new(ViewAuth(state.clone())))
        .with_blob_contributor(contributor.clone())
        .install_into(&mut context)
        .unwrap();
    let entity = AssetView {
        document: Value::BlobRef(key.clone()),
        preview: Value::BlobRef(BlobRef::new("blob_hidden_unbound").unwrap()),
    };
    let options = ViewOptions::read_only()
        .with_read_lifetime(Duration::from_secs(30))
        .unwrap()
        .with_cache_lifetime(Duration::from_secs(50));
    let prepared = context
        .prepare_view(ViewInput::Entity(&entity), &options)
        .await
        .unwrap();
    assert!(prepared.value().get("preview").is_none());
    assert_eq!(prepared.value()["document"]["fileName"], "sample.pdf");
    assert_eq!(
        prepared.value()["document"]["contentType"],
        "application/pdf"
    );
    assert_eq!(
        prepared.value()["document"]["sizeBytes"],
        bytes.len() as u64
    );
    let url = prepared.value()["document"]["url"].as_str().unwrap();
    assert!(url.starts_with(&endpoint));
    assert!(!url.contains(key.as_str()));
    let (headers, body) = get(address, url, None).await;
    assert!(headers.starts_with(b"HTTP/1.1 200 OK\r\n"));
    let content_type = b"content-type: application/pdf\r\n";
    assert!(
        headers
            .windows(content_type.len())
            .any(|part| part.eq_ignore_ascii_case(content_type))
    );
    assert_eq!(body, bytes);
    let (headers, body) = get(address, url, Some("bytes=0-15")).await;
    assert!(headers.starts_with(b"HTTP/1.1 206 Partial Content\r\n"));
    assert_eq!(body, bytes[..16]);
    assert_eq!(entity.document, Value::BlobRef(key.clone()));
    context
        .reuse_prepared_view(&prepared, &options)
        .await
        .unwrap();
    assert_eq!(
        context
            .prepare_view(
                ViewInput::Entity(&entity),
                &ViewOptions::read_only().with_context_checked_read()
            )
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::CapabilityMissing,
    );

    let document_deadline = chrono::Utc::now() + chrono::TimeDelta::seconds(12);
    let principal = TrustedReferencePrincipal::new("oidc", "actor-a", "Tenant", 7).unwrap();
    let descriptor = EntityDescriptor::new("Asset")
        .property(
            PropertyDescriptor::new("document", DataType::BlobRef).blob_field_spec(
                BlobFieldSpec::new("Asset", "document", ["pdf"], true, "default").unwrap(),
            ),
        )
        .property(
            PropertyDescriptor::new("preview", DataType::BlobRef).blob_field_spec(
                BlobFieldSpec::new("Asset", "preview", ["pdf"], true, "default").unwrap(),
            ),
        );
    let mut governed = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor))
        .with_trusted_reference_principal(principal);
    ViewPreparationService::new(Arc::new(ViewAuth(state.clone())))
        .with_document_contributor(Arc::new(DocumentIssuer(document_deadline)))
        .with_blob_contributor(contributor)
        .install_into(&mut governed)
        .unwrap();
    let governed_view = governed
        .prepare_view(
            ViewInput::Entity(&entity),
            &ViewOptions::governed()
                .with_read_lifetime(Duration::from_secs(30))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(governed_view.expires_at().unwrap() <= document_deadline);
    assert!(
        governed_view.value()["document"]["url"]
            .as_str()
            .unwrap()
            .starts_with(&endpoint)
    );

    state.allowed.store(false, Ordering::SeqCst);
    assert_eq!(
        context
            .reuse_prepared_view(&prepared, &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::AccessDenied
    );
    state.allowed.store(true, Ordering::SeqCst);
    state.show_preview.store(true, Ordering::SeqCst);
    assert_eq!(
        context
            .prepare_view(ViewInput::Entity(&entity), &ViewOptions::read_only())
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::AccessDenied
    );
    state.show_preview.store(false, Ordering::SeqCst);
    stop_send.send(()).unwrap();
    worker.await.unwrap().unwrap();
    let failed_after_commit = context
        .prepare_view(
            ViewInput::Entity(&entity),
            &ViewOptions::read_only().after_commit(),
        )
        .await
        .unwrap_err();
    assert_eq!(failed_after_commit.code, ViewErrorCode::CapabilityMissing);
    assert_eq!(
        failed_after_commit.business_completion,
        teaql_runtime::BusinessCompletion::Committed,
    );
    fixture.bind(target, Some(key), None, policy, 102).await;
    assert_eq!(
        context
            .reuse_prepared_view(&prepared, &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::AccessDenied
    );
}
