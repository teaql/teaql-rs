//! Real current UserContext + PostgreSQL + immutable FileStore + pinned parser ELF.
use super::*;
use futures_util::stream;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU8},
    },
    time::Duration,
};
use teaql_blob_content_native::{
    ProcessBlobContentVerifier, VerificationLimits, WorkerExecutable, WorkerResourceLimits,
};
use teaql_blob_store_file::FileBlobStore;
use teaql_core::{BlobFieldSpec, DataType, EntityDescriptor, PropertyDescriptor};
use teaql_runtime::{InMemoryMetadataStore, RuntimeModule, UserContext};
use tempfile::TempDir;

struct State {
    identity: Mutex<(String, String)>,
    field: AtomicBool,
    access: AtomicBool,
    verified_access: AtomicBool,
    now: AtomicI64,
    hook: AtomicU8,
    store_calls: AtomicUsize,
    verified_calls: AtomicUsize,
}
struct Principal(Arc<State>);
struct Auth;
impl BlobUploadAuthorization for Auth {
    fn identity(&self, ctx: &UserContext) -> BlobResult<BlobUploadIdentity> {
        let state = ctx
            .get_resource::<Principal>()
            .ok_or(BlobErrorCode::BindDenied)?;
        let identity = state.0.identity.lock().unwrap();
        BlobUploadIdentity::new(
            BlobScope::new("service", identity.0.clone())?,
            identity.1.clone(),
        )
    }
    fn can_write_field(&self, ctx: &UserContext, _: &BlobFieldTarget) -> BlobResult<bool> {
        Ok(ctx
            .get_resource::<Principal>()
            .ok_or(BlobErrorCode::BindDenied)?
            .0
            .field
            .load(Ordering::SeqCst))
    }
    fn authorize_upload(
        &self,
        ctx: &UserContext,
        _: &BlobFieldTarget,
        action: BlobUploadAction,
        info: &BlobUploadFileInfo,
    ) -> BlobResult<bool> {
        let state = &ctx
            .get_resource::<Principal>()
            .ok_or(BlobErrorCode::BindDenied)?
            .0;
        if action == BlobUploadAction::Confirm && info.declared_content_type.is_some() {
            state.verified_calls.fetch_add(1, Ordering::SeqCst);
            if !state.verified_access.load(Ordering::SeqCst) {
                return Ok(false);
            }
        }
        Ok(state.access.load(Ordering::SeqCst))
    }
}
impl BlobUploadClock for State {
    fn now_millis(&self) -> BlobResult<i64> {
        Ok(self.now.load(Ordering::SeqCst))
    }
}
struct Store {
    inner: FileBlobStore,
    state: Arc<State>,
}
impl Store {
    fn call(&self) {
        self.state.store_calls.fetch_add(1, Ordering::SeqCst);
    }
    fn hook(&self, stage: u8) {
        let mode = self.state.hook.load(Ordering::SeqCst);
        if stage == 1 && mode == 6 {
            self.state.identity.lock().unwrap().0 = "tenant-b".into();
        }
        if stage == 2 {
            match mode {
                1 => self.state.field.store(false, Ordering::SeqCst),
                2 => self.state.access.store(false, Ordering::SeqCst),
                3 => self.state.identity.lock().unwrap().0 = "tenant-b".into(),
                4 => self.state.identity.lock().unwrap().1 = "actor-b".into(),
                5 => self.state.now.store(999999999, Ordering::SeqCst),
                _ => (),
            }
        }
    }
}
#[async_trait::async_trait]
impl BlobStore for Store {
    async fn validate_ready(&self) -> BlobResult<()> {
        self.inner.validate_ready().await
    }
    fn capabilities(&self) -> BlobStoreCapabilities {
        let mut value = self.inner.capabilities();
        if self.state.hook.load(Ordering::SeqCst) == 8 {
            value.range = false;
        }
        value
    }
    fn maximum_write_bytes(&self) -> u64 {
        self.inner.maximum_write_bytes()
    }
    async fn create_upload(&self, id: &str) -> BlobResult<BlobUploadTarget> {
        self.call();
        let result = self.inner.create_upload(id).await;
        self.hook(1);
        result
    }
    async fn write(
        &self,
        target: &BlobUploadTarget,
        source: BlobContentStream,
        max: u64,
    ) -> BlobResult<StoredBlob> {
        self.call();
        let result = self.inner.write(target, source, max).await;
        self.hook(2);
        result
    }
    async fn stat(&self, location: &str) -> BlobResult<Option<StoredBlob>> {
        self.call();
        let value = self.inner.stat(location).await?;
        if let Some(value) = value {
            let mode = self.state.hook.load(Ordering::SeqCst);
            return Ok(Some(StoredBlob::new(
                value.location(),
                if mode == 10 {
                    "wrong"
                } else {
                    value.content_version()
                },
                value.size_bytes() + u64::from(mode == 11),
                if mode == 12 {
                    "a".repeat(64)
                } else {
                    value.sha256().into()
                },
            )?));
        }
        Ok(None)
    }
    async fn seal_for_deletion(
        &self,
        location: &str,
        version: Option<&str>,
    ) -> BlobResult<Option<StoredBlob>> {
        self.call();
        self.inner.seal_for_deletion(location, version).await
    }
    async fn open_range(
        &self,
        location: &str,
        version: &str,
        offset: u64,
        length: u64,
    ) -> BlobResult<BlobRangeContent> {
        self.call();
        let mut result = self
            .inner
            .open_range(location, version, offset, length)
            .await?;
        if self.state.hook.load(Ordering::SeqCst) == 7 {
            result.offset = 1;
        }
        Ok(result)
    }
    async fn delete_if_version(
        &self,
        location: &str,
        version: &str,
    ) -> BlobResult<BlobDeleteResult> {
        self.call();
        self.inner.delete_if_version(location, version).await
    }
}
fn model(extensions: &[&str]) -> InMemoryMetadataStore {
    InMemoryMetadataStore::new().with_entity(
        EntityDescriptor::new("Asset").property(
            PropertyDescriptor::new("document", DataType::BlobRef).blob_field_spec(
                BlobFieldSpec::new(
                    "Asset",
                    "document",
                    extensions.iter().copied(),
                    true,
                    "default",
                )
                .unwrap(),
            ),
        ),
    )
}
fn policies(maximum: u64, session: u64) -> BlobPolicies {
    let mut profile = BlobPolicyProfile::named("default");
    profile.overrides.max_size_bytes = Some(maximum);
    profile.overrides.minimum_seconds = Some(0);
    profile.overrides.unbound_seconds = Some(1);
    profile.overrides.unreferenced_seconds = Some(1);
    profile.overrides.upload_session_seconds = Some(session);
    BlobPolicies::new(
        PolicyLimits::default(),
        profile,
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .unwrap()
}
fn content(bytes: Vec<u8>) -> BlobContentStream {
    Box::pin(stream::iter(
        bytes
            .chunks(37)
            .map(|p| Ok(Bytes::copy_from_slice(p)))
            .collect::<Vec<_>>(),
    ))
}
fn files() -> PathBuf {
    PathBuf::from(
        std::env::var_os("TEAQL_BLOB_RUST_FILES").expect("explicit actual fixture directory"),
    )
}
fn bytes(extension: &str) -> Vec<u8> {
    fs::read(files().join(format!("files/sample.{extension}"))).unwrap()
}
fn info(name: &str) -> BlobUploadFileInfo {
    BlobUploadFileInfo {
        file_name: name.into(),
        declared_size: None,
        declared_content_type: None,
    }
}
struct App {
    f: Fixture,
    _temp: TempDir,
    store: Arc<Store>,
    verifier: ProcessBlobContentVerifier,
    state: Arc<State>,
    service: BlobUploadService,
    ctx: UserContext,
}
impl App {
    async fn new(maximum: u64, session: u64) -> Self {
        let f = Fixture::new(true).await;
        let temp = tempfile::Builder::new()
            .prefix("teaql-r5-service-")
            .tempdir()
            .unwrap();
        let worker = PathBuf::from(
            std::env::var_os("TEAQL_BLOB_RUST_WORKER")
                .expect("separately built explicit parser worker"),
        );
        let installed = temp.path().join("parser-worker");
        fs::copy(worker, &installed).unwrap();
        fs::set_permissions(&installed, fs::Permissions::from_mode(0o500)).unwrap();
        let pin = format!("{:x}", Sha256::digest(fs::read(&installed).unwrap()));
        let verifier = ProcessBlobContentVerifier::new(
            temp.path().join("parser"),
            VerificationLimits::default(),
            WorkerExecutable::new(installed, pin).unwrap(),
            WorkerResourceLimits::default(),
        )
        .unwrap();
        verifier.initialize().await.unwrap();
        let state = Arc::new(State {
            identity: Mutex::new(("tenant-a".into(), "actor-a".into())),
            field: AtomicBool::new(true),
            access: AtomicBool::new(true),
            verified_access: AtomicBool::new(true),
            now: AtomicI64::new(1000),
            hook: AtomicU8::new(0),
            store_calls: AtomicUsize::new(0),
            verified_calls: AtomicUsize::new(0),
        });
        let inner = FileBlobStore::new(temp.path().join("store"), 104857600, 2).unwrap();
        inner.initialize().await.unwrap();
        let store = Arc::new(Store {
            inner,
            state: state.clone(),
        });
        let mut ctx = UserContext::new()
            .with_metadata(model(&FORMATS))
            .with_user_identifier("untrusted-default-is-ignored");
        ctx.insert_resource(Principal(state.clone()));
        let service = BlobUploadService::new(
            f.catalog.clone(),
            store.clone(),
            Arc::new(verifier.clone()),
            policies(maximum, session),
            Arc::new(Auth),
            state.clone(),
        );
        service.validate_ready(&ctx).await.unwrap();
        Self {
            f,
            _temp: temp,
            store,
            verifier,
            state,
            service,
            ctx,
        }
    }
    fn scope(&self) -> BlobScope {
        BlobScope::new("service", "tenant-a").unwrap()
    }
    fn target(&self) -> BlobFieldTarget {
        self.f.target(42, "document")
    }
    async fn begin(&self, key: &str) -> BlobUploadSession {
        self.service
            .begin(&self.ctx, self.target(), info("sample.pdf"), key)
            .await
            .unwrap()
    }
    async fn record(&self, session: &BlobUploadSession) -> BlobCatalogRecord {
        self.f
            .catalog
            .find(&self.scope(), session.reference())
            .await
            .unwrap()
            .unwrap()
    }
    async fn assert_initiated(&self, session: &BlobUploadSession, written: bool) {
        let record = self.record(session).await;
        assert_eq!(record.state(), BlobState::Initiated);
        assert_eq!(record.actual_mime(), None);
        assert_eq!(record.completed_at(), None);
        assert_eq!(record.content_version().is_some(), written);
        let row=self.f.admin.get().await.unwrap().query_one(&format!("SELECT state,actual_mime,completed_at FROM \"{}\".teaql_blob_object WHERE blob_key=$1",self.f.schema),&[&session.reference().as_str()]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "initiated");
        assert_eq!(row.get::<_, Option<String>>(1), None);
        assert_eq!(row.get::<_, Option<i64>>(2), None);
    }
    async fn finish(self) {
        assert!(self.verifier.stats().running.is_empty());
        assert_eq!(self.verifier.stats().started, self.verifier.stats().reaped);
        self.f.finish().await;
    }
}

#[tokio::test]
async fn module_install_is_passive_preserves_context_and_rejects_duplicate() {
    let mut app = App::new(104857600, 60).await;
    let module = BlobReferenceModule::new(Arc::new(app.service.clone()));
    let calls = app.state.store_calls.load(Ordering::SeqCst);
    module.install_into(&mut app.ctx).unwrap();
    assert_eq!(app.state.store_calls.load(Ordering::SeqCst), calls);
    assert!(app.ctx.entity("Asset").is_some());
    assert!(app.ctx.get_resource::<Principal>().is_some());
    let installed = app.ctx.get_resource::<BlobRuntimeResource>().unwrap();
    installed.0.upload().validate_ready(&app.ctx).await.unwrap();
    assert_eq!(
        module.install_into(&mut app.ctx).unwrap_err().code,
        BlobErrorCode::PolicyInvalid
    );
    module.validate_upload_ready(&app.ctx).await.unwrap();

    let requirement = || RuntimeModule::new().require_capability(BlobReferenceModule::CAPABILITY);
    let mut business_first = UserContext::new().with_module(requirement());
    module.install_into(&mut business_first).unwrap();
    let mut infrastructure_first = UserContext::new();
    module.install_into(&mut infrastructure_first).unwrap();
    let infrastructure_first = infrastructure_first.with_module(requirement());
    for context in [&business_first, &infrastructure_first] {
        assert!(context.get_resource::<BlobRuntimeResource>().is_some());
        assert!(matches!(
            context.validate_capabilities(),
            Err(teaql_runtime::RuntimeError::Capability {
                code: "BLOB_CAPABILITY_MISSING",
                ..
            })
        ));
    }
    app.finish().await;
}

#[tokio::test]
async fn service_all_ten_real_formats_upload_verify_bind_unbind_and_collect_exact_versions() {
    let app = App::new(104857600, 60).await;
    for (index, extension) in FORMATS.into_iter().enumerate() {
        let target = app.f.target(42 + index as u64, "document");
        let input = BlobUploadFileInfo {
            file_name: format!("C:\\client\\sample.{}", extension.to_ascii_uppercase()),
            declared_size: Some(bytes(extension).len() as u64),
            declared_content_type: Some("  application/wrong  ".into()),
        };
        let key = format!("format-{extension}");
        let session = app
            .service
            .begin(&app.ctx, target.clone(), input.clone(), &key)
            .await
            .unwrap();
        let replay = app
            .service
            .begin(&app.ctx, target.clone(), input, &key)
            .await
            .unwrap();
        assert_eq!(session.reference(), replay.reference());
        assert_eq!(session.expires_at_millis(), replay.expires_at_millis());
        let data = bytes(extension);
        let receipt = app
            .service
            .write(&app.ctx, session.upload_id(), content(data.clone()))
            .await
            .unwrap();
        assert_eq!(receipt.size_bytes(), data.len() as u64);
        app.assert_initiated(&session, true).await;
        let reference = app
            .service
            .complete(&app.ctx, session.upload_id())
            .await
            .unwrap();
        assert_eq!(&reference, session.reference());
        let started = app.verifier.stats().started;
        assert_eq!(
            app.service
                .complete(&app.ctx, session.upload_id())
                .await
                .unwrap(),
            reference
        );
        assert_eq!(
            app.verifier.stats().started,
            started,
            "idempotent confirmation does not parse again"
        );
        let record = app.record(&session).await;
        assert_eq!(
            record.actual_mime(),
            teaql_core::blob::content_type(extension)
        );
        assert_eq!(
            record.sha256(),
            Some(format!("{:x}", Sha256::digest(&data)).as_str())
        );
        let now = app.state.now.load(Ordering::SeqCst);
        let tx = app.f.root.begin().await.unwrap();
        let prepared = app
            .f
            .catalog
            .prepare_bindings(
                tx.as_ref(),
                &app.scope(),
                vec![change(
                    target.clone(),
                    None,
                    Some(reference.clone()),
                    record.policy().clone(),
                )],
                now,
            )
            .await
            .unwrap();
        app.f.write(tx.as_ref(), &target, Some(&reference)).await;
        app.f
            .catalog
            .apply_bindings(tx.as_ref(), prepared, now)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let row = app
            .f
            .admin
            .get()
            .await
            .unwrap()
            .query_one(
                &format!(
                    "SELECT document FROM \"{}\".r5_assets WHERE id=$1",
                    app.f.schema
                ),
                &[&(target.id().unwrap() as i64)],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), reference.as_str());
        assert!(!serde_json::to_string(&reference).unwrap().contains("http"));
        let tx = app.f.root.begin().await.unwrap();
        let prepared = app
            .f
            .catalog
            .prepare_bindings(
                tx.as_ref(),
                &app.scope(),
                vec![change(
                    target.clone(),
                    Some(reference.clone()),
                    None,
                    record.policy().clone(),
                )],
                now,
            )
            .await
            .unwrap();
        app.f.write(tx.as_ref(), &target, None).await;
        app.f
            .catalog
            .apply_bindings(tx.as_ref(), prepared, now)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let claim = app
            .f
            .catalog
            .claim_deletion(&app.scope(), &reference, now + 1000)
            .await
            .unwrap()
            .unwrap();
        let sealed = app
            .store
            .seal_for_deletion(claim.storage_location(), claim.content_version())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(Some(sealed.content_version()), record.content_version());
        assert_eq!(
            app.store
                .delete_if_version(sealed.location(), sealed.content_version())
                .await
                .unwrap(),
            BlobDeleteResult::Deleted
        );
        app.f
            .catalog
            .acknowledge_deleted(
                &app.scope(),
                &reference,
                Some(sealed.content_version().into()),
                now + 1000,
            )
            .await
            .unwrap();
        assert!(app.store.stat(sealed.location()).await.unwrap().is_none());
    }
    assert_eq!(app.f.count("teaql_blob_object").await, 10);
    assert!(app.state.verified_calls.load(Ordering::SeqCst) >= 10);
    app.finish().await;
}

#[tokio::test]
async fn service_denials_and_missing_principal_model_or_declared_limits_do_no_storage_io() {
    let mut app = App::new(1024, 60).await;
    for which in 0..2 {
        app.state.field.store(which != 0, Ordering::SeqCst);
        app.state.access.store(which != 1, Ordering::SeqCst);
        assert_eq!(
            app.service
                .begin(&app.ctx, app.target(), info("sample.pdf"), "denied")
                .await
                .unwrap_err()
                .code,
            BlobErrorCode::BindDenied
        );
    }
    app.state.field.store(true, Ordering::SeqCst);
    app.state.access.store(true, Ordering::SeqCst);
    let ctx = UserContext::new().with_metadata(model(&FORMATS));
    assert_eq!(
        app.service
            .begin(&ctx, app.target(), info("sample.pdf"), "missing-principal")
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::BindDenied
    );
    app.ctx.set_metadata(model(&["png"]));
    assert_eq!(
        app.service
            .begin(&app.ctx, app.target(), info("sample.pdf"), "model")
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::FormatNotAllowed
    );
    app.ctx.set_metadata(model(&FORMATS));
    let mut input = info("sample.pdf");
    input.declared_size = Some(1025);
    assert_eq!(
        app.service
            .begin(&app.ctx, app.target(), input, "size")
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::SizeExceeded
    );
    for name in ["no-extension", "bad.txt", "bad\n.pdf", ""] {
        assert_eq!(
            app.service
                .begin(&app.ctx, app.target(), info(name), "name")
                .await
                .unwrap_err()
                .code,
            BlobErrorCode::FormatNotAllowed
        );
    }
    assert_eq!(app.state.store_calls.load(Ordering::SeqCst), 0);
    assert_eq!(app.f.count("teaql_blob_object").await, 0);
    app.finish().await;
}

#[tokio::test]
async fn service_canonical_replay_conflicts_and_cross_context_scope_cannot_expose_a_session() {
    let app = App::new(104857600, 60).await;
    let session = app.begin("replay").await;
    let normalized = app
        .service
        .begin(&app.ctx, app.target(), info("dir/sample.pdf"), "replay")
        .await
        .unwrap();
    assert_eq!(normalized.reference(), session.reference());
    let mut changed = info("different.pdf");
    assert_eq!(
        app.service
            .begin(&app.ctx, app.target(), changed.clone(), "replay")
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::IdempotencyConflict
    );
    changed.file_name = "sample.pdf".into();
    changed.declared_size = Some(1);
    assert_eq!(
        app.service
            .begin(&app.ctx, app.target(), changed, "replay")
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::IdempotencyConflict
    );
    // A separate request context, independent principal resource and misleading
    // user_identifier cannot make this shared service reuse its first context.
    let other_state = Arc::new(State {
        identity: Mutex::new(("tenant-b".into(), "actor-b".into())),
        field: AtomicBool::new(true),
        access: AtomicBool::new(true),
        verified_access: AtomicBool::new(true),
        now: AtomicI64::new(1000),
        hook: AtomicU8::new(0),
        store_calls: AtomicUsize::new(0),
        verified_calls: AtomicUsize::new(0),
    });
    let mut other = UserContext::new()
        .with_metadata(model(&FORMATS))
        .with_user_identifier("actor-a");
    other.insert_resource(Principal(other_state.clone()));
    let before = app.state.store_calls.load(Ordering::SeqCst);
    assert_eq!(
        app.service
            .write(&other, session.upload_id(), content(bytes("pdf")))
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::BindDenied
    );
    assert_eq!(
        app.service
            .complete(&other, session.upload_id())
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::BindDenied
    );
    assert_eq!(app.state.store_calls.load(Ordering::SeqCst), before);
    assert_eq!(
        app.state.identity.lock().unwrap().0,
        "tenant-a",
        "the original context must stay unchanged for this oracle"
    );
    app.assert_initiated(&session, false).await;
    // Another actor may use this scope only when its own current host policies
    // approve it. The service pins identity within an operation, never per instance.
    other_state.identity.lock().unwrap().0 = "tenant-a".into();
    app.service
        .write(&other, session.upload_id(), content(bytes("pdf")))
        .await
        .unwrap();
    assert_eq!(
        &app.service
            .complete(&other, session.upload_id())
            .await
            .unwrap(),
        session.reference()
    );
    app.finish().await;
}

#[tokio::test]
async fn service_after_write_field_policy_scope_actor_or_time_change_rolls_back_journal() {
    for mode in 1..=5 {
        let app = App::new(104857600, 60).await;
        let session = app.begin("late-denial").await;
        app.state.hook.store(mode, Ordering::SeqCst);
        let error = app
            .service
            .write(&app.ctx, session.upload_id(), content(bytes("pdf")))
            .await
            .unwrap_err();
        assert_eq!(
            error.code,
            if mode == 5 {
                BlobErrorCode::UploadExpired
            } else {
                BlobErrorCode::BindDenied
            }
        );
        assert_eq!(
            error.completion,
            Some(PhysicalTransactionOutcome::RolledBack)
        );
        app.assert_initiated(&session, false).await;
        let record = app.record(&session).await;
        assert!(
            app.store
                .inner
                .stat(record.storage_location())
                .await
                .unwrap()
                .is_some(),
            "actual immutable bytes may exist but must remain unverified"
        );
        app.finish().await;
    }
}

#[tokio::test]
async fn service_after_candidate_creation_scope_change_creates_no_catalog_record() {
    let app = App::new(104857600, 60).await;
    app.state.hook.store(6, Ordering::SeqCst);
    assert_eq!(
        app.service
            .begin(&app.ctx, app.target(), info("sample.pdf"), "create-denial")
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::BindDenied
    );
    assert_eq!(app.f.count("teaql_blob_object").await, 0);
    app.finish().await;
}

#[tokio::test]
async fn service_actual_verification_or_final_blob_policy_failure_never_promotes_written_content() {
    let app = App::new(104857600, 60).await;
    let session = app.begin("fake-pdf").await;
    app.service
        .write(&app.ctx, session.upload_id(), content(bytes("png")))
        .await
        .unwrap();
    assert_eq!(
        app.service
            .complete(&app.ctx, session.upload_id())
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::FormatNotAllowed
    );
    app.assert_initiated(&session, true).await;
    let session = app.begin("verified-policy").await;
    app.service
        .write(&app.ctx, session.upload_id(), content(bytes("pdf")))
        .await
        .unwrap();
    app.state.verified_access.store(false, Ordering::SeqCst);
    let before = app.verifier.stats().started;
    assert_eq!(
        app.service
            .complete(&app.ctx, session.upload_id())
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::BindDenied
    );
    assert!(app.verifier.stats().started > before);
    app.assert_initiated(&session, true).await;
    app.finish().await;
}

#[tokio::test]
async fn service_unjournaled_bytes_and_inconsistent_provider_range_are_rejected() {
    let app = App::new(104857600, 60).await;
    let session = app.begin("unjournaled").await;
    let record = app.record(&session).await;
    let target = app
        .store
        .inner
        .create_upload(session.upload_id())
        .await
        .unwrap();
    app.store
        .inner
        .write(&target, content(bytes("pdf")), 104857600)
        .await
        .unwrap();
    assert_eq!(
        app.service
            .complete(&app.ctx, session.upload_id())
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::UploadIncomplete
    );
    app.assert_initiated(&session, false).await;
    let before = app.verifier.stats().started;
    app.service
        .write(&app.ctx, session.upload_id(), content(bytes("pdf")))
        .await
        .unwrap();
    app.state.hook.store(7, Ordering::SeqCst);
    assert_eq!(
        app.service
            .complete(&app.ctx, session.upload_id())
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::UploadIncomplete
    );
    assert_eq!(app.verifier.stats().started, before);
    assert_eq!(record.storage_location(), target.location());
    app.assert_initiated(&session, true).await;
    app.finish().await;
}

#[tokio::test]
async fn service_deadline_interrupts_pending_source_and_releases_actual_gc_waiter() {
    let app = App::new(104857600, 1).await;
    let session = app.begin("timeout").await;
    let (send, entered) = tokio::sync::oneshot::channel();
    let source: BlobContentStream = Box::pin(stream::once(async move {
        send.send(()).unwrap();
        std::future::pending::<BlobResult<Bytes>>().await
    }));
    let service = app.service.clone();
    let mut ctx = UserContext::new().with_metadata(model(&FORMATS));
    ctx.insert_resource(Principal(app.state.clone()));
    let id = session.upload_id().to_owned();
    let work = tokio::spawn(async move { service.write(&ctx, &id, source).await });
    entered.await.unwrap();
    let catalog = app.f.catalog.clone();
    let scope = app.scope();
    let key = session.reference().clone();
    let expiry = session.expires_at_millis();
    let gc = tokio::spawn(async move { catalog.claim_deletion(&scope, &key, expiry).await });
    app.f.wait_for_lock().await;
    let error = tokio::time::timeout(Duration::from_secs(10), work)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, BlobErrorCode::UploadExpired);
    assert_eq!(
        error.completion,
        Some(PhysicalTransactionOutcome::RolledBack)
    );
    assert_eq!(
        gc.await.unwrap().unwrap().unwrap().state(),
        BlobState::Deleting
    );
    app.finish().await;
}

#[tokio::test]
async fn service_caller_cancellation_drops_storage_stream_and_owned_sql_backend() {
    let app = App::new(104857600, 60).await;
    let session = app.begin("cancel").await;
    let (send, entered) = tokio::sync::oneshot::channel();
    let source: BlobContentStream = Box::pin(stream::once(async move {
        send.send(()).unwrap();
        std::future::pending::<BlobResult<Bytes>>().await
    }));
    let service = app.service.clone();
    let mut ctx = UserContext::new().with_metadata(model(&FORMATS));
    ctx.insert_resource(Principal(app.state.clone()));
    let id = session.upload_id().to_owned();
    let work = tokio::spawn(async move { service.write(&ctx, &id, source).await });
    entered.await.unwrap();
    let row=app.f.admin.get().await.unwrap().query_one("SELECT pid::bigint FROM pg_stat_activity WHERE application_name=$1 AND state='idle in transaction'",&[&app.f.schema]).await.unwrap();
    let pid = row.get(0);
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    app.f.wait_backend_gone(pid).await;
    app.assert_initiated(&session, false).await;
    assert_eq!(
        fs::read_dir(app._temp.path().join("store/staging"))
            .unwrap()
            .count(),
        0
    );
    app.finish().await;
}

#[tokio::test]
async fn service_all_seven_shared_upload_cases_use_real_storage_parser_and_current_model() {
    let cases: serde_json::Value =
        serde_json::from_slice(&fs::read(files().join("upload-cases.json")).unwrap()).unwrap();
    assert_eq!(cases["cases"].as_array().unwrap().len(), 7);
    for case in cases["cases"].as_array().unwrap() {
        let mut app = App::new(case["maxSizeBytes"].as_u64().unwrap(), 60).await;
        let extensions = case["allowedExtensions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>();
        app.ctx.set_metadata(model(&extensions));
        let result = async {
            let input = BlobUploadFileInfo {
                file_name: case["fileName"].as_str().unwrap().into(),
                declared_size: None,
                declared_content_type: Some(case["declaredContentType"].as_str().unwrap().into()),
            };
            let session = app
                .service
                .begin(&app.ctx, app.target(), input, "shared")
                .await?;
            app.service
                .write(
                    &app.ctx,
                    session.upload_id(),
                    content(fs::read(files().join(case["path"].as_str().unwrap())).unwrap()),
                )
                .await?;
            app.service.complete(&app.ctx, session.upload_id()).await
        }
        .await;
        if case.get("expected").is_some() {
            assert!(result.is_ok(), "{}", case["id"]);
        } else {
            assert_eq!(
                result.unwrap_err().code.code(),
                case["error"].as_str().unwrap(),
                "{}",
                case["id"]
            );
        }
        app.finish().await;
    }
}

#[tokio::test]
async fn service_current_model_hard_limit_retention_and_source_errors_are_enforced() {
    let mut app = App::new(104857600, 60).await;
    let session = app.begin("current").await;
    let failed: BlobContentStream = Box::pin(stream::iter(vec![
        Ok(Bytes::from_static(b"partial")),
        Err(BlobErrorCode::StoreFailed.into()),
    ]));
    assert_eq!(
        app.service
            .write(&app.ctx, session.upload_id(), failed)
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::StoreFailed
    );
    app.assert_initiated(&session, false).await;
    assert_eq!(
        app.service
            .write(&app.ctx, session.upload_id(), content(vec![]))
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::SizeExceeded
    );
    app.assert_initiated(&session, false).await;
    // The failed empty immutable write uses a separate session; the main session
    // below must retain its own original generation for successful verification.
    let session = app.begin("current-valid").await;
    app.service
        .write(&app.ctx, session.upload_id(), content(bytes("pdf")))
        .await
        .unwrap();
    let calls = app.state.store_calls.load(Ordering::SeqCst);
    app.ctx.set_metadata(model(&["png"]));
    assert_eq!(
        app.service
            .complete(&app.ctx, session.upload_id())
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::FormatNotAllowed
    );
    assert_eq!(app.state.store_calls.load(Ordering::SeqCst), calls);
    app.ctx.set_metadata(model(&FORMATS));
    let limits = PolicyLimits {
        max_upload_size_bytes: bytes("pdf").len() as u64 - 1,
        ..PolicyLimits::default()
    };
    let mut tightened_profile = BlobPolicyProfile::named("default");
    tightened_profile.overrides.max_size_bytes = Some(limits.max_upload_size_bytes);
    let tight =
        BlobPolicies::new(limits, tightened_profile, BTreeMap::new(), BTreeMap::new()).unwrap();
    let service = BlobUploadService::new(
        app.f.catalog.clone(),
        app.store.clone(),
        Arc::new(app.verifier.clone()),
        tight,
        Arc::new(Auth),
        app.state.clone(),
    );
    assert_eq!(
        service
            .complete(&app.ctx, session.upload_id())
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::SizeExceeded
    );
    app.assert_initiated(&session, true).await;
    app.service
        .complete(&app.ctx, session.upload_id())
        .await
        .unwrap();
    app.state.now.store(2000, Ordering::SeqCst);
    let calls = app.state.store_calls.load(Ordering::SeqCst);
    assert_eq!(
        app.service
            .complete(&app.ctx, session.upload_id())
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::ReferenceExpired
    );
    assert_eq!(app.state.store_calls.load(Ordering::SeqCst), calls);
    app.finish().await;
}

#[tokio::test]
async fn service_provider_mismatches_and_unsupported_direct_mode_fail_explicitly() {
    let app = App::new(104857600, 60).await;
    let session = app.begin("provider").await;
    app.service
        .write(&app.ctx, session.upload_id(), content(bytes("pdf")))
        .await
        .unwrap();
    let count = app.verifier.stats().started;
    for mode in 10..=12 {
        app.state.hook.store(mode, Ordering::SeqCst);
        assert_eq!(
            app.service
                .complete(&app.ctx, session.upload_id())
                .await
                .unwrap_err()
                .code,
            BlobErrorCode::UploadIncomplete
        );
        app.assert_initiated(&session, true).await;
    }
    assert_eq!(app.verifier.stats().started, count);
    app.state.hook.store(8, Ordering::SeqCst);
    assert_eq!(
        app.service.validate_ready(&app.ctx).await.unwrap_err().code,
        BlobErrorCode::CapabilityMissing
    );
    app.state.hook.store(0, Ordering::SeqCst);
    let mut profile = BlobPolicyProfile::named("default");
    profile.overrides.upload_mode = Some(UploadMode::Direct);
    let direct = BlobPolicies::new(
        PolicyLimits::default(),
        profile,
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .unwrap();
    let service = BlobUploadService::new(
        app.f.catalog.clone(),
        app.store.clone(),
        Arc::new(app.verifier.clone()),
        direct,
        Arc::new(Auth),
        app.state.clone(),
    );
    let calls = app.state.store_calls.load(Ordering::SeqCst);
    assert_eq!(
        service.validate_ready(&app.ctx).await.unwrap_err().code,
        BlobErrorCode::CapabilityMissing
    );
    assert_eq!(
        service
            .begin(&app.ctx, app.target(), info("sample.pdf"), "direct")
            .await
            .unwrap_err()
            .code,
        BlobErrorCode::CapabilityMissing
    );
    assert_eq!(app.state.store_calls.load(Ordering::SeqCst), calls);
    app.finish().await;
}

#[tokio::test]
async fn service_new_entity_target_without_id_can_bind_after_real_sql_identity_allocation() {
    let app = App::new(104857600, 60).await;
    let target = BlobFieldTarget::new("Asset", None, "document").unwrap();
    let session = app
        .service
        .begin(&app.ctx, target, info("sample.pdf"), "create")
        .await
        .unwrap();
    app.service
        .write(&app.ctx, session.upload_id(), content(bytes("pdf")))
        .await
        .unwrap();
    let key = app
        .service
        .complete(&app.ctx, session.upload_id())
        .await
        .unwrap();
    let blob = app.record(&session).await;
    // A real owned sequence allocates the final business ID inside this SQL owner.
    execute(
        app.f.root.as_ref(),
        "CREATE SEQUENCE r5_allocated_asset START 100",
        vec![],
    )
    .await;
    let tx = app.f.root.begin().await.unwrap();
    let id = scalar(tx.as_ref(), "SELECT nextval('r5_allocated_asset') AS value").await as u64;
    let target = app.f.target(id, "document");
    let prepared = app
        .f
        .catalog
        .prepare_bindings(
            tx.as_ref(),
            &app.scope(),
            vec![change(
                target.clone(),
                None,
                Some(key.clone()),
                blob.policy().clone(),
            )],
            1000,
        )
        .await
        .unwrap();
    app.f.write(tx.as_ref(), &target, Some(&key)).await;
    app.f
        .catalog
        .apply_bindings(tx.as_ref(), prepared, 1000)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let row=app.f.admin.get().await.unwrap().query_one(&format!("SELECT a.id,a.document,b.entity_id FROM \"{0}\".r5_assets a JOIN \"{0}\".teaql_blob_binding b ON b.entity_id=a.id WHERE a.document=$1",app.f.schema),&[&key.as_str()]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 100);
    assert_eq!(row.get::<_, i64>(2), 100);
    assert_eq!(row.get::<_, String>(1), key.as_str());
    app.finish().await;
}
