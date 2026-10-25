use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Value as Json, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use teaql_core::{BlobRef, Value};
use teaql_runtime::*;

struct Policy {
    revoked: AtomicBool,
    entity_denied: AtomicBool,
    identity: Mutex<ViewIdentity>,
    masks: Mutex<BTreeMap<String, ViewFieldDecision>>,
    calls: AtomicUsize,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            revoked: AtomicBool::new(false),
            entity_denied: AtomicBool::new(false),
            identity: Mutex::new(ViewIdentity::new("oidc", "alice", "Tenant:7").unwrap()),
            masks: Mutex::new(BTreeMap::new()),
            calls: AtomicUsize::new(0),
        }
    }
}
impl ViewAuthorization for Policy {
    fn identity(&self, _: &UserContext) -> Result<ViewIdentity, ViewErrorCode> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.identity.lock().unwrap().clone())
    }
    fn can_read_entity(
        &self,
        _: &UserContext,
        _: &ViewEntityTarget,
        _: &ViewPath,
    ) -> Result<bool, ViewErrorCode> {
        Ok(!self.entity_denied.load(Ordering::SeqCst))
    }
    fn field(
        &self,
        _: &UserContext,
        _: &ViewEntityTarget,
        name: &str,
        _: &ViewPath,
    ) -> Result<ViewFieldDecision, ViewErrorCode> {
        Ok(if name == "hidden" || self.revoked.load(Ordering::SeqCst) {
            ViewFieldDecision::Hidden
        } else {
            self.masks
                .lock()
                .unwrap()
                .get(name)
                .cloned()
                .unwrap_or_else(ViewFieldDecision::value)
        })
    }
}
struct Fixture {
    values: BTreeMap<String, Value>,
    target: ViewEntityTarget,
    reads: Mutex<Vec<String>>,
    cyclic: bool,
    ready: bool,
}
impl Fixture {
    fn json(fields: &Json) -> Self {
        Self {
            values: fields
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), Value::Json(value.clone())))
                .collect(),
            target: ViewEntityTarget {
                entity: "ViewAsset".into(),
                id: fields.get("id").and_then(Json::as_u64),
                revision: fields.get("version").and_then(Json::as_i64),
            },
            reads: Mutex::new(Vec::new()),
            cyclic: false,
            ready: true,
        }
    }
    fn blob() -> Self {
        let mut value = Self::json(&json!({"id":42,"version":1,"title":"visible"}));
        value.values.insert(
            "file".into(),
            Value::BlobRef(BlobRef::new("blob_fixture").unwrap()),
        );
        value
    }
}
impl ViewEntity for Fixture {
    fn target(&self) -> ViewEntityTarget {
        self.target.clone()
    }
    fn loaded_fields(&self) -> Vec<ViewField> {
        let mut fields: Vec<_> = self
            .values
            .keys()
            .map(|name| ViewField::new(name, if name == "file" { "attachment" } else { name }))
            .collect();
        if self.cyclic {
            fields.push(ViewField::new("self", "self"));
        }
        fields
    }
    fn read_field(&self, name: &str) -> Result<ViewInput<'_>, ViewErrorCode> {
        assert_ne!(name, "hidden", "hidden getter must never execute");
        self.reads.lock().unwrap().push(name.to_owned());
        if name == "self" {
            return Ok(ViewInput::Entity(self));
        }
        Ok(ViewInput::Value(
            self.values
                .get(name)
                .ok_or(ViewErrorCode::ResultUnsupported)?,
        ))
    }
    fn presentation_ready(&self) -> bool {
        self.ready
    }
}
fn base() -> (UserContext, Arc<Policy>, ViewPreparationService) {
    let policy = Arc::new(Policy::default());
    let service = ViewPreparationService::new(policy.clone());
    let mut context = UserContext::new();
    service.install_into(&mut context).unwrap();
    (context, policy, service)
}
fn now() -> DateTime<Utc> {
    "2026-10-26T00:00:00Z".parse().unwrap()
}

#[tokio::test]
async fn shared_six_wire_cases_preserve_null_absence_mask_and_revision_zero() {
    let fixture: Json =
        serde_json::from_str(include_str!("fixtures/view-preparation-cases.json")).unwrap();
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 6);
    for case in fixture["cases"].as_array().unwrap() {
        let (context, policy, _) = base();
        if let Some(mask) = case["mask"].as_str() {
            policy
                .masks
                .lock()
                .unwrap()
                .insert(mask.into(), ViewFieldDecision::Mask(Some("***".into())));
        }
        let entity = Fixture::json(&case["fields"]);
        let before = entity.values.clone();
        let prepared = context
            .prepare_view(ViewInput::Entity(&entity), &ViewOptions::read_only())
            .await
            .unwrap();
        assert_eq!(prepared.value(), &case["expected"], "case {}", case["id"]);
        assert_eq!(entity.values, before);
        assert!(prepared.document_context().is_none());
        assert_eq!(prepared.cache_policy_at(now()), ViewCachePolicy::NoStore);
        if case["mask"] == "title" {
            assert!(
                !entity
                    .reads
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|name| name == "title")
            );
        }
    }
}

#[tokio::test]
async fn authorized_copy_never_calls_hidden_or_masked_getters_or_adds_unloaded_fields() {
    let (context, policy, _) = base();
    let mut entity = Fixture::json(&json!({"title":"secret","hidden":"secret","nullable":null}));
    policy
        .masks
        .lock()
        .unwrap()
        .insert("title".into(), ViewFieldDecision::Mask(Some("***".into())));
    let options = ViewOptions::read_only();
    let prepared = context
        .prepare_view(ViewInput::Entity(&entity), &options)
        .await
        .unwrap();
    entity
        .values
        .insert("nullable".into(), Value::Text("later mutation".into()));
    assert_eq!(prepared.value(), &json!({"title":"***","nullable":null}));
    assert_eq!(entity.reads.lock().unwrap().as_slice(), &["nullable"]);
    assert!(
        !prepared
            .value()
            .as_object()
            .unwrap()
            .contains_key("unloaded")
    );
    context
        .reuse_prepared_view(&prepared, &options)
        .await
        .unwrap();
    assert_eq!(entity.reads.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn entity_cycles_only_return_currently_visible_loaded_identity() {
    let (context, _, _) = base();
    let mut entity = Fixture::json(&json!({"id":42,"version":1,"title":"visible"}));
    entity.cyclic = true;
    let prepared = context
        .prepare_view(ViewInput::Entity(&entity), &ViewOptions::read_only())
        .await
        .unwrap();
    assert_eq!(
        prepared.value(),
        &json!({"id":42,"version":1,"title":"visible","self":{"id":42,"version":1}})
    );
}

#[tokio::test]
async fn provenance_options_and_identity_are_checked_on_reuse() {
    let (mut context, policy, service) = base();
    let options = ViewOptions::read_only();
    let prepared = context
        .prepare_view(
            ViewInput::Json(&json!({"prepared":true,"title":"plain"})),
            &options,
        )
        .await
        .unwrap();
    assert!(std::ptr::eq(
        context
            .reuse_prepared_view(&prepared, &options)
            .await
            .unwrap(),
        &prepared
    ));
    assert_eq!(
        context
            .reuse_prepared_view(&prepared, &ViewOptions::read_only())
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::PreparedMismatch
    );
    policy.identity.lock().unwrap().subject = "bob".into();
    assert_eq!(
        context
            .reuse_prepared_view(&prepared, &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::AccessDenied
    );
    assert!(service.install_into(&mut context).is_err());
    let changed = service.with_clock(Arc::new(now));
    assert_eq!(
        changed
            .prepare(&context, ViewInput::Json(&Json::Null), &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::PreparedMismatch
    );
}

#[tokio::test]
async fn current_entity_field_and_pending_submission_deny_complete_view() {
    let (context, policy, _) = base();
    let mut entity = Fixture::json(&json!({"id":42,"version":1,"title":"visible"}));
    let options = ViewOptions::read_only();
    let prepared = context
        .prepare_view(ViewInput::Entity(&entity), &options)
        .await
        .unwrap();
    policy.revoked.store(true, Ordering::SeqCst);
    assert_eq!(
        context
            .reuse_prepared_view(&prepared, &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::AccessDenied
    );
    policy.revoked.store(false, Ordering::SeqCst);
    policy.entity_denied.store(true, Ordering::SeqCst);
    assert_eq!(
        context
            .prepare_view(ViewInput::Entity(&entity), &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::AccessDenied
    );
    policy.entity_denied.store(false, Ordering::SeqCst);
    entity.ready = false;
    assert_eq!(
        context
            .prepare_view(ViewInput::Entity(&entity), &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::TransactionUnfinished
    );
}

#[tokio::test]
async fn raw_blob_nonfinite_numbers_and_duplicate_members_are_rejected() {
    let (context, _, _) = base();
    let options = ViewOptions::read_only();
    for value in [
        Value::BlobRef(BlobRef::new("blob_raw").unwrap()),
        Value::F64(f64::NAN),
        Value::F64(f64::INFINITY),
        Value::List(vec![Value::BlobRef(BlobRef::new("blob_nested").unwrap())]),
    ] {
        assert_eq!(
            context
                .prepare_view(ViewInput::Value(&value), &options)
                .await
                .unwrap_err()
                .code,
            ViewErrorCode::ResultUnsupported
        );
    }
    assert_eq!(
        context
            .prepare_view(
                ViewInput::Object(vec![
                    ("same", ViewInput::Json(&Json::Null)),
                    ("same", ViewInput::Json(&Json::Null))
                ]),
                &options
            )
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::ResultUnsupported
    );
    assert_eq!(
        context
            .prepare_view(ViewInput::Entity(&Fixture::blob()), &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::CapabilityMissing
    );
}

#[tokio::test]
async fn graph_limits_reject_depth_nodes_and_large_text() {
    let (context, _, _) = base();
    let options = ViewOptions::read_only();
    let mut nested = Json::Null;
    for _ in 0..65 {
        nested = json!([nested]);
    }
    for value in [
        nested,
        Json::Array(vec![Json::Null; 10_000]),
        Json::String("x".repeat(8_388_609)),
    ] {
        assert_eq!(
            context
                .prepare_view(ViewInput::Json(&value), &options)
                .await
                .unwrap_err()
                .code,
            ViewErrorCode::LimitExceeded
        );
    }
}

#[tokio::test]
async fn governed_missing_capability_and_principal_fail_before_contributors() {
    let (context, policy, _) = base();
    assert_eq!(
        context
            .prepare_view(
                ViewInput::Json(&json!({"prepared":true})),
                &ViewOptions::governed()
            )
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::CapabilityMissing
    );
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
    let events = Arc::new(Mutex::new(Vec::new()));
    let doc = Arc::new(DocumentSpy {
        events: events.clone(),
        deny: Arc::new(AtomicBool::new(false)),
    });
    let mut context = UserContext::new();
    ViewPreparationService::new(policy)
        .with_document_contributor(doc)
        .install_into(&mut context)
        .unwrap();
    assert_eq!(
        context
            .prepare_view(ViewInput::Json(&Json::Null), &ViewOptions::governed())
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::AccessDenied
    );
    assert!(events.lock().unwrap().is_empty());
}

struct DocumentSpy {
    events: Arc<Mutex<Vec<&'static str>>>,
    deny: Arc<AtomicBool>,
}
struct DocumentValidation {
    events: Arc<Mutex<Vec<&'static str>>>,
    deny: Arc<AtomicBool>,
}
#[async_trait]
impl CurrentDocumentViewValidation for DocumentValidation {
    async fn validate(&self, _: &UserContext, _: &DocumentContext) -> Result<(), ViewErrorCode> {
        self.events.lock().unwrap().push("doc.validate");
        if self.deny.load(Ordering::SeqCst) {
            Err(ViewErrorCode::AccessDenied)
        } else {
            Ok(())
        }
    }
}
#[async_trait]
impl ViewDocumentContributor for DocumentSpy {
    async fn prepare(
        &self,
        _: &UserContext,
        graph: &AuthorizedResponseGraph,
        _: &ViewOptions,
    ) -> Result<DocumentViewProjection, ViewErrorCode> {
        self.events.lock().unwrap().push("doc.prepare");
        assert!(
            graph
                .replace_scalars([(ViewPath::default().field("unknown"), json!("forged"))])
                .is_err()
        );
        assert!(
            graph
                .replace_scalars([(ViewPath::default().field("attachment"), json!("raw-key"))])
                .is_err()
        );
        assert!(
            graph
                .replace_scalars([(ViewPath::default().field("title"), json!(["expanded"]))])
                .is_err()
        );
        let graph =
            graph.replace_scalars([(ViewPath::default().field("id"), json!("row:root"))])?;
        Ok(DocumentViewProjection {
            graph,
            context: DocumentContext {
                principal: TrustedReferencePrincipal::new("oidc", "alice", "Tenant", 7).unwrap(),
                scope: ReferenceDocumentScope::new("view-doc", "edit", "ViewAsset", 42, 1).unwrap(),
                expires_at: now() + TimeDelta::seconds(60),
            },
            credentials: json!({"fixture":"opaque"}),
            validation: Arc::new(DocumentValidation {
                events: self.events.clone(),
                deny: self.deny.clone(),
            }),
        })
    }
}
struct BlobSpy {
    events: Arc<Mutex<Vec<&'static str>>>,
    fail: bool,
    overlong: bool,
    revoke: Option<Arc<Policy>>,
}
struct BlobValidation(Arc<Mutex<Vec<&'static str>>>);
#[async_trait]
impl CurrentBlobViewValidation for BlobValidation {
    async fn validate(
        &self,
        _: &UserContext,
        _: &ViewOptions,
        _: Option<&DocumentContext>,
    ) -> Result<(), ViewErrorCode> {
        self.0.lock().unwrap().push("blob.validate");
        Ok(())
    }
}
#[async_trait]
impl ViewBlobContributor for BlobSpy {
    async fn project(
        &self,
        _: &UserContext,
        graph: &AuthorizedResponseGraph,
        _: &ViewOptions,
        document: Option<&DocumentContext>,
    ) -> Result<BlobViewProjection, ViewErrorCode> {
        self.events.lock().unwrap().push("blob.project");
        if self.fail {
            return Err(ViewErrorCode::PreparationFailed);
        }
        if let Some(policy) = &self.revoke {
            policy.revoked.store(true, Ordering::SeqCst);
        }
        if let Some(document) = document {
            assert_eq!(document.scope.document_id, "view-doc");
        }
        Ok(BlobViewProjection {
            reads: graph
                .blobs()
                .iter()
                .enumerate()
                .map(|(index, target)| BlobViewRead {
                    reference: target.reference.clone(),
                    url: format!("https://storage.example/read/{index}?sig=fixture"),
                    expires_at: now() + TimeDelta::seconds(if self.overlong { 90 } else { 30 }),
                    file_name: "file.pdf".into(),
                    content_type: "application/pdf".into(),
                    size_bytes: 7,
                    descriptor: true,
                    max_cache_age: Duration::from_secs(15),
                    context_checked: false,
                })
                .collect(),
            validation: Arc::new(BlobValidation(self.events.clone())),
        })
    }
}
fn with_spies(
    policy: Arc<Policy>,
    document: Arc<DocumentSpy>,
    blob: Arc<BlobSpy>,
    clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
) -> UserContext {
    let mut context = UserContext::new().with_trusted_reference_principal(
        TrustedReferencePrincipal::new("oidc", "alice", "Tenant", 7).unwrap(),
    );
    ViewPreparationService::new(policy)
        .with_document_contributor(document)
        .with_blob_contributor(blob)
        .with_clock(clock)
        .install_into(&mut context)
        .unwrap();
    context
}

#[tokio::test]
async fn document_precedes_blob_and_reuse_checks_authority_without_resigning() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let policy = Arc::new(Policy::default());
    let deny = Arc::new(AtomicBool::new(false));
    let context = with_spies(
        policy.clone(),
        Arc::new(DocumentSpy {
            events: events.clone(),
            deny: deny.clone(),
        }),
        Arc::new(BlobSpy {
            events: events.clone(),
            fail: false,
            overlong: false,
            revoke: None,
        }),
        Arc::new(now),
    );
    let options = ViewOptions::governed().with_cache_lifetime(Duration::from_secs(300));
    let entity = Fixture::blob();
    let prepared = context
        .prepare_view(ViewInput::Entity(&entity), &options)
        .await
        .unwrap();
    assert_eq!(
        events.lock().unwrap().as_slice(),
        &[
            "doc.prepare",
            "blob.project",
            "doc.validate",
            "blob.validate"
        ]
    );
    assert_eq!(
        prepared.value()["attachment"]["url"],
        "https://storage.example/read/0?sig=fixture"
    );
    assert_eq!(prepared.value()["id"], "row:root");
    assert_eq!(entity.values["id"], Value::Json(json!(42)));
    assert!(prepared.value()["attachment"].get("key").is_none());
    assert_eq!(
        prepared.cache_policy_at(now() + TimeDelta::seconds(25)),
        ViewCachePolicy::PrivateMaxAge(Duration::from_secs(5))
    );
    context
        .reuse_prepared_view(&prepared, &options)
        .await
        .unwrap();
    let before = events.lock().unwrap().clone();
    policy.revoked.store(true, Ordering::SeqCst);
    assert_eq!(
        context
            .reuse_prepared_view(&prepared, &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::AccessDenied
    );
    assert_eq!(
        *events.lock().unwrap(),
        before,
        "generic policy denial precedes all resource validation"
    );
    policy.revoked.store(false, Ordering::SeqCst);
    deny.store(true, Ordering::SeqCst);
    assert_eq!(
        context
            .reuse_prepared_view(&prepared, &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::AccessDenied
    );
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| **event == "doc.prepare")
            .count(),
        1
    );
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| **event == "blob.project")
            .count(),
        1
    );
}

#[tokio::test]
async fn projection_failure_preserves_commit_and_expiry_context_checks_are_enforced() {
    for (fail, overlong, checked) in [
        (true, false, false),
        (false, true, false),
        (false, false, true),
    ] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let policy = Arc::new(Policy::default());
        let context = with_spies(
            policy,
            Arc::new(DocumentSpy {
                events: events.clone(),
                deny: Arc::new(AtomicBool::new(false)),
            }),
            Arc::new(BlobSpy {
                events,
                fail,
                overlong,
                revoke: None,
            }),
            Arc::new(now),
        );
        let mut options = ViewOptions::governed().after_commit();
        if checked {
            options = options.with_context_checked_read();
        }
        let error = context
            .prepare_view(ViewInput::Entity(&Fixture::blob()), &options)
            .await
            .unwrap_err();
        assert_eq!(error.business_completion, BusinessCompletion::Committed);
        assert_eq!(error.code, ViewErrorCode::PreparationFailed);
        assert!(!format!("{error:?}").contains("sig="));
    }
}

#[tokio::test]
async fn permission_change_during_projection_denies_whole_view_before_resource_validation() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let policy = Arc::new(Policy::default());
    let context = with_spies(
        policy.clone(),
        Arc::new(DocumentSpy {
            events: events.clone(),
            deny: Arc::new(AtomicBool::new(false)),
        }),
        Arc::new(BlobSpy {
            events: events.clone(),
            fail: false,
            overlong: false,
            revoke: Some(policy),
        }),
        Arc::new(now),
    );
    assert_eq!(
        context
            .prepare_view(
                ViewInput::Entity(&Fixture::blob()),
                &ViewOptions::governed()
            )
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::AccessDenied
    );
    assert_eq!(
        events.lock().unwrap().as_slice(),
        &["doc.prepare", "blob.project"]
    );
}

#[tokio::test]
async fn expired_reuse_and_delivery_cache_never_extend_credential_deadline() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let time = Arc::new(Mutex::new(now()));
    let clock_time = time.clone();
    let context = with_spies(
        Arc::new(Policy::default()),
        Arc::new(DocumentSpy {
            events: events.clone(),
            deny: Arc::new(AtomicBool::new(false)),
        }),
        Arc::new(BlobSpy {
            events,
            fail: false,
            overlong: false,
            revoke: None,
        }),
        Arc::new(move || *clock_time.lock().unwrap()),
    );
    let options = ViewOptions::governed().with_cache_lifetime(Duration::from_secs(90));
    let prepared = context
        .prepare_view(ViewInput::Entity(&Fixture::blob()), &options)
        .await
        .unwrap();
    *time.lock().unwrap() = now() + TimeDelta::seconds(30);
    assert_eq!(
        prepared.cache_policy_at(*time.lock().unwrap()),
        ViewCachePolicy::NoStore
    );
    assert_eq!(
        context
            .reuse_prepared_view(&prepared, &options)
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::Expired
    );
}

#[test]
fn invalid_identity_and_zero_or_fractional_read_lifetimes_are_rejected() {
    for text in ["", " ", "alice\n", "alice\u{85}"] {
        assert!(ViewIdentity::new("realm", text, "boundary").is_err());
    }
    assert!(
        ViewOptions::read_only()
            .with_read_lifetime(Duration::ZERO)
            .is_err()
    );
    assert!(
        ViewOptions::read_only()
            .with_read_lifetime(Duration::from_millis(1))
            .is_err()
    );
}

struct Executor(u8);
struct TestTransaction(u8);
impl teaql_data_service::DataServiceExecutor for Executor {
    type Error = std::io::Error;
    fn capabilities(&self) -> teaql_data_service::DataServiceCapabilities {
        teaql_data_service::DataServiceCapabilities {
            transaction: true,
            ..Default::default()
        }
    }
}
impl teaql_data_service::DataServiceExecutor for TestTransaction {
    type Error = std::io::Error;
    fn capabilities(&self) -> teaql_data_service::DataServiceCapabilities {
        teaql_data_service::DataServiceCapabilities::default()
    }
}
impl teaql_data_service::QueryExecutor for TestTransaction {
    async fn query(
        &self,
        _: teaql_data_service::QueryRequest,
    ) -> Result<teaql_data_service::QueryResult, Self::Error> {
        unreachable!("presentation must not query")
    }
}
impl teaql_data_service::MutationExecutor for TestTransaction {
    async fn mutate(
        &self,
        _: teaql_data_service::MutationRequest,
    ) -> Result<teaql_data_service::MutationResult, Self::Error> {
        unreachable!("presentation must not mutate")
    }
}
impl teaql_data_service::TransactionExecutor for Executor {
    type Tx<'a> = TestTransaction;
    async fn begin(&self) -> Result<TestTransaction, Self::Error> {
        if self.0 == 3 {
            std::future::pending().await
        } else {
            Ok(TestTransaction(self.0))
        }
    }
}
impl teaql_data_service::Transaction for TestTransaction {
    type Error = std::io::Error;
    async fn commit(self) -> Result<(), Self::Error> {
        if self.0 == 1 {
            Err(std::io::Error::other("commit acknowledgement lost"))
        } else if self.0 == 2 {
            std::future::pending().await
        } else {
            Ok(())
        }
    }
    async fn rollback(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn typed_outer_transaction_blocks_preparation_until_physical_commit_or_rollback() {
    for rollback in [false, true] {
        let (mut context, _, _) = base();
        context.insert_resource(Executor(0));
        let result = context
            .execute_in_transaction::<Executor, (), _>(|scope| {
                Box::pin(async move {
                    let error = scope
                        .context()
                        .prepare_view(ViewInput::Json(&Json::Null), &ViewOptions::read_only())
                        .await
                        .unwrap_err();
                    assert_eq!(error.code, ViewErrorCode::TransactionUnfinished);
                    if rollback {
                        Err(RuntimeError::Transaction("requested rollback".into()))
                    } else {
                        Ok(())
                    }
                })
            })
            .await;
        assert_eq!(result.is_err(), rollback);
        assert!(!context.has_unfinished_view_transaction());
        context
            .prepare_view(ViewInput::Json(&Json::Null), &ViewOptions::read_only())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn completion_error_or_cancelled_owner_poison_context_instead_of_claiming_rollback() {
    let (mut context, _, _) = base();
    context.insert_resource(Executor(1));
    assert!(
        context
            .execute_in_transaction::<Executor, (), _>(|_| Box::pin(async { Ok(()) }))
            .await
            .is_err()
    );
    assert!(context.has_unfinished_view_transaction());
    assert_eq!(
        context
            .prepare_view(ViewInput::Json(&Json::Null), &ViewOptions::read_only())
            .await
            .unwrap_err()
            .code,
        ViewErrorCode::TransactionUnfinished
    );
    for cancel_commit in [false, true] {
        let (mut context, _, _) = base();
        context.insert_resource(Executor(if cancel_commit { 2 } else { 0 }));
        let mut pending = Box::pin(context.execute_in_transaction::<Executor, (), _>(|_| {
            Box::pin(async move {
                if cancel_commit {
                    Ok(())
                } else {
                    std::future::pending().await
                }
            })
        }));
        assert!(futures_util::poll!(&mut pending).is_pending());
        assert!(context.has_unfinished_view_transaction());
        drop(pending);
        assert!(context.has_unfinished_view_transaction());
    }
    let (mut context, _, _) = base();
    context.insert_resource(Executor(3));
    let mut pending =
        Box::pin(context.execute_in_transaction::<Executor, (), _>(|_| Box::pin(async { Ok(()) })));
    assert!(futures_util::poll!(&mut pending).is_pending());
    drop(pending);
    assert!(
        context.has_unfinished_view_transaction(),
        "cancelled begin is not rollback proof"
    );
}

#[test]
fn readiness_requires_explicit_view_and_governed_services_without_overwriting_business_resources() {
    let policy = Arc::new(Policy::default());
    let mut context = RuntimeModule::new()
        .require_capability("view.v1")
        .into_context();
    context.insert_resource(237_u64);
    assert!(context.validate_capabilities().is_err());
    ViewPreparationService::new(policy)
        .install_into(&mut context)
        .unwrap();
    context.validate_capabilities().unwrap();
    assert_eq!(context.get_resource::<u64>(), Some(&237));
    RuntimeModule::new()
        .require_capability("view.governed.v1")
        .apply_to(&mut context);
    assert!(context.validate_capabilities().is_err());
}
