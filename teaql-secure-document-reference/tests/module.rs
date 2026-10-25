use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use teaql_runtime::{
    AcceptedContextualDocument, ContextBoundDocumentService, ContextualDocument,
    DocumentAcceptRequest, DocumentOpenRequest, DocumentRoundTripError, ReferenceDocumentScope,
    ReferenceIdentity, RoundTripReferenceError, TrustedReferencePrincipal, UserContext,
};
use teaql_secure_document_reference::{
    ContextBoundReferenceRuntime, DeploymentProfile, ReferenceKey, SecureDocumentReferenceModule,
    StaticReferenceKeyProvider,
};

fn provider(calls: Arc<AtomicUsize>) -> Arc<ContextBoundReferenceRuntime> {
    Arc::new(
        ContextBoundReferenceRuntime::governed(
            DeploymentProfile::Test,
            "module-test",
            "test",
            Arc::new(
                StaticReferenceKeyProvider::new(
                    "fixture",
                    [ReferenceKey::new("fixture", [37; 32]).unwrap()],
                )
                .unwrap(),
            ),
            Arc::new(
                move |_: &TrustedReferencePrincipal,
                      _: &ReferenceDocumentScope,
                      _: &ReferenceIdentity| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            ),
        )
        .unwrap(),
    )
}

#[test]
fn installation_is_passive_preserves_resources_and_rejects_overwrite() {
    #[derive(PartialEq, Debug)]
    struct BusinessResource(u32);
    let calls = Arc::new(AtomicUsize::new(0));
    let module = SecureDocumentReferenceModule::new(provider(calls.clone()));
    let mut context = UserContext::new().with_module(
        teaql_runtime::RuntimeModule::new()
            .descriptor(teaql_core::EntityDescriptor::new("BusinessFixture")),
    );
    assert!(context.entity("BusinessFixture").is_some());
    context.insert_resource(BusinessResource(237));
    context.put_local("business", "preserved");
    module.install_into(&mut context).unwrap();
    assert!(context.entity("BusinessFixture").is_some());
    assert_eq!(
        context.get_resource::<BusinessResource>(),
        Some(&BusinessResource(237))
    );
    assert_eq!(
        context.local("business"),
        Some(&teaql_core::Value::from("preserved"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(module.install_into(&mut context).is_err());
    SecureDocumentReferenceModule::validate_endpoint(&context, false).unwrap();
    assert!(SecureDocumentReferenceModule::validate_endpoint(&context, true).is_err());
}

#[test]
fn shared_module_does_not_share_principals() {
    let module = SecureDocumentReferenceModule::new(provider(Arc::new(AtomicUsize::new(0))));
    let mut alice = UserContext::new().with_trusted_reference_principal(
        TrustedReferencePrincipal::new("oidc", "alice", "Tenant", 1).unwrap(),
    );
    let mut bob = UserContext::new().with_trusted_reference_principal(
        TrustedReferencePrincipal::new("oidc", "bob", "Tenant", 1).unwrap(),
    );
    module.install_into(&mut alice).unwrap();
    module.install_into(&mut bob).unwrap();
    let scope = ReferenceDocumentScope::new("doc", "edit", "Order", 1, 1).unwrap();
    let reference = alice
        .reference_for(
            ReferenceIdentity::new("Order", 1, 1).unwrap(),
            &scope,
            Duration::from_secs(60),
        )
        .unwrap();
    assert_eq!(
        bob.resolve_reference(&reference, "Order", &scope)
            .unwrap_err(),
        RoundTripReferenceError::scope_mismatch()
    );
}

struct UnprotectedService(Arc<AtomicUsize>);
#[async_trait]
impl ContextBoundDocumentService for UnprotectedService {
    async fn open_document(
        &self,
        _: &UserContext,
        _: &TrustedReferencePrincipal,
        _: DocumentOpenRequest,
    ) -> Result<ContextualDocument, DocumentRoundTripError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ContextualDocument {
            business_id: "test".into(),
            document_token: "invalid".into(),
            body: serde_json::Value::Null,
        })
    }
    async fn accept_document(
        &self,
        _: &UserContext,
        _: &TrustedReferencePrincipal,
        _: DocumentAcceptRequest,
    ) -> Result<AcceptedContextualDocument, DocumentRoundTripError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(DocumentRoundTripError::invalid())
    }
}

#[tokio::test]
async fn governed_service_cannot_execute_without_reference_protection() {
    let calls = Arc::new(AtomicUsize::new(0));
    let context = UserContext::new()
        .with_context_bound_document_service(Arc::new(UnprotectedService(calls.clone())))
        .with_trusted_reference_principal(
            TrustedReferencePrincipal::new("oidc", "alice", "Tenant", 1).unwrap(),
        );
    assert!(context.reference_mode().is_err());
    assert!(SecureDocumentReferenceModule::validate_endpoint(&context, true).is_err());
    assert!(
        context
            .open_document(
                DocumentOpenRequest::new("Order", "test", "edit", Duration::from_secs(60)).unwrap()
            )
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
