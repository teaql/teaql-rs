mod document_round_trip;
mod round_trip_reference;

pub use round_trip_reference::{
    ContextBoundReferenceRuntime, DeploymentProfile, ReferenceAuthorizationPolicy, ReferenceKey,
    ReferenceKeyProvider, StaticReferenceKeyProvider, UNSAFE_EXPOSE_RAW_ENTITY_IDS_ACKNOWLEDGEMENT,
    UNSAFE_EXPOSE_RAW_ENTITY_IDS_ENVIRONMENT,
};

use std::sync::Arc;
use std::time::Duration;
use teaql_runtime::{
    BlobSelectionClaim, BlobSelectionTarget, BoundDocumentSnapshot, ContextBoundDocumentService,
    DocumentRoundTripError, DocumentSnapshot, ExternalEntityReference, ReferenceDocumentScope,
    ReferenceIdentity, ReferenceMode, ReferenceStartupNotice, ResolvedEntityReference,
    RoundTripReferenceError, RoundTripReferenceProvider, TrustedReferencePrincipal, UserContext,
    VerifiedBlobSelection, VerifiedBoundDocumentSnapshot, VerifiedDocumentSnapshot,
};

impl RoundTripReferenceProvider for ContextBoundReferenceRuntime {
    fn mode(&self) -> ReferenceMode {
        self.mode()
    }
    fn startup_notice(&self) -> Option<ReferenceStartupNotice> {
        self.startup_notice()
    }
    fn issue(
        &self,
        principal: &TrustedReferencePrincipal,
        scope: &ReferenceDocumentScope,
        identity: ReferenceIdentity,
        lifetime: Duration,
    ) -> Result<ExternalEntityReference, RoundTripReferenceError> {
        self.issue(principal, scope, identity, lifetime)
    }
    fn consume(
        &self,
        principal: &TrustedReferencePrincipal,
        scope: &ReferenceDocumentScope,
        reference: &ExternalEntityReference,
        expected_entity_type: &str,
    ) -> Result<ResolvedEntityReference, RoundTripReferenceError> {
        self.consume(principal, scope, reference, expected_entity_type)
    }
    fn issue_document_snapshot(
        &self,
        principal: &TrustedReferencePrincipal,
        snapshot: DocumentSnapshot,
        lifetime: Duration,
    ) -> Result<String, DocumentRoundTripError> {
        self.issue_document_snapshot(principal, snapshot, lifetime)
    }
    fn consume_document_snapshot(
        &self,
        principal: &TrustedReferencePrincipal,
        token: &str,
        expected_aggregate_type: &str,
        expected_purpose: &str,
    ) -> Result<VerifiedDocumentSnapshot, DocumentRoundTripError> {
        self.consume_document_snapshot(principal, token, expected_aggregate_type, expected_purpose)
    }
    fn issue_bound_document_snapshot(
        &self,
        principal: &TrustedReferencePrincipal,
        snapshot: BoundDocumentSnapshot,
        lifetime: Duration,
    ) -> Result<String, DocumentRoundTripError> {
        self.issue_bound_document_snapshot(principal, snapshot, lifetime)
    }
    fn consume_bound_document_snapshot(
        &self,
        principal: &TrustedReferencePrincipal,
        token: &str,
        expected_aggregate_type: &str,
        expected_purpose: &str,
    ) -> Result<VerifiedBoundDocumentSnapshot, DocumentRoundTripError> {
        self.consume_bound_document_snapshot(
            principal,
            token,
            expected_aggregate_type,
            expected_purpose,
        )
    }
    fn issue_blob_selection(
        &self,
        principal: &TrustedReferencePrincipal,
        document_token: &str,
        claim: BlobSelectionClaim,
        lifetime: Duration,
    ) -> Result<String, DocumentRoundTripError> {
        self.issue_blob_selection(principal, document_token, claim, lifetime)
    }
    fn consume_blob_selection(
        &self,
        principal: &TrustedReferencePrincipal,
        document_token: &str,
        token: &str,
        expected_target: &BlobSelectionTarget,
    ) -> Result<VerifiedBlobSelection, DocumentRoundTripError> {
        self.consume_blob_selection(principal, document_token, token, expected_target)
    }
}

/// Explicit optional provider/service installation. Business module registries are untouched.
pub struct SecureDocumentReferenceModule {
    provider: Arc<dyn RoundTripReferenceProvider>,
    document_service: Option<Arc<dyn ContextBoundDocumentService>>,
}

impl SecureDocumentReferenceModule {
    pub const CAPABILITY: &'static str = "secure_document.v1";

    pub fn new(provider: Arc<dyn RoundTripReferenceProvider>) -> Self {
        Self {
            provider,
            document_service: None,
        }
    }

    pub fn with_document_service(mut self, service: Arc<dyn ContextBoundDocumentService>) -> Self {
        self.document_service = Some(service);
        self
    }

    /// Passive insertion of shared services; no principal, token, I/O, schema or jobs.
    pub fn install_into(&self, context: &mut UserContext) -> Result<(), RoundTripReferenceError> {
        if context.has_round_trip_reference_provider()
            || (self.document_service.is_some() && context.has_context_bound_document_service())
        {
            return Err(RoundTripReferenceError::configuration());
        }
        context.set_round_trip_reference_runtime(self.provider.clone());
        if let Some(service) = &self.document_service {
            context.set_context_bound_document_service(service.clone());
        }
        Ok(())
    }

    /// Host endpoint readiness: document operations also need their application policy/service.
    pub fn validate_endpoint(
        context: &UserContext,
        needs_document_service: bool,
    ) -> Result<(), DocumentRoundTripError> {
        if !context.has_round_trip_reference_provider()
            || (needs_document_service && !context.has_context_bound_document_service())
        {
            return Err(DocumentRoundTripError::configuration());
        }
        Ok(())
    }
}
