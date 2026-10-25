use super::graph::freeze;
use super::{
    AuthorizedResponseGraph, BusinessCompletion, CurrentDocumentViewValidation, DocumentContext,
    DocumentViewProjection, ViewAuthorization, ViewErrorCode, ViewInput, ViewPreparationError,
};
use crate::document_round_trip::ContextBoundDocumentServiceResource;
use crate::round_trip_reference::ContextBoundReferenceRuntimeResource;
use crate::{
    ContextBoundDocumentService, ContextualDocument, DocumentAcceptRequest, DocumentOpenRequest,
    RoundTripReferenceProvider, TrustedReferencePrincipal, UserContext, VerifiedDocumentSnapshot,
};
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

/// Only a successful call to this context's registered service can create this source.
/// Public ContextualDocument/client JSON has no equivalent preparation provenance.
pub struct DocumentView {
    document: ContextualDocument,
    validation: Arc<ExistingDocumentValidation>,
    completion: BusinessCompletion,
}
impl DocumentView {
    pub fn completion(&self) -> BusinessCompletion {
        self.completion
    }
    pub(crate) async fn projection(
        &self,
        context: &UserContext,
        authorization: &dyn ViewAuthorization,
    ) -> Result<DocumentViewProjection, ViewErrorCode> {
        let document_context = DocumentContext {
            principal: self.validation.principal.clone(),
            scope: self.validation.verified.snapshot.scope.clone(),
            expires_at: self.validation.verified.expires_at,
        };
        self.validation.validate(context, &document_context).await?;
        let graph: AuthorizedResponseGraph =
            freeze(context, authorization, ViewInput::Json(&self.document.body))?;
        Ok(DocumentViewProjection {
            graph,
            context: document_context,
            credentials: json!({"businessId":self.document.business_id,"documentToken":self.document.document_token}),
            validation: self.validation.clone(),
        })
    }
}
impl std::fmt::Debug for DocumentView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DocumentView([redacted])")
    }
}
pub struct AcceptedDocumentView {
    pub document: DocumentView,
    pub previous_revision: i64,
    pub new_revision: i64,
    pub changed_entities: Vec<crate::ReferenceIdentity>,
}

/// Legacy service failures retain their existing business semantics. They are never
/// relabeled as a known rollback or commit by the presentation adapter.
#[derive(Debug)]
pub enum DocumentViewError {
    Business(crate::DocumentRoundTripError),
    Presentation(ViewPreparationError),
}
impl From<crate::DocumentRoundTripError> for DocumentViewError {
    fn from(value: crate::DocumentRoundTripError) -> Self {
        Self::Business(value)
    }
}
impl From<ViewPreparationError> for DocumentViewError {
    fn from(value: ViewPreparationError) -> Self {
        Self::Presentation(value)
    }
}
impl std::fmt::Display for DocumentViewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Business(error) => error.fmt(f),
            Self::Presentation(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for DocumentViewError {}

struct ExistingDocumentValidation {
    service: Arc<dyn ContextBoundDocumentService>,
    provider: Arc<dyn RoundTripReferenceProvider>,
    principal: TrustedReferencePrincipal,
    verified: VerifiedDocumentSnapshot,
    token: String,
}
#[async_trait]
impl CurrentDocumentViewValidation for ExistingDocumentValidation {
    async fn validate(
        &self,
        context: &UserContext,
        document: &DocumentContext,
    ) -> Result<(), ViewErrorCode> {
        let current_service = context
            .get_resource::<ContextBoundDocumentServiceResource>()
            .ok_or(ViewErrorCode::CapabilityMissing)?;
        let current_provider = context
            .get_resource::<ContextBoundReferenceRuntimeResource>()
            .ok_or(ViewErrorCode::CapabilityMissing)?;
        if !Arc::ptr_eq(&self.service, &current_service.0)
            || !Arc::ptr_eq(&self.provider, &current_provider.0)
            || context
                .trusted_reference_principal()
                .map_err(|_| ViewErrorCode::AccessDenied)?
                != &self.principal
            || document.principal != self.principal
            || document.scope != self.verified.snapshot.scope
            || document.expires_at != self.verified.expires_at
        {
            return Err(ViewErrorCode::AccessDenied);
        }
        let verified = context
            .consume_document_snapshot(
                &self.token,
                &document.scope.aggregate_type,
                &document.scope.purpose,
            )
            .map_err(document_code)?;
        if verified != self.verified {
            return Err(ViewErrorCode::AccessDenied);
        }
        self.service
            .validate_prepared_document(context, &self.principal, &verified)
            .await
            .map_err(document_code)
    }
}

impl UserContext {
    /// Calls the existing application service exactly once, then adopts its outbound tqd1.
    pub async fn open_document_for_view(
        &self,
        request: DocumentOpenRequest,
    ) -> Result<DocumentView, DocumentViewError> {
        let completion = BusinessCompletion::Query;
        self.document_view_ready(completion)?;
        let aggregate = request.aggregate_type.clone();
        let purpose = request.purpose.clone();
        let document = self.open_document(request).await?;
        self.capture_document_view(document, &aggregate, &purpose, completion)
            .await
            .map_err(|code| DocumentViewError::Presentation(failure(code, completion)))
    }
    /// The successful service result is committed. Later presentation errors retain that fact.
    pub async fn accept_document_for_view(
        &self,
        request: DocumentAcceptRequest,
    ) -> Result<AcceptedDocumentView, DocumentViewError> {
        self.document_view_ready(BusinessCompletion::Query)?;
        let aggregate = request.expected_aggregate_type.clone();
        let purpose = request.purpose.clone();
        let accepted = self.accept_document(request).await?;
        let completion = BusinessCompletion::Committed;
        let document = self
            .capture_document_view(accepted.document, &aggregate, &purpose, completion)
            .await
            .map_err(|code| failure(code, completion))?;
        Ok(AcceptedDocumentView {
            document,
            previous_revision: accepted.previous_revision,
            new_revision: accepted.new_revision,
            changed_entities: accepted.changed_entities,
        })
    }
    fn document_view_ready(
        &self,
        completion: BusinessCompletion,
    ) -> Result<(), ViewPreparationError> {
        if self.has_unfinished_view_transaction() {
            return Err(failure(ViewErrorCode::TransactionUnfinished, completion));
        }
        if !self.has_context_bound_document_service()
            || !self.has_round_trip_reference_provider()
            || self.get_resource::<super::ViewServiceResource>().is_none()
        {
            return Err(failure(ViewErrorCode::CapabilityMissing, completion));
        }
        if !self
            .get_resource::<ContextBoundDocumentServiceResource>()
            .is_some_and(|s| s.0.supports_view_preparation())
            || self.reference_mode() != Ok(crate::ReferenceMode::Governed)
        {
            return Err(failure(ViewErrorCode::CapabilityMissing, completion));
        }
        Ok(())
    }
    async fn capture_document_view(
        &self,
        document: ContextualDocument,
        aggregate: &str,
        purpose: &str,
        completion: BusinessCompletion,
    ) -> Result<DocumentView, ViewErrorCode> {
        let verified = self
            .consume_document_snapshot(&document.document_token, aggregate, purpose)
            .map_err(document_code)?;
        if verified.snapshot.business_id != document.business_id {
            return Err(ViewErrorCode::AccessDenied);
        }
        let principal = self
            .trusted_reference_principal()
            .map_err(|_| ViewErrorCode::AccessDenied)?
            .clone();
        let service = self
            .get_resource::<ContextBoundDocumentServiceResource>()
            .ok_or(ViewErrorCode::CapabilityMissing)?
            .0
            .clone();
        service
            .validate_prepared_document(self, &principal, &verified)
            .await
            .map_err(document_code)?;
        let provider = self
            .get_resource::<ContextBoundReferenceRuntimeResource>()
            .ok_or(ViewErrorCode::CapabilityMissing)?
            .0
            .clone();
        Ok(DocumentView {
            validation: Arc::new(ExistingDocumentValidation {
                service,
                provider,
                principal,
                verified,
                token: document.document_token.clone(),
            }),
            document,
            completion,
        })
    }
}
fn failure(code: ViewErrorCode, business_completion: BusinessCompletion) -> ViewPreparationError {
    ViewPreparationError {
        code,
        business_completion,
    }
}
fn document_code(error: crate::DocumentRoundTripError) -> ViewErrorCode {
    match error.code() {
        "ROUND_TRIP_REFERENCE_EXPIRED" => ViewErrorCode::Expired,
        "ROUND_TRIP_REFERENCE_CONFIGURATION_INVALID" => ViewErrorCode::CapabilityMissing,
        "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED" | "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH" => {
            ViewErrorCode::AccessDenied
        }
        _ => ViewErrorCode::PreparationFailed,
    }
}
