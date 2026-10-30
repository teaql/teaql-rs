use super::graph::{freeze, validate_output};
use super::{
    AuthorizedResponseGraph, BusinessCompletion, PreparedView, ViewAuthorization, ViewCachePolicy,
    ViewErrorCode, ViewIdentity, ViewInput, ViewMode, ViewOptions, ViewPreparationError,
    ViewServiceResource,
};
use crate::{ReferenceDocumentScope, TrustedReferencePrincipal, UserContext};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Value as Json, json};
use std::{sync::Arc, time::Duration};

/// Server-only authority. Renderers must not serialize this internal context.
#[derive(Clone, PartialEq, Eq)]
pub struct DocumentContext {
    pub principal: TrustedReferencePrincipal,
    pub scope: ReferenceDocumentScope,
    pub expires_at: DateTime<Utc>,
}

#[async_trait]
pub trait CurrentDocumentViewValidation: Send + Sync {
    /// Recheck document/purpose authority without issuing tokens or loading business data.
    async fn validate(
        &self,
        context: &UserContext,
        document: &DocumentContext,
    ) -> Result<(), ViewErrorCode>;
}
pub struct DocumentViewProjection {
    pub graph: AuthorizedResponseGraph,
    pub context: DocumentContext,
    pub credentials: Json,
    pub validation: Arc<dyn CurrentDocumentViewValidation>,
}
#[async_trait]
pub trait ViewDocumentContributor: Send + Sync {
    async fn prepare(
        &self,
        context: &UserContext,
        graph: &AuthorizedResponseGraph,
        options: &ViewOptions,
    ) -> Result<DocumentViewProjection, ViewErrorCode>;
}

#[async_trait]
pub trait CurrentBlobViewValidation: Send + Sync {
    /// Immutable resource facts only; current context/policy is supplied on every reuse.
    async fn validate(
        &self,
        context: &UserContext,
        options: &ViewOptions,
        document: Option<&DocumentContext>,
    ) -> Result<(), ViewErrorCode>;
}
pub struct BlobViewRead {
    pub reference: teaql_core::BlobRef,
    pub url: String,
    pub expires_at: DateTime<Utc>,
    pub file_name: String,
    pub content_type: String,
    pub size_bytes: u64,
    pub descriptor: bool,
    pub max_cache_age: Duration,
    pub context_checked: bool,
}
pub struct BlobViewProjection {
    pub reads: Vec<BlobViewRead>,
    pub validation: Arc<dyn CurrentBlobViewValidation>,
}
#[async_trait]
pub trait ViewBlobContributor: Send + Sync {
    async fn project(
        &self,
        context: &UserContext,
        graph: &AuthorizedResponseGraph,
        options: &ViewOptions,
        document: Option<&DocumentContext>,
    ) -> Result<BlobViewProjection, ViewErrorCode>;
}

/// Shared services/configuration only. Installation is passive and never replaces business registries.
#[derive(Clone)]
pub struct ViewPreparationService {
    issuer: Arc<()>,
    authorization: Arc<dyn ViewAuthorization>,
    documents: Option<Arc<dyn ViewDocumentContributor>>,
    blobs: Option<Arc<dyn ViewBlobContributor>>,
    clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}
impl ViewPreparationService {
    pub const CAPABILITY: &'static str = "view.v1";
    pub fn new(authorization: Arc<dyn ViewAuthorization>) -> Self {
        Self {
            issuer: Arc::new(()),
            authorization,
            documents: None,
            blobs: None,
            clock: Arc::new(Utc::now),
        }
    }
    pub fn with_document_contributor(
        mut self,
        contributor: Arc<dyn ViewDocumentContributor>,
    ) -> Self {
        self.issuer = Arc::new(());
        self.documents = Some(contributor);
        self
    }
    pub fn with_blob_contributor(mut self, contributor: Arc<dyn ViewBlobContributor>) -> Self {
        self.issuer = Arc::new(());
        self.blobs = Some(contributor);
        self
    }
    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>) -> Self {
        self.issuer = Arc::new(());
        self.clock = clock;
        self
    }
    pub fn install_into(&self, context: &mut UserContext) -> Result<(), ViewErrorCode> {
        if context.get_resource::<ViewServiceResource>().is_some() {
            return Err(ViewErrorCode::PreparationFailed);
        }
        context.insert_resource(ViewServiceResource(Arc::new(self.clone())));
        Ok(())
    }
    /// Checks the installed view service and the exact authorized Blob contributor.
    pub fn installed_blob_contributor_matches(
        context: &UserContext,
        contributor: &Arc<dyn ViewBlobContributor>,
    ) -> bool {
        context
            .get_resource::<ViewServiceResource>()
            .and_then(|resource| resource.0.blobs.as_ref())
            .is_some_and(|installed| Arc::ptr_eq(installed, contributor))
    }
    pub(crate) fn supports_governed(&self, context: &UserContext) -> bool {
        self.documents.is_some()
            || (context
                .get_resource::<crate::document_round_trip::ContextBoundDocumentServiceResource>()
                .is_some_and(|s| s.0.supports_view_preparation())
                && context.has_round_trip_reference_provider())
    }
    fn boundary(&self, context: &UserContext) -> Result<ViewIdentity, ViewErrorCode> {
        let resource = context
            .get_resource::<ViewServiceResource>()
            .ok_or(ViewErrorCode::CapabilityMissing)?;
        if !Arc::ptr_eq(&self.issuer, &resource.0.issuer) {
            return Err(ViewErrorCode::PreparedMismatch);
        }
        if context.has_unfinished_view_transaction() {
            return Err(ViewErrorCode::TransactionUnfinished);
        }
        let identity = self.authorization.identity(context)?;
        identity.validate()?;
        Ok(identity)
    }
    fn check_identity(
        &self,
        context: &UserContext,
        identity: &ViewIdentity,
    ) -> Result<(), ViewErrorCode> {
        if self.boundary(context)? != *identity {
            Err(ViewErrorCode::AccessDenied)
        } else {
            Ok(())
        }
    }
    fn document_boundary(
        &self,
        context: &UserContext,
        document: &DocumentContext,
    ) -> Result<(), ViewErrorCode> {
        document
            .principal
            .validate()
            .map_err(|_| ViewErrorCode::AccessDenied)?;
        document
            .scope
            .validate()
            .map_err(|_| ViewErrorCode::AccessDenied)?;
        if context
            .trusted_reference_principal()
            .map_err(|_| ViewErrorCode::AccessDenied)?
            != &document.principal
        {
            return Err(ViewErrorCode::AccessDenied);
        }
        if document.expires_at <= (self.clock)() {
            return Err(ViewErrorCode::Expired);
        }
        Ok(())
    }
    pub async fn prepare(
        &self,
        context: &UserContext,
        input: ViewInput<'_>,
        options: &ViewOptions,
    ) -> Result<PreparedView, ViewPreparationError> {
        let completion = match &input {
            ViewInput::Document(source) if source.completion() == BusinessCompletion::Committed => {
                BusinessCompletion::Committed
            }
            _ => options.completion,
        };
        self.prepare_inner(context, input, options)
            .await
            .map_err(|code| ViewPreparationError {
                code,
                business_completion: completion,
            })
    }
    async fn prepare_inner(
        &self,
        context: &UserContext,
        input: ViewInput<'_>,
        options: &ViewOptions,
    ) -> Result<PreparedView, ViewErrorCode> {
        if options.mode == ViewMode::Governed && !self.supports_governed(context) {
            return Err(ViewErrorCode::CapabilityMissing);
        }
        if options.mode == ViewMode::Governed {
            context
                .trusted_reference_principal()
                .map_err(|_| ViewErrorCode::AccessDenied)?
                .validate()
                .map_err(|_| ViewErrorCode::AccessDenied)?;
        }
        let identity = self.boundary(context)?;
        let (graph, document) = match input {
            ViewInput::Document(source) => {
                if options.mode != ViewMode::Governed || source.completion() != options.completion {
                    return Err(ViewErrorCode::PreparedMismatch);
                }
                let projection = source
                    .projection(context, self.authorization.as_ref())
                    .await?;
                (projection.graph.clone(), Some(projection))
            }
            input => {
                let graph = freeze(context, self.authorization.as_ref(), input)?;
                let document = if options.mode == ViewMode::Governed {
                    let projection = self
                        .documents
                        .as_ref()
                        .ok_or(ViewErrorCode::CapabilityMissing)?
                        .prepare(context, &graph, options)
                        .await?;
                    if !graph.same_surface(&projection.graph) {
                        return Err(ViewErrorCode::AccessDenied);
                    }
                    Some(projection)
                } else {
                    None
                };
                (graph, document)
            }
        };
        let projected_graph = document.as_ref().map_or(&graph, |d| &d.graph);
        graph.revalidate(context, self.authorization.as_ref())?;
        self.check_identity(context, &identity)?;
        if let Some(document) = &document {
            self.document_boundary(context, &document.context)?;
        }
        let blobs = if projected_graph.blobs().is_empty() {
            None
        } else {
            Some(
                self.blobs
                    .as_ref()
                    .ok_or(ViewErrorCode::CapabilityMissing)?
                    .project(
                        context,
                        projected_graph,
                        options,
                        document.as_ref().map(|d| &d.context),
                    )
                    .await?,
            )
        };
        let mut expires_at = document.as_ref().map(|d| d.context.expires_at);
        let mut cache = options.cache_lifetime;
        let mut value = projected_graph.value().clone();
        if let Some(projection) = &blobs {
            if projection.reads.len() != projected_graph.blobs().len() {
                return Err(ViewErrorCode::PreparationFailed);
            }
            for (read, target) in projection.reads.iter().zip(projected_graph.blobs()) {
                let url =
                    url::Url::parse(&read.url).map_err(|_| ViewErrorCode::PreparationFailed)?;
                if read.reference != target.reference
                    || !matches!(url.scheme(), "http" | "https")
                    || url.host_str().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.fragment().is_some()
                    || read.expires_at <= (self.clock)()
                    || document
                        .as_ref()
                        .is_some_and(|d| read.expires_at > d.context.expires_at)
                    || (options.context_checked_read && !read.context_checked)
                {
                    return Err(ViewErrorCode::PreparationFailed);
                }
                let rendered = if read.descriptor {
                    json!({"url":read.url,"expiresAt":read.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),"fileName":read.file_name,"contentType":read.content_type,"sizeBytes":read.size_bytes})
                } else {
                    Json::String(read.url.clone())
                };
                *target
                    .path
                    .value_mut(&mut value)
                    .ok_or(ViewErrorCode::PreparationFailed)? = rendered;
                expires_at =
                    Some(expires_at.map_or(read.expires_at, |old| old.min(read.expires_at)));
                cache = cache.min(read.max_cache_age);
            }
        }
        validate_output(&value, document.as_ref().map(|d| &d.credentials))?;
        // Current generic policy always precedes resource authority I/O, and follows it too.
        graph.revalidate(context, self.authorization.as_ref())?;
        self.check_identity(context, &identity)?;
        if let Some(document) = &document {
            self.document_boundary(context, &document.context)?;
            document
                .validation
                .validate(context, &document.context)
                .await?;
        }
        if let Some(blobs) = &blobs {
            blobs
                .validation
                .validate(context, options, document.as_ref().map(|d| &d.context))
                .await?;
        }
        graph.revalidate(context, self.authorization.as_ref())?;
        self.check_identity(context, &identity)?;
        if let Some(document) = &document {
            self.document_boundary(context, &document.context)?;
        }
        let now = (self.clock)();
        if let Some(expiry) = expires_at {
            if expiry <= now {
                return Err(ViewErrorCode::Expired);
            }
            cache = cache.min(
                (expiry - now)
                    .to_std()
                    .map_err(|_| ViewErrorCode::Expired)?,
            );
        }
        let cache_policy = if cache.as_secs() == 0 {
            ViewCachePolicy::NoStore
        } else {
            ViewCachePolicy::PrivateMaxAge(Duration::from_secs(cache.as_secs()))
        };
        Ok(PreparedView {
            issuer: self.issuer.clone(),
            presentation: options.presentation.clone(),
            identity,
            graph,
            value,
            document,
            blobs,
            expires_at,
            cache_policy,
            options: option_values(options),
        })
    }
    /// Presentation-local validation returns the existing output without signing again.
    pub async fn reuse<'a>(
        &self,
        context: &UserContext,
        prepared: &'a PreparedView,
        options: &ViewOptions,
    ) -> Result<&'a PreparedView, ViewPreparationError> {
        let completion = if prepared.business_completion() == BusinessCompletion::Committed {
            BusinessCompletion::Committed
        } else {
            options.completion
        };
        self.reuse_inner(context, prepared, options)
            .await
            .map_err(|code| ViewPreparationError {
                code,
                business_completion: completion,
            })?;
        Ok(prepared)
    }
    async fn reuse_inner(
        &self,
        context: &UserContext,
        prepared: &PreparedView,
        options: &ViewOptions,
    ) -> Result<(), ViewErrorCode> {
        if !Arc::ptr_eq(&self.issuer, &prepared.issuer)
            || !Arc::ptr_eq(&options.presentation, &prepared.presentation)
            || option_values(options) != prepared.options
        {
            return Err(ViewErrorCode::PreparedMismatch);
        }
        self.check_identity(context, &prepared.identity)?;
        if prepared
            .expires_at
            .is_some_and(|expiry| expiry <= (self.clock)())
        {
            return Err(ViewErrorCode::Expired);
        }
        prepared
            .graph
            .revalidate(context, self.authorization.as_ref())?;
        if let Some(document) = &prepared.document {
            self.document_boundary(context, &document.context)?;
            document
                .validation
                .validate(context, &document.context)
                .await?;
        }
        if let Some(blobs) = &prepared.blobs {
            blobs
                .validation
                .validate(
                    context,
                    options,
                    prepared.document.as_ref().map(|d| &d.context),
                )
                .await?;
        }
        prepared
            .graph
            .revalidate(context, self.authorization.as_ref())?;
        self.check_identity(context, &prepared.identity)?;
        if let Some(document) = &prepared.document {
            self.document_boundary(context, &document.context)?;
        }
        if prepared
            .expires_at
            .is_some_and(|expiry| expiry <= (self.clock)())
        {
            return Err(ViewErrorCode::Expired);
        }
        Ok(())
    }
}
fn option_values(
    options: &ViewOptions,
) -> (
    ViewMode,
    BusinessCompletion,
    Duration,
    Option<Duration>,
    bool,
) {
    (
        options.mode,
        options.completion,
        options.cache_lifetime,
        options.read_lifetime,
        options.context_checked_read,
    )
}

impl UserContext {
    pub async fn prepare_view(
        &self,
        input: ViewInput<'_>,
        options: &ViewOptions,
    ) -> Result<PreparedView, ViewPreparationError> {
        let completion = match &input {
            ViewInput::Document(source) if source.completion() == BusinessCompletion::Committed => {
                BusinessCompletion::Committed
            }
            _ => options.completion,
        };
        self.get_resource::<ViewServiceResource>()
            .ok_or(ViewPreparationError {
                code: ViewErrorCode::CapabilityMissing,
                business_completion: completion,
            })?
            .0
            .prepare(self, input, options)
            .await
    }
    pub async fn reuse_prepared_view<'a>(
        &self,
        prepared: &'a PreparedView,
        options: &ViewOptions,
    ) -> Result<&'a PreparedView, ViewPreparationError> {
        let completion = if prepared.business_completion() == BusinessCompletion::Committed {
            BusinessCompletion::Committed
        } else {
            options.completion
        };
        self.get_resource::<ViewServiceResource>()
            .ok_or(ViewPreparationError {
                code: ViewErrorCode::CapabilityMissing,
                business_completion: completion,
            })?
            .0
            .reuse(self, prepared, options)
            .await
    }
}
