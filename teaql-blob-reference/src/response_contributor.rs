//! Authorized final-view Blob projection. No Key is serialized or signed before
//! the generic view service has frozen the visible, loaded response graph.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use teaql_core::{BlobRef, DataType, blob::content_type};
use teaql_runtime::{
    AuthorizedResponseGraph, BlobViewProjection, BlobViewRead, CurrentBlobViewValidation,
    DocumentContext, UserContext, ViewBlobContributor, ViewErrorCode, ViewOptions,
};

use crate::{
    BlobError, BlobErrorCode, BlobFieldTarget, BlobPolicies, BlobPolicySnapshot, BlobReadRequest,
    BlobState, BlobStore, BlobUploadClock, BlobUploadIdentity, CachePolicy, PostgresBlobCatalog,
    ResponseShape,
};

/// The host must combine its current field permission with Blob-specific read
/// permission. Governed reads also receive the trusted document context.
pub trait BlobReadAuthorization: Send + Sync {
    fn identity(&self, context: &UserContext) -> Result<BlobUploadIdentity, BlobError>;
    fn authorize_read(
        &self,
        context: &UserContext,
        target: &BlobFieldTarget,
        reference: &BlobRef,
        document: Option<&DocumentContext>,
    ) -> Result<bool, BlobError>;
}

#[derive(Clone)]
pub struct BlobResponseContributor {
    pub(crate) catalog: PostgresBlobCatalog,
    pub(crate) store: Arc<dyn BlobStore>,
    pub(crate) policies: Arc<BlobPolicies>,
    authorization: Arc<dyn BlobReadAuthorization>,
    clock: Arc<dyn BlobUploadClock>,
}

#[derive(Clone)]
struct ReadEvidence {
    target: BlobFieldTarget,
    reference: BlobRef,
    binding_version: i64,
    content_version: String,
    policy: BlobPolicySnapshot,
}

struct ReadValidation {
    contributor: BlobResponseContributor,
    identity: BlobUploadIdentity,
    document: Option<DocumentContext>,
    evidence: Vec<ReadEvidence>,
}

fn failed(error: BlobError) -> ViewErrorCode {
    match error.code {
        BlobErrorCode::BindDenied | BlobErrorCode::ReadDenied => ViewErrorCode::AccessDenied,
        BlobErrorCode::CapabilityMissing => ViewErrorCode::CapabilityMissing,
        BlobErrorCode::ReferenceExpired => ViewErrorCode::Expired,
        _ => ViewErrorCode::PreparationFailed,
    }
}

impl BlobResponseContributor {
    pub fn new(
        catalog: PostgresBlobCatalog,
        store: Arc<dyn BlobStore>,
        policies: Arc<BlobPolicies>,
        authorization: Arc<dyn BlobReadAuthorization>,
        clock: Arc<dyn BlobUploadClock>,
    ) -> Self {
        Self {
            catalog,
            store,
            policies,
            authorization,
            clock,
        }
    }

    fn identity(&self, context: &UserContext) -> Result<BlobUploadIdentity, ViewErrorCode> {
        self.authorization
            .identity(context)
            .map_err(|_| ViewErrorCode::AccessDenied)
    }

    fn require_read(
        &self,
        context: &UserContext,
        target: &BlobFieldTarget,
        reference: &BlobRef,
        document: Option<&DocumentContext>,
    ) -> Result<(), ViewErrorCode> {
        if self
            .authorization
            .authorize_read(context, target, reference, document)
            .map_err(|_| ViewErrorCode::AccessDenied)?
        {
            Ok(())
        } else {
            Err(ViewErrorCode::AccessDenied)
        }
    }

    fn policy(
        &self,
        context: &UserContext,
        target: &BlobFieldTarget,
    ) -> Result<BlobPolicySnapshot, ViewErrorCode> {
        let property = context
            .entity(target.entity())
            .and_then(|entity| entity.property_by_name(target.field()))
            .ok_or(ViewErrorCode::AccessDenied)?;
        if property.data_type != DataType::BlobRef {
            return Err(ViewErrorCode::AccessDenied);
        }
        let spec = property
            .blob_field_spec
            .as_ref()
            .ok_or(ViewErrorCode::CapabilityMissing)?;
        if spec.entity() != target.entity()
            || spec.field() != target.field()
            || spec.nullable() != property.nullable
        {
            return Err(ViewErrorCode::CapabilityMissing);
        }
        self.policies.resolve(spec).map_err(failed)
    }

    fn verified_object<'a>(
        evidence: &ReadEvidence,
        binding: &crate::BlobBinding,
        object: &'a crate::BlobCatalogRecord,
    ) -> Result<(&'a str, u64), ViewErrorCode> {
        if binding.reference.as_ref() != Some(&evidence.reference)
            || binding.version != evidence.binding_version
            || object.reference() != &evidence.reference
            || object.state() != BlobState::Available
            || object.content_version() != Some(evidence.content_version.as_str())
            || !evidence.policy.extensions.contains(object.extension())
        {
            return Err(ViewErrorCode::AccessDenied);
        }
        let mime = object
            .actual_mime()
            .ok_or(ViewErrorCode::PreparationFailed)?;
        if content_type(object.extension()) != Some(mime) {
            return Err(ViewErrorCode::PreparationFailed);
        }
        let size = object
            .size_bytes()
            .ok_or(ViewErrorCode::PreparationFailed)?;
        if size > evidence.policy.max_size || (size == 0 && !evidence.policy.allow_empty) {
            return Err(ViewErrorCode::AccessDenied);
        }
        Ok((mime, size))
    }
}

#[async_trait]
impl ViewBlobContributor for BlobResponseContributor {
    async fn project(
        &self,
        context: &UserContext,
        graph: &AuthorizedResponseGraph,
        options: &ViewOptions,
        document: Option<&DocumentContext>,
    ) -> Result<BlobViewProjection, ViewErrorCode> {
        let identity = self.identity(context)?;
        let capabilities = self.store.capabilities();
        if !capabilities.signed_reads {
            return Err(ViewErrorCode::CapabilityMissing);
        }
        if options.context_checked_read() && !capabilities.context_checked_reads {
            return Err(ViewErrorCode::CapabilityMissing);
        }
        let now = self.clock.now_millis().map_err(failed)?;
        if now <= 0 {
            return Err(ViewErrorCode::PreparationFailed);
        }
        let document_remaining = document
            .map(|value| {
                // Leave one second for signing and the view service's final checks.
                value
                    .expires_at
                    .timestamp_millis()
                    .checked_sub(now)
                    .map(|remaining| (remaining / 1000).saturating_sub(1))
                    .ok_or(ViewErrorCode::Expired)
            })
            .transpose()?;
        if document_remaining.is_some_and(|seconds| seconds <= 0) {
            return Err(ViewErrorCode::Expired);
        }
        let mut targets = BTreeSet::new();
        let mut prepared = Vec::with_capacity(graph.blobs().len());
        for blob in graph.blobs() {
            let target = BlobFieldTarget::new(&blob.target.entity, blob.target.id, &blob.canonical)
                .map_err(failed)?;
            let policy = self.policy(context, &target)?;
            self.require_read(context, &target, &blob.reference, document)?;
            targets.insert(target.clone());
            prepared.push((target, blob.reference.clone(), policy));
        }
        let bound = self
            .catalog
            .bound_objects(identity.scope(), &targets)
            .await
            .map_err(failed)?;
        let mut requests = Vec::new();
        let mut request_indexes = BTreeMap::new();
        let mut reads = Vec::with_capacity(prepared.len());
        let mut evidence = Vec::with_capacity(prepared.len());
        for (target, reference, policy) in prepared {
            let (binding, object) = bound.get(&target).ok_or(ViewErrorCode::AccessDenied)?;
            let content_version = object
                .content_version()
                .ok_or(ViewErrorCode::PreparationFailed)?
                .to_owned();
            let item = ReadEvidence {
                target: target.clone(),
                reference: reference.clone(),
                binding_version: binding.version,
                content_version: content_version.clone(),
                policy: policy.clone(),
            };
            let (mime, size) = Self::verified_object(&item, binding, object)?;
            let ttl = self
                .policies
                .read_ttl(
                    &policy,
                    options.read_lifetime().map(|value| value.as_secs()),
                    capabilities.maximum_read_ttl.as_secs(),
                    document_remaining.map(|value| value as u64),
                )
                .map_err(failed)?;
            let disposition = self.policies.read_disposition(&policy, mime);
            let cache_seconds = match policy.cache_policy() {
                CachePolicy::NoStore => None,
                CachePolicy::PrivateMaxAge(seconds) => Some(seconds.min(ttl)),
            };
            let key = (
                identity.scope().realm().to_owned(),
                identity.scope().boundary().to_owned(),
                reference.as_str().to_owned(),
                object.storage_location().to_owned(),
                content_version,
                policy.profile().to_owned(),
                policy.version(),
                mime.to_owned(),
                object.original_name().to_owned(),
                disposition as u8,
                ttl,
                cache_seconds,
            );
            let index = if let Some(index) = request_indexes.get(&key) {
                *index
            } else {
                let request = BlobReadRequest::new(
                    object.storage_location(),
                    object
                        .content_version()
                        .ok_or(ViewErrorCode::PreparationFailed)?,
                    mime,
                    object.original_name(),
                    disposition,
                    Duration::from_secs(ttl),
                    cache_seconds.map_or(CachePolicy::NoStore, CachePolicy::PrivateMaxAge),
                )
                .map_err(failed)?;
                let index = requests.len();
                requests.push(request);
                request_indexes.insert(key, index);
                index
            };
            reads.push((
                reference,
                object.original_name().to_owned(),
                mime.to_owned(),
                size,
                policy.response_shape() == ResponseShape::Descriptor,
                cache_seconds,
                ttl,
                index,
            ));
            evidence.push(item);
        }
        if self.identity(context)? != identity {
            return Err(ViewErrorCode::AccessDenied);
        }
        for item in &evidence {
            self.require_read(context, &item.target, &item.reference, document)?;
        }
        let signed = self
            .store
            .sign_read_batch(&requests)
            .await
            .map_err(failed)?;
        if signed.len() != requests.len() {
            return Err(ViewErrorCode::PreparationFailed);
        }
        let after = self.clock.now_millis().map_err(failed)?;
        let mut projected = Vec::with_capacity(reads.len());
        for (
            reference,
            file_name,
            content_type,
            size_bytes,
            descriptor,
            cache_seconds,
            ttl,
            index,
        ) in reads
        {
            let signed = &signed[index];
            let expires = signed.expires_at_millis();
            if expires <= after
                || expires
                    > after
                        .checked_add(
                            (ttl as i64)
                                .checked_mul(1000)
                                .ok_or(ViewErrorCode::PreparationFailed)?,
                        )
                        .ok_or(ViewErrorCode::PreparationFailed)?
                || document.is_some_and(|value| expires > value.expires_at.timestamp_millis())
            {
                return Err(ViewErrorCode::PreparationFailed);
            }
            let expires_at = DateTime::<Utc>::from_timestamp_millis(expires)
                .ok_or(ViewErrorCode::PreparationFailed)?;
            let remaining = ((expires - after) / 1000).max(0) as u64;
            projected.push(BlobViewRead {
                reference,
                url: signed.as_str().to_owned(),
                expires_at,
                file_name,
                content_type,
                size_bytes,
                descriptor,
                max_cache_age: Duration::from_secs(cache_seconds.unwrap_or(0).min(remaining)),
                context_checked: capabilities.context_checked_reads,
            });
        }
        Ok(BlobViewProjection {
            reads: projected,
            validation: Arc::new(ReadValidation {
                contributor: self.clone(),
                identity,
                document: document.cloned(),
                evidence,
            }),
        })
    }
}

#[async_trait]
impl CurrentBlobViewValidation for ReadValidation {
    async fn validate(
        &self,
        context: &UserContext,
        options: &ViewOptions,
        document: Option<&DocumentContext>,
    ) -> Result<(), ViewErrorCode> {
        if self.document.as_ref() != document
            || self.contributor.identity(context)? != self.identity
        {
            return Err(ViewErrorCode::AccessDenied);
        }
        let capabilities = self.contributor.store.capabilities();
        if !capabilities.signed_reads
            || options.context_checked_read() && !capabilities.context_checked_reads
        {
            return Err(ViewErrorCode::CapabilityMissing);
        }
        let mut targets = BTreeSet::new();
        for item in &self.evidence {
            if self.contributor.policy(context, &item.target)? != item.policy {
                return Err(ViewErrorCode::AccessDenied);
            }
            self.contributor
                .require_read(context, &item.target, &item.reference, document)?;
            targets.insert(item.target.clone());
        }
        let bound = self
            .contributor
            .catalog
            .bound_objects(self.identity.scope(), &targets)
            .await
            .map_err(failed)?;
        for item in &self.evidence {
            let (binding, object) = bound.get(&item.target).ok_or(ViewErrorCode::AccessDenied)?;
            BlobResponseContributor::verified_object(item, binding, object)?;
            self.contributor
                .require_read(context, &item.target, &item.reference, document)?;
        }
        if self.contributor.identity(context)? != self.identity {
            return Err(ViewErrorCode::AccessDenied);
        }
        Ok(())
    }
}
