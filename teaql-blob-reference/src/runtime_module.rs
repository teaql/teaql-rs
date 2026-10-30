//! Passive installation of the current Blob services into a request context.
//! Full `blob.v1` readiness also requires the authorized view contributor.
use std::sync::Arc;

use teaql_core::{BlobFieldSpec, DataType};
use teaql_runtime::{
    BlobCapabilityResource, BlobMutationParticipantResource, RuntimeBlobCapability,
    RuntimeBlobMutationParticipant, UserContext, ViewBlobContributor, ViewPreparationService,
};

use crate::{
    BlobErrorCode, BlobMutationParticipant, BlobResponseContributor, BlobResult, BlobUploadService,
};

/// Shared infrastructure only. Every operation still receives its current
/// `UserContext`; this value never captures a request identity.
pub struct BlobRuntime {
    upload: Arc<BlobUploadService>,
    mutation: Option<Arc<BlobMutationParticipant>>,
    response: Option<Arc<BlobResponseContributor>>,
}

/// Bound to one validated model and one exact set of installed participants.
struct ValidatedBlobCapability {
    runtime: Arc<BlobRuntime>,
    fields: Vec<BlobFieldSpec>,
}

impl RuntimeBlobCapability for ValidatedBlobCapability {
    fn is_ready(&self, context: &UserContext) -> bool {
        let Some(installed) = context.get_resource::<BlobRuntimeResource>() else {
            return false;
        };
        if !Arc::ptr_eq(&installed.0, &self.runtime) {
            return false;
        }
        let (Some(mutation), Some(response)) = (&self.runtime.mutation, &self.runtime.response)
        else {
            return false;
        };
        let expected_mutation: Arc<dyn RuntimeBlobMutationParticipant> = mutation.clone();
        let expected_response: Arc<dyn ViewBlobContributor> = response.clone();
        context
            .get_resource::<BlobMutationParticipantResource>()
            .is_some_and(|installed| Arc::ptr_eq(&installed.0, &expected_mutation))
            && ViewPreparationService::installed_blob_contributor_matches(
                context,
                &expected_response,
            )
            && modeled_fields(context).is_ok_and(|fields| fields == self.fields)
    }
}

fn modeled_fields(context: &UserContext) -> BlobResult<Vec<BlobFieldSpec>> {
    let mut fields = Vec::new();
    for entity in context.all_entities() {
        for property in &entity.properties {
            match (&property.data_type, &property.blob_field_spec) {
                (DataType::BlobRef, Some(spec))
                    if spec.entity() == entity.name
                        && spec.field() == property.name
                        && spec.nullable() == property.nullable =>
                {
                    fields.push(spec.clone());
                }
                (DataType::BlobRef, _) | (_, Some(_)) => {
                    return Err(BlobErrorCode::PolicyInvalid.into());
                }
                _ => {}
            }
        }
    }
    fields
        .sort_by(|left, right| (left.entity(), left.field()).cmp(&(right.entity(), right.field())));
    Ok(fields)
}

impl BlobRuntime {
    pub fn upload(&self) -> &BlobUploadService {
        &self.upload
    }

    pub fn mutation(&self) -> Option<&BlobMutationParticipant> {
        self.mutation.as_deref()
    }

    /// Supply this contributor to the generic `ViewPreparationService` during
    /// explicit host assembly. The Blob module does not replace view policy.
    pub fn response(&self) -> Option<Arc<BlobResponseContributor>> {
        self.response.clone()
    }
}

/// Dedicated resource slot, separate from the business `RuntimeModule`.
pub struct BlobRuntimeResource(pub Arc<BlobRuntime>);

pub struct BlobReferenceModule {
    runtime: Arc<BlobRuntime>,
}

impl BlobReferenceModule {
    pub const CAPABILITY: &'static str = "blob.v1";

    pub fn new(upload: Arc<BlobUploadService>) -> Self {
        Self {
            runtime: Arc::new(BlobRuntime {
                upload,
                mutation: None,
                response: None,
            }),
        }
    }

    pub fn with_mutation(mut self, mutation: Arc<BlobMutationParticipant>) -> Self {
        self.runtime = Arc::new(BlobRuntime {
            upload: self.runtime.upload.clone(),
            mutation: Some(mutation),
            response: self.runtime.response.clone(),
        });
        self
    }

    pub fn with_response(mut self, response: Arc<BlobResponseContributor>) -> Self {
        self.runtime = Arc::new(BlobRuntime {
            upload: self.runtime.upload.clone(),
            mutation: self.runtime.mutation.clone(),
            response: Some(response),
        });
        self
    }

    /// Installs shared services without SQL, store I/O, migrations, or jobs.
    pub fn install_into(&self, context: &mut UserContext) -> BlobResult<()> {
        if context.get_resource::<BlobRuntimeResource>().is_some()
            || context.get_resource::<BlobCapabilityResource>().is_some()
            || context
                .get_resource::<BlobMutationParticipantResource>()
                .is_some()
        {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        if let Some(mutation) = &self.runtime.mutation {
            context.insert_resource(BlobMutationParticipantResource(mutation.clone()));
        }
        context.insert_resource(BlobRuntimeResource(self.runtime.clone()));
        Ok(())
    }

    /// Upload-only readiness. Full `blob.v1` remains unavailable until the
    /// same-transaction binding and authorized view participants are validated.
    pub async fn validate_upload_ready(&self, context: &UserContext) -> BlobResult<()> {
        self.runtime.upload.validate_ready(context).await
    }

    /// Explicit host readiness after business metadata, transaction participant,
    /// authorized view service, schema, store and verifier have been assembled.
    /// This does no migration or write and records a claim only on success.
    pub async fn validate_ready(&self, context: &mut UserContext) -> BlobResult<()> {
        if context.get_resource::<BlobCapabilityResource>().is_some()
            || !context
                .get_resource::<BlobRuntimeResource>()
                .is_some_and(|resource| Arc::ptr_eq(&resource.0, &self.runtime))
        {
            return Err(BlobErrorCode::CapabilityMissing.into());
        }
        let (Some(mutation), Some(response)) = (&self.runtime.mutation, &self.runtime.response)
        else {
            return Err(BlobErrorCode::CapabilityMissing.into());
        };
        let expected_mutation: Arc<dyn RuntimeBlobMutationParticipant> = mutation.clone();
        let expected_response: Arc<dyn ViewBlobContributor> = response.clone();
        if !context
            .get_resource::<BlobMutationParticipantResource>()
            .is_some_and(|resource| Arc::ptr_eq(&resource.0, &expected_mutation))
            || !ViewPreparationService::installed_blob_contributor_matches(
                context,
                &expected_response,
            )
            || mutation.catalog.resource_identity()
                != self.runtime.upload.catalog.resource_identity()
            || response.catalog.resource_identity()
                != self.runtime.upload.catalog.resource_identity()
            || !Arc::ptr_eq(&response.store, &self.runtime.upload.store)
            || !response.store.capabilities().signed_reads
        {
            return Err(BlobErrorCode::CapabilityMissing.into());
        }
        let fields = modeled_fields(context)?;
        for field in &fields {
            let upload_policy = self.runtime.upload.policies.resolve(field)?;
            if mutation.policies.resolve(field)? != upload_policy
                || response.policies.resolve(field)? != upload_policy
            {
                return Err(BlobErrorCode::PolicyInvalid.into());
            }
        }
        self.runtime.upload.validate_ready(context).await?;
        context.insert_resource(BlobCapabilityResource(Arc::new(ValidatedBlobCapability {
            runtime: self.runtime.clone(),
            fields,
        })));
        Ok(())
    }
}
