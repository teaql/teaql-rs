//! Passive installation of the current Blob services into a request context.
//! Full `blob.v1` readiness also requires the authorized view contributor.
use std::sync::Arc;

use teaql_runtime::{BlobMutationParticipantResource, UserContext};

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
}
