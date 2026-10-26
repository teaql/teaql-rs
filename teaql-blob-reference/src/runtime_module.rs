//! Passive installation of the current Blob services into a request context.
//! Full `blob.v1` readiness also requires the mutation and view participants.
use std::sync::Arc;

use teaql_runtime::UserContext;

use crate::{BlobErrorCode, BlobResult, BlobUploadService};

/// Shared infrastructure only. Every operation still receives its current
/// `UserContext`; this value never captures a request identity.
pub struct BlobRuntime {
    upload: Arc<BlobUploadService>,
}

impl BlobRuntime {
    pub fn upload(&self) -> &BlobUploadService {
        &self.upload
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
            runtime: Arc::new(BlobRuntime { upload }),
        }
    }

    /// Installs shared services without SQL, store I/O, migrations, or jobs.
    pub fn install_into(&self, context: &mut UserContext) -> BlobResult<()> {
        if context.get_resource::<BlobRuntimeResource>().is_some() {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        context.insert_resource(BlobRuntimeResource(self.runtime.clone()));
        Ok(())
    }

    /// Upload-only readiness. Full `blob.v1` remains unavailable until the
    /// same-transaction binding and authorized view participants are installed.
    pub async fn validate_upload_ready(&self, context: &UserContext) -> BlobResult<()> {
        self.runtime.upload.validate_ready(context).await
    }
}
