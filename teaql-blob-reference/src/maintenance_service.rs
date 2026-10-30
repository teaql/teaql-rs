//! Explicit, host-scheduled Blob collection. Construction starts no worker or I/O.
use crate::{
    BlobDeleteResult, BlobErrorCode as Code, BlobResult, BlobScope, BlobState, BlobStore,
    BlobUploadClock, PostgresBlobCatalog, UploadMode,
};
use std::{fmt, sync::Arc};
use teaql_core::BlobRef;
use teaql_runtime::UserContext;

/// Resolve the current request's trusted scope and maintenance permission.
pub trait BlobMaintenanceAuthorization: Send + Sync {
    fn scope(&self, context: &UserContext) -> BlobResult<BlobScope>;
    fn can_maintain(&self, context: &UserContext, scope: &BlobScope) -> BlobResult<bool>;
}

#[derive(Clone)]
pub struct BlobMaintenanceCursor {
    scope: BlobScope,
    after: BlobRef,
}
impl fmt::Debug for BlobMaintenanceCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobMaintenanceCursor([redacted])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobMaintenanceStatus {
    Deleted,
    AlreadyMissing,
    NotEligible,
    VersionConflict,
    Failed,
}

pub struct BlobMaintenanceOutcome {
    reference: BlobRef,
    status: BlobMaintenanceStatus,
    error: Option<Code>,
}
impl BlobMaintenanceOutcome {
    pub fn reference(&self) -> &BlobRef {
        &self.reference
    }
    pub fn status(&self) -> BlobMaintenanceStatus {
        self.status
    }
    pub fn error(&self) -> Option<Code> {
        self.error
    }
}
impl fmt::Debug for BlobMaintenanceOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobMaintenanceOutcome")
            .field("status", &self.status)
            .field("error", &self.error)
            .finish()
    }
}

#[derive(Debug)]
pub struct BlobMaintenanceBatch {
    pub outcomes: Vec<BlobMaintenanceOutcome>,
    pub next: Option<BlobMaintenanceCursor>,
}

pub struct BlobMaintenanceService {
    catalog: PostgresBlobCatalog,
    store: Arc<dyn BlobStore>,
    authorization: Arc<dyn BlobMaintenanceAuthorization>,
    clock: Arc<dyn BlobUploadClock>,
    batch_size: usize,
}
impl BlobMaintenanceService {
    pub fn new(
        catalog: PostgresBlobCatalog,
        store: Arc<dyn BlobStore>,
        authorization: Arc<dyn BlobMaintenanceAuthorization>,
        clock: Arc<dyn BlobUploadClock>,
        batch_size: usize,
    ) -> BlobResult<Self> {
        if !(1..=1000).contains(&batch_size) {
            return Err(Code::PolicyInvalid.into());
        }
        Ok(Self {
            catalog,
            store,
            authorization,
            clock,
            batch_size,
        })
    }

    fn authorize(
        &self,
        context: &UserContext,
        expected: Option<&BlobScope>,
    ) -> BlobResult<BlobScope> {
        let scope = self
            .authorization
            .scope(context)
            .map_err(|_| Code::BindDenied)?;
        if expected.is_some_and(|expected| expected != &scope)
            || !self
                .authorization
                .can_maintain(context, &scope)
                .map_err(|_| Code::BindDenied)?
        {
            return Err(Code::BindDenied.into());
        }
        Ok(scope)
    }

    /// Run one bounded pass. The caller owns scheduling and worker lifetime.
    pub async fn run_batch(
        &self,
        context: &UserContext,
        cursor: Option<&BlobMaintenanceCursor>,
    ) -> BlobResult<BlobMaintenanceBatch> {
        let scope = self.authorize(context, None)?;
        if cursor.is_some_and(|cursor| cursor.scope != scope) {
            return Err(Code::BindDenied.into());
        }
        if !self.store.capabilities().version_conditional_delete {
            return Err(Code::CapabilityMissing.into());
        }
        self.store.validate_ready().await?;
        let candidates = self
            .catalog
            .deletion_candidates(
                &scope,
                cursor.map(|cursor| &cursor.after),
                self.clock.now_millis()?,
                self.batch_size,
            )
            .await?;
        let next = if candidates.len() == self.batch_size {
            Some(BlobMaintenanceCursor {
                scope: scope.clone(),
                after: candidates
                    .last()
                    .expect("nonempty batch")
                    .reference()
                    .clone(),
            })
        } else {
            None
        };
        let mut outcomes = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let reference = candidate.reference().clone();
            let result = self.collect(context, &scope, &candidate).await;
            match result {
                Ok(outcome) => outcomes.push(outcome),
                Err(error) if error.code == Code::BindDenied => return Err(error),
                Err(error) => outcomes.push(BlobMaintenanceOutcome {
                    reference,
                    status: if error.code == Code::IdempotencyConflict {
                        BlobMaintenanceStatus::VersionConflict
                    } else {
                        BlobMaintenanceStatus::Failed
                    },
                    error: Some(error.code),
                }),
            }
        }
        Ok(BlobMaintenanceBatch { outcomes, next })
    }

    async fn collect(
        &self,
        context: &UserContext,
        scope: &BlobScope,
        candidate: &crate::BlobCatalogRecord,
    ) -> BlobResult<BlobMaintenanceOutcome> {
        self.authorize(context, Some(scope))?;
        if candidate.policy().upload_mode() != UploadMode::ServerStream {
            return Err(Code::CapabilityMissing.into());
        }
        let reference = candidate.reference().clone();
        let Some(mut blob) = self
            .catalog
            .claim_deletion(scope, &reference, self.clock.now_millis()?)
            .await?
        else {
            return Ok(BlobMaintenanceOutcome {
                reference,
                status: BlobMaintenanceStatus::NotEligible,
                error: None,
            });
        };
        self.authorize(context, Some(scope))?;
        let observed = self
            .store
            .seal_for_deletion(blob.storage_location(), blob.content_version())
            .await?;
        self.authorize(context, Some(scope))?;
        if blob.content_version().is_none() {
            match observed {
                None => {
                    self.catalog
                        .acknowledge_deleted(scope, &reference, None, self.clock.now_millis()?)
                        .await?;
                    return Ok(BlobMaintenanceOutcome {
                        reference,
                        status: BlobMaintenanceStatus::AlreadyMissing,
                        error: None,
                    });
                }
                Some(ref content) => {
                    blob = self
                        .catalog
                        .capture_deletion_content(scope, &reference, content)
                        .await?;
                    if blob.state() == BlobState::Deleted {
                        return Ok(BlobMaintenanceOutcome {
                            reference,
                            status: BlobMaintenanceStatus::NotEligible,
                            error: None,
                        });
                    }
                }
            }
        } else if observed.as_ref().is_some_and(|content| {
            content.content_version() != blob.content_version().expect("known version")
        }) {
            return Err(Code::IdempotencyConflict.into());
        }
        if blob.state() != BlobState::Deleting {
            return Err(Code::IdempotencyConflict.into());
        }
        self.authorize(context, Some(scope))?;
        let version = blob.content_version().ok_or(Code::CatalogFailed)?;
        let deleted = self
            .store
            .delete_if_version(blob.storage_location(), version)
            .await?;
        if deleted == BlobDeleteResult::VersionMismatch {
            return Ok(BlobMaintenanceOutcome {
                reference,
                status: BlobMaintenanceStatus::VersionConflict,
                error: Some(Code::IdempotencyConflict),
            });
        }
        self.authorize(context, Some(scope))?;
        self.catalog
            .acknowledge_deleted(
                scope,
                &reference,
                Some(version.to_owned()),
                self.clock.now_millis()?,
            )
            .await?;
        Ok(BlobMaintenanceOutcome {
            reference,
            status: if deleted == BlobDeleteResult::Deleted {
                BlobMaintenanceStatus::Deleted
            } else {
                BlobMaintenanceStatus::AlreadyMissing
            },
            error: None,
        })
    }
}
