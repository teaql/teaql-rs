//! Linear ownership keeps a real SQL lock alive across async storage/auth work.
use super::*;
use crate::StoredBlob;
use teaql_data_service::infrastructure_sql::InfrastructureTransaction;

/// Trusted upload-orchestrator handle. No SQL, credentials or mutable owner escape.
/// Every asynchronous mutation consumes the lease. Failure/cancellation cannot
/// leave a caller holding a handle it can catch and commit after a partial update.
pub struct BlobUploadLease {
    catalog: PostgresBlobCatalog,
    transaction: Box<dyn InfrastructureTransaction>,
    identity: PhysicalTransactionIdentity,
    blob: BlobCatalogRecord,
}
impl fmt::Debug for BlobUploadLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobUploadLease([redacted])")
    }
}
impl BlobUploadLease {
    pub(super) async fn acquire(
        catalog: PostgresBlobCatalog,
        scope: &BlobScope,
        reference: &BlobRef,
    ) -> BlobResult<Self> {
        let transaction = catalog.root.begin().await?;
        let identity = catalog.require_active(transaction.as_ref())?;
        let result = async {
            catalog
                .validate_upload_support_on(transaction.as_ref())
                .await?;
            // PostgreSQL 14+ otherwise defaults to noticing a closed client only
            // after a blocked query completes. Keep lock-waiting cancelled work
            // interruptible too, without changing any session/global setting or
            // imposing an unrelated statement timeout on a live caller.
            fetch(
                transaction.as_ref(),
                "SELECT set_config('client_connection_check_interval','100ms',true) AS value",
                vec![],
            )
            .await?;
            catalog.validate_schema_on(transaction.as_ref()).await?;
            catalog
                .lock_blob(transaction.as_ref(), scope, reference)
                .await
        }
        .await;
        match result {
            Ok(blob) => Ok(Self {
                catalog,
                transaction,
                identity,
                blob,
            }),
            Err(error) => Err(rollback_error(transaction, identity, error).await),
        }
    }
    pub fn record(&self) -> &BlobCatalogRecord {
        &self.blob
    }

    /// Journal immutable storage facts only. MIME/completed_at/state stay unverified.
    /// Hold this lease before the actual storage write and reauthorize afterwards.
    pub async fn record_write(mut self, content: &StoredBlob, now: i64) -> BlobResult<Self> {
        match self.write_inner(content, now).await {
            Ok(blob) => {
                self.blob = blob;
                Ok(self)
            }
            Err(error) => Err(self.abort(error).await),
        }
    }
    async fn write_inner(&self, content: &StoredBlob, now: i64) -> BlobResult<BlobCatalogRecord> {
        self.catalog.require_active(self.transaction.as_ref())?;
        let blob = &self.blob;
        if matches!(blob.state, BlobState::Deleting | BlobState::Deleted)
            || now >= blob.upload_expires_at
        {
            return Err(Code::UploadExpired.into());
        }
        if content.location() != blob.storage_location {
            return Err(Code::ReferenceInvalid.into());
        }
        name(content.content_version(), 1024)?;
        digest(content.sha256())?;
        if content.size_bytes() > blob.policy.max_size
            || content.size_bytes() == 0 && !blob.policy.allow_empty
        {
            return Err(Code::SizeExceeded.into());
        }
        signed(content.size_bytes())?;
        if blob.content_version.is_some()
            && (blob.content_version.as_deref() != Some(content.content_version())
                || blob.size_bytes != Some(content.size_bytes())
                || blob.sha256.as_deref() != Some(content.sha256()))
        {
            return Err(Code::IdempotencyConflict.into());
        }
        if blob.state == BlobState::Initiated {
            execute(self.transaction.as_ref(),"UPDATE teaql_blob_object SET size_bytes=$1,sha256=$2,content_version=$3 WHERE blob_key=$4",vec![signed(content.size_bytes())?,content.sha256().into(),content.content_version().into(),blob.reference.as_str().into()]).await?;
        }
        self.catalog
            .lock_blob(self.transaction.as_ref(), &blob.scope, &blob.reference)
            .await
    }
    /// Only actual server-side verified facts belong here. Prior written generation,
    /// size and hash must match, so a stale confirmation cannot replace that journal.
    pub async fn complete_verified(
        mut self,
        content: VerifiedContent,
        now: i64,
    ) -> BlobResult<Self> {
        match self.complete_inner(content, now).await {
            Ok(blob) => {
                self.blob = blob;
                Ok(self)
            }
            Err(error) => Err(self.abort(error).await),
        }
    }
    async fn complete_inner(
        &self,
        content: VerifiedContent,
        now: i64,
    ) -> BlobResult<BlobCatalogRecord> {
        self.catalog.require_active(self.transaction.as_ref())?;
        content.validate()?;
        let blob = &self.blob;
        if blob.content_version.is_some()
            && (blob.content_version.as_deref() != Some(&content.content_version)
                || blob.size_bytes != Some(content.size_bytes)
                || blob.sha256.as_deref() != Some(&content.sha256))
        {
            return Err(Code::IdempotencyConflict.into());
        }
        if matches!(
            blob.state,
            BlobState::UploadedUnbound | BlobState::Available
        ) {
            if blob.actual_mime.as_deref() != Some(&content.content_type) {
                return Err(Code::IdempotencyConflict.into());
            }
            return Ok(blob.clone());
        }
        if blob.state != BlobState::Initiated || now >= blob.upload_expires_at {
            return Err(Code::UploadExpired.into());
        }
        if content_type(&blob.extension) != Some(content.content_type.as_str()) {
            return Err(Code::FormatNotAllowed.into());
        }
        if content.size_bytes > blob.policy.max_size
            || content.size_bytes == 0 && !blob.policy.allow_empty
        {
            return Err(Code::SizeExceeded.into());
        }
        execute(self.transaction.as_ref(),"UPDATE teaql_blob_object SET state='uploaded_unbound',actual_mime=$1,size_bytes=$2,sha256=$3,content_version=$4,completed_at=$5 WHERE blob_key=$6",vec![content.content_type.into(),signed(content.size_bytes)?,content.sha256.into(),content.content_version.into(),now.into(),blob.reference.as_str().into()]).await?;
        self.catalog
            .lock_blob(self.transaction.as_ref(), &blob.scope, &blob.reference)
            .await
    }
    pub async fn commit(self) -> BlobResult<BlobCatalogRecord> {
        self.transaction.commit().await.map_err(|error| {
            let mut error: BlobError = error.into();
            error.completion = Some(self.identity.outcome());
            error
        })?;
        Ok(self.blob)
    }
    pub async fn rollback(self) -> BlobResult<()> {
        self.transaction.rollback().await.map_err(|error| {
            let mut error: BlobError = error.into();
            error.completion = Some(self.identity.outcome());
            error
        })
    }
    async fn abort(self, error: BlobError) -> BlobError {
        rollback_error(self.transaction, self.identity, error).await
    }
}
async fn rollback_error(
    transaction: Box<dyn InfrastructureTransaction>,
    identity: PhysicalTransactionIdentity,
    mut error: BlobError,
) -> BlobError {
    if transaction.rollback().await.is_err() {
        error = Code::TransactionRequired.into();
    }
    error.completion = Some(identity.outcome());
    error
}
