//! Actual current-context upload admission and immutable store/verifier orchestration.
use crate::*;
use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use teaql_core::{BlobFieldSpec, BlobRef, DataType};
use teaql_data_service::infrastructure_sql::PhysicalTransactionOutcome;
use teaql_runtime::UserContext;

#[derive(Clone, PartialEq, Eq)]
pub struct BlobUploadIdentity {
    scope: BlobScope,
    actor: String,
}
impl BlobUploadIdentity {
    /// Host-verified identity only. Never inferred from an OS/user-name default.
    pub fn new(scope: BlobScope, actor: impl Into<String>) -> BlobResult<Self> {
        let actor = actor.into();
        name(&actor, 256).map_err(|_| BlobErrorCode::BindDenied)?;
        Ok(Self { scope, actor })
    }
    pub fn scope(&self) -> &BlobScope {
        &self.scope
    }
    pub fn actor(&self) -> &str {
        &self.actor
    }
}
impl fmt::Debug for BlobUploadIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobUploadIdentity([redacted])")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobUploadAction {
    Begin,
    Write,
    Confirm,
}
#[derive(Clone)]
pub struct BlobUploadFileInfo {
    pub file_name: String,
    pub declared_size: Option<u64>,
    pub declared_content_type: Option<String>,
}
impl fmt::Debug for BlobUploadFileInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobUploadFileInfo([redacted])")
    }
}
/// Required host policy. Both field permission and Blob-specific policy must pass.
/// Implementations resolve the actual supplied context on every call; no defaults.
pub trait BlobUploadAuthorization: Send + Sync {
    fn identity(&self, context: &UserContext) -> BlobResult<BlobUploadIdentity>;
    fn can_write_field(&self, context: &UserContext, target: &BlobFieldTarget) -> BlobResult<bool>;
    fn authorize_upload(
        &self,
        context: &UserContext,
        target: &BlobFieldTarget,
        action: BlobUploadAction,
        info: &BlobUploadFileInfo,
    ) -> BlobResult<bool>;
}
pub trait BlobUploadClock: Send + Sync {
    fn now_millis(&self) -> BlobResult<i64>;
}
pub struct SystemBlobUploadClock;
impl BlobUploadClock for SystemBlobUploadClock {
    fn now_millis(&self) -> BlobResult<i64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_millis()).ok())
            .ok_or_else(|| BlobErrorCode::PolicyInvalid.into())
    }
}
#[derive(Clone)]
pub struct BlobUploadSession {
    upload_id: String,
    reference: BlobRef,
    expires_at_millis: i64,
}
impl BlobUploadSession {
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }
    pub fn reference(&self) -> &BlobRef {
        &self.reference
    }
    pub fn expires_at_millis(&self) -> i64 {
        self.expires_at_millis
    }
}
impl fmt::Debug for BlobUploadSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobUploadSession([redacted])")
    }
}
#[derive(Clone)]
pub struct BlobUploadWriteReceipt {
    upload_id: String,
    size_bytes: u64,
}
impl BlobUploadWriteReceipt {
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}
impl fmt::Debug for BlobUploadWriteReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobUploadWriteReceipt([redacted])")
    }
}
#[derive(Clone)]
pub struct BlobUploadService {
    catalog: PostgresBlobCatalog,
    store: Arc<dyn BlobStore>,
    verifier: Arc<dyn BlobContentVerifier>,
    policies: Arc<BlobPolicies>,
    authorization: Arc<dyn BlobUploadAuthorization>,
    clock: Arc<dyn BlobUploadClock>,
}
impl BlobUploadService {
    /// Passive service assembly; no I/O, background tasks, schema or capability claim.
    pub fn new(
        catalog: PostgresBlobCatalog,
        store: Arc<dyn BlobStore>,
        verifier: Arc<dyn BlobContentVerifier>,
        policies: BlobPolicies,
        authorization: Arc<dyn BlobUploadAuthorization>,
        clock: Arc<dyn BlobUploadClock>,
    ) -> Self {
        Self {
            catalog,
            store,
            verifier,
            policies: Arc::new(policies),
            authorization,
            clock,
        }
    }
    pub async fn validate_ready(&self, context: &UserContext) -> BlobResult<()> {
        let mut fields = Vec::new();
        for entity in context.all_entities() {
            for property in &entity.properties {
                if let Some(spec) = &property.blob_field_spec {
                    if property.data_type != DataType::BlobRef
                        || property.nullable != spec.nullable()
                        || spec.entity() != entity.name
                        || spec.field() != property.name
                    {
                        return Err(BlobErrorCode::PolicyInvalid.into());
                    }
                    let policy = self.policies.resolve(spec)?;
                    if policy.upload_mode() != UploadMode::ServerStream {
                        return Err(BlobErrorCode::CapabilityMissing.into());
                    }
                    fields.push(spec);
                } else if property.data_type == DataType::BlobRef {
                    return Err(BlobErrorCode::PolicyInvalid.into());
                }
            }
        }
        self.policies.validate_model(fields)?;
        self.require_store()?;
        self.catalog.validate_upload_ready().await?;
        self.store.validate_ready().await?;
        self.verifier.validate_ready().await
    }
    fn require_store(&self) -> BlobResult<()> {
        let capabilities = self.store.capabilities();
        if !capabilities.range
            || !capabilities.version_conditional_delete
            || self.store.maximum_write_bytes() == 0
        {
            return Err(BlobErrorCode::CapabilityMissing.into());
        }
        Ok(())
    }
    fn field<'a>(
        &self,
        context: &'a UserContext,
        target: &BlobFieldTarget,
    ) -> BlobResult<&'a BlobFieldSpec> {
        let property = context
            .entity(target.entity())
            .and_then(|entity| entity.property_by_name(target.field()))
            .ok_or(BlobErrorCode::ReferenceInvalid)?;
        let spec = property
            .blob_field_spec
            .as_ref()
            .ok_or(BlobErrorCode::ReferenceInvalid)?;
        if property.data_type != DataType::BlobRef
            || property.nullable != spec.nullable()
            || spec.entity() != target.entity()
            || spec.field() != target.field()
        {
            return Err(BlobErrorCode::ReferenceInvalid.into());
        }
        Ok(spec)
    }
    fn current(
        &self,
        context: &UserContext,
        identity: &BlobUploadIdentity,
        target: &BlobFieldTarget,
        action: BlobUploadAction,
        info: &BlobUploadFileInfo,
        extension: &str,
    ) -> BlobResult<()> {
        if &self.authorization.identity(context)? != identity
            || !self.authorization.can_write_field(context, target)?
            || !self
                .authorization
                .authorize_upload(context, target, action, info)?
        {
            return Err(BlobErrorCode::BindDenied.into());
        }
        if !self
            .field(context, target)?
            .allowed_extensions()
            .contains(extension)
        {
            return Err(BlobErrorCode::FormatNotAllowed.into());
        }
        self.policies.require_upload_format(extension)
    }
    pub async fn begin(
        &self,
        context: &UserContext,
        target: BlobFieldTarget,
        input: BlobUploadFileInfo,
        idempotency_key: &str,
    ) -> BlobResult<BlobUploadSession> {
        let info = normalize(input)?;
        name(idempotency_key, 128)?;
        let extension = info
            .file_name
            .rsplit_once('.')
            .map_or("", |(_, extension)| extension)
            .to_ascii_lowercase();
        let identity = self.authorization.identity(context)?;
        self.current(
            context,
            &identity,
            &target,
            BlobUploadAction::Begin,
            &info,
            &extension,
        )?;
        self.require_store()?;
        if info.declared_size.is_some_and(|size| {
            size > self
                .policies
                .maximum_upload_size_bytes()
                .min(self.store.maximum_write_bytes())
        }) {
            return Err(BlobErrorCode::SizeExceeded.into());
        }
        let digest = request_digest(&target, &info);
        if let Some(blob) = self
            .catalog
            .find_upload_request(identity.scope(), idempotency_key, &digest)
            .await?
        {
            return self.session(context, &identity, &blob, &info).await;
        }
        let policy = self.policies.resolve(self.field(context, &target)?)?;
        if !policy
            .field_spec()?
            .allowed_extensions()
            .contains(&extension)
        {
            return Err(BlobErrorCode::FormatNotAllowed.into());
        }
        if info.declared_size.is_some_and(|size| {
            size > policy
                .max_size_bytes()
                .min(self.store.maximum_write_bytes())
        }) {
            return Err(BlobErrorCode::SizeExceeded.into());
        }
        if policy.upload_mode() != UploadMode::ServerStream {
            return Err(BlobErrorCode::CapabilityMissing.into());
        }
        let upload_id = format!("bu_{}", random_id()?);
        let reference = BlobRef::new(format!("blob_{}", random_id()?))
            .map_err(|_| BlobErrorCode::ReferenceInvalid)?;
        let candidate = self.store.create_upload(&upload_id).await?;
        if candidate.upload_id() != upload_id {
            return Err(BlobErrorCode::StoreFailed.into());
        }
        self.current(
            context,
            &identity,
            &target,
            BlobUploadAction::Begin,
            &info,
            &extension,
        )?;
        let blob = self
            .catalog
            .begin_upload(
                UploadDraft {
                    scope: identity.scope().clone(),
                    reference,
                    upload_id,
                    request_key: idempotency_key.into(),
                    request_digest: digest,
                    original_name: info.file_name.clone(),
                    extension,
                    storage_location: candidate.location().into(),
                    target,
                    policy,
                },
                self.clock.now_millis()?,
            )
            .await?;
        self.session(context, &identity, &blob, &info).await
    }
    async fn session(
        &self,
        context: &UserContext,
        identity: &BlobUploadIdentity,
        blob: &BlobCatalogRecord,
        info: &BlobUploadFileInfo,
    ) -> BlobResult<BlobUploadSession> {
        if blob.scope() != identity.scope() {
            return Err(BlobErrorCode::BindDenied.into());
        }
        if matches!(blob.state(), BlobState::Deleting | BlobState::Deleted) {
            return Err(BlobErrorCode::UploadExpired.into());
        }
        if blob.policy().upload_mode() != UploadMode::ServerStream {
            return Err(BlobErrorCode::CapabilityMissing.into());
        }
        self.current(
            context,
            identity,
            blob.target(),
            BlobUploadAction::Begin,
            info,
            blob.extension(),
        )?;
        let target = self.store.create_upload(blob.upload_id()).await?;
        require_location(blob, &target)?;
        self.current(
            context,
            identity,
            blob.target(),
            BlobUploadAction::Begin,
            info,
            blob.extension(),
        )?;
        Ok(BlobUploadSession {
            upload_id: blob.upload_id().into(),
            reference: blob.reference().clone(),
            expires_at_millis: blob.upload_expires_at(),
        })
    }
    async fn upload(
        &self,
        context: &UserContext,
        id: &str,
        action: BlobUploadAction,
    ) -> BlobResult<(BlobUploadIdentity, BlobCatalogRecord)> {
        let identity = self.authorization.identity(context)?;
        let blob = self
            .catalog
            .find_upload(identity.scope(), id)
            .await?
            .ok_or(BlobErrorCode::BindDenied)?;
        self.current(
            context,
            &identity,
            blob.target(),
            action,
            &facts(&blob),
            blob.extension(),
        )?;
        self.require_store()?;
        if blob.policy().upload_mode() != UploadMode::ServerStream {
            return Err(BlobErrorCode::CapabilityMissing.into());
        }
        Ok((identity, blob))
    }
    fn maximum(&self, blob: &BlobCatalogRecord) -> u64 {
        blob.policy()
            .max_size_bytes()
            .min(self.policies.maximum_upload_size_bytes())
            .min(self.store.maximum_write_bytes())
    }
    fn writable(&self, blob: &BlobCatalogRecord) -> BlobResult<()> {
        if matches!(blob.state(), BlobState::Deleting | BlobState::Deleted)
            || self.clock.now_millis()? >= blob.upload_expires_at()
        {
            return Err(BlobErrorCode::UploadExpired.into());
        }
        Ok(())
    }
    fn confirmable(&self, blob: &BlobCatalogRecord) -> BlobResult<Option<i64>> {
        if blob.state() == BlobState::Initiated {
            self.writable(blob)?;
            return Ok(Some(blob.upload_expires_at()));
        }
        let deadline = blob.eligible_at()?;
        BlobRetention::require_bindable(blob.state(), self.clock.now_millis()?, deadline)?;
        Ok(deadline)
    }
    async fn until<T>(
        &self,
        deadline: Option<i64>,
        work: impl Future<Output = BlobResult<T>>,
    ) -> BlobResult<T> {
        let result = if let Some(deadline) = deadline {
            let remaining = deadline
                .checked_sub(self.clock.now_millis()?)
                .filter(|remaining| *remaining > 0)
                .ok_or(BlobErrorCode::UploadExpired)?;
            tokio::time::timeout(Duration::from_millis(remaining as u64), work)
                .await
                .map_err(|_| BlobError::from(BlobErrorCode::UploadExpired))?
        } else {
            work.await
        };
        if let Some(deadline) = deadline
            && self.clock.now_millis()? >= deadline
        {
            return Err(BlobErrorCode::UploadExpired.into());
        }
        result
    }
    pub async fn write(
        &self,
        context: &UserContext,
        id: &str,
        source: BlobContentStream,
    ) -> BlobResult<BlobUploadWriteReceipt> {
        let (identity, blob) = self.upload(context, id, BlobUploadAction::Write).await?;
        self.writable(&blob)?;
        let deadline = Some(blob.upload_expires_at());
        let lease = self
            .until(
                deadline,
                self.catalog.lock_upload(identity.scope(), blob.reference()),
            )
            .await?;
        let blob = lease.record().clone();
        let result = async {
            self.writable(&blob)?;
            self.current(
                context,
                &identity,
                blob.target(),
                BlobUploadAction::Write,
                &facts(&blob),
                blob.extension(),
            )?;
            let target = self
                .until(deadline, self.store.create_upload(blob.upload_id()))
                .await?;
            require_location(&blob, &target)?;
            self.current(
                context,
                &identity,
                blob.target(),
                BlobUploadAction::Write,
                &facts(&blob),
                blob.extension(),
            )?;
            let stream = deadline_stream(source, blob.upload_expires_at(), self.clock.clone());
            let content = self
                .until(
                    deadline,
                    self.store.write(&target, stream, self.maximum(&blob)),
                )
                .await;
            self.writable(&blob)?;
            let content = content?;
            if content.location() != blob.storage_location()
                || content.size_bytes() > self.maximum(&blob)
            {
                return Err(BlobErrorCode::StoreFailed.into());
            }
            self.current(
                context,
                &identity,
                blob.target(),
                BlobUploadAction::Write,
                &BlobUploadFileInfo {
                    file_name: blob.original_name().into(),
                    declared_size: Some(content.size_bytes()),
                    declared_content_type: None,
                },
                blob.extension(),
            )?;
            Ok(content)
        }
        .await;
        let content = match result {
            Ok(content) => content,
            Err(error) => return Err(rollback(lease, error).await),
        };
        let lease = lease
            .record_write(&content, self.clock.now_millis()?)
            .await?;
        if let Err(error) = self.writable(&blob).and_then(|_| {
            self.current(
                context,
                &identity,
                blob.target(),
                BlobUploadAction::Write,
                &BlobUploadFileInfo {
                    file_name: blob.original_name().into(),
                    declared_size: Some(content.size_bytes()),
                    declared_content_type: None,
                },
                blob.extension(),
            )
        }) {
            return Err(rollback(lease, error).await);
        }
        lease.commit().await?;
        Ok(BlobUploadWriteReceipt {
            upload_id: blob.upload_id().into(),
            size_bytes: content.size_bytes(),
        })
    }
    pub async fn complete(&self, context: &UserContext, id: &str) -> BlobResult<BlobRef> {
        let (identity, blob) = self.upload(context, id, BlobUploadAction::Confirm).await?;
        let deadline = self.confirmable(&blob)?;
        let lease = self
            .until(
                deadline,
                self.catalog.lock_upload(identity.scope(), blob.reference()),
            )
            .await?;
        let blob = lease.record().clone();
        let result = async {
            self.confirmable(&blob)?;
            self.current(
                context,
                &identity,
                blob.target(),
                BlobUploadAction::Confirm,
                &facts(&blob),
                blob.extension(),
            )?;
            if blob.content_version().is_none() {
                return Err(BlobErrorCode::UploadIncomplete.into());
            }
            let content = self
                .until(deadline, self.store.stat(blob.storage_location()))
                .await?
                .ok_or(BlobErrorCode::UploadIncomplete)?;
            if content.location() != blob.storage_location()
                || Some(content.content_version()) != blob.content_version()
                || Some(content.size_bytes()) != blob.size_bytes()
                || Some(content.sha256()) != blob.sha256()
            {
                return Err(BlobErrorCode::UploadIncomplete.into());
            }
            if content.size_bytes() > self.maximum(&blob) {
                return Err(BlobErrorCode::SizeExceeded.into());
            }
            if blob.state() != BlobState::Initiated {
                return Ok(None);
            }
            let bytes = self
                .until(
                    deadline,
                    self.store.open_range(
                        blob.storage_location(),
                        content.content_version(),
                        0,
                        content.size_bytes(),
                    ),
                )
                .await?;
            if bytes.content != content || bytes.offset != 0 || bytes.length != content.size_bytes()
            {
                return Err(BlobErrorCode::UploadIncomplete.into());
            }
            let verified = self
                .until(
                    deadline,
                    self.verifier.verify(
                        bytes.stream,
                        blob.extension(),
                        self.maximum(&blob),
                        blob.policy().allow_empty(),
                    ),
                )
                .await?;
            if verified.size_bytes() != content.size_bytes()
                || verified.sha256() != content.sha256()
                || teaql_core::blob::content_type(blob.extension()) != Some(verified.content_type())
            {
                return Err(BlobErrorCode::UploadIncomplete.into());
            }
            Ok(Some(VerifiedContent {
                content_type: verified.content_type().into(),
                size_bytes: verified.size_bytes(),
                sha256: verified.sha256().into(),
                content_version: content.content_version().into(),
            }))
        }
        .await;
        let content = match result {
            Ok(content) => content,
            Err(error) => return Err(rollback(lease, error).await),
        };
        let info = if let Some(content) = &content {
            BlobUploadFileInfo {
                file_name: blob.original_name().into(),
                declared_size: Some(content.size_bytes),
                declared_content_type: Some(content.content_type.clone()),
            }
        } else {
            facts(&blob)
        };
        if let Err(error) = self.confirmable(&blob).and_then(|_| {
            self.current(
                context,
                &identity,
                blob.target(),
                BlobUploadAction::Confirm,
                &info,
                blob.extension(),
            )
        }) {
            return Err(rollback(lease, error).await);
        }
        let lease = if let Some(content) = content {
            lease
                .complete_verified(content, self.clock.now_millis()?)
                .await?
        } else {
            lease
        };
        if let Err(error) = self.confirmable(&blob).and_then(|_| {
            self.current(
                context,
                &identity,
                blob.target(),
                BlobUploadAction::Confirm,
                &info,
                blob.extension(),
            )
        }) {
            return Err(rollback(lease, error).await);
        }
        Ok(lease.commit().await?.reference().clone())
    }
}
async fn rollback(lease: BlobUploadLease, mut error: BlobError) -> BlobError {
    if let Err(error) = lease.rollback().await {
        return error;
    }
    error.completion = Some(PhysicalTransactionOutcome::RolledBack);
    error
}
fn facts(blob: &BlobCatalogRecord) -> BlobUploadFileInfo {
    BlobUploadFileInfo {
        file_name: blob.original_name().into(),
        declared_size: blob.size_bytes(),
        declared_content_type: blob.actual_mime().map(str::to_owned),
    }
}
fn require_location(blob: &BlobCatalogRecord, target: &BlobUploadTarget) -> BlobResult<()> {
    if target.upload_id() != blob.upload_id() || target.location() != blob.storage_location() {
        return Err(BlobErrorCode::StoreFailed.into());
    }
    Ok(())
}
fn normalize(mut info: BlobUploadFileInfo) -> BlobResult<BlobUploadFileInfo> {
    info.file_name = info
        .file_name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or("")
        .to_owned();
    if info.file_name.len() > 512 || name(&info.file_name, 512).is_err() {
        return Err(BlobErrorCode::FormatNotAllowed.into());
    }
    if let Some(mime) = &mut info.declared_content_type {
        *mime = mime.trim().to_ascii_lowercase();
        if mime.len() > 160 || mime.chars().any(char::is_control) {
            return Err(BlobErrorCode::FormatNotAllowed.into());
        }
    }
    Ok(info)
}
fn random_id() -> BlobResult<String> {
    let mut bytes = [0u8; 24];
    getrandom::fill(&mut bytes).map_err(|_| BlobError::from(BlobErrorCode::StoreFailed))?;
    Ok(bytes.iter().map(|value| format!("{value:02x}")).collect())
}
// Same length-delimited canonical request bytes as Java's upload orchestrator.
fn request_digest(target: &BlobFieldTarget, info: &BlobUploadFileInfo) -> String {
    let mut hash = Sha256::new();
    fn text(hash: &mut Sha256, text: &str) {
        hash.update((text.len() as u32).to_be_bytes());
        hash.update(text.as_bytes());
    }
    for value in [target.entity(), target.field(), &info.file_name] {
        text(&mut hash, value);
    }
    for value in [target.id(), info.declared_size] {
        hash.update([u8::from(value.is_some())]);
        if let Some(value) = value {
            hash.update(value.to_be_bytes());
        }
    }
    hash.update([u8::from(info.declared_content_type.is_some())]);
    if let Some(mime) = &info.declared_content_type {
        text(&mut hash, mime);
    }
    format!("{:x}", hash.finalize())
}
fn deadline_stream(
    source: BlobContentStream,
    expiry: i64,
    clock: Arc<dyn BlobUploadClock>,
) -> BlobContentStream {
    Box::pin(stream::unfold(
        Some((source, clock)),
        move |state| async move {
            let (mut source, clock) = state?;
            if clock.now_millis().map_or(true, |now| now >= expiry) {
                return Some((Err(BlobErrorCode::UploadExpired.into()), None));
            }
            let next = source.next().await?;
            if clock.now_millis().map_or(true, |now| now >= expiry) {
                return Some((Err(BlobErrorCode::UploadExpired.into()), None));
            }
            Some((next, Some((source, clock))))
        },
    ))
}
