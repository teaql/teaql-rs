use crate::{
    BlobError, BlobErrorCode as Code, BlobFieldTarget, BlobPolicySnapshot, BlobResult,
    BlobRetention, BlobScope, Sharing, add_seconds, name,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
};
use teaql_core::{BlobRef, CompactRow, Value, blob::content_type};
use teaql_data_service::infrastructure_sql::{
    InfrastructureSql, InfrastructureStatement, PhysicalTransactionIdentity, SqlResourceIdentity,
};

const MIGRATION: &str = include_str!("postgres-catalog-v1.sql");
type Work<'a, T> = Pin<Box<dyn Future<Output = BlobResult<T>> + Send + 'a>>;
mod upload;
pub use upload::BlobUploadLease;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobState {
    Initiated,
    UploadedUnbound,
    Available,
    Deleting,
    Deleted,
}
impl BlobState {
    fn parse(value: &str) -> BlobResult<Self> {
        match value {
            "initiated" => Ok(Self::Initiated),
            "uploaded_unbound" => Ok(Self::UploadedUnbound),
            "available" => Ok(Self::Available),
            "deleting" => Ok(Self::Deleting),
            "deleted" => Ok(Self::Deleted),
            _ => Err(Code::CatalogFailed.into()),
        }
    }
}
#[derive(Clone)]
pub struct BlobCatalogRecord {
    pub(crate) scope: BlobScope,
    pub(crate) reference: BlobRef,
    pub(crate) state: BlobState,
    pub(crate) upload_id: String,
    pub(crate) original_name: String,
    pub(crate) extension: String,
    pub(crate) actual_mime: Option<String>,
    pub(crate) size_bytes: Option<u64>,
    pub(crate) sha256: Option<String>,
    pub(crate) content_version: Option<String>,
    pub(crate) storage_location: String,
    pub(crate) target: BlobFieldTarget,
    pub(crate) policy: BlobPolicySnapshot,
    pub(crate) created_at: i64,
    pub(crate) upload_expires_at: i64,
    pub(crate) completed_at: Option<i64>,
    pub(crate) first_bound_at: Option<i64>,
    pub(crate) unreferenced_at: Option<i64>,
    pub(crate) retention_floor_at: Option<i64>,
    pub(crate) effective_unref_seconds: u64,
    pub(crate) single_binding: bool,
    pub(crate) deleted_at: Option<i64>,
}
impl fmt::Debug for BlobCatalogRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobCatalogRecord([redacted])")
    }
}
impl BlobCatalogRecord {
    pub fn reference(&self) -> &BlobRef {
        &self.reference
    }
    pub fn scope(&self) -> &BlobScope {
        &self.scope
    }
    pub fn state(&self) -> BlobState {
        self.state
    }
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }
    pub fn original_name(&self) -> &str {
        &self.original_name
    }
    pub fn extension(&self) -> &str {
        &self.extension
    }
    pub fn actual_mime(&self) -> Option<&str> {
        self.actual_mime.as_deref()
    }
    pub fn size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }
    pub fn sha256(&self) -> Option<&str> {
        self.sha256.as_deref()
    }
    pub fn content_version(&self) -> Option<&str> {
        self.content_version.as_deref()
    }
    pub fn storage_location(&self) -> &str {
        &self.storage_location
    }
    pub fn target(&self) -> &BlobFieldTarget {
        &self.target
    }
    pub fn policy(&self) -> &BlobPolicySnapshot {
        &self.policy
    }
    pub fn created_at(&self) -> i64 {
        self.created_at
    }
    pub fn upload_expires_at(&self) -> i64 {
        self.upload_expires_at
    }
    pub fn completed_at(&self) -> Option<i64> {
        self.completed_at
    }
    pub fn first_bound_at(&self) -> Option<i64> {
        self.first_bound_at
    }
    pub fn unreferenced_at(&self) -> Option<i64> {
        self.unreferenced_at
    }
    pub fn retention_floor_at(&self) -> Option<i64> {
        self.retention_floor_at
    }
    pub fn effective_unreferenced_seconds(&self) -> u64 {
        self.effective_unref_seconds
    }
    pub fn deleted_at(&self) -> Option<i64> {
        self.deleted_at
    }
    pub(crate) fn eligible_at(&self) -> BlobResult<Option<i64>> {
        match self.state {
            BlobState::Initiated => Ok(Some(self.upload_expires_at)),
            BlobState::UploadedUnbound => self
                .completed_at
                .map(|time| BlobRetention::unbound_eligible_at(time, self.policy.unbound))
                .transpose(),
            BlobState::Available => self
                .unreferenced_at
                .map(|time| {
                    BlobRetention::unreferenced_eligible_at(
                        time,
                        self.effective_unref_seconds,
                        self.retention_floor_at,
                    )
                })
                .transpose(),
            BlobState::Deleting => Ok(Some(i64::MIN)),
            BlobState::Deleted => Ok(None),
        }
    }
}

/// Trusted server-generated upload identity/locator and canonical request digest.
pub struct UploadDraft {
    pub scope: BlobScope,
    pub reference: BlobRef,
    pub upload_id: String,
    pub request_key: String,
    pub request_digest: String,
    pub original_name: String,
    pub extension: String,
    pub storage_location: String,
    pub target: BlobFieldTarget,
    pub policy: BlobPolicySnapshot,
}
impl UploadDraft {
    fn validate(&self) -> BlobResult<()> {
        for (value, max) in [
            (&self.upload_id, 128),
            (&self.request_key, 128),
            (&self.original_name, 512),
            (&self.storage_location, 2048),
        ] {
            name(value, max)?;
        }
        digest(&self.request_digest)?;
        self.policy.to_json()?;
        if !self.policy.extensions.contains(&self.extension)
            || self.target.entity() != self.policy.entity
            || self.target.field() != self.policy.field
        {
            return Err(Code::ReferenceInvalid.into());
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct VerifiedContent {
    pub content_type: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub content_version: String,
}
impl VerifiedContent {
    fn validate(&self) -> BlobResult<()> {
        name(&self.content_type, 160)?;
        name(&self.content_version, 1024)?;
        digest(&self.sha256)?;
        if self.size_bytes > i64::MAX as u64 {
            return Err(Code::SizeExceeded.into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobBinding {
    pub scope: BlobScope,
    pub target: BlobFieldTarget,
    pub reference: Option<BlobRef>,
    pub retired_reference: Option<BlobRef>,
    pub version: i64,
    pub bound_at: Option<i64>,
    pub retain_until: Option<i64>,
}
/// Framework plan input. Host permission review precedes this low-level catalog API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingChangeKind {
    /// Assign or clear a field on an entity that remains live.
    FieldWrite,
    /// Release the binding because the owning entity is being deleted.
    EntityDelete,
}
pub struct BindingChange {
    pub target: BlobFieldTarget,
    pub previous: Option<BlobRef>,
    pub next: Option<BlobRef>,
    pub expected_binding_version: Option<i64>,
    pub policy: BlobPolicySnapshot,
    pub kind: BindingChangeKind,
}
struct PreparedChange {
    previous: BlobBinding,
    next: Option<BlobRef>,
    retain_until: Option<i64>,
    policy: BlobPolicySnapshot,
    kind: BindingChangeKind,
}
/// Frozen physical owner, locked rows and accepted policy. It cannot be reused in another transaction.
pub struct PreparedBlobBindings {
    identity: PhysicalTransactionIdentity,
    changes: Vec<PreparedChange>,
    objects: BTreeSet<BlobRef>,
    scope: BlobScope,
    prepared_at: i64,
}
impl fmt::Debug for PreparedBlobBindings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PreparedBlobBindings([opaque])")
    }
}

#[derive(Clone)]
pub struct PostgresBlobCatalog {
    root: Arc<dyn InfrastructureSql>,
    resource: SqlResourceIdentity,
}
impl PostgresBlobCatalog {
    pub fn new(root: Arc<dyn InfrastructureSql>) -> BlobResult<Self> {
        if root.database_kind() != "postgresql" || root.transaction_identity().is_some() {
            return Err(Code::TransactionRequired.into());
        }
        Ok(Self {
            resource: root.resource_identity(),
            root,
        })
    }
    pub fn resource_identity(&self) -> &SqlResourceIdentity {
        &self.resource
    }
    fn require_resource(&self, sql: &dyn InfrastructureSql) -> BlobResult<()> {
        if sql.database_kind() != "postgresql" || sql.resource_identity() != self.resource {
            Err(Code::TransactionRequired.into())
        } else {
            Ok(())
        }
    }
    fn require_active(
        &self,
        sql: &dyn InfrastructureSql,
    ) -> BlobResult<PhysicalTransactionIdentity> {
        self.require_resource(sql)?;
        let identity = sql
            .transaction_identity()
            .ok_or(Code::TransactionRequired)?;
        identity
            .require_active()
            .map_err(|_| Code::TransactionRequired)?;
        if identity.resource() != &self.resource {
            return Err(Code::TransactionRequired.into());
        }
        Ok(identity)
    }
    async fn transaction<T: Send + 'static>(
        &self,
        work: impl for<'a> FnOnce(&'a dyn InfrastructureSql) -> Work<'a, T> + Send,
    ) -> BlobResult<T> {
        let transaction = self.root.begin().await?;
        let identity = self.require_active(transaction.as_ref())?;
        let result = work(transaction.as_ref()).await;
        match result {
            Ok(value) => match transaction.commit().await {
                Ok(()) => Ok(value),
                Err(error) => {
                    let mut error: BlobError = error.into();
                    error.completion = Some(identity.outcome());
                    Err(error)
                }
            },
            Err(mut error) => {
                if transaction.rollback().await.is_err() {
                    error = Code::TransactionRequired.into();
                }
                error.completion = Some(identity.outcome());
                Err(error)
            }
        }
    }
    /// Explicit fixed owned migration; passive construction never creates schema.
    pub async fn migrate(&self) -> BlobResult<()> {
        let catalog = self.clone();
        self.transaction(move |sql|Box::pin(async move {
            fetch(sql,"SELECT 1 AS value FROM pg_advisory_xact_lock(73402110318407)",vec![]).await?;
            execute(sql,"CREATE TABLE IF NOT EXISTS teaql_blob_schema (id INTEGER PRIMARY KEY CHECK (id = 1), schema_version INTEGER NOT NULL, schema_digest VARCHAR(64) NOT NULL)",vec![]).await?;
            execute(sql,"INSERT INTO teaql_blob_schema VALUES (1,0,'pending') ON CONFLICT (id) DO NOTHING",vec![]).await?;
            let row=one(fetch(sql,"SELECT schema_version,schema_digest FROM teaql_blob_schema WHERE id=1 FOR UPDATE",vec![]).await?)?;
            match integer(&row,"schema_version")? {
                0=>{ for statement in MIGRATION.split(';').map(str::trim).filter(|s|!s.is_empty()) { execute(sql,statement,vec![]).await?; }
                    execute(sql,"UPDATE teaql_blob_schema SET schema_version=1,schema_digest=$1 WHERE id=1",vec![migration_digest().into()]).await?; },
                1 if text(&row,"schema_digest")?==migration_digest()=>(),
                _=>return Err(Code::SchemaMissing.into()),
            }
            catalog.validate_schema_on(sql).await
        })).await
    }
    pub async fn validate_schema(&self) -> BlobResult<()> {
        self.validate_schema_on(self.root.as_ref()).await
    }
    /// Read-only upload readiness; no transaction, setting change or provisioning.
    pub async fn validate_upload_ready(&self) -> BlobResult<()> {
        self.validate_schema().await?;
        self.validate_upload_support_on(self.root.as_ref()).await
    }
    async fn validate_upload_support_on(&self, sql: &dyn InfrastructureSql) -> BlobResult<()> {
        self.require_resource(sql)?;
        let row=one(fetch(sql,"SELECT current_setting('server_version_num')::bigint AS version,current_setting('client_connection_check_interval',true) AS disconnect",vec![]).await?)?;
        if integer(&row, "version")? < 140000
            || !matches!(row.get("disconnect"), Some(Value::Text(_)))
        {
            return Err(Code::CapabilityMissing.into());
        }
        Ok(())
    }
    async fn validate_schema_on(&self, sql: &dyn InfrastructureSql) -> BlobResult<()> {
        self.require_resource(sql)?;
        let result:BlobResult<()>=async {
            let row=one(fetch(sql,"SELECT schema_version,schema_digest FROM teaql_blob_schema WHERE id=1",vec![]).await?)?;
            if integer(&row,"schema_version")?!=1 || text(&row,"schema_digest")?!=migration_digest() { return Err(Code::SchemaMissing.into()); }
            fetch(sql,"SELECT blob_key,scope_realm,scope_boundary,upload_id,request_key,request_digest,state,original_name,extension,actual_mime,size_bytes,sha256,content_version,storage_location,target_entity,target_id,target_field,policy_json,created_at,upload_expires_at,completed_at,first_bound_at,unreferenced_at,retention_floor_at,effective_unref_seconds,single_binding,deleted_at FROM teaql_blob_object WHERE FALSE",vec![]).await?;
            fetch(sql,"SELECT scope_realm,scope_boundary,entity_type,entity_id,field_name,blob_key,retired_key,binding_version,bound_at,retain_until FROM teaql_blob_binding WHERE FALSE",vec![]).await?;
            let mut required:BTreeMap<&str,&str>=BTreeMap::from([
                ("teaql_blob_object_pk","PRIMARY KEY (blob_key)"),("teaql_blob_upload_uk","UNIQUE (upload_id)"),
                ("teaql_blob_request_uk","UNIQUE (scope_realm, scope_boundary, request_key)"),
                ("teaql_blob_binding_pk","PRIMARY KEY (scope_realm, scope_boundary, entity_type, entity_id, field_name)"),
                ("teaql_blob_binding_fk","FOREIGN KEY (blob_key) REFERENCES teaql_blob_object(blob_key)"),
                ("teaql_blob_retired_fk","FOREIGN KEY (retired_key) REFERENCES teaql_blob_object(blob_key)")]);
            for row in fetch(sql,"SELECT conname,convalidated,pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid IN ('teaql_blob_object'::regclass,'teaql_blob_binding'::regclass)",vec![]).await? {
                let name=text(&row,"conname")?;
                if let Some(expected)=required.remove(name) && (row.get("convalidated")!=Some(&Value::Bool(true)) || text(&row,"definition")?!=expected) { return Err(Code::SchemaMissing.into()); }
            }
            if !required.is_empty() { return Err(Code::SchemaMissing.into()); } Ok(())
        }.await;
        result.map_err(|_| Code::SchemaMissing.into())
    }
    pub async fn begin_upload(
        &self,
        draft: UploadDraft,
        now: i64,
    ) -> BlobResult<BlobCatalogRecord> {
        draft.validate()?;
        let catalog = self.clone();
        self.transaction(move |sql|Box::pin(async move {
            catalog.validate_schema_on(sql).await?;
            let expires=add_seconds(now,draft.policy.upload_session)?;
            execute(sql,"INSERT INTO teaql_blob_object (blob_key,scope_realm,scope_boundary,upload_id,request_key,request_digest,state,original_name,extension,storage_location,target_entity,target_id,target_field,policy_json,created_at,upload_expires_at,effective_unref_seconds,single_binding) VALUES ($1,$2,$3,$4,$5,$6,'initiated',$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) ON CONFLICT (scope_realm,scope_boundary,request_key) DO NOTHING",vec![draft.reference.as_str().into(),draft.scope.realm().into(),draft.scope.boundary().into(),draft.upload_id.into(),draft.request_key.clone().into(),draft.request_digest.clone().into(),draft.original_name.into(),draft.extension.into(),draft.storage_location.into(),draft.target.entity().into(),optional_id(draft.target.id()),draft.target.field().into(),draft.policy.to_json()?.into(),now.into(),expires.into(),signed(draft.policy.unreferenced)?,Value::I64(i64::from(draft.policy.sharing==Sharing::SingleBinding))]).await?;
            let row=one(fetch(sql,"SELECT * FROM teaql_blob_object WHERE scope_realm=$1 AND scope_boundary=$2 AND request_key=$3 FOR UPDATE",vec![draft.scope.realm().into(),draft.scope.boundary().into(),draft.request_key.into()]).await?)?;
            if text(&row,"request_digest")?!=draft.request_digest { return Err(Code::IdempotencyConflict.into()); }
            record(row)
        })).await
    }
    pub async fn find(
        &self,
        scope: &BlobScope,
        reference: &BlobRef,
    ) -> BlobResult<Option<BlobCatalogRecord>> {
        self.validate_schema().await?;
        let mut rows=fetch(self.root.as_ref(),"SELECT * FROM teaql_blob_object WHERE blob_key=$1 AND scope_realm=$2 AND scope_boundary=$3",vec![reference.as_str().into(),scope.realm().into(),scope.boundary().into()]).await?;
        if rows.len() > 1 {
            return Err(Code::CatalogFailed.into());
        }
        rows.pop().map(record).transpose()
    }
    pub async fn find_upload(
        &self,
        scope: &BlobScope,
        upload_id: &str,
    ) -> BlobResult<Option<BlobCatalogRecord>> {
        name(upload_id, 128)?;
        self.validate_schema().await?;
        let rows=fetch(self.root.as_ref(),"SELECT * FROM teaql_blob_object WHERE scope_realm=$1 AND scope_boundary=$2 AND upload_id=$3",vec![scope.realm().into(),scope.boundary().into(),upload_id.into()]).await?;
        optional_record(rows)
    }
    /// Trusted canonical request digest; a mismatch never returns an old session.
    pub async fn find_upload_request(
        &self,
        scope: &BlobScope,
        request_key: &str,
        request_digest: &str,
    ) -> BlobResult<Option<BlobCatalogRecord>> {
        name(request_key, 128)?;
        digest(request_digest)?;
        self.validate_schema().await?;
        let rows=fetch(self.root.as_ref(),"SELECT * FROM teaql_blob_object WHERE scope_realm=$1 AND scope_boundary=$2 AND request_key=$3",vec![scope.realm().into(),scope.boundary().into(),request_key.into()]).await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = one(rows)?;
        if text(&row, "request_digest")? != request_digest {
            return Err(Code::IdempotencyConflict.into());
        }
        Ok(Some(record(row)?))
    }
    /// Own the physical row lock across storage I/O and current-context rechecks.
    /// Dropping an unfinished lease drops the adapter's transaction/connection.
    pub async fn lock_upload(
        &self,
        scope: &BlobScope,
        reference: &BlobRef,
    ) -> BlobResult<BlobUploadLease> {
        BlobUploadLease::acquire(self.clone(), scope, reference).await
    }
    /// Actual format/bytes verification is the upload service's responsibility, never a client completion flag.
    pub async fn complete_verified(
        &self,
        scope: &BlobScope,
        reference: &BlobRef,
        content: VerifiedContent,
        now: i64,
    ) -> BlobResult<BlobCatalogRecord> {
        content.validate()?;
        self.lock_upload(scope, reference)
            .await?
            .complete_verified(content, now)
            .await?
            .commit()
            .await
    }
    pub async fn binding(
        &self,
        scope: &BlobScope,
        target: &BlobFieldTarget,
    ) -> BlobResult<Option<BlobBinding>> {
        self.validate_schema().await?;
        let mut rows=fetch(self.root.as_ref(),"SELECT * FROM teaql_blob_binding WHERE scope_realm=$1 AND scope_boundary=$2 AND entity_type=$3 AND entity_id=$4 AND field_name=$5",target_args(scope,target)?).await?;
        if rows.len() > 1 {
            return Err(Code::CatalogFailed.into());
        }
        rows.pop().map(binding).transpose()
    }
    /// Read the authoritative old binding while holding its row lock on the
    /// caller's physical transaction. The returned version is suitable for
    /// `prepare_bindings`; a root-connection read is not.
    pub async fn binding_for_update(
        &self,
        sql: &dyn InfrastructureSql,
        scope: &BlobScope,
        target: &BlobFieldTarget,
    ) -> BlobResult<BlobBinding> {
        self.require_active(sql)?;
        let result = self.lock_binding(sql, scope, target).await;
        if result.is_err() {
            sql.mark_rollback_only()?;
        }
        result
    }
    pub async fn prepare_bindings(
        &self,
        sql: &dyn InfrastructureSql,
        scope: &BlobScope,
        changes: Vec<BindingChange>,
        now: i64,
    ) -> BlobResult<PreparedBlobBindings> {
        let identity = self.require_active(sql)?;
        let result = self.prepare_inner(sql, scope, changes, now, identity).await;
        if result.is_err() {
            sql.mark_rollback_only()?;
        }
        result
    }
    async fn prepare_inner(
        &self,
        sql: &dyn InfrastructureSql,
        scope: &BlobScope,
        changes: Vec<BindingChange>,
        now: i64,
        identity: PhysicalTransactionIdentity,
    ) -> BlobResult<PreparedBlobBindings> {
        if changes.len() > 1000 {
            return Err(Code::PolicyInvalid.into());
        }
        let mut ordered = BTreeMap::new();
        for change in changes {
            change.policy.to_json()?;
            if change.target.id().is_none()
                || change.policy.entity != change.target.entity()
                || change.policy.field != change.target.field()
                || (change.kind == BindingChangeKind::EntityDelete && change.next.is_some())
            {
                return Err(Code::ReferenceInvalid.into());
            }
            if ordered.insert(change.target.clone(), change).is_some() {
                return Err(Code::IdempotencyConflict.into());
            }
        }
        self.validate_schema_on(sql).await?;
        let mut prepared = Vec::new();
        let mut objects = BTreeSet::new();
        for change in ordered.into_values() {
            let old = self.lock_binding(sql, scope, &change.target).await?;
            if old.reference != change.previous
                || change
                    .expected_binding_version
                    .is_some_and(|expected| old.version != expected)
            {
                return Err(Code::IdempotencyConflict.into());
            }
            objects.extend(old.reference.iter().chain(change.next.iter()).cloned());
            let retain_until = change
                .next
                .as_ref()
                .map(|_| add_seconds(now, change.policy.minimum))
                .transpose()?;
            old.version.checked_add(1).ok_or(Code::CatalogFailed)?;
            prepared.push(PreparedChange {
                previous: old,
                next: change.next,
                retain_until,
                policy: change.policy,
                kind: change.kind,
            });
        }
        let mut locked = BTreeMap::new();
        for key in &objects {
            locked.insert(key.clone(), self.lock_blob(sql, scope, key).await?);
        }
        for change in &prepared {
            if change.previous.reference == change.next {
                continue;
            }
            if let Some(next) = &change.next {
                let blob = &locked[next];
                BlobRetention::require_bindable(blob.state, now, blob.eligible_at()?)?;
                if !change.policy.extensions.contains(&blob.extension)
                    || blob.actual_mime.as_deref() != content_type(&blob.extension)
                {
                    return Err(Code::FormatNotAllowed.into());
                }
                if blob.size_bytes.is_none_or(|size| {
                    size > change.policy.max_size || size == 0 && !change.policy.allow_empty
                }) {
                    return Err(Code::SizeExceeded.into());
                }
                if change.policy.sharing == Sharing::SingleBinding || blob.single_binding {
                    let current = self.binding_count(sql, next).await?;
                    let added = prepared
                        .iter()
                        .filter(|c| c.next.as_ref() == Some(next) && c.previous.reference != c.next)
                        .count() as i64;
                    let removed = prepared
                        .iter()
                        .filter(|c| {
                            c.previous.reference.as_ref() == Some(next)
                                && c.previous.reference != c.next
                        })
                        .count() as i64;
                    if current + added - removed > 1 {
                        return Err(Code::BindDenied.into());
                    }
                }
            } else if !change.policy.nullable && change.kind != BindingChangeKind::EntityDelete {
                return Err(Code::ReferenceInvalid.into());
            }
        }
        Ok(PreparedBlobBindings {
            identity,
            changes: prepared,
            objects,
            scope: scope.clone(),
            prepared_at: now,
        })
    }
    pub async fn apply_bindings(
        &self,
        sql: &dyn InfrastructureSql,
        prepared: PreparedBlobBindings,
        now: i64,
    ) -> BlobResult<()> {
        if self.require_active(sql)? != prepared.identity {
            return Err(Code::TransactionRequired.into());
        }
        let result = self.apply_inner(sql, prepared, now).await;
        if result.is_err() {
            sql.mark_rollback_only()?;
        }
        result
    }
    async fn apply_inner(
        &self,
        sql: &dyn InfrastructureSql,
        prepared: PreparedBlobBindings,
        now: i64,
    ) -> BlobResult<()> {
        let now = now.max(prepared.prepared_at);
        let changes = prepared
            .changes
            .into_iter()
            .map(|mut change| {
                change.retain_until = change
                    .next
                    .as_ref()
                    .map(|_| add_seconds(now, change.policy.minimum))
                    .transpose()?;
                Ok(change)
            })
            .collect::<BlobResult<Vec<_>>>()?;
        for change in changes {
            let old = change.previous;
            if old.reference == change.next {
                continue;
            }
            let mut args = vec![
                optional_key(change.next.as_ref()),
                optional_key(old.reference.as_ref()),
                (old.version + 1).into(),
                if change.next.is_some() {
                    now.into()
                } else {
                    Value::Null
                },
                change.retain_until.map_or(Value::Null, Value::I64),
            ];
            args.extend(target_args(&old.scope, &old.target)?);
            args.push(old.version.into());
            if execute(sql,"UPDATE teaql_blob_binding SET blob_key=$1,retired_key=$2,binding_version=$3,bound_at=$4,retain_until=$5 WHERE scope_realm=$6 AND scope_boundary=$7 AND entity_type=$8 AND entity_id=$9 AND field_name=$10 AND binding_version=$11",args).await?!=1 { return Err(Code::IdempotencyConflict.into()); }
            if let Some(next) = &change.next {
                let floor = change.retain_until.ok_or(Code::CatalogFailed)?;
                execute(sql,"UPDATE teaql_blob_object SET retention_floor_at=CASE WHEN retention_floor_at IS NULL OR retention_floor_at<$1 THEN $1 ELSE retention_floor_at END,effective_unref_seconds=GREATEST(effective_unref_seconds,$2),single_binding=GREATEST(single_binding,$3) WHERE blob_key=$4",vec![floor.into(),signed(change.policy.unreferenced)?,Value::I64(i64::from(change.policy.sharing==Sharing::SingleBinding)),next.as_str().into()]).await?;
            }
        }
        for key in prepared.objects {
            if self.binding_count(sql, &key).await? > 0 {
                execute(sql,"UPDATE teaql_blob_object SET state='available',first_bound_at=COALESCE(first_bound_at,$1),unreferenced_at=NULL WHERE blob_key=$2",vec![now.into(),key.as_str().into()]).await?;
            } else {
                execute(sql,"UPDATE teaql_blob_object SET unreferenced_at=COALESCE(unreferenced_at,$1) WHERE blob_key=$2 AND state='available' AND scope_realm=$3 AND scope_boundary=$4",vec![now.into(),key.as_str().into(),prepared.scope.realm().into(),prepared.scope.boundary().into()]).await?;
            }
        }
        Ok(())
    }
    pub async fn claim_deletion(
        &self,
        scope: &BlobScope,
        key: &BlobRef,
        now: i64,
    ) -> BlobResult<Option<BlobCatalogRecord>> {
        let catalog = self.clone();
        let scope = scope.clone();
        let key = key.clone();
        self.transaction(move |sql| {
            Box::pin(async move {
                catalog.validate_schema_on(sql).await?;
                let blob = catalog.lock_blob(sql, &scope, &key).await?;
                if !BlobRetention::collectable(
                    blob.state,
                    catalog.binding_count(sql, &key).await?,
                    now,
                    blob.eligible_at()?,
                ) {
                    return Ok(None);
                }
                execute(
                    sql,
                    "UPDATE teaql_blob_object SET state='deleting' WHERE blob_key=$1",
                    vec![key.as_str().into()],
                )
                .await?;
                Ok(Some(catalog.lock_blob(sql, &scope, &key).await?))
            })
        })
        .await
    }
    /// Trusted maintenance acknowledgement after successful version-conditional physical deletion.
    pub async fn acknowledge_deleted(
        &self,
        scope: &BlobScope,
        key: &BlobRef,
        content_version: Option<String>,
        now: i64,
    ) -> BlobResult<()> {
        let catalog = self.clone();
        let scope = scope.clone();
        let key = key.clone();
        self.transaction(move |sql| {
            Box::pin(async move {
                catalog.validate_schema_on(sql).await?;
                let blob = catalog.lock_blob(sql, &scope, &key).await?;
                if blob.state == BlobState::Deleted && blob.content_version == content_version {
                    return Ok(());
                }
                if blob.state != BlobState::Deleting
                    || blob.content_version != content_version
                    || catalog.binding_count(sql, &key).await? != 0
                {
                    return Err(Code::IdempotencyConflict.into());
                }
                execute(
                    sql,
                    "UPDATE teaql_blob_object SET state='deleted',deleted_at=$1 WHERE blob_key=$2",
                    vec![now.into(), key.as_str().into()],
                )
                .await?;
                Ok(())
            })
        })
        .await
    }
    async fn lock_blob(
        &self,
        sql: &dyn InfrastructureSql,
        scope: &BlobScope,
        key: &BlobRef,
    ) -> BlobResult<BlobCatalogRecord> {
        self.require_active(sql)?;
        let rows=fetch(sql,"SELECT * FROM teaql_blob_object WHERE blob_key=$1 AND scope_realm=$2 AND scope_boundary=$3 FOR UPDATE",vec![key.as_str().into(),scope.realm().into(),scope.boundary().into()]).await?;
        if rows.is_empty() {
            return Err(Code::BindDenied.into());
        }
        record(one(rows)?)
    }
    async fn lock_binding(
        &self,
        sql: &dyn InfrastructureSql,
        scope: &BlobScope,
        target: &BlobFieldTarget,
    ) -> BlobResult<BlobBinding> {
        self.require_active(sql)?;
        execute(sql,"INSERT INTO teaql_blob_binding (scope_realm,scope_boundary,entity_type,entity_id,field_name,binding_version) VALUES ($1,$2,$3,$4,$5,0) ON CONFLICT (scope_realm,scope_boundary,entity_type,entity_id,field_name) DO NOTHING",target_args(scope,target)?).await?;
        binding(one(fetch(sql,"SELECT * FROM teaql_blob_binding WHERE scope_realm=$1 AND scope_boundary=$2 AND entity_type=$3 AND entity_id=$4 AND field_name=$5 FOR UPDATE",target_args(scope,target)?).await?)?)
    }
    async fn binding_count(&self, sql: &dyn InfrastructureSql, key: &BlobRef) -> BlobResult<i64> {
        integer(
            &one(fetch(
                sql,
                "SELECT count(*) AS binding_count FROM teaql_blob_binding WHERE blob_key=$1",
                vec![key.as_str().into()],
            )
            .await?)?,
            "binding_count",
        )
    }
}
fn migration_digest() -> String {
    format!("{:x}", Sha256::digest(MIGRATION.as_bytes()))
}
async fn fetch(
    sql: &dyn InfrastructureSql,
    statement: &str,
    params: Vec<Value>,
) -> BlobResult<Vec<CompactRow>> {
    Ok(sql
        .fetch(&InfrastructureStatement::new(statement, params)?)
        .await?)
}
async fn execute(
    sql: &dyn InfrastructureSql,
    statement: &str,
    params: Vec<Value>,
) -> BlobResult<u64> {
    Ok(sql
        .execute(&InfrastructureStatement::new(statement, params)?)
        .await?)
}
fn digest(value: &str) -> BlobResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Err(Code::ReferenceInvalid.into())
    } else {
        Ok(())
    }
}
fn signed(value: u64) -> BlobResult<Value> {
    i64::try_from(value)
        .map(Value::I64)
        .map_err(|_| Code::ReferenceInvalid.into())
}
fn optional_id(value: Option<u64>) -> Value {
    value.map_or(Value::Null, Value::U64)
}
fn optional_key(value: Option<&BlobRef>) -> Value {
    value.map_or(Value::Null, |value| value.as_str().into())
}
fn target_args(scope: &BlobScope, target: &BlobFieldTarget) -> BlobResult<Vec<Value>> {
    Ok(vec![
        scope.realm().into(),
        scope.boundary().into(),
        target.entity().into(),
        signed(target.id().ok_or(Code::TransactionRequired)?)?,
        target.field().into(),
    ])
}
fn one(mut rows: Vec<CompactRow>) -> BlobResult<CompactRow> {
    if rows.len() != 1 {
        Err(Code::CatalogFailed.into())
    } else {
        rows.pop().ok_or_else(|| Code::CatalogFailed.into())
    }
}
fn optional_record(mut rows: Vec<CompactRow>) -> BlobResult<Option<BlobCatalogRecord>> {
    if rows.len() > 1 {
        return Err(Code::CatalogFailed.into());
    }
    rows.pop().map(record).transpose()
}
fn text<'a>(row: &'a CompactRow, name: &str) -> BlobResult<&'a str> {
    match row.get(name) {
        Some(Value::Text(value)) => Ok(value),
        _ => Err(Code::CatalogFailed.into()),
    }
}
fn optional_text(row: &CompactRow, name: &str) -> BlobResult<Option<String>> {
    match row.get(name) {
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(Value::Null) => Ok(None),
        _ => Err(Code::CatalogFailed.into()),
    }
}
fn integer(row: &CompactRow, name: &str) -> BlobResult<i64> {
    row.get(name)
        .and_then(Value::try_i64)
        .ok_or_else(|| Code::CatalogFailed.into())
}
fn optional_integer(row: &CompactRow, name: &str) -> BlobResult<Option<i64>> {
    match row.get(name) {
        Some(Value::Null) => Ok(None),
        Some(value) => value
            .try_i64()
            .map(Some)
            .ok_or_else(|| Code::CatalogFailed.into()),
        _ => Err(Code::CatalogFailed.into()),
    }
}
fn reference(value: Option<String>) -> BlobResult<Option<BlobRef>> {
    value
        .map(|value| BlobRef::new(value).map_err(|_| Code::CatalogFailed.into()))
        .transpose()
}
fn binding(row: CompactRow) -> BlobResult<BlobBinding> {
    let id = integer(&row, "entity_id")?;
    let version = integer(&row, "binding_version")?;
    if id <= 0 || version < 0 {
        return Err(Code::CatalogFailed.into());
    }
    Ok(BlobBinding {
        scope: BlobScope::new(text(&row, "scope_realm")?, text(&row, "scope_boundary")?)?,
        target: BlobFieldTarget::new(
            text(&row, "entity_type")?,
            Some(id as u64),
            text(&row, "field_name")?,
        )?,
        reference: reference(optional_text(&row, "blob_key")?)?,
        retired_reference: reference(optional_text(&row, "retired_key")?)?,
        version,
        bound_at: optional_integer(&row, "bound_at")?,
        retain_until: optional_integer(&row, "retain_until")?,
    })
}
fn record(row: CompactRow) -> BlobResult<BlobCatalogRecord> {
    let size = optional_integer(&row, "size_bytes")?;
    let target_id = optional_integer(&row, "target_id")?;
    let effective = integer(&row, "effective_unref_seconds")?;
    if size.is_some_and(|value| value < 0)
        || target_id.is_some_and(|value| value <= 0)
        || effective < 0
    {
        return Err(Code::CatalogFailed.into());
    }
    let single = integer(&row, "single_binding")?;
    if !(0..=1).contains(&single) {
        return Err(Code::CatalogFailed.into());
    }
    let blob = BlobCatalogRecord {
        scope: BlobScope::new(text(&row, "scope_realm")?, text(&row, "scope_boundary")?)?,
        reference: BlobRef::new(text(&row, "blob_key")?).map_err(|_| Code::CatalogFailed)?,
        state: BlobState::parse(text(&row, "state")?)?,
        upload_id: text(&row, "upload_id")?.into(),
        original_name: text(&row, "original_name")?.into(),
        extension: text(&row, "extension")?.into(),
        actual_mime: optional_text(&row, "actual_mime")?,
        size_bytes: size.map(|size| size as u64),
        sha256: optional_text(&row, "sha256")?,
        content_version: optional_text(&row, "content_version")?,
        storage_location: text(&row, "storage_location")?.into(),
        target: BlobFieldTarget::new(
            text(&row, "target_entity")?,
            target_id.map(|value| value as u64),
            text(&row, "target_field")?,
        )?,
        policy: BlobPolicySnapshot::from_json(text(&row, "policy_json")?)?,
        created_at: integer(&row, "created_at")?,
        upload_expires_at: integer(&row, "upload_expires_at")?,
        completed_at: optional_integer(&row, "completed_at")?,
        first_bound_at: optional_integer(&row, "first_bound_at")?,
        unreferenced_at: optional_integer(&row, "unreferenced_at")?,
        retention_floor_at: optional_integer(&row, "retention_floor_at")?,
        effective_unref_seconds: effective as u64,
        single_binding: single == 1,
        deleted_at: optional_integer(&row, "deleted_at")?,
    };
    if !blob.policy.extensions.contains(&blob.extension)
        || blob.target.entity() != blob.policy.entity
        || blob.target.field() != blob.policy.field
        || blob.effective_unref_seconds < blob.policy.unreferenced
        || blob.policy.sharing == Sharing::SingleBinding && !blob.single_binding
    {
        return Err(Code::CatalogFailed.into());
    }
    let verified = matches!(
        blob.state,
        BlobState::UploadedUnbound | BlobState::Available
    ) || blob.completed_at.is_some();
    if !verified {
        let written =
            blob.size_bytes.is_some() || blob.sha256.is_some() || blob.content_version.is_some();
        if blob.actual_mime.is_some()
            || written
                && (blob.size_bytes.is_none_or(|size| {
                    size > blob.policy.max_size || size == 0 && !blob.policy.allow_empty
                }) || blob
                    .sha256
                    .as_deref()
                    .is_none_or(|sha| digest(sha).is_err())
                    || blob
                        .content_version
                        .as_deref()
                        .is_none_or(|version| name(version, 1024).is_err()))
        {
            return Err(Code::CatalogFailed.into());
        }
    }
    if verified
        && (blob.completed_at.is_none()
            || blob.actual_mime.as_deref() != content_type(&blob.extension)
            || blob.size_bytes.is_none_or(|size| {
                size > blob.policy.max_size || size == 0 && !blob.policy.allow_empty
            })
            || blob
                .sha256
                .as_deref()
                .is_none_or(|sha| digest(sha).is_err())
            || blob
                .content_version
                .as_deref()
                .is_none_or(|version| name(version, 1024).is_err()))
    {
        return Err(Code::CatalogFailed.into());
    }
    if blob.state == BlobState::Available
        && (blob.first_bound_at.is_none() || blob.retention_floor_at.is_none())
    {
        return Err(Code::CatalogFailed.into());
    }
    Ok(blob)
}
