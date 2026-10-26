//! Approved mutation intent to transaction-owned Blob binding changes.
//! Planning performs no SQL, upload, signing or content I/O.

use std::{collections::BTreeMap, sync::Arc};

use teaql_core::{BlobRef, DataType, Value};
use teaql_data_service::infrastructure_sql::InfrastructureSql;
use teaql_runtime::{
    BlobMutationFuture, BlobSelectionTarget, BlobSelectionTargetKind, BoundDocumentSnapshot,
    DocumentMutationKind, DocumentRelationCompleteness, DocumentRoundTripError, EntityKey,
    MutationOperationKind, MutationPlan, RuntimeBlobMutationParticipant, RuntimeBlobMutationPlan,
    RuntimeError, RuntimePreparedBlobMutation, UserContext,
};

use crate::{
    BindingChange, BindingChangeKind, BlobErrorCode, BlobFieldTarget, BlobPolicies,
    BlobPolicySnapshot, BlobResult, BlobUploadClock, BlobUploadIdentity, PostgresBlobCatalog,
    PreparedBlobBindings,
};

/// The host must verify its entity/field permission and the Blob-specific
/// operation on the supplied current context. There is no permissive default.
pub trait BlobBindAuthorization: Send + Sync {
    fn identity(&self, context: &UserContext) -> BlobResult<BlobUploadIdentity>;
    /// Explicitly confirm that the document principal and Blob identity are
    /// the same current actor. Existing non-document participants are unchanged.
    fn authorize_document_actor(
        &self,
        _context: &UserContext,
        _principal: &teaql_runtime::TrustedReferencePrincipal,
        _identity: &BlobUploadIdentity,
    ) -> BlobResult<bool> {
        Ok(false)
    }
    fn authorize_bind(
        &self,
        context: &UserContext,
        target: &BlobFieldTarget,
        action: BlobBindAction,
        next: Option<&BlobRef>,
    ) -> BlobResult<bool>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobBindAction {
    Create,
    Replace,
    Clear,
    Delete,
    Recover,
}

struct PlannedChange {
    entity: EntityKey,
    target: BlobFieldTarget,
    next: Option<BlobRef>,
    policy: BlobPolicySnapshot,
    kind: BindingChangeKind,
    action: BlobBindAction,
    issued: Option<IssuedBinding>,
}

struct IssuedBinding {
    reference: Option<BlobRef>,
    version: u64,
}

/// An application-selected credential and its expected target from the submitted patch.
/// The target must be built from the current command, not from the credential itself.
pub struct DocumentBlobSelectionInput<'a> {
    pub token: &'a str,
    pub target: BlobSelectionTarget,
}

pub struct DocumentBlobMutationRequest<'a> {
    pub document_token: &'a str,
    pub aggregate_type: &'a str,
    pub purpose: &'a str,
    pub command_id: &'a str,
    pub selections: &'a [DocumentBlobSelectionInput<'a>],
}

struct DocumentSelectionAuthority {
    document_token: String,
    document: BoundDocumentSnapshot,
    aggregate_type: String,
    purpose: String,
    selections: Vec<(BlobSelectionTarget, String, BlobRef)>,
}

/// Frozen plan for one already-reviewed business mutation. An unresolved
/// create ID must be supplied by the save executor before any business DML.
pub struct BlobMutationPlan {
    participant: Arc<()>,
    identity: BlobUploadIdentity,
    changes: Vec<PlannedChange>,
    document: Option<DocumentSelectionAuthority>,
}

impl BlobMutationPlan {
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

pub struct PreparedBlobMutation {
    participant: Arc<()>,
    identity: BlobUploadIdentity,
    changes: Vec<(BlobFieldTarget, BlobBindAction, Option<BlobRef>)>,
    bindings: PreparedBlobBindings,
    document: Option<DocumentSelectionAuthority>,
}

impl std::fmt::Debug for PreparedBlobMutation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PreparedBlobMutation([redacted])")
    }
}

#[derive(Clone)]
pub struct BlobMutationParticipant {
    instance: Arc<()>,
    pub(crate) catalog: PostgresBlobCatalog,
    pub(crate) policies: Arc<BlobPolicies>,
    authorization: Arc<dyn BlobBindAuthorization>,
    clock: Arc<dyn BlobUploadClock>,
}

impl BlobMutationParticipant {
    pub fn new(
        catalog: PostgresBlobCatalog,
        policies: Arc<BlobPolicies>,
        authorization: Arc<dyn BlobBindAuthorization>,
        clock: Arc<dyn BlobUploadClock>,
    ) -> Self {
        Self {
            instance: Arc::new(()),
            catalog,
            policies,
            authorization,
            clock,
        }
    }

    /// Call only after the host's MutationPolicy approval. A caller must not
    /// derive binding intent from an unreviewed client payload.
    pub fn plan(
        &self,
        context: &UserContext,
        approved: &MutationPlan,
    ) -> BlobResult<Option<BlobMutationPlan>> {
        let mut changes = Vec::new();
        let mut seen_modeled = false;
        for operation in &approved.operations {
            let descriptor = context
                .entity(&operation.entity.entity)
                .ok_or(BlobErrorCode::PolicyInvalid)?;
            for property in &descriptor.properties {
                if property.data_type != DataType::BlobRef && property.blob_field_spec.is_none() {
                    continue;
                }
                let spec = property
                    .blob_field_spec
                    .as_ref()
                    .ok_or(BlobErrorCode::PolicyInvalid)?;
                if property.data_type != DataType::BlobRef
                    || spec.entity() != descriptor.name
                    || spec.field() != property.name
                    || spec.nullable() != property.nullable
                {
                    return Err(BlobErrorCode::PolicyInvalid.into());
                }
                let value = operation.changed_values.get(&property.name);
                if operation.kind == MutationOperationKind::Update && value.is_none() {
                    continue;
                }
                seen_modeled = true;
                if operation.kind == MutationOperationKind::Create
                    && value.is_none()
                    && spec.nullable()
                {
                    continue;
                }
                let (next, kind, action) = match operation.kind {
                    MutationOperationKind::Delete => (
                        None,
                        BindingChangeKind::EntityDelete,
                        BlobBindAction::Delete,
                    ),
                    MutationOperationKind::Create => (
                        reference(value)?,
                        BindingChangeKind::FieldWrite,
                        BlobBindAction::Create,
                    ),
                    MutationOperationKind::Update => {
                        let next = reference(value)?;
                        let action = if next.is_some() {
                            BlobBindAction::Replace
                        } else {
                            BlobBindAction::Clear
                        };
                        (next, BindingChangeKind::FieldWrite, action)
                    }
                    MutationOperationKind::Recover => (
                        reference(value)?,
                        BindingChangeKind::FieldWrite,
                        BlobBindAction::Recover,
                    ),
                };
                let policy = self.policies.resolve(spec)?;
                if next.is_none() && !policy.nullable && kind != BindingChangeKind::EntityDelete {
                    return Err(BlobErrorCode::ReferenceInvalid.into());
                }
                let id = operation.entity.id.try_u64().filter(|id| *id != 0);
                if id.is_none() && operation.kind != MutationOperationKind::Create {
                    return Err(BlobErrorCode::TransactionRequired.into());
                }
                changes.push(PlannedChange {
                    entity: operation.entity.clone(),
                    target: BlobFieldTarget::new(&descriptor.name, id, &property.name)?,
                    next,
                    policy,
                    kind,
                    action,
                    issued: None,
                });
            }
        }
        if !seen_modeled {
            return Ok(None);
        }
        let identity = self.authorization.identity(context)?;
        for change in &changes {
            self.require_authorized(context, change, &identity)?;
        }
        Ok(Some(BlobMutationPlan {
            participant: self.instance.clone(),
            identity,
            changes,
            document: None,
        }))
    }

    /// Bind every changed Blob value in an approved document mutation to its
    /// tqs1 credential. A null clear is authorized by the tqd2 binding instead.
    /// This is planning only: the physical owner rechecks the tokens and locks
    /// current catalog bindings during `prepare` before business DML.
    pub fn plan_document_selections(
        &self,
        context: &UserContext,
        approved: &MutationPlan,
        request: DocumentBlobMutationRequest<'_>,
    ) -> BlobResult<Option<BlobMutationPlan>> {
        let DocumentBlobMutationRequest {
            document_token,
            aggregate_type,
            purpose,
            command_id,
            selections,
        } = request;
        if command_id.trim().is_empty()
            || approved.execution_id != command_id
            || approved.root_entity_type != aggregate_type
        {
            return Err(BlobErrorCode::BindDenied.into());
        }
        let verified = context
            .consume_bound_document_snapshot(document_token, aggregate_type, purpose)
            .map_err(document_error)?;
        let document = verified.document;
        let mut selected = BTreeMap::new();
        let mut credentials = Vec::with_capacity(selections.len());
        for input in selections {
            if input.target.command_id != command_id
                || input.target.scope != document.snapshot.scope
            {
                return Err(BlobErrorCode::BindDenied.into());
            }
            let claim = context
                .consume_blob_selection(document_token, input.token, &input.target)
                .map_err(document_error)?
                .claim;
            let key = selection_key(&document, &claim.target)?;
            credentials.push((
                claim.target.clone(),
                input.token.to_owned(),
                claim.reference.clone(),
            ));
            if selected
                .insert(key, (claim.target, claim.reference))
                .is_some()
            {
                return Err(BlobErrorCode::BindDenied.into());
            }
        }
        let Some(mut plan) = self.plan(context, approved)? else {
            return if selected.is_empty() {
                Ok(None)
            } else {
                Err(BlobErrorCode::BindDenied.into())
            };
        };
        self.require_document_actor(context, &plan.identity)?;
        for change in &mut plan.changes {
            let key = (change.entity.clone(), change.target.field().to_owned());
            if change.action != BlobBindAction::Create {
                let mut rows = document.snapshot.entities.iter().filter(|row| {
                    row.identity.entity_type == change.entity.entity
                        && Some(row.identity.id) == change.entity.id.try_u64()
                });
                let row = rows.next().ok_or(BlobErrorCode::BindDenied)?;
                let operation = approved
                    .operations
                    .iter()
                    .find(|operation| operation.entity == change.entity)
                    .ok_or(BlobErrorCode::BindDenied)?;
                if rows.next().is_some() || operation.original_version != Some(row.identity.version)
                {
                    return Err(BlobErrorCode::BindDenied.into());
                }
            }
            match (change.action, change.next.as_ref()) {
                (BlobBindAction::Create | BlobBindAction::Replace, Some(next)) => {
                    let Some((target, selected_ref)) = selected.remove(&key) else {
                        return Err(BlobErrorCode::BindDenied.into());
                    };
                    if &selected_ref != next
                        || target.target_entity != change.target.entity()
                        || (change.action == BlobBindAction::Create)
                            != (target.kind == BlobSelectionTargetKind::NewClientKey)
                    {
                        return Err(BlobErrorCode::BindDenied.into());
                    }
                    if target.kind == BlobSelectionTargetKind::ExistingRow {
                        change.issued = Some(issued_binding(
                            &document,
                            &target.target_key,
                            &target.canonical_field,
                            true,
                        )?);
                    }
                }
                (BlobBindAction::Clear, None) => {
                    let row_key =
                        existing_row_key(&document, &change.entity, change.target.field(), false)?;
                    let binding = issued_binding(&document, &row_key, change.target.field(), true)?;
                    if !document.bindings.iter().any(|candidate| {
                        candidate.row_key == row_key
                            && candidate.canonical_field == change.target.field()
                            && candidate.nullable
                    }) {
                        return Err(BlobErrorCode::BindDenied.into());
                    }
                    change.issued = Some(binding);
                }
                (BlobBindAction::Delete, None) => {
                    let row_key =
                        existing_row_key(&document, &change.entity, change.target.field(), true)?;
                    if !document.snapshot.relations.iter().any(|relation| {
                        relation.completeness == DocumentRelationCompleteness::Complete
                            && relation.writable
                            && relation.row_keys.contains(&row_key)
                            && relation
                                .allowed_mutations
                                .contains(&DocumentMutationKind::RemoveChild)
                    }) {
                        return Err(BlobErrorCode::BindDenied.into());
                    }
                    change.issued = Some(issued_binding(
                        &document,
                        &row_key,
                        change.target.field(),
                        false,
                    )?);
                }
                (BlobBindAction::Create, None) => {
                    if !matches!(change.entity.id, Value::Text(_))
                        || !document.snapshot.relations.iter().any(|relation| {
                            relation.completeness == DocumentRelationCompleteness::Complete
                                && relation.writable
                                && relation
                                    .allowed_mutations
                                    .contains(&DocumentMutationKind::CreateChild)
                        })
                    {
                        return Err(BlobErrorCode::BindDenied.into());
                    }
                }
                _ => return Err(BlobErrorCode::BindDenied.into()),
            }
        }
        if !selected.is_empty() {
            return Err(BlobErrorCode::BindDenied.into());
        }
        plan.document = Some(DocumentSelectionAuthority {
            document_token: document_token.to_owned(),
            aggregate_type: aggregate_type.to_owned(),
            purpose: purpose.to_owned(),
            document,
            selections: credentials,
        });
        Ok(Some(plan))
    }

    fn require_authorized(
        &self,
        context: &UserContext,
        change: &PlannedChange,
        identity: &BlobUploadIdentity,
    ) -> BlobResult<()> {
        if &self.authorization.identity(context)? != identity
            || !self.authorization.authorize_bind(
                context,
                &change.target,
                change.action,
                change.next.as_ref(),
            )?
        {
            return Err(BlobErrorCode::BindDenied.into());
        }
        Ok(())
    }

    fn require_document_actor(
        &self,
        context: &UserContext,
        identity: &BlobUploadIdentity,
    ) -> BlobResult<()> {
        let principal = context
            .trusted_reference_principal()
            .map_err(|_| BlobErrorCode::BindDenied)?;
        if self
            .authorization
            .authorize_document_actor(context, principal, identity)?
        {
            Ok(())
        } else {
            Err(BlobErrorCode::BindDenied.into())
        }
    }

    /// Resolve generated IDs and lock authoritative old bindings on the
    /// business connection. This must precede its first business DML.
    pub async fn prepare(
        &self,
        context: &UserContext,
        sql: &dyn InfrastructureSql,
        plan: BlobMutationPlan,
        resolved_ids: &BTreeMap<EntityKey, Value>,
    ) -> BlobResult<PreparedBlobMutation> {
        let result = self.prepare_inner(context, sql, plan, resolved_ids).await;
        if result.is_err() {
            sql.mark_rollback_only()?;
        }
        result
    }

    async fn prepare_inner(
        &self,
        context: &UserContext,
        sql: &dyn InfrastructureSql,
        plan: BlobMutationPlan,
        resolved_ids: &BTreeMap<EntityKey, Value>,
    ) -> BlobResult<PreparedBlobMutation> {
        if !Arc::ptr_eq(&self.instance, &plan.participant) {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        if let Some(authority) = &plan.document {
            validate_document_authority(context, authority)?;
        }
        let current = self.authorization.identity(context)?;
        if plan.document.is_some() {
            self.require_document_actor(context, &current)?;
        }
        if current != plan.identity {
            return Err(BlobErrorCode::BindDenied.into());
        }
        let now = self.clock.now_millis()?;
        let mut ordered = BTreeMap::new();
        for mut change in plan.changes {
            let id = resolved_ids
                .get(&change.entity)
                .unwrap_or(&change.entity.id)
                .try_u64()
                .filter(|id| *id != 0)
                .ok_or(BlobErrorCode::TransactionRequired)?;
            change.target =
                BlobFieldTarget::new(change.target.entity(), Some(id), change.target.field())?;
            self.require_authorized(context, &change, &current)?;
            if ordered.insert(change.target.clone(), change).is_some() {
                return Err(BlobErrorCode::IdempotencyConflict.into());
            }
        }
        let mut catalog_changes = Vec::with_capacity(ordered.len());
        let mut authorization_checks = Vec::with_capacity(ordered.len());
        for change in ordered.into_values() {
            let old = self
                .catalog
                .binding_for_update(sql, current.scope(), &change.target)
                .await?;
            if change.issued.as_ref().is_some_and(|issued| {
                u64::try_from(old.version).ok() != Some(issued.version)
                    || old.reference != issued.reference
            }) {
                return Err(BlobErrorCode::IdempotencyConflict.into());
            }
            authorization_checks.push((change.target.clone(), change.action, change.next.clone()));
            catalog_changes.push(BindingChange {
                target: change.target,
                previous: old.reference,
                next: change.next,
                expected_binding_version: Some(old.version),
                policy: change.policy,
                kind: change.kind,
            });
        }
        let bindings = self
            .catalog
            .prepare_bindings(sql, current.scope(), catalog_changes, now)
            .await?;
        Ok(PreparedBlobMutation {
            participant: self.instance.clone(),
            identity: current,
            changes: authorization_checks,
            bindings,
            document: plan.document,
        })
    }

    /// Apply after successful business DML and before authoritative readback
    /// and COMMIT. Any error vetoes the physical owner's later COMMIT.
    pub async fn apply(
        &self,
        context: &UserContext,
        sql: &dyn InfrastructureSql,
        prepared: PreparedBlobMutation,
    ) -> BlobResult<()> {
        let result = self.apply_inner(context, sql, prepared).await;
        if result.is_err() {
            sql.mark_rollback_only()?;
        }
        result
    }

    async fn apply_inner(
        &self,
        context: &UserContext,
        sql: &dyn InfrastructureSql,
        prepared: PreparedBlobMutation,
    ) -> BlobResult<()> {
        if !Arc::ptr_eq(&self.instance, &prepared.participant) {
            return Err(BlobErrorCode::PolicyInvalid.into());
        }
        if let Some(authority) = &prepared.document {
            validate_document_authority(context, authority)?;
        }
        if self.authorization.identity(context)? != prepared.identity {
            return Err(BlobErrorCode::BindDenied.into());
        }
        if prepared.document.is_some() {
            self.require_document_actor(context, &prepared.identity)?;
        }
        for (target, action, next) in &prepared.changes {
            if !self
                .authorization
                .authorize_bind(context, target, *action, next.as_ref())?
            {
                return Err(BlobErrorCode::BindDenied.into());
            }
        }
        self.catalog
            .apply_bindings(sql, prepared.bindings, self.clock.now_millis()?)
            .await
    }
}

fn validate_document_authority(
    context: &UserContext,
    authority: &DocumentSelectionAuthority,
) -> BlobResult<()> {
    let current = context
        .consume_bound_document_snapshot(
            &authority.document_token,
            &authority.aggregate_type,
            &authority.purpose,
        )
        .map_err(document_error)?;
    if current.document != authority.document {
        return Err(BlobErrorCode::BindDenied.into());
    }
    for (target, token, reference) in &authority.selections {
        let current = context
            .consume_blob_selection(&authority.document_token, token, target)
            .map_err(document_error)?;
        if &current.claim.reference != reference {
            return Err(BlobErrorCode::BindDenied.into());
        }
    }
    Ok(())
}

fn document_error(error: DocumentRoundTripError) -> crate::BlobError {
    match error.code() {
        "ROUND_TRIP_REFERENCE_EXPIRED" => BlobErrorCode::ReferenceExpired,
        "ROUND_TRIP_REFERENCE_CONFIGURATION_INVALID" => BlobErrorCode::CapabilityMissing,
        "ROUND_TRIP_REFERENCE_SCOPE_MISMATCH" | "ROUND_TRIP_REFERENCE_AUTHORIZATION_REQUIRED" => {
            BlobErrorCode::BindDenied
        }
        _ => BlobErrorCode::ReferenceInvalid,
    }
    .into()
}

fn selection_key(
    document: &BoundDocumentSnapshot,
    target: &BlobSelectionTarget,
) -> BlobResult<(EntityKey, String)> {
    if document.snapshot.scope != target.scope {
        return Err(BlobErrorCode::BindDenied.into());
    }
    let entity = match target.kind {
        BlobSelectionTargetKind::ExistingRow => {
            let row = document
                .snapshot
                .entity(&target.target_key)
                .ok_or(BlobErrorCode::BindDenied)?;
            if row.identity.entity_type != target.target_entity
                || !row.writable_fields.contains(&target.canonical_field)
                || !row
                    .allowed_mutations
                    .contains(&DocumentMutationKind::Update)
            {
                return Err(BlobErrorCode::BindDenied.into());
            }
            EntityKey::new(target.target_entity.clone(), row.identity.id)
        }
        BlobSelectionTargetKind::NewClientKey => {
            if !document.snapshot.relations.iter().any(|relation| {
                relation.completeness == DocumentRelationCompleteness::Complete
                    && relation.writable
                    && relation
                        .allowed_mutations
                        .contains(&DocumentMutationKind::CreateChild)
            }) {
                return Err(BlobErrorCode::BindDenied.into());
            }
            EntityKey::new(
                target.target_entity.clone(),
                Value::Text(target.target_key.clone()),
            )
        }
    };
    Ok((entity, target.canonical_field.clone()))
}

fn existing_row_key(
    document: &BoundDocumentSnapshot,
    entity: &EntityKey,
    field: &str,
    removing: bool,
) -> BlobResult<String> {
    let mut rows = document.snapshot.entities.iter().filter(|row| {
        row.identity.entity_type == entity.entity
            && Some(row.identity.id) == entity.id.try_u64()
            && (removing && row.loaded_fields.contains(field)
                || !removing && row.writable_fields.contains(field))
            && row.allowed_mutations.contains(&if removing {
                DocumentMutationKind::RemoveChild
            } else {
                DocumentMutationKind::Update
            })
    });
    let row = rows.next().ok_or(BlobErrorCode::BindDenied)?;
    if rows.next().is_some() {
        return Err(BlobErrorCode::BindDenied.into());
    }
    Ok(row.row_key.clone())
}

fn issued_binding(
    document: &BoundDocumentSnapshot,
    row_key: &str,
    field: &str,
    require_writable: bool,
) -> BlobResult<IssuedBinding> {
    let binding = document
        .bindings
        .iter()
        .find(|binding| {
            binding.row_key == row_key
                && binding.canonical_field == field
                && (!require_writable || binding.writable)
        })
        .ok_or(BlobErrorCode::BindDenied)?;
    Ok(IssuedBinding {
        reference: binding.reference.clone(),
        version: binding.binding_version,
    })
}

fn reference(value: Option<&Value>) -> BlobResult<Option<BlobRef>> {
    match value {
        None | Some(Value::Null | Value::TypedNull(DataType::BlobRef)) => Ok(None),
        Some(Value::BlobRef(reference)) => Ok(Some(reference.clone())),
        _ => Err(BlobErrorCode::ReferenceInvalid.into()),
    }
}

struct RuntimePlan {
    participant: BlobMutationParticipant,
    plan: BlobMutationPlan,
}

struct RuntimePrepared {
    participant: BlobMutationParticipant,
    prepared: PreparedBlobMutation,
}

impl RuntimeBlobMutationParticipant for BlobMutationParticipant {
    fn plan(
        &self,
        context: &UserContext,
        approved: &MutationPlan,
    ) -> Result<Option<Box<dyn RuntimeBlobMutationPlan>>, RuntimeError> {
        BlobMutationParticipant::plan(self, context, approved)
            .map(|plan| {
                plan.map(|plan| {
                    Box::new(RuntimePlan {
                        participant: self.clone(),
                        plan,
                    }) as Box<dyn RuntimeBlobMutationPlan>
                })
            })
            .map_err(|error| RuntimeError::Policy(error.to_string()))
    }
}

impl RuntimeBlobMutationPlan for RuntimePlan {
    fn prepare<'a>(
        self: Box<Self>,
        context: &'a UserContext,
        sql: &'a dyn InfrastructureSql,
        resolved_ids: &'a BTreeMap<EntityKey, Value>,
    ) -> BlobMutationFuture<'a, Box<dyn RuntimePreparedBlobMutation>> {
        Box::pin(async move {
            let prepared = self
                .participant
                .prepare(context, sql, self.plan, resolved_ids)
                .await
                .map_err(|error| RuntimeError::Transaction(error.to_string()))?;
            Ok(Box::new(RuntimePrepared {
                participant: self.participant,
                prepared,
            }) as Box<dyn RuntimePreparedBlobMutation>)
        })
    }
}

impl RuntimePreparedBlobMutation for RuntimePrepared {
    fn apply<'a>(
        self: Box<Self>,
        context: &'a UserContext,
        sql: &'a dyn InfrastructureSql,
    ) -> BlobMutationFuture<'a, ()> {
        Box::pin(async move {
            self.participant
                .apply(context, sql, self.prepared)
                .await
                .map_err(|error| RuntimeError::Transaction(error.to_string()))
        })
    }
}
