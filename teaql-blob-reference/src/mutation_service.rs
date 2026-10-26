//! Approved mutation intent to transaction-owned Blob binding changes.
//! Planning performs no SQL, upload, signing or content I/O.

use std::{collections::BTreeMap, sync::Arc};

use teaql_core::{BlobRef, DataType, Value};
use teaql_data_service::infrastructure_sql::InfrastructureSql;
use teaql_runtime::{
    BlobMutationFuture, EntityKey, MutationOperationKind, MutationPlan,
    RuntimeBlobMutationParticipant, RuntimeBlobMutationPlan, RuntimeError,
    RuntimePreparedBlobMutation, UserContext,
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
}

/// Frozen plan for one already-reviewed business mutation. An unresolved
/// create ID must be supplied by the save executor before any business DML.
pub struct BlobMutationPlan {
    participant: Arc<()>,
    identity: BlobUploadIdentity,
    changes: Vec<PlannedChange>,
}

impl BlobMutationPlan {
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

#[derive(Debug)]
pub struct PreparedBlobMutation {
    participant: Arc<()>,
    identity: BlobUploadIdentity,
    changes: Vec<(BlobFieldTarget, BlobBindAction, Option<BlobRef>)>,
    bindings: PreparedBlobBindings,
}

#[derive(Clone)]
pub struct BlobMutationParticipant {
    instance: Arc<()>,
    catalog: PostgresBlobCatalog,
    policies: Arc<BlobPolicies>,
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
        }))
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
        let current = self.authorization.identity(context)?;
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
        if self.authorization.identity(context)? != prepared.identity {
            return Err(BlobErrorCode::BindDenied.into());
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
