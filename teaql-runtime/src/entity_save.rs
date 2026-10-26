#![allow(clippy::items_after_test_module)] // Save-contract tests intentionally sit near the API.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use teaql_core::{Entity, MutationValues, Value};

use crate::ledger_keys::{
    canonical_field_from_storage, canonical_values_from_storage, storage_fields_from_ledger,
    storage_values_from_ledger,
};
use crate::{
    DataServiceError, GraphNode, GraphOperation, ObjectLocation, RuntimeError, UserContext,
};

tokio::task_local! {
    static GRAPH_FIX_TIME: teaql_core::time::Timestamp;
    static GRAPH_FIX_EVIDENCE: Arc<std::sync::Mutex<Vec<crate::FixEvidence>>>;
}

pub(crate) fn current_graph_fix_time() -> teaql_core::time::Timestamp {
    GRAPH_FIX_TIME
        .try_with(|value| *value)
        .unwrap_or_else(|_| teaql_core::time::Timestamp::now())
}

pub(crate) fn record_graph_fix_evidence(evidence: crate::FixEvidence) {
    let _ = GRAPH_FIX_EVIDENCE.try_with(|current| current.lock().unwrap().push(evidence));
}

// ---------------------------------------------------------------------------
// DynGraphSaver — type-erased graph save capability
// ---------------------------------------------------------------------------

type ErasedSavedEntity = Box<dyn std::any::Any + Send>;
/// The flag is true only after attachment to the provider's physical owner.
type SavedEntityDecoder =
    Box<dyn FnOnce(GraphNode, bool) -> Result<ErasedSavedEntity, RuntimeError> + Send>;

/// Object-safe trait for saving a [`GraphNode`] tree to the database.
///
/// A concrete implementation is registered in [`UserContext`] during setup so
/// that [`Audited::save`] can persist entities without exposing the underlying
/// executor type to business code.
pub(crate) trait DynGraphSaver: Send + Sync {
    fn save_graph_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        node: GraphNode,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>>;

    #[cfg(test)]
    fn save_ledger_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        node: GraphNode,
        root: crate::EntityRuntimeState,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>>;

    fn save_ledger_typed_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        node: GraphNode,
        root: crate::EntityRuntimeState,
        draft: crate::LedgerDraft,
        decode: SavedEntityDecoder,
    ) -> Pin<Box<dyn Future<Output = Result<ErasedSavedEntity, RuntimeError>> + Send + 'a>>;
}

/// Marker struct that implements [`DynGraphSaver`] for a specific executor type `E`.
///
/// `E` is the full executor type (e.g. `SqlDataServiceExecutor<SqliteDialect, …>`).
/// The struct itself is zero-sized; the actual executor is retrieved from
/// [`UserContext`] at call time.
pub(crate) struct GraphSaverFor<E> {
    _marker: PhantomData<fn() -> E>,
}

impl<E> GraphSaverFor<E> {
    pub(crate) fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

fn approved_blob_plan(
    context: &UserContext,
    approved: &crate::MutationPlan,
) -> Result<Option<Box<dyn crate::RuntimeBlobMutationPlan>>, RuntimeError> {
    if !context.needs_blob_binding_participant(approved) {
        return Ok(None);
    }
    context
        .get_resource::<crate::BlobMutationParticipantResource>()
        .ok_or_else(|| RuntimeError::Capability {
            code: "BLOB_CAPABILITY_MISSING",
            capability: "blob.v1".to_owned(),
        })?
        .0
        .plan(context, approved)?
        .map(Some)
        .ok_or_else(|| RuntimeError::Capability {
            code: "BLOB_CAPABILITY_MISSING",
            capability: "blob.v1".to_owned(),
        })
}

impl<E> DynGraphSaver for GraphSaverFor<E>
where
    E: teaql_data_service::QueryExecutor
        + teaql_data_service::MutationExecutor
        + teaql_data_service::TransactionExecutor
        + Send
        + Sync
        + 'static,
    for<'tx> <E as teaql_data_service::TransactionExecutor>::Tx<'tx>: Send + Sync,
{
    fn save_graph_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        node: GraphNode,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            teaql_core::MutationIntent::from_optional(node.comment.as_deref())?;
            let entity = node.entity.clone();
            let executor = context
                .require_resource::<E>()
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            // Build and review the complete immutable plan before opening the
            // transaction. A denied policy therefore cannot begin a provider
            // transaction or perform a partial graph mutation.
            let plan = crate::EntityDataService::for_executor(context, &entity, executor)
                .plan_graph(node)
                .await
                .map_err(data_service_error_into_runtime)?;
            let policy_plan = crate::graph_policy_plan(context, &plan)?;
            let binding_available = context
                .get_resource::<crate::BlobMutationParticipantResource>()
                .is_some();
            let governance = context
                .review_mutation_plan_with_blob_participant(&policy_plan, binding_available)?;
            let mut blob_plan = approved_blob_plan(context, &policy_plan)?;
            let view_boundary = context.view_transaction_boundary();
            let tx = teaql_data_service::TransactionExecutor::begin(executor)
                .await
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            let (result, audit_events) = crate::commit_audit::collect(
                context,
                crate::with_mutation_governance(governance, async {
                    let prepared_blob = if let Some(blob_plan) = blob_plan.take() {
                        let sql = teaql_data_service::DataServiceExecutor::infrastructure_sql(&tx)
                            .ok_or_else(|| {
                                DataServiceError::Runtime(RuntimeError::Transaction(
                                    "BLOB_TRANSACTION_REQUIRED".to_owned(),
                                ))
                            })?;
                        let resolved_ids = BTreeMap::new();
                        Some(
                            blob_plan
                                .prepare(context, sql, &resolved_ids)
                                .await
                                .map_err(DataServiceError::Runtime)?,
                        )
                    } else {
                        None
                    };
                    let eds = crate::EntityDataService::for_executor(context, entity, &tx);
                    eds.execute_graph_plan_internal_with_before_readback(plan, || async {
                        if let Some(prepared_blob) = prepared_blob {
                            let sql =
                                teaql_data_service::DataServiceExecutor::infrastructure_sql(&tx)
                                    .expect("prepared Blob binding requires the same SQL owner");
                            prepared_blob
                                .apply(context, sql)
                                .await
                                .map_err(DataServiceError::Runtime)?;
                        }
                        Ok(())
                    })
                    .await
                }),
            )
            .await;
            match result {
                Ok(saved) => {
                    teaql_data_service::Transaction::commit(tx)
                        .await
                        .map_err(|e| RuntimeError::Graph(e.to_string()))?;
                    view_boundary.complete();
                    crate::commit_audit::deliver(context, audit_events)?;
                    Ok(saved)
                }
                Err(error) => {
                    teaql_data_service::Transaction::rollback(tx)
                        .await
                        .map_err(|e| RuntimeError::Graph(e.to_string()))?;
                    view_boundary.complete();
                    Err(match error {
                        DataServiceError::Runtime(r) => r,
                        other => RuntimeError::Graph(other.to_string()),
                    })
                }
            }
        })
    }

    #[cfg(test)]
    fn save_ledger_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        mut node: GraphNode,
        root: crate::EntityRuntimeState,
    ) -> Pin<Box<dyn Future<Output = Result<GraphNode, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            if root.has_pending_dynamic_mutations() {
                context.dynamic_graph_provider()?;
            }
            capture_ledger_trace_scopes(&root, &node)?;
            let entity = node.entity.clone();
            let executor = context
                .require_resource::<E>()
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            let descriptor = context.require_entity(&entity)?;
            let id_prop = descriptor.id_property().ok_or_else(|| {
                RuntimeError::Graph(format!("entity {entity} has no id property"))
            })?;
            let current_id = node
                .values
                .get(&id_prop.name)
                .cloned()
                .unwrap_or(Value::I64(0));
            let root_key =
                root.original_key_for(&crate::EntityKey::new(entity.clone(), current_id));
            reject_cancelled_new_root(&root, &root_key)?;
            let policy_plan = crate::ledger_policy_plan(context, &node, &root);
            let binding_available = context
                .get_resource::<crate::BlobMutationParticipantResource>()
                .is_some();
            let governance = context
                .review_mutation_plan_with_blob_participant(&policy_plan, binding_available)?;
            let blob_plan = approved_blob_plan(context, &policy_plan)?;
            let view_boundary = context.view_transaction_boundary();
            let tx = teaql_data_service::TransactionExecutor::begin(executor)
                .await
                .map_err(|e| RuntimeError::Graph(e.to_string()))?;
            let (result, audit_events) = crate::commit_audit::collect(
                context,
                crate::with_mutation_governance(governance, async {
                    let eds = crate::EntityDataService::for_executor(context, &entity, &tx)
                        .with_ledger_privacy(&root)
                        .map_err(DataServiceError::Runtime)?
                        .with_mutation_intent(
                            teaql_core::MutationIntent::from_optional(node.comment.as_deref())
                                .map_err(RuntimeError::from)
                                .map_err(DataServiceError::Runtime)?,
                        );
                    let locations = ledger_object_locations_for(&root, &node);
                    let mut generated_ids = eds
                        .execute_ledger_plan_internal_with_blob(root.clone(), &locations, blob_plan)
                        .await?;
                    if let Some(readbacks) = &mut generated_ids.dynamic_readbacks {
                        node.dynamic_fields = readbacks
                            .remove(&root_key)
                            .map(Box::new)
                            .or(node.dynamic_fields.take());
                    }
                    if let Some(new_id) = generated_ids.get(&root_key) {
                        node.values.insert(id_prop.name.clone(), new_id.clone());
                    }
                    // All mutated rows have already been read from this
                    // transaction, including database defaults and triggers.
                    // An unmodified root still needs its own current-row read.
                    if let Some(snapshot) = generated_ids.canonical_rows.remove(&root_key) {
                        node.values = BTreeMap::from(snapshot).into();
                    } else {
                        let persisted_id =
                            node.values.get(&id_prop.name).cloned().ok_or_else(|| {
                                DataServiceError::Runtime(RuntimeError::Graph(format!(
                                    "saved {entity} missing identity field {}",
                                    id_prop.name
                                )))
                            })?;
                        node.values = eds
                            .fetch_graph_current_row_internal(
                                &entity,
                                &id_prop.name,
                                &persisted_id,
                                root.get_trace_chain(&root_key),
                            )
                            .await?
                            .map(Into::into)
                            .ok_or_else(|| {
                                DataServiceError::Runtime(RuntimeError::Graph(format!(
                                    "persisted {entity} record could not be read back"
                                )))
                            })?;
                    }
                    Ok(())
                }),
            )
            .await;
            match result {
                Ok(()) => {
                    teaql_data_service::Transaction::commit(tx)
                        .await
                        .map_err(|e| RuntimeError::Graph(e.to_string()))?;
                }
                Err(error) => {
                    teaql_data_service::Transaction::rollback(tx)
                        .await
                        .map_err(|e| RuntimeError::Graph(e.to_string()))?;
                    view_boundary.complete();
                    return Err(match error {
                        DataServiceError::Runtime(r) => r,
                        other => RuntimeError::Graph(other.to_string()),
                    });
                }
            }
            view_boundary.complete();
            root.clear_committed();
            crate::commit_audit::deliver(context, audit_events)?;
            Ok(node)
        })
    }

    fn save_ledger_typed_dyn<'a>(
        &'a self,
        context: &'a UserContext,
        mut node: GraphNode,
        root: crate::EntityRuntimeState,
        draft: crate::LedgerDraft,
        decode: SavedEntityDecoder,
    ) -> Pin<Box<dyn Future<Output = Result<ErasedSavedEntity, RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            let execution_root = draft.execution_state().clone();
            let entity = node.entity.clone();
            let executor = context
                .require_resource::<E>()
                .map_err(|error| RuntimeError::Graph(error.to_string()))?;
            let descriptor = context.require_entity(&entity)?;
            let id_property = descriptor.id_property().ok_or_else(|| {
                RuntimeError::Graph(format!("entity {entity} has no id property"))
            })?;
            let current_id = node
                .values
                .get(&id_property.name)
                .cloned()
                .unwrap_or(Value::I64(0));
            let root_key =
                execution_root.original_key_for(&crate::EntityKey::new(entity.clone(), current_id));
            reject_cancelled_new_root(&execution_root, &root_key)?;
            let policy_plan = crate::ledger_policy_plan(context, &node, &execution_root);
            let binding_available = context
                .get_resource::<crate::BlobMutationParticipantResource>()
                .is_some();
            let governance = context
                .review_mutation_plan_with_blob_participant(&policy_plan, binding_available)?;
            let blob_plan = approved_blob_plan(context, &policy_plan)?;
            let view_boundary = context.view_transaction_boundary();
            let tx = match teaql_data_service::TransactionExecutor::begin(executor).await {
                Ok(tx) => tx,
                Err(error) => {
                    view_boundary.complete();
                    return Err(RuntimeError::Graph(error.to_string()));
                }
            };
            let owner = teaql_data_service::Transaction::physical_identity(&tx);
            let native = owner.is_some();
            let root_to_clear = root.clone();
            let tx_ref = &tx;
            let (result, audit_events) = crate::commit_audit::collect(
                context,
                crate::with_mutation_governance(governance, async move {
                    let submission = match owner.as_ref() {
                        Some(owner) => {
                            Some(draft.attach(owner).map_err(DataServiceError::Runtime)?)
                        }
                        None => {
                            drop(draft);
                            // Legacy transports have no physical completion
                            // identity, so execution resumes on the restored
                            // live ledger. Rebind its trace scopes to the
                            // reviewed graph before ID allocation.
                            capture_ledger_trace_scopes(&root, &node)
                                .map_err(DataServiceError::Runtime)?;
                            None
                        }
                    };
                    let submitted_root = submission.as_ref().map_or_else(
                        || root.clone(),
                        |submission| submission.execution_state().clone(),
                    );
                    let service = crate::EntityDataService::for_executor(context, &entity, tx_ref)
                        .with_ledger_privacy(&submitted_root)
                        .map_err(DataServiceError::Runtime)?
                        .with_mutation_intent(
                            teaql_core::MutationIntent::from_optional(node.comment.as_deref())
                                .map_err(RuntimeError::from)
                                .map_err(DataServiceError::Runtime)?,
                        );
                    let locations = ledger_object_locations_for(&submitted_root, &node);
                    let outcome = service
                        .execute_ledger_plan_internal_with_blob(
                            submitted_root.clone(),
                            &locations,
                            blob_plan,
                        )
                        .await?;
                    if let Some(readbacks) = &outcome.dynamic_readbacks {
                        node.dynamic_fields = readbacks
                            .get(&root_key)
                            .cloned()
                            .map(Box::new)
                            .or(node.dynamic_fields.take());
                    }
                    if let Some(new_id) = outcome.get(&root_key) {
                        node.values.insert(id_property.name.clone(), new_id.clone());
                    }
                    if let Some(snapshot) = outcome.canonical_rows.get(&root_key).cloned() {
                        node.values = BTreeMap::from(snapshot).into();
                    } else {
                        let persisted_id =
                            node.values.get(&id_property.name).cloned().ok_or_else(|| {
                                DataServiceError::Runtime(RuntimeError::Graph(format!(
                                    "saved {entity} missing identity field {}",
                                    id_property.name
                                )))
                            })?;
                        node.values = service
                            .fetch_graph_current_row_internal(
                                &entity,
                                &id_property.name,
                                &persisted_id,
                                submitted_root.get_trace_chain(&root_key),
                            )
                            .await?
                            .map(Into::into)
                            .ok_or_else(|| {
                                DataServiceError::Runtime(RuntimeError::Graph(format!(
                                    "persisted {entity} record could not be read back"
                                )))
                            })?;
                    }
                    // Typed decoding is part of the physical transaction. A
                    // failed decoder drops the unacknowledged submission and
                    // prevents this owner from committing.
                    let saved =
                        decode(node, submission.is_some()).map_err(DataServiceError::Runtime)?;
                    if let Some(submission) = submission {
                        let (rows, allocated_ids, dynamic_rows) = outcome.into_submission_parts();
                        submission
                            .acknowledge_with_readbacks(rows, allocated_ids, dynamic_rows)
                            .map_err(DataServiceError::Runtime)?;
                    }
                    Ok(saved)
                }),
            )
            .await;
            match result {
                Ok(saved) => {
                    teaql_data_service::Transaction::commit(tx)
                        .await
                        .map_err(|error| RuntimeError::Graph(error.to_string()))?;
                    view_boundary.complete();
                    if !native {
                        root_to_clear.clear_committed();
                    }
                    crate::commit_audit::deliver(context, audit_events)?;
                    Ok(saved)
                }
                Err(error) => {
                    teaql_data_service::Transaction::rollback(tx)
                        .await
                        .map_err(|rollback| RuntimeError::Graph(rollback.to_string()))?;
                    view_boundary.complete();
                    Err(data_service_error_into_runtime(error))
                }
            }
        })
    }
}

fn loaded_relation_fields<T: Entity>(entity: &T) -> Vec<String> {
    // Capture only immutable load markers before GraphNode extraction consumes
    // the entity. No relationship objects or mutation ownership are cloned.
    T::entity_descriptor()
        .relations
        .into_iter()
        .filter(|relation| entity.is_field_loaded(&relation.name))
        .map(|relation| relation.name)
        .collect()
}

fn decode_saved_ledger_entity<T: Entity>(
    mut saved: GraphNode,
    context: &dyn std::any::Any,
    preserve_relations: bool,
    original_relation_fields: &[String],
) -> Result<T, RuntimeError> {
    let binding = context.downcast_ref::<crate::SavedEntityContext>();
    if binding.is_some_and(|binding| binding.preserves_relations() != preserve_relations) {
        return Err(RuntimeError::Graph("SAVE_HYDRATION_CONTEXT_INVALID".into()));
    }
    // An edge entry can be an unfetched identity-only placeholder. Only the
    // original entity's explicit load markers prove that detail was loaded.
    let restored_fields = (preserve_relations && !original_relation_fields.is_empty()).then(|| {
        let mut fields = saved.values.keys().cloned().collect::<BTreeSet<_>>();
        fields.extend(original_relation_fields.iter().cloned());
        fields
    });
    let fields = saved.dynamic_fields.take();
    let row = teaql_core::CompactRow::from_map(saved.values.into());
    let mut entity = T::from_compact_row_after_save(row, context, preserve_relations)
        .map_err(|error| RuntimeError::Graph(error.to_string()))?;
    if let Some(fields) = restored_fields {
        // Authoritative scalar SQL does not repeat the relation columns. Keep
        // loaded markers only for the immutable edges intentionally retained.
        entity.set_checker_loaded_fields(fields);
    }
    if let Some(fields) = fields {
        let base = entity.loaded_state_snapshot().ok_or_else(|| {
            RuntimeError::Graph("indexed state missing from dynamic save result".into())
        })?;
        let state = teaql_core::LoadedSnapshot::with_dynamic_fields(&base, &fields)
            .map_err(RuntimeError::Graph)?;
        entity
            .install_loaded_dynamic_fields(*fields, state)
            .map_err(|error| RuntimeError::Graph(error.to_string()))?;
    }
    if let Some(binding) = binding {
        let state = entity
            .__teaql_runtime_state_any()
            .and_then(|state| state.downcast_ref::<crate::EntityRuntimeState>());
        if !state.is_some_and(|state| binding.owns_ledger(state)) {
            return Err(RuntimeError::Graph(
                "SAVE_HYDRATION_BINDING_REQUIRED".into(),
            ));
        }
    }
    Ok(entity)
}

fn reject_cancelled_new_root(
    root: &crate::EntityRuntimeState,
    root_key: &crate::EntityKey,
) -> Result<(), RuntimeError> {
    if root.new_keys().contains(root_key) && root.deleted_keys().contains(root_key) {
        return Err(RuntimeError::Graph(format!(
            "cancelled new root {root_key:?}: create-then-delete has no persisted entity to return"
        )));
    }
    Ok(())
}

/// A scalar-only save can reuse an already loaded, immutable relation graph.
/// A changed relation key or a changed local FK would make that graph stale,
/// so the returned entity must expose those relations as NotLoaded instead.
fn can_preserve_loaded_relations(
    root: &crate::EntityRuntimeState,
    root_key: &crate::EntityKey,
    descriptor: &teaql_core::EntityDescriptor,
) -> bool {
    if !root.new_keys().is_empty() || !root.deleted_keys().is_empty() {
        return false;
    }
    if root
        .current_change_set()
        .dynamic_changes()
        .is_some_and(|changes| changes.keys().any(|key| key != root_key))
    {
        return false;
    }
    let changes = root.current_change_set();
    changes.changes().iter().all(|(key, fields)| {
        key == root_key
            && fields.keys().all(|field| {
                !descriptor
                    .relations
                    .iter()
                    .any(|relation| relation.name == *field || relation.local_key == *field)
            })
    })
}

#[cfg(test)]
mod save_relation_state_tests {
    use super::can_preserve_loaded_relations;
    use crate::{EntityKey, EntityRuntimeState};
    use teaql_core::{EntityDescriptor, RelationDescriptor, Value};

    fn descriptor() -> EntityDescriptor {
        let mut descriptor = EntityDescriptor::new("Order");
        descriptor
            .relations
            .push(RelationDescriptor::new("customer", "Customer").local_key("customer_id"));
        descriptor
    }

    #[test]
    fn scalar_change_keeps_snapshot_but_relation_changes_invalidate_it() {
        let root = EntityRuntimeState::default();
        let order = EntityKey::new("Order", Value::I64(1));
        let child = EntityKey::new("OrderLine", Value::I64(2));
        root.set(order.clone(), "total_amount", Value::I64(100));
        assert!(can_preserve_loaded_relations(&root, &order, &descriptor()));
        root.set(child, "sku", Value::Text("CHANGED".into()));
        assert!(!can_preserve_loaded_relations(&root, &order, &descriptor()));

        let root = EntityRuntimeState::default();
        root.set(order.clone(), "customer_id", Value::I64(3));
        assert!(!can_preserve_loaded_relations(&root, &order, &descriptor()));

        let root = EntityRuntimeState::default();
        root.set(order.clone(), "customer", Value::I64(3));
        assert!(!can_preserve_loaded_relations(&root, &order, &descriptor()));
    }

    #[derive(Clone, Debug, PartialEq, teaql_macros::TeaqlEntity)]
    #[teaql(entity = "ReadbackPlatform")]
    struct Platform {
        #[teaql(id)]
        id: u64,
        name: String,
    }

    #[teaql_macros::teaql_entity]
    #[derive(Clone, Debug, PartialEq, teaql_macros::TeaqlEntity)]
    #[teaql(entity = "ReadbackSchool")]
    struct School {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        name: String,
        platform_id: Option<u64>,
        #[teaql(relation(
            target = "ReadbackPlatform",
            local_key = "platform_id",
            foreign_key = "id"
        ))]
        platform: Option<Box<Platform>>,
        #[teaql(skip)]
        __load_state: teaql_core::eval::LoadState,
    }

    fn saved(id: u64, platform_id: Value) -> crate::GraphNode {
        let mut node = crate::GraphNode::new("ReadbackSchool");
        node.values.insert("id".into(), Value::U64(id));
        node.values.insert("version".into(), Value::I64(2));
        node.values
            .insert("name".into(), Value::Text("renamed".into()));
        node.values.insert("platform_id".into(), platform_id);
        node
    }

    #[test]
    fn scalar_readback_retains_exact_loaded_edge_marker_and_readonly_graph() {
        use crate::LedgerEntity as _;
        use teaql_core::Entity as _;
        let root = EntityRuntimeState::default();
        let mut graph = crate::EntityGraphBuilder::default();
        graph.install_relation_option(
            "ReadbackSchool",
            1,
            "platform",
            Some(Platform {
                id: 7,
                name: "campus".into(),
            }),
        );
        root.freeze_graph(graph).unwrap();
        let row = super::decode_saved_ledger_entity::<School>(
            saved(1, Value::U64(7)),
            &root,
            true,
            &["platform".into()],
        )
        .unwrap();
        assert!(row.is_field_loaded("name"));
        assert!(row.is_field_loaded("platform"));
        let state = row.entity_runtime_state().unwrap();
        assert_eq!(
            state
                .resolve_relation_option::<Platform>("ReadbackSchool", 1, "platform")
                .unwrap()
                .as_ref()
                .unwrap()
                .name,
            "campus"
        );
        assert!(state.current_change_set().is_empty());
        let invalidated = super::decode_saved_ledger_entity::<School>(
            saved(1, Value::U64(9)),
            &root,
            false,
            &["platform".into()],
        )
        .unwrap();
        assert!(!invalidated.is_field_loaded("platform"));
        assert!(
            invalidated
                .entity_runtime_state()
                .unwrap()
                .resolve_relation_option::<Platform>("ReadbackSchool", 1, "platform")
                .is_none()
        );
    }

    #[test]
    fn readback_never_invents_loaded_relation_from_fk_or_another_owner() {
        use teaql_core::Entity as _;
        let root = EntityRuntimeState::default();
        let mut graph = crate::EntityGraphBuilder::default();
        graph.install(
            7,
            Platform {
                id: 7,
                name: "cached entity alone is insufficient".into(),
            },
        );
        graph.install_relation_option("OtherOwner", 1, "platform", Some(7_u64));
        graph.install_relation_option("ReadbackSchool", 2, "platform", Some(7_u64));
        root.freeze_graph(graph).unwrap();
        let row =
            super::decode_saved_ledger_entity::<School>(saved(1, Value::U64(7)), &root, true, &[])
                .unwrap();
        assert!(!row.is_field_loaded("platform"));
    }

    #[test]
    fn readback_retains_explicit_loaded_null_without_marking_absent_null_loaded() {
        use teaql_core::Entity as _;
        let root = EntityRuntimeState::default();
        let mut graph = crate::EntityGraphBuilder::default();
        graph.install_relation_option::<Platform>("ReadbackSchool", 1, "platform", None);
        root.freeze_graph(graph).unwrap();
        let loaded = super::decode_saved_ledger_entity::<School>(
            saved(1, Value::Null),
            &root,
            true,
            &["platform".into()],
        )
        .unwrap();
        let absent =
            super::decode_saved_ledger_entity::<School>(saved(2, Value::Null), &root, true, &[])
                .unwrap();
        assert!(loaded.is_field_loaded("platform"));
        assert!(!absent.is_field_loaded("platform"));
    }

    #[test]
    fn scalar_readback_retains_original_loaded_marker_with_legacy_flat_entity_index() {
        use teaql_core::Entity as _;
        let root = EntityRuntimeState::default();
        let mut graph = crate::EntityGraphBuilder::default();
        graph.install(
            7,
            Platform {
                id: 7,
                name: "legacy flat graph".into(),
            },
        );
        root.freeze_graph(graph).unwrap();
        let mut original =
            super::decode_saved_ledger_entity::<School>(saved(1, Value::U64(7)), &root, true, &[])
                .unwrap();
        assert!(!original.is_field_loaded("platform"));
        original.set_checker_loaded_fields(
            ["id", "version", "name", "platform_id", "platform"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        );
        let snapshot = super::loaded_relation_fields(&original);
        let row = super::decode_saved_ledger_entity::<School>(
            saved(1, Value::U64(7)),
            &root,
            true,
            &snapshot,
        )
        .unwrap();
        assert!(row.is_field_loaded("platform"));
    }
}

#[cfg(test)]
mod transactional_ledger_readback_tests {
    use super::{DynGraphSaver, GraphSaverFor, SavedEntityDecoder};
    use crate::{
        EntityKey, EntityRuntimeState, GraphNode, InMemoryMetadataStore, MutationDecision,
        MutationPlan, MutationPolicy, MutationPolicyIdentity, MutationPolicySource, RawAuditEvent,
        RawAuditEventSink, RuntimeError, UserContext,
    };
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use teaql_core::{
        BlobFieldSpec, BlobRef, DataType, EntityDescriptor, PropertyDescriptor, RelationDescriptor,
        Value,
    };
    use teaql_data_service::{
        DataServiceCapabilities, DataServiceExecutor, ExecutionMetadata, MutationExecutor,
        MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult, Transaction,
        TransactionExecutor,
    };

    #[derive(Default)]
    struct State {
        row: BTreeMap<String, Value>,
        child_row: Option<BTreeMap<String, Value>>,
        calls: Vec<&'static str>,
        fail_readback: bool,
        fail_child_readback: bool,
    }

    #[derive(Clone)]
    struct Ambient(Arc<Mutex<State>>);

    struct Tx(Arc<Mutex<State>>);

    struct NativeAmbient(Arc<Mutex<State>>);

    struct NativeTx {
        inner: Tx,
        owner: teaql_data_service::infrastructure_sql::PhysicalTransactionOwner,
    }

    impl DataServiceExecutor for NativeAmbient {
        type Error = std::io::Error;

        fn capabilities(&self) -> DataServiceCapabilities {
            Ambient(self.0.clone()).capabilities()
        }
    }

    impl DataServiceExecutor for NativeTx {
        type Error = std::io::Error;

        fn capabilities(&self) -> DataServiceCapabilities {
            self.inner.capabilities()
        }
    }

    impl QueryExecutor for NativeAmbient {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            Ambient(self.0.clone()).query(request).await
        }
    }

    impl MutationExecutor for NativeAmbient {
        async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
            Ambient(self.0.clone()).mutate(request).await
        }
    }

    impl QueryExecutor for NativeTx {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            self.inner.query(request).await
        }
    }

    impl MutationExecutor for NativeTx {
        async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
            self.inner.mutate(request).await
        }
    }

    impl TransactionExecutor for NativeAmbient {
        type Tx<'a> = NativeTx;

        async fn begin(&self) -> Result<Self::Tx<'_>, Self::Error> {
            let inner = TransactionExecutor::begin(&Ambient(self.0.clone())).await?;
            Ok(NativeTx {
                inner,
                owner: teaql_data_service::infrastructure_sql::SqlResourceIdentity::new().begin(),
            })
        }
    }

    impl Transaction for NativeTx {
        type Error = std::io::Error;

        fn physical_identity(
            &self,
        ) -> Option<teaql_data_service::infrastructure_sql::PhysicalTransactionIdentity> {
            Some(self.owner.identity())
        }

        async fn commit(mut self) -> Result<(), Self::Error> {
            self.owner.prepare_commit().map_err(std::io::Error::other)?;
            self.inner.commit().await?;
            self.owner.committed().map_err(std::io::Error::other)
        }

        async fn rollback(self) -> Result<(), Self::Error> {
            self.inner.rollback().await?;
            self.owner.rolled_back().map_err(std::io::Error::other)
        }
    }

    #[derive(Clone)]
    struct CaptureEvents(Arc<Mutex<Vec<RawAuditEvent>>>);

    impl RawAuditEventSink for CaptureEvents {
        fn on_event(
            &self,
            _context: &UserContext,
            event: &RawAuditEvent,
        ) -> Result<(), RuntimeError> {
            self.0.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    struct DenyTaskPolicy;

    impl MutationPolicy for DenyTaskPolicy {
        fn identity(&self) -> MutationPolicyIdentity {
            MutationPolicyIdentity::new("task-policy", "1", "sha256:deny-task")
        }

        fn review(&self, _context: &UserContext, plan: &MutationPlan) -> MutationDecision {
            assert_eq!(plan.request_key, "Task.saveGraph");
            assert_eq!(plan.operations.len(), 1);
            MutationDecision::deny("TASK-DENIED", "task mutation denied", ["Task.name"])
        }
    }

    fn row_result(row: &BTreeMap<String, Value>) -> QueryResult {
        QueryResult {
            rows: vec![teaql_core::CompactRow::from_map(row.clone())],
            metadata: ExecutionMetadata::unrecorded_query(1),
        }
    }

    impl DataServiceExecutor for Ambient {
        type Error = std::io::Error;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities {
                query: true,
                mutation: true,
                transaction: true,
                ..Default::default()
            }
        }
    }

    impl DataServiceExecutor for Tx {
        type Error = std::io::Error;

        fn capabilities(&self) -> DataServiceCapabilities {
            Ambient(self.0.clone()).capabilities()
        }
    }

    impl QueryExecutor for Ambient {
        async fn query(&self, _request: QueryRequest) -> Result<QueryResult, Self::Error> {
            let mut state = self.0.lock().unwrap();
            state.calls.push("ambient-query");
            Ok(row_result(&state.row))
        }
    }

    impl QueryExecutor for Tx {
        async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
            let mut state = self.0.lock().unwrap();
            state.calls.push("transaction-query");
            let child = request.query.entity == "TaskChild";
            if child {
                state.calls.push("child-query");
            }
            if (state.fail_readback || child && state.fail_child_readback)
                && state.calls.contains(&"transaction-mutate")
            {
                return Ok(QueryResult {
                    rows: Vec::new(),
                    metadata: ExecutionMetadata::unrecorded_query(0),
                });
            }
            Ok(match (child, &state.child_row) {
                (true, Some(row)) => row_result(row),
                (true, None) => QueryResult {
                    rows: Vec::new(),
                    metadata: ExecutionMetadata::unrecorded_query(0),
                },
                (false, _) => row_result(&state.row),
            })
        }
    }

    impl MutationExecutor for Ambient {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            panic!("ledger writes must use the transaction executor")
        }
    }

    impl MutationExecutor for Tx {
        async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
            let mut state = self.0.lock().unwrap();
            state.calls.push("transaction-mutate");
            match request.command {
                teaql_data_service::MutationCommand::Update(command) => {
                    assert_eq!(command.expected_version, Some(1));
                    let row = if command.entity == "TaskChild" {
                        state.child_row.as_mut().expect("child row must exist")
                    } else {
                        &mut state.row
                    };
                    for (field, value) in command.values {
                        row.insert(field, value);
                    }
                }
                teaql_data_service::MutationCommand::Delete(command) => {
                    assert_eq!(command.expected_version, Some(1));
                    state.row.insert("version".to_owned(), Value::I64(-2));
                }
                other => panic!("unexpected mutation: {other:?}"),
            }
            Ok(MutationResult {
                affected_rows: 1,
                generated_values: Default::default(),
                persisted_snapshot: None,
                metadata: ExecutionMetadata::unrecorded_query(0),
            })
        }
    }

    impl TransactionExecutor for Ambient {
        type Tx<'a> = Tx;

        async fn begin(&self) -> Result<Self::Tx<'_>, Self::Error> {
            self.0.lock().unwrap().calls.push("begin");
            Ok(Tx(self.0.clone()))
        }
    }

    impl Transaction for Tx {
        type Error = std::io::Error;

        async fn commit(self) -> Result<(), Self::Error> {
            let mut state = self.0.lock().unwrap();
            if state.calls.contains(&"simulate-commit-failure") {
                state.calls.push("commit-failed");
                return Err(std::io::Error::other("simulated commit failure"));
            }
            state.calls.push("commit");
            // Simulate a concurrent writer becoming visible just after commit.
            state.row.insert("version".to_owned(), Value::I64(3));
            state
                .row
                .insert("name".to_owned(), Value::Text("other writer".to_owned()));
            Ok(())
        }

        async fn rollback(self) -> Result<(), Self::Error> {
            let mut state = self.0.lock().unwrap();
            state.calls.push("rollback");
            state.row.insert("version".to_owned(), Value::I64(1));
            state
                .row
                .insert("name".to_owned(), Value::Text("before".to_owned()));
            if let Some(row) = &mut state.child_row {
                row.insert("version".to_owned(), Value::I64(1));
                row.insert("name".to_owned(), Value::Text("child before".to_owned()));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn owned_native_decoder_failure_rolls_back_before_commit() {
        let state = Arc::new(Mutex::new(State {
            row: BTreeMap::from([
                ("id".to_owned(), Value::I64(1)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::from("before")),
            ]),
            ..Default::default()
        }));
        let descriptor = EntityDescriptor::new("Task")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text));
        let mut context = UserContext::default()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor));
        context.insert_resource(NativeAmbient(state.clone()));
        let root = EntityRuntimeState::default();
        let key = EntityKey::new_static("Task", 1_i64);
        root.set_original_version(key.clone(), 1);
        root.set(key.clone(), "name", Value::from("submitted"));
        let mut draft = root.reserve_submission(None).unwrap();
        let node = GraphNode::new("Task")
            .comment("owned typed decoder failure")
            .value("id", 1_i64)
            .value("version", 1_i64)
            .value("name", "before");
        super::capture_ledger_trace_scopes(draft.execution_state(), &node).unwrap();
        draft.seal_reviewed_intent().unwrap();
        let decode_state = state.clone();
        let decode_root = root.clone();
        let decoder: SavedEntityDecoder = Box::new(move |saved, native| {
            assert!(native);
            assert_eq!(saved.values.get("version"), Some(&Value::I64(2)));
            assert!(decode_root.has_pending_submission());
            assert!(!decode_state.lock().unwrap().calls.contains(&"commit"));
            Err(RuntimeError::Graph("decoder deliberately failed".into()))
        });
        let error = GraphSaverFor::<NativeAmbient>::new()
            .save_ledger_typed_dyn(&context, node, root.clone(), draft, decoder)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("decoder deliberately failed"),
            "{error}"
        );
        let state = state.lock().unwrap();
        assert_eq!(state.calls.last(), Some(&"rollback"));
        assert!(!state.calls.contains(&"commit"));
        assert_eq!(state.row.get("name"), Some(&Value::from("before")));
        assert!(!root.has_pending_submission());
        assert_eq!(root.get_original_version(&key), Some(1));
        assert_eq!(root.get(&key, "name"), Some(Value::from("submitted")));
    }

    #[tokio::test]
    async fn ledger_save_returns_transaction_snapshot_before_concurrent_commit_race() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(Mutex::new(State {
            row: BTreeMap::from([
                ("id".to_owned(), Value::I64(1)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("before".to_owned())),
            ]),
            calls: Vec::new(),
            fail_readback: false,
            ..Default::default()
        }));
        let descriptor = EntityDescriptor::new("Task")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text));
        let context = UserContext::default()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor))
            .with_event_sink(CaptureEvents(events.clone()));
        let root = EntityRuntimeState::default();
        let key = EntityKey::new_static("Task", 1_i64);
        root.set_original_version(key.clone(), 1);
        root.set(key, "name", Value::Text("updated".to_owned()));
        let node = GraphNode::new("Task")
            .comment("verify transaction-owned ledger readback")
            .value("id", Value::I64(1))
            .value("version", Value::I64(1))
            .value("name", Value::Text("before".to_owned()));
        let mut context = context;
        context.insert_resource(Ambient(state.clone()));

        let saved = GraphSaverFor::<Ambient>::new()
            .save_ledger_dyn(&context, node, root)
            .await
            .unwrap();
        assert_eq!(saved.values.get("version"), Some(&Value::I64(2)));
        assert_eq!(
            saved.values.get("name"),
            Some(&Value::Text("updated".to_owned()))
        );
        let state = state.lock().unwrap();
        assert_eq!(state.row.get("version"), Some(&Value::I64(3)));
        assert_eq!(state.calls.last(), Some(&"commit"));
        assert!(!state.calls.contains(&"ambient-query"));
        let events = events.lock().unwrap();
        assert!(!events.is_empty());
        let governance = events[0]
            .mutation_governance
            .as_ref()
            .expect("audit event must retain the mutation governance snapshot");
        assert_eq!(governance.request_key, "Task.saveGraph");
        assert_eq!(governance.source, MutationPolicySource::GeneratedDefault);
        assert_eq!(governance.operations.len(), 1);
    }

    #[tokio::test]
    async fn failed_authoritative_readback_rolls_back_before_reporting_failure() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(Mutex::new(State {
            row: BTreeMap::from([
                ("id".to_owned(), Value::I64(1)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("before".to_owned())),
            ]),
            calls: Vec::new(),
            fail_readback: true,
            ..Default::default()
        }));
        let descriptor = EntityDescriptor::new("Task")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text));
        let mut context = UserContext::default()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor))
            .with_event_sink(CaptureEvents(events.clone()));
        context.insert_resource(Ambient(state.clone()));
        let root = EntityRuntimeState::default();
        let key = EntityKey::new_static("Task", 1_i64);
        root.set_original_version(key.clone(), 1);
        root.set(key, "name", Value::Text("updated".to_owned()));
        let node = GraphNode::new("Task")
            .comment("verify rollback when authoritative readback fails")
            .value("id", Value::I64(1))
            .value("version", Value::I64(1))
            .value("name", Value::Text("before".to_owned()));

        let error = GraphSaverFor::<Ambient>::new()
            .save_ledger_dyn(&context, node, root.clone())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("could not be read back"));
        let state = state.lock().unwrap();
        assert_eq!(state.calls.last(), Some(&"rollback"));
        assert!(!state.calls.contains(&"commit"));
        assert_eq!(state.row.get("version"), Some(&Value::I64(1)));
        assert!(
            events.lock().unwrap().is_empty(),
            "a rolled-back write must not emit a committed audit event"
        );
        assert_eq!(
            root.get_original_version(&EntityKey::new_static("Task", 1_i64)),
            Some(1)
        );
    }

    #[tokio::test]
    async fn missing_child_readback_rolls_back_entire_ledger_after_both_writes() {
        let state = Arc::new(Mutex::new(State {
            row: BTreeMap::from([
                ("id".to_owned(), Value::I64(1)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("before".to_owned())),
            ]),
            child_row: Some(BTreeMap::from([
                ("id".to_owned(), Value::I64(2)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("child before".to_owned())),
            ])),
            fail_child_readback: true,
            ..Default::default()
        }));
        let descriptor = |entity: &str| {
            EntityDescriptor::new(entity)
                .property(PropertyDescriptor::new("id", DataType::I64).id())
                .property(PropertyDescriptor::new("version", DataType::I64).version())
                .property(PropertyDescriptor::new("name", DataType::Text))
        };
        let mut context = UserContext::default().with_metadata(
            InMemoryMetadataStore::new()
                .with_entity(descriptor("Task"))
                .with_entity(descriptor("TaskChild")),
        );
        context.insert_resource(Ambient(state.clone()));
        let root = EntityRuntimeState::default();
        let root_key = EntityKey::new_static("Task", 1_i64);
        let child_key = EntityKey::new_static("TaskChild", 2_i64);
        root.set_original_version(root_key.clone(), 1);
        root.set_original_version(child_key.clone(), 1);
        root.set(root_key.clone(), "name", Value::Text("updated".to_owned()));
        root.set(
            child_key.clone(),
            "name",
            Value::Text("child updated".to_owned()),
        );
        let node = GraphNode::new("Task")
            .comment("verify complete graph readback before commit")
            .value("id", 1_i64)
            .value("version", 1_i64)
            .value("name", "before");

        let error = GraphSaverFor::<Ambient>::new()
            .save_ledger_dyn(&context, node, root.clone())
            .await
            .expect_err("missing child readback must abort the root save");
        assert!(
            error
                .to_string()
                .contains("TaskChild record could not be read back")
        );
        let state = state.lock().unwrap();
        assert_eq!(
            state
                .calls
                .iter()
                .filter(|call| **call == "transaction-mutate")
                .count(),
            2
        );
        assert!(state.calls.contains(&"child-query"));
        assert_eq!(state.calls.last(), Some(&"rollback"));
        assert!(!state.calls.contains(&"commit"));
        assert_eq!(
            state.row.get("name"),
            Some(&Value::Text("before".to_owned()))
        );
        assert_eq!(
            state.child_row.as_ref().and_then(|row| row.get("name")),
            Some(&Value::Text("child before".to_owned()))
        );
        assert_eq!(root.get_original_version(&root_key), Some(1));
        assert_eq!(root.get_original_version(&child_key), Some(1));
    }

    #[tokio::test]
    async fn graph_flush_uses_persisted_id_for_a_returned_entity_alias() {
        use teaql_data_service::infrastructure_sql::SqlResourceIdentity;
        let state = Arc::new(Mutex::new(State {
            row: BTreeMap::from([
                ("id".to_owned(), Value::I64(42)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("before".to_owned())),
            ]),
            ..Default::default()
        }));
        let descriptor = EntityDescriptor::new("Task")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text));
        let context = UserContext::default()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor));
        let root = EntityRuntimeState::default();
        let temporary = EntityKey::new_static("Task", 0_i64);
        let returned = EntityKey::new_static("Task", 42_i64);
        root.mark_as_new(temporary.clone());
        root.set(temporary.clone(), "name", "before");
        let mut first_owner = SqlResourceIdentity::new().begin();
        root.begin_submission(&first_owner.identity())
            .unwrap()
            .acknowledge_with_allocated_ids(
                [(
                    temporary.clone(),
                    teaql_core::EntitySnapshot::from(state.lock().unwrap().row.clone()),
                )],
                [(temporary.clone(), Value::I64(42))],
            )
            .unwrap();
        first_owner.prepare_commit().unwrap();
        first_owner.committed().unwrap();
        root.set(returned.clone(), "name", "updated");
        root.set_comment("update previously allocated Task");

        let ambient = Ambient(state.clone());
        let tx = TransactionExecutor::begin(&ambient).await.unwrap();
        let service = crate::EntityDataService::for_executor(&context, "Task", &tx);
        let outcome = service
            .execute_ledger_plan_internal(root, &BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(outcome.generated_ids.len(), 0);
        let readback = outcome.canonical_rows.get(&temporary).unwrap();
        assert_eq!(readback.get("id"), Some(&Value::I64(42)));
        assert_eq!(readback.get("version"), Some(&Value::I64(2)));
        assert_eq!(readback.get("name"), Some(&Value::from("updated")));
        {
            let state_guard = state.lock().unwrap();
            let calls = &state_guard.calls;
            assert_eq!(
                calls
                    .iter()
                    .filter(|call| **call == "transaction-mutate")
                    .count(),
                1
            );
            assert!(!calls.contains(&"ambient-query"));
        }
        Transaction::rollback(tx).await.unwrap();
    }

    #[tokio::test]
    async fn graph_flush_rebinds_child_foreign_key_from_a_previously_allocated_parent() {
        use teaql_data_service::infrastructure_sql::SqlResourceIdentity;
        let state = Arc::new(Mutex::new(State {
            row: BTreeMap::from([
                ("id".to_owned(), Value::I64(42)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("parent".to_owned())),
            ]),
            child_row: Some(BTreeMap::from([
                ("id".to_owned(), Value::I64(2)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("child before".to_owned())),
                ("task_id".to_owned(), Value::I64(42)),
            ])),
            ..Default::default()
        }));
        let task = EntityDescriptor::new("Task")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text))
            .relation(
                RelationDescriptor::new("children", "TaskChild")
                    .local_key("id")
                    .foreign_key("task_id")
                    .many(),
            );
        let child = EntityDescriptor::new("TaskChild")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text))
            .property(PropertyDescriptor::new("task_id", DataType::I64));
        let context = UserContext::default().with_metadata(
            InMemoryMetadataStore::new()
                .with_entity(task)
                .with_entity(child),
        );
        let root = EntityRuntimeState::default();
        let parent_temporary = EntityKey::new_static("Task", 0_i64);
        let child_key = EntityKey::new_static("TaskChild", 2_i64);
        root.mark_as_new(parent_temporary.clone());
        root.set(parent_temporary.clone(), "name", "parent");
        let mut first_owner = SqlResourceIdentity::new().begin();
        root.begin_submission(&first_owner.identity())
            .unwrap()
            .acknowledge_with_allocated_ids(
                [(
                    parent_temporary.clone(),
                    teaql_core::EntitySnapshot::from(state.lock().unwrap().row.clone()),
                )],
                [(parent_temporary.clone(), Value::I64(42))],
            )
            .unwrap();
        first_owner.prepare_commit().unwrap();
        first_owner.committed().unwrap();
        root.set_original_version(child_key.clone(), 1);
        root.set(child_key.clone(), "task_id", Value::I64(0));
        root.set_comment("bind child to allocated parent");

        let ambient = Ambient(state.clone());
        let tx = TransactionExecutor::begin(&ambient).await.unwrap();
        let outcome = crate::EntityDataService::for_executor(&context, "Task", &tx)
            .execute_ledger_plan_internal(root.clone(), &BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(
            outcome
                .canonical_rows
                .get(&child_key)
                .unwrap()
                .get("task_id"),
            Some(&Value::I64(42))
        );
        assert_eq!(
            root.current_change_set().get(&child_key, "task_id"),
            Some(&Value::I64(0)),
            "rebinding must leave retryable source intent unchanged"
        );
        Transaction::rollback(tx).await.unwrap();
    }

    #[tokio::test]
    async fn soft_delete_returns_authoritative_tombstone_before_commit() {
        let state = Arc::new(Mutex::new(State {
            row: BTreeMap::from([
                ("id".to_owned(), Value::I64(1)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("before".to_owned())),
            ]),
            calls: Vec::new(),
            fail_readback: false,
            ..Default::default()
        }));
        let descriptor = EntityDescriptor::new("Task")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text));
        let mut context = UserContext::default()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor));
        context.insert_resource(Ambient(state.clone()));
        let root = EntityRuntimeState::default();
        let key = EntityKey::new_static("Task", 1_i64);
        root.set_original_version(key.clone(), 1);
        root.mark_as_delete(key);
        let node = GraphNode::new("Task")
            .comment("verify persisted soft-delete return value")
            .value("id", Value::I64(1))
            .value("version", Value::I64(1))
            .value("name", Value::Text("before".to_owned()));

        let saved = GraphSaverFor::<Ambient>::new()
            .save_ledger_dyn(&context, node, root)
            .await
            .unwrap();
        assert_eq!(saved.values.get("version"), Some(&Value::I64(-2)));
        let state = state.lock().unwrap();
        assert_eq!(state.calls.last(), Some(&"commit"));
        assert!(!state.calls.contains(&"ambient-query"));
    }

    #[tokio::test]
    async fn failed_commit_does_not_deliver_audit_or_clear_retryable_ledger() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(Mutex::new(State {
            row: BTreeMap::from([
                ("id".to_owned(), Value::I64(1)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("before".to_owned())),
            ]),
            calls: vec!["simulate-commit-failure"],
            fail_readback: false,
            ..Default::default()
        }));
        let descriptor = EntityDescriptor::new("Task")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text));
        let mut context = UserContext::default()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor))
            .with_event_sink(CaptureEvents(events.clone()));
        context.insert_resource(Ambient(state.clone()));
        let root = EntityRuntimeState::default();
        let key = EntityKey::new_static("Task", 1_i64);
        root.set_original_version(key.clone(), 1);
        root.set(key, "name", Value::Text("updated".to_owned()));
        let node = GraphNode::new("Task")
            .comment("verify failed commit audit boundary")
            .value("id", 1_i64)
            .value("version", 1_i64)
            .value("name", "before");
        let error = GraphSaverFor::<Ambient>::new()
            .save_ledger_dyn(&context, node, root.clone())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("simulated commit failure"));
        assert!(events.lock().unwrap().is_empty());
        assert!(!root.current_change_set().changes().is_empty());
        assert!(state.lock().unwrap().calls.contains(&"commit-failed"));
    }

    struct FailingAuditSink;

    impl RawAuditEventSink for FailingAuditSink {
        fn on_event(&self, _: &UserContext, _: &RawAuditEvent) -> Result<(), RuntimeError> {
            Err(RuntimeError::Event("simulated sink failure".to_owned()))
        }
    }

    #[tokio::test]
    async fn audit_sink_failure_is_reported_as_already_committed_without_replaying_ledger() {
        let state = Arc::new(Mutex::new(State {
            row: BTreeMap::from([
                ("id".to_owned(), Value::I64(1)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("before".to_owned())),
            ]),
            ..Default::default()
        }));
        let descriptor = EntityDescriptor::new("Task")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text));
        let mut context = UserContext::default()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor))
            .with_event_sink(FailingAuditSink);
        context.insert_resource(Ambient(state.clone()));
        let root = EntityRuntimeState::default();
        let key = EntityKey::new_static("Task", 1_i64);
        root.set_original_version(key.clone(), 1);
        root.set(key, "name", Value::Text("updated".to_owned()));
        let node = GraphNode::new("Task")
            .comment("verify post-commit audit failure")
            .value("id", 1_i64)
            .value("version", 1_i64)
            .value("name", "before");
        let error = GraphSaverFor::<Ambient>::new()
            .save_ledger_dyn(&context, node, root.clone())
            .await
            .unwrap_err();
        assert!(matches!(error, RuntimeError::AuditAfterCommit { .. }));
        assert!(error.to_string().contains("AUDIT_DELIVERY_AFTER_COMMIT"));
        assert!(state.lock().unwrap().calls.contains(&"commit"));
        assert!(!state.lock().unwrap().calls.contains(&"rollback"));
        assert!(root.current_change_set().changes().is_empty());
    }

    #[tokio::test]
    async fn cancelled_new_root_cannot_return_a_fictitious_persisted_entity() {
        let state = Arc::new(Mutex::new(State::default()));
        let descriptor = EntityDescriptor::new("Task")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text));
        let mut context = UserContext::default()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor));
        context.insert_resource(Ambient(state.clone()));
        let root = EntityRuntimeState::default();
        let key = EntityKey::new_static("Task", 7_i64);
        root.mark_as_new(key.clone());
        root.mark_as_delete(key);
        let node = GraphNode::new("Task")
            .comment("verify cancellation of a newly created root")
            .value("id", Value::I64(7))
            .value("version", Value::I64(0))
            .value("name", Value::Text("cancelled".to_owned()));

        let error = GraphSaverFor::<Ambient>::new()
            .save_ledger_dyn(&context, node, root)
            .await
            .expect_err("cancelled root has no persisted row to return");
        assert!(error.to_string().contains("cancelled new root"));
        let calls = &state.lock().unwrap().calls;
        assert!(!calls.contains(&"transaction-mutate"));
        assert!(!calls.contains(&"commit"));
    }

    #[tokio::test]
    async fn mutation_policy_denial_happens_before_transaction_begin() {
        let state = Arc::new(Mutex::new(State {
            row: BTreeMap::from([
                ("id".to_owned(), Value::I64(1)),
                ("version".to_owned(), Value::I64(1)),
                ("name".to_owned(), Value::Text("before".to_owned())),
            ]),
            calls: Vec::new(),
            fail_readback: false,
            ..Default::default()
        }));
        let descriptor = EntityDescriptor::new("Task")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("name", DataType::Text));
        let policy: Arc<dyn MutationPolicy> = Arc::new(DenyTaskPolicy);
        let mut context = UserContext::default()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor))
            .with_mutation_policy_registry(move |request_key: &str| {
                (request_key == "Task.saveGraph").then(|| policy.clone())
            });
        context.insert_resource(Ambient(state.clone()));
        let root = EntityRuntimeState::default();
        let key = EntityKey::new_static("Task", 1_i64);
        root.set_original_version(key.clone(), 1);
        root.set(key, "name", Value::Text("denied".to_owned()));
        let node = GraphNode::new("Task")
            .comment("verify customer mutation-policy denial before transaction begin")
            .value("id", Value::I64(1))
            .value("version", Value::I64(1))
            .value("name", Value::Text("before".to_owned()));

        let error = GraphSaverFor::<Ambient>::new()
            .save_ledger_dyn(&context, node, root)
            .await
            .expect_err("the customer policy must reject the mutation");

        assert!(error.to_string().contains("TASK-DENIED"));
        assert!(state.lock().unwrap().calls.is_empty());
    }

    #[tokio::test]
    async fn modeled_blob_ledger_write_without_binding_participant_never_begins_transaction() {
        let state = Arc::new(Mutex::new(State {
            row: BTreeMap::from([
                ("id".to_owned(), Value::I64(1)),
                ("version".to_owned(), Value::I64(1)),
                (
                    "document".to_owned(),
                    Value::BlobRef(BlobRef::new("blob_existing_binding").unwrap()),
                ),
            ]),
            ..Default::default()
        }));
        let spec = BlobFieldSpec::new("Task", "document", ["pdf"], true, "default").unwrap();
        let descriptor = EntityDescriptor::new("Task")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("document", DataType::BlobRef).blob_field_spec(spec));
        let mut context = UserContext::default()
            .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor));
        context.insert_resource(Ambient(state.clone()));
        let root = EntityRuntimeState::default();
        let key = EntityKey::new_static("Task", 1_i64);
        root.set_original_version(key.clone(), 1);
        let reference = BlobRef::new("blob_requires_binding").unwrap();
        root.set(key.clone(), "document", reference.clone());
        let node = GraphNode::new("Task")
            .comment("reject unbound blob write")
            .value("id", Value::I64(1))
            .value("version", Value::I64(1))
            .value("document", reference);
        let graph_node = node.clone();

        let error = GraphSaverFor::<Ambient>::new()
            .save_ledger_dyn(&context, node, root.clone())
            .await
            .expect_err("modeled Blob write needs transaction binding");
        assert!(matches!(
            error,
            RuntimeError::Capability {
                code: "BLOB_CAPABILITY_MISSING",
                ..
            }
        ));
        assert!(state.lock().unwrap().calls.is_empty());
        assert_eq!(
            root.get(&key, "document"),
            Some(Value::BlobRef(
                BlobRef::new("blob_requires_binding").unwrap()
            ))
        );

        let error = GraphSaverFor::<Ambient>::new()
            .save_graph_dyn(&context, graph_node)
            .await
            .expect_err("graph save cannot bypass the binding participant");
        assert!(matches!(
            error,
            RuntimeError::Capability {
                code: "BLOB_CAPABILITY_MISSING",
                ..
            }
        ));
        assert!(!state.lock().unwrap().calls.contains(&"begin"));
        assert!(!state.lock().unwrap().calls.contains(&"transaction-mutate"));
    }
}

// ---------------------------------------------------------------------------
// Standalone graph-node extraction (no executor needed)
// ---------------------------------------------------------------------------

/// Convert a typed entity into a [`GraphNode`] tree.
///
/// This only requires metadata (entity descriptors) from the [`UserContext`],
/// **not** the database executor.  It is the standalone equivalent of
/// [`EntityDataService::graph_node_from_entity`].
pub fn graph_node_from_entity<T: Entity>(
    context: &UserContext,
    entity: T,
) -> Result<GraphNode, RuntimeError> {
    if entity.has_pending_dynamic_mutations() {
        return Err(RuntimeError::DynamicField(
            teaql_core::dynamic_fields::DynamicFieldError {
                code: "DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED",
                field: T::ENTITY_NAME.to_owned(),
            },
        ));
    }
    extract_graph_node(context, entity)
}

fn extract_graph_node<T: Entity>(
    context: &UserContext,
    entity: T,
) -> Result<GraphNode, RuntimeError> {
    let descriptor = T::entity_descriptor();
    let loaded_fields = descriptor
        .properties
        .iter()
        .filter(|property| entity.is_field_loaded(&property.name))
        .map(|property| Value::Text(property.name.clone()))
        .collect::<Vec<_>>();
    let dirty_fields = entity.dirty_fields();
    let original_values = entity.original_values();
    let is_new = entity.is_new();
    let is_deleted = entity.is_marked_as_delete();
    let comment = entity.get_comment();
    let dynamic_fields = entity.dynamic_field_values().cloned().map(Box::new);
    let mut node = graph_node_from_values(context, &descriptor.name, entity.into_values())?;
    node.values
        .insert("_loaded_fields".to_owned(), Value::List(loaded_fields));
    node.dirty_fields = dirty_fields;
    node.original_values = original_values;
    node.dynamic_fields = dynamic_fields;
    if is_new {
        node.operation = GraphOperation::Create;
    }
    if is_deleted {
        node.operation = GraphOperation::Remove;
        node.relations.clear();
    }
    if let Some(c) = comment {
        node.set_comment(c);
    }
    Ok(node)
}

/// Recursively convert entity mutation values into a [`GraphNode`] tree.
///
/// Relations are resolved via the entity descriptors stored in `context`.
fn graph_node_from_values(
    context: &UserContext,
    entity: &str,
    values: MutationValues,
) -> Result<GraphNode, RuntimeError> {
    let descriptor = context.require_entity(entity)?;
    let mut node = GraphNode::new(entity);

    for (field, value) in values {
        if field == "_comment" {
            if let Value::Text(comment) = value {
                node.set_comment(comment);
            }
            continue;
        }
        if field == "_dirty_fields" {
            if let Value::List(fields) = value {
                let mut dirty = BTreeSet::new();
                for f in fields {
                    if let Value::Text(t) = f {
                        dirty.insert(t);
                    }
                }
                node.dirty_fields = Some(dirty);
            }
            continue;
        }
        if field == "_original_values" {
            if let Value::Object(orig) = value {
                node.original_values = Some(orig.into());
            }
            continue;
        }
        if field == "_is_new" {
            if matches!(value, Value::Bool(true)) {
                node.operation = GraphOperation::Create;
            }
            continue;
        }
        if field == "_is_deleted" {
            if matches!(value, Value::Bool(true)) {
                node.operation = GraphOperation::Remove;
            }
            continue;
        }
        let Some(relation) = descriptor.relation_by_name(&field) else {
            node.values.insert(field, value);
            continue;
        };

        match value {
            Value::Null => {
                node.relations.entry(field).or_default();
            }
            Value::Object(record) => {
                let child =
                    graph_node_from_values(context, &relation.target_entity, record.into())?;
                node.relations.entry(field).or_default().push(child);
            }
            Value::List(values) => {
                let children = node.relations.entry(field.clone()).or_default();
                for value in values {
                    let Value::Object(record) = value else {
                        return Err(RuntimeError::Graph(format!(
                            "relation {}.{} expects object children, got {:?}",
                            entity, field, value
                        )));
                    };
                    children.push(graph_node_from_values(
                        context,
                        &relation.target_entity,
                        record.into(),
                    )?);
                }
            }
            other => {
                return Err(RuntimeError::Graph(format!(
                    "relation {}.{} expects object/list/null, got {:?}",
                    entity, field, other
                )));
            }
        }
    }

    Ok(node)
}

fn merge_relation_mutations_into_root(
    context: &UserContext,
    root: &crate::EntityRuntimeState,
    node: &GraphNode,
) -> Result<(), RuntimeError> {
    for children in node.relations.values() {
        for child in children {
            let id = child.values.get("id").cloned().ok_or_else(|| {
                RuntimeError::Graph(format!(
                    "related mutation {} is missing its id",
                    child.entity
                ))
            })?;
            let key = crate::EntityKey::new(child.entity.clone(), id);
            let descriptor = context.require_entity(&child.entity)?;

            match child.operation {
                GraphOperation::Create => {
                    root.mark_as_new(key.clone());
                    // Validate aliases before writing any part of this child.
                    canonical_values_from_storage(descriptor, &child.values)?;
                    for (field, value) in &child.values {
                        merge_graph_value_into_ledger(root, &key, descriptor, field, value)?;
                    }
                }
                GraphOperation::Upsert => {
                    if let Some(fields) = &child.dirty_fields {
                        for field in fields {
                            if let Some(value) = child.values.get(field) {
                                merge_graph_value_into_ledger(
                                    root, &key, descriptor, field, value,
                                )?;
                            }
                        }
                    }
                }
                GraphOperation::Remove => root.mark_as_delete(key.clone()),
                GraphOperation::Reference => {}
            }

            if let Some(version) = child
                .original_values
                .as_ref()
                .and_then(|values| values.get("version"))
                .and_then(Value::try_i64)
            {
                root.set_original_version(key, version);
            }
            merge_relation_mutations_into_root(context, root, child)?;
        }
    }
    Ok(())
}

fn merge_graph_value_into_ledger(
    root: &crate::EntityRuntimeState,
    key: &crate::EntityKey,
    descriptor: &teaql_core::EntityDescriptor,
    storage_field: &str,
    value: &Value,
) -> Result<(), RuntimeError> {
    let canonical_field = canonical_field_from_storage(descriptor, storage_field)?;
    if canonical_field != storage_field
        && let Some(existing) = root.get(key, storage_field)
    {
        // Libraries generated before canonical relation keys wrote the local
        // FK name into the Ledger. Do not add a second alias for the same value.
        // New libraries write only the canonical relation name.
        if existing == *value {
            return Ok(());
        }
        return Err(RuntimeError::Graph(format!(
            "conflicting relation ledger aliases {}.{} and {}.{}",
            descriptor.name, storage_field, descriptor.name, canonical_field
        )));
    }
    root.set(key.clone(), canonical_field, value.clone());
    Ok(())
}

#[cfg(test)]
mod relation_ledger_key_tests {
    use super::merge_relation_mutations_into_root;
    use crate::{
        EntityKey, EntityRuntimeState, GraphNode, GraphOperation, InMemoryMetadataStore,
        UserContext,
    };
    use std::collections::BTreeSet;
    use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor, RelationDescriptor, Value};

    fn context() -> UserContext {
        UserContext::default().with_metadata(
            InMemoryMetadataStore::new().with_entity(
                EntityDescriptor::new("School")
                    .property(PropertyDescriptor::new("id", DataType::U64).id())
                    .property(PropertyDescriptor::new("school_type_id", DataType::U64))
                    .relation(
                        RelationDescriptor::new("school_type", "SchoolType")
                            .local_key("school_type_id"),
                    ),
            ),
        )
    }

    #[test]
    fn created_and_updated_child_graphs_merge_only_canonical_relation_keys() {
        let key = EntityKey::new("School", 8_u64);
        let root = EntityRuntimeState::default();
        let parent = GraphNode::new("Platform").relation(
            "schools",
            GraphNode::new("School")
                .operation(GraphOperation::Create)
                .value("id", 8_u64)
                .value("school_type_id", 1001_u64),
        );
        merge_relation_mutations_into_root(&context(), &root, &parent).unwrap();
        let changes = root.current_change_set();
        let values = changes.changes().get(&key).unwrap();
        assert_eq!(values.get("school_type"), Some(&Value::U64(1001)));
        assert!(!values.contains_key("school_type_id"));

        let root = EntityRuntimeState::default();
        let mut child = GraphNode::new("School")
            .value("id", 8_u64)
            .value("school_type_id", 1002_u64);
        child.dirty_fields = Some(BTreeSet::from(["school_type_id".to_owned()]));
        let parent = GraphNode::new("Platform").relation("schools", child);
        merge_relation_mutations_into_root(&context(), &root, &parent).unwrap();
        let changes = root.current_change_set();
        let values = changes.changes().get(&key).unwrap();
        assert_eq!(values.get("school_type"), Some(&Value::U64(1002)));
        assert!(!values.contains_key("school_type_id"));
    }

    #[test]
    fn old_storage_key_is_not_duplicated_when_child_graph_agrees() {
        let key = EntityKey::new("School", 8_u64);
        let root = EntityRuntimeState::default();
        root.set(key.clone(), "school_type_id", Value::U64(1001));
        let parent = GraphNode::new("Platform").relation(
            "schools",
            GraphNode::new("School")
                .operation(GraphOperation::Create)
                .value("id", 8_u64)
                .value("school_type_id", 1001_u64),
        );
        merge_relation_mutations_into_root(&context(), &root, &parent).unwrap();
        let changes = root.current_change_set();
        let values = changes.changes().get(&key).unwrap();
        assert_eq!(values.get("school_type_id"), Some(&Value::U64(1001)));
        assert!(!values.contains_key("school_type"));
    }

    #[test]
    fn old_storage_key_conflict_is_not_silently_accepted() {
        let key = EntityKey::new("School", 8_u64);
        let root = EntityRuntimeState::default();
        root.set(key, "school_type_id", Value::U64(1001));
        let parent = GraphNode::new("Platform").relation(
            "schools",
            GraphNode::new("School")
                .operation(GraphOperation::Create)
                .value("id", 8_u64)
                .value("school_type_id", 1002_u64),
        );
        assert!(
            merge_relation_mutations_into_root(&context(), &root, &parent)
                .unwrap_err()
                .to_string()
                .contains("conflicting relation ledger aliases")
        );
    }
}

/// Build real graph ancestry once before policy/execution, not on UserContext.
/// Every pending item receives its own immutable token, including deletions.
fn capture_ledger_trace_scopes(
    root: &crate::EntityRuntimeState,
    node: &GraphNode,
) -> Result<(), RuntimeError> {
    let intent = teaql_core::MutationIntent::from_optional(node.comment.as_deref())?;
    fn visit(
        root: &crate::EntityRuntimeState,
        node: &GraphNode,
        scope: Option<Arc<crate::TraceScopeToken>>,
        scopes: &mut BTreeMap<crate::EntityKey, Arc<crate::TraceScopeToken>>,
        unassigned: &mut BTreeMap<crate::EntityKey, Arc<crate::TraceScopeToken>>,
    ) {
        if let (Some(id), Some(scope)) = (node.id(), &scope) {
            let key =
                root.original_key_for(&crate::EntityKey::new(node.entity.clone(), id.clone()));
            if matches!(id, Value::Null | Value::U64(0) | Value::I64(0))
                && node.audit_trace_node().is_some()
            {
                unassigned
                    .entry(key.clone())
                    .or_insert_with(|| scope.clone());
            }
            // The graph/location traversal uses one deterministic first route
            // for an entity reached more than once.
            scopes.entry(key).or_insert_with(|| scope.clone());
        }
        for children in node.relations.values() {
            for child in children {
                let child_scope = child.trace_scope(scope.clone(), scopes.len() as u64);
                visit(root, child, child_scope, scopes, unassigned);
            }
        }
    }
    let root_scope = node.trace_scope(None, 0).expect("validated root comment");
    let mut scopes = BTreeMap::new();
    let mut unassigned = BTreeMap::new();
    visit(
        root,
        node,
        Some(root_scope.clone()),
        &mut scopes,
        &mut unassigned,
    );

    // Explicitly composed standalone pending items can be outside a loaded
    // relation projection. They still belong to this root request, not Context.
    let mut pending = root
        .current_change_set()
        .changes()
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    pending.extend(root.deleted_keys());
    pending.extend(root.new_keys());
    if let Some(dynamic) = root.current_change_set().dynamic_changes() {
        pending.extend(dynamic.keys().cloned());
    }
    for key in pending {
        scopes.entry(key.clone()).or_insert_with(|| {
            match root
                .get_entity_comment(&key)
                .filter(|text| !text.trim().is_empty())
            {
                Some(reason) => {
                    let scope = Arc::new(crate::TraceScopeToken {
                        parent: Some(root_scope.clone()),
                        track: teaql_core::TraceNode::typed(
                            teaql_core::TraceKind::AuditReason,
                            key.entity.to_string(),
                            key.id.try_u64().filter(|id| *id > 0),
                            reason,
                        ),
                        node_index: 0,
                    });
                    if matches!(key.id, Value::Null | Value::U64(0) | Value::I64(0)) {
                        unassigned.insert(key.clone(), scope.clone());
                    }
                    scope
                }
                None => root_scope.clone(),
            }
        });
    }
    root.set_comment(intent.comment());
    root.replace_trace_scopes(scopes, unassigned);
    Ok(())
}

#[cfg(test)]
mod trace_scope_capture_tests {
    use super::*;
    use crate::{EntityKey, EntityRuntimeState, InMemoryMetadataStore};
    use teaql_core::{
        DataType, EntityDescriptor, PropertyDescriptor, RelationDescriptor, TraceKind,
    };

    fn trace(
        root: &EntityRuntimeState,
        entity: &str,
        id: u64,
    ) -> Vec<(String, Option<u64>, String)> {
        root.get_trace_chain(&EntityKey::new(entity, id))
            .into_iter()
            .map(|node| {
                assert_eq!(node.kind, TraceKind::AuditReason);
                (node.entity_type, node.entity_id, node.comment)
            })
            .collect()
    }

    #[test]
    fn captures_nested_ledger_ancestry_including_deleted_child_and_same_numeric_id() {
        let root = EntityRuntimeState::default();
        let graph = GraphNode::new("Order")
            .value("id", 100_u64)
            .comment("submit order")
            .relation("items", GraphNode::new("OrderItem").value("id", 201_u64))
            .relation(
                "items",
                GraphNode::new("OrderItem")
                    .value("id", 202_u64)
                    .remove()
                    .comment("remove unavailable item"),
            )
            .relation(
                "payment",
                GraphNode::new("Payment")
                    .value("id", 100_u64)
                    .comment("authorize payment")
                    .relation(
                        "attempts",
                        GraphNode::new("PaymentAttempt").value("id", 401_u64),
                    ),
            )
            .relation(
                "shipment",
                GraphNode::new("Shipment")
                    .value("id", 501_u64)
                    .comment("dispatch shipment"),
            );
        root.mark_as_delete(EntityKey::new("OrderItem", 202_u64));
        capture_ledger_trace_scopes(&root, &graph).unwrap();
        let order = ("Order".to_owned(), Some(100), "submit order".to_owned());
        let payment = (
            "Payment".to_owned(),
            Some(100),
            "authorize payment".to_owned(),
        );
        assert_eq!(trace(&root, "Order", 100), std::slice::from_ref(&order));
        assert_eq!(trace(&root, "OrderItem", 201), std::slice::from_ref(&order));
        assert_eq!(
            trace(&root, "Payment", 100),
            [order.clone(), payment.clone()]
        );
        assert_eq!(
            trace(&root, "PaymentAttempt", 401),
            [order.clone(), payment]
        );
        assert_eq!(
            trace(&root, "Shipment", 501),
            [
                order.clone(),
                (
                    "Shipment".to_owned(),
                    Some(501),
                    "dispatch shipment".to_owned()
                )
            ]
        );
        assert_eq!(
            trace(&root, "OrderItem", 202),
            [
                order,
                (
                    "OrderItem".to_owned(),
                    Some(202),
                    "remove unavailable item".to_owned()
                )
            ]
        );
    }

    #[test]
    fn retry_rebuilds_request_scopes_instead_of_appending_or_reusing_old_root_intent() {
        let root = EntityRuntimeState::default();
        let mut graph = GraphNode::new("Order")
            .value("id", 100_u64)
            .comment("first attempt")
            .relation(
                "payment",
                GraphNode::new("Payment")
                    .value("id", 301_u64)
                    .comment("authorize payment"),
            );
        capture_ledger_trace_scopes(&root, &graph).unwrap();
        graph.set_comment("retry after review");
        capture_ledger_trace_scopes(&root, &graph).unwrap();
        assert_eq!(
            trace(&root, "Payment", 301),
            [
                (
                    "Order".to_owned(),
                    Some(100),
                    "retry after review".to_owned()
                ),
                (
                    "Payment".to_owned(),
                    Some(301),
                    "authorize payment".to_owned()
                ),
            ]
        );
        root.clear_committed();
        assert!(trace(&root, "Payment", 301).is_empty());
    }

    #[test]
    fn allocation_rebinds_owned_ancestors_without_assigning_inherited_siblings() {
        let root = EntityRuntimeState::default();
        let mut graph = GraphNode::new("Order")
            .value("id", 0_u64)
            .comment("submit late order")
            .relation("items", GraphNode::new("OrderItem").value("id", 0_u64))
            .relation(
                "payment",
                GraphNode::new("Payment")
                    .value("id", 0_u64)
                    .comment("authorize late payment")
                    .relation(
                        "attempts",
                        GraphNode::new("PaymentAttempt").value("id", 401_u64),
                    ),
            );
        // A pending item outside the loaded projection must use this exact
        // root token, not a second unowned copy of its pre-allocation reason.
        let standalone = EntityKey::new("Shipment", 501_u64);
        root.mark_as_new(standalone.clone());
        root.set_entity_comment(standalone, "dispatch standalone shipment");
        capture_ledger_trace_scopes(&root, &graph).unwrap();
        let allocated = BTreeMap::from([
            (EntityKey::new("Order", 0_u64), Value::U64(100)),
            (EntityKey::new("OrderItem", 0_u64), Value::U64(201)),
            (EntityKey::new("Payment", 0_u64), Value::U64(301)),
        ]);
        root.enrich_allocated_trace_scopes(&allocated);
        let order = (
            "Order".to_owned(),
            Some(100),
            "submit late order".to_owned(),
        );
        assert_eq!(trace(&root, "OrderItem", 0), std::slice::from_ref(&order));
        assert_eq!(
            trace(&root, "PaymentAttempt", 401),
            [
                order.clone(),
                (
                    "Payment".to_owned(),
                    Some(301),
                    "authorize late payment".to_owned()
                )
            ]
        );
        assert_eq!(
            trace(&root, "Shipment", 501),
            [
                order,
                (
                    "Shipment".to_owned(),
                    Some(501),
                    "dispatch standalone shipment".to_owned()
                )
            ]
        );

        // Failed saves retain old ledger keys. Recapture on retry must discard
        // the previous allocation rather than retaining its ID or root reason.
        graph.set_comment("retry late order");
        capture_ledger_trace_scopes(&root, &graph).unwrap();
        root.enrich_allocated_trace_scopes(&BTreeMap::from([
            (EntityKey::new("Order", 0_u64), Value::U64(102)),
            (EntityKey::new("Payment", 0_u64), Value::U64(303)),
        ]));
        assert_eq!(
            trace(&root, "PaymentAttempt", 401),
            [
                ("Order".to_owned(), Some(102), "retry late order".to_owned()),
                (
                    "Payment".to_owned(),
                    Some(303),
                    "authorize late payment".to_owned()
                )
            ]
        );
        assert_eq!(graph.id(), Some(&Value::U64(0)));
        root.clear_committed();
        assert!(trace(&root, "PaymentAttempt", 401).is_empty());
    }

    #[test]
    fn allocation_uses_scope_ownership_when_unassigned_same_type_keys_differ() {
        let root = EntityRuntimeState::default();
        let graph = GraphNode::new("Order")
            .value("id", 100_u64)
            .comment("submit order")
            .relation(
                "payments",
                GraphNode::new("Payment")
                    .value("id", Value::Null)
                    .comment("first payment"),
            )
            .relation(
                "payments",
                GraphNode::new("Payment")
                    .value("id", 0_u64)
                    .comment("second payment"),
            );
        capture_ledger_trace_scopes(&root, &graph).unwrap();
        let first = EntityKey::new("Payment", Value::Null);
        let second = EntityKey::new("Payment", 0_u64);
        root.enrich_allocated_trace_scopes(&BTreeMap::from([
            (first.clone(), Value::U64(301)),
            (second.clone(), Value::U64(302)),
        ]));
        assert_eq!(
            root.get_trace_chain(&first).last().unwrap().entity_id,
            Some(301)
        );
        assert_eq!(
            root.get_trace_chain(&second).last().unwrap().entity_id,
            Some(302)
        );
    }

    #[test]
    fn composed_deleted_child_is_discovered_from_loaded_snapshot_without_field_changes_or_io() {
        let mut child = EntityRuntimeState::default();
        let key = EntityKey::new("OrderItem", 202_u64);
        child.set_original_compact_row(
            "OrderItem",
            teaql_core::CompactRow::from_map(BTreeMap::from([
                ("id".to_owned(), Value::U64(202)),
                ("version".to_owned(), Value::I64(3)),
                ("order".to_owned(), Value::U64(100)),
            ])),
        );
        child.set_entity_comment(key.clone(), "remove unavailable item");
        child.mark_as_delete(key.clone());
        let root = EntityRuntimeState::default();
        root.adopt_mutations_from(&child);
        assert!(root.first_composition_error().is_none());
        assert!(root.current_change_set().changes().is_empty());
        let context = UserContext::default().with_metadata(
            InMemoryMetadataStore::new()
                .with_entity(
                    EntityDescriptor::new("Order")
                        .property(PropertyDescriptor::new("id", DataType::U64).id())
                        .relation(
                            RelationDescriptor::new("items", "OrderItem")
                                .local_key("id")
                                .foreign_key("order")
                                .many(),
                        ),
                )
                .with_entity(
                    EntityDescriptor::new("OrderItem")
                        .property(PropertyDescriptor::new("id", DataType::U64).id()),
                ),
        );
        let mut graph = GraphNode::new("Order")
            .value("id", 100_u64)
            .comment("submit order");
        let mut visited = BTreeSet::from([EntityKey::new("Order", 100_u64)]);
        hydrate_ledger_relations(&context, &root, &mut graph, &mut visited).unwrap();
        let deleted = &graph.relations["items"][0];
        assert_eq!(deleted.operation, GraphOperation::Remove);
        assert_eq!(deleted.comment.as_deref(), Some("remove unavailable item"));
        capture_ledger_trace_scopes(&root, &graph).unwrap();
        assert_eq!(
            trace(&root, "OrderItem", 202),
            [
                ("Order".to_owned(), Some(100), "submit order".to_owned()),
                (
                    "OrderItem".to_owned(),
                    Some(202),
                    "remove unavailable item".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn mixed_signed_provider_ids_and_unsigned_generated_references_keep_grandchild_ancestry() {
        // SQLite rows use I64 while generated relationship setters use U64.
        // Relation matching must use entity identity, not Value variant equality.
        let mut payment = EntityRuntimeState::default();
        let payment_key = EntityKey::new("Payment", 301_u64);
        payment.set_original_compact_row(
            "Payment",
            teaql_core::CompactRow::from_map(BTreeMap::from([
                ("id".to_owned(), Value::I64(301)),
                ("version".to_owned(), Value::I64(1)),
                ("order".to_owned(), Value::I64(100)),
            ])),
        );
        payment.set(payment_key.clone(), "reference_code", "authorized");
        payment.set_entity_comment(payment_key, "authorize payment");
        let root = EntityRuntimeState::default();
        root.adopt_mutations_from(&payment);
        for (id, parent) in [(401_u64, 301_u64), (402_u64, 302_u64)] {
            let key = EntityKey::new("PaymentAttempt", id);
            root.mark_as_new(key.clone());
            root.set(key, "payment", Value::U64(parent));
        }
        let context = UserContext::default().with_metadata(
            InMemoryMetadataStore::new()
                .with_entity(
                    EntityDescriptor::new("Order")
                        .property(PropertyDescriptor::new("id", DataType::U64).id())
                        .relation(
                            RelationDescriptor::new("payments", "Payment")
                                .local_key("id")
                                .foreign_key("order")
                                .many(),
                        ),
                )
                .with_entity(
                    EntityDescriptor::new("Payment")
                        .property(PropertyDescriptor::new("id", DataType::U64).id())
                        .property(PropertyDescriptor::new("order", DataType::U64))
                        .property(PropertyDescriptor::new("reference_code", DataType::Text))
                        .relation(
                            RelationDescriptor::new("attempts", "PaymentAttempt")
                                .local_key("id")
                                .foreign_key("payment")
                                .many(),
                        ),
                )
                .with_entity(
                    EntityDescriptor::new("PaymentAttempt")
                        .property(PropertyDescriptor::new("id", DataType::U64).id())
                        .property(PropertyDescriptor::new("payment", DataType::U64)),
                ),
        );
        let mut graph = GraphNode::new("Order")
            .value("id", 100_u64)
            .comment("submit order");
        let mut visited = BTreeSet::from([EntityKey::new("Order", 100_u64)]);
        hydrate_ledger_relations(&context, &root, &mut graph, &mut visited).unwrap();
        let payments = &graph.relations["payments"];
        assert_eq!(
            payments.len(),
            1,
            "signed loaded Payment FK matches unsigned Order ID"
        );
        let attempts = &payments[0].relations["attempts"];
        assert_eq!(
            attempts.len(),
            1,
            "only this payment's unsigned FK matches its signed ID"
        );
        assert_eq!(attempts[0].values["id"].try_u64(), Some(401));
        capture_ledger_trace_scopes(&root, &graph).unwrap();
        assert_eq!(
            trace(&root, "PaymentAttempt", 401),
            [
                ("Order".to_owned(), Some(100), "submit order".to_owned()),
                (
                    "Payment".to_owned(),
                    Some(301),
                    "authorize payment".to_owned()
                ),
            ]
        );
        assert_eq!(
            trace(&root, "PaymentAttempt", 402),
            [("Order".to_owned(), Some(100), "submit order".to_owned()),]
        );
    }
}

fn hydrate_ledger_relations(
    context: &UserContext,
    root: &crate::EntityRuntimeState,
    node: &mut GraphNode,
    visited: &mut BTreeSet<crate::EntityKey>,
) -> Result<(), RuntimeError> {
    let descriptor = context.require_entity(&node.entity)?;
    for relation in &descriptor.relations {
        let Some(local_value) = node.values.get(&relation.local_key).cloned() else {
            continue;
        };
        let existing = node.relations.entry(relation.name.clone()).or_default();
        let existing_keys = existing
            .iter()
            .filter_map(|child| {
                child.values.get("id").cloned().map(|id| {
                    root.original_key_for(&crate::EntityKey::new(child.entity.clone(), id))
                })
            })
            .collect::<BTreeSet<_>>();
        let mut discovered = Vec::new();
        let changes = root.current_change_set();
        let mut pending = changes.changes().keys().cloned().collect::<BTreeSet<_>>();
        if let Some(dynamic) = changes.dynamic_changes() {
            pending.extend(dynamic.keys().cloned());
        }
        pending.extend(root.deleted_keys());
        pending.extend(root.new_keys());
        for key in &pending {
            if key.entity.as_ref() != relation.target_entity || existing_keys.contains(key) {
                continue;
            }
            let original_values = root.original_values_for(key);
            let target_descriptor = context.require_entity(&key.entity)?;
            let mut values: crate::EntityValues = original_values
                .clone()
                .map(|values| BTreeMap::from(values).into())
                .unwrap_or_default();
            if let Some(changes) = changes.changes().get(key) {
                values.extend(storage_values_from_ledger(target_descriptor, changes)?);
            }
            let foreign_value = if relation.foreign_key == "id" {
                Some(key.id.clone())
            } else {
                values.get(&relation.foreign_key).cloned()
            };
            // Provider-loaded IDs can be signed while generated FK setters are
            // unsigned. Reuse the ledger's typed identity normalization rather
            // than comparing Value variants; never associate two absent refs.
            let matches_reference = !matches!(local_value, Value::Null)
                && foreign_value.is_some_and(|foreign| {
                    root.original_key_for(&crate::EntityKey::new(
                        relation.target_entity.clone(),
                        foreign,
                    )) == root.original_key_for(&crate::EntityKey::new(
                        relation.target_entity.clone(),
                        local_value.clone(),
                    ))
                });
            if !matches_reference || !visited.insert(key.clone()) {
                continue;
            }
            values
                .entry("id".to_owned())
                .or_insert_with(|| key.id.clone());
            let operation = if root.deleted_keys().contains(key) {
                GraphOperation::Remove
            } else if root.new_keys().contains(key) || root.get_original_version(key).is_none() {
                GraphOperation::Create
            } else {
                GraphOperation::Upsert
            };
            let mut child = GraphNode::new(key.entity.to_string());
            child.values = values;
            child.operation = operation;
            child.comment = root.get_entity_comment(key);
            child.original_values = original_values;
            child.dirty_fields = Some(storage_fields_from_ledger(
                target_descriptor,
                &root.changed_field_names(key),
            )?);
            if operation != GraphOperation::Remove {
                hydrate_ledger_relations(context, root, &mut child, visited)?;
            }
            discovered.push(child);
        }
        existing.extend(discovered);
    }
    Ok(())
}

fn preflight_graph(
    context: &UserContext,
    node: &mut GraphNode,
    location: &ObjectLocation,
    root: Option<&crate::EntityRuntimeState>,
) -> Result<(), RuntimeError> {
    if !matches!(
        node.operation,
        GraphOperation::Remove | GraphOperation::Reference
    ) {
        let before = node.values.clone();
        let status = match node.operation {
            GraphOperation::Create => crate::CheckObjectStatus::Create,
            GraphOperation::Upsert => crate::CheckObjectStatus::Update,
            GraphOperation::Remove | GraphOperation::Reference => unreachable!(),
        };
        crate::mark_entity_status(&mut node.values, status);
        if let Some(fields) = &node.dirty_fields {
            node.values.insert(
                "_dirty_fields".to_owned(),
                Value::List(fields.iter().cloned().map(Value::Text).collect()),
            );
        }
        let result = context.check_and_fix_values_at(&node.entity, &mut node.values, location);
        crate::clear_entity_status(&mut node.values);
        node.values.remove("_dirty_fields");
        result?;

        if let Some(root) = root
            && let Some(id) = node.values.get("id").cloned()
        {
            let key = crate::EntityKey::new(node.entity.clone(), id);
            let descriptor = context.require_entity(&node.entity)?;
            for (field, value) in &node.values {
                if before.get(field) != Some(value) {
                    let name = canonical_field_from_storage(descriptor, field)?;
                    root.set(key.clone(), name, value.clone());
                }
            }
        }
    }

    for (relation, children) in &mut node.relations {
        for (index, child) in children.iter_mut().enumerate() {
            let child_location = location.clone().member(relation).element(index);
            preflight_graph(context, child, &child_location, root)?;
        }
    }
    Ok(())
}

fn preflight_pending_dynamic_entities(
    context: &UserContext,
    root: &crate::EntityRuntimeState,
    node: &GraphNode,
) -> Result<(), RuntimeError> {
    let changes = root.current_change_set();
    let Some(dynamic) = changes.dynamic_changes() else {
        return Ok(());
    };
    let locations = ledger_object_locations_for(root, node);
    for key in dynamic.keys() {
        if locations.contains_key(key) || root.deleted_keys().contains(key) {
            continue;
        }
        let mut pending = GraphNode::new(key.entity.to_string());
        pending.original_values = root.original_values_for(key);
        pending.values = pending
            .original_values
            .clone()
            .map(|values| BTreeMap::from(values).into())
            .unwrap_or_default();
        if let Some(values) = changes.changes().get(key) {
            let descriptor = context.require_entity(&key.entity)?;
            pending
                .values
                .extend(storage_values_from_ledger(descriptor, values)?);
        }
        pending
            .values
            .insert("id".into(), root.persisted_id_for(key));
        let loaded = pending
            .values
            .keys()
            .filter(|field| !field.starts_with('_'))
            .cloned()
            .map(Value::Text)
            .collect();
        pending
            .values
            .insert("_loaded_fields".into(), Value::List(loaded));
        let descriptor = context.require_entity(&key.entity)?;
        pending.dirty_fields = Some(storage_fields_from_ledger(
            descriptor,
            &root.changed_field_names(key),
        )?);
        pending.operation = if root.new_keys().contains(key) {
            GraphOperation::Create
        } else {
            GraphOperation::Upsert
        };
        preflight_graph(context, &mut pending, &ObjectLocation::root(), Some(root))?;
    }
    Ok(())
}

/// Retain the model-relative path discovered during graph preflight for the
/// sparse SQL-payload gate. Multiple references to one ledger entity may
/// exist; the first path in deterministic relation order is its diagnostic
/// location, and a root entity always retains the empty root path.
fn ledger_object_locations(node: &GraphNode) -> BTreeMap<crate::EntityKey, ObjectLocation> {
    fn visit(
        node: &GraphNode,
        location: &ObjectLocation,
        locations: &mut BTreeMap<crate::EntityKey, ObjectLocation>,
    ) {
        if let Some(id) = node.values.get("id").cloned() {
            let key = crate::EntityKey::new(node.entity.clone(), id);
            locations.entry(key).or_insert_with(|| location.clone());
        }
        for (relation, children) in &node.relations {
            for (index, child) in children.iter().enumerate() {
                let child_location = location.clone().member(relation).element(index);
                visit(child, &child_location, locations);
            }
        }
    }

    let mut locations = BTreeMap::new();
    visit(node, &ObjectLocation::root(), &mut locations);
    locations
}

fn ledger_object_locations_for(
    root: &crate::EntityRuntimeState,
    node: &GraphNode,
) -> BTreeMap<crate::EntityKey, ObjectLocation> {
    let mut locations = BTreeMap::new();
    for (key, location) in ledger_object_locations(node) {
        locations
            .entry(root.original_key_for(&key))
            .or_insert(location);
    }
    locations
}

#[cfg(test)]
mod ledger_location_tests {
    use super::ledger_object_locations;
    use crate::{EntityKey, GraphNode};
    use teaql_core::Value;

    #[test]
    fn nested_ledger_entity_keeps_model_and_json_error_paths() {
        let mut order = GraphNode::new("Order");
        order.values.insert("id".to_owned(), Value::U64(7));
        let mut line = GraphNode::new("OrderLine");
        line.values.insert("id".to_owned(), Value::U64(9));
        order.relations.insert("line_items".to_owned(), vec![line]);

        let locations = ledger_object_locations(&order);
        assert!(locations[&EntityKey::new("Order", 7_u64)].is_root());
        let child = &locations[&EntityKey::new("OrderLine", 9_u64)];
        assert_eq!(child.model_path(), "line_items[0]");
        assert_eq!(child.instance_path(), "/lineItems/0");
    }
}

// ---------------------------------------------------------------------------
// AuditedSaveExt — the `.save(&context)` method on `Audited<T>`
// ---------------------------------------------------------------------------

/// Extension trait that provides the `.save(&context)` method on [`Audited<T>`](teaql_core::Audited).
///
/// # Example
/// ```ignore
/// use teaql_runtime::AuditedSaveExt;
///
/// school.audit_as("创建学校").save(&context).await?;
/// ```
pub trait AuditedSaveExt {
    type Entity;

    fn save<'a>(
        self,
        context: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Entity, RuntimeError>> + Send + 'a>>;
}

impl<T> AuditedSaveExt for teaql_core::Audited<T>
where
    T: Entity + Send + 'static,
{
    type Entity = T;

    fn save<'a>(
        self,
        context: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Entity, RuntimeError>> + Send + 'a>> {
        Box::pin(
            context.observe_mutation_request(|| T::entity_descriptor().name, async move {
                let entity = self.into_entity(); // applies comment onto the entity
                let root = entity
                    .__teaql_runtime_state_any()
                    .and_then(|state| state.downcast_ref::<crate::EntityRuntimeState>())
                    .cloned();
                if root.is_some()
                    || entity.has_pending_dynamic_mutations()
                    || entity.dynamic_field_values().is_some()
                {
                    if root.is_none() {
                        return Err(RuntimeError::Graph(
                            "dynamic save requires the runtime mutation ledger carrier".into(),
                        ));
                    }
                    let evidence = Arc::new(std::sync::Mutex::new(Vec::new()));
                    let result = GRAPH_FIX_TIME
                        .scope(
                            teaql_core::time::Timestamp::now(),
                            GRAPH_FIX_EVIDENCE.scope(
                                evidence.clone(),
                                Box::pin(save_ledger_entity_body(entity, root, context)),
                            ),
                        )
                        .await;
                    context.replace_last_fix_evidence(evidence.lock().unwrap().clone());
                    return result;
                }
                let mut node = graph_node_from_entity(context, entity)?;
                preflight_graph(context, &mut node, &ObjectLocation::root(), None)?;
                let saver = context
                    .require_resource::<Arc<dyn DynGraphSaver>>()
                    .map_err(|e| {
                        RuntimeError::Graph(format!(
                            "no DynGraphSaver registered — did you call register_executor()? ({})",
                            e
                        ))
                    })?;
                let saved = saver.save_graph_dyn(context, node).await?;
                T::from_compact_row(teaql_core::CompactRow::from_map(saved.values.into()))
                    .map_err(|e| RuntimeError::Graph(e.to_string()))
            }),
        )
    }
}

/// Persist an audited generated entity, including pending ledger changes that
/// may span multiple related entities sharing the same [`EntityRuntimeState`](crate::EntityRuntimeState).
///
/// Generated service crates use this as the implementation behind
/// `entity.audit_as("why").save(&context)`. The audited wrapper is required by the
/// function signature; no unaudited entity write entry point is exposed.
#[doc(hidden)]
pub async fn save_audited_ledger_entity<T>(
    audited: teaql_core::Audited<T>,
    context: &UserContext,
) -> Result<T, RuntimeError>
where
    T: crate::LedgerEntity + Send + 'static,
{
    let evidence = Arc::new(std::sync::Mutex::new(Vec::new()));
    let result = context
        .observe_mutation_request(
            || T::entity_descriptor().name,
            GRAPH_FIX_TIME.scope(
                teaql_core::time::Timestamp::now(),
                GRAPH_FIX_EVIDENCE.scope(
                    evidence.clone(),
                    save_audited_ledger_entity_inner(audited, context),
                ),
            ),
        )
        .await;
    context.replace_last_fix_evidence(evidence.lock().unwrap().clone());
    result
}

/// Persist an audited generated entity through an executor that is already
/// bound to an outer transaction.
///
/// This function never commits or rolls back the executor. The returned
/// mutation ledger must only be cleared after the owner commits the enclosing
/// transaction; retaining it on rollback keeps the mutation intent retryable.
/// Native infrastructure executors require the completion-aware overload below.
#[doc(hidden)]
pub async fn save_audited_ledger_entity_with_executor<T, E>(
    audited: teaql_core::Audited<T>,
    context: &UserContext,
    executor: &E,
) -> Result<(T, Option<crate::EntityRuntimeState>), RuntimeError>
where
    T: crate::LedgerEntity + Send + 'static,
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync,
{
    if teaql_data_service::DataServiceExecutor::infrastructure_sql(executor).is_some() {
        return Err(RuntimeError::Transaction(
            "NATIVE_RAW_EXECUTOR_REQUIRES_COMMIT_AUDIT_OWNER".into(),
        ));
    }
    save_audited_ledger_entity_with_executor_on_owner(audited, context, executor, None).await
}

/// Save through a caller-owned native transaction, delivering audit only after
/// its physical COMMIT. The caller still owns commit/rollback; cancellation,
/// save failure and an unacknowledged readback veto a later COMMIT. The owned
/// context remains available to the completion participant until settlement.
pub async fn save_audited_ledger_entity_with_native_executor<T, E>(
    audited: teaql_core::Audited<T>,
    context: Arc<UserContext>,
    executor: &E,
) -> Result<T, RuntimeError>
where
    T: crate::LedgerEntity + Send + 'static,
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync,
{
    let owner = teaql_data_service::DataServiceExecutor::infrastructure_sql(executor)
        .and_then(|sql| sql.transaction_identity())
        .ok_or_else(|| RuntimeError::Transaction("NATIVE_PHYSICAL_TRANSACTION_REQUIRED".into()))?;
    let audit = crate::commit_audit::NativeAuditCompletion::enlist(context.clone(), &owner)?;
    let (result, events) = crate::commit_audit::collect(
        &context,
        save_audited_ledger_entity_with_executor_on_owner(
            audited,
            &context,
            executor,
            Some(&owner),
        ),
    )
    .await;
    match result {
        Ok((entity, None)) => {
            audit.ready(events)?;
            Ok(entity)
        }
        Ok((_, Some(_))) => {
            audit.abort();
            Err(RuntimeError::Transaction(
                "NATIVE_PHYSICAL_TRANSACTION_REQUIRED".into(),
            ))
        }
        Err(error) => {
            audit.abort();
            Err(error)
        }
    }
}

pub(crate) async fn save_audited_ledger_entity_with_executor_on_owner<T, E>(
    audited: teaql_core::Audited<T>,
    context: &UserContext,
    executor: &E,
    physical_owner: Option<&teaql_data_service::infrastructure_sql::PhysicalTransactionIdentity>,
) -> Result<(T, Option<crate::EntityRuntimeState>), RuntimeError>
where
    T: crate::LedgerEntity + Send + 'static,
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync,
{
    let evidence = Arc::new(std::sync::Mutex::new(Vec::new()));
    let result = context
        .observe_mutation_request(
            || T::entity_descriptor().name,
            GRAPH_FIX_TIME.scope(
                teaql_core::time::Timestamp::now(),
                GRAPH_FIX_EVIDENCE.scope(
                    evidence.clone(),
                    save_audited_ledger_entity_with_executor_inner(
                        audited,
                        context,
                        executor,
                        physical_owner,
                    ),
                ),
            ),
        )
        .await;
    context.replace_last_fix_evidence(evidence.lock().unwrap().clone());
    result
}

async fn save_audited_ledger_entity_with_executor_inner<T, E>(
    audited: teaql_core::Audited<T>,
    context: &UserContext,
    executor: &E,
    physical_owner: Option<&teaql_data_service::infrastructure_sql::PhysicalTransactionIdentity>,
) -> Result<(T, Option<crate::EntityRuntimeState>), RuntimeError>
where
    T: crate::LedgerEntity + Send + 'static,
    E: teaql_data_service::QueryExecutor + teaql_data_service::MutationExecutor + Send + Sync,
{
    let entity = audited.into_entity();
    let root = entity.entity_runtime_state();
    if let Some(root) = root.as_ref() {
        if let Some(owner) = physical_owner {
            root.require_submission_owner(owner)?;
        } else if let Some(error) = root.first_composition_error() {
            return Err(RuntimeError::Graph(format!(
                "generated entity graph attachment failed before save: {error}"
            )));
        }
    }
    if root
        .as_ref()
        .is_some_and(|root| root.has_pending_dynamic_mutations())
    {
        context.dynamic_graph_provider()?;
    }
    let original_relation_fields = loaded_relation_fields(&entity);
    let mut node = extract_graph_node(context, entity)?;

    if let Some(root) = root {
        let mut draft = physical_owner
            .map(|owner| root.reserve_submission(Some(owner)))
            .transpose()?;
        let execution_root = draft
            .as_ref()
            .map_or_else(|| root.clone(), |draft| draft.execution_state().clone());
        let root_id = node.values.get("id").cloned().unwrap_or(Value::I64(0));
        let root_key =
            execution_root.original_key_for(&crate::EntityKey::new(node.entity.clone(), root_id));
        reject_cancelled_new_root(&execution_root, &root_key)?;
        if let Some(changes) = execution_root.current_change_set().changes().get(&root_key) {
            let descriptor = context.require_entity(&node.entity)?;
            node.values
                .extend(storage_values_from_ledger(descriptor, changes)?);
        }
        let mut visited = BTreeSet::from([root_key.clone()]);
        hydrate_ledger_relations(context, &execution_root, &mut node, &mut visited)?;
        preflight_graph(
            context,
            &mut node,
            &ObjectLocation::root(),
            Some(&execution_root),
        )?;
        merge_relation_mutations_into_root(context, &execution_root, &node)?;
        preflight_pending_dynamic_entities(context, &execution_root, &node)?;
        capture_ledger_trace_scopes(&execution_root, &node)?;
        let has_ledger_changes = !execution_root.current_change_set().is_empty()
            || !execution_root.deleted_keys().is_empty()
            || !execution_root.new_keys().is_empty();
        if has_ledger_changes {
            if let Some(draft) = draft.as_mut() {
                draft.seal_reviewed_intent()?;
            }
            let entity_name = node.entity.clone();
            let descriptor = context.require_entity(&entity_name)?;
            let preserve_relations =
                can_preserve_loaded_relations(&execution_root, &root_key, descriptor);
            let id_property = descriptor.id_property().ok_or_else(|| {
                RuntimeError::Graph(format!("entity {entity_name} has no id property"))
            })?;
            let data_service =
                crate::EntityDataService::for_executor(context, &entity_name, executor)
                    .with_ledger_privacy(&execution_root)?
                    .with_mutation_intent(teaql_core::MutationIntent::from_optional(
                        node.comment.as_deref(),
                    )?);
            let locations = ledger_object_locations_for(&execution_root, &node);
            let policy_plan = crate::ledger_policy_plan(context, &node, &execution_root);
            let binding_available = physical_owner.is_some()
                && teaql_data_service::DataServiceExecutor::infrastructure_sql(executor).is_some()
                && context
                    .get_resource::<crate::BlobMutationParticipantResource>()
                    .is_some();
            let governance = context
                .review_mutation_plan_with_blob_participant(&policy_plan, binding_available)?;
            let blob_plan = approved_blob_plan(context, &policy_plan)?;
            let submission = if let Some(owner) = physical_owner {
                Some(
                    draft
                        .take()
                        .expect("physical owner has a draft")
                        .attach(owner)?,
                )
            } else {
                None
            };
            let submitted_root = submission.as_ref().map_or_else(
                || execution_root.clone(),
                |submission| submission.execution_state().clone(),
            );
            let generated_ids = crate::with_mutation_governance(
                governance,
                data_service.execute_ledger_plan_internal_with_blob(
                    submitted_root.clone(),
                    &locations,
                    blob_plan,
                ),
            )
            .await
            .map_err(data_service_error_into_runtime)?;

            if let Some(readbacks) = &generated_ids.dynamic_readbacks {
                node.dynamic_fields = readbacks
                    .get(&root_key)
                    .cloned()
                    .map(Box::new)
                    .or(node.dynamic_fields.take());
            }
            if let Some(new_id) = generated_ids.get(&root_key) {
                node.values.insert(id_property.name.clone(), new_id.clone());
            }
            // Reuse the authoritative transaction-owned row, including a
            // soft-delete tombstone. Only an unmodified root needs a separate
            // current-row query.
            if let Some(snapshot) = generated_ids.canonical_rows.get(&root_key).cloned() {
                node.values = BTreeMap::from(snapshot).into();
            } else {
                let persisted_id =
                    node.values.get(&id_property.name).cloned().ok_or_else(|| {
                        RuntimeError::Graph(format!(
                            "saved {entity_name} missing identity field {}",
                            id_property.name
                        ))
                    })?;
                node.values = data_service
                    .fetch_graph_current_row_internal(
                        &entity_name,
                        &id_property.name,
                        &persisted_id,
                        submitted_root.get_trace_chain(&root_key),
                    )
                    .await
                    .map_err(data_service_error_into_runtime)?
                    .map(Into::into)
                    .ok_or_else(|| {
                        RuntimeError::Graph(format!(
                            "persisted {entity_name} record could not be read back"
                        ))
                    })?;
            }
            let entity = if submission.is_some() {
                let saved_context = crate::SavedEntityContext::new(&root, preserve_relations);
                decode_saved_ledger_entity::<T>(
                    node,
                    &saved_context,
                    preserve_relations,
                    &original_relation_fields,
                )?
            } else {
                decode_saved_ledger_entity::<T>(
                    node,
                    &root,
                    preserve_relations,
                    &original_relation_fields,
                )?
            };
            if let Some(submission) = submission {
                let (rows, allocated_ids, dynamic_rows) = generated_ids.into_submission_parts();
                submission.acknowledge_with_readbacks(rows, allocated_ids, dynamic_rows)?;
                return Ok((entity, None));
            }
            return Ok((entity, Some(root)));
        }
    }

    preflight_graph(context, &mut node, &ObjectLocation::root(), None)?;
    let entity_name = node.entity.clone();
    let data_service = crate::EntityDataService::for_executor(context, entity_name, executor);
    let plan = data_service
        .plan_graph(node)
        .await
        .map_err(data_service_error_into_runtime)?;
    let policy_plan = crate::graph_policy_plan(context, &plan)?;
    let binding_available = physical_owner.is_some()
        && teaql_data_service::DataServiceExecutor::infrastructure_sql(executor).is_some()
        && context
            .get_resource::<crate::BlobMutationParticipantResource>()
            .is_some();
    let governance =
        context.review_mutation_plan_with_blob_participant(&policy_plan, binding_available)?;
    let blob_plan = approved_blob_plan(context, &policy_plan)?;
    let saved = crate::with_mutation_governance(governance, async {
        let prepared_blob = if let Some(blob_plan) = blob_plan {
            let sql = teaql_data_service::DataServiceExecutor::infrastructure_sql(executor)
                .ok_or_else(|| {
                    DataServiceError::Runtime(RuntimeError::Transaction(
                        "BLOB_TRANSACTION_REQUIRED".to_owned(),
                    ))
                })?;
            let resolved_ids = BTreeMap::new();
            Some(
                blob_plan
                    .prepare(context, sql, &resolved_ids)
                    .await
                    .map_err(DataServiceError::Runtime)?,
            )
        } else {
            None
        };
        data_service
            .execute_graph_plan_internal_with_before_readback(plan, || async {
                if let Some(prepared_blob) = prepared_blob {
                    let sql = teaql_data_service::DataServiceExecutor::infrastructure_sql(executor)
                        .expect("prepared Blob binding requires the same SQL owner");
                    prepared_blob
                        .apply(context, sql)
                        .await
                        .map_err(DataServiceError::Runtime)?;
                }
                Ok(())
            })
            .await
    })
    .await
    .map_err(data_service_error_into_runtime)?;
    let entity = T::from_compact_row(teaql_core::CompactRow::from_map(saved.values.into()))
        .map_err(|error| RuntimeError::Graph(error.to_string()))?;
    Ok((entity, None))
}

fn data_service_error_into_runtime<E: std::error::Error>(
    error: DataServiceError<E>,
) -> RuntimeError {
    match error {
        DataServiceError::Runtime(error) => error,
        other => RuntimeError::Graph(other.to_string()),
    }
}

async fn save_audited_ledger_entity_inner<T>(
    audited: teaql_core::Audited<T>,
    context: &UserContext,
) -> Result<T, RuntimeError>
where
    T: crate::LedgerEntity + Send + 'static,
{
    let entity = audited.into_entity();
    let root = entity.entity_runtime_state();
    save_ledger_entity_body(entity, root, context).await
}

async fn save_ledger_entity_body<T: Entity + Send + 'static>(
    entity: T,
    root: Option<crate::EntityRuntimeState>,
    context: &UserContext,
) -> Result<T, RuntimeError> {
    if let Some(error) = root
        .as_ref()
        .and_then(|root| root.first_composition_error())
    {
        return Err(RuntimeError::Graph(format!(
            "generated entity graph attachment failed before save: {error}"
        )));
    }
    if root
        .as_ref()
        .is_some_and(|root| root.has_pending_dynamic_mutations())
    {
        context.dynamic_graph_provider()?;
    }
    let original_relation_fields = loaded_relation_fields(&entity);
    let mut node = extract_graph_node(context, entity)?;
    let saver = context
        .require_resource::<Arc<dyn DynGraphSaver>>()
        .map_err(|e| {
            RuntimeError::Graph(format!(
                "no DynGraphSaver registered — did you call register_executor()? ({e})"
            ))
        })?;

    if let Some(root) = root {
        let mut draft = root.reserve_submission(None)?;
        let execution_root = draft.execution_state().clone();
        let root_id = node.values.get("id").cloned().unwrap_or(Value::I64(0));
        let root_key =
            execution_root.original_key_for(&crate::EntityKey::new(node.entity.clone(), root_id));
        reject_cancelled_new_root(&execution_root, &root_key)?;
        if let Some(changes) = execution_root.current_change_set().changes().get(&root_key) {
            let descriptor = context.require_entity(&node.entity)?;
            node.values
                .extend(storage_values_from_ledger(descriptor, changes)?);
        }
        let mut visited = BTreeSet::from([root_key.clone()]);
        hydrate_ledger_relations(context, &execution_root, &mut node, &mut visited)?;
        preflight_graph(
            context,
            &mut node,
            &ObjectLocation::root(),
            Some(&execution_root),
        )?;
        merge_relation_mutations_into_root(context, &execution_root, &node)?;
        preflight_pending_dynamic_entities(context, &execution_root, &node)?;
        capture_ledger_trace_scopes(&execution_root, &node)?;
        let has_ledger_changes = !execution_root.current_change_set().is_empty()
            || !execution_root.deleted_keys().is_empty()
            || !execution_root.new_keys().is_empty();
        if has_ledger_changes {
            draft.seal_reviewed_intent()?;
            let descriptor = context.require_entity(&node.entity)?;
            let preserve_relations =
                can_preserve_loaded_relations(&execution_root, &root_key, descriptor);
            let decode_root = root.clone();
            let decode: SavedEntityDecoder = Box::new(move |saved, native| {
                let decoded = if native {
                    let binding = crate::SavedEntityContext::new(&decode_root, preserve_relations);
                    decode_saved_ledger_entity::<T>(
                        saved,
                        &binding,
                        preserve_relations,
                        &original_relation_fields,
                    )?
                } else {
                    decode_saved_ledger_entity::<T>(
                        saved,
                        &decode_root,
                        preserve_relations,
                        &original_relation_fields,
                    )?
                };
                Ok(Box::new(decoded))
            });
            let saved = saver
                .save_ledger_typed_dyn(context, node, root, draft, decode)
                .await?;
            return saved
                .downcast::<T>()
                .map(|saved| *saved)
                .map_err(|_| RuntimeError::Graph("SAVE_HYDRATION_TYPE_MISMATCH".into()));
        }
        drop(draft);
        let saved = saver.save_graph_dyn(context, node).await?;
        return decode_saved_ledger_entity::<T>(
            saved,
            &crate::EntityRuntimeState::default(),
            false,
            &[],
        );
    }

    preflight_graph(context, &mut node, &ObjectLocation::root(), None)?;
    let saved = saver.save_graph_dyn(context, node).await?;
    decode_saved_ledger_entity::<T>(saved, &crate::EntityRuntimeState::default(), false, &[])
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use teaql_core::{Entity, Record, TeaqlEntity, Value};
    use teaql_macros::{TeaqlEntity as DeriveTeaqlEntity, teaql_entity};

    use super::*;
    use crate::{
        CheckObjectStatus, CheckResults, InMemoryCheckerRegistry, InMemoryMetadataStore,
        TypedChecker, TypedEntityChecker,
    };

    #[teaql_entity]
    #[derive(Clone, Debug, PartialEq, DeriveTeaqlEntity)]
    #[teaql(entity = "DirtyTrackedOrder", table = "dirty_tracked_order")]
    struct DirtyTrackedOrder {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        code: String,
    }

    struct DirtyFieldProbe(Arc<AtomicBool>);

    #[derive(Debug, DeriveTeaqlEntity)]
    #[teaql(entity = "PlainReadback", table = "plain_readback")]
    struct PlainReadback {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        code: String,
    }

    #[test]
    fn native_saved_decoder_rejects_missing_binding_and_mismatched_context() {
        let root = crate::EntityRuntimeState::default();
        let context = crate::SavedEntityContext::new(&root, false);
        let node = GraphNode::new("PlainReadback")
            .value("id", 42_u64)
            .value("version", 1_i64)
            .value("code", "SQL");
        let error = decode_saved_ledger_entity::<PlainReadback>(node.clone(), &context, false, &[])
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("SAVE_HYDRATION_BINDING_REQUIRED")
        );
        let error =
            decode_saved_ledger_entity::<PlainReadback>(node, &context, true, &[]).unwrap_err();
        assert!(error.to_string().contains("SAVE_HYDRATION_CONTEXT_INVALID"));
    }

    #[test]
    fn native_saved_decoder_retains_the_actual_journal_and_pretransaction_baseline() {
        use teaql_data_service::infrastructure_sql::SqlResourceIdentity;
        let loaded =
            DirtyTrackedOrder::from_compact_row(teaql_core::CompactRow::from_map(Record::from([
                ("id".into(), Value::U64(42)),
                ("version".into(), Value::I64(1)),
                ("code".into(), Value::from("before")),
            ])))
            .unwrap();
        let root = loaded.__teaql_runtime_state().clone();
        let key = crate::EntityKey::new_static("DirtyTrackedOrder", 42_u64);
        root.set(key.clone(), "code", "submitted");
        let owner = SqlResourceIdentity::new().begin();
        root.begin_submission(&owner.identity())
            .unwrap()
            .acknowledge([(
                key.clone(),
                teaql_core::EntitySnapshot::from(Record::from([
                    ("id".into(), Value::U64(42)),
                    ("version".into(), Value::I64(2)),
                    ("code".into(), Value::from("submitted")),
                ])),
            )])
            .unwrap();
        root.set(key.clone(), "code", "late");
        let node = GraphNode::new("DirtyTrackedOrder")
            .value("id", 42_u64)
            .value("version", 2_i64)
            .value("code", "submitted");
        let context = crate::SavedEntityContext::new(&root, false);
        let saved =
            decode_saved_ledger_entity::<DirtyTrackedOrder>(node, &context, false, &[]).unwrap();
        assert_eq!(saved.__teaql_runtime_state(), &root);
        assert!(saved.__teaql_runtime_state().has_pending_submission());
        owner.rolled_back().unwrap();
        assert_eq!(
            saved.__teaql_runtime_state().get_original_version(&key),
            Some(1)
        );
        assert_eq!(
            saved
                .__teaql_runtime_state()
                .original_snapshot()
                .unwrap()
                .get("code"),
            Some(&Value::from("before"))
        );
        assert_eq!(root.get(&key, "code"), Some(Value::from("late")));
    }

    struct IntentGateNoIo;
    impl teaql_data_service::DataServiceExecutor for IntentGateNoIo {
        type Error = std::io::Error;
        fn capabilities(&self) -> teaql_data_service::DataServiceCapabilities {
            teaql_data_service::DataServiceCapabilities::default()
        }
    }
    impl teaql_data_service::QueryExecutor for IntentGateNoIo {
        async fn query(
            &self,
            _: teaql_data_service::QueryRequest,
        ) -> Result<teaql_data_service::QueryResult, Self::Error> {
            panic!("missing root intent reached query IO")
        }
    }
    impl teaql_data_service::MutationExecutor for IntentGateNoIo {
        async fn mutate(
            &self,
            _: teaql_data_service::MutationRequest,
        ) -> Result<teaql_data_service::MutationResult, Self::Error> {
            panic!("missing root intent reached mutation IO")
        }
    }
    impl teaql_data_service::TransactionExecutor for IntentGateNoIo {
        type Tx<'a> = Self;
        async fn begin(&self) -> Result<Self, Self::Error> {
            panic!("missing root intent allocated a transaction")
        }
    }
    impl teaql_data_service::Transaction for IntentGateNoIo {
        type Error = std::io::Error;
        async fn commit(self) -> Result<(), Self::Error> {
            panic!("unexpected commit")
        }
        async fn rollback(self) -> Result<(), Self::Error> {
            panic!("unexpected rollback")
        }
    }

    struct RejectOwnedLedger;
    impl crate::MutationPolicy for RejectOwnedLedger {
        fn identity(&self) -> crate::MutationPolicyIdentity {
            crate::MutationPolicyIdentity::new("reject-owned-ledger", "1", "sha256:reject-owned")
        }

        fn review(
            &self,
            _context: &UserContext,
            plan: &crate::MutationPlan,
        ) -> crate::MutationDecision {
            assert_eq!(plan.request_key, "DirtyTrackedOrder.saveGraph");
            assert_eq!(plan.operations.len(), 1);
            crate::MutationDecision::deny("OWNED-DENIED", "owned mutation denied", ["code"])
        }
    }

    struct CountOwnedChecks(Arc<AtomicUsize>);
    impl TypedChecker<DirtyTrackedOrder> for CountOwnedChecks {
        fn check_and_fix_typed(
            &self,
            _context: &UserContext,
            _entity: &mut DirtyTrackedOrder,
            _status: CheckObjectStatus,
            _location: &ObjectLocation,
            _results: &mut CheckResults,
        ) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn owned_typed_policy_denial_restores_draft_before_begin() {
        let loaded =
            DirtyTrackedOrder::from_compact_row(teaql_core::CompactRow::from_map(Record::from([
                ("id".into(), Value::U64(42)),
                ("version".into(), Value::I64(1)),
                ("code".into(), Value::from("before")),
            ])))
            .unwrap();
        let root = loaded.__teaql_runtime_state().clone();
        let key = crate::EntityKey::new_static("DirtyTrackedOrder", 42_u64);
        root.set(key.clone(), "code", "denied");
        let policy: Arc<dyn crate::MutationPolicy> = Arc::new(RejectOwnedLedger);
        let checker_calls = Arc::new(AtomicUsize::new(0));
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new().with_entity(DirtyTrackedOrder::entity_descriptor()),
            )
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(
                TypedEntityChecker::<DirtyTrackedOrder, _>::new(CountOwnedChecks(
                    checker_calls.clone(),
                )),
            ))
            .with_mutation_policy_registry(move |request_key: &str| {
                (request_key == "DirtyTrackedOrder.saveGraph").then(|| policy.clone())
            });
        context.register_executor(IntentGateNoIo);
        let error = teaql_core::Audited::new(loaded, "deny owned typed save")
            .save(&context)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("OWNED-DENIED"));
        assert!(!root.has_pending_submission());
        assert_eq!(checker_calls.load(Ordering::SeqCst), 1);
        assert_eq!(root.get_original_version(&key), Some(1));
        assert_eq!(root.get(&key, "code"), Some(Value::from("denied")));
    }

    #[tokio::test]
    async fn graph_and_ledger_savers_reject_missing_intent_before_policy_and_transaction() {
        for logging in [false, true] {
            let mut context = UserContext::new().with_mutation_policy_registry(
                |_: &str| -> Option<Arc<dyn crate::MutationPolicy>> {
                    panic!("missing root intent reached customer policy registry")
                },
            );
            if !logging {
                context.disable_sql_log();
            }
            context.register_executor(IntentGateNoIo);
            let saver = GraphSaverFor::<IntentGateNoIo>::new();
            for comment in [
                None,
                Some(""),
                Some(" \t\r\n"),
                Some("\u{85}"),
                Some("\u{a0}"),
                Some("\u{2003}"),
            ] {
                let mut node = GraphNode::new("DirtyTrackedOrder")
                    .value("id", 1_u64)
                    .relation(
                        "children",
                        GraphNode::new("Child").comment("valid child reason"),
                    );
                node.comment = comment.map(str::to_owned);
                for error in [
                    saver
                        .save_graph_dyn(&context, node.clone())
                        .await
                        .unwrap_err(),
                    saver
                        .save_ledger_dyn(&context, node, crate::EntityRuntimeState::default())
                        .await
                        .unwrap_err(),
                ] {
                    let RuntimeError::RequestIntent(error) = error else {
                        panic!("unexpected diagnostic: {error:?}")
                    };
                    assert_eq!(error.code(), "REQUEST_COMMENT_REQUIRED");
                    assert_eq!(error.field, "comment");
                    assert_eq!(error.request_kind, teaql_core::RequestKind::Mutation);
                }
            }
        }
    }

    impl TypedChecker<DirtyTrackedOrder> for DirtyFieldProbe {
        fn check_and_fix_typed(
            &self,
            _context: &UserContext,
            entity: &mut DirtyTrackedOrder,
            status: CheckObjectStatus,
            _location: &ObjectLocation,
            _results: &mut CheckResults,
        ) {
            let saw_code = status.is_update()
                && entity
                    .dirty_fields()
                    .is_some_and(|fields| fields.contains("code"));
            self.0.store(saw_code, Ordering::SeqCst);
        }
    }

    #[test]
    fn graph_preflight_preserves_dirty_fields_for_typed_checker() {
        let observed = Arc::new(AtomicBool::new(false));
        let context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new().with_entity(DirtyTrackedOrder::entity_descriptor()),
            )
            .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(
                TypedEntityChecker::<DirtyTrackedOrder, _>::new(DirtyFieldProbe(observed.clone())),
            ));
        let row = Record::from([
            ("id".to_owned(), Value::U64(7)),
            ("version".to_owned(), Value::I64(3)),
            ("code".to_owned(), Value::Text("ORD-OLD".to_owned())),
        ]);
        let entity = DirtyTrackedOrder::from_compact_row(teaql_core::CompactRow::from_map(row))
            .expect("materialize tracked entity");
        entity.__teaql_runtime_state().set(
            crate::EntityKey::new("DirtyTrackedOrder", 7_u64),
            "code",
            Value::Text("ORD-NEW".to_owned()),
        );

        let mut node = graph_node_from_entity(&context, entity).expect("build graph node");
        preflight_graph(&context, &mut node, &ObjectLocation::root(), None)
            .expect("preflight succeeds");

        assert!(observed.load(Ordering::SeqCst));
        assert!(!node.values.contains_key("_dirty_fields"));
    }
}
