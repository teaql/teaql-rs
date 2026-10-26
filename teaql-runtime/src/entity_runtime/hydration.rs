//! Save readback and query hydration have different mutation ownership.
use super::*;

/// Framework-only save hydration. The actual ledger is shared; row load state
/// belongs to the new entity view. No context/principal or commit authority.
#[doc(hidden)]
pub struct SavedEntityContext {
    state: EntityRuntimeState,
    preserve_relations: bool,
}
impl SavedEntityContext {
    pub fn new(root: &EntityRuntimeState, preserve_relations: bool) -> Self {
        Self {
            preserve_relations,
            state: EntityRuntimeState {
                inner: root.inner.clone(),
                graph: if preserve_relations {
                    root.graph.promote()
                } else {
                    EntityGraphReference::Strong(Arc::default())
                },
                json_view: if preserve_relations {
                    root.json_view
                } else {
                    0
                },
                loaded_snapshot: LoadedSnapshotCarrier::default(),
                dynamic_fields: None,
            },
        }
    }
    pub(crate) fn preserves_relations(&self) -> bool {
        self.preserve_relations
    }
    pub(crate) fn owns_ledger(&self, state: &EntityRuntimeState) -> bool {
        if self.state.inner.shares_slot(&state.inner) {
            return true;
        }
        match (self.state.inner.get(), state.inner.get()) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}
impl EntityRuntimeState {
    /// Derive bridge: ordinary query rows keep independent mutation ledgers.
    /// Only an explicit save hydration object shares submitted/live intent.
    #[doc(hidden)]
    pub fn from_hydration_context(context: Option<&dyn Any>) -> Self {
        if let Some(saved) = context.and_then(|value| value.downcast_ref::<SavedEntityContext>()) {
            saved.state.clone()
        } else {
            context
                .and_then(|value| value.downcast_ref::<Self>())
                .map(Self::fresh_with_shared_graph)
                .unwrap_or_default()
        }
    }
}

#[cfg(test)]
mod tests;
