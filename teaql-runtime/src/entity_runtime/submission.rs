//! Owned submitted intent. SQL execution uses a separate immutable capture; edits
//! made while it runs stay in the live ledger and survive physical completion.
use super::*;
use teaql_data_service::infrastructure_sql::{
    InfrastructureSqlError, PhysicalTransactionIdentity, PhysicalTransactionOutcome,
    PhysicalTransactionParticipant,
};

/// Rare authoritative readback payload, kept off ordinary mutable ledger frames.
#[derive(Clone, Debug, Default)]
pub(super) struct SubmittedRows {
    rows: BTreeMap<EntityKey, EntitySnapshot>,
    origins: BTreeMap<EntityKey, RowOrigin>,
}
#[derive(Clone, Debug)]
enum RowOrigin {
    Existing(Option<i64>),
    New,
}
impl SubmittedRows {
    fn baseline(ledger: &EntityMutationLedger) -> Box<Self> {
        let mut baseline = ledger.submitted_rows.clone().unwrap_or_default();
        for (key, snapshot) in &ledger.original_snapshots {
            let version = match snapshot {
                OriginalSnapshot::Materialized(row) => row.get("version").and_then(Value::try_i64),
                OriginalSnapshot::Compact(row) => row.get("version").and_then(Value::try_i64),
            };
            baseline
                .origins
                .entry(key.clone())
                .or_insert(RowOrigin::Existing(version));
        }
        for (key, version) in ledger
            .original_versions
            .first
            .iter()
            .map(|(key, version)| (key, version))
            .chain(ledger.original_versions.overflow.iter())
        {
            baseline
                .origins
                .entry(key.clone())
                .or_insert(RowOrigin::Existing(Some(*version)));
        }
        for key in &ledger.new_keys {
            baseline.rows.remove(key);
            baseline.origins.insert(key.clone(), RowOrigin::New);
        }
        baseline
    }
    pub(super) fn version(&self, key: &EntityKey) -> Option<Option<i64>> {
        self.origins.get(key).map(|origin| match origin {
            RowOrigin::Existing(version) => *version,
            RowOrigin::New => None,
        })
    }
    pub(super) fn snapshot(
        &self,
        key: &EntityKey,
        originals: &BTreeMap<EntityKey, OriginalSnapshot>,
    ) -> Option<Option<EntitySnapshot>> {
        if let Some(row) = self.rows.get(key) {
            return Some(Some(row.clone()));
        }
        match self.origins.get(key)? {
            RowOrigin::New => Some(None),
            RowOrigin::Existing(_) => Some(originals.get(key).map(|row| match row {
                OriginalSnapshot::Materialized(row) => row.clone(),
                OriginalSnapshot::Compact(row) => EntitySnapshot::from(row.clone().into_map()),
            })),
        }
    }
}
impl std::ops::Deref for SubmittedRows {
    type Target = BTreeMap<EntityKey, EntitySnapshot>;
    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}
impl std::ops::DerefMut for SubmittedRows {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rows
    }
}

#[derive(Clone, Debug)]
pub(super) struct PendingLedger {
    owner: PhysicalTransactionIdentity,
    batches: Vec<DetachedLedger>,
    active: Option<usize>,
    failed: bool,
    initial_versions: OriginalVersions,
    initial_rows: Option<Box<SubmittedRows>>,
}
#[derive(Clone, Debug)]
struct DetachedLedger {
    level: usize,
    intent: EntityMutationLedger,
}
impl PendingLedger {
    pub(super) fn protects_level(&self, level: usize) -> bool {
        self.batches.iter().any(|batch| batch.level >= level)
    }
    pub(super) fn get(&self, level: usize, key: &EntityKey, field: &str) -> Option<Value> {
        self.batches
            .iter()
            .rev()
            .filter(|batch| batch.level == level)
            .find_map(|batch| batch.intent.change_sets.current()?.get(key, field).cloned())
    }
}

/// Framework-only capture for execution on the supplied native physical owner.
/// It cannot acknowledge COMMIT. Dropping before acknowledge vetoes that owner.
#[doc(hidden)]
pub struct LedgerSubmission {
    root: MutationLedgerHandle,
    owner: PhysicalTransactionIdentity,
    batch: usize,
    execution: EntityRuntimeState,
    acknowledged: bool,
}
impl LedgerSubmission {
    pub fn execution_state(&self) -> &EntityRuntimeState {
        &self.execution
    }

    /// The caller supplies authoritative canonical rows read on the same SQL
    /// executor, after all submitted writes. This is not a client write API.
    pub fn acknowledge(
        mut self,
        rows: impl IntoIterator<Item = (EntityKey, EntitySnapshot)>,
    ) -> Result<(), crate::RuntimeError> {
        let mut ledger = self.root.lock().unwrap_or_else(|error| error.into_inner());
        let pending = ledger.pending.as_mut().ok_or_else(unavailable)?;
        if pending.owner != self.owner
            || pending.active != Some(self.batch)
            || self.owner.outcome() != PhysicalTransactionOutcome::Active
        {
            return Err(unavailable());
        }
        pending.active = None;
        for (key, row) in rows {
            let version = row.get("version").and_then(Value::try_i64);
            if let Some(version) = version {
                ledger.original_versions.insert(key.clone(), version);
            }
            let rows = ledger.submitted_rows.get_or_insert_with(Default::default);
            rows.origins
                .insert(key.clone(), RowOrigin::Existing(version));
            rows.rows.insert(key, row);
        }
        self.acknowledged = true;
        Ok(())
    }
}
impl Drop for LedgerSubmission {
    fn drop(&mut self) {
        if self.acknowledged {
            return;
        }
        let mut ledger = self.root.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(pending) = ledger
            .pending
            .as_mut()
            .filter(|pending| pending.owner == self.owner)
        {
            pending.active = None;
            pending.failed = true;
        }
    }
}

struct Completion {
    // The actual owner retains attempted intent even if all entity views are
    // dropped. Its terminal queue drain breaks this ownership cycle.
    root: MutationLedgerHandle,
    owner: PhysicalTransactionIdentity,
}
impl PhysicalTransactionParticipant for Completion {
    fn before_commit(&self) -> Result<(), InfrastructureSqlError> {
        let ledger = self.root.lock().unwrap_or_else(|error| error.into_inner());
        match ledger.pending.as_ref() {
            Some(pending)
                if pending.owner == self.owner && !pending.failed && pending.active.is_none() =>
            {
                Ok(())
            }
            _ => Err(InfrastructureSqlError::CompletionFailed),
        }
    }
    fn after_completion(
        &self,
        outcome: PhysicalTransactionOutcome,
    ) -> Result<(), InfrastructureSqlError> {
        let mut ledger = self.root.lock().unwrap_or_else(|error| error.into_inner());
        let Some(pending) = ledger.pending.take() else {
            return Ok(());
        };
        if pending.owner != self.owner {
            ledger.pending = Some(pending);
            return Err(InfrastructureSqlError::CompletionFailed);
        }
        let invalid_commit = outcome == PhysicalTransactionOutcome::Committed
            && (pending.failed || pending.active.is_some());
        // If restoration panics, a partially settled graph must stay fenced,
        // including acknowledged rollback. The owner still reports SQL truth.
        let was_fenced = ledger.completion_fenced;
        ledger.completion_fenced = true;
        if outcome != PhysicalTransactionOutcome::Committed || invalid_commit {
            restore(&mut ledger, *pending);
        }
        if outcome != PhysicalTransactionOutcome::Unknown && !invalid_commit {
            ledger.completion_fenced = was_fenced;
        }
        if invalid_commit {
            Err(InfrastructureSqlError::CompletionFailed)
        } else {
            Ok(())
        }
    }
}

impl EntityRuntimeState {
    /// Verify provenance before preflight/Checker/Fix or provider I/O. An
    /// uncertain graph cannot be made retryable by clearing or replacing intent.
    #[doc(hidden)]
    pub fn require_submission_owner(
        &self,
        owner: &PhysicalTransactionIdentity,
    ) -> Result<(), crate::RuntimeError> {
        self.read_context(Ok(()), |ledger| validate(ledger, owner))?;
        owner.require_active().map_err(|_| unavailable())
    }

    /// Capture the current stack level and lifecycle intent on one real owner.
    /// The frozen execution view has its own ledger; no live edits leak into SQL.
    #[doc(hidden)]
    pub fn begin_submission(
        &self,
        owner: &PhysicalTransactionIdentity,
    ) -> Result<LedgerSubmission, crate::RuntimeError> {
        let root = self.context().clone();
        let mut ledger = root.lock().unwrap_or_else(|error| error.into_inner());
        validate(&ledger, owner)?;
        owner.require_active().map_err(|_| unavailable())?;
        if ledger.pending.is_none() {
            owner
                .enlist(Arc::new(Completion {
                    root: root.clone(),
                    owner: owner.clone(),
                }))
                .map_err(|_| unavailable())?;
            ledger.pending = Some(Box::new(PendingLedger {
                owner: owner.clone(),
                batches: Vec::new(),
                active: None,
                failed: false,
                initial_versions: ledger.original_versions.clone(),
                initial_rows: Some(SubmittedRows::baseline(&ledger)),
            }));
        }
        ledger.change_sets.current_mut();
        let level = ledger.change_sets.stack.len() - 1;
        // Never recursively copy earlier submissions or their owner handles.
        let mut pending = ledger.pending.take().expect("validated submission owner");
        let captured = ledger.clone();
        let execution = EntityRuntimeState {
            inner: LazyLedgerSlot::default(),
            graph: self.graph.promote(),
            json_view: self.json_view,
            loaded_snapshot: self.loaded_snapshot.clone(),
            dynamic_fields: self.dynamic_fields.clone(),
        };
        execution
            .inner
            .get_or_init(|| Arc::new(Mutex::new(captured.clone())));
        ledger.change_sets.clear_current();
        ledger.new_keys.clear();
        ledger.deleted_keys.clear();
        ledger.comment = None;
        ledger.entity_comments.clear();
        // Trace/snapshot provenance stays available to later edits. The capture
        // owns its immutable copy for the in-flight executor.
        let batch = pending.batches.len();
        pending.batches.push(DetachedLedger {
            level,
            intent: captured,
        });
        pending.active = Some(batch);
        ledger.pending = Some(pending);
        drop(ledger);
        Ok(LedgerSubmission {
            root,
            owner: owner.clone(),
            batch,
            execution,
            acknowledged: false,
        })
    }

    #[doc(hidden)]
    pub fn has_pending_submission(&self) -> bool {
        self.read_context(false, |ledger| ledger.pending.is_some())
    }
    #[doc(hidden)]
    pub fn requires_reload_after_completion(&self) -> bool {
        self.read_context(false, |ledger| ledger.completion_fenced)
    }
}
fn unavailable() -> crate::RuntimeError {
    crate::RuntimeError::Transaction("LEDGER_PHYSICAL_COMPLETION_REQUIRED".into())
}
fn validate(
    ledger: &EntityMutationLedger,
    owner: &PhysicalTransactionIdentity,
) -> Result<(), crate::RuntimeError> {
    if ledger.completion_fenced
        || !ledger.composition_errors.is_empty()
        || ledger.pending.as_ref().is_some_and(|pending| {
            pending.owner != *owner || pending.active.is_some() || pending.failed
        })
    {
        Err(unavailable())
    } else {
        Ok(())
    }
}
fn merge_change_set(target: &mut EntityChangeSet, source: &EntityChangeSet) {
    for (key, values) in &source.changes {
        for (field, value) in values {
            target.set(key.clone(), field.clone(), value.clone());
        }
    }
    if let Some(dynamic) = &source.dynamic_changes {
        for (key, fields) in dynamic.iter() {
            let metadata = source
                .dynamic_metadata
                .as_ref()
                .expect("typed dynamic provenance");
            for (code, mutation) in fields {
                target.set_dynamic(
                    key.clone(),
                    code.clone(),
                    mutation.clone(),
                    metadata.definitions[key].clone(),
                    metadata
                        .originals
                        .get(key)
                        .and_then(|values| values.get(code))
                        .cloned()
                        .flatten(),
                );
            }
        }
    }
}
fn restore(ledger: &mut EntityMutationLedger, pending: PendingLedger) {
    let live_changes = std::mem::take(&mut ledger.change_sets);
    let live_new = std::mem::take(&mut ledger.new_keys);
    let live_deleted = std::mem::take(&mut ledger.deleted_keys);
    let live_comment = ledger.comment.take();
    let live_reasons = std::mem::take(&mut ledger.entity_comments);
    for batch in pending.batches {
        while ledger.change_sets.stack.len() <= batch.level {
            ledger.change_sets.push();
        }
        if let Some(changes) = batch.intent.change_sets.current() {
            merge_change_set(&mut ledger.change_sets.stack[batch.level], changes);
        }
        ledger.new_keys.extend(batch.intent.new_keys);
        ledger.deleted_keys.extend(batch.intent.deleted_keys);
        ledger.comment = batch.intent.comment.or(ledger.comment.take());
        ledger.entity_comments.extend(batch.intent.entity_comments);
    }
    for (level, changes) in live_changes.stack.iter().enumerate() {
        while ledger.change_sets.stack.len() <= level {
            ledger.change_sets.push();
        }
        merge_change_set(&mut ledger.change_sets.stack[level], changes);
    }
    ledger.new_keys.extend(live_new);
    ledger.deleted_keys.extend(live_deleted);
    ledger.comment = live_comment.or(ledger.comment.take());
    ledger.entity_comments.extend(live_reasons);
    ledger.original_versions = pending.initial_versions;
    ledger.submitted_rows = pending.initial_rows;
}

#[cfg(test)]
mod tests;
