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
    /// A returned entity view may carry the allocated ID while its original
    /// ledger key remains stable for rollback and late edits.
    aliases: BTreeMap<EntityKey, EntityKey>,
    pub(super) persisted_ids: BTreeMap<EntityKey, Value>,
    dynamic: BTreeMap<EntityKey, SubmittedDynamicRow>,
}
#[derive(Clone, Debug, Default)]
struct SubmittedDynamicRow {
    // Keep each independently read field across partial later readbacks. A
    // deleted field is known with None, distinct from an unobserved code.
    values: BTreeMap<String, Option<Value>>,
}
#[derive(Clone, Debug)]
enum RowOrigin {
    Existing(Option<i64>),
    New,
}
impl SubmittedRows {
    pub(super) fn original_key<'a>(&'a self, key: &'a EntityKey) -> &'a EntityKey {
        self.aliases.get(key).unwrap_or(key)
    }
    pub(super) fn persisted_id(&self, key: &EntityKey) -> Value {
        let original = self.original_key(key);
        self.persisted_ids
            .get(original)
            .cloned()
            .unwrap_or_else(|| original.id.clone())
    }
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
        self.origins
            .get(self.original_key(key))
            .map(|origin| match origin {
                RowOrigin::Existing(version) => *version,
                RowOrigin::New => None,
            })
    }
    pub(super) fn snapshot(
        &self,
        key: &EntityKey,
        originals: &BTreeMap<EntityKey, OriginalSnapshot>,
    ) -> Option<Option<EntitySnapshot>> {
        let key = self.original_key(key);
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
#[derive(Clone, Debug)]
pub(super) struct DraftLedger {
    sequence: u64,
    level: usize,
    intent: EntityMutationLedger,
}
impl DraftLedger {
    pub(super) fn protects_level(&self, level: usize) -> bool {
        self.level >= level
    }
    pub(super) fn get(&self, level: usize, key: &EntityKey, field: &str) -> Option<Value> {
        (self.level == level)
            .then(|| self.intent.change_sets.current()?.get(key, field).cloned())
            .flatten()
    }
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

/// An ownerless capture for preflight and immutable policy review. Dropping it
/// before attachment restores the frozen intent without creating SQL uncertainty.
#[doc(hidden)]
pub struct LedgerDraft {
    root: MutationLedgerHandle,
    sequence: u64,
    execution: Option<EntityRuntimeState>,
}
impl LedgerDraft {
    pub fn execution_state(&self) -> &EntityRuntimeState {
        self.execution.as_ref().expect("unattached draft")
    }

    /// Record trusted Checker/Fix changes made on the isolated execution view
    /// before constructing the immutable policy plan. Later business edits on
    /// the live root remain outside this reviewed submission.
    pub fn seal_reviewed_intent(&mut self) -> Result<(), crate::RuntimeError> {
        let execution = self.execution.as_ref().ok_or_else(unavailable)?;
        let captured = execution
            .context()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if captured.pending.is_some() || captured.draft.is_some() || captured.completion_fenced {
            return Err(unavailable());
        }
        let mut ledger = self.root.lock().unwrap_or_else(|error| error.into_inner());
        let draft = ledger
            .draft
            .as_mut()
            .filter(|draft| draft.sequence == self.sequence)
            .ok_or_else(unavailable)?;
        draft.intent = captured;
        Ok(())
    }

    /// Attach only after the real provider has opened its physical transaction.
    /// No synthetic owner is accepted or created by the draft.
    pub fn attach(
        mut self,
        owner: &PhysicalTransactionIdentity,
    ) -> Result<LedgerSubmission, crate::RuntimeError> {
        let mut ledger = self.root.lock().unwrap_or_else(|error| error.into_inner());
        let Some(draft) = ledger.draft.as_ref() else {
            return Err(unavailable());
        };
        if draft.sequence != self.sequence {
            return Err(unavailable());
        }
        validate_owner(&ledger, owner)?;
        owner.require_active().map_err(|_| unavailable())?;
        if ledger.pending.is_none() {
            owner
                .enlist(Arc::new(Completion {
                    root: self.root.clone(),
                    owner: owner.clone(),
                }))
                .map_err(|_| unavailable())?;
            ledger.pending = Some(Box::new(PendingLedger {
                owner: owner.clone(),
                batches: Vec::new(),
                active: None,
                failed: false,
                initial_versions: draft.intent.original_versions.clone(),
                initial_rows: Some(SubmittedRows::baseline(&draft.intent)),
            }));
        }
        let draft = ledger.draft.take().expect("validated draft");
        let pending = ledger.pending.as_mut().expect("actual physical owner");
        let batch = pending.batches.len();
        pending.batches.push(DetachedLedger {
            level: draft.level,
            intent: draft.intent,
        });
        pending.active = Some(batch);
        let execution = self.execution.take().expect("unattached draft");
        drop(ledger);
        Ok(LedgerSubmission {
            root: self.root.clone(),
            owner: owner.clone(),
            batch,
            execution,
            acknowledged: false,
        })
    }
}
impl Drop for LedgerDraft {
    fn drop(&mut self) {
        if self.execution.is_none() {
            return;
        }
        let mut ledger = self.root.lock().unwrap_or_else(|error| error.into_inner());
        if ledger
            .draft
            .as_ref()
            .is_some_and(|draft| draft.sequence == self.sequence)
        {
            let draft = ledger.draft.take().expect("current draft");
            restore_draft(&mut ledger, *draft);
        }
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
        self,
        rows: impl IntoIterator<Item = (EntityKey, EntitySnapshot)>,
    ) -> Result<(), crate::RuntimeError> {
        self.acknowledge_with_allocated_ids(rows, std::iter::empty())
    }

    /// Bind SQL-allocated identities to their original ledger keys. Both the
    /// old and returned entity handles then address the same submitted row.
    #[doc(hidden)]
    pub fn acknowledge_with_allocated_ids(
        self,
        rows: impl IntoIterator<Item = (EntityKey, EntitySnapshot)>,
        allocated_ids: impl IntoIterator<Item = (EntityKey, Value)>,
    ) -> Result<(), crate::RuntimeError> {
        self.acknowledge_with_readbacks(rows, allocated_ids, std::iter::empty())
    }

    /// Acknowledge authoritative extension values together with canonical
    /// rows. The extension payload is required for every executed dynamic
    /// write, including a delete whose readback no longer contains the code.
    #[doc(hidden)]
    pub fn acknowledge_with_readbacks(
        mut self,
        rows: impl IntoIterator<Item = (EntityKey, EntitySnapshot)>,
        allocated_ids: impl IntoIterator<Item = (EntityKey, Value)>,
        dynamic_rows: impl IntoIterator<
            Item = (EntityKey, teaql_core::dynamic_fields::DynamicFieldValues),
        >,
    ) -> Result<(), crate::RuntimeError> {
        let mut readbacks = BTreeMap::new();
        for (key, row) in rows {
            if readbacks.insert(key, row).is_some() {
                return Err(unavailable());
            }
        }
        let mut allocated = BTreeMap::new();
        for (key, id) in allocated_ids {
            if allocated.insert(key, id).is_some() {
                return Err(unavailable());
            }
        }
        let mut dynamic = BTreeMap::new();
        for (key, values) in dynamic_rows {
            if dynamic.insert(key, values).is_some() {
                return Err(unavailable());
            }
        }
        let mut ledger = self.root.lock().unwrap_or_else(|error| error.into_inner());
        let pending = ledger.pending.as_ref().ok_or_else(unavailable)?;
        if pending.owner != self.owner
            || pending.active != Some(self.batch)
            || self.owner.outcome() != PhysicalTransactionOutcome::Active
        {
            return Err(unavailable());
        }
        let intent = &pending.batches[self.batch].intent;
        let change_set = intent.change_sets.current().ok_or_else(unavailable)?;
        let expected_dynamic = change_set
            .dynamic_changes()
            .into_iter()
            .flat_map(|changes| changes.keys())
            .filter(|key| !intent.deleted_keys.contains(*key))
            .cloned()
            .collect::<BTreeSet<_>>();
        if dynamic.keys().cloned().collect::<BTreeSet<_>>() != expected_dynamic {
            return Err(unavailable());
        }
        let mut submitted_dynamic = BTreeMap::new();
        for (key, values) in dynamic {
            let changes = change_set
                .dynamic_changes()
                .and_then(|changes| changes.get(&key))
                .ok_or_else(unavailable)?;
            let definitions = change_set
                .dynamic_definitions(&key)
                .ok_or_else(unavailable)?;
            let returned_definitions = values.definitions();
            if definitions.owner_type() != returned_definitions.owner_type()
                || definitions.revision() != returned_definitions.revision()
                || definitions.fields() != returned_definitions.fields()
            {
                return Err(unavailable());
            }
            for (code, mutation) in changes {
                let found = values.values().contains_key(code);
                if matches!(
                    mutation,
                    teaql_core::dynamic_fields::DynamicFieldMutation::Set(_)
                ) != found
                {
                    return Err(unavailable());
                }
            }
            let mut known_codes = values
                .selected_codes()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            known_codes.extend(changes.keys().cloned());
            submitted_dynamic.insert(
                key,
                SubmittedDynamicRow {
                    values: known_codes
                        .into_iter()
                        .map(|code| {
                            let value = values.values().get(&code).cloned();
                            (code, value)
                        })
                        .collect(),
                },
            );
        }
        let mut canonical_owners = BTreeMap::new();
        for (key, row) in &readbacks {
            if let (Some(previous), Some(next)) =
                (pending_alias_id(&ledger, key), allocated.get(key))
                && EntityKey::new(key.entity.to_string(), previous.clone())
                    != EntityKey::new(key.entity.to_string(), next.clone())
            {
                return Err(unavailable());
            }
            let actual = allocated
                .get(key)
                .or_else(|| pending_alias_id(&ledger, key))
                .unwrap_or(&key.id);
            let returned_id = row.get("id").ok_or_else(unavailable)?;
            let canonical = EntityKey::new(key.entity.to_string(), actual.clone());
            if EntityKey::new(key.entity.to_string(), returned_id.clone()) != canonical
                || canonical_owners
                    .insert(canonical.clone(), key.clone())
                    .is_some()
                || ledger
                    .submitted_rows
                    .as_ref()
                    .and_then(|rows| rows.aliases.get(&canonical))
                    .is_some_and(|owner| owner != key)
                || (canonical != *key && claimed_elsewhere(&ledger, &canonical, key))
            {
                return Err(unavailable());
            }
        }
        if allocated.keys().any(|key| !readbacks.contains_key(key)) {
            return Err(unavailable());
        }
        ledger.pending.as_mut().expect("validated owner").active = None;
        for (key, row) in readbacks {
            let version = row.get("version").and_then(Value::try_i64);
            if let Some(version) = version {
                ledger.original_versions.insert(key.clone(), version);
            }
            let rows = ledger.submitted_rows.get_or_insert_with(Default::default);
            if let Some(id) = allocated.get(&key) {
                let canonical = EntityKey::new(key.entity.to_string(), id.clone());
                if canonical != key {
                    rows.aliases.insert(canonical, key.clone());
                    rows.persisted_ids.insert(key.clone(), id.clone());
                }
            }
            rows.origins
                .insert(key.clone(), RowOrigin::Existing(version));
            rows.rows.insert(key, row);
        }
        if !submitted_dynamic.is_empty() {
            let rows = ledger.submitted_rows.get_or_insert_with(Default::default);
            for (key, row) in submitted_dynamic {
                rows.dynamic
                    .entry(key)
                    .or_default()
                    .values
                    .extend(row.values);
            }
        }
        self.acknowledged = true;
        Ok(())
    }
}
impl EntityRuntimeState {
    pub(crate) fn submitted_dynamic_original_for(
        &self,
        key: &EntityKey,
        code: &str,
    ) -> Option<Option<Value>> {
        self.read_context(None, |ledger| {
            let key = ledger.original_key(key);
            let row = ledger.submitted_rows.as_ref()?.dynamic.get(&key)?;
            row.values.get(code).cloned()
        })
    }
}
fn pending_alias_id<'a>(ledger: &'a EntityMutationLedger, key: &EntityKey) -> Option<&'a Value> {
    ledger.submitted_rows.as_ref()?.persisted_ids.get(key)
}

fn claimed_elsewhere(
    ledger: &EntityMutationLedger,
    canonical: &EntityKey,
    original: &EntityKey,
) -> bool {
    if ledger
        .submitted_rows
        .as_ref()
        .and_then(|rows| rows.aliases.get(canonical))
        == Some(original)
    {
        return false;
    }
    let in_intent = |intent: &EntityMutationLedger| {
        intent.original_versions.get(canonical).is_some()
            || intent.original_snapshots.contains_key(canonical)
            || intent.new_keys.contains(canonical)
            || intent.deleted_keys.contains(canonical)
            || intent.change_sets.stack.iter().any(|changes| {
                changes.changes.contains_key(canonical)
                    || changes
                        .dynamic_changes
                        .as_ref()
                        .is_some_and(|dynamic| dynamic.contains_key(canonical))
            })
    };
    in_intent(ledger)
        || ledger.submitted_rows.as_ref().is_some_and(|rows| {
            rows.rows.contains_key(canonical) || rows.origins.contains_key(canonical)
        })
        || ledger
            .pending
            .as_ref()
            .is_some_and(|pending| pending.batches.iter().any(|batch| in_intent(&batch.intent)))
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
    /// Freeze exactly the current save intent before Checker/Fix and policy
    /// review. An existing owner is required for a repeated outer flush; an
    /// owned save passes None before it opens its physical transaction.
    #[doc(hidden)]
    pub fn reserve_submission(
        &self,
        existing_owner: Option<&PhysicalTransactionIdentity>,
    ) -> Result<LedgerDraft, crate::RuntimeError> {
        let root = self.context().clone();
        let mut ledger = root.lock().unwrap_or_else(|error| error.into_inner());
        if ledger.draft.is_some()
            || ledger.completion_fenced
            || !ledger.composition_errors.is_empty()
        {
            return Err(unavailable());
        }
        if let Some(owner) = existing_owner {
            validate_owner(&ledger, owner)?;
            owner.require_active().map_err(|_| unavailable())?;
        } else if ledger.pending.is_some() {
            return Err(unavailable());
        }
        ledger.change_sets.current_mut();
        let level = ledger.change_sets.stack.len() - 1;
        let mut frozen = ledger.clone();
        frozen.pending = None;
        frozen.draft = None;
        let execution = EntityRuntimeState {
            inner: LazyLedgerSlot::default(),
            graph: self.graph.promote(),
            json_view: self.json_view,
            loaded_snapshot: self.loaded_snapshot.clone(),
            dynamic_fields: self.dynamic_fields.clone(),
        };
        execution
            .inner
            .get_or_init(|| Arc::new(Mutex::new(frozen.clone())));
        ledger.draft_sequence = ledger
            .draft_sequence
            .checked_add(1)
            .ok_or_else(unavailable)?;
        let sequence = ledger.draft_sequence;
        ledger.draft = Some(Box::new(DraftLedger {
            sequence,
            level,
            intent: frozen,
        }));
        ledger.change_sets.clear_current();
        ledger.new_keys.clear();
        ledger.deleted_keys.clear();
        ledger.comment = None;
        ledger.entity_comments.clear();
        drop(ledger);
        Ok(LedgerDraft {
            root,
            sequence,
            execution: Some(execution),
        })
    }

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
    if ledger.draft.is_some() {
        return Err(unavailable());
    }
    validate_owner(ledger, owner)
}
fn validate_owner(
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
fn restore_draft(ledger: &mut EntityMutationLedger, draft: DraftLedger) {
    let current = &mut ledger.change_sets.stack[draft.level];
    let live = std::mem::take(current);
    if let Some(frozen) = draft.intent.change_sets.current() {
        merge_change_set(current, frozen);
    }
    merge_change_set(current, &live);
    ledger.new_keys.extend(draft.intent.new_keys);
    ledger.deleted_keys.extend(draft.intent.deleted_keys);
    let live_comment = ledger.comment.take();
    ledger.comment = live_comment.or(draft.intent.comment);
    for (key, reason) in draft.intent.entity_comments {
        ledger.entity_comments.entry(key).or_insert(reason);
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
    let attempted_aliases = ledger
        .submitted_rows
        .as_ref()
        .map(|rows| rows.aliases.clone())
        .unwrap_or_default();
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
    if !attempted_aliases.is_empty() {
        let rows = ledger.submitted_rows.get_or_insert_with(Default::default);
        for (alias, original) in attempted_aliases {
            rows.aliases.entry(alias).or_insert(original);
        }
    }
}

#[cfg(test)]
mod tests;
