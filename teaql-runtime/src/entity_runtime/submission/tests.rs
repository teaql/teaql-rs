use super::*;
use teaql_data_service::infrastructure_sql::SqlResourceIdentity;
fn key(id: u64) -> EntityKey {
    EntityKey::new_static("Order", id)
}
fn row(name: &str, version: i64) -> EntitySnapshot {
    BTreeMap::from([
        ("id".into(), Value::U64(42)),
        ("name".into(), Value::Text(name.into())),
        ("version".into(), Value::I64(version)),
    ])
    .into()
}

#[test]
fn ownerless_review_draft_freezes_intent_and_restores_it_on_denial() {
    let root = EntityRuntimeState::default();
    root.set(key(42), "name", "reviewed");
    root.mark_as_new(key(99));
    root.set(key(99), "name", "new child");
    root.set_comment("reviewed reason");
    let draft = root.reserve_submission(None).unwrap();
    assert_eq!(
        draft.execution_state().get(&key(42), "name"),
        Some(Value::from("reviewed"))
    );
    assert!(draft.execution_state().is_new(&key(99)));
    assert_eq!(root.get(&key(42), "name"), Some(Value::from("reviewed")));
    assert!(root.current_change_set().is_empty());
    root.set(key(42), "name", "late");
    root.clear_committed();
    assert!(root.reserve_submission(None).is_err());
    drop(draft);
    assert_eq!(root.get(&key(42), "name"), Some(Value::from("late")));
    assert_eq!(
        root.current_change_set().get(&key(42), "name"),
        Some(&Value::from("late"))
    );
    assert!(root.is_new(&key(99)));
    assert_eq!(root.get_comment().as_deref(), Some("reviewed reason"));
    assert!(!root.has_pending_submission());
    assert!(!root.requires_reload_after_completion());
}

#[test]
fn trusted_fix_is_sealed_into_reviewed_intent_without_capturing_late_edit() {
    let root = EntityRuntimeState::default();
    root.set(key(42), "name", "preflight");
    let mut draft = root.reserve_submission(None).unwrap();
    draft.execution_state().set(key(42), "name", "fixed");
    draft.seal_reviewed_intent().unwrap();
    assert_eq!(root.get(&key(42), "name"), Some(Value::from("fixed")));
    root.set(key(42), "name", "late");
    let mut owner = SqlResourceIdentity::new().begin();
    let submission = draft.attach(&owner.identity()).unwrap();
    assert_eq!(
        submission.execution_state().get(&key(42), "name"),
        Some(Value::from("fixed"))
    );
    submission
        .acknowledge([(key(42), row("fixed", 8))])
        .unwrap();
    owner.prepare_commit().unwrap();
    owner.committed().unwrap();
    assert_eq!(root.get(&key(42), "name"), Some(Value::from("late")));
}

#[test]
fn reviewed_draft_attaches_only_to_actual_owner_and_keeps_late_edits() {
    let root = EntityRuntimeState::default();
    root.set_original_version(key(42), 7);
    root.set(key(42), "name", "reviewed");
    let draft = root.reserve_submission(None).unwrap();
    root.set(key(42), "name", "late before BEGIN");
    let mut owner = SqlResourceIdentity::new().begin();
    let submission = draft.attach(&owner.identity()).unwrap();
    assert!(root.has_pending_submission());
    assert_eq!(
        submission.execution_state().get(&key(42), "name"),
        Some(Value::from("reviewed"))
    );
    submission
        .acknowledge([(key(42), row("reviewed", 8))])
        .unwrap();
    owner.prepare_commit().unwrap();
    owner.committed().unwrap();
    assert_eq!(root.get_original_version(&key(42)), Some(8));
    assert_eq!(
        root.get(&key(42), "name"),
        Some(Value::from("late before BEGIN"))
    );
    assert_eq!(
        root.current_change_set().get(&key(42), "name"),
        Some(&Value::from("late before BEGIN"))
    );
}

#[test]
fn repeated_review_draft_rejects_foreign_owner_without_losing_pending_work() {
    let root = EntityRuntimeState::default();
    root.set_original_version(key(42), 7);
    let mut owner = SqlResourceIdentity::new().begin();
    root.set(key(42), "name", "first");
    root.begin_submission(&owner.identity())
        .unwrap()
        .acknowledge([(key(42), row("first", 8))])
        .unwrap();
    root.set(key(42), "name", "second");
    let draft = root.reserve_submission(Some(&owner.identity())).unwrap();
    assert_eq!(
        draft.execution_state().get_original_version(&key(42)),
        Some(8)
    );
    let foreign = SqlResourceIdentity::new().begin();
    assert!(draft.attach(&foreign.identity()).is_err());
    assert_eq!(root.get(&key(42), "name"), Some(Value::from("second")));
    assert_eq!(
        root.current_change_set().get(&key(42), "name"),
        Some(&Value::from("second"))
    );
    assert!(root.has_pending_submission());
    owner.prepare_commit().unwrap();
    owner.committed().unwrap();
    assert_eq!(root.get_original_version(&key(42)), Some(8));
}

#[test]
fn captured_intent_and_live_edits_are_separate_until_the_actual_owner_commits() {
    let root = EntityRuntimeState::default();
    let alias = root.clone();
    root.set(key(42), "name", "submitted");
    root.set_original_version(key(42), 7);
    root.set_comment("first request");
    root.set_entity_comment(key(42), "first entity reason");
    let mut owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    let batch = root.begin_submission(&identity).unwrap();
    assert!(root.has_pending_submission());
    assert!(root.current_change_set().is_empty());
    assert_eq!(alias.get(&key(42), "name"), Some(Value::from("submitted")));
    alias.set(key(42), "name", "late");
    alias.set_comment("next request");
    alias.set_entity_comment(key(42), "next entity reason");
    assert_eq!(
        batch.execution_state().get(&key(42), "name"),
        Some(Value::from("submitted"))
    );
    root.clear_committed();
    assert_eq!(alias.get(&key(42), "name"), Some(Value::from("late")));
    batch.acknowledge([(key(42), row("submitted", 8))]).unwrap();
    assert_eq!(root.get_original_version(&key(42)), Some(8));
    owner.prepare_commit().unwrap();
    owner.committed().unwrap();
    assert!(!root.has_pending_submission());
    assert!(!root.requires_reload_after_completion());
    assert_eq!(
        root.current_change_set().get(&key(42), "name"),
        Some(&Value::from("late"))
    );
    assert_eq!(root.get_comment().as_deref(), Some("next request"));
    assert_eq!(
        root.get_entity_comment(&key(42)).as_deref(),
        Some("next entity reason")
    );
}

#[test]
fn repeated_submissions_consume_only_their_own_edits_and_advance_authoritative_versions() {
    let root = EntityRuntimeState::default();
    root.set_original_version(key(42), 7);
    let mut owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    root.mark_as_new(key(42));
    root.set(key(42), "name", "first");
    let first = root.begin_submission(&identity).unwrap();
    assert!(first.execution_state().is_new(&key(42)));
    assert!(!root.is_new(&key(42)));
    first.acknowledge([(key(42), row("first", 8))]).unwrap();
    root.set(key(42), "name", "second");
    let second = root.begin_submission(&identity).unwrap();
    assert!(!second.execution_state().is_new(&key(42)));
    assert_eq!(
        second.execution_state().get_original_version(&key(42)),
        Some(8)
    );
    second.acknowledge([(key(42), row("second", 9))]).unwrap();
    // Even an equal-valued write after the last submitted snapshot remains intent.
    root.set(key(42), "name", "second");
    owner.prepare_commit().unwrap();
    owner.committed().unwrap();
    assert_eq!(root.get_original_version(&key(42)), Some(9));
    assert_eq!(
        root.current_change_set().get(&key(42), "name"),
        Some(&Value::from("second"))
    );
}

#[test]
fn rollback_restores_original_versions_all_submitted_intent_and_latest_live_edits() {
    let root = EntityRuntimeState::default();
    root.set_original_version(key(42), 7);
    root.mark_as_new(key(99));
    root.set(key(99), "name", "new child");
    let owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    root.set(key(42), "name", "first");
    root.set(key(42), "note", "keep");
    root.set_comment("first request");
    root.set_entity_comment(key(42), "first entity");
    root.begin_submission(&identity)
        .unwrap()
        .acknowledge([(key(42), row("first", 8))])
        .unwrap();
    root.set(key(42), "name", "second");
    root.begin_submission(&identity)
        .unwrap()
        .acknowledge([(key(42), row("second", 9))])
        .unwrap();
    root.set(key(42), "name", "late");
    root.mark_as_delete(key(77));
    root.set_entity_comment(key(42), "late reason");
    owner.rolled_back().unwrap();
    assert_eq!(root.get_original_version(&key(42)), Some(7));
    assert_eq!(root.get(&key(42), "name"), Some(Value::from("late")));
    assert_eq!(root.get(&key(42), "note"), Some(Value::from("keep")));
    assert!(root.is_new(&key(99)));
    assert!(root.is_marked_as_delete(&key(77)));
    assert_eq!(root.get_comment().as_deref(), Some("first request"));
    assert_eq!(
        root.get_entity_comment(&key(42)).as_deref(),
        Some("late reason")
    );
    assert!(!root.has_pending_submission());
    assert!(!root.requires_reload_after_completion());
    let next = SqlResourceIdentity::new().begin();
    assert!(root.begin_submission(&next.identity()).is_ok());
}

#[test]
fn unknown_fences_every_alias_and_cannot_be_erased_by_clearing_intent() {
    for explicit in [false, true] {
        let root = EntityRuntimeState::default();
        let alias = root.clone();
        root.set(key(42), "name", "submitted");
        let owner = SqlResourceIdentity::new().begin();
        let identity = owner.identity();
        root.begin_submission(&identity)
            .unwrap()
            .acknowledge([])
            .unwrap();
        if explicit {
            owner.unknown().unwrap();
        } else {
            drop(owner);
        }
        assert!(root.requires_reload_after_completion());
        assert!(alias.requires_reload_after_completion());
        root.clear_committed();
        root.clear_current_change_set();
        let next = SqlResourceIdentity::new().begin();
        assert!(alias.require_submission_owner(&next.identity()).is_err());
        assert!(alias.begin_submission(&next.identity()).is_err());
        let independent = EntityRuntimeState::default();
        assert!(independent.begin_submission(&next.identity()).is_ok());
    }
}

#[test]
fn foreign_owner_inflight_and_abandoned_submission_are_rejected_before_commit() {
    let root = EntityRuntimeState::default();
    root.set(key(42), "name", "first");
    let mut owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    let foreign = SqlResourceIdentity::new().begin();
    let batch = root.begin_submission(&identity).unwrap();
    assert!(root.require_submission_owner(&foreign.identity()).is_err());
    assert!(root.begin_submission(&identity).is_err());
    assert!(owner.prepare_commit().is_err());
    drop(batch);
    assert!(root.begin_submission(&identity).is_err());
    owner.rolled_back().unwrap();
    assert_eq!(root.get(&key(42), "name"), Some(Value::from("first")));
    assert!(root.begin_submission(&foreign.identity()).is_ok());
}

#[test]
fn physical_commit_ignoring_an_inflight_veto_fences_instead_of_discarding_uncertain_intent() {
    let root = EntityRuntimeState::default();
    root.set(key(42), "name", "first");
    let mut owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    let batch = root.begin_submission(&identity).unwrap();
    assert!(owner.prepare_commit().is_err());
    assert!(owner.committed().is_err());
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Committed);
    assert!(root.requires_reload_after_completion());
    assert_eq!(root.get(&key(42), "name"), Some(Value::from("first")));
    drop(batch);
}

#[test]
fn stack_scope_protects_submitted_levels_and_keeps_unsent_lower_intent() {
    let root = EntityRuntimeState::default();
    root.set(key(42), "name", "lower");
    root.set(key(42), "note", "lower only");
    root.push_change_set();
    root.set(key(42), "name", "submitted top");
    let mut owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    let batch = root.begin_submission(&identity).unwrap();
    assert_eq!(
        root.get(&key(42), "name"),
        Some(Value::from("submitted top"))
    );
    assert!(root.pop_change_set().is_none());
    root.push_change_set();
    root.set(key(42), "name", "unsubmitted upper");
    assert!(root.pop_change_set().is_some());
    batch
        .acknowledge([(key(42), row("submitted top", 8))])
        .unwrap();
    owner.prepare_commit().unwrap();
    owner.committed().unwrap();
    assert!(root.current_change_set().is_empty());
    assert!(root.pop_change_set().is_some());
    assert_eq!(
        root.current_change_set().get(&key(42), "name"),
        Some(&Value::from("lower"))
    );
    assert_eq!(root.get(&key(42), "note"), Some(Value::from("lower only")));
}

#[test]
fn graph_state_settles_before_any_committed_notification_and_releases_ownership() {
    struct Readback {
        root: EntityRuntimeState,
    }
    impl PhysicalTransactionParticipant for Readback {
        fn after_completion(
            &self,
            _: PhysicalTransactionOutcome,
        ) -> Result<(), InfrastructureSqlError> {
            Ok(())
        }
        fn after_commit(&self) -> Result<(), InfrastructureSqlError> {
            assert!(!self.root.has_pending_submission());
            assert_eq!(self.root.get(&key(42), "name"), Some(Value::from("late")));
            Ok(())
        }
    }
    let root = EntityRuntimeState::default();
    root.set(key(42), "name", "first");
    let mut owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    root.begin_submission(&identity)
        .unwrap()
        .acknowledge([(key(42), row("first", 8))])
        .unwrap();
    root.set(key(42), "name", "late");
    identity
        .enlist(Arc::new(Readback { root: root.clone() }))
        .unwrap();
    owner.prepare_commit().unwrap();
    owner.committed().unwrap();
    assert!(identity.is_settled());
}

#[test]
fn pending_or_unknown_composition_is_explicitly_rejected_without_losing_detached_intent() {
    let source = EntityRuntimeState::default();
    source.set(key(42), "name", "pending");
    let target = EntityRuntimeState::default();
    target.set(key(99), "name", "independent");
    let owner = SqlResourceIdentity::new().begin();
    source
        .begin_submission(&owner.identity())
        .unwrap()
        .acknowledge([])
        .unwrap();
    assert!(matches!(
        source.first_composition_error(),
        Some(LedgerCompositionError::PhysicalCompletionRequired)
    ));
    assert_eq!(
        target.adopt_mutations_from_checked(&source),
        Err(LedgerCompositionError::PhysicalCompletionRequired)
    );
    assert_eq!(
        source.adopt_mutations_from_checked(&target),
        Err(LedgerCompositionError::PhysicalCompletionRequired)
    );
    assert_eq!(source.get(&key(42), "name"), Some(Value::from("pending")));
    assert_eq!(
        target.get(&key(99), "name"),
        Some(Value::from("independent"))
    );
    drop(owner);
    assert!(matches!(
        source.first_composition_error(),
        Some(LedgerCompositionError::PhysicalCompletionRequired)
    ));
    assert_eq!(
        target.adopt_mutations_from_checked(&source),
        Err(LedgerCompositionError::PhysicalCompletionRequired)
    );
}

#[test]
fn dynamic_intent_definition_and_originals_restore_without_overwriting_late_mutations() {
    use teaql_core::dynamic_fields::{DynamicFieldDefinitions, DynamicFieldMutation};
    for committed in [false, true] {
        let root = EntityRuntimeState::default();
        let definitions = DynamicFieldDefinitions::new(
            "Order",
            "extension-v1",
            [("note".into(), teaql_core::DataType::Text)],
        )
        .unwrap();
        root.write_context(|ledger| {
            ledger.change_sets.current_mut().set_dynamic(
                key(42),
                "note".into(),
                DynamicFieldMutation::Set(Value::from("submitted")),
                definitions.clone(),
                Some(Value::from("before")),
            )
        });
        let mut owner = SqlResourceIdentity::new().begin();
        let identity = owner.identity();
        let batch = root.begin_submission(&identity).unwrap();
        let captured = batch.execution_state().current_change_set();
        assert_eq!(captured.dynamic_definitions(&key(42)), Some(&definitions));
        assert_eq!(
            captured.dynamic_originals(&key(42)).unwrap()["note"],
            Some(Value::from("before"))
        );
        root.write_context(|ledger| {
            ledger.change_sets.current_mut().set_dynamic(
                key(42),
                "note".into(),
                DynamicFieldMutation::Delete,
                definitions.clone(),
                Some(Value::from("submitted")),
            )
        });
        batch.acknowledge([]).unwrap();
        if committed {
            owner.prepare_commit().unwrap();
            owner.committed().unwrap();
        } else {
            owner.rolled_back().unwrap();
        }
        let pending = root.current_change_set();
        assert_eq!(
            pending.dynamic_changes().unwrap()[&key(42)]["note"],
            DynamicFieldMutation::Delete
        );
        assert_eq!(pending.dynamic_definitions(&key(42)), Some(&definitions));
        assert_eq!(
            pending.dynamic_originals(&key(42)).unwrap()["note"],
            Some(Value::from(if committed { "submitted" } else { "before" }))
        );
    }
}

#[test]
fn submitted_sql_rows_supply_old_values_for_next_execution_and_roll_back_to_the_loaded_row() {
    let mut root = EntityRuntimeState::default();
    root.set_original_compact_row(
        "Order",
        teaql_core::CompactRow::from_map(row("loaded", 7).into()),
    );
    root.set(key(42), "name", "submitted");
    let owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    root.begin_submission(&identity)
        .unwrap()
        .acknowledge([(key(42), row("submitted", 8))])
        .unwrap();
    assert_eq!(
        root.original_snapshot().unwrap().get("name"),
        Some(&Value::from("submitted"))
    );
    assert_eq!(
        root.original_values_for(&key(42)).unwrap().get("name"),
        Some(&Value::from("submitted"))
    );
    root.set(key(42), "name", "late");
    let second = root.begin_submission(&identity).unwrap();
    assert_eq!(
        second
            .execution_state()
            .original_values_for(&key(42))
            .unwrap()
            .get("name"),
        Some(&Value::from("submitted"))
    );
    second.acknowledge([(key(42), row("late", 9))]).unwrap();
    owner.rolled_back().unwrap();
    assert_eq!(root.get_original_version(&key(42)), Some(7));
    assert_eq!(
        root.original_snapshot().unwrap().get("name"),
        Some(&Value::from("loaded"))
    );
    assert_eq!(
        root.original_values_for(&key(42)).unwrap().get("name"),
        Some(&Value::from("loaded"))
    );
}

#[test]
fn dropping_all_entity_views_does_not_erase_a_veto_and_terminal_drain_releases_the_ledger() {
    for outcome in 0..3 {
        let root = EntityRuntimeState::default();
        root.set(key(42), "name", "submitted");
        let weak = Arc::downgrade(root.context());
        let mut owner = SqlResourceIdentity::new().begin();
        let identity = owner.identity();
        let batch = root.begin_submission(&identity).unwrap();
        if outcome == 0 {
            drop(batch);
        } else {
            batch.acknowledge([]).unwrap();
        }
        drop(root);
        assert!(weak.upgrade().is_some());
        match outcome {
            0 => {
                assert!(owner.prepare_commit().is_err());
                owner.rolled_back().unwrap();
            }
            1 => {
                owner.prepare_commit().unwrap();
                owner.committed().unwrap();
            }
            _ => drop(owner),
        }
        assert!(weak.upgrade().is_none());
        assert!(identity.is_settled());
    }
}

#[test]
fn failed_state_restoration_keeps_the_ledger_fenced_after_acknowledged_rollback() {
    use teaql_core::dynamic_fields::DynamicFieldMutation;
    let root = EntityRuntimeState::default();
    // Internal fault: public typed mutation APIs always install this provenance.
    root.write_context(|ledger| {
        ledger.change_sets.current_mut().dynamic_changes = Some(Box::new(BTreeMap::from([(
            key(42),
            BTreeMap::from([("note".into(), DynamicFieldMutation::Delete)]),
        )])));
    });
    let owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    root.begin_submission(&identity)
        .unwrap()
        .acknowledge([])
        .unwrap();
    assert_eq!(
        owner.rolled_back(),
        Err(InfrastructureSqlError::CompletionFailed)
    );
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::RolledBack);
    assert!(identity.has_completion_error());
    assert!(root.requires_reload_after_completion());
    root.clear_committed();
    let next = SqlResourceIdentity::new().begin();
    assert!(root.begin_submission(&next.identity()).is_err());
    assert!(root.first_composition_error().is_some());
}
