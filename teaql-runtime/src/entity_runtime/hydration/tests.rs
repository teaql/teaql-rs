use super::*;
use teaql_core::{CompactRow, Entity, TeaqlEntity};
use teaql_data_service::infrastructure_sql::{PhysicalTransactionOutcome, SqlResourceIdentity};
use teaql_macros::{TeaqlEntity as DeriveEntity, teaql_entity};

#[teaql_entity]
#[derive(Clone, Debug, DeriveEntity)]
#[teaql(entity = "SavedOrder", table = "saved_order")]
struct SavedOrder {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
}
fn key() -> EntityKey {
    EntityKey::new_static(SavedOrder::ENTITY_NAME, 42_u64)
}
fn row(name: &str, version: i64) -> CompactRow {
    CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(42)),
        ("version".into(), Value::I64(version)),
        ("name".into(), Value::from(name)),
    ]))
}
fn root() -> EntityRuntimeState {
    let mut state = EntityRuntimeState::default();
    state.set_original_compact_row(SavedOrder::ENTITY_NAME, row("loaded", 1));
    state
}
fn graph(state: &EntityRuntimeState) {
    let mut graph = EntityGraphBuilder::default();
    graph.install_relation_list("SavedOrder", 42, "children", SmartList::from(vec![7_u64]));
    state.freeze_graph(graph).unwrap();
}

#[test]
fn save_hydration_shares_live_intent_with_or_without_preserved_relations() {
    for preserve in [false, true] {
        let original = root();
        graph(&original);
        original.set(key(), "name", "late");
        let saved_context = SavedEntityContext::new(&original, preserve);
        let saved =
            SavedOrder::from_compact_row_with_context(row("SQL", 2), &saved_context).unwrap();
        let state = saved.__teaql_runtime_state();
        assert_eq!(state, &original);
        assert_eq!(state.get(&key(), "name"), Some(Value::from("late")));
        assert_eq!(
            state.has_relation_view("SavedOrder", 42, "children"),
            preserve
        );
        assert_eq!(saved.version, 2);
        assert_eq!(saved.name, "SQL");
        state.set(key(), "name", "after hydration");
        assert_eq!(
            original.get(&key(), "name"),
            Some(Value::from("after hydration"))
        );
    }
}

#[test]
fn ordinary_query_context_keeps_independent_ledgers_with_shared_readonly_graph() {
    let original = root();
    graph(&original);
    original.set(key(), "name", "source pending");
    let first =
        SavedOrder::from_compact_row_with_context(row("query first", 1), &original).unwrap();
    let second =
        SavedOrder::from_compact_row_with_context(row("query second", 1), &original).unwrap();
    assert_ne!(first.__teaql_runtime_state(), &original);
    assert_ne!(
        first.__teaql_runtime_state(),
        second.__teaql_runtime_state()
    );
    assert!(
        first
            .__teaql_runtime_state()
            .has_relation_view("SavedOrder", 42, "children")
    );
    first
        .__teaql_runtime_state()
        .set(key(), "name", "first only");
    assert_eq!(second.__teaql_runtime_state().get(&key(), "name"), None);
    assert_eq!(
        original.get(&key(), "name"),
        Some(Value::from("source pending"))
    );
}

#[test]
fn save_hydration_keeps_pending_owner_and_rolls_back_shared_baselines() {
    for outcome in [
        PhysicalTransactionOutcome::Committed,
        PhysicalTransactionOutcome::RolledBack,
        PhysicalTransactionOutcome::Unknown,
    ] {
        let original = root();
        original.set(key(), "name", "submitted");
        let mut owner = SqlResourceIdentity::new().begin();
        let identity = owner.identity();
        let submission = original.begin_submission(&identity).unwrap();
        submission
            .acknowledge([(
                key(),
                teaql_core::EntitySnapshot::from(row("submitted", 2).into_map()),
            )])
            .unwrap();
        let context = SavedEntityContext::new(&original, false);
        let saved =
            SavedOrder::from_compact_row_after_save(row("submitted", 2), &context, false).unwrap();
        assert!(saved.__teaql_runtime_state().has_pending_submission());
        saved.__teaql_runtime_state().set(key(), "name", "late");
        match outcome {
            PhysicalTransactionOutcome::Committed => {
                owner.prepare_commit().unwrap();
                owner.committed().unwrap();
            }
            PhysicalTransactionOutcome::RolledBack => owner.rolled_back().unwrap(),
            _ => owner.unknown().unwrap(),
        }
        let state = saved.__teaql_runtime_state();
        assert!(!state.has_pending_submission());
        assert_eq!(
            state.get_original_version(&key()),
            Some(if outcome == PhysicalTransactionOutcome::Committed {
                2
            } else {
                1
            })
        );
        assert_eq!(state.get(&key(), "name"), Some(Value::from("late")));
        assert_eq!(
            state.requires_reload_after_completion(),
            outcome == PhysicalTransactionOutcome::Unknown
        );
    }
}

#[test]
fn save_hydration_does_not_allocate_a_mutation_ledger_for_a_clean_readback() {
    let original = root();
    assert!(!original.has_mutation_context());
    let saved_context = SavedEntityContext::new(&original, false);
    let saved = SavedOrder::from_compact_row_with_context(row("SQL", 2), &saved_context).unwrap();
    assert!(!original.has_mutation_context());
    assert!(!saved.__teaql_runtime_state().has_mutation_context());
    let alias = saved.clone();
    alias
        .__teaql_runtime_state()
        .set(key(), "name", "first mutation");
    assert_eq!(
        original.get(&key(), "name"),
        Some(Value::from("first mutation"))
    );
}

#[test]
fn unmanaged_save_keeps_legacy_independence_until_a_physical_journal_is_installed() {
    for preserve in [false, true] {
        let original = root();
        graph(&original);
        original.set(key(), "name", "original intent");
        let saved =
            SavedOrder::from_compact_row_after_save(row("SQL", 2), &original, preserve).unwrap();
        assert_ne!(saved.__teaql_runtime_state(), &original);
        assert!(
            saved
                .__teaql_runtime_state()
                .current_change_set()
                .is_empty()
        );
        assert_eq!(
            saved
                .__teaql_runtime_state()
                .has_relation_view("SavedOrder", 42, "children"),
            preserve
        );
    }
}

#[test]
fn current_save_context_is_explicit_and_never_inferred_from_past_journal() {
    let original = root();
    original.set(key(), "name", "submitted");
    let mut owner = SqlResourceIdentity::new().begin();
    original
        .begin_submission(&owner.identity())
        .unwrap()
        .acknowledge([(
            key(),
            teaql_core::EntitySnapshot::from(row("submitted", 2).into_map()),
        )])
        .unwrap();
    owner.prepare_commit().unwrap();
    owner.committed().unwrap();
    let implicit =
        SavedOrder::from_compact_row_after_save(row("later", 3), &original, false).unwrap();
    let context = SavedEntityContext::new(&original, false);
    let explicit =
        SavedOrder::from_compact_row_after_save(row("later", 3), &context, false).unwrap();
    assert_eq!(
        (
            implicit.__teaql_runtime_state() == &original,
            explicit.__teaql_runtime_state() == &original
        ),
        (false, true)
    );
}

#[test]
fn rolled_back_new_readback_cannot_supply_an_existing_row_version_or_snapshot() {
    let original = EntityRuntimeState::default();
    original.mark_as_new(key());
    original.set(key(), "name", "new");
    let owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    original
        .begin_submission(&identity)
        .unwrap()
        .acknowledge([(
            key(),
            teaql_core::EntitySnapshot::from(row("new", 1).into_map()),
        )])
        .unwrap();
    let context = SavedEntityContext::new(&original, false);
    let saved = SavedOrder::from_compact_row_after_save(row("new", 1), &context, false).unwrap();
    owner.rolled_back().unwrap();
    let state = saved.__teaql_runtime_state();
    assert!(state.is_new(&key()));
    assert_eq!(state.get_original_version(&key()), None);
    assert_eq!(state.original_snapshot(), None);
    assert_eq!(state.original_values_for(&key()), None);
}

#[test]
fn unversioned_row_rollback_keeps_original_values_without_inventing_a_version() {
    let entity = "Unversioned";
    let k = EntityKey::new_static(entity, 42_u64);
    let old = teaql_core::CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(42)),
        ("name".into(), Value::from("loaded")),
    ]));
    let new = teaql_core::CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(42)),
        ("name".into(), Value::from("SQL")),
    ]));
    let mut original = EntityRuntimeState::default();
    original.set_original_compact_row(entity, old);
    original.set(k.clone(), "name", "SQL");
    let owner = SqlResourceIdentity::new().begin();
    original
        .begin_submission(&owner.identity())
        .unwrap()
        .acknowledge([(
            k.clone(),
            teaql_core::EntitySnapshot::from(new.clone().into_map()),
        )])
        .unwrap();
    let context = SavedEntityContext::new(&original, false);
    let mut state = EntityRuntimeState::from_hydration_context(Some(&context));
    state.set_original_compact_row(entity, new);
    owner.rolled_back().unwrap();
    assert_eq!(state.get_original_version(&k), None);
    assert_eq!(
        state.original_snapshot().unwrap().get("name"),
        Some(&Value::from("loaded"))
    );
    assert_eq!(
        state.original_values_for(&k).unwrap().get("name"),
        Some(&Value::from("loaded"))
    );
}
