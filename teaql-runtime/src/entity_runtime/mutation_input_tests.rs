use super::*;

fn loaded() -> EntityRuntimeState {
    let mut state = EntityRuntimeState::default();
    state.set_original_compact_row(
        "Probe",
        teaql_core::CompactRow::from_map(BTreeMap::from([
            ("id".to_owned(), Value::I64(7)),
            ("version".to_owned(), Value::I64(5)),
            ("name".to_owned(), Value::from("kept")),
            ("_total".to_owned(), Value::I64(10)),
            ("physical_alias".to_owned(), Value::from("kept")),
        ])),
    );
    state
}

#[test]
fn raw_equal_value_is_retained_until_model_admission() {
    let state = loaded();
    let key = EntityKey::new_static("Probe", 7_u64);
    state.set(key.clone(), "name", "kept");
    assert_eq!(state.get(&key, "name"), Some(Value::from("kept")));
    assert!(state.changed_field_names(&key).contains("name"));
}

#[test]
fn raw_storage_alias_cannot_disappear_before_canonical_validation() {
    let state = loaded();
    let key = EntityKey::new_static("Probe", 7_u64);
    state.set(key.clone(), "physical_alias", "kept");
    assert_eq!(state.get(&key, "physical_alias"), Some(Value::from("kept")));
}

#[test]
fn readonly_property_mutation_cannot_disappear_as_an_equal_noop() {
    let state = loaded();
    let key = EntityKey::new_static("Probe", 7_u64);
    state.set(key.clone(), "_total", 10_i64);
    assert_eq!(state.get(&key, "_total"), Some(Value::I64(10)));
}

#[test]
fn raw_unknown_null_remains_explicit_input() {
    let state = loaded();
    let key = EntityKey::new_static("Probe", 7_u64);
    state.set(key.clone(), "unknown", Value::Null);
    assert_eq!(state.get(&key, "unknown"), Some(Value::Null));
}

#[test]
fn raw_same_id_of_another_type_is_not_mistaken_for_the_snapshot_owner() {
    let state = loaded();
    let key = EntityKey::new_static("Other", 7_u64);
    state.set(key.clone(), "name", "kept");
    assert_eq!(state.get(&key, "name"), Some(Value::from("kept")));
}
