use super::*;
use std::collections::HashMap;
use teaql_core::dynamic_fields::{DynamicFieldDefinitions, DynamicFieldState, DynamicFieldValues};

fn loaded(value: Option<Value>) -> EntityRuntimeState {
    let definitions = DynamicFieldDefinitions::new(
        "Entry",
        "baseline-v1",
        [
            ("note".into(), teaql_core::DataType::Text),
            ("other".into(), teaql_core::DataType::Text),
        ],
    )
    .unwrap();
    let mut state = EntityRuntimeState::default();
    state.install_loaded_dynamic_fields(
        DynamicFieldValues::from_values(
            definitions,
            value
                .map(|value| HashMap::from([("note".into(), value)]))
                .unwrap_or_default(),
        )
        .unwrap(),
    );
    state
}

#[test]
fn equal_loaded_value_and_null_do_not_allocate_mutation_state() {
    for value in [Value::from("same"), Value::Null] {
        let mut state = loaded(Some(value.clone()));
        state
            .update_dynamic_field(EntityKey::new("Entry", 1_u64), "note", value)
            .unwrap();
        assert!(!state.has_mutation_context());
        assert!(state.current_change_set().is_empty());
    }
}

#[test]
fn restoring_known_null_cancels_only_its_code_across_all_frames() {
    let mut state = loaded(Some(Value::Null));
    let key = EntityKey::new("Entry", 1_u64);
    state.set_original_version(key.clone(), 7);
    state
        .update_dynamic_field(key.clone(), "note", "outer".into())
        .unwrap();
    state.push_change_set();
    state
        .update_dynamic_field(key.clone(), "note", "inner".into())
        .unwrap();
    state
        .update_dynamic_field(key.clone(), "note", Value::Null)
        .unwrap();
    assert!(state.changed_field_names(&key).is_empty());
    assert_eq!(
        state
            .loaded_dynamic_fields()
            .unwrap()
            .field("note")
            .unwrap()
            .state(),
        DynamicFieldState::Null
    );
    state.pop_change_set();
    assert!(state.current_change_set().is_empty());
    assert_eq!(state.get_original_version(&key), Some(7));
}

#[test]
fn restoring_one_extension_keeps_other_fields_keys_and_lifecycle() {
    let mut state = loaded(Some("original".into()));
    let key = EntityKey::new("Entry", 1_u64);
    let other = EntityKey::new("AnotherType", 1_u64);
    state.mark_as_delete(key.clone());
    state.set(other.clone(), "name", Value::from("keep other type"));
    state.set(key.clone(), "name", Value::from("keep fixed edit"));
    state
        .update_dynamic_field(key.clone(), "note", "edit".into())
        .unwrap();
    state
        .update_dynamic_field(key.clone(), "other", "keep extension".into())
        .unwrap();
    state
        .update_dynamic_field(key.clone(), "note", "original".into())
        .unwrap();
    assert_eq!(
        state.current_change_set().dynamic_changes().unwrap()[&key].len(),
        1
    );
    assert_eq!(
        state.current_change_set().get(&other, "name"),
        Some(&Value::from("keep other type"))
    );
    assert!(state.changed_field_names(&key).contains("name"));
    assert!(state.deleted_keys().contains(&key));
}

#[test]
fn unknown_original_null_is_not_a_proven_baseline() {
    let mut state = loaded(None);
    let key = EntityKey::new("Entry", 1_u64);
    state
        .update_dynamic_field(key.clone(), "note", "edit".into())
        .unwrap();
    state
        .update_dynamic_field(key.clone(), "note", Value::Null)
        .unwrap();
    assert!(state.current_change_set().dynamic_changes().unwrap()[&key].contains_key("note"));
}

#[test]
fn new_entity_keeps_even_an_equal_prepared_assignment() {
    let mut state = loaded(Some("same".into()));
    let key = EntityKey::new("Entry", 1_u64);
    state.mark_as_new(key.clone());
    state
        .update_dynamic_field(key.clone(), "note", "same".into())
        .unwrap();
    assert!(state.is_new(&key));
    assert!(!state.current_change_set().is_empty());
}

#[test]
fn representation_sensitive_or_nonfinite_numbers_are_not_cancelled() {
    assert!(!same_dynamic_baseline(&Value::F64(0.0), &Value::F64(-0.0)));
    assert!(!same_dynamic_baseline(
        &Value::F64(f64::NAN),
        &Value::F64(f64::NAN)
    ));
    assert!(same_dynamic_baseline(&Value::F64(1.0), &Value::F64(1.0)));
}
