//! Wire data must not supply internal mutation or availability state.
use std::collections::BTreeMap;
use teaql_core::{Entity, TeaqlEntity as _, Value};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_runtime::{InMemoryMetadataStore, UserContext};

#[teaql_entity]
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "WireStateProbe", indexed_layout)]
struct WireStateProbe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, Value>,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}

impl WireStateProbe {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "wire-state-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("name", 2)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("name", "name", "name"),
    ];
}

fn context() -> UserContext {
    // No executor: successful presentation reads cannot fetch or save data.
    UserContext::new().with_metadata(
        InMemoryMetadataStore::new().with_entity(WireStateProbe::entity_descriptor()),
    )
}

#[test]
fn reserved_runtime_keys_reject_in_entities_and_batches_without_exposing_values() {
    let context = context();
    for key in [
        "_comment",
        "_dirty_fields",
        "_original_values",
        "_is_new",
        "_is_deleted",
        "__load_state",
        "__teaql_runtime_state",
    ] {
        let mut input = serde_json::json!({"id":1,"version":7,"name":"legitimate name"});
        input[key] = serde_json::json!({"payload":"PRIVATE-STATE-CANARY"});
        let error = context
            .decode_json_entity::<WireStateProbe>(&input)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("incoming runtime state is forbidden")
        );
        assert!(!error.to_string().contains("PRIVATE-STATE-CANARY"));
        let batch = serde_json::json!([{"id":2,"name":"valid first row"}, input]);
        let error = context
            .decode_json_entities::<WireStateProbe>(&batch)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("incoming runtime state is forbidden")
        );
        assert!(!error.to_string().contains("PRIVATE-STATE-CANARY"));
    }
}

#[test]
fn legitimate_readonly_properties_do_not_install_fixed_slots_or_mutation_intent() {
    let row = context()
        .decode_json_entity::<WireStateProbe>(&serde_json::json!({
            "id":1,"version":7,"name":"native","_name":"derived","_count":0,"_null":null,
        }))
        .unwrap();
    assert_eq!(row.name, "native");
    assert!(row.has_dynamic_property("_name"));
    assert!(row.has_dynamic_property("_count"));
    assert!(row.has_dynamic_property("_null"));
    assert!(row.dynamic_property("_null").is_none());
    assert!(!row.has_dynamic_property("_missing"));
    assert!(row.dynamic_property("_missing").is_none());
    let state = row.loaded_state_snapshot().unwrap();
    for key in ["_name", "_count", "_null", "_missing"] {
        assert_eq!(state.layout().index(key), None);
        assert!(!state.is_loaded(key));
    }
    assert!(row.dirty_fields().is_none());
    assert!(!row.has_pending_dynamic_mutations());
}
