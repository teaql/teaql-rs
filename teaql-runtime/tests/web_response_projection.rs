//! Typed WebResponse must use presentation, never mutation/checker values.
use std::collections::BTreeMap;
use std::sync::Arc;
use teaql_core::{CompactRow, Entity, Record, SmartList, TeaqlEntity as _, Value, WebResponse};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_runtime::{InMemoryMetadataStore, UserContext};

#[teaql_entity]
#[derive(Clone, Debug, TeaqlEntity)]
#[teaql(entity = "ResponseProjection", indexed_layout)]
struct ResponseProjection {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    note: Option<String>,
    secret_note: String,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, Value>,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}

impl ResponseProjection {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "response-projection-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("version", 1),
        ("name", 2),
        ("note", 3),
        ("secret_note", 4),
    ];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("name", "name", "name"),
        ("note", "note", "note"),
        ("secret_note", "secret_note", "secret_note"),
    ];
}

fn row() -> ResponseProjection {
    // No executor or ID generator: wrapping cannot fetch, allocate or save.
    let context = UserContext::new().with_metadata(
        InMemoryMetadataStore::new().with_entity(ResponseProjection::entity_descriptor()),
    );
    let mut row = context
        .decode_json_entity::<ResponseProjection>(&serde_json::json!({
            "id":41,"version":7,"name":"visible","note":null,"_count":0,"_null":null
        }))
        .unwrap();
    row.set_comment("PRIVATE-EDITING-COMMENT");
    row
}

fn safe(json: &serde_json::Value) {
    assert_eq!(json["id"], 41);
    assert_eq!(json["version"], 7);
    assert_eq!(json["name"], "visible");
    assert!(json.get("note").is_some_and(serde_json::Value::is_null));
    assert_eq!(json["_count"], 0);
    assert!(json.get("_null").is_some_and(serde_json::Value::is_null));
    for key in [
        "secret_note",
        "secretNote",
        "_comment",
        "_dirty_fields",
        "_original_values",
        "_is_new",
        "_is_deleted",
        "__load_state",
        "__teaql_runtime_state",
    ] {
        assert!(json.get(key).is_none(), "not a presentation field: {key}");
    }
    assert!(!json.to_string().contains("PRIVATE-EDITING-COMMENT"));
}

#[test]
fn existing_entity_presentation_is_safe_without_web_response() {
    safe(&row().into_json());
}

#[test]
fn typed_response_constructors_do_not_publish_mutation_metadata() {
    let row = row();
    let expected = row.clone().into_json();
    safe(&expected);
    let mutation = row.clone().into_values();
    assert!(
        mutation.contains_key("_comment"),
        "calibrate distinct save representation"
    );
    for response in [
        WebResponse::from_entity(&row),
        WebResponse::from_entities([row.clone()]),
        WebResponse::from_smart_list(SmartList::new(vec![row])),
    ] {
        assert_eq!(response.data, vec![expected.clone()]);
        safe(&response.data[0]);
    }
}

#[test]
fn response_preserves_loaded_null_query_properties_and_fixed_absence() {
    let row = row();
    assert!(row.is_field_loaded("note"));
    assert!(!row.is_field_loaded("secret_note"));
    assert!(row.has_dynamic_property("_null"));
    assert!(!row.has_dynamic_property("_missing"));
    let response = WebResponse::from_entity(&row);
    safe(&response.data[0]);
    assert!(response.data[0].get("_missing").is_none());
}

#[test]
fn borrowed_response_does_not_change_availability_or_pending_intent() {
    let mut row = row();
    row.mark_for_deletion();
    let state = row.loaded_state_snapshot().unwrap();
    let before = row.clone().into_values();
    let dirty = row.dirty_fields();
    let original = row.original_values();
    let comment = row.get_comment();
    let response = WebResponse::from_entity(&row);
    safe(&response.data[0]);
    assert_eq!(row.clone().into_values(), before);
    assert_eq!(row.dirty_fields(), dirty);
    assert_eq!(row.original_values(), original);
    assert_eq!(row.get_comment(), comment);
    assert!(row.is_marked_as_delete());
    assert!(Arc::ptr_eq(&state, &row.loaded_state_snapshot().unwrap()));
}

#[test]
fn response_keeps_count_flat_facets_empty_arrays_and_explicit_records() {
    let facet = CompactRow::from_map(Record::from([("_sum".into(), Value::Null)]));
    let rows = SmartList::new(vec![row()])
        .with_total_count(73)
        .with_facet("totals", SmartList::new(vec![facet]))
        .with_facet("empty", SmartList::new(vec![]));
    let response = WebResponse::from_smart_list(rows).to_json_value();
    safe(&response["data"][0]);
    assert_eq!(response["recordCount"], 73);
    assert_eq!(
        response["facets"]["totals"],
        serde_json::json!([{"_sum":null}])
    );
    assert_eq!(response["facets"]["empty"], serde_json::json!([]));
    let empty = WebResponse::from_smart_list(SmartList::<ResponseProjection>::new(vec![]));
    assert_eq!(empty.record_count, 0);
    assert!(empty.data.is_empty());
    let raw = Record::from([("application_value".into(), Value::Text("explicit".into()))]);
    assert_eq!(
        WebResponse::from_records([raw]).data,
        vec![serde_json::json!({"application_value":"explicit"})]
    );
}
