use std::collections::BTreeMap;
use teaql_core::{CompactRow, Entity, TeaqlEntity, Value};

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(
    entity = "School",
    subtype(name = "Academy", table = "Legacy Academy", id_column = "Legacy ID")
)]
struct School {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    root: String,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, Value>,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}

fn row(subtype: Option<Value>) -> CompactRow {
    let mut fields = BTreeMap::from([
        ("id".into(), Value::U64(1)),
        ("version".into(), Value::I64(1)),
        ("name".into(), Value::from("Parent view")),
        (
            "root".into(),
            Value::from("A normal modeled field, not runtime state"),
        ),
    ]);
    if let Some(subtype) = subtype {
        fields.insert("_type_".into(), subtype);
    }
    CompactRow::from_map(fields)
}

#[test]
fn subtype_hint_is_readonly_and_does_not_replace_the_static_view() {
    let entity: School = School::from_compact_row(row(Some(Value::from("Academy")))).unwrap();
    assert_eq!(School::ENTITY_NAME, "School");
    assert_eq!(entity.runtime_type(), "Academy");
    assert_eq!(entity.root, "A normal modeled field, not runtime state");
    assert_eq!(entity.clone().runtime_type(), "Academy");
    assert!(entity.clone().into_values().get("_type_").is_none());
    assert_eq!(entity.into_json()["_type_"], "Academy");
}

#[test]
fn missing_subtype_uses_the_native_view_identity() {
    assert_eq!(
        School::from_compact_row(row(None)).unwrap().runtime_type(),
        "School"
    );
    assert_eq!(
        School::from_compact_row(row(Some(Value::Null)))
            .unwrap()
            .runtime_type(),
        "School"
    );
    let descriptor = School::entity_descriptor();
    assert_eq!(
        descriptor.inheritance_children[0].table_name,
        "Legacy Academy"
    );
    assert_eq!(descriptor.inheritance_children[0].id_column, "Legacy ID");
    assert!(
        descriptor
            .storage_descriptor()
            .inheritance_children
            .is_empty()
    );
    assert_eq!(School::INHERITANCE_VIEW_METADATA_REVISION, 1);
}

#[test]
fn unknown_or_ambiguous_subtype_values_fail_without_echoing_them() {
    for value in [
        Value::from("__teaql_ambiguous_subtype__"),
        Value::from("PRIVATE-INVALID-SUBTYPE"),
        Value::I64(9),
    ] {
        let error = School::from_compact_row(row(Some(value))).unwrap_err();
        assert!(error.message.contains("subtype metadata"));
        assert!(!error.message.contains("PRIVATE-INVALID-SUBTYPE"));
    }
}
