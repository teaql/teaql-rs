use std::collections::BTreeMap;
use teaql_core::{CompactRow, Entity, Value};

#[teaql_macros::teaql_entity]
#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[allow(
    clippy::duplicated_attributes,
    reason = "Each distinct subtype descriptor must retain its own explicit id_column"
)]
#[teaql(
    entity = "School",
    subtype(name = "Academy", table = "academy_data", id_column = "id"),
    subtype(name = "College", table = "college_data", id_column = "id")
)]
struct ParentView {
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

fn view(id: u64, subtype: &str) -> ParentView {
    ParentView::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(id)),
        ("version".into(), Value::I64(1)),
        ("name".into(), Value::from("Typed parent")),
        ("_type_".into(), Value::from(subtype)),
    ])))
    .unwrap()
}

#[test]
fn generated_runtime_carriers_share_static_tags_instead_of_row_dictionaries() {
    let first = view(1, "Academy");
    let second = view(2, "Academy");
    assert_eq!(first.runtime_type(), "Academy");
    assert!(std::ptr::eq(
        first.runtime_type().as_ptr(),
        second.runtime_type().as_ptr()
    ));
    assert!(first.dynamic.is_empty());
    assert!(first.clone().into_values().get("_type_").is_none());
    assert_eq!(first.clone().into_json()["_type_"], "Academy");
    assert!(
        first
            .__teaql_runtime_state()
            .current_change_set()
            .changes()
            .is_empty()
    );
    assert_eq!(
        std::mem::size_of::<Option<&teaql_core::RuntimeSubtypeIdentity>>(),
        std::mem::size_of::<usize>()
    );
}

#[test]
fn sharing_a_graph_keeps_each_materialized_views_own_subtype_tag() {
    let academy = view(1, "Academy");
    let mut college = view(2, "College");
    college.on_loaded(academy.__teaql_runtime_state());
    assert_eq!(college.runtime_type(), "College");
    assert_eq!(academy.runtime_type(), "Academy");
    assert_eq!(college.clone().runtime_type(), "College");
    assert_eq!(college.clone().into_json()["_type_"], "College");
}

#[test]
fn recovery_marker_is_graph_intent_not_a_json_value_or_immediate_version_change() {
    let mut entity = view(3, "Academy");
    entity.mark_to_recover();
    assert_eq!(
        entity.version, 1,
        "marking must not pretend the database has changed"
    );
    assert!(entity.is_marked_as_recover());
    assert_eq!(
        entity.clone().into_values().get("_is_recovered"),
        Some(&Value::Bool(true))
    );
    assert!(entity.clone().into_json().get("_is_recovered").is_none());
    entity.mark_for_deletion();
    assert!(!entity.is_marked_as_recover());
    assert!(entity.is_marked_as_delete());
}
