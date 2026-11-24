use super::*;
use crate::{EntityKey, EntityRuntimeState, InMemoryMetadataStore};
use teaql_core::{CompactRow, DataType, EntityDescriptor, PropertyDescriptor, RelationDescriptor};

#[test]
fn json_restoration_keeps_numeric_wire_representations_distinct() {
    let value = |text| Value::Json(serde_json::from_str(text).unwrap());
    assert!(!same_value(&value("0.0"), &value("-0.0")));
    assert!(!same_value(&value("1"), &value("1.0")));
    assert!(same_value(
        &value(r#"{"count":1.0}"#),
        &value(r#"{"count":1.0}"#)
    ));
}

fn context() -> UserContext {
    let descriptor = |name| {
        EntityDescriptor::new(name)
            .property(PropertyDescriptor::new("id", DataType::U64).id())
            .property(PropertyDescriptor::new("version", DataType::I64))
            .property(PropertyDescriptor::new("name", DataType::Text))
            .property(PropertyDescriptor::new("note", DataType::Text))
            .property(PropertyDescriptor::new("school_type_id", DataType::U64))
            .relation(
                RelationDescriptor::new("school_type", "SchoolType").local_key("school_type_id"),
            )
    };
    UserContext::default().with_metadata(
        InMemoryMetadataStore::default()
            .with_entity(descriptor("Probe"))
            .with_entity(descriptor("Other")),
    )
}
fn fixture(owner: &str) -> (EntityRuntimeState, EntityKey, GraphNode) {
    let mut root = EntityRuntimeState::default();
    let values = BTreeMap::from([
        ("id".into(), Value::I64(7)),
        ("version".into(), Value::I64(5)),
        ("name".into(), Value::from("kept")),
        ("note".into(), Value::Null),
        ("school_type_id".into(), Value::I64(1001)),
    ]);
    root.set_original_compact_row(owner.to_owned(), CompactRow::from_map(values.clone()));
    let mut node = GraphNode::new(owner);
    node.values = values.into();
    (root, EntityKey::new(owner, 7_u64), node)
}

#[test]
fn known_value_and_null_restore_only_after_admission() {
    for (field, final_value) in [("name", Value::from("kept")), ("note", Value::Null)] {
        let (root, key, mut node) = fixture("Probe");
        root.set(key.clone(), field, "intermediate");
        root.set(key.clone(), field, final_value);
        node.dirty_fields = Some(BTreeSet::from([field.to_owned()]));
        assert!(!root.current_change_set().is_empty());
        prepare(&context(), &root, &mut node).unwrap();
        assert!(root.current_change_set().is_empty());
        assert!(graph_is_unchanged(&node));
        assert_eq!(root.get_original_version(&key), Some(5));
    }
}

#[test]
fn complete_validation_precedes_any_discard() {
    for invalid in ["unknown", "_total", "requiredString"] {
        let (root, key, mut node) = fixture("Probe");
        root.set(key.clone(), "name", "kept");
        root.set(key.clone(), invalid, Value::Null);
        let before = root.current_change_set();
        assert!(prepare(&context(), &root, &mut node).is_err());
        assert_eq!(root.current_change_set(), before);
        assert_eq!(node.operation, GraphOperation::Upsert);
    }
}

#[test]
fn restore_does_not_remove_other_native_or_other_type_intent() {
    let (root, key, mut node) = fixture("Probe");
    let other = EntityKey::new_static("Other", 7_u64);
    root.set(key.clone(), "name", "kept");
    root.set(key.clone(), "note", "real edit");
    root.set(other.clone(), "name", "other edit");
    node.values.insert("note".into(), Value::from("real edit"));
    node.dirty_fields = Some(BTreeSet::from(["name".into(), "note".into()]));
    prepare(&context(), &root, &mut node).unwrap();
    assert_eq!(root.get(&key, "name"), None);
    assert_eq!(root.get(&key, "note"), Some(Value::from("real edit")));
    assert_eq!(root.get(&other, "name"), Some(Value::from("other edit")));
    assert_eq!(node.dirty_fields, Some(BTreeSet::from(["note".into()])));
    assert!(!graph_is_unchanged(&node));
}

#[test]
fn original_from_another_type_cannot_cancel_equal_value() {
    let (root, _, _) = fixture("Probe");
    let (_, key, mut node) = fixture("Other");
    root.set(key.clone(), "name", "kept");
    prepare(&context(), &root, &mut node).unwrap();
    assert_eq!(root.get(&key, "name"), Some(Value::from("kept")));
    assert!(!graph_is_unchanged(&node));
}

#[test]
fn known_partial_view_is_clean_but_unknown_null_edit_is_not() {
    let (root, key, mut node) = fixture("Probe");
    node.values.remove("note");
    node.values.remove("school_type_id");
    prepare(&context(), &root, &mut node).unwrap();
    assert!(graph_is_unchanged(&node));
    let mut sparse = EntityRuntimeState::default();
    sparse.set_original_compact_row(
        "Probe",
        CompactRow::from_map(BTreeMap::from([
            ("id".into(), Value::U64(7)),
            ("version".into(), Value::I64(5)),
            ("name".into(), Value::from("kept")),
        ])),
    );
    sparse.set(key.clone(), "note", Value::Null);
    node.operation = GraphOperation::Upsert;
    node.values.insert("note".into(), Value::Null);
    prepare(&context(), &sparse, &mut node).unwrap();
    assert_eq!(sparse.get(&key, "note"), Some(Value::Null));
    assert!(!graph_is_unchanged(&node));
}

#[test]
fn new_and_deleted_entities_have_no_restoration_shortcut() {
    for new in [true, false] {
        let (root, key, mut node) = fixture("Probe");
        if new {
            root.mark_as_new(key.clone());
        } else {
            root.mark_as_delete(key.clone());
        }
        root.set(key.clone(), "name", "kept");
        prepare(&context(), &root, &mut node).unwrap();
        assert_eq!(root.get(&key, "name"), Some(Value::from("kept")));
        assert!(!graph_is_unchanged(&node));
    }
}

#[test]
fn restoration_clears_only_that_field_across_staged_frames() {
    let (root, key, mut node) = fixture("Probe");
    root.set(key.clone(), "name", "intermediate");
    root.set(key.clone(), "note", "other");
    root.push_change_set();
    root.set(key.clone(), "name", "kept");
    prepare(&context(), &root, &mut node).unwrap();
    assert_eq!(root.get(&key, "name"), None);
    assert_eq!(root.get(&key, "note"), Some(Value::from("other")));
    root.pop_change_set().unwrap();
    assert_eq!(root.get(&key, "name"), None);
    assert_eq!(root.get(&key, "note"), Some(Value::from("other")));
}

#[test]
fn canonical_fk_can_restore_without_swallowing_compatibility_alias() {
    for canonical in [true, false] {
        let (root, key, mut node) = fixture("Probe");
        let field = if canonical {
            "school_type"
        } else {
            "school_type_id"
        };
        root.set(key.clone(), field, 1001_u64);
        node.dirty_fields = Some(BTreeSet::from([field.into()]));
        prepare(&context(), &root, &mut node).unwrap();
        assert_eq!(root.get(&key, field).is_none(), canonical);
        assert_eq!(graph_is_unchanged(&node), canonical);
    }
}

#[test]
fn clean_parent_still_visits_dirty_child() {
    let (root, _, mut node) = fixture("Probe");
    let (child, child_key, mut child_node) = fixture("Other");
    child.set(child_key.clone(), "note", "real edit");
    child_node
        .values
        .insert("note".into(), Value::from("real edit"));
    root.adopt_mutations_from(&child);
    node.relations.insert("children".into(), vec![child_node]);
    prepare(&context(), &root, &mut node).unwrap();
    assert_eq!(node.operation, GraphOperation::Reference);
    assert!(!graph_is_unchanged(&node));
    assert_eq!(root.get(&child_key, "note"), Some(Value::from("real edit")));
}

#[test]
fn untracked_direct_value_edits_and_system_fields_are_not_discarded() {
    let (root, _, mut node) = fixture("Probe");
    node.values
        .insert("name".into(), Value::from("direct edit"));
    prepare(&context(), &root, &mut node).unwrap();
    assert!(!graph_is_unchanged(&node));
    for (field, value) in [("id", Value::U64(7)), ("version", Value::I64(5))] {
        let (root, key, mut node) = fixture("Probe");
        root.set(key.clone(), field, value.clone());
        prepare(&context(), &root, &mut node).unwrap();
        assert_eq!(root.get(&key, field), Some(value));
        assert!(!graph_is_unchanged(&node));
    }
}

#[test]
fn exact_scalar_representation_controls_restoration() {
    assert!(same_value(&Value::I64(7), &Value::U64(7)));
    assert!(!same_value(&Value::I64(-1), &Value::U64(u64::MAX)));
    assert!(same_value(&Value::Null, &Value::TypedNull(DataType::Text)));
    assert!(!same_value(&Value::F64(0.0), &Value::F64(-0.0)));
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(!same_value(&Value::F64(value), &Value::F64(value)));
    }
    let decimal = |value: &str| Value::Decimal(value.parse().unwrap());
    assert!(!same_value(&decimal("3.50"), &decimal("3.5")));
    assert!(same_value(&decimal("3.50"), &decimal("3.50")));
    assert!(!same_value(
        &Value::List(vec![decimal("3.50")]),
        &Value::List(vec![decimal("3.5")])
    ));
}

#[test]
fn legacy_unqualified_snapshot_does_not_prove_ownership() {
    let (_, key, mut node) = fixture("Probe");
    let root = EntityRuntimeState::default();
    root.set_original_snapshot(BTreeMap::from(node.values.clone()).into());
    root.set(key.clone(), "name", "kept");
    prepare(&context(), &root, &mut node).unwrap();
    assert_eq!(root.get(&key, "name"), Some(Value::from("kept")));
    assert!(!graph_is_unchanged(&node));
}

#[test]
fn provider_millisecond_snapshot_matches_typed_timestamp_only_by_metadata() {
    let time = Value::Timestamp(teaql_core::time::Timestamp(1234));
    assert!(same_property_value(
        DataType::Timestamp,
        &Value::I64(1234),
        &time
    ));
    assert!(same_property_value(
        DataType::Timestamp,
        &time,
        &Value::U64(1234)
    ));
    assert!(!same_property_value(
        DataType::I64,
        &Value::I64(1234),
        &time
    ));
    assert!(!same_property_value(
        DataType::Timestamp,
        &Value::F64(1234.0),
        &time
    ));
    assert!(!same_property_value(
        DataType::Timestamp,
        &Value::from("1234"),
        &time
    ));
    assert!(!same_property_value(
        DataType::Timestamp,
        &Value::U64(u64::MAX),
        &time
    ));
}
