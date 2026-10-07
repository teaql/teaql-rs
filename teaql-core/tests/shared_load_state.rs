use std::collections::HashSet;
use std::sync::Arc;
use teaql_core::eval::LoadState;
use teaql_core::{FieldLayout, LoadedSnapshot};

fn layout(revision: &str) -> Arc<FieldLayout> {
    let names: Vec<_> = (0..130)
        .map(|index| match index {
            0 => "id".to_owned(),
            1 => "version".to_owned(),
            2 => "school_type".to_owned(),
            _ => format!("field_{index}"),
        })
        .collect();
    let indexes: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(index, name)| (name.as_str(), index))
        .collect();
    let mappings: Vec<_> = names
        .iter()
        .map(|name| {
            if name == "school_type" {
                (name.as_str(), "school_type_id", "school_type")
            } else {
                (name.as_str(), name.as_str(), name.as_str())
            }
        })
        .collect();
    let members: Vec<_> = mappings.iter().map(|(_, member, _)| *member).collect();
    FieldLayout::from_generated(
        "School",
        revision,
        &indexes,
        &mappings,
        &["school_type", "child_list"],
        &members,
    )
    .unwrap()
}

#[test]
fn flat_relation_availability_is_shared_shape_metadata_not_a_row_value() {
    use teaql_core::{CompactRow, CompactRowLayout, RelationShapeCache, Value};
    let shape = CompactRowLayout::new(Arc::from(["id".to_owned(), "school_type_id".to_owned()]));
    let mut rows: Vec<_> = (1..=3)
        .map(|id| CompactRow::with_layout(shape.clone(), vec![Value::U64(id), Value::U64(1001)]))
        .collect();
    let mut cache = RelationShapeCache::default();
    rows[0].mark_relation_loaded("school_type", &mut cache);
    rows[1].mark_relation_loaded("school_type", &mut cache);
    assert!(Arc::ptr_eq(
        &rows[0].shared_layout(),
        &rows[1].shared_layout()
    ));
    assert_eq!(rows[0].len(), 2);
    assert!(!rows[0].contains_key("school_type"));
    assert!(!rows[0].clone().into_map().contains_key("school_type"));
    CompactRow::share_layouts(&mut rows);
    assert!(!Arc::ptr_eq(
        &rows[0].shared_layout(),
        &rows[2].shared_layout()
    ));
    let entity_layout = layout("flat-v1");
    let LoadState::Indexed(first) = rows[0].indexed_load_state(entity_layout.clone()) else {
        panic!("indexed shape");
    };
    let LoadState::Indexed(second) = rows[1].indexed_load_state(entity_layout) else {
        panic!("indexed shape");
    };
    assert!(Arc::ptr_eq(&first, &second));
    assert!(first.is_loaded("school_type"));
    assert!(first.is_loaded("school_type_id"));
    assert!(!rows[2].is_loaded_relation("school_type"));
    rows[0].insert("_count".into(), Value::U64(0));
    assert!(rows[0].is_loaded_relation("school_type"));
    assert!(!rows[2].is_loaded_relation("school_type"));
}

#[test]
fn bitmap_and_lazy_overflow_keep_all_boundary_slots() {
    let layout = layout("v1");
    let state = Arc::new(LoadedSnapshot::projection(
        layout,
        ["id", "field_31", "field_32", "field_63"],
    ));
    assert_eq!(state.bits(), 1 | 1 << 31 | 1 << 32 | 1 << 63);
    assert!(state.overflow().is_none());
    let wide = LoadedSnapshot::with_loaded(&state, "field_64", true).unwrap();
    let wide = LoadedSnapshot::with_loaded(&wide, "field_65", true).unwrap();
    let wide = LoadedSnapshot::with_loaded(&wide, "field_129", true).unwrap();
    assert_eq!(wide.overflow(), Some(&HashSet::from([64, 65, 129])));
    assert_eq!(wide.bits(), state.bits());
    assert!(!state.is_loaded("field_64"));
    let cleared = LoadedSnapshot::with_loaded(&wide, "field_65", false).unwrap();
    assert!(!cleared.is_loaded("field_65"));
    assert!(wide.is_loaded("field_65"));
}

#[test]
fn value_only_changes_retain_identity_and_availability_diverges_privately() {
    let state = Arc::new(LoadedSnapshot::projection(
        layout("v1"),
        ["id", "field_64", "#extra_note"],
    ));
    let same = LoadedSnapshot::with_loaded(&state, "field_64", true).unwrap();
    assert!(Arc::ptr_eq(&state, &same));
    let changed = LoadedSnapshot::with_loaded(&same, "field_65", true).unwrap();
    assert!(!Arc::ptr_eq(&state, &changed));
    assert!(changed.is_loaded("field_65"));
    assert!(!same.is_loaded("field_65"));
    assert!(state.selected_names().unwrap().contains("#extra_note"));
    assert_eq!(state.overflow(), Some(&HashSet::from([64])));
}

#[test]
fn unchanged_dynamic_selection_preserves_identity_and_relation_markers() {
    use std::collections::HashMap;
    use teaql_core::dynamic_fields::{DynamicFieldDefinitions, DynamicFieldValues};
    use teaql_core::{DataType, Value};
    let definitions = DynamicFieldDefinitions::new(
        "School",
        "dynamic-v1",
        [
            ("note".into(), DataType::Text),
            ("other".into(), DataType::Text),
        ],
    )
    .unwrap();
    let values = |fields| DynamicFieldValues::from_values(definitions.clone(), fields).unwrap();
    let base =
        LoadedSnapshot::projection(layout("v1"), ["id", "field_64", "child_list"]).into_shared();
    let null = values(HashMap::from([("note".into(), Value::Null)]));
    let selected = LoadedSnapshot::with_dynamic_fields(&base, &null).unwrap();
    let real = values(HashMap::from([(
        "note".into(),
        Value::Text("private".into()),
    )]));
    assert!(Arc::ptr_eq(
        &selected,
        &LoadedSnapshot::with_dynamic_fields(&selected, &real).unwrap()
    ));
    let other = values(HashMap::from([("other".into(), Value::Null)]));
    let changed = LoadedSnapshot::with_dynamic_fields(&selected, &other).unwrap();
    assert!(!Arc::ptr_eq(&selected, &changed));
    assert!(changed.is_loaded("#other"));
    assert!(!changed.is_loaded("#note"));
    assert!(selected.is_loaded("#note"));
    assert!(changed.is_loaded("child_list"));
    assert!(changed.is_loaded("field_64"));
    let absent = values(HashMap::new());
    let cleared = LoadedSnapshot::with_dynamic_fields(&changed, &absent).unwrap();
    assert!(Arc::ptr_eq(&base, &cleared));
    assert!(Arc::ptr_eq(
        &cleared,
        &LoadedSnapshot::with_dynamic_fields(&cleared, &absent).unwrap()
    ));
    let foreign = DynamicFieldValues::from_values(
        DynamicFieldDefinitions::new("Foreign", "dynamic-v1", []).unwrap(),
        HashMap::new(),
    )
    .unwrap();
    assert!(LoadedSnapshot::with_dynamic_fields(&cleared, &foreign).is_err());
}

#[test]
fn loaded_fk_does_not_claim_target_details_or_dynamic_property_availability() {
    let state = Arc::new(LoadedSnapshot::projection(
        layout("v1"),
        ["school_type_id", "_total_amount"],
    ));
    assert!(state.is_loaded("school_type_id"));
    assert!(!state.is_loaded("school_type"));
    assert!(!state.is_loaded("_total_amount"));
    assert!(LoadedSnapshot::with_loaded(&state, "typo", true).is_err());
    let detail = LoadedSnapshot::with_loaded(&state, "school_type", true).unwrap();
    assert!(detail.is_loaded("school_type"));
    assert!(!state.is_loaded("school_type"));
}

#[test]
fn invalid_generated_tables_fail_instead_of_renumbering() {
    for indexes in [
        vec![("id", 0), ("version", 0)],
        vec![("id", 0), ("version", 64)],
        vec![("id", 0), ("id", 1)],
    ] {
        assert!(
            FieldLayout::from_generated(
                "School",
                "v1",
                &indexes,
                &[("id", "id", "id"), ("version", "version", "version")],
                &[],
                &["id", "version"]
            )
            .is_err()
        );
    }
    assert!(
        FieldLayout::from_generated(
            "School",
            "v1",
            &[("id", 0), ("version", 1)],
            &[("id", "id", "id")],
            &[],
            &["id", "version"]
        )
        .is_err()
    );
    assert!(
        FieldLayout::from_generated(
            "School",
            "v1",
            &[("id", 0), ("version", 1)],
            &[("id", "id", "id"), ("version", "version", "version")],
            &[],
            &["id", "version", "missing"]
        )
        .is_err()
    );
}

#[test]
fn layout_revision_and_internal_wire_boundaries_remain_distinct() {
    let left = Arc::new(LoadedSnapshot::projection(layout("v1"), ["id"]));
    let right = Arc::new(LoadedSnapshot::projection(layout("v2"), ["id"]));
    assert_ne!(left, right);
    let mut state = LoadState::Indexed(left);
    state.mark_loaded("version").unwrap();
    assert!(state.is_loaded("version"));
    assert!(serde_json::to_string(&state).is_err());
    assert!(
        serde_json::from_str::<LoadState>(r#"{"Indexed":{"bits":18446744073709551615}}"#).is_err()
    );
}

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "TypedIndexedRow", indexed_layout)]
struct TypedIndexedRow {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    #[teaql(column = "base_url")]
    display_name: Option<String>,
    active: bool,
    #[teaql(skip)]
    __load_state: LoadState,
}

impl TypedIndexedRow {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "typed-fixture-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("base_url", 2), ("active", 3)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("base_url", "display_name", "base_url"),
        ("active", "active", "active"),
    ];
}

#[test]
fn independently_built_same_shape_rows_share_snapshot_and_leave_absent_defaults_out_of_mutations() {
    use teaql_core::{CompactRow, Entity, Record, Value};
    let decode = |id| {
        TypedIndexedRow::from_compact_row(CompactRow::from_map(Record::from([
            ("id".to_owned(), Value::U64(id)),
            ("version".to_owned(), Value::I64(1)),
        ])))
        .unwrap()
    };
    let first = decode(1);
    let second = decode(2);
    let (LoadState::Indexed(left), LoadState::Indexed(right)) =
        (&first.__load_state, &second.__load_state)
    else {
        panic!("indexed state")
    };
    assert!(Arc::ptr_eq(left, right));
    assert!(!first.is_field_loaded("active"));
    assert!(!first.active);
    assert!(!first.into_values().contains_key("active"));
    let actual = TypedIndexedRow::from_compact_row(CompactRow::from_map(Record::from([
        ("id".to_owned(), Value::U64(3)),
        ("version".to_owned(), Value::I64(0)),
        ("active".to_owned(), Value::Bool(false)),
    ])))
    .unwrap();
    assert!(actual.is_field_loaded("active"));
    let values = actual.into_values();
    assert_eq!(values.get("active"), Some(&Value::Bool(false)));
    assert_eq!(values.get("version"), Some(&Value::I64(0)));
}

#[test]
fn cow_rejoins_compatible_shape_without_retaining_overflow_or_cross_revision_masks() {
    let type_layout = layout("v1");
    let empty = LoadedSnapshot::projection(type_layout.clone(), []).into_shared();
    let populated = LoadedSnapshot::with_loaded(&empty, "field_64", true).unwrap();
    let cleared = LoadedSnapshot::with_loaded(&populated, "field_64", false).unwrap();
    assert!(Arc::ptr_eq(&empty, &cleared));
    assert!(cleared.overflow().is_none());
    let selected = LoadedSnapshot::with_loaded(&empty, "#extra_note", true).unwrap();
    let cleared = LoadedSnapshot::with_loaded(&selected, "#extra_note", false).unwrap();
    assert!(Arc::ptr_eq(&empty, &cleared));
    assert!(cleared.selected_names().is_none());
    let foreign = LoadState::Indexed(LoadedSnapshot::projection(super_layout(), []).into_shared());
    assert!(foreign.into_indexed(type_layout).is_err());
    fn super_layout() -> Arc<FieldLayout> {
        layout("v2")
    }
}

#[test]
fn changed_and_manual_row_shapes_are_grouped_before_decode_without_union() {
    use teaql_core::{CompactRow, Record, Value};
    let mut rows = vec![
        CompactRow::from_map(Record::from([("id".to_owned(), Value::U64(1))])),
        CompactRow::from_map(Record::from([
            ("id".to_owned(), Value::U64(2)),
            ("version".to_owned(), Value::I64(1)),
        ])),
        CompactRow::from_map(Record::from([("id".to_owned(), Value::U64(3))])),
    ];
    CompactRow::share_layouts(&mut rows);
    assert!(Arc::ptr_eq(
        &rows[0].shared_layout(),
        &rows[2].shared_layout()
    ));
    assert!(!Arc::ptr_eq(
        &rows[0].shared_layout(),
        &rows[1].shared_layout()
    ));
    assert!(!rows[0].contains_key("version"));
    assert_eq!(rows[2].get("id"), Some(&Value::U64(3)));
}

#[test]
fn physical_scalar_alias_decodes_actual_payload_not_a_loaded_default() {
    use teaql_core::{CompactRow, Entity, Record, Value};
    let row = TypedIndexedRow::from_compact_row(CompactRow::from_map(Record::from([
        ("id".to_owned(), Value::U64(1)),
        ("version".to_owned(), Value::I64(1)),
        ("base_url".to_owned(), Value::Text("actual URL".to_owned())),
    ])))
    .unwrap();
    assert_eq!(row.display_name.as_deref(), Some("actual URL"));
    assert!(row.is_field_loaded("display_name"));
}

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "TypedParent", indexed_layout)]
struct TypedParent {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    #[teaql(column = "related")]
    related_id: Option<u64>,
    #[teaql(relation(
        target = "TypedIndexedRow",
        local_key = "related_id",
        foreign_key = "id"
    ))]
    related: Option<Box<TypedIndexedRow>>,
    #[teaql(skip)]
    __load_state: LoadState,
}
impl TypedParent {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "typed-parent-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("related", 2)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("related", "related_id", "related"),
    ];
}

#[test]
fn physical_fk_alias_and_nested_object_are_distinguished_without_losing_identity() {
    use teaql_core::{CompactRow, Entity, Record, Value};
    for (value, id, details) in [
        (Value::U64(7), Some(7), false),
        (Value::Null, None, false),
        (
            Value::object(Record::from([
                ("id".to_owned(), Value::U64(7)),
                ("version".to_owned(), Value::I64(1)),
            ])),
            Some(7),
            true,
        ),
    ] {
        let row = TypedParent::from_compact_row(CompactRow::from_map(Record::from([
            ("id".to_owned(), Value::U64(1)),
            ("version".to_owned(), Value::I64(1)),
            ("related".to_owned(), value),
        ])))
        .unwrap();
        assert_eq!(row.related_id, id);
        assert!(row.is_field_loaded("related_id"));
        assert_eq!(row.is_field_loaded("related"), details);
        assert_eq!(
            row.related.as_ref().map(|value| value.id),
            if details { id } else { None }
        );
    }
}

#[test]
fn concurrent_decoders_reuse_one_immutable_snapshot() {
    use teaql_core::{CompactRow, CompactRowLayout, Entity, Value};
    let shape = CompactRowLayout::new(Arc::from(["id".to_owned(), "version".to_owned()]));
    let rows = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for index in 0..16 {
            let shape = shape.clone();
            handles.push(scope.spawn(move || {
                TypedIndexedRow::from_compact_row(CompactRow::with_layout(
                    shape,
                    vec![Value::U64(index + 1), Value::I64(1)],
                ))
                .unwrap()
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    let LoadState::Indexed(first) = &rows[0].__load_state else {
        panic!("indexed state")
    };
    for row in &rows {
        let LoadState::Indexed(state) = &row.__load_state else {
            panic!("indexed state")
        };
        assert!(Arc::ptr_eq(first, state));
    }
}

#[test]
fn typed_decode_shares_one_snapshot_for_small_and_large_results_without_sharing_values() {
    use teaql_core::{CompactRow, CompactRowLayout, Entity, Value};
    for count in [1, 100, 10_000] {
        let shape = CompactRowLayout::new(Arc::from([
            "id".to_owned(),
            "version".to_owned(),
            "display_name".to_owned(),
        ]));
        let rows: Vec<_> = (0..count)
            .map(|index| {
                TypedIndexedRow::from_compact_row(CompactRow::with_layout(
                    shape.clone(),
                    vec![
                        Value::U64(index as u64 + 1),
                        Value::I64(1),
                        if index == 0 {
                            Value::Null
                        } else {
                            Value::Text(format!("row-{index}"))
                        },
                    ],
                ))
                .unwrap()
            })
            .collect();
        let LoadState::Indexed(first) = &rows[0].__load_state else {
            panic!("indexed load state not installed")
        };
        for row in &rows {
            let LoadState::Indexed(state) = &row.__load_state else {
                panic!("non-indexed row")
            };
            assert!(Arc::ptr_eq(first, state));
            assert!(state.overflow().is_none());
            assert!(state.selected_names().is_none());
            assert!(row.is_field_loaded("base_url"));
        }
        assert!(rows[0].display_name.is_none());
        if count > 1 {
            assert_eq!(rows[1].display_name.as_deref(), Some("row-1"));
        }
    }
}

#[test]
fn row_shape_changes_detach_while_value_changes_keep_the_projection_cache() {
    use teaql_core::{CompactRow, CompactRowLayout, Entity, Value};
    let shape = CompactRowLayout::new(Arc::from(["id".to_owned(), "version".to_owned()]));
    let first = CompactRow::with_layout(shape.clone(), vec![Value::U64(1), Value::I64(1)]);
    let mut second = CompactRow::with_layout(shape, vec![Value::U64(2), Value::I64(1)]);
    second.insert("id".to_owned(), Value::U64(3));
    assert!(Arc::ptr_eq(&first.shared_layout(), &second.shared_layout()));
    second.insert("display_name".to_owned(), Value::Null);
    assert!(!Arc::ptr_eq(
        &first.shared_layout(),
        &second.shared_layout()
    ));
    let first = TypedIndexedRow::from_compact_row(first).unwrap();
    let mut second = TypedIndexedRow::from_compact_row(second).unwrap();
    assert!(!first.is_field_loaded("display_name"));
    assert!(second.is_field_loaded("display_name"));
    second.set_checker_loaded_fields(std::collections::BTreeSet::from(["id".to_owned()]));
    assert!(!second.is_field_loaded("display_name"));
    assert!(!first.is_field_loaded("display_name"));
}
