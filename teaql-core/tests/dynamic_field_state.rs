use std::collections::HashMap;
#[test]
fn storage_binding_is_opaque_and_does_not_clone_the_definition_dictionary() {
    let definitions = definitions();
    let first = definitions.with_storage_binding("PRIVATE-PROFILE-CANARY", 17);
    let same = first.with_storage_binding("PRIVATE-PROFILE-CANARY", 17);
    assert!(std::sync::Arc::ptr_eq(&first, &same));
    assert!(std::ptr::eq(definitions.fields(), first.fields()));
    assert!(definitions.same_schema(&first));
    assert_ne!(definitions, first);
    assert_ne!(first, definitions.with_storage_binding("other", 17));
    assert_ne!(
        first,
        definitions.with_storage_binding("PRIVATE-PROFILE-CANARY", 18)
    );
    assert!(!format!("{first:?}").contains("PRIVATE-PROFILE-CANARY"));
}
use std::sync::Arc;
use teaql_core::dynamic_fields::{DynamicFieldDefinitions, DynamicFieldState, DynamicFieldValues};
use teaql_core::{DataType, Value};

#[test]
fn compact_enhancements_merge_private_values_and_share_actual_union_geometry() {
    use teaql_core::dynamic_fields::DynamicFieldMergeShapes;
    use teaql_core::{CompactRow, CompactRowLayout, RelationShapeCache};
    for count in [1, 100, 10_000] {
        let defs = definitions();
        let left_fields = DynamicFieldValues::from_batch(
            defs.clone(),
            (0..count).map(|index| {
                HashMap::from([
                    ("note".into(), Value::Text(format!("private-{index}"))),
                    ("number".into(), Value::I64(index as i64)),
                ])
            }),
        )
        .unwrap();
        let right_fields = DynamicFieldValues::from_batch(
            defs,
            (0..count).map(|_| {
                HashMap::from([
                    ("extra".into(), Value::Null),
                    ("number".into(), Value::I64(0)),
                ])
            }),
        )
        .unwrap();
        let layout = CompactRowLayout::new(Arc::from(["id".into(), "name".into()]));
        let mut relations = RelationShapeCache::default();
        let mut merges = DynamicFieldMergeShapes::default();
        let mut rows = Vec::with_capacity(count);
        for (index, (left, right)) in left_fields.into_iter().zip(right_fields).enumerate() {
            let mut row =
                CompactRow::with_layout(layout.clone(), vec![(index as u64).into(), "old".into()]);
            row.set_loaded_dynamic_fields(left);
            let mut enhancement = CompactRow::with_layout(
                layout.clone(),
                vec![(index as u64).into(), "updated".into()],
            );
            enhancement.set_loaded_dynamic_fields(right);
            enhancement.mark_relation_loaded("platform", &mut relations);
            row.try_extend(enhancement, &mut merges).unwrap();
            assert!(row.is_loaded_relation("platform"));
            assert_eq!(row.get("name"), Some(&Value::Text("updated".into())));
            rows.push(row);
        }
        CompactRow::share_layouts(&mut rows);
        let merged_layout = rows[0].shared_layout();
        let mut first_shape = None;
        for (index, row) in rows.iter_mut().enumerate() {
            assert!(Arc::ptr_eq(&merged_layout, &row.shared_layout()));
            let fields = row.take_loaded_dynamic_fields().unwrap();
            if let Some(shape) = &first_shape {
                assert!(Arc::ptr_eq(shape, fields.selected_codes()));
            } else {
                first_shape = Some(fields.selected_codes().clone());
            }
            assert_eq!(
                fields.field("note").unwrap().value(),
                Some(&Value::Text(format!("private-{index}")))
            );
            assert_eq!(
                fields.field("extra").unwrap().state(),
                DynamicFieldState::Null
            );
            assert_eq!(
                fields.field("number").unwrap().value(),
                Some(&Value::I64(0))
            );
            assert_eq!(
                fields.field("active").unwrap().state(),
                DynamicFieldState::NotLoaded
            );
            assert!(!row.clone().into_map().contains_key("#note"));
        }
    }
}

#[test]
fn incompatible_dynamic_enhancements_reject_atomically_without_provenance_leaks() {
    use teaql_core::CompactRow;
    use teaql_core::dynamic_fields::DynamicFieldMergeShapes;
    let trusted = definitions().with_storage_binding("PRIVATE-PROFILE-CANARY", 17);
    let candidates = [
        definitions(),
        definitions().with_storage_binding("other", 17),
        definitions().with_storage_binding("PRIVATE-PROFILE-CANARY", 18),
        DynamicFieldDefinitions::new("Other", "dynamic-v1", [("note".into(), DataType::Text)])
            .unwrap(),
        DynamicFieldDefinitions::new("School", "revision-2", [("note".into(), DataType::Text)])
            .unwrap(),
        DynamicFieldDefinitions::new("School", "dynamic-v1", [("note".into(), DataType::I64)])
            .unwrap(),
    ];
    for defs in candidates {
        let mut row = CompactRow::new(
            Arc::from(["id".into(), "name".into()]),
            vec![1_u64.into(), "keep".into()],
        );
        row.set_loaded_dynamic_fields(
            DynamicFieldValues::from_values(
                trusted.clone(),
                HashMap::from([("note".into(), "keep original".into())]),
            )
            .unwrap(),
        );
        let before = row.clone();
        let mut other = CompactRow::new(
            Arc::from(["id".into(), "name".into()]),
            vec![1_u64.into(), "replace".into()],
        );
        other.set_loaded_dynamic_fields(
            DynamicFieldValues::from_values(defs, HashMap::new()).unwrap(),
        );
        let error = row
            .try_extend(other, &mut DynamicFieldMergeShapes::default())
            .unwrap_err();
        assert_eq!(error.code, "DYNAMIC_FIELD_INCOMPATIBLE_VIEW");
        assert!(!error.to_string().contains("PRIVATE"));
        assert_eq!(row, before);
    }
}

#[test]
fn enhancement_empty_selection_keeps_existing_values_and_superset_keeps_its_shared_arc() {
    use teaql_core::dynamic_fields::DynamicFieldMergeShapes;
    let defs = definitions();
    let mut fields = DynamicFieldValues::from_values(
        defs.clone(),
        HashMap::from([("note".into(), "existing".into())]),
    )
    .unwrap();
    let shape = fields.selected_codes().clone();
    fields
        .merge_loaded(
            DynamicFieldValues::from_values(defs.clone(), HashMap::new()).unwrap(),
            &mut DynamicFieldMergeShapes::default(),
        )
        .unwrap();
    assert!(Arc::ptr_eq(&shape, fields.selected_codes()));
    let other = DynamicFieldValues::from_values(
        defs,
        HashMap::from([
            ("note".into(), Value::Null),
            ("extra".into(), "added".into()),
        ]),
    )
    .unwrap();
    let superset = other.selected_codes().clone();
    fields
        .merge_loaded(other, &mut DynamicFieldMergeShapes::default())
        .unwrap();
    assert!(Arc::ptr_eq(&superset, fields.selected_codes()));
    assert_eq!(
        fields.field("note").unwrap().state(),
        DynamicFieldState::Null
    );
}

fn definitions() -> Arc<DynamicFieldDefinitions> {
    DynamicFieldDefinitions::new(
        "School",
        "dynamic-v1",
        [
            ("note".into(), DataType::Text),
            ("extra".into(), DataType::Text),
            ("number".into(), DataType::I64),
            ("active".into(), DataType::Bool),
        ],
    )
    .unwrap()
}

#[test]
fn wrapper_preserves_zero_false_empty_null_and_not_loaded_without_io() {
    let row = DynamicFieldValues::from_values(
        definitions(),
        HashMap::from([
            ("note".into(), Value::Text(String::new())),
            ("number".into(), Value::I64(0)),
            ("active".into(), Value::Bool(false)),
        ]),
    )
    .unwrap();
    for code in ["note", "number", "active"] {
        let field = row.field(code).unwrap();
        assert_eq!(field.state(), DynamicFieldState::Value);
        assert!(field.is_loaded());
        assert!(field.value().is_some());
    }
    let field = row.field("extra").unwrap();
    assert_eq!(field.state(), DynamicFieldState::NotLoaded);
    assert!(!field.is_loaded());
    assert!(field.value().is_none());
    let nil = DynamicFieldValues::from_values(
        definitions(),
        HashMap::from([("extra".into(), Value::TypedNull(DataType::Text))]),
    )
    .unwrap();
    assert_eq!(nil.field("extra").unwrap().state(), DynamicFieldState::Null);
    assert!(nil.field("extra").unwrap().is_loaded());
    assert!(nil.field("extra").unwrap().value().is_none());
    assert_eq!(
        row.field("unknown").unwrap_err().code,
        "DYNAMIC_FIELD_NOT_FOUND"
    );
}

#[test]
fn one_selection_snapshot_per_actual_batch_shape_and_private_row_values() {
    for count in [1, 100, 10_000] {
        let mut rows = DynamicFieldValues::from_batch(
            definitions(),
            (0..count).map(|index| {
                HashMap::from([(
                    "note".into(),
                    if index % 2 == 0 {
                        Value::Text(index.to_string())
                    } else {
                        Value::Null
                    },
                )])
            }),
        )
        .unwrap();
        for row in &rows {
            assert!(Arc::ptr_eq(row.definitions(), rows[0].definitions()));
            assert!(Arc::ptr_eq(row.selected_codes(), rows[0].selected_codes()));
        }
        let cloned = rows[0].clone();
        rows.clear();
        assert_eq!(
            cloned.field("note").unwrap().value(),
            Some(&Value::Text("0".into()))
        );
    }
    let rows = DynamicFieldValues::from_batch(
        definitions(),
        [
            HashMap::from([("note".into(), Value::Text("private".into()))]),
            HashMap::from([("note".into(), Value::Null)]),
            HashMap::new(),
        ],
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        rows[0].selected_codes(),
        rows[1].selected_codes()
    ));
    assert!(!Arc::ptr_eq(
        rows[1].selected_codes(),
        rows[2].selected_codes()
    ));
    assert_eq!(
        rows[1].field("note").unwrap().state(),
        DynamicFieldState::Null
    );
    assert_eq!(
        rows[2].field("note").unwrap().state(),
        DynamicFieldState::NotLoaded
    );
    assert_eq!(
        rows[0].field("extra").unwrap().state(),
        DynamicFieldState::NotLoaded
    );
}

#[test]
fn unknown_definitions_wrong_types_and_derived_namespace_are_rejected_without_values_in_errors() {
    for code in ["#note", "_total", " "] {
        assert!(
            DynamicFieldDefinitions::new("School", "v1", [(code.into(), DataType::Text)]).is_err()
        );
    }
    assert_eq!(
        DynamicFieldDefinitions::new(
            "School",
            "v1",
            [
                ("note".into(), DataType::Text),
                ("note".into(), DataType::Bool),
            ]
        )
        .unwrap_err()
        .code,
        "DYNAMIC_FIELD_DUPLICATE_CODE"
    );
    let mismatch = DynamicFieldValues::from_values(
        definitions(),
        HashMap::from([("active".into(), Value::Text("PRIVATE-CANARY".into()))]),
    )
    .unwrap_err();
    assert_eq!(mismatch.code, "DYNAMIC_FIELD_TYPE_MISMATCH");
    assert!(!mismatch.to_string().contains("PRIVATE-CANARY"));
    assert!(
        DynamicFieldValues::from_values(
            definitions(),
            HashMap::from([("note".into(), Value::TypedNull(DataType::I64)),])
        )
        .is_err()
    );
    assert!(
        DynamicFieldValues::from_values(
            definitions(),
            HashMap::from([("_total".into(), Value::I64(1)),])
        )
        .is_err()
    );
}

#[test]
fn selecting_another_shape_does_not_widen_old_rows_or_revisions() {
    let original = DynamicFieldValues::from_values(
        definitions(),
        HashMap::from([("note".into(), Value::Text("private".into()))]),
    )
    .unwrap();
    let another = DynamicFieldValues::from_values(
        original.definitions().clone(),
        HashMap::from([
            ("note".into(), Value::Null),
            ("extra".into(), Value::Text("new".into())),
        ]),
    )
    .unwrap();
    assert!(original.selected_codes().contains("note"));
    assert!(!original.selected_codes().contains("extra"));
    assert!(another.selected_codes().contains("extra"));
    assert_eq!(
        original.field("note").unwrap().value(),
        Some(&Value::Text("private".into()))
    );
    let other_type =
        DynamicFieldDefinitions::new("Platform", "dynamic-v1", [("note".into(), DataType::Text)])
            .unwrap();
    assert_ne!(original.definitions(), &other_type);
}

#[test]
fn dynamic_hydration_preserves_fixed_bits_relations_and_sibling_state() {
    use teaql_core::{FieldLayout, LoadedSnapshot};
    let layout = FieldLayout::from_generated(
        "School",
        "fixed-v1",
        &[("id", 0), ("version", 1), ("name", 2)],
        &[
            ("id", "id", "id"),
            ("version", "version", "version"),
            ("name", "name", "name"),
        ],
        &["platform"],
        &["id", "version", "name"],
    )
    .unwrap();
    let base =
        LoadedSnapshot::projection(layout.clone(), ["id", "version", "platform"]).into_shared();
    let rows = DynamicFieldValues::from_batch(
        definitions(),
        [
            HashMap::from([("note".into(), Value::Text("private".into()))]),
            HashMap::from([("note".into(), Value::Null)]),
            HashMap::new(),
        ],
    )
    .unwrap();
    let value_state = LoadedSnapshot::with_dynamic_fields(&base, &rows[0]).unwrap();
    let null_state = LoadedSnapshot::with_dynamic_fields(&base, &rows[1]).unwrap();
    assert!(Arc::ptr_eq(&value_state, &null_state));
    assert!(Arc::ptr_eq(
        &base,
        &LoadedSnapshot::with_dynamic_fields(&base, &rows[2]).unwrap()
    ));
    assert_eq!(base.bits(), value_state.bits());
    assert!(value_state.is_loaded("platform"));
    assert!(value_state.is_loaded("#note"));
    assert!(!base.is_loaded("#note"));
    assert!(!value_state.is_loaded("name"));
    assert!(!value_state.is_loaded("_note"));
    let cleared = LoadedSnapshot::with_dynamic_fields(&value_state, &rows[2]).unwrap();
    assert!(Arc::ptr_eq(&cleared, &base));
    assert!(value_state.is_loaded("#note"));
    let another_owner = DynamicFieldValues::from_values(
        DynamicFieldDefinitions::new("Platform", "v1", [("note".into(), DataType::Text)]).unwrap(),
        HashMap::new(),
    )
    .unwrap();
    assert!(LoadedSnapshot::with_dynamic_fields(&base, &another_owner).is_err());
}

#[test]
fn query_selection_is_one_shared_pointer_and_cloning_cannot_change_siblings() {
    use teaql_core::{SelectQuery, dynamic_fields::DynamicFieldSelection};
    assert_eq!(
        std::mem::size_of::<Option<Arc<DynamicFieldSelection>>>(),
        std::mem::size_of::<usize>()
    );
    let query = SelectQuery::new("School").select_dynamic_fields(
        DynamicFieldSelection::fields([("note".into(), DataType::Text)]).unwrap(),
    );
    let mut clone = query.clone();
    assert!(Arc::ptr_eq(
        query.dynamic_field_selection.as_ref().unwrap(),
        clone.dynamic_field_selection.as_ref().unwrap()
    ));
    clone = clone.select_dynamic_fields(DynamicFieldSelection::All);
    assert!(!Arc::ptr_eq(
        query.dynamic_field_selection.as_ref().unwrap(),
        clone.dynamic_field_selection.as_ref().unwrap()
    ));
    assert!(
        !query
            .dynamic_field_selection
            .as_ref()
            .unwrap()
            .contains("extra")
    );
    assert!(
        clone
            .dynamic_field_selection
            .as_ref()
            .unwrap()
            .contains("extra")
    );
}

#[test]
fn value_only_assignment_retains_selection_and_delete_detaches_only_that_payload() {
    let mut rows = DynamicFieldValues::from_batch(
        definitions(),
        [
            HashMap::from([("note".into(), Value::Text("first".into()))]),
            HashMap::from([("note".into(), Value::Null)]),
        ],
    )
    .unwrap();
    let shared = rows[1].selected_codes().clone();
    rows[0].assign("note", Value::Null).unwrap();
    assert!(Arc::ptr_eq(&shared, rows[0].selected_codes()));
    assert_eq!(
        rows[0].field("note").unwrap().state(),
        DynamicFieldState::Null
    );
    assert!(rows[0].assign("note", Value::Bool(false)).is_err());
    assert_eq!(
        rows[0].field("note").unwrap().state(),
        DynamicFieldState::Null
    );
    rows[0].delete("note").unwrap();
    assert!(!Arc::ptr_eq(&shared, rows[0].selected_codes()));
    assert_eq!(
        rows[0].field("note").unwrap().state(),
        DynamicFieldState::NotLoaded
    );
    assert_eq!(
        rows[1].field("note").unwrap().state(),
        DynamicFieldState::Null
    );
    assert!(rows[0].delete("unknown").is_err());
    rows[0]
        .assign("extra", Value::Text("private".into()))
        .unwrap();
    assert_eq!(
        rows[1].field("extra").unwrap().state(),
        DynamicFieldState::NotLoaded
    );
}
