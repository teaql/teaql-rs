use std::sync::Arc;
use teaql_core::dynamic_fields::DynamicFieldMergeShapes;
use teaql_core::dynamic_properties::DynamicPropertyDefinitions;
use teaql_core::eval::LoadState;
use teaql_core::{CompactRow, CompactRowLayout, DataType, FieldLayout, Value};

fn layout() -> Arc<FieldLayout> {
    FieldLayout::from_generated(
        "Probe",
        "v1",
        &[("id", 0), ("version", 1), ("name", 2)],
        &[
            ("id", "id", "id"),
            ("version", "version", "version"),
            ("name", "name", "name"),
        ],
        &[],
        &["id", "version", "name"],
    )
    .unwrap()
}

#[test]
fn compatible_result_shapes_share_schema_not_values_or_presence() {
    let definitions = DynamicPropertyDefinitions::new([
        ("_count".into(), DataType::I64),
        ("_missing".into(), DataType::I64),
    ])
    .unwrap();
    let columns = CompactRowLayout::new(Arc::from(["id".into(), "_count".into()]))
        .with_dynamic_property_definitions(definitions.clone());
    let mut rows = vec![
        CompactRow::with_layout(columns.clone(), vec![Value::U64(1), Value::I64(0)]),
        CompactRow::with_layout(columns, vec![Value::U64(2), Value::Null]),
    ];
    CompactRow::share_layouts(&mut rows);
    let fixed = layout();
    let LoadState::Indexed(first) = rows[0].indexed_load_state(fixed.clone()) else {
        panic!()
    };
    let LoadState::Indexed(second) = rows[1].indexed_load_state(fixed.clone()) else {
        panic!()
    };
    assert!(Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(
        first.dynamic_property_definitions().unwrap(),
        &definitions
    ));
    assert_eq!(definitions.data_type("_missing"), Some(DataType::I64));
    assert!(!rows[0].contains_key("_missing"));
    assert!(!first.is_loaded("_count"));
    assert_eq!(fixed.index("_count"), None);
    assert_eq!(fixed.field_count(), 3);
    assert_eq!(rows[0].get("_count"), Some(&Value::I64(0)));
    assert_eq!(rows[1].get("_count"), Some(&Value::Null));
    assert!(
        rows.iter()
            .all(|row| row.validate_dynamic_properties().is_ok())
    );
}

#[test]
fn shape_changes_preserve_metadata_and_conflicting_enhancements_are_atomic() {
    let definitions = DynamicPropertyDefinitions::new([("_count".into(), DataType::I64)]).unwrap();
    let columns = CompactRowLayout::new(Arc::from(["id".into(), "_count".into()]))
        .with_dynamic_property_definitions(definitions);
    let mut row = CompactRow::with_layout(columns, vec![Value::U64(1), Value::I64(7)]);
    row.insert("name".into(), Value::Text("native".into()));
    row.remove("name");
    let old = row.clone();
    let conflicting = CompactRowLayout::new(Arc::from(["id".into(), "_count".into()]))
        .with_dynamic_property_definitions(
            DynamicPropertyDefinitions::new([("_count".into(), DataType::Text)]).unwrap(),
        );
    let mut mixed = vec![
        old.clone(),
        CompactRow::with_layout(
            conflicting.clone(),
            vec![Value::U64(2), Value::Text("private".into())],
        ),
    ];
    CompactRow::share_layouts(&mut mixed);
    assert!(!Arc::ptr_eq(
        &mixed[0].shared_layout(),
        &mixed[1].shared_layout()
    ));
    let error = row
        .try_extend(
            CompactRow::with_layout(
                conflicting,
                vec![Value::U64(2), Value::Text("secret".into())],
            ),
            &mut DynamicFieldMergeShapes::default(),
        )
        .unwrap_err();
    assert!(!error.to_string().contains("secret"));
    assert_eq!(row, old);
    assert!(row.validate_dynamic_properties().is_ok());
}

#[test]
fn explicit_types_validate_null_variants_without_creating_presence() {
    for name in ["name", "#count", "_"] {
        assert!(DynamicPropertyDefinitions::new([(name.into(), DataType::I64)]).is_err());
    }
    assert!(
        DynamicPropertyDefinitions::new([
            ("_count".into(), DataType::I64),
            ("_count".into(), DataType::I64)
        ])
        .is_err()
    );
    let definitions = DynamicPropertyDefinitions::new([("_count".into(), DataType::I64)]).unwrap();
    for value in [
        Value::Null,
        Value::TypedNull(DataType::I64),
        Value::I64(0),
        Value::Json(0.into()),
    ] {
        definitions.validate_value("_count", &value).unwrap();
    }
    assert!(
        definitions
            .validate_value("_count", &Value::TypedNull(DataType::Text))
            .is_err()
    );
    assert!(
        definitions
            .validate_value("_count", &Value::Text("secret".into()))
            .unwrap_err()
            .contains("_count")
    );
}
