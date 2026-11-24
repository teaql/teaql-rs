use teaql_core::dynamic_properties::DynamicPropertyDefinitions;
use teaql_core::{DataType, EntityDescriptor, Expr, PropertyDescriptor, Value};

#[test]
fn binary_values_preserve_bytes_and_never_coerce_to_text_or_numbers() {
    for bytes in [vec![], vec![0], vec![0xb0, 0xa2], vec![0xff, 0, 0x27]] {
        let value = Value::Binary(bytes.clone());
        assert_eq!(value.try_binary(), Some(bytes.as_slice()));
        assert_eq!(value.try_text(), None);
        assert_eq!(value.try_i64(), None);
        assert_eq!(value.try_decimal(), None);
        assert_ne!(value, Value::Null);
        assert_ne!(value, Value::List(vec![]));
    }
    assert_eq!(Value::Null.try_binary(), None);
    assert_eq!(Value::Text("AA==".into()).try_binary(), None);
}

#[test]
fn binary_json_uses_base64_without_lossy_utf8_or_integer_arrays() {
    for (bytes, base64) in [(vec![], ""), (vec![0], "AA=="), (vec![0xb0, 0xa2], "sKI=")] {
        assert_eq!(
            Value::Binary(bytes).to_json_value(),
            serde_json::json!(base64)
        );
    }
    assert_eq!(
        Value::TypedNull(DataType::Binary).to_json_value(),
        serde_json::Value::Null
    );
    assert_eq!(
        Value::binary_from_base64("sKI="),
        Some(Value::Binary(vec![0xb0, 0xa2]))
    );
    for invalid in ["sKI", "sKI===", "sKJ=", "?", "\nAA=="] {
        assert_eq!(Value::binary_from_base64(invalid), None);
    }
}

#[test]
fn binary_dynamic_property_metadata_is_distinct_from_text_and_json() {
    let definitions =
        DynamicPropertyDefinitions::new([("_encoded".into(), DataType::Binary)]).unwrap();
    for value in [
        Value::Binary(vec![]),
        Value::Binary(vec![0xb0, 0xa2]),
        Value::Null,
        Value::TypedNull(DataType::Binary),
    ] {
        definitions.validate_value("_encoded", &value).unwrap();
    }
    for value in [
        Value::from("sKI="),
        Value::Json(serde_json::json!("sKI=")),
        Value::TypedNull(DataType::Text),
        Value::List(vec![]),
    ] {
        assert!(definitions.validate_value("_encoded", &value).is_err());
    }
}

#[test]
fn gbk_grouping_exposes_a_binary_result_but_rejects_binary_or_numeric_inputs() {
    use teaql_core::aggregation_plan::grouping_expression_source;
    let descriptor = EntityDescriptor::new("Kind")
        .property(PropertyDescriptor::new("label", DataType::Text).column_name("Legacy Label"))
        .property(PropertyDescriptor::new("amount", DataType::Decimal))
        .property(PropertyDescriptor::new("bytes", DataType::Binary));
    assert_eq!(
        grouping_expression_source(&descriptor, &Expr::gbk(Expr::column("label"))).unwrap(),
        ("label".into(), DataType::Binary)
    );
    for expression in [
        Expr::gbk(Expr::column("amount")),
        Expr::gbk(Expr::column("bytes")),
        Expr::gbk(Expr::gbk(Expr::column("label"))),
        Expr::soundex(Expr::gbk(Expr::column("label"))),
    ] {
        assert!(grouping_expression_source(&descriptor, &expression).is_err());
    }
}
