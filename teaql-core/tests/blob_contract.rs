use teaql_core::{BlobFieldSpec, BlobRef, Value};

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "BlobAsset")]
struct BlobAsset {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    document: Option<BlobRef>,
}

#[test]
fn native_text_rows_decode_to_typed_values_and_reject_invalid_keys() {
    use teaql_core::{CompactRow, Entity, Record, TeaqlEntity};
    let row = |value| {
        Record::from([
            ("id".into(), Value::U64(1)),
            ("version".into(), Value::I64(1)),
            ("document".into(), value),
        ])
    };
    let entity =
        BlobAsset::from_compact_row(CompactRow::from_map(row(Value::Text("blob_native".into()))))
            .unwrap();
    assert_eq!(entity.document.as_ref().unwrap().as_str(), "blob_native");
    assert!(matches!(
        entity.into_values().get("document"),
        Some(Value::BlobRef(_))
    ));
    assert!(
        BlobAsset::from_compact_row(CompactRow::from_map(row(Value::Text(
            "https://example.invalid/file".into()
        ))))
        .is_err()
    );
    assert!(
        BlobAsset::from_compact_row(CompactRow::from_map(row(Value::Text("".into())))).is_err()
    );
    assert!(
        BlobAsset::from_compact_row(CompactRow::from_map(row(Value::Null)))
            .unwrap()
            .document
            .is_none()
    );
    let descriptor = <BlobAsset as TeaqlEntity>::entity_descriptor();
    assert_eq!(
        descriptor.property_by_name("document").unwrap().data_type,
        teaql_core::DataType::BlobRef
    );
}

#[test]
fn shared_keys_json_and_all_formats_match() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/blob-value-cases.json")).unwrap();
    for key in cases["valid"].as_array().unwrap() {
        let reference = BlobRef::new(key.as_str().unwrap()).unwrap();
        assert_eq!(reference.as_str(), key.as_str().unwrap());
        assert_eq!(serde_json::to_value(&reference).unwrap(), *key);
        assert_eq!(
            serde_json::from_value::<BlobRef>(key.clone()).unwrap(),
            reference
        );
        assert_eq!(Value::from(&reference).to_json_value(), *key);
        assert!(Value::from(&reference).try_text().is_none());
        assert!(!format!("{reference:?}").contains(reference.as_str()));
    }
    for group in ["invalid", "invalidJson"] {
        for value in cases[group].as_array().unwrap() {
            assert!(serde_json::from_value::<BlobRef>(value.clone()).is_err());
        }
    }
    assert!(serde_json::from_str::<BlobRef>("null").is_err());
    assert_eq!(
        serde_json::from_str::<Option<BlobRef>>("null").unwrap(),
        None
    );
    for (extension, mime) in cases["formats"].as_object().unwrap() {
        let spec =
            BlobFieldSpec::new("Asset", "document", [extension.as_str()], true, "default").unwrap();
        assert_eq!(
            spec.allowed_content_types().into_iter().collect::<Vec<_>>(),
            vec![mime.as_str().unwrap()]
        );
    }
}

#[test]
fn field_contract_is_canonical_and_has_no_sample_default() {
    let spec = BlobFieldSpec::new(
        "Asset",
        "document",
        ["pdf", "docx", "pdf"],
        false,
        "document",
    )
    .unwrap();
    assert_eq!(
        spec.allowed_extensions()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["docx", "pdf"]
    );
    assert_eq!(spec.protocol_version(), "blob.v1");
    assert_eq!(spec.max_files(), 1);
    assert!(
        BlobFieldSpec::new("Asset", "document", Vec::<String>::new(), true, "default").is_err()
    );
    assert!(BlobFieldSpec::new("Asset", "document", ["DOCX"], true, "default").is_err());
}
