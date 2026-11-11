use super::*;
use crate::RuntimeModule;
use std::collections::BTreeMap;
use teaql_core::{Entity, Value, eval::LoadState, time::Timestamp};

#[teaql_macros::teaql_entity]
#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "WireRoot", indexed_layout)]
#[teaql(reverse_relation(
    name = "single_probe",
    target = "WireProbe",
    local_key = "id",
    foreign_key = "root_record_id",
    json_type = "Probe"
))]
#[teaql(reverse_relation(
    name = "probe_list",
    target = "WireProbe",
    local_key = "id",
    foreign_key = "root_record_id",
    many,
    json_type = "Probe"
))]
struct Root {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    display_name: Option<String>,
    #[teaql(skip)]
    __load_state: LoadState,
}
impl Root {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "wire-root-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("display_name", 2)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("display_name", "display_name", "display_name"),
    ];
}
#[teaql_macros::teaql_entity]
#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "WireProbe", indexed_layout)]
struct Probe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    reference_code: Option<String>,
    #[teaql(integer32)]
    count: i64,
    long_value: i64,
    amount: teaql_core::Decimal,
    flag: bool,
    ratio: f64,
    weight: f32,
    day: chrono::NaiveDate,
    clock: String,
    at: Timestamp,
    raw: serde_json::Value,
    #[teaql(column = "root_record")]
    root_record_id: Option<u64>,
    #[teaql(relation(target = "WireRoot", local_key = "root_record_id", foreign_key = "id"))]
    root_record: Option<Box<Root>>,
    #[teaql(dynamic)]
    properties: BTreeMap<String, Value>,
    #[teaql(skip)]
    __load_state: LoadState,
}
impl Probe {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "wire-probe-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("version", 1),
        ("reference_code", 2),
        ("count", 3),
        ("long_value", 4),
        ("amount", 5),
        ("flag", 6),
        ("ratio", 7),
        ("weight", 8),
        ("day", 9),
        ("clock", 10),
        ("at", 11),
        ("raw", 12),
        ("root_record", 13),
    ];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("reference_code", "reference_code", "reference_code"),
        ("count", "count", "count"),
        ("long_value", "long_value", "long_value"),
        ("amount", "amount", "amount"),
        ("flag", "flag", "flag"),
        ("ratio", "ratio", "ratio"),
        ("weight", "weight", "weight"),
        ("day", "day", "day"),
        ("clock", "clock", "clock"),
        ("at", "at", "at"),
        ("raw", "raw", "raw"),
        ("root_record", "root_record_id", "root_record"),
    ];
}
fn model() -> Model {
    Model::new(vec![
        Definition::new(
            "WireRoot",
            vec![
                Field::new("id", "id", Kind::Id),
                Field::new("version", "version", Kind::Version),
                Field::new("display_name", "display_name", Kind::Text).nullable(true),
                Field::new(
                    "single_probe",
                    "single_probe",
                    Kind::One {
                        target: "WireProbe".into(),
                    },
                )
                .nullable(true),
                Field::new(
                    "probe_list",
                    "probe_list",
                    Kind::Many {
                        target: "WireProbe".into(),
                    },
                ),
            ],
        ),
        Definition::new(
            "WireProbe",
            vec![
                Field::new("id", "id", Kind::Id),
                Field::new("version", "version", Kind::Version),
                Field::new("reference_code", "reference_code", Kind::Text).nullable(true),
                Field::new("count", "count", Kind::Integer),
                Field::new("long_value", "long_value", Kind::Long),
                Field::new("amount", "amount", Kind::Decimal),
                Field::new("flag", "flag", Kind::Boolean),
                Field::new("ratio", "ratio", Kind::Double),
                Field::new("weight", "weight", Kind::Float),
                Field::new("day", "day", Kind::Date),
                Field::new("clock", "clock", Kind::Time),
                Field::new("at", "at", Kind::DateTime),
                Field::new("raw", "raw", Kind::Json),
                Field::new(
                    "root_record",
                    "root_record_id",
                    Kind::Reference {
                        target: "WireRoot".into(),
                        detail_key: "root_record".into(),
                    },
                )
                .nullable(true),
            ],
        ),
    ])
    .unwrap()
}
fn context() -> UserContext {
    RuntimeModule::new()
        .entity::<Root>()
        .entity::<Probe>()
        .entity_json_model(model())
        .into_context()
}

#[test]
fn typed_json_module_installs_shared_schema_without_sharing_entity_ledgers() {
    let module = RuntimeModule::new()
        .entity::<Root>()
        .entity::<Probe>()
        .entity_json_model(model());
    let first = module.clone().into_context();
    let second = module.into_context();
    assert!(Arc::ptr_eq(
        &first.get_resource::<Model>().unwrap().schemas,
        &second.get_resource::<Model>().unwrap().schemas
    ));
    let payload = r#"{"id":"1","version":"2","displayName":"Original"}"#;
    let mut a = first
        .decode_entity_json::<Root>(payload, JsonFieldNamingProfile::CamelCase)
        .unwrap();
    let b = second
        .decode_entity_json::<Root>(payload, JsonFieldNamingProfile::CamelCase)
        .unwrap();
    a.mark_for_deletion();
    assert!(a.is_marked_as_delete());
    assert!(!b.is_marked_as_delete());
}

#[test]
fn typed_json_inverse_to_one_preserves_loaded_null_and_omitted_without_fake_fk() {
    let context = context();
    for profile in PROFILES {
        for detail in [
            Some(
                serde_json::json!({profile.render("id"):"7",profile.render("version"):"2",profile.render("root_record"):"1"}),
            ),
            Some(serde_json::Value::Null),
            None,
        ] {
            let mut payload =
                serde_json::json!({profile.render("id"):"1", profile.render("version"):"1"});
            if let Some(value) = &detail {
                payload[profile.render("single_probe")] = value.clone();
            }
            let row = context
                .decode_entity_json::<Root>(&payload.to_string(), profile)
                .unwrap();
            assert_eq!(row.is_field_loaded("single_probe"), detail.is_some());
            let output: serde_json::Value =
                serde_json::from_str(&context.encode_entity_json(&row, profile).unwrap()).unwrap();
            assert_eq!(output.get(profile.render("single_probe")), detail.as_ref());
            assert!(output.get("single_probe_id").is_none());
        }
        let invalid = serde_json::json!({profile.render("id"):"1",profile.render("version"):"1",profile.render("single_probe"):"7"});
        assert_eq!(
            context
                .decode_entity_json::<Root>(&invalid.to_string(), profile)
                .unwrap_err()
                .code,
            "WIRE_ENTITY_OBJECT_REQUIRED"
        );
    }
}
fn native() -> Probe {
    Probe::from_compact_row(
        BTreeMap::from([
            ("id".into(), Value::U64(u64::MAX)),
            ("version".into(), Value::I64(i64::MAX)),
            ("reference_code".into(), Value::Null),
            ("count".into(), Value::I64(i32::MIN.into())),
            ("long_value".into(), Value::I64(i64::MIN)),
            (
                "amount".into(),
                Value::Decimal("123456789012.3456700".parse().unwrap()),
            ),
            ("flag".into(), Value::Bool(false)),
            ("ratio".into(), Value::F64(-0.125)),
            ("weight".into(), Value::F64(1.5)),
            (
                "day".into(),
                Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()),
            ),
            ("clock".into(), Value::Text("23:59:59.123456789".into())),
            ("at".into(), Value::Timestamp(Timestamp(1709208000789))),
            (
                "raw".into(),
                Value::Json(serde_json::json!({"message":"Unicode: 精确值","count":7})),
            ),
            ("root_record_id".into(), Value::U64(1)),
        ])
        .into(),
    )
    .unwrap()
}

#[test]
fn typed_json_query_aliases_preserve_selected_null_zero_and_native_field_namespace() {
    let context = context();
    for profile in PROFILES {
        let mut row = native();
        row.properties.insert("total".into(), Value::Null);
        row.properties.insert("rows".into(), Value::I64(0));
        row.properties
            .insert("reference_code".into(), Value::Text("derived".into()));
        row.properties.insert("_known".into(), Value::Bool(false));
        let before = row.clone().into_values();
        let state = row.loaded_state_snapshot().unwrap();
        let encoded: serde_json::Value =
            serde_json::from_str(&context.encode_entity_json(&row, profile).unwrap()).unwrap();
        assert_eq!(encoded.get("_total"), Some(&serde_json::Value::Null));
        assert_eq!(encoded["_rows"], 0);
        assert_eq!(encoded["_known"], false);
        assert_eq!(encoded["_reference_code"], "derived");
        assert_eq!(
            encoded[profile.render("reference_code")],
            serde_json::Value::Null
        );
        assert!(!encoded.as_object().unwrap().contains_key("total"));
        assert!(!encoded.as_object().unwrap().contains_key("rows"));
        assert_eq!(row.clone().into_values(), before);
        assert_eq!(row.loaded_state_snapshot().unwrap(), state);
        let mut legacy = teaql_core::EntityJsonTraversal::default();
        let legacy = row.borrowed_json_values(&mut legacy).unwrap();
        assert!(legacy.contains_key("total"));
        assert!(!legacy.contains_key("_total"));
    }
}

#[test]
fn typed_json_query_alias_collision_and_reserved_alias_are_rejected_without_value_leak() {
    let context = context();
    for profile in PROFILES {
        for alias in [
            "total",
            "comment",
            "dirty_fields",
            "original_values",
            "is_new",
            "is_deleted",
        ] {
            let mut row = native();
            row.properties
                .insert(alias.into(), Value::Text("PRIVATE-CANARY".into()));
            if alias == "total" {
                row.properties.insert("_total".into(), Value::I64(0));
            }
            let error = context.encode_entity_json(&row, profile).unwrap_err();
            assert_eq!(error.code, "WIRE_FIELD_COLLISION");
            assert!(!error.to_string().contains("PRIVATE-CANARY"));
        }
    }
}

#[test]
fn typed_json_scalar_round_trip_uses_three_profiles_without_precision_or_state_loss() {
    let context = context();
    for profile in PROFILES {
        let row = native();
        let snapshot = row.loaded_state_snapshot().unwrap();
        let payload = context.encode_entity_json(&row, profile).unwrap();
        let value: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value[profile.render("id")], u64::MAX.to_string());
        assert_eq!(value[profile.render("version")], i64::MAX.to_string());
        assert_eq!(value[profile.render("long_value")], i64::MIN.to_string());
        assert_eq!(value[profile.render("count")], i32::MIN);
        assert_eq!(value[profile.render("amount")], "123456789012.3456700");
        assert_eq!(value[profile.render("root_record")], "1");
        assert!(value.get("root_record_id").is_none());
        let restored = context
            .decode_entity_json::<Probe>(&payload, profile)
            .unwrap();
        assert_eq!(restored.id, row.id);
        assert_eq!(restored.version, row.version);
        assert_eq!(restored.long_value, row.long_value);
        assert_eq!(restored.amount.to_string(), row.amount.to_string());
        assert_eq!(restored.weight, 1.5);
        assert_eq!(restored.ratio, -0.125);
        assert_eq!(restored.day, row.day);
        assert_eq!(restored.clock, row.clock);
        assert_eq!(restored.at, row.at);
        assert_eq!(restored.raw, row.raw);
        assert!(restored.is_field_loaded("reference_code"));
        assert!(restored.reference_code.is_none());
        assert!(!restored.is_new());
        assert!(restored.dirty_fields().is_none());
        assert!(Arc::ptr_eq(
            &snapshot,
            &row.loaded_state_snapshot().unwrap()
        ));
        assert!(row.dirty_fields().is_none());
    }
}
#[test]
fn typed_json_sparse_and_batch_decode_keep_projection_and_independent_ledgers() {
    let context = context();
    let model = model();
    for profile in PROFILES {
        let payload = format!(
            "{{\"{}\":\"9\",\"{}\":\"7\",\"{}\":null}}",
            profile.render("id"),
            profile.render("version"),
            profile.render("reference_code")
        );
        let mut rows = model
            .deserialize_many::<Probe>(&context, &format!("[{payload},{payload}]"), profile)
            .unwrap();
        assert!(rows[0].is_field_loaded("reference_code"));
        assert!(!rows[0].is_field_loaded("amount"));
        assert!(!rows[0].is_new());
        let shape0 = rows[0].loaded_state_snapshot().unwrap();
        let shape1 = rows[1].loaded_state_snapshot().unwrap();
        assert!(Arc::ptr_eq(&shape0, &shape1));
        rows[0].mark_for_deletion();
        assert!(rows[0].is_marked_as_delete());
        assert!(!rows[1].is_marked_as_delete());
        let output = model.serialize(&rows[1], profile).unwrap();
        let json: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert!(json.get(profile.render("amount")).is_none());
        assert!(json[profile.render("reference_code")].is_null());
    }
}
#[test]
fn typed_json_duplicate_names_aliases_and_wrong_profile_are_not_silently_accepted() {
    let context = context();
    for payload in [
        r#"{"referenceCode":"first","referenceCode":"PRIVATE-CANARY"}"#,
        r#"{"referenceCode":"first","reference_code":"PRIVATE-CANARY"}"#,
        r#"{"rootRecord":"1","root_record":"1"}"#,
    ] {
        let error = context
            .decode_entity_json::<Probe>(payload, JsonFieldNamingProfile::CamelCase)
            .unwrap_err();
        assert_eq!(error.code, "WIRE_FIELD_COLLISION");
        assert!(!error.to_string().contains("PRIVATE-CANARY"));
    }
    for payload in [
        r#"{"ReferenceCode":"PRIVATE-CANARY"}"#,
        r#"{"root_record_id":1}"#,
        r#"{"unknown":"PRIVATE-CANARY"}"#,
    ] {
        let error = context
            .decode_entity_json::<Probe>(payload, JsonFieldNamingProfile::CamelCase)
            .unwrap_err();
        assert_eq!(error.code, "WIRE_UNKNOWN_FIELD");
        assert!(!error.to_string().contains("PRIVATE-CANARY"));
    }
}
#[test]
fn typed_json_invalid_scalar_classes_reject_without_exposing_input() {
    let context = context();
    for (field, value) in [
        ("longValue", "9223372036854775808"),
        ("id", "18446744073709551616"),
        ("version", "01"),
        ("amount", "0.12345678901234567890123456789"),
        ("amount", "1e3"),
        ("amount", "PRIVATE-CANARY"),
        ("day", "2023-02-29"),
        ("day", "2024-02-29T00:00:00Z"),
        ("clock", "24:00:00"),
        ("clock", "23:59:60"),
        ("clock", "12:34:56+08:00"),
        ("at", "2024-01-01T00:00:00.123456Z"),
        ("at", "2024-12-31T23:59:60Z"),
    ] {
        let error = context
            .decode_entity_json::<Probe>(
                &serde_json::json!({field:value}).to_string(),
                JsonFieldNamingProfile::CamelCase,
            )
            .unwrap_err();
        assert_eq!(error.instance_path, format!("/{field}"));
        assert!(!error.to_string().contains("PRIVATE-CANARY"));
    }
    for payload in [
        r#"{"longValue":9007199254740992}"#,
        r#"{"id":9007199254740992}"#,
        r#"{"count":2147483648}"#,
        r#"{"count":1.1}"#,
        r#"{"count":"1"}"#,
        r#"{"amount":0.1}"#,
        r#"{"weight":1e40}"#,
        r#"{"flag":0}"#,
        r#"{"count":null}"#,
    ] {
        assert!(
            context
                .decode_entity_json::<Probe>(payload, JsonFieldNamingProfile::CamelCase)
                .is_err()
        );
    }
}
#[test]
fn typed_json_serialization_rejects_nonfinite_instead_of_converting_to_null() {
    let context = context();
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut row = native();
        row.ratio = value;
        let error = context
            .encode_entity_json(&row, JsonFieldNamingProfile::CamelCase)
            .unwrap_err();
        assert_eq!(error.code, "WIRE_INVALID_TYPE");
        assert_eq!(error.instance_path, "/ratio");
        assert_eq!(error.location.model_path(), "ratio");
    }
    let mut row = native();
    row.weight = f32::NAN;
    assert!(
        context
            .encode_entity_json(&row, JsonFieldNamingProfile::CamelCase)
            .is_err()
    );
}
#[test]
fn typed_json_forward_reverse_and_filtered_details_preserve_real_fk_identity() {
    let context = context();
    for profile in PROFILES {
        let mut payload = serde_json::Map::new();
        payload.insert(profile.render("id"), "1".into());
        payload.insert(profile.render("version"), "1".into());
        payload.insert(profile.render("display_name"), "Platform".into());
        payload.insert(profile.render("probe_list"),serde_json::json!([{profile.render("id"):"7",profile.render("version"):"2",profile.render("root_record"):"1",profile.render("reference_code"):null}]));
        let root = context
            .decode_entity_json::<Root>(&serde_json::Value::Object(payload).to_string(), profile)
            .unwrap();
        let output = context.encode_entity_json(&root, profile).unwrap();
        let json: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(
            json[profile.render("probe_list")][0][profile.render("root_record")],
            "1"
        );
        let restored = context
            .decode_entity_json::<Root>(&output, profile)
            .unwrap();
        assert_eq!(restored.id, 1);
        assert!(restored.is_field_loaded("probe_list"));
        let forward = serde_json::json!({profile.render("id"):"7",profile.render("version"):"2",profile.render("root_record"):{profile.render("id"):"1",profile.render("version"):"1"}});
        let child = context
            .decode_entity_json::<Probe>(&forward.to_string(), profile)
            .unwrap();
        assert_eq!(child.root_record_id, Some(1));
        let json: serde_json::Value =
            serde_json::from_str(&context.encode_entity_json(&child, profile).unwrap()).unwrap();
        assert_eq!(
            json[profile.render("root_record")][profile.render("id")],
            "1"
        );
        assert!(
            json[profile.render("root_record")]
                .get(profile.render("display_name"))
                .is_none()
        );
    }
}
#[test]
fn typed_json_limits_reserved_state_and_nested_paths_are_structured() {
    let context = context();
    let model = model();
    let limited = model
        .clone()
        .with_limits(Limits {
            max_bytes: 32,
            max_depth: 3,
            max_nodes: 5,
        })
        .unwrap();
    for text in [
        r#"{"referenceCode":"this string is too long for the explicit byte limit"}"#,
        r#"{"raw":[[[[[1]]]]]}"#,
    ] {
        assert_eq!(
            limited
                .deserialize::<Probe>(&context, text, JsonFieldNamingProfile::CamelCase)
                .unwrap_err()
                .code,
            "WIRE_JSON_LIMIT"
        );
    }
    for key in [
        "_comment",
        "_dirty_fields",
        "__load_state",
        "__teaql_runtime_state",
    ] {
        assert_eq!(
            model
                .deserialize::<Probe>(
                    &context,
                    &serde_json::json!({key:"PRIVATE"}).to_string(),
                    JsonFieldNamingProfile::CamelCase
                )
                .unwrap_err()
                .code,
            "WIRE_RUNTIME_STATE"
        );
    }
    let error = model
        .deserialize::<Root>(
            &context,
            r#"{"probeList":[{"amount":"PRIVATE"}]}"#,
            JsonFieldNamingProfile::CamelCase,
        )
        .unwrap_err();
    assert_eq!(error.instance_path, "/probeList/0/amount");
    assert_eq!(error.location.model_path(), "probe_list[0].amount");
    assert!(!error.to_wire().to_string().contains("PRIVATE"));
    assert!(
        model
            .deserialize::<Probe>(
                &context,
                r#"{"raw":{"a/b":1,"a/b":2}}"#,
                JsonFieldNamingProfile::CamelCase
            )
            .unwrap_err()
            .instance_path
            .ends_with("/a~1b")
    );
}
#[test]
fn typed_json_unknown_model_and_metadata_collisions_fail_closed() {
    assert!(Model::new(vec![Definition::new("x", vec![])]).is_err());
    let mut fields = vec![
        Field::new("id", "id", Kind::Id),
        Field::new("version", "version", Kind::Version),
    ];
    fields.push(Field::new("a_b", "first", Kind::Text));
    fields.push(Field::new("a__b", "second", Kind::Text));
    assert!(Model::new(vec![Definition::new("x", fields)]).is_err());
    assert_eq!(
        UserContext::new()
            .encode_entity_json(&native(), JsonFieldNamingProfile::CamelCase)
            .unwrap_err()
            .code,
        "WIRE_MODEL_NOT_INSTALLED"
    );
    assert!(
        model()
            .with_limits(Limits {
                max_bytes: 1,
                max_depth: 0,
                max_nodes: 1
            })
            .is_err()
    );
}

#[test]
fn typed_json_temporal_normalization_and_numeric_endpoints_are_explicit() {
    let context = context();
    let row=context.decode_entity_json::<Probe>(r#"{"at":"2024-02-29T20:00:00.789+08:00","clock":"00:00:00","count":2147483647,"longValue":"9223372036854775807","amount":"3.50"}"#,JsonFieldNamingProfile::CamelCase).unwrap();
    assert_eq!(row.at, Timestamp(1709208000789));
    assert_eq!(row.clock, "00:00:00");
    assert_eq!(row.count, i32::MAX as i64);
    assert_eq!(row.long_value, i64::MAX);
    assert_eq!(row.amount.to_string(), "3.50");
    let value: serde_json::Value = serde_json::from_str(
        &context
            .encode_entity_json(&row, JsonFieldNamingProfile::CamelCase)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["at"], "2024-02-29T12:00:00.789Z");
    assert_eq!(value["amount"], "3.50");
    let mut row = native();
    row.properties.insert("_count".into(), Value::U64(u64::MAX));
    assert_eq!(
        context
            .encode_entity_json(&row, JsonFieldNamingProfile::CamelCase)
            .unwrap_err()
            .code,
        "WIRE_UNTYPED_INTEGER_PRECISION"
    );
    assert!(
        context
            .decode_entity_json::<Probe>(r#"{"raw":{"n":1e20}}"#, JsonFieldNamingProfile::CamelCase)
            .is_err()
    );
}

#[test]
fn typed_json_strict_traversal_does_not_silently_truncate_cycles_or_limits() {
    let context = context();
    let mut row = native();
    row.root_record = Some(Box::new(
        context
            .decode_json_entity::<Root>(&serde_json::json!({"id":1,"version":1}))
            .unwrap(),
    ));
    let limited = model()
        .with_limits(Limits {
            max_bytes: 1024,
            max_depth: 1,
            max_nodes: 100,
        })
        .unwrap();
    assert_eq!(
        limited
            .serialize(&row, JsonFieldNamingProfile::CamelCase)
            .unwrap_err()
            .code,
        "WIRE_JSON_LIMIT"
    );
    let mut traversal = teaql_core::EntityJsonTraversal::for_strict_wire(10, 10);
    assert!(traversal.enter("WireProbe", 1));
    assert!(!traversal.enter("WireProbe", 1));
    assert_eq!(
        traversal.failure(),
        Some(teaql_core::EntityJsonTraversalError::Cycle)
    );
    let mut traversal = teaql_core::EntityJsonTraversal::for_strict_wire(10, 1);
    assert!(traversal.enter("WireProbe", 1));
    assert!(!traversal.enter("WireRoot", 2));
    assert_eq!(
        traversal.failure(),
        Some(teaql_core::EntityJsonTraversalError::Limit)
    );
}

#[test]
fn typed_json_reference_mismatch_empty_lists_nulls_and_omission_are_distinct() {
    let context = context();
    let mut row = native();
    row.root_record = Some(Box::new(
        context
            .decode_json_entity::<Root>(&serde_json::json!({"id":2,"version":1}))
            .unwrap(),
    ));
    let error = context
        .encode_entity_json(&row, JsonFieldNamingProfile::CamelCase)
        .unwrap_err();
    assert_eq!(error.code, "WIRE_REFERENCE_ID_MISMATCH");
    assert_eq!(error.instance_path, "/rootRecord");
    for profile in PROFILES {
        let root=context.decode_entity_json::<Root>(&serde_json::json!({profile.render("id"):"1",profile.render("version"):"1",profile.render("probe_list"):[]}).to_string(),profile).unwrap();
        let output: serde_json::Value =
            serde_json::from_str(&context.encode_entity_json(&root, profile).unwrap()).unwrap();
        assert_eq!(output[profile.render("probe_list")], serde_json::json!([]));
        assert!(output.get(profile.render("display_name")).is_none());
        let row=context.decode_entity_json::<Probe>(&serde_json::json!({profile.render("id"):"7",profile.render("version"):"1",profile.render("root_record"):null}).to_string(),profile).unwrap();
        let output: serde_json::Value =
            serde_json::from_str(&context.encode_entity_json(&row, profile).unwrap()).unwrap();
        assert!(output[profile.render("root_record")].is_null());
        assert!(row.is_field_loaded("root_record"));
    }
}

struct PresentationOnly;
impl teaql_core::TeaqlEntity for PresentationOnly {
    const ENTITY_NAME: &'static str = "WireProbe";
    fn entity_descriptor() -> teaql_core::EntityDescriptor {
        teaql_core::EntityDescriptor::new("WireProbe")
    }
}
impl Entity for PresentationOnly {
    fn from_compact_row(_: teaql_core::CompactRow) -> Result<Self, teaql_core::EntityError> {
        Ok(Self)
    }
    fn into_values(self) -> teaql_core::MutationValues {
        BTreeMap::new().into()
    }
    fn borrowed_json(&self, _: &mut teaql_core::EntityJsonTraversal) -> Option<serde_json::Value> {
        panic!("a strict codec must never use the lossy presentation fallback")
    }
}
#[test]
fn typed_json_unsupported_carrier_does_not_fall_back_to_presentation_json() {
    assert_eq!(
        model()
            .serialize(&PresentationOnly, JsonFieldNamingProfile::CamelCase)
            .unwrap_err()
            .code,
        "WIRE_ENTITY_CARRIER_REQUIRED"
    );
}

#[test]
fn typed_json_rejects_invalid_json_and_does_not_mutate_shared_model_limits() {
    let context = context();
    let model = model();
    for payload in [
        r#"{"referenceCode":"\uD800"}"#,
        r#"{"flag":true} {"flag":false}"#,
        r#"{"ratio":NaN}"#,
    ] {
        assert_eq!(
            model
                .deserialize::<Probe>(&context, payload, JsonFieldNamingProfile::CamelCase)
                .unwrap_err()
                .code,
            "WIRE_INVALID_JSON"
        );
    }
    let tiny = model
        .clone()
        .with_limits(Limits {
            max_bytes: 2,
            max_depth: 2,
            max_nodes: 2,
        })
        .unwrap();
    assert!(
        tiny.serialize(&native(), JsonFieldNamingProfile::CamelCase)
            .is_err()
    );
    assert!(
        model
            .serialize(&native(), JsonFieldNamingProfile::CamelCase)
            .is_ok()
    );
}

#[test]
fn typed_json_error_has_expected_type_and_matching_safe_human_machine_locations() {
    let context = context();
    let error = context
        .decode_entity_json::<Probe>(
            r#"{"longValue":"PRIVATE-CANARY"}"#,
            JsonFieldNamingProfile::CamelCase,
        )
        .unwrap_err();
    assert_eq!(error.location.model_path(), "long_value");
    assert_eq!(error.instance_path, "/longValue");
    assert_eq!(error.expected_kind, Some("signed 64-bit integer string"));
    let wire = serde_json::to_value(&error).unwrap();
    assert_eq!(wire, error.to_wire());
    assert_eq!(wire["expectedKind"], "signed 64-bit integer string");
    assert!(error.to_string().contains("/longValue"));
    assert!(error.to_string().contains("signed 64-bit"));
    assert!(!error.to_string().contains("PRIVATE-CANARY"));
    assert!(!wire.to_string().contains("PRIVATE-CANARY"));
    let error = context
        .decode_entity_json::<Probe>(r#"{"rootRecord":null}"#, JsonFieldNamingProfile::CamelCase)
        .unwrap_err();
    assert_eq!(error.code, "WIRE_ENTITY_ID_REQUIRED");
    assert_eq!(error.instance_path, "/id");
}
