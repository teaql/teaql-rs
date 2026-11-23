use chrono::NaiveTime;
use teaql_core::{CompactRow, Entity, Record, Value};

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "LocalTimeProbe")]
struct LocalTimeProbe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    clock: NaiveTime,
    optional_clock: Option<NaiveTime>,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}

#[test]
fn native_time_preserves_nanoseconds_without_timezone_or_epoch_conversion() {
    for clock in [
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
        NaiveTime::from_hms_nano_opt(23, 59, 59, 987_654_321).unwrap(),
    ] {
        let value = Value::from(clock);
        assert_eq!(value, Value::Text(clock.to_string()));
        assert_eq!(value.try_time(), Some(clock));
        assert_eq!(
            value.to_json_value(),
            serde_json::Value::String(clock.to_string())
        );
    }
    for invalid in ["not-a-time", "24:00:00", "09:30:00Z", "2026-11-04 09:30:00"] {
        assert_eq!(Value::from(invalid).try_time(), None);
    }
    assert_eq!(Value::I64(9_300_000).try_time(), None);
    assert_eq!(Value::Null.try_time(), None);
}

#[test]
fn derived_native_time_hydrates_and_preserves_null_and_absent_state() {
    let clock = NaiveTime::from_hms_nano_opt(9, 30, 0, 123_456_789).unwrap();
    let row = LocalTimeProbe::from_compact_row(CompactRow::from_map(Record::from([
        ("id".into(), Value::U64(1)),
        ("version".into(), Value::I64(1)),
        ("clock".into(), Value::Text(clock.to_string())),
        ("optional_clock".into(), Value::Null),
    ])))
    .unwrap();
    assert_eq!(row.clock, clock);
    assert_eq!(row.optional_clock, None);
    assert!(row.is_field_loaded("optional_clock"));
    let values = row.into_values();
    assert_eq!(values.get("clock"), Some(&Value::Text(clock.to_string())));
    assert_eq!(values.get("optional_clock"), Some(&Value::Null));
    let sparse = LocalTimeProbe::from_compact_row(CompactRow::from_map(Record::from([
        ("id".into(), Value::U64(2)),
        ("version".into(), Value::I64(1)),
    ])))
    .unwrap();
    assert!(!sparse.is_field_loaded("clock"));
    assert!(!sparse.is_field_loaded("optional_clock"));
}

#[test]
fn invalid_native_time_is_a_field_error_without_private_payload() {
    for value in [Value::from("PRIVATE-TIME-CANARY"), Value::I64(930)] {
        let result = LocalTimeProbe::from_compact_row(CompactRow::from_map(Record::from([
            ("id".into(), Value::U64(1)),
            ("version".into(), Value::I64(1)),
            ("clock".into(), value),
        ])));
        let error = result
            .expect_err("invalid typed time must not become midnight or JSON")
            .to_string();
        assert!(error.contains("clock"));
        assert!(!error.contains("PRIVATE-TIME-CANARY"));
    }
}
