use inheritance_matching_service_core::School;
use inheritance_matching_service_core::teaql_core::{CompactRow, Entity, SmartList, Value};
use std::collections::BTreeMap;

#[test]
fn pending_recovery_survives_an_unloaded_list_without_a_dummy_scalar_edit() {
    let mut child = School::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(41)),
        ("version".into(), Value::I64(-4)),
    ])))
    .unwrap();
    assert!(!child.is_new());
    assert!(child.dirty_fields().is_none());
    child.mark_to_recover();
    let mut list = SmartList::new(vec![child]);
    list.is_loaded = false;
    let Some(Value::List(values)) = list.into_value() else {
        panic!("recovery intent was discarded as an unchanged list")
    };
    assert_eq!(values.len(), 1);
    let Value::Object(values) = &values[0] else {
        panic!("missing child mutation payload")
    };
    assert_eq!(values.get("_is_recovered"), Some(&Value::Bool(true)));
    assert_eq!(values.get("version"), Some(&Value::I64(-4)));
}
