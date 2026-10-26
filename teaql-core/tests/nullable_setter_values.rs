use teaql_core::{Value, time::Timestamp};
#[test]
fn optional_generated_setter_inputs_distinguish_unknown_from_zero() {
    fn setter_input(value: impl Into<Value>) -> Value {
        value.into()
    }
    assert_eq!(setter_input(None::<i64>), Value::Null);
    assert_eq!(setter_input(Some(0_i64)), Value::I64(0));
    assert_eq!(setter_input(Some(7_i64)), Value::I64(7));
    assert_eq!(setter_input(None::<String>), Value::Null);
    assert_eq!(
        setter_input(Some("unknown".to_owned())),
        Value::Text("unknown".into())
    );
    assert_eq!(setter_input(None::<serde_json::Value>), Value::Null);
    assert_eq!(
        setter_input(Some(serde_json::json!({"status":"failed","counts":null}))),
        Value::Json(serde_json::json!({"status":"failed","counts":null}))
    );
    assert_eq!(
        setter_input(Some(Timestamp(123))),
        Value::Timestamp(Timestamp(123))
    );
}
