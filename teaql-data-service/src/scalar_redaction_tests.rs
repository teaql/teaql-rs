//! Differential privacy regression against the pre-optimization implementation.
use super::*;
use std::collections::BTreeMap;
use teaql_core::{DataType, Value};

fn fixtures() -> Vec<Value> {
    vec![
        Value::Null,
        Value::TypedNull(DataType::U64),
        Value::Text(String::new()),
        Value::from("private"),
        Value::Bool(false),
        Value::I64(-7),
        Value::U64(u64::MAX),
        Value::F64(-0.0),
        Value::F64(f64::INFINITY),
        Value::F64(f64::NAN),
        Value::Decimal("12.340".parse().unwrap()),
        Value::Date("2026-10-29".parse().unwrap()),
        Value::Timestamp(teaql_core::time::Timestamp(-123)),
        Value::List(vec![Value::from("ordinary"), Value::U64(7), Value::Null]),
        Value::Object(BTreeMap::from([
            ("password".into(), Value::from("secret")),
            (
                "ordinary".into(),
                Value::List(vec![Value::U64(7), Value::from("nested")]),
            ),
        ])),
        Value::Json(serde_json::json!({"ordinary":[1,"",null],"password":{"value":"secret"}})),
    ]
}

#[test]
fn scalar_fast_path_matches_previous_privacy_rules_and_order() {
    for generated_sql in [false, true] {
        for sql in ["SELECT ordinary", "SELECT password"] {
            for policy in [
                SqlParameterLogPolicy::Plain,
                SqlParameterLogPolicy::Masked,
                SqlParameterLogPolicy::Credential,
                SqlParameterLogPolicy::Unknown,
            ] {
                for invalid in [false, true] {
                    for value in fixtures() {
                        let source = SqlLogContext {
                            generated_sql,
                            parameter_policies: if invalid { vec![] } else { vec![policy] },
                            intent_redactions: SqlIntentRedactions(vec![(
                                "inherited".into(),
                                true,
                            )]),
                            ..Default::default()
                        };
                        let params = [value];
                        assert_eq!(
                            SqlIntentRedactions::from_bindings(&source, &params, sql).0,
                            previous_from_bindings(&source, &params, sql).0
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn target_capture_preserves_dedup_forced_flags_and_compound_fallback() {
    let mut actual = SqlIntentRedactions(vec![("7".into(), false), ("private".into(), true)]);
    let mut expected = actual.clone();
    for _ in 0..2 {
        for value in fixtures().into_iter().chain([Value::U64(7)]) {
            actual.capture_target_id(&value);
            previous_capture(&mut expected, &value);
            assert_eq!(actual.0, expected.0);
        }
    }
    for allow_plaintext in [false, true] {
        let mut a = vec![];
        let mut e = vec![];
        actual.extend_secrets(allow_plaintext, &mut a);
        expected.extend_secrets(allow_plaintext, &mut e);
        assert_eq!(a, e);
    }
    assert!(!format!("{actual:?}").contains("secret"));
}

// Frozen oracle: update only if the privacy contract intentionally changes.
fn previous_capture(output: &mut SqlIntentRedactions, id: &teaql_core::Value) {
    let source = SqlLogContext {
        generated_sql: true,
        parameter_policies: vec![SqlParameterLogPolicy::Masked],
        ..Default::default()
    };
    for (text, _) in previous_from_bindings(&source, std::slice::from_ref(id), "").0 {
        if !output
            .0
            .iter()
            .any(|(existing, forced)| existing == &text && *forced)
        {
            output.0.push((text, true));
        }
    }
}

fn previous_from_bindings(
    source: &SqlLogContext,
    params: &[teaql_core::Value],
    sql: &str,
) -> SqlIntentRedactions {
    let invalid = source.parameter_policies.len() != params.len();
    let statement_credential = is_credential_log_name(sql) && (!source.generated_sql || invalid);
    let mut output = source.intent_redactions.clone();
    for (index, value) in params.iter().enumerate() {
        let policy = if invalid {
            SqlParameterLogPolicy::Unknown
        } else {
            source.parameter_policies[index]
        };
        if !statement_credential
            && policy == SqlParameterLogPolicy::Plain
            && !matches!(
                value,
                teaql_core::Value::Object(_)
                    | teaql_core::Value::List(_)
                    | teaql_core::Value::Json(_)
            )
        {
            continue;
        }
        let forced = statement_credential
            || matches!(
                policy,
                SqlParameterLogPolicy::Unknown | SqlParameterLogPolicy::Credential
            );
        if let teaql_core::Value::Text(text) = value {
            if !text.is_empty() {
                output.0.push((text.clone(), forced));
            }
            continue;
        }
        // Inspect nested credential keys even when the outer field is plain.
        let mut pending = vec![value];
        let mut strings = Vec::new();
        let mut credential = statement_credential || policy == SqlParameterLogPolicy::Credential;
        while let Some(item) = pending.pop() {
            use teaql_core::Value;
            let text = match item {
                Value::Null | Value::TypedNull(_) => continue,
                Value::Object(object) => {
                    credential |= object.keys().any(|name| is_credential_log_name(name));
                    pending.extend(object.values());
                    continue;
                }
                Value::List(array) => {
                    pending.extend(array);
                    continue;
                }
                Value::Json(json) => {
                    let mut json_pending = vec![json];
                    while let Some(item) = json_pending.pop() {
                        if let Some(object) = item.as_object() {
                            credential |= object.keys().any(|name| is_credential_log_name(name));
                            json_pending.extend(object.values());
                        } else if let Some(array) = item.as_array() {
                            json_pending.extend(array);
                        } else if !item.is_null() {
                            let text = item
                                .as_str()
                                .map(str::to_owned)
                                .unwrap_or_else(|| item.to_string());
                            if !text.is_empty() {
                                strings.push(text);
                            }
                        }
                    }
                    continue;
                }
                Value::Text(text) => text.clone(),
                Value::Bool(value) => value.to_string(),
                Value::I64(value) => value.to_string(),
                Value::U64(value) => value.to_string(),
                Value::F64(value) => value.to_string(),
                Value::Decimal(value) => value.to_string(),
                Value::Date(value) => value.to_string(),
                Value::Timestamp(value) => value.as_millis().to_string(),
            };
            if !text.is_empty() {
                strings.push(text);
            }
        }
        let forced = credential || policy == SqlParameterLogPolicy::Unknown;
        if forced || policy == SqlParameterLogPolicy::Masked {
            output
                .0
                .extend(strings.into_iter().map(|text| (text, forced)));
        }
    }
    output
}
