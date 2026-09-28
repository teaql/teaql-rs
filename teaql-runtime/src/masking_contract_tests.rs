//! Contract tests intentionally independent of the current redaction implementation.
use super::sql_entry;
use crate::log_formatter::{HumanReaderFormatter, LogFormatter, SqlLogEntry, SqlLogOperation};
use std::time::{Duration, SystemTime};
use teaql_core::Value;

#[test]
fn mask_golden() {
    for line in include_str!("../../test-vectors/masking-v1.tsv")
        .lines()
        .skip(1)
    {
        let values: Vec<_> = line.split('\t').collect();
        assert_eq!(
            crate::event::mask_audit_value(values[1]),
            values[2],
            "{}",
            values[0]
        );
        check_audit(values[1], values[2]);
        let mut source = entry(values[1]);
        source.log_context.parameter_policies =
            vec![teaql_data_service::SqlParameterLogPolicy::Masked];
        let projected = sql_entry(&source, false);
        assert_eq!(
            projected.params,
            vec![Value::Text(values[2].into())],
            "SQL field policy: {}",
            values[0]
        );
        assert!(projected.debug_sql.contains(&format!(
            "name = '{}' /* masked */",
            values[2].replace('\'', "''")
        )));
        let event = crate::RawAuditEvent::created(
            "Customer",
            teaql_core::Record::from([("name".into(), Value::Text(values[1].into()))]),
        );
        let safe = event.build_safe_event(&["name".into()], None);
        assert_eq!(
            safe.fields[0].value.as_deref(),
            Some(values[2]),
            "actual event: {}",
            values[0]
        );
        assert_eq!(
            event.values.get("name"),
            Some(&Value::Text(values[1].into()))
        );
    }
}

fn entry(value: &str) -> SqlLogEntry {
    let sql = format!("UPDATE customer SET name = '{}'", value.replace('\'', "''"));
    SqlLogEntry {
        log_context: teaql_data_service::SqlLogContext {
            database_kind: Some("sqlite".into()),
            ..Default::default()
        },
        operation: SqlLogOperation::Update,
        comment: Some("what: edit customer".into()),
        purpose: Some("why: verify mask contract".into()),
        audit_reason: Some("verify mask contract".into()),
        trace_path: vec![],
        sql: "UPDATE customer SET name = ?".into(),
        params: vec![Value::Text(value.into())],
        debug_sql: sql.clone(),
        pretty_sql: sql,
        started_at: SystemTime::now(),
        ended_at: SystemTime::now(),
        elapsed: Duration::from_micros(1),
        result_count: None,
        result_type: None,
        affected_rows: Some(1),
        result_summary: "1 row affected".into(),
    }
}

#[test]
fn target_id_scrubs_sql_free_text_without_changing_plain_binding_or_row_count() {
    use teaql_data_service::SqlParameterLogPolicy as Policy;
    let mut source = entry("unused");
    source.log_context.generated_sql = true;
    source.log_context.parameter_policies = vec![Policy::Plain];
    source
        .log_context
        .intent_redactions
        .capture_target_id(&Value::U64(1));
    source.sql = "UPDATE customer SET version = version + 1 WHERE id = ?".into();
    source.params = vec![Value::U64(1)];
    source.comment = Some("what: update customer 1".into());
    source.audit_reason = Some("what: update customer 1".into());
    source.trace_path.push(teaql_core::TraceNode::typed(
        teaql_core::TraceKind::AuditReason,
        "Customer",
        Some(1),
        "what: update customer 1",
    ));
    source.result_summary = "1 rows affected".into();
    for allow in [false, true] {
        let safe = sql_entry(&source, allow);
        assert_eq!(
            safe.audit_reason.as_deref(),
            Some("what: update customer [REDACTED]")
        );
        assert_eq!(
            safe.comment.as_deref(),
            Some("what: update customer [REDACTED]")
        );
        assert_eq!(
            safe.trace_path[0].comment,
            "what: update customer [REDACTED]"
        );
        assert_eq!(safe.result_summary, "1 rows affected");
        assert_eq!(safe.params, vec![Value::U64(1)]);
        assert!(safe.log_context.intent_redactions.is_empty());
    }
    assert_eq!(
        source.audit_reason.as_deref(),
        Some("what: update customer 1")
    );
}

#[test]
fn retained_debug_readback_downgrades_after_clone() {
    use teaql_data_service::{SqlIntentRedactions, SqlLogContext, SqlParameterLogPolicy as Policy};
    let write = SqlLogContext {
        generated_sql: true,
        parameter_policies: vec![Policy::Masked],
        ..Default::default()
    };
    let mut source = entry("unused");
    source.sql = "SELECT name FROM customer WHERE id = ? LIMIT 10000".into();
    source.params = vec![Value::U64(1)];
    source.log_context.generated_sql = true;
    source.log_context.parameter_policies = vec![Policy::Plain];
    source.log_context.intent_redactions =
        SqlIntentRedactions::from_bindings(&write, &[Value::from("Riverside")], "UPDATE customer");
    source.comment = Some("what: read Riverside authoritative snapshot".into());
    let debug = sql_entry(&source, true).clone();
    assert!(debug.log_context.intent_redactions.is_empty());
    let safe = sql_entry(&debug, false);
    assert_eq!(
        safe.comment.as_deref(),
        Some("what: read [REDACTED] authoritative snapshot")
    );
    assert!(safe.debug_sql.contains("WHERE id = 1 LIMIT 10000"));
    assert!(!format!("{safe:?}").contains("Riverside"));
    let debug_again = sql_entry(&debug, true);
    assert_eq!(sql_entry(&debug_again, false).comment, safe.comment);
}

#[test]
fn modified_or_reconstructed_debug_hides_unknown_inherited_intent() {
    for reconstructed in [false, true] {
        let mut source = entry("Riverside");
        source.log_context.parameter_policies =
            vec![teaql_data_service::SqlParameterLogPolicy::Masked];
        let mut debug = sql_entry(&source, true);
        if reconstructed {
            debug.log_context = teaql_data_service::SqlLogContext {
                database_kind: Some("sqlite".into()),
                parameter_policies: vec![teaql_data_service::SqlParameterLogPolicy::Masked],
                ..Default::default()
            }; // Only the SQL header identifies this reconstructed debug record.
        }
        debug.comment = Some("INHERITED-CANARY".into());
        debug.trace_path.push(teaql_core::TraceNode::new(
            "INHERITED-CANARY",
            Some(1),
            "INHERITED-CANARY",
        ));
        debug.result_summary = "INHERITED-CANARY".into();
        debug.result_type = Some("INHERITED-CANARY".into());
        let safe = sql_entry(&debug, false);
        assert!(!format!("{safe:?}").contains("CANARY"));
        assert!(safe.debug_sql.contains("'Ri*****de' /* masked */"));
    }
}

#[test]
fn safe_projection_never_upgrades_its_label() {
    let source = entry("Riverside");
    let safe = sql_entry(&source, false);
    let again = sql_entry(&safe, true);
    assert_eq!(again.log_context.mode.as_deref(), Some("safe"));
    assert_eq!(again.debug_sql, safe.debug_sql);
}

#[test]
fn cloned_debug_alternatives_are_independent_and_thread_safe() {
    let mut source = entry("Riverside");
    source.log_context.parameter_policies = vec![teaql_data_service::SqlParameterLogPolicy::Masked];
    source.comment = Some("find Riverside".into());
    let debug = sql_entry(&source, true);
    let mut safe = sql_entry(&debug, false);
    safe.comment = Some("POISON".into());
    safe.params[0] = Value::from("POISON");
    std::thread::scope(|scope| {
        for _ in 0..16 {
            let debug = debug.clone();
            scope.spawn(move || {
                let safe = sql_entry(&debug, false);
                assert_eq!(safe.params, vec![Value::from("Ri*****de")]);
                assert_eq!(safe.comment.as_deref(), Some("find [REDACTED]"));
                assert!(!format!("{safe:?}").contains("Riverside"));
            });
        }
    });
    assert!(!format!("{:?}", debug.log_context).contains("Riverside"));
}

#[test]
fn malformed_mask_flags_fail_closed_in_both_modes() {
    for allow in [false, true] {
        let mut source = entry("Riverside");
        source.log_context.parameter_policies =
            vec![teaql_data_service::SqlParameterLogPolicy::Plain];
        source.log_context.masked_parameters = vec![false, false];
        let result = sql_entry(&source, allow);
        assert_eq!(
            result.log_context.omission_reason.as_deref(),
            Some("parameter mask count mismatch")
        );
        assert!(!format!("{result:?}").contains("Riverside"));
    }
}

#[test]
fn trace_names_and_result_text_follow_sensitive_bindings() {
    let mut source = entry("Riverside");
    source.trace_path.push(teaql_core::TraceNode::new(
        "Riverside",
        Some(1),
        "Riverside",
    ));
    source.result_summary = "rows for Riverside".into();
    source.result_type = Some("Riverside".into());
    let safe = sql_entry(&source, false);
    assert!(!format!("{safe:?}").contains("Riverside"));
}

#[test]
fn omission_reason_is_not_an_untrusted_payload_channel() {
    let mut source = entry("Riverside");
    source.log_context.omission_reason = Some("INHERITED-CANARY".into());
    assert!(!format!("{:?}", sql_entry(&source, false)).contains("CANARY"));
}

#[test]
fn inherited_intent_has_safe_and_debug_views_without_retaining_secrets() {
    use teaql_data_service::{SqlIntentRedactions, SqlLogContext, SqlParameterLogPolicy as Policy};
    let write = SqlLogContext {
        generated_sql: true,
        parameter_policies: vec![
            Policy::Masked,
            Policy::Plain,
            Policy::Credential,
            Policy::Unknown,
        ],
        ..Default::default()
    };
    let params = vec![
        Value::from("Riverside"),
        Value::from("PublicAddress"),
        Value::from("PASSWORD-CANARY"),
        Value::from("UNKNOWN-CANARY"),
    ];
    let mut source = entry("unused");
    source.sql = "SELECT name FROM customer WHERE id = ?".into();
    source.params = vec![Value::U64(1)];
    source.log_context.generated_sql = true;
    source.log_context.parameter_policies = vec![Policy::Plain];
    source.log_context.intent_redactions =
        SqlIntentRedactions::from_bindings(&write, &params, "UPDATE customer");
    let intent = "Riverside PublicAddress PASSWORD-CANARY UNKNOWN-CANARY";
    source.comment = Some(intent.into());
    source.purpose = Some(intent.into());
    source.audit_reason = Some(intent.into());
    source.trace_path.push(teaql_core::TraceNode::typed(
        teaql_core::TraceKind::AuditReason,
        "Customer",
        Some(1),
        intent,
    ));
    assert!(!format!("{:?}", source.log_context).contains("CANARY"));
    for allow in [false, true] {
        let result = sql_entry(&source, allow);
        assert!(result.log_context.intent_redactions.is_empty());
        assert_eq!(result.params, vec![Value::U64(1)]);
        assert_eq!(
            result.comment.as_ref().unwrap().contains("Riverside"),
            allow
        );
        assert_eq!(result.trace_path[0].comment.contains("Riverside"), allow);
        assert!(
            result
                .audit_reason
                .as_ref()
                .unwrap()
                .contains("PublicAddress")
        );
        assert!(!format!("{result:?}").contains("PASSWORD-CANARY"));
        assert!(!format!("{result:?}").contains("UNKNOWN-CANARY"));
        if allow {
            assert!(
                result
                    .debug_sql
                    .contains("DEBUG PLAINTEXT; EXPLICIT OPT-IN")
            );
        }
    }
    let safe = sql_entry(&source, false);
    assert!(!format!("{:?}", sql_entry(&safe, true)).contains("Riverside"));
    assert_eq!(source.comment.as_deref(), Some(intent));
    assert_eq!(params[0], Value::from("Riverside"));
}

#[test]
fn inherited_intent_protects_nested_credentials_and_invalid_policies_under_debug() {
    use teaql_data_service::{SqlIntentRedactions, SqlLogContext, SqlParameterLogPolicy as Policy};
    let source = SqlLogContext {
        generated_sql: true,
        parameter_policies: vec![Policy::Plain],
        ..Default::default()
    };
    let nested = Value::Json(
        serde_json::json!({"payload": [{"access_token": "NESTED-CANARY"}], "note": "SIBLING-CANARY"}),
    );
    let protection = SqlIntentRedactions::from_bindings(&source, &[nested], "UPDATE customer");
    let mut secrets = Vec::new();
    protection.extend_secrets(true, &mut secrets);
    assert!(secrets.contains(&"NESTED-CANARY".into()));
    assert!(secrets.contains(&"SIBLING-CANARY".into()));
    let invalid = SqlIntentRedactions::from_bindings(
        &source,
        &[Value::from("FIRST-CANARY"), Value::from("SECOND-CANARY")],
        "UPDATE customer",
    );
    secrets.clear();
    invalid.extend_secrets(true, &mut secrets);
    assert_eq!(secrets.len(), 2);
    assert!(secrets.contains(&"FIRST-CANARY".into()));
    assert!(secrets.contains(&"SECOND-CANARY".into()));
}

#[test]
fn inherited_intent_uses_longest_first_and_does_not_mask_plain_values() {
    use teaql_data_service::{SqlIntentRedactions, SqlLogContext, SqlParameterLogPolicy as Policy};
    let write = SqlLogContext {
        generated_sql: true,
        parameter_policies: vec![Policy::Masked],
        ..Default::default()
    };
    let mut source = entry("River");
    source.log_context.parameter_policies = vec![Policy::Masked];
    source.log_context.intent_redactions =
        SqlIntentRedactions::from_bindings(&write, &[Value::from("Riverside")], "UPDATE customer");
    source.comment = Some("Riverside River".into());
    assert_eq!(
        sql_entry(&source, false).comment.as_deref(),
        Some("[REDACTED] [REDACTED]")
    );
    let plain = SqlLogContext {
        generated_sql: true,
        parameter_policies: vec![Policy::Plain],
        ..Default::default()
    };
    assert!(
        SqlIntentRedactions::from_bindings(&plain, &[Value::from("Riverside")], "UPDATE customer")
            .is_empty()
    );
}

#[test]
fn inherited_numeric_intent_matches_typed_value_text_not_json_normalization() {
    use teaql_data_service::{SqlIntentRedactions, SqlLogContext, SqlParameterLogPolicy as Policy};
    let write = SqlLogContext {
        generated_sql: true,
        parameter_policies: vec![Policy::Masked],
        ..Default::default()
    };
    for value in [
        Value::F64(12345.0),
        Value::List(vec![Value::F64(12345.0)]),
        Value::F64(f64::NAN),
    ] {
        let mut expected = Vec::new();
        super::collect_strings(&value, &mut expected);
        let protection = SqlIntentRedactions::from_bindings(&write, &[value], "UPDATE customer");
        let mut actual = Vec::new();
        protection.extend_secrets(false, &mut actual);
        assert_eq!(actual, expected);
    }
}

fn check_expanded_sql(raw: &str, masked: &str) {
    let source = entry(raw);
    let safe = sql_entry(&source, false);
    assert_eq!(source.params, vec![Value::Text(raw.into())]);
    // No field policy is attached: unknown provenance needs full masking.
    let expected = "name = '";
    assert!(
        safe.debug_sql.contains(expected),
        "mask {raw:?}: expected {expected}, got {}",
        safe.debug_sql
    );
    let log = HumanReaderFormatter.format_sql_log(&[], &safe);
    assert!(
        log.contains(expected),
        "formatted log must retain expanded SQL"
    );
    assert!(log.to_lowercase().contains("masked"));
    assert!(!safe.debug_sql.contains("name = ?"));
    assert!(!log.contains("[REDACTED SQL"));
    if !raw.is_empty() {
        assert!(!log.contains(&format!("'{}'", raw.replace('\'', "''"))));
    }
    if raw.len() >= 8 && !masked.starts_with('*') {
        assert!(!log.contains(&masked.replace('\'', "''")));
    }
}

#[test]
fn mask_contract_debug_provenance() {
    for _ in 0..2 {
        let mut source = entry("Riverside");
        source.log_context.parameter_policies =
            vec![teaql_data_service::SqlParameterLogPolicy::Masked];
        let safe = sql_entry(&source, true);
        let log = HumanReaderFormatter.format_sql_log(&[], &safe);
        assert!(log.contains("'Riverside'"));
        assert!(log.to_uppercase().contains("DEBUG"));
        assert!(
            log.to_uppercase().contains("PLAINTEXT"),
            "every plaintext record must identify debug permission: {log}"
        );
    }
}

fn check_audit(raw: &str, masked: &str) {
    let safe = crate::event::build_safe_audit_field("name", Some(raw), &["name".into()], None);
    assert!(safe.masked);
    assert_eq!(
        safe.value.as_deref(),
        Some(masked),
        "legacy mask must remain in the real audit path"
    );
}

macro_rules! mask_case {
    ($module:ident, $raw:expr, $masked:expr) => {
        mod $module {
            #[test]
            fn mask_contract_expanded_sql() {
                super::check_expanded_sql($raw, $masked);
            }
            #[test]
            fn mask_contract_audit_algorithm() {
                super::check_audit($raw, $masked);
            }
        }
    };
}
mask_case!(empty, "", "");
mask_case!(short, "Ada", "***");
mask_case!(digits, "12345678", "********");
mask_case!(boundary, "ABCDEFGH", "AB****GH");
mask_case!(long, "Riverside", "Ri*****de");
mask_case!(quote, "O'Reilly", "O'****ly");

#[test]
fn mixed_policies_preserve_sql_and_execution_values() {
    use teaql_data_service::SqlParameterLogPolicy::{Credential, Masked, Plain, Unknown};
    let mut source = entry("Riverside");
    source.sql =
        "UPDATE customer SET name = ?, status = ?, password = ?, note = ? WHERE id = ?".into();
    source.params = vec![
        "Riverside".into(),
        "ACTIVE".into(),
        "PASSWORD-CANARY".into(),
        "UNKNOWN-CANARY".into(),
        Value::I64(17),
    ];
    source.debug_sql = "UNTRUSTED-LEGACY-DEBUG-CANARY".into();
    source.log_context.generated_sql = true;
    source.log_context.parameter_policies = vec![Masked, Plain, Credential, Unknown, Plain];
    for allow in [false, true] {
        let safe = sql_entry(&source, allow);
        assert!(safe.debug_sql.contains(if allow {
            "name = 'Riverside'"
        } else {
            "name = 'Ri*****de' /* masked */"
        }));
        assert!(safe.debug_sql.contains("status = 'ACTIVE'"));
        assert!(safe.debug_sql.contains("WHERE id = 17"));
        assert!(
            safe.debug_sql
                .contains("password = '[REDACTED]' /* masked */")
        );
        assert!(safe.debug_sql.contains("note = '[REDACTED]' /* masked */"));
        assert!(safe.debug_sql.contains("NOT REPLAYABLE"));
        let all = format!("{safe:?}");
        for canary in [
            "PASSWORD-CANARY",
            "UNKNOWN-CANARY",
            "UNTRUSTED-LEGACY-DEBUG-CANARY",
        ] {
            assert!(!all.contains(canary));
        }
    }
    assert_eq!(source.params[2], Value::Text("PASSWORD-CANARY".into()));
}

#[test]
fn safe_projection_is_idempotent_and_cannot_be_unmasked_later() {
    let mut source = entry("Riverside");
    source.log_context.parameter_policies = vec![teaql_data_service::SqlParameterLogPolicy::Masked];
    let safe = sql_entry(&source, false);
    assert_eq!(sql_entry(&safe, false), safe);
    let debug_after_safe = sql_entry(&safe, true);
    assert!(!debug_after_safe.debug_sql.contains("Riverside"));
    assert!(debug_after_safe.debug_sql.contains("Ri*****de"));
    assert!(debug_after_safe.debug_sql.contains("NOT REPLAYABLE"));
}

#[test]
fn unknown_provenance_is_hidden_even_with_explicit_debug_permission() {
    let source = entry("UNKNOWN-CANARY");
    for allow in [false, true] {
        let safe = sql_entry(&source, allow);
        assert!(!format!("{safe:?}").contains("UNKNOWN-CANARY"));
        assert!(safe.debug_sql.contains("name = '[REDACTED]' /* masked */"));
    }
}

#[test]
fn binding_or_policy_errors_omit_sql_without_echoing_values() {
    for (sql, policy_count) in [
        ("UPDATE customer SET name = ?, code = ?", 1),
        ("UPDATE customer SET name = ?", 2),
    ] {
        let mut source = entry("PRIVATE-CANARY");
        source.sql = sql.into();
        source.log_context.parameter_policies =
            vec![teaql_data_service::SqlParameterLogPolicy::Plain; policy_count];
        let safe = sql_entry(&source, false);
        assert!(safe.log_context.omission_reason.is_some());
        assert!(!format!("{safe:?}").contains("PRIVATE-CANARY"));
        assert!(safe.debug_sql.contains("NOT REPLAYABLE"));
        assert_eq!(sql_entry(&safe, false), safe);
    }
}

#[test]
fn untrusted_literal_sql_never_falls_back_to_raw_debug_sql() {
    for sql in [
        "SELECT 'LITERAL-CANARY'",
        "SELECT name /* LITERAL-CANARY */ FROM customer",
        "SELECT 12345678",
    ] {
        let mut source = entry("unused");
        source.sql = sql.into();
        source.params.clear();
        for allow in [false, true] {
            let safe = sql_entry(&source, allow);
            assert_eq!(
                safe.log_context.omission_reason.as_deref(),
                Some("untrusted SQL literals or comments")
            );
            assert!(!format!("{safe:?}").contains("LITERAL-CANARY"));
            assert!(!safe.debug_sql.contains("12345678"));
        }
    }
}

#[test]
fn annotations_are_scrubbed_without_changing_runtime_counts() {
    let mut source = entry("1");
    source.comment = Some("edit customer 1".into());
    let safe = sql_entry(&source, false);
    assert_eq!(safe.comment.as_deref(), Some("edit customer [REDACTED]"));
    assert_eq!(safe.affected_rows, Some(1));
    assert_eq!(safe.result_summary, "1 row affected");
}

#[test]
fn nested_credentials_override_plain_field_policy() {
    let mut source = entry("unused");
    source.params = vec![Value::Json(
        serde_json::json!({"nested":{"api_key":"NESTED-CANARY"}}),
    )];
    source.log_context.generated_sql = true;
    source.log_context.parameter_policies = vec![teaql_data_service::SqlParameterLogPolicy::Plain];
    for allow in [false, true] {
        assert!(!format!("{:?}", sql_entry(&source, allow)).contains("NESTED-CANARY"));
    }
}

#[test]
fn unknown_dialect_is_reported_instead_of_guessed() {
    let mut source = entry("PRIVATE-CANARY");
    source.log_context.database_kind = None;
    let safe = sql_entry(&source, false);
    assert_eq!(
        safe.log_context.omission_reason.as_deref(),
        Some("unknown database dialect")
    );
    assert!(!format!("{safe:?}").contains("PRIVATE-CANARY"));
}

#[test]
fn masked_postgres_array_preserves_shape_and_masks_each_element() {
    let mut source = entry("unused");
    source.sql = "SELECT id FROM customer WHERE name = ANY($1)".into();
    source.log_context.database_kind = Some("postgresql".into());
    source.log_context.generated_sql = true;
    source.log_context.parameter_policies = vec![teaql_data_service::SqlParameterLogPolicy::Masked];
    source.params = vec![Value::List(vec![
        "Riverside".into(),
        "Ada".into(),
        Value::Null,
    ])];
    let safe = sql_entry(&source, false);
    assert!(
        safe.debug_sql
            .contains("ARRAY['Ri*****de', '***', NULL] /* masked */")
    );
    assert!(safe.log_context.omission_reason.is_none());
    assert_eq!(
        source.params[0],
        Value::List(vec!["Riverside".into(), "Ada".into(), Value::Null])
    );
}
