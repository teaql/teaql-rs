//! Log projections only: original execution inputs and business events stay intact.
use crate::{RawAuditEvent, SqlLogEntry};
use teaql_core::{Record, TraceNode, Value};

pub(crate) const ENV: &str = "TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS";
pub(crate) const ACK: &str = "I_UNDERSTAND_SENSITIVE_DATA_MAY_BE_WRITTEN_TO_DISK";
const REDACTED: &str = "[REDACTED]";
const SQL_REDACTED: &str = "[REDACTED SQL; NOT REPLAYABLE]";

fn accepts(value: Option<&str>) -> bool {
    value == Some(ACK)
}

pub(crate) fn plaintext_enabled() -> bool {
    let enabled = accepts(std::env::var(ENV).ok().as_deref());
    if enabled {
        static WARNING: std::sync::Once = std::sync::Once::new();
        WARNING.call_once(|| eprintln!("TeaQL WARNING: sensitive plaintext logging enabled; application data may be written to disk. Authentication secrets remain redacted."));
    }
    enabled
}

pub(crate) fn credential_name(name: &str) -> bool {
    let name: String = name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect();
    [
        "password",
        "passwd",
        "passphrase",
        "privatekey",
        "secret",
        "accesstoken",
        "refreshtoken",
        "idtoken",
        "apikey",
        "authorization",
        "credential",
        "sessiontoken",
        "magiclinktoken",
    ]
    .iter()
    .any(|word| name.contains(word))
}

pub(crate) fn has_credentials(value: &Value) -> bool {
    match value {
        Value::Object(fields) => fields
            .iter()
            .any(|(k, v)| credential_name(k) || has_credentials(v)),
        Value::List(values) => values.iter().any(has_credentials),
        Value::Json(value) => json_has_credentials(value),
        _ => false,
    }
}

fn json_has_credentials(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(fields) => fields
            .iter()
            .any(|(k, v)| credential_name(k) || json_has_credentials(v)),
        serde_json::Value::Array(values) => values.iter().any(json_has_credentials),
        _ => false,
    }
}

pub(crate) fn collect_strings(value: &Value, output: &mut Vec<String>) {
    match value {
        Value::Null | Value::TypedNull(_) => (),
        Value::Text(value) => {
            if !value.is_empty() {
                output.push(value.clone());
            }
        }
        Value::Object(fields) => fields.values().for_each(|v| collect_strings(v, output)),
        Value::List(values) => values.iter().for_each(|v| collect_strings(v, output)),
        Value::Json(value) => collect_json_strings(value, output),
        Value::Bool(v) => output.push(v.to_string()),
        Value::I64(v) => output.push(v.to_string()),
        Value::U64(v) => output.push(v.to_string()),
        Value::F64(v) => output.push(v.to_string()),
        Value::Decimal(v) => output.push(v.to_string()),
        Value::Date(v) => output.push(v.to_string()),
        Value::Timestamp(v) => output.push(v.as_millis().to_string()),
    }
}

fn collect_json_strings(value: &serde_json::Value, output: &mut Vec<String>) {
    match value {
        serde_json::Value::String(s) => {
            if !s.is_empty() {
                output.push(s.clone());
            }
        }
        serde_json::Value::Array(values) => {
            values.iter().for_each(|v| collect_json_strings(v, output))
        }
        serde_json::Value::Object(values) => values
            .values()
            .for_each(|v| collect_json_strings(v, output)),
        serde_json::Value::Null => (),
        _ => output.push(value.to_string()),
    }
}

pub(crate) fn scrub(text: &mut String, secrets: &[String]) {
    for secret in secrets {
        *text = text.replace(secret, REDACTED);
    }
}

pub(crate) fn scrub_trace(trace: &mut [TraceNode], secrets: &[String]) {
    for node in trace {
        scrub(&mut node.comment, secrets);
    }
}

pub(crate) fn sql_entry(entry: &SqlLogEntry, allow: bool) -> SqlLogEntry {
    let mut result = entry.clone();
    let credentials = credential_name(&entry.sql)
        || credential_name(&entry.debug_sql)
        || entry.params.iter().any(has_credentials);
    if allow && !credentials {
        return result;
    }
    let mut secrets = Vec::new();
    entry
        .params
        .iter()
        .for_each(|v| collect_strings(v, &mut secrets));
    secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
    // There is no parameter-to-field provenance here. Arbitrary literal SQL
    // cannot be safely interpreted by a cross-dialect log formatter.
    if entry.sql.contains(['\'', '"', '`', '$'])
        || entry.sql.contains("--")
        || entry.sql.contains("/*")
        || entry.sql.chars().any(|c| c.is_ascii_digit())
    {
        result.sql = SQL_REDACTED.into();
    }
    scrub(&mut result.sql, &secrets);
    for value in &mut result.params {
        *value = Value::Null;
    }
    result.debug_sql = SQL_REDACTED.into();
    result.pretty_sql = SQL_REDACTED.into();
    for text in [
        &mut result.comment,
        &mut result.purpose,
        &mut result.audit_reason,
    ]
    .into_iter()
    .flatten()
    {
        scrub(text, &secrets);
    }
    scrub_trace(&mut result.trace_path, &secrets);
    result
}

pub(crate) fn audit_event(event: &RawAuditEvent, allow: bool) -> RawAuditEvent {
    let mut result = event.clone();
    let mut secrets = Vec::new();
    fn redact(field: &str, value: &mut Value, allow: bool, secrets: &mut Vec<String>) {
        if credential_name(field)
            || has_credentials(value)
            || (!allow && field != "id" && field != "version")
        {
            collect_strings(value, secrets);
            if !matches!(value, Value::Null | Value::TypedNull(_)) {
                *value = REDACTED.into();
            }
        }
    }
    fn record(record: &mut Record, allow: bool, secrets: &mut Vec<String>) {
        for (field, value) in record {
            redact(field, value, allow, secrets);
        }
    }
    record(&mut result.values, allow, &mut secrets);
    for r in [&mut result.old_values, &mut result.new_values]
        .into_iter()
        .flatten()
    {
        record(r, allow, &mut secrets);
    }
    for change in &mut result.changes {
        for v in [&mut change.old_value, &mut change.new_value]
            .into_iter()
            .flatten()
        {
            redact(&change.field, v, allow, &mut secrets);
        }
    }
    secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
    scrub_trace(&mut result.trace_chain, &secrets);
    if let Some(identity) = &mut result.bootstrap_audit {
        scrub(&mut identity.reason, &secrets);
        scrub(&mut identity.actor, &secrets);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_ack_only() {
        for value in [
            None,
            Some(""),
            Some("true"),
            Some("1"),
            Some(" I_UNDERSTAND_SENSITIVE_DATA_MAY_BE_WRITTEN_TO_DISK"),
        ] {
            assert!(!accepts(value));
        }
        assert!(accepts(Some(ACK)));
    }
    #[test]
    fn audit_projection_preserves_original_and_never_exposes_credentials() {
        let event = RawAuditEvent::created(
            "Customer",
            Record::from([
                ("name".into(), "PRIVATE-CUSTOMER".into()),
                ("access_token".into(), "TOKEN-CANARY".into()),
                ("id".into(), 1_i64.into()),
            ]),
        );
        let safe = audit_event(&event, false);
        assert!(!format!("{safe:?}").contains("PRIVATE-CUSTOMER"));
        let debug = audit_event(&event, true);
        assert!(format!("{debug:?}").contains("PRIVATE-CUSTOMER"));
        assert!(!format!("{debug:?}").contains("TOKEN-CANARY"));
        assert!(format!("{event:?}").contains("TOKEN-CANARY"));
    }

    #[test]
    fn file_log_gate_runs_in_isolated_processes() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory =
            std::env::temp_dir().join(format!("teaql-log-privacy-{}-{unique}", std::process::id()));
        std::fs::create_dir(&directory).unwrap();
        for (label, ack) in [
            ("unset", None),
            ("invalid", Some("true")),
            ("enabled", Some(ACK)),
        ] {
            let normal = directory.join(format!("{label}-normal.log"));
            let audit = directory.join(format!("{label}-audit.log"));
            let sql = directory.join(format!("{label}-sql.log"));
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "log_privacy::tests::file_log_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env_remove(ENV)
                .env_remove("TEAQL_TRACE_MODE")
                .env_remove("TEAQL_AUDIT_LOG_ENTITIES")
                .env_remove("TEAQL_SQL_LOG_TABLES")
                .env("TEAQL_LOG_ENDPOINT", &normal)
                .env("TEAQL_AUDIT_DEBUG_ENDPOINT", &audit)
                .env("TEAQL_SQL_DEBUG_ENDPOINT", &sql)
                .env("TEAQL_AUDIT_LOG", "_full_with_payload")
                .env("TEAQL_SQL_LOG", "_full");
            if let Some(ack) = ack {
                command.env(ENV, ack);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let safe = std::fs::read_to_string(normal).unwrap();
            assert!(!safe.contains("PRIVATE-CUSTOMER"));
            assert!(!safe.contains("TOKEN-CANARY"));
            let diagnostic = format!(
                "{}{}",
                std::fs::read_to_string(audit).unwrap_or_default(),
                std::fs::read_to_string(sql).unwrap_or_default()
            );
            assert!(!diagnostic.contains("TOKEN-CANARY"));
            assert_eq!(diagnostic.contains("PRIVATE-CUSTOMER"), ack == Some(ACK));
            assert_eq!(
                String::from_utf8_lossy(&output.stderr).contains("may be written to disk"),
                ack == Some(ACK)
            );
        }
        // Keep the tiny logs as inspectable evidence; each run owns a unique directory.
        eprintln!("Log privacy evidence: {}", directory.display());
    }

    #[test]
    #[ignore = "subprocess fixture for file_log_gate_runs_in_isolated_processes"]
    fn file_log_child() {
        use crate::log_formatter::LogManager;
        let event = RawAuditEvent::created(
            "Customer",
            Record::from([
                ("name".into(), "PRIVATE-CUSTOMER".into()),
                ("password".into(), "TOKEN-CANARY".into()),
            ]),
        );
        LogManager::write_audit_log(&event);
        let now = std::time::SystemTime::now();
        let entry = SqlLogEntry {
            operation: crate::SqlLogOperation::Update,
            comment: Some("edit PRIVATE-CUSTOMER".into()),
            purpose: Some("debug fixture".into()),
            audit_reason: None,
            trace_path: vec![],
            sql: "UPDATE customer SET name = ?".into(),
            params: vec!["PRIVATE-CUSTOMER".into()],
            debug_sql: "UPDATE customer SET name = 'PRIVATE-CUSTOMER'".into(),
            pretty_sql: String::new(),
            started_at: now,
            ended_at: now,
            elapsed: std::time::Duration::ZERO,
            result_count: None,
            result_type: None,
            affected_rows: Some(1),
            result_summary: "1 rows affected".into(),
        };
        LogManager::write_sql_log(&[], &entry);
        LogManager::write_sensitive_sql_log(&[], &entry);
        let mut credential = entry.clone();
        credential.sql = "UPDATE customer SET password = ?".into();
        credential.params = vec!["TOKEN-CANARY".into()];
        credential.comment = Some("edit TOKEN-CANARY".into());
        credential.debug_sql = "UPDATE customer SET password = 'TOKEN-CANARY'".into();
        LogManager::write_sql_log(&[], &credential);
        LogManager::write_sensitive_sql_log(&[], &credential);
    }
}
