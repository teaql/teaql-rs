//! Log projections only: original execution inputs and business events stay intact.
#[cfg(test)]
#[path = "masking_contract_tests.rs"]
mod masking_contract_tests;
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
    teaql_data_service::is_credential_log_name(name)
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
        scrub(&mut node.entity_type, secrets);
    }
}

fn mask_business_value(value: &Value) -> Value {
    match value {
        Value::Null | Value::TypedNull(_) => value.clone(),
        Value::List(values) => Value::List(values.iter().map(mask_business_value).collect()),
        Value::Object(_) | Value::Json(_) => Value::Text(REDACTED.into()),
        _ => {
            let mut text = Vec::new();
            collect_strings(value, &mut text);
            Value::Text(crate::event::mask_audit_value(&text.join("")))
        }
    }
}

pub(crate) fn sql_entry(entry: &SqlLogEntry, allow: bool) -> SqlLogEntry {
    let allow = allow && entry.log_context.mode.as_deref() != Some("safe");
    let was_debug = entry.log_context.mode.as_deref() == Some("debug_plaintext")
        || entry
            .debug_sql
            .starts_with("-- TeaQL DEBUG PLAINTEXT; EXPLICIT OPT-IN");
    let alternative = if was_debug {
        entry
            .log_context
            .projection_state
            .get::<SafeSqlProjection>()
            .filter(|state| state.fingerprint == sql_fingerprint(entry))
            .map(|state| &state.safe)
    } else {
        None
    };
    if !allow && let Some(safe) = alternative {
        return safe.clone();
    }
    let mut result = sql_entry_inner(entry, allow, was_debug && !allow);
    if allow {
        let safe = alternative
            .cloned()
            .unwrap_or_else(|| sql_entry_inner(entry, false, was_debug));
        let fingerprint = sql_fingerprint(&result);
        result.log_context.projection_state =
            teaql_data_service::SqlProjectionState::new(SafeSqlProjection { fingerprint, safe });
    }
    result
}

// No source bindings or raw source entry are retained. The safe alternative has
// no state of its own, so there is no ownership cycle or recursive object graph.
struct SafeSqlProjection {
    fingerprint: [u8; 32],
    safe: SqlLogEntry,
}

fn sql_fingerprint(entry: &SqlLogEntry) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    // Debug preserves native value types/decimal text; opaque state is excluded.
    Sha256::digest(format!("{entry:?}").as_bytes()).into()
}

fn sql_entry_inner(entry: &SqlLogEntry, allow: bool, hide_intent: bool) -> SqlLogEntry {
    use teaql_data_service::SqlParameterLogPolicy as Policy;
    use teaql_sql::{DatabaseKind, render_sql_value, render_sql_with};
    let mut result = entry.clone();
    result.log_context.projection_state = Default::default();
    let kind = match entry.log_context.database_kind.as_deref() {
        Some("postgresql") => Some(DatabaseKind::PostgreSql),
        Some("sqlite") => Some(DatabaseKind::Sqlite),
        Some("mysql") => Some(DatabaseKind::MySql),
        _ => None,
    };
    let policies = &entry.log_context.parameter_policies;
    let invalid_policies = !policies.is_empty() && policies.len() != entry.params.len();
    let invalid_masks = !entry.log_context.masked_parameters.is_empty()
        && entry.log_context.masked_parameters.len() != entry.params.len();
    // Text is only used to increase protection. It can never mark a value plain.
    let credentials = credential_name(&entry.sql)
        && (!entry.log_context.generated_sql || policies.is_empty() || invalid_policies);
    let mut secrets = Vec::new();
    let mut masked = Vec::with_capacity(entry.params.len());
    for (index, value) in entry.params.iter().enumerate() {
        let policy = if invalid_policies || invalid_masks {
            Policy::Unknown
        } else {
            policies.get(index).copied().unwrap_or_default()
        };
        let already_masked = !invalid_masks
            && entry.log_context.mode.is_some()
            && entry.log_context.masked_parameters.get(index) == Some(&true);
        let force = credentials || has_credentials(value) || policy == Policy::Credential;
        // Unknown provenance is never a plaintext permission, even with opt-in.
        let protect = force || policy == Policy::Unknown || (policy == Policy::Masked && !allow);
        masked.push(protect || already_masked);
        if protect && !already_masked {
            collect_strings(value, &mut secrets);
            result.params[index] = match value {
                Value::Null | Value::TypedNull(_) => value.clone(),
                _ if policy == Policy::Masked && !force => mask_business_value(value),
                _ => Value::Text(REDACTED.into()),
            };
        }
    }
    entry
        .log_context
        .intent_redactions
        .extend_secrets(allow, &mut secrets);
    secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
    for text in [
        &mut result.comment,
        &mut result.purpose,
        &mut result.audit_reason,
        &mut result.result_type,
    ]
    .into_iter()
    .flatten()
    {
        scrub(text, &secrets);
    }
    scrub(&mut result.result_summary, &secrets);
    scrub_trace(&mut result.trace_path, &secrets);
    result.log_context.intent_redactions.clear();
    result.log_context.masked_parameters = masked;
    result.log_context.mode = Some(if allow { "debug_plaintext" } else { "safe" }.into());
    let rendered = (|| {
        if entry.log_context.omission_reason.is_some() {
            return Err("previously omitted SQL");
        }
        let kind = kind.ok_or("unknown database dialect")?;
        if invalid_policies {
            return Err("parameter policy count mismatch");
        }
        if invalid_masks {
            return Err("parameter mask count mismatch");
        }
        if !entry.log_context.generated_sql {
            let template =
                render_sql_with(&entry.sql, kind, entry.params.len(), |_| Ok(String::new()))?;
            if template.contains(['\'', '"', '`'])
                || template.contains("--")
                || template.contains("/*")
                || template.chars().any(|c| c.is_ascii_digit())
            {
                return Err("untrusted SQL literals or comments");
            }
        }
        render_sql_with(&entry.sql, kind, result.params.len(), |index| {
            let mut literal = render_sql_value(&result.params[index], kind)?;
            if result.log_context.masked_parameters[index] {
                literal.push_str(" /* masked */");
            }
            Ok(literal)
        })
    })();
    let has_masked = result.log_context.masked_parameters.iter().any(|v| *v);
    let mode = if allow {
        "DEBUG PLAINTEXT; EXPLICIT OPT-IN"
    } else {
        "SAFE"
    };
    match rendered {
        Ok(sql) => {
            result.log_context.omission_reason = None;
            result.debug_sql = format!(
                "-- TeaQL {mode}{}\n{sql}",
                if has_masked {
                    "; MASKED; NOT REPLAYABLE"
                } else {
                    ""
                }
            );
        }
        Err(reason) => {
            // A failed render revokes even previously plain values. Scrub the
            // accompanying intent/trace too, not just the SQL and bind array.
            entry
                .params
                .iter()
                .for_each(|value| collect_strings(value, &mut secrets));
            secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
            for text in [
                &mut result.comment,
                &mut result.purpose,
                &mut result.audit_reason,
                &mut result.result_type,
            ]
            .into_iter()
            .flatten()
            {
                scrub(text, &secrets);
            }
            scrub(&mut result.result_summary, &secrets);
            scrub_trace(&mut result.trace_path, &secrets);
            let reason = match entry.log_context.omission_reason.as_deref() {
                None => reason,
                Some(
                    reason @ ("parameter policy count mismatch"
                    | "parameter mask count mismatch"
                    | "unknown database dialect"
                    | "untrusted SQL literals or comments"
                    | "unsupported escape string"
                    | "unterminated quoted SQL"
                    | "ambiguous MySQL backslash quoting"
                    | "unsupported executable SQL comment"
                    | "unterminated SQL comment"
                    | "unsupported MySQL hash comment"
                    | "missing binding"
                    | "invalid binding index"
                    | "unsupported dollar expression"
                    | "unterminated dollar quote"
                    | "unsupported numbered positional binding"
                    | "unsupported named binding"
                    | "unused binding"
                    | "non-finite SQL number"
                    | "ambiguous MySQL backslash literal"),
                ) => reason,
                Some(_) => "previously omitted SQL",
            };
            result.log_context.omission_reason = Some(reason.into());
            result.sql = SQL_REDACTED.into();
            // With an unknown template no binding is demonstrably safe either.
            result.params.fill(Value::Null);
            result.debug_sql =
                format!("-- TeaQL {mode}; MASKED; NOT REPLAYABLE\n[SQL omitted: {reason}]");
        }
    }
    result.pretty_sql = result.debug_sql.clone();
    if hide_intent {
        for text in [
            &mut result.comment,
            &mut result.purpose,
            &mut result.audit_reason,
            &mut result.result_type,
        ]
        .into_iter()
        .flatten()
        {
            *text = REDACTED.into();
        }
        result.result_summary = REDACTED.into();
        for node in &mut result.trace_path {
            node.comment = REDACTED.into();
            node.entity_type = REDACTED.into();
        }
    }
    // Counts are execution facts, not binding values. Preserve a recognized
    // count-only summary even when a sensitive binding happens to equal "1".
    let count_summary = entry
        .result_count
        .map(|n| (n as u64, "returned"))
        .or_else(|| entry.affected_rows.map(|n| (n, "affected")));
    if let Some((count, verb)) = count_summary {
        if [
            format!("{count} row {verb}"),
            format!("{count} rows {verb}"),
        ]
        .contains(&entry.result_summary)
        {
            result.result_summary.clone_from(&entry.result_summary);
        } else if hide_intent {
            result.result_summary = format!("{count} rows {verb}");
        }
    }
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
    if let Some(id) = event.values.get("id") {
        collect_strings(id, &mut secrets);
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
    fn audit_trace_scrubs_target_id_for_all_mutation_kinds() {
        let id = Value::I64(1001);
        let events = [
            RawAuditEvent::created("Order", Record::from([("id".into(), id.clone())])),
            RawAuditEvent::updated("Order", Record::from([("id".into(), id.clone())])),
            RawAuditEvent::deleted("Order", id.clone(), Some(1)),
            RawAuditEvent::recovered("Order", id.clone(), 1),
        ];
        for mut event in events {
            event.trace_chain.push(teaql_core::TraceNode::typed(
                teaql_core::TraceKind::AuditReason,
                "Order",
                Some(1001),
                "change order 1001",
            ));
            let safe = event.build_safe_event(&[], None);
            assert_eq!(safe.trace_chain[0].comment, "change order [REDACTED]");
            let standard = audit_event(&event, false);
            assert_eq!(standard.trace_chain[0].comment, "change order [REDACTED]");
            assert_eq!(event.trace_chain[0].comment, "change order 1001");
            assert_eq!(event.values.get("id"), Some(&id));
        }
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
            assert!(!safe.contains("RETAINED-WRITE-CANARY"));
            assert!(safe.contains("read authoritative snapshot"));
            assert!(safe.contains(&format!(
                "name = '{}' /* masked */",
                crate::event::mask_audit_value("PRIVATE-CUSTOMER")
            )));
            assert!(!safe.contains("name = ?"));
            assert!(!safe.contains("Parameterized SQL:"));
            assert!(safe.contains("NOT REPLAYABLE"));
            let sql_diagnostic = std::fs::read_to_string(&sql).unwrap_or_default();
            if ack == Some(ACK) {
                assert!(sql_diagnostic.contains("DEBUG PLAINTEXT; EXPLICIT OPT-IN"));
                assert!(sql_diagnostic.contains("name = 'PRIVATE-CUSTOMER'"));
                assert!(!sql_diagnostic.contains("TOKEN-CANARY"));
            } else {
                assert!(sql_diagnostic.is_empty());
            }
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
            log_context: teaql_data_service::SqlLogContext {
                database_kind: Some("sqlite".into()),
                parameter_policies: vec![teaql_data_service::SqlParameterLogPolicy::Masked],
                ..Default::default()
            },
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
        // A retained debug record has no pending raw provenance. The normal
        // file sink must still downgrade a clone safely, independent of env.
        let mut readback = entry.clone();
        readback.log_context.intent_redactions =
            teaql_data_service::SqlIntentRedactions::from_bindings(
                &entry.log_context,
                &[Value::from("RETAINED-WRITE-CANARY")],
                &entry.sql,
            );
        readback.log_context.parameter_policies =
            vec![teaql_data_service::SqlParameterLogPolicy::Plain];
        readback.sql = "SELECT name FROM customer WHERE id = ?".into();
        readback.params = vec![Value::U64(1)];
        readback.comment = Some("read authoritative snapshot RETAINED-WRITE-CANARY".into());
        let retained = sql_entry(&readback, true).clone();
        assert!(
            retained
                .comment
                .as_ref()
                .unwrap()
                .contains("RETAINED-WRITE-CANARY")
        );
        LogManager::write_sql_log(&retained.trace_path, &retained);
    }
}
