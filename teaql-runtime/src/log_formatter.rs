use crate::event::RawAuditEvent;
use teaql_core::TraceNode;

/// Represents a log entry for SQL execution
pub use crate::context::{SqlLogEntry, SqlLogOperation};

/// A trait for defining how logs should be formatted before being output
pub trait LogFormatter: Send + Sync {
    /// Format an SQL log entry along with its trace chain
    fn format_sql_log(&self, trace_chain: &[TraceNode], entry: &SqlLogEntry) -> String;

    /// Format an audit or mutation event log
    fn format_audit_log(&self, event: &RawAuditEvent) -> String;

    /// Format an audit event for an explicitly configured sensitive sink.
    ///
    /// The conservative default remains value-free so adding the sensitive
    /// sink does not force third-party formatters to disclose payloads or to
    /// implement a new required trait method. Formatters that deliberately
    /// support raw audit diagnostics must override this method.
    fn format_sensitive_audit_log(&self, event: &RawAuditEvent) -> String {
        self.format_audit_log(event)
    }
}

/// A human-readable log formatter, designed for developers and operators.
/// Formats time, elapsed duration, and entity changes cleanly.
pub struct HumanReaderFormatter;

impl HumanReaderFormatter {
    fn format_trace_chain(&self, trace_chain: &[TraceNode]) -> String {
        if !trace_chain.is_empty() {
            trace_chain
                .iter()
                .enumerate()
                .map(|(level, n)| format!("{}:{:?}:{}={}", level, n.kind, n.entity_type, n.comment))
                .collect::<Vec<_>>()
                .join(" -> ")
        } else {
            Default::default()
        }
    }

    fn format_audit_log_internal(&self, event: &RawAuditEvent, include_values: bool) -> String {
        let ts = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let trace_str = self.format_trace_chain(&event.trace_chain);
        let trace_display = if !trace_str.is_empty() {
            format!(" (Trace: {})", trace_str)
        } else {
            Default::default()
        };

        let field_changes = event
            .changes
            .iter()
            .filter(|change| !change.field.starts_with('_'))
            .map(|change| {
                if include_values {
                    let value = change
                        .new_value
                        .as_ref()
                        .map(|value| format!("{:?}", value))
                        .unwrap_or_else(|| "null".to_owned());
                    format!("{}: {}", change.field, value)
                } else {
                    change.field.clone()
                }
            })
            .collect::<Vec<_>>();
        let fields_part = if field_changes.is_empty() {
            String::new()
        } else if include_values {
            format!(" {{{}}}", field_changes.join(", "))
        } else {
            format!(" fields=[{}]", field_changes.join(", "))
        };

        format!(
            "[{}]-[AUDIT]-Entity [{}:{}] {:?}{}{}",
            ts,
            event.entity,
            audit_entity_id(event),
            event.kind,
            trace_display,
            fields_part
        )
    }
}

impl LogFormatter for HumanReaderFormatter {
    fn format_sql_log(&self, trace_chain: &[TraceNode], entry: &SqlLogEntry) -> String {
        let ts = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let trace_str = self.format_trace_chain(trace_chain);
        let trace_display = if !trace_str.is_empty() {
            format!(" - [{}]", trace_str)
        } else {
            Default::default()
        };

        let elapsed_us = (entry.elapsed.as_secs_f64() * 1_000_000.0).round() as u64;
        let intent = format!(
            "comment={:?} purpose={:?} auditReason={:?}",
            entry.comment, entry.purpose, entry.audit_reason
        );
        let mut output = format!(
            "[{}]-[{:>5}µs]-[DEBUG]-SqlLogEntry{} - [{}] {}\n          Parameterized SQL: {}",
            ts,
            elapsed_us,
            trace_display,
            entry.result_summary,
            intent,
            entry.sql.replace('\n', " ")
        );
        if !entry.debug_sql.is_empty() {
            output.push_str(&format!(
                " params={:?}\n          Debug SQL: {}",
                entry.params,
                entry.debug_sql.replace('\n', " ")
            ));
        }
        output
    }

    fn format_audit_log(&self, event: &RawAuditEvent) -> String {
        self.format_audit_log_internal(event, false)
    }

    fn format_sensitive_audit_log(&self, event: &RawAuditEvent) -> String {
        self.format_audit_log_internal(event, true)
    }
}

/// A structured or debug formatter intended for machine consumption or fallback
pub struct DebugReaderFormatter;

impl DebugReaderFormatter {
    fn format_trace_chain(&self, trace_chain: &[TraceNode]) -> String {
        match trace_chain.is_empty() {
            true => "(Trace: None)".to_string(),
            false => format!(
                "(Trace: {})",
                trace_chain
                    .iter()
                    .enumerate()
                    .map(|(level, n)| {
                        format!("{}:{:?}:{}={}", level, n.kind, n.entity_type, n.comment)
                    })
                    .collect::<Vec<_>>()
                    .join(" -> ")
            ),
        }
    }
}

impl LogFormatter for DebugReaderFormatter {
    fn format_sql_log(&self, trace_chain: &[TraceNode], entry: &SqlLogEntry) -> String {
        let trace_str = self.format_trace_chain(trace_chain);
        format!("[SQL_LOG] {} - Event: {:?}", trace_str, entry)
    }

    fn format_audit_log(&self, event: &RawAuditEvent) -> String {
        let trace_str = self.format_trace_chain(&event.trace_chain);
        let fields = event
            .changes
            .iter()
            .filter(|change| !change.field.starts_with('_'))
            .map(|change| change.field.as_str())
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "[AUDIT_LOG] {} - entity={} id={} kind={:?} fields=[{}]",
            trace_str,
            event.entity,
            audit_entity_id(event),
            event.kind,
            fields
        )
    }

    fn format_sensitive_audit_log(&self, event: &RawAuditEvent) -> String {
        let trace_str = self.format_trace_chain(&event.trace_chain);
        format!("[AUDIT_LOG_RAW] {} - Event: {:?}", trace_str, event)
    }
}

fn audit_entity_id(event: &RawAuditEvent) -> String {
    event
        .new_values
        .as_ref()
        .and_then(|values| values.get("id"))
        .or_else(|| event.values.get("id"))
        .map(|value| format!("{:?}", value))
        .unwrap_or_else(|| "Unknown".to_owned())
}

/// Factory pattern for instantiating the correct log formatter
pub struct LogFormatterFactory;

impl LogFormatterFactory {
    /// Returns a singleton reference to the configured LogFormatter.
    /// It dynamically switches based on the TEAQL_LOG_FORMAT environment variable.
    pub fn get_formatter() -> &'static (dyn LogFormatter + Send + Sync) {
        static FORMATTER: std::sync::OnceLock<Box<dyn LogFormatter + Send + Sync>> =
            std::sync::OnceLock::new();
        FORMATTER
            .get_or_init(|| {
                let format =
                    std::env::var("TEAQL_LOG_FORMAT").unwrap_or_else(|_| "human".to_string());
                match format.as_str() {
                    "json" | "debug" => Box::new(DebugReaderFormatter),
                    _ => Box::new(HumanReaderFormatter),
                }
            })
            .as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Silent,
    Summary,
    Full,
    FullWithPayload,
}

impl LogLevel {
    pub fn parse(s: &str, default: LogLevel) -> Self {
        match s {
            "_silent" => LogLevel::Silent,
            "_summary" => LogLevel::Summary,
            "_full" => LogLevel::Full,
            "_full_with_payload" => LogLevel::FullWithPayload,
            _ => default,
        }
    }
}

pub struct LogConfig {
    pub audit_level: LogLevel,
    pub sql_level: LogLevel,
    pub tool_level: LogLevel,
    pub audit_entities: Option<Vec<String>>,
    pub sql_tables: Option<Vec<String>>,
    pub tool_focus: Option<Vec<String>>,
}

impl LogConfig {
    pub fn load() -> Self {
        let audit_level = LogLevel::parse(
            &std::env::var("TEAQL_AUDIT_LOG").unwrap_or_default(),
            LogLevel::Full,
        );
        let sql_level = LogLevel::parse(
            &std::env::var("TEAQL_SQL_LOG").unwrap_or_default(),
            LogLevel::Summary,
        );
        let tool_level = LogLevel::parse(
            &std::env::var("TEAQL_TOOL_LOG").unwrap_or_default(),
            LogLevel::Full,
        );

        let audit_entities = std::env::var("TEAQL_AUDIT_LOG_ENTITIES")
            .ok()
            .map(|s| s.split(',').map(|s| s.trim().to_string()).collect());
        let sql_tables = std::env::var("TEAQL_SQL_LOG_TABLES")
            .ok()
            .map(|s| s.split(',').map(|s| s.trim().to_string()).collect());
        let tool_focus = std::env::var("TEAQL_TOOL_LOG_FOCUS")
            .ok()
            .map(|s| s.split(',').map(|s| s.trim().to_string()).collect());

        Self {
            audit_level,
            sql_level,
            tool_level,
            audit_entities,
            sql_tables,
            tool_focus,
        }
    }

    pub fn should_log_audit(&self, entity: &str) -> bool {
        if self.audit_level == LogLevel::Silent {
            return false;
        }
        if let Some(entities) = &self.audit_entities
            && !entities.iter().any(|e| e.eq_ignore_ascii_case(entity))
        {
            return false;
        }
        true
    }

    pub fn should_log_sensitive_audit(&self, entity: &str) -> bool {
        self.audit_level == LogLevel::FullWithPayload && self.should_log_audit(entity)
    }

    pub fn should_log_sql(&self, sql: &str) -> bool {
        if self.sql_level == LogLevel::Silent {
            return false;
        }
        if let Some(tables) = &self.sql_tables {
            let sql_lower = sql.to_ascii_lowercase();
            if !tables
                .iter()
                .any(|t| sql_lower.contains(&t.to_ascii_lowercase()))
            {
                return false;
            }
        }
        true
    }

    pub fn should_log_tool(&self, module: &str) -> bool {
        if self.tool_level == LogLevel::Silent {
            return false;
        }
        if let Some(focus) = &self.tool_focus
            && !focus.iter().any(|f| f.eq_ignore_ascii_case(module))
        {
            return false;
        }
        true
    }
}

/// Manager that handles reading the endpoint environment variable and dispatching to the factory
pub struct LogManager;

static LOG_ENDPOINT: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
static SQL_DEBUG_ENDPOINT: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
static AUDIT_DEBUG_ENDPOINT: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
static HEADER_WRITTEN: std::sync::Once = std::sync::Once::new();

const EXTREME_TEST_FLAG: &str =
    "__i_agree_to_disable_runtime_trace_only_for_extreme_performance_testing";

impl LogManager {
    pub fn config() -> &'static LogConfig {
        static CONFIG: std::sync::OnceLock<LogConfig> = std::sync::OnceLock::new();
        CONFIG.get_or_init(LogConfig::load)
    }

    fn get_log_endpoint() -> Option<&'static str> {
        LOG_ENDPOINT
            .get_or_init(|| {
                let mode = std::env::var("TEAQL_TRACE_MODE").unwrap_or_default();
                if mode == "off" {
                    let ack = std::env::var("TEAQL_TRACE_OFF_ACK").unwrap_or_default();
                    if ack == EXTREME_TEST_FLAG {
                        return Some("off".to_string());
                    }
                    // If they didn't sign the waiver, ignore the off request and fallthrough
                }

                std::env::var("TEAQL_LOG_ENDPOINT")
                    .ok()
                    .filter(|v| !v.is_empty())
                    .or_else(|| {
                        if let Ok(val) = std::env::var("TEAQL_DOMAIN")
                            && !val.is_empty()
                        {
                            return Some(format!("{}.log", val));
                        }
                        let exe_name = std::env::current_exe()
                            .ok()
                            .and_then(|p| p.file_name().map(|s| s.to_string_lossy().into_owned()))
                            .unwrap_or_else(|| "teaql".to_string());
                        Some(format!("{}.log", exe_name))
                    })
            })
            .as_deref()
    }

    fn get_sql_debug_endpoint() -> Option<&'static str> {
        SQL_DEBUG_ENDPOINT
            .get_or_init(|| {
                std::env::var("TEAQL_SQL_DEBUG_ENDPOINT")
                    .ok()
                    .filter(|endpoint| !endpoint.trim().is_empty())
            })
            .as_deref()
    }

    fn get_audit_debug_endpoint() -> Option<&'static str> {
        AUDIT_DEBUG_ENDPOINT
            .get_or_init(|| {
                std::env::var("TEAQL_AUDIT_DEBUG_ENDPOINT")
                    .ok()
                    .filter(|endpoint| !endpoint.trim().is_empty())
            })
            .as_deref()
    }

    fn write_header_if_needed(endpoint: &str) {
        if endpoint == "off" {
            return;
        }
        HEADER_WRITTEN.call_once(|| {
            let header = include_str!("log_header.txt");
            match endpoint {
                "stdout" => println!("{}", header),
                path => {
                    if let Ok(mut file) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                    {
                        use std::io::Write;
                        let _ = writeln!(file, "{}", header);
                    }
                }
            }
        });
    }

    fn write_to_file(content: &str) {
        if let Some(endpoint) = Self::get_log_endpoint() {
            if endpoint == "off" {
                return;
            }

            Self::write_header_if_needed(endpoint);

            match endpoint {
                "stdout" => println!("{}", content),
                path => {
                    if let Ok(mut file) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                    {
                        use std::io::Write;
                        let _ = writeln!(file, "{}", content);
                    }
                }
            }
        }
    }

    pub fn write_sql_log(trace_chain: &[TraceNode], entry: &SqlLogEntry) {
        if !Self::config().should_log_sql(&entry.sql) {
            return;
        }
        if let Some(endpoint) = Self::get_log_endpoint() {
            if endpoint == "off" {
                return;
            }
            let content = LogFormatterFactory::get_formatter().format_sql_log(trace_chain, entry);
            Self::write_to_file(&content);
        }
    }

    pub(crate) fn write_sensitive_sql_log(trace_chain: &[TraceNode], entry: &SqlLogEntry) {
        if !Self::config().should_log_sql(&entry.sql)
            || matches!(Self::get_log_endpoint(), Some("off"))
        {
            return;
        }
        let Some(endpoint) = Self::get_sql_debug_endpoint() else {
            return;
        };
        let content = LogFormatterFactory::get_formatter().format_sql_log(trace_chain, entry);
        // If a diagnostic statement exceeds the bound, its partial rendering
        // must be visibly non-executable rather than appearing copy-pasteable.
        let content = truncate_sensitive_sql_log(&content, 64 * 1024);
        match endpoint {
            "stdout" => println!("{content}"),
            path => {
                if let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                {
                    use std::io::Write;
                    let _ = writeln!(file, "{content}");
                }
            }
        }
    }

    pub fn write_audit_log(event: &RawAuditEvent) {
        if !Self::config().should_log_audit(&event.entity) {
            return;
        }
        if let Some(endpoint) = Self::get_log_endpoint() {
            if endpoint == "off" {
                return;
            }
            let content = LogFormatterFactory::get_formatter().format_audit_log(event);
            Self::write_to_file(&content);
        }
        if !Self::config().should_log_sensitive_audit(&event.entity) {
            return;
        }
        let Some(endpoint) = Self::get_audit_debug_endpoint() else {
            return;
        };
        let content = LogFormatterFactory::get_formatter().format_sensitive_audit_log(event);
        let content = truncate_sensitive_audit_log(&content, 64 * 1024);
        match endpoint {
            "stdout" => println!("{content}"),
            path => {
                if let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                {
                    use std::io::Write;
                    let _ = writeln!(file, "{content}");
                }
            }
        }
    }
}

fn truncate_sensitive_sql_log(content: &str, max_bytes: usize) -> String {
    if content.len() <= max_bytes {
        return content.to_owned();
    }
    let mut end = max_bytes;
    while !content.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n[TRUNCATED; NOT EXECUTABLE: diagnostic SQL exceeded {} bytes]",
        &content[..end],
        max_bytes
    )
}

fn truncate_sensitive_audit_log(content: &str, max_bytes: usize) -> String {
    if content.len() <= max_bytes {
        return content.to_owned();
    }
    let mut end = max_bytes;
    while !content.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n[TRUNCATED: sensitive audit event exceeded {} bytes]",
        &content[..end],
        max_bytes
    )
}

#[cfg(test)]
mod diagnostic_log_tests {
    use super::{
        DebugReaderFormatter, HumanReaderFormatter, LogConfig, LogFormatter, LogLevel,
        truncate_sensitive_audit_log, truncate_sensitive_sql_log,
    };
    use crate::RawAuditEvent;
    use teaql_core::{Record, Value};

    #[test]
    fn bounded_diagnostic_log_marks_partial_sql_non_executable() {
        assert_eq!(truncate_sensitive_sql_log("SELECT 1", 100), "SELECT 1");
        let truncated = truncate_sensitive_sql_log("SELECT '🔐private-value'", 11);
        assert!(truncated.contains("TRUNCATED; NOT EXECUTABLE"));
        assert!(!truncated.contains("private-value"));
    }

    #[test]
    fn ordinary_audit_formatters_disclose_field_names_but_not_values() {
        let event = RawAuditEvent::created(
            "School",
            Record::from([
                ("id".to_owned(), Value::U64(7)),
                (
                    "name".to_owned(),
                    Value::Text("private-school-name".to_owned()),
                ),
            ]),
        );

        for formatter in [
            &HumanReaderFormatter as &dyn LogFormatter,
            &DebugReaderFormatter as &dyn LogFormatter,
        ] {
            let safe = formatter.format_audit_log(&event);
            assert!(safe.contains("School"));
            assert!(safe.contains("name"));
            assert!(!safe.contains("private-school-name"));

            let sensitive = formatter.format_sensitive_audit_log(&event);
            assert!(sensitive.contains("private-school-name"));
        }
    }

    #[test]
    fn bounded_sensitive_audit_log_does_not_split_utf8() {
        let truncated = truncate_sensitive_audit_log("audit 🔐private-value", 10);
        assert!(truncated.contains("TRUNCATED"));
        assert!(!truncated.contains("private-value"));
    }

    #[test]
    fn raw_audit_output_requires_full_with_payload_level() {
        let config = |audit_level| LogConfig {
            audit_level,
            sql_level: LogLevel::Silent,
            tool_level: LogLevel::Silent,
            audit_entities: None,
            sql_tables: None,
            tool_focus: None,
        };

        assert!(!config(LogLevel::Summary).should_log_sensitive_audit("School"));
        assert!(!config(LogLevel::Full).should_log_sensitive_audit("School"));
        assert!(config(LogLevel::FullWithPayload).should_log_sensitive_audit("School"));
    }
}
