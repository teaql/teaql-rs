use crate::event::RawAuditEvent;
use teaql_core::TraceNode;

/// Represents a log entry for SQL execution
pub use crate::context::SqlLogEntry;

/// A trait for defining how logs should be formatted before being output
pub trait LogFormatter: Send + Sync {
    /// Format an SQL log entry along with its trace chain
    fn format_sql_log(&self, trace_chain: &[TraceNode], entry: &SqlLogEntry) -> String;
    
    /// Format an audit or mutation event log
    fn format_audit_log(&self, event: &RawAuditEvent) -> String;
}

/// A human-readable log formatter, designed for developers and operators.
/// Formats time, elapsed duration, and entity changes cleanly.
pub struct HumanReaderFormatter;

impl HumanReaderFormatter {
    fn format_trace_chain(&self, trace_chain: &[TraceNode]) -> String {
        if trace_chain.is_empty() {
            "".to_string()
        } else {
            trace_chain.iter().map(|n| n.comment.clone()).collect::<Vec<_>>().join(" -> ")
        }
    }
}

impl LogFormatter for HumanReaderFormatter {
    fn format_sql_log(&self, trace_chain: &[TraceNode], entry: &SqlLogEntry) -> String {
        let ts = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let trace_str = self.format_trace_chain(trace_chain);
        let trace_display = if trace_str.is_empty() {
            "".to_string()
        } else {
            format!(" - [{}]", trace_str)
        };
        
        let elapsed_us = (entry.elapsed.as_secs_f64() * 1_000_000.0).round() as u64;
        format!("[{}]-[{:>5}µs]-[DEBUG]-SqlLogEntry{} - [{}]\n          {}", 
            ts, elapsed_us, trace_display, entry.result_summary, entry.pretty_sql.replace('\n', " "))
    }
    
    fn format_audit_log(&self, event: &RawAuditEvent) -> String {
        let ts = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let trace_str = self.format_trace_chain(&event.trace_chain);
        let trace_display = if trace_str.is_empty() {
            String::new()
        } else {
            format!(" (Trace: {})", trace_str)
        };
        
        let mut field_changes = Vec::new();
        for change in &event.changes {
            let val = change.new_value.as_ref().map(|v| format!("{:?}", v)).unwrap_or_else(|| "null".to_string());
            field_changes.push(format!("{}: {}", change.field, val));
        }
        let fields_part = if field_changes.is_empty() {
            String::new()
        } else {
            format!(" {{{}}}", field_changes.join(", "))
        };
        
        let mut entity_id = "Unknown".to_string();
        if let Some(vals) = &event.new_values {
            if let Some(id_val) = vals.get("id") {
                entity_id = format!("{:?}", id_val);
            }
        }
        
        format!("[{}]-[AUDIT]-Entity [{}:{}] {:?}{}{}", ts, event.entity, entity_id, event.kind, trace_display, fields_part)
    }
}

/// A structured or debug formatter intended for machine consumption or fallback
pub struct DebugReaderFormatter;

impl DebugReaderFormatter {
    fn format_trace_chain(&self, trace_chain: &[TraceNode]) -> String {
        if trace_chain.is_empty() {
            "(Trace: None)".to_string()
        } else {
            format!("(Trace: {})", trace_chain.iter().map(|n| n.comment.clone()).collect::<Vec<_>>().join(" -> "))
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
        format!("[AUDIT_LOG] {} - Event: {:?}", trace_str, event)
    }
}

/// Factory pattern for instantiating the correct log formatter
pub struct LogFormatterFactory;

impl LogFormatterFactory {
    /// Returns a singleton reference to the configured LogFormatter.
    /// It dynamically switches based on the TEAQL_LOG_FORMAT environment variable.
    pub fn get_formatter() -> &'static (dyn LogFormatter + Send + Sync) {
        static FORMATTER: std::sync::OnceLock<Box<dyn LogFormatter + Send + Sync>> = std::sync::OnceLock::new();
        FORMATTER.get_or_init(|| {
            let format = std::env::var("TEAQL_LOG_FORMAT").unwrap_or_else(|_| "human".to_string());
            if format == "json" || format == "debug" {
                Box::new(DebugReaderFormatter)
            } else {
                Box::new(HumanReaderFormatter)
            }
        }).as_ref()
    }
}

/// Manager that handles reading the endpoint environment variable and dispatching to the factory
pub struct LogManager;

static LOG_ENDPOINT: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();

impl LogManager {
    fn get_log_endpoint() -> Option<&'static str> {
        LOG_ENDPOINT.get_or_init(|| {
            std::env::var("TEAQL_LOG_ENDPOINT").ok().filter(|s| !s.is_empty())
        }).as_deref()
    }

    fn write_to_file(content: &str) {
        if let Some(endpoint) = Self::get_log_endpoint() {
            if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(endpoint) {
                use std::io::Write;
                let _ = writeln!(file, "{}", content);
            }
        }
    }

    pub fn write_sql_log(trace_chain: &[TraceNode], entry: &SqlLogEntry) {
        if Self::get_log_endpoint().is_none() {
            return;
        }
        let content = LogFormatterFactory::get_formatter().format_sql_log(trace_chain, entry);
        Self::write_to_file(&content);
    }

    pub fn write_audit_log(event: &RawAuditEvent) {
        if Self::get_log_endpoint().is_none() {
            return;
        }
        let content = LogFormatterFactory::get_formatter().format_audit_log(event);
        Self::write_to_file(&content);
    }
}
