use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use super::UserContext;
use teaql_core::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlLogOperation {
    Select,
    Insert,
    Update,
    Delete,
    Recover,
}

impl SqlLogOperation {
    pub fn is_select(self) -> bool {
        matches!(self, Self::Select)
    }

    pub fn is_mutation(self) -> bool {
        !self.is_select()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlLogOptions {
    pub select: bool,
    pub mutation: bool,
}

impl Default for SqlLogOptions {
    fn default() -> Self {
        Self::all()
    }
}

impl SqlLogOptions {
    pub fn disabled() -> Self {
        Self {
            select: false,
            mutation: false,
        }
    }

    pub fn select_only() -> Self {
        Self {
            select: true,
            mutation: false,
        }
    }

    pub fn mutation_only() -> Self {
        Self {
            select: false,
            mutation: true,
        }
    }

    pub fn all() -> Self {
        Self {
            select: true,
            mutation: true,
        }
    }

    pub fn enabled_for(self, operation: SqlLogOperation) -> bool {
        if operation.is_select() {
            self.select
        } else {
            self.mutation
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlLogEntry {
    pub operation: SqlLogOperation,
    pub comment: Option<String>,
    pub purpose: Option<String>,
    pub audit_reason: Option<String>,
    pub trace_path: Vec<teaql_core::TraceNode>,
    pub sql: String,
    pub params: Vec<Value>,
    pub debug_sql: String,
    pub pretty_sql: String,
    pub started_at: SystemTime,
    pub ended_at: SystemTime,
    pub elapsed: Duration,
    pub result_count: Option<usize>,
    pub result_type: Option<String>,
    pub affected_rows: Option<u64>,
    pub result_summary: String,
}

impl SqlLogEntry {
    /// Number of bound parameters without exposing their values.
    ///
    /// Safe/default log entries retain one `Value::Null` slot per parameter;
    /// sensitive diagnostic entries retain the original values. The count is
    /// therefore stable across both views of the same execution.
    pub fn parameter_count(&self) -> usize {
        self.params.len()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnifiedLogEntry {
    pub timestamp: SystemTime,
    pub user_identifier: Option<String>,
    pub trace_chain: Vec<teaql_core::TraceNode>,
    pub payload: LogPayload,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // Boxing Sql would break the public constructor shape.
pub enum LogPayload {
    Sql(SqlLogEntry),
    Info(InfoLogEntry),
}

#[derive(Debug, Clone, PartialEq)]
pub struct InfoLogEntry {
    pub message: String,
}

#[derive(Clone, Default)]
pub struct UnifiedLogBuffer {
    pub entries: Arc<Mutex<Vec<UnifiedLogEntry>>>,
}

impl UserContext {
    pub fn with_sql_log_options(mut self, options: SqlLogOptions) -> Self {
        self.sql_log_options = options;
        self
    }

    pub fn set_sql_log_options(&mut self, options: SqlLogOptions) {
        self.sql_log_options = options;
    }

    pub fn enable_select_sql_log(&mut self) {
        self.sql_log_options.select = true;
    }

    pub fn enable_mutation_sql_log(&mut self) {
        self.sql_log_options.mutation = true;
    }

    pub fn disable_select_sql_log(&mut self) {
        self.sql_log_options.select = false;
    }

    pub fn disable_mutation_sql_log(&mut self) {
        self.sql_log_options.mutation = false;
    }

    pub fn enable_all_sql_log(&mut self) {
        self.sql_log_options = SqlLogOptions::all();
    }

    pub fn disable_sql_log(&mut self) {
        self.sql_log_options = SqlLogOptions::disabled();
        self.clear_sql_logs();
    }

    pub fn sql_log_options(&self) -> SqlLogOptions {
        self.sql_log_options
    }

    pub fn sql_logs(&self) -> Vec<SqlLogEntry> {
        self.sql_log_entries
            .lock()
            .map(|entries| entries.clone())
            .unwrap_or_default()
    }

    pub fn clear_sql_logs(&self) {
        if let Ok(mut entries) = self.sql_log_entries.lock() {
            entries.clear();
        }
    }

    pub(crate) fn record_metadata_log(&self, metadata: &teaql_data_service::ExecutionMetadata) {
        let operation = match metadata.operation {
            teaql_data_service::DataServiceOperation::Query => SqlLogOperation::Select,
            teaql_data_service::DataServiceOperation::Insert => SqlLogOperation::Insert,
            teaql_data_service::DataServiceOperation::Update => SqlLogOperation::Update,
            teaql_data_service::DataServiceOperation::Delete => SqlLogOperation::Delete,
            teaql_data_service::DataServiceOperation::Recover => SqlLogOperation::Update,
            teaql_data_service::DataServiceOperation::Batch => SqlLogOperation::Update,
            teaql_data_service::DataServiceOperation::Schema => SqlLogOperation::Update,
        };
        if !self.sql_log_options.enabled_for(operation) {
            return;
        }
        let trace_path =
            canonical_sql_trace_path(operation, &metadata.backend, &metadata.trace_chain);
        let result_summary = metadata
            .result_count
            .map(|count| format!("{count} rows returned"))
            .or_else(|| {
                metadata
                    .affected_rows
                    .map(|affected| format!("{affected} rows affected"))
            })
            .unwrap_or_default();
        let debug_sql = metadata.debug_query.as_deref().unwrap_or_default();
        let sensitive_entry = SqlLogEntry {
            operation,
            comment: trace_value(&metadata.trace_chain, teaql_core::TraceKind::Comment)
                .or_else(|| metadata.comment.clone()),
            purpose: trace_value(&metadata.trace_chain, teaql_core::TraceKind::Purpose),
            audit_reason: trace_value(&metadata.trace_chain, teaql_core::TraceKind::AuditReason),
            trace_path: trace_path.clone(),
            sql: metadata.parameterized_query.clone().unwrap_or_default(),
            params: metadata.params.clone(),
            pretty_sql: pretty_sql(debug_sql),
            debug_sql: debug_sql.to_owned(),
            started_at: metadata.started_at,
            ended_at: metadata.ended_at,
            elapsed: metadata
                .ended_at
                .duration_since(metadata.started_at)
                .unwrap_or_default(),
            result_count: metadata.result_count,
            result_type: None,
            affected_rows: metadata.affected_rows,
            result_summary,
        };
        // The ordinary context buffer and default operator log are safe
        // telemetry. Values and copy-paste SQL are only sent to an explicitly
        // configured diagnostic sink, never retained in this buffer.
        let mut safe_entry = sensitive_entry.clone();
        // Preserve only the non-sensitive shape. Clearing the vector used to
        // make a parameterized query indistinguishable from a literal-only
        // query, while retaining the values would leak customer data.
        safe_entry
            .params
            .iter_mut()
            .for_each(|value| *value = Value::Null);
        safe_entry.debug_sql.clear();
        safe_entry.pretty_sql.clear();
        self.append_sql_log(metadata.started_at, trace_path, safe_entry, sensitive_entry);
    }

    fn append_sql_log(
        &self,
        timestamp: SystemTime,
        trace_path: Vec<teaql_core::TraceNode>,
        safe_entry: SqlLogEntry,
        sensitive_entry: SqlLogEntry,
    ) {
        if let Ok(mut entries) = self.sql_log_entries.lock() {
            entries.push(safe_entry.clone());
        }
        if let Some(buffer) = self.get_resource::<UnifiedLogBuffer>()
            && let Ok(mut entries) = buffer.entries.lock()
        {
            entries.push(UnifiedLogEntry {
                timestamp,
                user_identifier: self.user_identifier.clone(),
                trace_chain: trace_path.clone(),
                payload: LogPayload::Sql(safe_entry.clone()),
            });
        }
        crate::log_formatter::LogManager::write_sql_log(&trace_path, &safe_entry);
        crate::log_formatter::LogManager::write_sensitive_sql_log(&trace_path, &sensitive_entry);
    }
}

fn trace_value(
    trace_path: &[teaql_core::TraceNode],
    kind: teaql_core::TraceKind,
) -> Option<String> {
    trace_path
        .iter()
        .rev()
        .find(|node| node.kind == kind)
        .map(|node| node.comment.clone())
}

fn canonical_sql_trace_path(
    operation: SqlLogOperation,
    backend: &str,
    source: &[teaql_core::TraceNode],
) -> Vec<teaql_core::TraceNode> {
    use teaql_core::{TraceKind, TraceNode};

    if source.iter().any(|node| node.kind == TraceKind::Operation)
        && source.iter().any(|node| node.kind == TraceKind::Provider)
        && source.iter().any(|node| node.kind == TraceKind::Sql)
    {
        return source
            .iter()
            .filter(|node| {
                !matches!(
                    node.kind,
                    TraceKind::Comment | TraceKind::Purpose | TraceKind::AuditReason
                )
            })
            .cloned()
            .collect();
    }
    let operation_entity = source
        .iter()
        .find(|node| !node.entity_type.trim().is_empty())
        .map(|node| node.entity_type.clone())
        .unwrap_or_else(|| "unknown".to_owned());
    let statement_entity = if operation.is_select() {
        operation_entity.clone()
    } else {
        source
            .iter()
            .rev()
            .find(|node| node.kind == TraceKind::Entity && !node.entity_type.trim().is_empty())
            .map(|node| node.entity_type.clone())
            .unwrap_or_else(|| operation_entity.clone())
    };
    let family = if operation.is_select() {
        "query"
    } else {
        "mutation"
    };
    let statement = match operation {
        SqlLogOperation::Select => "select",
        SqlLogOperation::Insert => "insert",
        SqlLogOperation::Update => "update",
        SqlLogOperation::Delete => "delete",
        SqlLogOperation::Recover => "recover",
    };
    let mut path = vec![TraceNode::typed(
        TraceKind::Operation,
        operation_entity,
        None,
        family,
    )];
    path.push(TraceNode::typed(
        if operation.is_select() {
            TraceKind::Request
        } else {
            TraceKind::Entity
        },
        statement_entity,
        None,
        "",
    ));
    path.extend(
        source
            .iter()
            .filter(|node| node.kind == TraceKind::Relation)
            .cloned(),
    );
    path.push(TraceNode::typed(
        TraceKind::Provider,
        if backend.trim().is_empty() {
            "unknown"
        } else {
            backend
        },
        None,
        "",
    ));
    path.push(TraceNode::typed(TraceKind::Sql, statement, None, ""));
    path
}

fn pretty_sql(sql: &str) -> String {
    let mut pretty = sql.to_owned();
    for keyword in [
        " FROM ",
        " WHERE ",
        " GROUP BY ",
        " HAVING ",
        " ORDER BY ",
        " LIMIT ",
        " OFFSET ",
        " RETURNING ",
    ] {
        pretty = pretty.replace(keyword, &format!("\n{}", keyword.trim_start()));
    }
    pretty.replace(" AND ", "\n  AND ")
}
