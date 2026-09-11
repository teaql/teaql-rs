use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use teaql_core::Value;
use teaql_sql::{CompiledQuery, DatabaseKind};

use super::UserContext;

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

#[derive(Debug, Clone, PartialEq)]
pub struct UnifiedLogEntry {
    pub timestamp: SystemTime,
    pub user_identifier: Option<String>,
    pub trace_chain: Vec<teaql_core::TraceNode>,
    pub payload: LogPayload,
}

#[derive(Debug, Clone, PartialEq)]
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_sql_log(
        &self,
        operation: SqlLogOperation,
        query: &CompiledQuery,
        database_kind: DatabaseKind,
        started_at: SystemTime,
        ended_at: SystemTime,
        elapsed: Duration,
        result_count: Option<usize>,
        result_type: Option<String>,
        affected_rows: Option<u64>,
        trace_chain: Vec<teaql_core::TraceNode>,
    ) {
        if !self.sql_log_options.enabled_for(operation) {
            return;
        }
        let debug_sql = query.debug_sql(database_kind);
        let result_summary = sql_result_summary(
            operation,
            result_count,
            result_type.as_deref(),
            affected_rows,
            &debug_sql,
        );
        let trace_path = canonical_sql_trace_path(
            operation,
            &format!("{database_kind:?}").to_ascii_lowercase(),
            &trace_chain,
        );
        let entry = SqlLogEntry {
            operation,
            comment: trace_value(&trace_chain, teaql_core::TraceKind::Comment),
            purpose: trace_value(&trace_chain, teaql_core::TraceKind::Purpose),
            audit_reason: trace_value(&trace_chain, teaql_core::TraceKind::AuditReason),
            trace_path: trace_path.clone(),
            sql: query.sql.clone(),
            params: query.params.clone(),
            pretty_sql: pretty_sql(&debug_sql),
            debug_sql,
            started_at,
            ended_at,
            elapsed,
            result_summary,
            result_count,
            result_type,
            affected_rows,
        };
        self.append_sql_log(started_at, trace_path, entry);
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
        let Some(debug_sql) = &metadata.debug_query else {
            return;
        };
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
        let entry = SqlLogEntry {
            operation,
            comment: trace_value(&metadata.trace_chain, teaql_core::TraceKind::Comment)
                .or_else(|| metadata.comment.clone()),
            purpose: trace_value(&metadata.trace_chain, teaql_core::TraceKind::Purpose),
            audit_reason: trace_value(&metadata.trace_chain, teaql_core::TraceKind::AuditReason),
            trace_path: trace_path.clone(),
            sql: metadata.parameterized_query.clone().unwrap_or_default(),
            params: metadata.params.clone(),
            pretty_sql: pretty_sql(debug_sql),
            debug_sql: debug_sql.clone(),
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
        self.append_sql_log(metadata.started_at, trace_path, entry);
    }

    fn append_sql_log(
        &self,
        timestamp: SystemTime,
        trace_path: Vec<teaql_core::TraceNode>,
        entry: SqlLogEntry,
    ) {
        if let Ok(mut entries) = self.sql_log_entries.lock() {
            entries.push(entry.clone());
        }
        if let Some(buffer) = self.get_resource::<UnifiedLogBuffer>() {
            if let Ok(mut entries) = buffer.entries.lock() {
                entries.push(UnifiedLogEntry {
                    timestamp,
                    user_identifier: self.user_identifier.clone(),
                    trace_chain: trace_path.clone(),
                    payload: LogPayload::Sql(entry.clone()),
                });
            }
        }
        crate::log_formatter::LogManager::write_sql_log(&trace_path, &entry);
    }
}

fn extract_id_from_sql(sql: &str) -> Option<String> {
    let sql_lower = sql.to_lowercase();
    let where_clause = &sql_lower[sql_lower.find("where")? + 5..];
    let bytes = where_clause.as_bytes();
    let mut index = 0;
    while index + 1 < bytes.len() {
        if &bytes[index..index + 2] == b"id" {
            let before_is_boundary = index == 0 || {
                let previous = bytes[index - 1] as char;
                !previous.is_ascii_alphanumeric() && previous != '_' && previous != '.'
            };
            let after_is_boundary = index + 2 == bytes.len() || {
                let next = bytes[index + 2] as char;
                !next.is_ascii_alphanumeric() && next != '_'
            };
            if before_is_boundary && after_is_boundary {
                let mut value_index = index + 2;
                while value_index < bytes.len() && (bytes[value_index] as char).is_whitespace() {
                    value_index += 1;
                }
                if value_index < bytes.len() && bytes[value_index] == b'=' {
                    value_index += 1;
                    while value_index < bytes.len() && (bytes[value_index] as char).is_whitespace()
                    {
                        value_index += 1;
                    }
                    let quoted = value_index < bytes.len() && bytes[value_index] == b'\'';
                    if quoted {
                        value_index += 1;
                    }
                    let mut value = String::new();
                    while value_index < bytes.len() {
                        let character = bytes[value_index] as char;
                        if (quoted && character == '\'')
                            || (!quoted
                                && !character.is_ascii_alphanumeric()
                                && character != '_'
                                && character != '-')
                        {
                            break;
                        }
                        value.push(character);
                        value_index += 1;
                    }
                    if !value.is_empty() {
                        return Some(value);
                    }
                }
            }
        }
        index += 1;
    }
    None
}

fn sql_result_summary(
    operation: SqlLogOperation,
    result_count: Option<usize>,
    result_type: Option<&str>,
    affected_rows: Option<u64>,
    debug_sql: &str,
) -> String {
    match operation {
        SqlLogOperation::Select => match result_count.unwrap_or(0) {
            0 => "MISS".to_owned(),
            1 => result_type
                .map(|result_type| {
                    extract_id_from_sql(debug_sql)
                        .map(|id| format!("{result_type}({id})"))
                        .unwrap_or_else(|| result_type.to_owned())
                })
                .unwrap_or_else(|| "row".to_owned()),
            count => result_type
                .map(|result_type| format!("{count}*{result_type}"))
                .unwrap_or_else(|| format!("{count}*rows")),
        },
        _ => format!("{} UPDATED", affected_rows.unwrap_or(0)),
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
    let entity = source
        .iter()
        .find(|node| !node.entity_type.trim().is_empty())
        .map(|node| node.entity_type.clone())
        .unwrap_or_else(|| "unknown".to_owned());
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
        entity.clone(),
        None,
        family,
    )];
    path.push(TraceNode::typed(
        if operation.is_select() {
            TraceKind::Request
        } else {
            TraceKind::Entity
        },
        entity,
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
