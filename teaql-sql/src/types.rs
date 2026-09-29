use teaql_core::{DataType, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseKind {
    PostgreSql,
    Sqlite,
    MySql,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledQuery {
    pub log_context: teaql_data_service::SqlLogContext,
    pub sql: String,
    pub params: Vec<Value>,
    pub comment: Option<String>,
}

impl CompiledQuery {
    pub fn sql_with_comment(&self) -> String {
        match &self.comment {
            Some(comment) if !comment.is_empty() => {
                let mut sql = String::with_capacity(comment.len() + self.sql.len() + 7);
                sql.push_str("/* ");
                if comment.contains("*/") {
                    sql.push_str(&comment.replace("*/", "* /"));
                } else {
                    sql.push_str(comment);
                }
                sql.push_str(" */ ");
                sql.push_str(&self.sql);
                sql
            }
            _ => self.sql.clone(),
        }
    }

    pub fn debug_sql(&self, kind: DatabaseKind) -> String {
        render_sql_with(&self.sql_with_comment(), kind, self.params.len(), |index| {
            render_sql_value(&self.params[index], kind)
        })
        .unwrap_or_else(|reason| format!("[SQL omitted: {reason}; NOT REPLAYABLE]"))
    }
}

/// Render already-projected bindings. The callback must never receive template
/// text or derive policy from a parameter name; policy belongs to the compiler.
/// Strict accounting prevents an incomplete rendering from looking replayable.
pub fn render_sql_with(
    sql: &str,
    kind: DatabaseKind,
    parameter_count: usize,
    mut literal: impl FnMut(usize) -> Result<String, &'static str>,
) -> Result<String, &'static str> {
    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len());
    let mut used = vec![false; parameter_count];
    let mut positional = 0;
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        match bytes[i] {
            b'\'' | b'"' | b'`' | b'[' => {
                let open = bytes[i];
                if open == b'[' && kind != DatabaseKind::Sqlite {
                    output.push('[');
                    i += 1;
                    continue;
                }
                if open == b'\''
                    && kind == DatabaseKind::PostgreSql
                    && i > 0
                    && matches!(bytes[i - 1], b'e' | b'E')
                {
                    return Err("unsupported escape string");
                }
                let close = if open == b'[' { b']' } else { open };
                i += 1;
                loop {
                    if i >= bytes.len() {
                        return Err("unterminated quoted SQL");
                    }
                    if bytes[i] == b'\\' && kind == DatabaseKind::MySql {
                        return Err("ambiguous MySQL backslash quoting");
                    }
                    if bytes[i] == close {
                        i += 1;
                        if i < bytes.len() && bytes[i] == close && open != b'[' {
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                output.push_str(&sql[start..i]);
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i += 2;
                while i < bytes.len() && !matches!(bytes[i], b'\n' | b'\r') {
                    i += 1;
                }
                output.push_str(&sql[start..i]);
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                if kind == DatabaseKind::MySql && matches!(bytes.get(i + 2), Some(b'!' | b'+')) {
                    return Err("unsupported executable SQL comment");
                }
                i += 2;
                let mut depth = 1;
                while i < bytes.len() && depth > 0 {
                    if bytes[i..].starts_with(b"/*") {
                        depth += 1;
                        i += 2;
                    } else if bytes[i..].starts_with(b"*/") {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                if depth != 0 {
                    return Err("unterminated SQL comment");
                }
                output.push_str(&sql[start..i]);
            }
            b'#' if kind == DatabaseKind::MySql => return Err("unsupported MySQL hash comment"),
            b'$' if kind == DatabaseKind::PostgreSql => {
                i += 1;
                if bytes.get(i).is_some_and(u8::is_ascii_digit) {
                    while bytes.get(i).is_some_and(u8::is_ascii_digit) {
                        i += 1;
                    }
                    let index = sql[start + 1..i]
                        .parse::<usize>()
                        .ok()
                        .and_then(|v| v.checked_sub(1))
                        .ok_or("invalid binding index")?;
                    if index >= parameter_count {
                        return Err("missing binding");
                    }
                    output.push_str(&literal(index)?);
                    used[index] = true;
                } else {
                    // PostgreSQL dollar-quoted bodies may contain fake bindings.
                    while bytes
                        .get(i)
                        .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
                    {
                        i += 1;
                    }
                    if bytes.get(i) != Some(&b'$') {
                        return Err("unsupported dollar expression");
                    }
                    i += 1;
                    let delimiter = &sql[start..i];
                    let end = sql[i..]
                        .find(delimiter)
                        .ok_or("unterminated dollar quote")?;
                    i += end + delimiter.len();
                    output.push_str(&sql[start..i]);
                }
            }
            b'?' if kind != DatabaseKind::PostgreSql => {
                if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    return Err("unsupported numbered positional binding");
                }
                if positional >= parameter_count {
                    return Err("missing binding");
                }
                output.push_str(&literal(positional)?);
                used[positional] = true;
                positional += 1;
                i += 1;
            }
            b':' | b'@' | b'$' if kind == DatabaseKind::Sqlite => {
                return Err("unsupported named binding");
            }
            _ => {
                let ch = sql[i..].chars().next().expect("valid character boundary");
                output.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    if used.iter().any(|used| !used) {
        return Err("unused binding");
    }
    Ok(output)
}

/// Format a safe value using the same dialect literal rules as debug SQL.
pub fn render_sql_value(value: &Value, kind: DatabaseKind) -> Result<String, &'static str> {
    fn finite(value: &Value) -> bool {
        match value {
            Value::F64(v) => v.is_finite(),
            Value::List(values) => values.iter().all(finite),
            _ => true,
        }
    }
    if !finite(value) {
        return Err("non-finite SQL number");
    }
    // Do not assume MySQL NO_BACKSLASH_ESCAPES. Fail closed instead of
    // displaying an executable-looking but semantically different literal.
    let literal = sql_literal(value, kind);
    if kind == DatabaseKind::MySql && literal.contains('\\') {
        return Err("ambiguous MySQL backslash literal");
    }
    Ok(literal)
}

fn sql_bool_literal(value: bool) -> &'static str {
    match value {
        true => "TRUE",
        false => "FALSE",
    }
}

fn sql_literal(value: &Value, kind: DatabaseKind) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Bool(value) => sql_bool_literal(*value).to_owned(),
        Value::I64(value) => value.to_string(),
        Value::U64(value) => value.to_string(),
        Value::F64(value) => value.to_string(),
        Value::Decimal(value) => value.to_string(),
        Value::Text(value) => quoted_sql_string(value),
        Value::Json(value) => quoted_sql_string(&value.to_string()),
        Value::Date(value) => match kind {
            DatabaseKind::PostgreSql => format!("DATE '{}'", value),
            DatabaseKind::MySql => format!("CAST('{}' AS DATE)", value),
            DatabaseKind::Sqlite => quoted_sql_string(&value.to_string()),
        },
        Value::Timestamp(value) => match kind {
            DatabaseKind::Sqlite => value.0.to_string(),
            DatabaseKind::PostgreSql => format!(
                "TIMESTAMPTZ '{}'",
                value.to_datetime().format("%Y-%m-%d %H:%M:%S%.3fZ")
            ),
            DatabaseKind::MySql => format!(
                "CAST('{}' AS DATETIME(3))",
                value
                    .to_datetime()
                    .naive_utc()
                    .format("%Y-%m-%d %H:%M:%S%.3f")
            ),
        },
        Value::Object(value) => {
            quoted_sql_string(&Value::Object(value.clone()).to_json_value().to_string())
        }
        Value::List(values) => {
            let values = values
                .iter()
                .map(|v| sql_literal(v, kind))
                .collect::<Vec<_>>()
                .join(", ");
            match kind {
                DatabaseKind::PostgreSql => format!("ARRAY[{values}]"),
                _ => format!("({values})"),
            }
        }
        Value::TypedNull(_) => "NULL".to_owned(),
    }
}

fn quoted_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCompileError {
    UnknownEntity(String),
    UnknownField(String),
    EmptyInList,
    MissingIdProperty(String),
    MissingVersionProperty(String),
    EmptyMutation(String),
    InvalidRecoverVersion(i64),
    UnsupportedSchemaType(DataType),
    InvalidSchemaShape {
        property: String,
        reason: String,
    },
    SchemaIdentifierTooLong {
        object: String,
        identifier: String,
        actual_bytes: usize,
        max_bytes: usize,
    },
    InvalidFunctionArguments(String),
    InvalidSubQueryOperator(String),
}

impl std::fmt::Display for SqlCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownEntity(entity) => write!(f, "unknown entity: {entity}"),
            Self::UnknownField(field) => write!(f, "unknown field: {field}"),
            Self::EmptyInList => write!(f, "IN requires at least one value"),
            Self::MissingIdProperty(entity) => write!(f, "entity {entity} has no id property"),
            Self::MissingVersionProperty(entity) => {
                write!(f, "entity {entity} has no version property")
            }
            Self::EmptyMutation(kind) => write!(f, "{kind} requires at least one writable field"),
            Self::InvalidRecoverVersion(version) => {
                write!(f, "recover requires a negative version, got {version}")
            }
            Self::UnsupportedSchemaType(data_type) => {
                write!(f, "unsupported schema type: {data_type:?}")
            }
            Self::InvalidSchemaShape { property, reason } => {
                write!(f, "invalid schema shape for property {property}: {reason}")
            }
            Self::SchemaIdentifierTooLong {
                object,
                identifier,
                actual_bytes,
                max_bytes,
            } => write!(
                f,
                "{object} SQL identifier {identifier:?} is {actual_bytes} bytes; this database allows at most {max_bytes} bytes. Shorten the model-derived table or column name before ensure_schema"
            ),
            Self::InvalidFunctionArguments(message) => write!(f, "{message}"),
            Self::InvalidSubQueryOperator(operator) => {
                write!(f, "subquery does not support operator: {operator}")
            }
        }
    }
}

impl std::error::Error for SqlCompileError {}
