use teaql_core::{DataType, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseKind {
    PostgreSql,
    Sqlite,
    MySql,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledQuery {
    pub sql: String,
    pub params: Vec<Value>,
    pub comment: Option<String>,
}

impl CompiledQuery {
    pub fn sql_with_comment(&self) -> String {
        match &self.comment {
            Some(comment) if !comment.is_empty() => {
                let escaped = comment.replace("*/", "* /");
                format!("/* {escaped} */ {}", self.sql)
            }
            _ => self.sql.clone(),
        }
    }

    pub fn debug_sql(&self, kind: DatabaseKind) -> String {
        let sql = self.sql_with_comment();
        match kind {
            DatabaseKind::PostgreSql => replace_postgres_placeholders(&sql, &self.params),
            DatabaseKind::Sqlite => {
                replace_positional_placeholders(&sql, &self.params, DatabaseKind::Sqlite)
            }
            DatabaseKind::MySql => {
                replace_positional_placeholders(&sql, &self.params, DatabaseKind::MySql)
            }
        }
    }
}

fn replace_postgres_placeholders(sql: &str, params: &[Value]) -> String {
    let mut output = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut state = SqlScanState::Sql;
    while let Some(ch) = chars.next() {
        match state {
            SqlScanState::Sql => match (ch, chars.peek().copied()) {
                ('\'', _) => { output.push(ch); state = SqlScanState::SingleQuote; }
                ('"', _) => { output.push(ch); state = SqlScanState::DoubleQuote; }
                ('-', Some('-')) => {
                    output.push_str("--"); chars.next(); state = SqlScanState::LineComment;
                }
                ('/', Some('*')) => {
                    output.push_str("/*"); chars.next(); state = SqlScanState::BlockComment;
                }
                ('$', Some(next)) if next.is_ascii_digit() => {
                    let mut index = String::new();
                    while let Some(next) = chars.peek().copied().filter(char::is_ascii_digit) {
                        index.push(next); chars.next();
                    }
                    if let Ok(index) = index.parse::<usize>()
                        && let Some(value) = index.checked_sub(1).and_then(|idx| params.get(idx))
                    {
                        output.push_str(&sql_literal(value, DatabaseKind::PostgreSql));
                    } else {
                        output.push('$'); output.push_str(&index);
                    }
                }
                _ => output.push(ch),
            },
            SqlScanState::SingleQuote => {
                output.push(ch);
                if ch == '\'' {
                    if matches!(chars.peek(), Some('\'')) {
                        output.push(chars.next().expect("peeked escaped quote"));
                    } else { state = SqlScanState::Sql; }
                }
            }
            SqlScanState::DoubleQuote => {
                output.push(ch);
                if ch == '"' {
                    if matches!(chars.peek(), Some('"')) {
                        output.push(chars.next().expect("peeked escaped identifier"));
                    } else { state = SqlScanState::Sql; }
                }
            }
            SqlScanState::LineComment => {
                output.push(ch);
                if matches!(ch, '\r' | '\n') { state = SqlScanState::Sql; }
            }
            SqlScanState::BlockComment => {
                output.push(ch);
                if ch == '*' && matches!(chars.peek(), Some('/')) {
                    output.push(chars.next().expect("peeked comment end"));
                    state = SqlScanState::Sql;
                }
            }
        }
    }
    output
}

fn replace_positional_placeholders(sql: &str, params: &[Value], kind: DatabaseKind) -> String {
    let mut output = String::with_capacity(sql.len());
    let mut params = params.iter();
    let mut state = SqlScanState::Sql;
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        match state {
            SqlScanState::Sql => match (ch, chars.peek().copied()) {
                ('\'', _) => {
                    output.push(ch);
                    state = SqlScanState::SingleQuote;
                }
                ('"', _) => {
                    output.push(ch);
                    state = SqlScanState::DoubleQuote;
                }
                ('-', Some('-')) => {
                    output.push(ch);
                    output.push(chars.next().expect("peeked line comment"));
                    state = SqlScanState::LineComment;
                }
                ('/', Some('*')) => {
                    output.push(ch);
                    output.push(chars.next().expect("peeked block comment"));
                    state = SqlScanState::BlockComment;
                }
                ('?', _) => match params.next() {
                    Some(value) => output.push_str(&sql_literal(value, kind)),
                    None => output.push(ch),
                },
                _ => output.push(ch),
            },
            SqlScanState::SingleQuote => {
                output.push(ch);
                if ch == '\'' {
                    if matches!(chars.peek(), Some('\'')) {
                        output.push(chars.next().expect("peeked escaped quote"));
                    } else {
                        state = SqlScanState::Sql;
                    }
                }
            }
            SqlScanState::DoubleQuote => {
                output.push(ch);
                if ch == '"' {
                    if matches!(chars.peek(), Some('"')) {
                        output.push(chars.next().expect("peeked escaped identifier quote"));
                    } else {
                        state = SqlScanState::Sql;
                    }
                }
            }
            SqlScanState::LineComment => {
                output.push(ch);
                if matches!(ch, '\r' | '\n') {
                    state = SqlScanState::Sql;
                }
            }
            SqlScanState::BlockComment => {
                output.push(ch);
                if ch == '*' && matches!(chars.peek(), Some('/')) {
                    output.push(chars.next().expect("peeked block comment end"));
                    state = SqlScanState::Sql;
                }
            }
        }
    }
    output
}

#[derive(Clone, Copy)]
enum SqlScanState {
    Sql,
    SingleQuote,
    DoubleQuote,
    LineComment,
    BlockComment,
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
                value.to_datetime().naive_utc().format("%Y-%m-%d %H:%M:%S%.3f")
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
            Self::InvalidFunctionArguments(message) => write!(f, "{message}"),
            Self::InvalidSubQueryOperator(operator) => {
                write!(f, "subquery does not support operator: {operator}")
            }
        }
    }
}

impl std::error::Error for SqlCompileError {}
