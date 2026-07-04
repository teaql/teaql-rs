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
        match kind {
            DatabaseKind::PostgreSql => replace_postgres_placeholders(&self.sql, &self.params),
            DatabaseKind::Sqlite => {
                replace_positional_placeholders(&self.sql, &self.params, DatabaseKind::Sqlite)
            }
            DatabaseKind::MySql => {
                replace_positional_placeholders(&self.sql, &self.params, DatabaseKind::MySql)
            }
        }
    }
}

fn handle_sql_quote(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    output: &mut String,
    in_string: &mut bool,
) {
    output.push('\'');
    match *in_string && matches!(chars.peek(), Some('\'')) {
        true => {
            output.push(chars.next().expect("peeked quote must exist"));
        }
        false => {
            *in_string = !*in_string;
        }
    }
}

fn replace_postgres_placeholders(sql: &str, params: &[Value]) -> String {
    let mut output = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_string = false;
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            handle_sql_quote(&mut chars, &mut output, &mut in_string);
            continue;
        }
        if !in_string && ch == '$' && chars.peek().is_some_and(|next| next.is_ascii_digit()) {
            let mut index = String::new();
            while let Some(next) = chars.peek().copied().filter(char::is_ascii_digit) {
                index.push(next);
                chars.next();
            }
            if let Ok(index) = index.parse::<usize>() {
                if let Some(value) = index.checked_sub(1).and_then(|idx| params.get(idx)) {
                    output.push_str(&sql_literal(value, DatabaseKind::PostgreSql));
                    continue;
                }
            }
            output.push('$');
            output.push_str(&index);
            continue;
        }
        output.push(ch);
    }
    output
}

fn replace_positional_placeholders(sql: &str, params: &[Value], kind: DatabaseKind) -> String {
    let mut output = String::with_capacity(sql.len());
    let mut params = params.iter();
    let mut in_string = false;
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            handle_sql_quote(&mut chars, &mut output, &mut in_string);
            continue;
        }
        if !in_string && ch == '?' {
            match params.next() {
                Some(value) => output.push_str(&sql_literal(value, kind)),
                None => output.push(ch),
            }
            continue;
        }
        output.push(ch);
    }
    output
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
        Value::Date(value) => quoted_sql_string(&value.to_string()),
        Value::Timestamp(value) => quoted_sql_string(&value.to_rfc3339()),
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
