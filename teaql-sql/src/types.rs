use teaql_core::{DataType, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseKind {
    PostgreSql,
    Sqlite,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledQuery {
    pub sql: String,
    pub params: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCompileError {
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
