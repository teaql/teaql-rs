use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
pub use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Bool,
    I64,
    U64,
    F64,
    Text,
    Json,
    Date,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Decimal(Decimal),
    Text(String),
    Json(serde_json::Value),
    Date(NaiveDate),
    Timestamp(DateTime<Utc>),
    Object(BTreeMap<String, Value>),
    List(Vec<Value>),
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<Decimal> for Value {
    fn from(value: Decimal) -> Self {
        Self::Decimal(value)
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Self {
        Self::Json(value)
    }
}

impl From<NaiveDate> for Value {
    fn from(value: NaiveDate) -> Self {
        Self::Date(value)
    }
}

impl From<DateTime<Utc>> for Value {
    fn from(value: DateTime<Utc>) -> Self {
        Self::Timestamp(value)
    }
}

impl Value {
    pub fn object(record: crate::Record) -> Self {
        Self::Object(record)
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => serde_json::Value::Bool(*value),
            Self::I64(value) => serde_json::Value::from(*value),
            Self::U64(value) => serde_json::Value::from(*value),
            Self::F64(value) => serde_json::Number::from_f64(*value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Self::Decimal(value) => serde_json::Value::String(value.to_string()),
            Self::Text(value) => serde_json::Value::String(value.clone()),
            Self::Json(value) => value.clone(),
            Self::Date(value) => serde_json::Value::String(value.to_string()),
            Self::Timestamp(value) => serde_json::Value::String(value.to_rfc3339()),
            Self::Object(record) => crate::record_to_json_value(record),
            Self::List(values) => {
                serde_json::Value::Array(values.iter().map(Value::to_json_value).collect())
            }
        }
    }
}
