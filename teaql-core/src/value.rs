use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::{DateTime, NaiveDate, Utc};
pub use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Bool,
    I64,
    U64,
    F64,
    Decimal,
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
    TypedNull(DataType),
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

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::I64(i64::from(value))
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Self::I64(i64::from(value))
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self::U64(u64::from(value))
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Self::U64(u64::from(value))
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Self::F64(f64::from(value))
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

    pub fn try_i64(&self) -> Option<i64> {
        match self {
            Self::I64(value) => Some(*value),
            Self::U64(value) => i64::try_from(*value).ok(),
            Self::Decimal(value) => value.to_i64(),
            _ => None,
        }
    }

    pub fn try_u64(&self) -> Option<u64> {
        match self {
            Self::U64(value) => Some(*value),
            Self::I64(value) => u64::try_from(*value).ok(),
            Self::Decimal(value) => value.to_u64(),
            _ => None,
        }
    }

    pub fn try_decimal(&self) -> Option<Decimal> {
        match self {
            Self::Decimal(value) => Some(*value),
            Self::I64(value) => Some(Decimal::from(*value)),
            Self::U64(value) => Some(Decimal::from(*value)),
            Self::Text(value) => Decimal::from_str(value).ok(),
            _ => None,
        }
    }

    pub fn try_f64(&self) -> Option<f64> {
        match self {
            Self::F64(value) => Some(*value),
            Self::I64(value) => Some(*value as f64),
            Self::U64(value) => Some(*value as f64),
            Self::Decimal(value) => value.to_f64(),
            _ => None,
        }
    }

    pub fn try_text(&self) -> Option<&str> {
        match self {
            Self::Text(value) => Some(value),
            _ => None,
        }
    }

    pub fn try_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn try_date(&self) -> Option<NaiveDate> {
        match self {
            Self::Date(value) => Some(*value),
            Self::Text(value) => {
                if let Ok(nd) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                    return Some(nd);
                }
                None
            }
            _ => None,
        }
    }

    pub fn try_timestamp(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Timestamp(value) => Some(*value),
            Self::Text(value) => {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
                    return Some(dt.with_timezone(&chrono::Utc));
                }
                if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
                    return Some(chrono::DateTime::from_naive_utc_and_offset(
                        ndt,
                        chrono::Utc,
                    ));
                }
                if let Ok(nd) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                    let ndt = nd.and_hms_opt(0, 0, 0)?;
                    return Some(chrono::DateTime::from_naive_utc_and_offset(
                        ndt,
                        chrono::Utc,
                    ));
                }
                None
            }
            _ => None,
        }
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
            Self::TypedNull(_) => serde_json::Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_try_i64_accepts_representable_numeric_variants() {
        assert_eq!(Value::I64(i64::MIN).try_i64(), Some(i64::MIN));
        assert_eq!(Value::I64(i64::MAX).try_i64(), Some(i64::MAX));
        assert_eq!(Value::U64(i64::MAX as u64).try_i64(), Some(i64::MAX));
        assert_eq!(Value::Decimal(Decimal::from(-42)).try_i64(), Some(-42));
    }

    #[test]
    fn value_try_i64_rejects_unsigned_overflow_and_unrelated_variants() {
        assert_eq!(Value::U64(i64::MAX as u64 + 1).try_i64(), None);
        assert_eq!(Value::U64(u64::MAX).try_i64(), None);
        assert_eq!(Value::F64(42.0).try_i64(), None);
        assert_eq!(Value::Text("42".to_owned()).try_i64(), None);
        assert_eq!(Value::Null.try_i64(), None);
    }

    #[test]
    fn value_try_u64_accepts_representable_numeric_variants() {
        assert_eq!(Value::U64(0).try_u64(), Some(0));
        assert_eq!(Value::U64(u64::MAX).try_u64(), Some(u64::MAX));
        assert_eq!(Value::I64(i64::MAX).try_u64(), Some(i64::MAX as u64));
        assert_eq!(Value::Decimal(Decimal::from(42)).try_u64(), Some(42));
    }

    #[test]
    fn value_try_u64_rejects_negative_and_unrelated_variants() {
        assert_eq!(Value::I64(-1).try_u64(), None);
        assert_eq!(Value::Decimal(Decimal::from(-1)).try_u64(), None);
        assert_eq!(Value::F64(42.0).try_u64(), None);
        assert_eq!(Value::Text("42".to_owned()).try_u64(), None);
        assert_eq!(Value::Null.try_u64(), None);
    }

    #[test]
    fn value_try_decimal_accepts_decimal_integer_and_text_variants() {
        let decimal = Decimal::from_str("123.450").expect("valid decimal");

        assert_eq!(Value::Decimal(decimal).try_decimal(), Some(decimal));
        assert_eq!(
            Value::I64(i64::MIN).try_decimal(),
            Some(Decimal::from(i64::MIN))
        );
        assert_eq!(
            Value::U64(u64::MAX).try_decimal(),
            Some(Decimal::from(u64::MAX))
        );
        assert_eq!(
            Value::Text("123.450".to_owned()).try_decimal(),
            Some(decimal)
        );
    }

    #[test]
    fn value_try_decimal_rejects_invalid_text_and_unrelated_variants() {
        assert_eq!(Value::Text("not-a-decimal".to_owned()).try_decimal(), None);
        assert_eq!(Value::Bool(true).try_decimal(), None);
        assert_eq!(Value::F64(1.5).try_decimal(), None);
        assert_eq!(Value::Null.try_decimal(), None);
    }

    #[test]
    fn value_try_f64_accepts_supported_numeric_variants() {
        assert_eq!(Value::F64(1.25).try_f64(), Some(1.25));
        assert_eq!(Value::I64(-2).try_f64(), Some(-2.0));
        assert_eq!(Value::U64(2).try_f64(), Some(2.0));
        assert_eq!(
            Value::Decimal(Decimal::from_str("1.5").expect("valid decimal")).try_f64(),
            Some(1.5)
        );
    }

    #[test]
    fn value_try_f64_rejects_unrelated_variants() {
        assert_eq!(Value::Text("1.5".to_owned()).try_f64(), None);
        assert_eq!(Value::Bool(true).try_f64(), None);
        assert_eq!(Value::Null.try_f64(), None);
    }

    #[test]
    fn value_try_date_accepts_date_and_iso_date_text() {
        let leap_day = NaiveDate::from_ymd_opt(2024, 2, 29).expect("valid leap day");

        assert_eq!(Value::Date(leap_day).try_date(), Some(leap_day));
        assert_eq!(
            Value::Text("2024-02-29".to_owned()).try_date(),
            Some(leap_day)
        );
    }

    #[test]
    fn value_try_date_rejects_invalid_dates_and_unrelated_variants() {
        assert_eq!(Value::Text("2023-02-29".to_owned()).try_date(), None);
        assert_eq!(
            Value::Text("2024-02-29T00:00:00Z".to_owned()).try_date(),
            None
        );
        assert_eq!(Value::I64(20240229).try_date(), None);
        assert_eq!(Value::Null.try_date(), None);
    }

    #[test]
    fn value_try_timestamp_accepts_timestamp_and_supported_text_formats() {
        let utc_timestamp = DateTime::parse_from_rfc3339("2024-01-02T03:04:05Z")
            .expect("valid RFC 3339 timestamp")
            .with_timezone(&Utc);
        let offset_timestamp = DateTime::parse_from_rfc3339("2024-01-02T03:04:05+08:00")
            .expect("valid RFC 3339 timestamp")
            .with_timezone(&Utc);
        let naive_timestamp = NaiveDate::from_ymd_opt(2024, 1, 2)
            .expect("valid date")
            .and_hms_opt(3, 4, 5)
            .expect("valid time");
        let midnight = NaiveDate::from_ymd_opt(2024, 1, 2)
            .expect("valid date")
            .and_hms_opt(0, 0, 0)
            .expect("valid time");

        assert_eq!(
            Value::Timestamp(utc_timestamp).try_timestamp(),
            Some(utc_timestamp)
        );
        assert_eq!(
            Value::Text("2024-01-02T03:04:05+08:00".to_owned()).try_timestamp(),
            Some(offset_timestamp)
        );
        assert_eq!(
            Value::Text("2024-01-02 03:04:05".to_owned()).try_timestamp(),
            Some(DateTime::from_naive_utc_and_offset(naive_timestamp, Utc))
        );
        assert_eq!(
            Value::Text("2024-01-02".to_owned()).try_timestamp(),
            Some(DateTime::from_naive_utc_and_offset(midnight, Utc))
        );
    }

    #[test]
    fn value_try_timestamp_normalizes_offsets_and_rejects_invalid_input() {
        let expected_utc = DateTime::parse_from_rfc3339("2024-01-01T19:04:05Z")
            .expect("valid RFC 3339 timestamp")
            .with_timezone(&Utc);

        assert_eq!(
            Value::Text("2024-01-02T03:04:05+08:00".to_owned()).try_timestamp(),
            Some(expected_utc)
        );
        assert_eq!(
            Value::Text("2024-13-40 25:61:61".to_owned()).try_timestamp(),
            None
        );
        assert_eq!(Value::Bool(true).try_timestamp(), None);
        assert_eq!(Value::Null.try_timestamp(), None);
    }
}
