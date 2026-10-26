use super::{Error, Field, Kind, failure, input::Node};
use crate::ObjectLocation;
use chrono::{Datelike, Timelike};
use serde_json::{Number, Value as Json};
use teaql_core::{Decimal, Value};
const SAFE: u64 = 9_007_199_254_740_991;
fn decimal_integer(text: &str, signed: bool) -> bool {
    let digits = if signed {
        text.strip_prefix('-').unwrap_or(text)
    } else {
        text
    };
    !digits.is_empty()
        && digits.bytes().all(|v| v.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'))
}
fn plain_decimal(text: &str) -> bool {
    let text = text.strip_prefix('-').unwrap_or(text);
    let mut pieces = text.split('.');
    let integer = pieces.next().unwrap_or("");
    decimal_integer(integer, false)
        && pieces.next().is_none_or(|fraction| {
            !fraction.is_empty() && fraction.bytes().all(|v| v.is_ascii_digit())
        })
        && pieces.next().is_none()
}
pub(super) fn read_id(node: Node) -> Option<u64> {
    match node {
        Node::Text(text) if decimal_integer(&text, false) => text.parse().ok(),
        Node::Number(value) => value.as_u64().filter(|number| *number <= SAFE),
        _ => None,
    }
}
fn read_signed(node: Node) -> Option<i64> {
    match node {
        Node::Text(text) if decimal_integer(&text, true) => text.parse().ok(),
        Node::Number(value) => value
            .as_i64()
            .filter(|number| number.unsigned_abs() <= SAFE),
        _ => None,
    }
}
fn time(text: &str) -> Option<chrono::NaiveTime> {
    if text.len() < 8 || text.as_bytes()[2] != b':' || text.as_bytes()[5] != b':' {
        return None;
    }
    if text.len() != 8 && (text.len() < 10 || text.len() > 18 || text.as_bytes()[8] != b'.') {
        return None;
    }
    if !text
        .bytes()
        .enumerate()
        .all(|(i, v)| matches!(i, 2 | 5 | 8) || v.is_ascii_digit())
    {
        return None;
    }
    chrono::NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
        .ok()
        .filter(|value| value.nanosecond() < 1_000_000_000)
}
fn date(text: &str) -> Option<chrono::NaiveDate> {
    if text.len() != 10
        || !text.bytes().enumerate().all(|(i, v)| {
            if i == 4 || i == 7 {
                v == b'-'
            } else {
                v.is_ascii_digit()
            }
        })
    {
        return None;
    }
    chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .ok()
        .filter(|value| (1..=9999).contains(&value.year()))
}
pub(super) fn read(
    field: &Field,
    node: Node,
    location: &ObjectLocation,
    path: &str,
) -> Result<Value, Error> {
    let bad = || {
        let mut error = failure("WIRE_INVALID_TYPE", location, path);
        error.expected_kind = Some(field.kind.description());
        error
    };
    if matches!(node, Node::Null) {
        return if field.nullable {
            Ok(Value::Null)
        } else {
            Err(bad())
        };
    }
    Ok(match &field.kind {
        Kind::Id => Value::U64(read_id(node).ok_or_else(bad)?),
        Kind::Long | Kind::Version => Value::I64(read_signed(node).ok_or_else(bad)?),
        Kind::Integer => {
            let Node::Number(value) = node else {
                return Err(bad());
            };
            Value::I64(
                value
                    .as_i64()
                    .filter(|n| i32::try_from(*n).is_ok())
                    .ok_or_else(bad)?,
            )
        }
        Kind::Boolean => {
            let Node::Bool(value) = node else {
                return Err(bad());
            };
            Value::Bool(value)
        }
        Kind::Text => {
            let Node::Text(value) = node else {
                return Err(bad());
            };
            Value::Text(value)
        }
        Kind::Decimal => {
            let Node::Text(text) = node else {
                return Err(bad());
            };
            if text.len() > 4096 || !plain_decimal(&text) {
                return Err(bad());
            }
            Value::Decimal(Decimal::from_str_exact(&text).map_err(|_| bad())?)
        }
        Kind::Float | Kind::Double => {
            let Node::Number(number) = node else {
                return Err(bad());
            };
            let value = number.as_f64().filter(|v| v.is_finite()).ok_or_else(bad)?;
            let value = if matches!(field.kind, Kind::Float) {
                let number = value as f32;
                if !number.is_finite() {
                    return Err(bad());
                }
                f64::from(number)
            } else {
                value
            };
            Value::F64(value)
        }
        Kind::Date => {
            let Node::Text(text) = node else {
                return Err(bad());
            };
            Value::Date(date(&text).ok_or_else(bad)?)
        }
        Kind::Time => {
            let Node::Text(text) = node else {
                return Err(bad());
            };
            Value::Text(time(&text).ok_or_else(bad)?.to_string())
        }
        Kind::DateTime => {
            let Node::Text(text) = node else {
                return Err(bad());
            };
            let value = chrono::DateTime::parse_from_rfc3339(&text).map_err(|_| bad())?;
            if !(1..=9999).contains(&value.year())
                || value.nanosecond() >= 1_000_000_000
                || value.nanosecond() % 1_000_000 != 0
            {
                return Err(failure("WIRE_TEMPORAL_PRECISION", location, path));
            }
            Value::Timestamp(teaql_core::time::Timestamp(value.timestamp_millis()))
        }
        Kind::Json => Value::Json(node.into_json(location, path)?),
        Kind::Reference { .. } | Kind::One { .. } | Kind::Many { .. } => return Err(bad()),
    })
}
pub(super) fn write(
    field: &Field,
    value: &Value,
    location: &ObjectLocation,
    path: &str,
) -> Result<Json, Error> {
    let bad = || {
        let mut error = failure("WIRE_INVALID_TYPE", location, path);
        error.expected_kind = Some(field.kind.description());
        error
    };
    if matches!(value, Value::Null) {
        return if field.nullable {
            Ok(Json::Null)
        } else {
            Err(bad())
        };
    }
    Ok(match (&field.kind, value) {
        (Kind::Id, Value::U64(value)) => Json::String(value.to_string()),
        (Kind::Id, Value::I64(value)) if *value >= 0 => Json::String(value.to_string()),
        (Kind::Version | Kind::Long, Value::I64(value)) => Json::String(value.to_string()),
        (Kind::Integer, Value::I64(value)) if i32::try_from(*value).is_ok() => {
            Json::Number((*value).into())
        }
        (Kind::Text, Value::Text(value)) => Json::String(value.clone()),
        (Kind::Boolean, Value::Bool(value)) => Json::Bool(*value),
        (Kind::Decimal, Value::Decimal(value)) => Json::String(value.to_string()),
        (Kind::Float | Kind::Double, Value::F64(value)) if value.is_finite() => {
            if matches!(field.kind, Kind::Float) && f64::from(*value as f32) != *value {
                return Err(bad());
            }
            Json::Number(Number::from_f64(*value).ok_or_else(bad)?)
        }
        (Kind::Date, Value::Date(value)) if (1..=9999).contains(&value.year()) => {
            Json::String(value.to_string())
        }
        (Kind::Time, Value::Text(value)) => Json::String(time(value).ok_or_else(bad)?.to_string()),
        (Kind::DateTime, Value::Timestamp(value)) => {
            let instant = chrono::DateTime::from_timestamp_millis(value.0)
                .filter(|v| (1..=9999).contains(&v.year()))
                .ok_or_else(bad)?;
            Json::String(instant.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        }
        (Kind::Json, Value::Json(value)) => value.clone(),
        _ => return Err(bad()),
    })
}
