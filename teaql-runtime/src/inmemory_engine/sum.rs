use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use teaql_core::{Aggregate, CompactRow, Decimal, Value};

use super::{InMemoryQueryError as Error, InMemoryQueryErrorKind as Kind};

#[derive(Clone, Copy)]
enum Domain {
    Signed,
    Unsigned,
    Decimal,
    Floating,
}

/// Accumulate an exact coefficient, then check the final carrier once. This
/// permits cancellation without order-dependent intermediate Decimal rounding.
/// Decimal's checked_add alone can still reduce scale and round a finite sum.
pub(super) fn exact_sum(aggregate: &Aggregate, rows: &[CompactRow]) -> Result<Value, Error> {
    let mut domain = None;
    let mut coefficient = BigInt::zero();
    let mut scale = 0;
    let mut floating = 0.0_f64;

    for value in rows.iter().filter_map(|row| row.get(&aggregate.field)) {
        if matches!(value, Value::Null) {
            continue;
        }
        if let Value::F64(number) = value {
            if !number.is_finite() {
                return Err(Error::sum(aggregate, Kind::NonFiniteValue));
            }
            match domain {
                None => domain = Some(Domain::Floating),
                Some(Domain::Floating) => {}
                _ => return Err(Error::sum(aggregate, Kind::MixedFloatingAndExact)),
            }
            floating += number;
            if !floating.is_finite() {
                return Err(Error::sum(aggregate, Kind::NumericOverflow));
            }
            continue;
        }
        let (mut next, next_scale, next_domain) = match value {
            Value::I64(number) => (BigInt::from(*number), 0, Domain::Signed),
            Value::U64(number) => (BigInt::from(*number), 0, Domain::Unsigned),
            Value::Decimal(number) => (
                BigInt::from(number.mantissa()),
                number.scale(),
                Domain::Decimal,
            ),
            _ => return Err(Error::sum(aggregate, Kind::NonNumericValue)),
        };
        domain = Some(match (domain, next_domain) {
            (None, incoming) => incoming,
            (Some(Domain::Floating), _) => {
                return Err(Error::sum(aggregate, Kind::MixedFloatingAndExact));
            }
            (Some(Domain::Signed), Domain::Signed) => Domain::Signed,
            (Some(Domain::Unsigned), Domain::Unsigned) => Domain::Unsigned,
            _ => Domain::Decimal,
        });
        if next_scale > scale {
            coefficient *= BigInt::from(10_u8).pow(next_scale - scale);
            scale = next_scale;
        } else if next_scale < scale {
            next *= BigInt::from(10_u8).pow(scale - next_scale);
        }
        coefficient += next;
    }

    match domain {
        None => Ok(Value::Null),
        Some(Domain::Floating) => Ok(Value::F64(floating)),
        Some(Domain::Signed) => coefficient
            .to_i64()
            .map(Value::I64)
            .ok_or_else(|| Error::sum(aggregate, Kind::NumericOverflow)),
        Some(Domain::Unsigned) => coefficient
            .to_u64()
            .map(Value::U64)
            .ok_or_else(|| Error::sum(aggregate, Kind::NumericOverflow)),
        Some(Domain::Decimal) => {
            let maximum = BigInt::from(Decimal::MAX.mantissa());
            let ten = BigInt::from(10_u8);
            while coefficient.abs() > maximum && scale > 0 && (&coefficient % &ten).is_zero() {
                coefficient /= &ten;
                scale -= 1;
            }
            if coefficient.abs() > maximum {
                let kind = if coefficient.abs() > &maximum * ten.pow(scale) {
                    Kind::NumericOverflow
                } else {
                    Kind::DecimalPrecisionLoss
                };
                return Err(Error::sum(aggregate, kind));
            }
            let mantissa = coefficient
                .to_i128()
                .ok_or_else(|| Error::sum(aggregate, Kind::NumericOverflow))?;
            Decimal::try_from_i128_with_scale(mantissa, scale)
                .map(Value::Decimal)
                .map_err(|_| Error::sum(aggregate, Kind::DecimalPrecisionLoss))
        }
    }
}
