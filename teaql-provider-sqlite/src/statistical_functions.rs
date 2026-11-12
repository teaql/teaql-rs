//! Bounded, connection-local statistical kernels. Values are never buffered.
//! INTEGER inputs are centered before floating conversion, so neighboring i64
//! values above 2^53 do not become the same observation. Decimal TEXT is not
//! implicitly parsed or approximated; modeled Decimal restrictions stay in SQL
//! compilation.
use rusqlite::functions::{Aggregate, Context, FunctionFlags};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, Error, Result};
use std::io::{Error as IoError, ErrorKind};

const FLAGS: FunctionFlags = FunctionFlags::SQLITE_UTF8.union(FunctionFlags::SQLITE_DETERMINISTIC);
const WORD: f64 = 4_294_967_296.0;

pub(crate) fn register(connection: &Connection) -> Result<()> {
    for (name, population, square_root) in [
        ("stddev", false, true),
        ("stddev_samp", false, true),
        ("stddev_pop", true, true),
        ("var_samp", false, false),
        ("var_pop", true, false),
    ] {
        connection.create_aggregate_function(
            name,
            1,
            FLAGS,
            MomentsFunction {
                population,
                square_root,
            },
        )?;
    }
    for (name, operation) in [
        ("bit_and", Bits::And),
        ("bit_or", Bits::Or),
        ("bit_xor", Bits::Xor),
    ] {
        connection.create_aggregate_function(name, 1, FLAGS, BitsFunction(operation))?;
    }
    Ok(())
}

fn invalid(message: &'static str) -> Error {
    Error::UserFunctionError(Box::new(IoError::new(ErrorKind::InvalidData, message)))
}

#[derive(Clone, Copy)]
enum Number {
    Integer(i64),
    Real(f64),
}
impl Number {
    fn parts(value: i64) -> (f64, f64) {
        ((value >> 32) as f64, (value & 0xffff_ffff) as f64)
    }
    fn difference(self, origin: Self) -> f64 {
        match (self, origin) {
            (Self::Integer(value), Self::Integer(origin)) => {
                let (high, low) = Self::parts(value);
                let (base_high, base_low) = Self::parts(origin);
                (high - base_high) * WORD + (low - base_low)
            }
            (Self::Real(value), Self::Integer(origin)) => {
                let (high, low) = Self::parts(origin);
                (value - high * WORD) - low
            }
            (Self::Integer(value), Self::Real(origin)) => {
                let (high, low) = Self::parts(value);
                (high * WORD - origin) + low
            }
            (Self::Real(value), Self::Real(origin)) => value - origin,
        }
    }
}

#[derive(Default)]
struct Moments {
    origin: Option<Number>,
    count: u64,
    mean: f64,
    m2: f64,
}
struct MomentsFunction {
    population: bool,
    square_root: bool,
}
impl Aggregate<Moments, Option<f64>> for MomentsFunction {
    fn init(&self, _: &mut Context<'_>) -> Result<Moments> {
        Ok(Moments::default())
    }
    fn step(&self, ctx: &mut Context<'_>, state: &mut Moments) -> Result<()> {
        let value = match ctx.get_raw(0) {
            ValueRef::Null => return Ok(()),
            ValueRef::Integer(value) => Number::Integer(value),
            ValueRef::Real(value) if value.is_finite() => Number::Real(value),
            _ => {
                return Err(invalid(
                    "SQLite statistical aggregate requires finite INTEGER or REAL values",
                ));
            }
        };
        let origin = *state.origin.get_or_insert(value);
        let centered = value.difference(origin);
        state.count = state
            .count
            .checked_add(1)
            .ok_or_else(|| invalid("SQLite statistical count overflow"))?;
        let delta = centered - state.mean;
        let mean = state.mean + delta / state.count as f64;
        let m2 = state.m2 + delta * (centered - mean);
        if !centered.is_finite() || !mean.is_finite() || !m2.is_finite() || m2 < 0.0 {
            return Err(invalid(
                "SQLite statistical aggregate exceeds finite numeric range",
            ));
        }
        state.mean = mean;
        state.m2 = m2;
        Ok(())
    }
    fn finalize(&self, _: &mut Context<'_>, state: Option<Moments>) -> Result<Option<f64>> {
        let Some(state) = state else { return Ok(None) };
        if state.count == 0 || (!self.population && state.count < 2) {
            return Ok(None);
        }
        let denominator = if self.population {
            state.count
        } else {
            state.count - 1
        };
        let variance = state.m2 / denominator as f64;
        Ok(Some(if self.square_root {
            variance.sqrt()
        } else {
            variance
        }))
    }
}

#[derive(Clone, Copy)]
enum Bits {
    And,
    Or,
    Xor,
}
struct BitsFunction(Bits);
impl Aggregate<Option<i64>, Option<i64>> for BitsFunction {
    fn init(&self, _: &mut Context<'_>) -> Result<Option<i64>> {
        Ok(None)
    }
    fn step(&self, ctx: &mut Context<'_>, value: &mut Option<i64>) -> Result<()> {
        let next = match ctx.get_raw(0) {
            ValueRef::Null => return Ok(()),
            ValueRef::Integer(value) => value,
            _ => return Err(invalid("SQLite bitwise aggregate requires INTEGER values")),
        };
        *value = Some(match *value {
            None => next,
            Some(previous) => match self.0 {
                Bits::And => previous & next,
                Bits::Or => previous | next,
                Bits::Xor => previous ^ next,
            },
        });
        Ok(())
    }
    fn finalize(&self, _: &mut Context<'_>, value: Option<Option<i64>>) -> Result<Option<i64>> {
        Ok(value.flatten())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        register(&connection).unwrap();
        register(&connection).unwrap();
        connection
    }
    #[test]
    fn sample_population_and_stddev_have_known_values_and_ignore_nulls() {
        let c = connection();
        let result:(f64,f64,f64,f64,f64)=c.query_row(
            "SELECT var_samp(v),var_pop(v),stddev(v),stddev_pop(v),stddev_samp(v) FROM(SELECT 800 v UNION ALL SELECT 800 UNION ALL SELECT 800 UNION ALL SELECT 800 UNION ALL SELECT 100 UNION ALL SELECT NULL)",
            [],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap();
        assert_eq!(result.0, 98000.0);
        assert_eq!(result.1, 78400.0);
        assert!((result.2 - 98000_f64.sqrt()).abs() < 1e-10);
        assert_eq!(result.3, 280.0);
        assert_eq!(result.2, result.4);
    }
    #[test]
    fn empty_null_and_singleton_are_not_confused() {
        let c = connection();
        for population in ["SELECT 1 v WHERE 0", "SELECT NULL v"] {
            let sql = format!(
                "SELECT var_samp(v),var_pop(v),stddev(v),bit_and(v),bit_or(v),bit_xor(v) FROM({population})"
            );
            let values = c
                .query_row(&sql, [], |r| {
                    Ok((0..6)
                        .map(|i| r.get::<_, Option<f64>>(i).unwrap())
                        .collect::<Vec<_>>())
                })
                .unwrap();
            assert_eq!(values, vec![None; 6]);
        }
        let values: (Option<f64>, f64, Option<f64>, f64) = c
            .query_row(
                "SELECT var_samp(7),var_pop(7),stddev(7),stddev_pop(7)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(values, (None, 0.0, None, 0.0));
    }
    #[test]
    fn neighboring_large_integers_preserve_variance_at_both_i64_boundaries() {
        let c = connection();
        for (left, right) in [
            (9_007_199_254_740_992_i64, 9_007_199_254_740_993_i64),
            (i64::MAX - 1, i64::MAX),
            (i64::MIN, i64::MIN + 1),
        ] {
            let values: (f64, f64) = c
                .query_row(
                    "SELECT var_samp(v),var_pop(v) FROM(SELECT ?1 v UNION ALL SELECT ?2)",
                    [left, right],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(values, (0.5, 0.25));
        }
    }
    #[test]
    fn mixed_integer_real_centering_does_not_erase_a_unit_difference() {
        let c = connection();
        for sql in [
            "SELECT var_pop(v) FROM(SELECT 9007199254740993 v UNION ALL SELECT 9007199254740992.0)",
            "SELECT var_pop(v) FROM(SELECT 9007199254740992.0 v UNION ALL SELECT 9007199254740993)",
            "SELECT var_pop(v) FROM(SELECT 9223372036854775807 v UNION ALL SELECT 9223372036854775808.0)",
        ] {
            assert_eq!(c.query_row(sql, [], |r| r.get::<_, f64>(0)).unwrap(), 0.25);
        }
    }
    #[test]
    fn bitwise_functions_preserve_signed_i64_bits() {
        let c = connection();
        let values:(i64,i64,i64)=c.query_row("SELECT bit_and(v),bit_or(v),bit_xor(v) FROM(SELECT -1 v UNION ALL SELECT -9223372036854775808 UNION ALL SELECT NULL)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
        assert_eq!(values, (i64::MIN, -1, i64::MAX));
    }
    #[test]
    fn groups_and_subsequent_queries_keep_independent_state() {
        let c = connection();
        for _ in 0..2 {
            let rows=c.prepare("SELECT k,var_pop(v),bit_xor(v) FROM(SELECT 1 k,1 v UNION ALL SELECT 1,3 UNION ALL SELECT 2,8) GROUP BY k ORDER BY k").unwrap()
                .query_map([],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,f64>(1)?,r.get::<_,i64>(2)?))).unwrap().collect::<Result<Vec<_>>>().unwrap();
            assert_eq!(rows, vec![(1, 1.0, 2), (2, 0.0, 8)]);
        }
    }
    #[test]
    fn invalid_storage_and_numeric_overflow_fail_without_echoing_values() {
        let c = connection();
        for sql in [
            "SELECT var_pop('PRIVATE-DECIMAL-CANARY')",
            "SELECT var_pop(X'31')",
            "SELECT bit_xor(1.5)",
            "SELECT var_samp(v) FROM(SELECT -1e308 v UNION ALL SELECT 1e308)",
        ] {
            let error = c
                .query_row(sql, [], |r| r.get::<_, f64>(0))
                .unwrap_err()
                .to_string();
            assert!(!error.contains("PRIVATE-DECIMAL-CANARY"));
        }
    }
}
