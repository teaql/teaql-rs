//! Connection-local exact Decimal primitives for the SQLite provider.
//!
//! Generated SQLite schema and modeled Q aggregation use these functions.
//! Existing NUMERIC-affinity columns require explicit migration; their
//! already-rounded REAL values cannot be reinterpreted as exact Decimal.

use std::io::{Error as IoError, ErrorKind};
use std::str::FromStr;

use num_bigint::BigInt;
use rusqlite::functions::{Aggregate, Context, FunctionFlags};
use rusqlite::{Connection, Error, Result};
use rust_decimal::Decimal;

const FLAGS: FunctionFlags = FunctionFlags::SQLITE_UTF8.union(FunctionFlags::SQLITE_DETERMINISTIC);

pub(crate) fn register(connection: &Connection) -> Result<()> {
    connection.create_collation("TEAQL_DECIMAL", |left, right| {
        match (Decimal::from_str(left), Decimal::from_str(right)) {
            (Ok(left), Ok(right)) => left.cmp(&right),
            // Existing corrupt text is rejected by the decoder and the
            // schema CHECK. A SQLite collation cannot return an error;
            // keep its comparison total without exposing the bad value.
            _ => left.cmp(right),
        }
    })?;
    connection.create_scalar_function("teaql_decimal_valid", 1, FLAGS, |ctx| {
        let Some(value) = ctx.get_raw(0).as_str().ok() else {
            return Ok(false);
        };
        Ok(Decimal::from_str(value).is_ok_and(|parsed| parsed.to_string() == value))
    })?;
    connection.create_aggregate_function("teaql_decimal_sum", 1, FLAGS, DecimalSum)?;
    Ok(())
}

struct DecimalSum;

struct DecimalTotal {
    coefficient: BigInt,
    scale: u32,
}

impl DecimalTotal {
    fn add(&mut self, value: Decimal) {
        if value.scale() > self.scale {
            self.coefficient *= BigInt::from(10).pow(value.scale() - self.scale);
            self.scale = value.scale();
        }
        self.coefficient +=
            BigInt::from(value.mantissa()) * BigInt::from(10).pow(self.scale - value.scale());
    }

    fn finish(self) -> Result<String> {
        let mut text = self.coefficient.to_string();
        if self.scale > 0 {
            let sign = usize::from(text.starts_with('-'));
            let digits = text.len() - sign;
            if digits <= self.scale as usize {
                text.insert_str(
                    sign,
                    &format!("0.{}", "0".repeat(self.scale as usize - digits)),
                );
            } else {
                text.insert(text.len() - self.scale as usize, '.');
            }
        }
        // Keep the observed scale when representable. Removing only trailing
        // fractional zeroes is exact; rounding a nonzero digit is never allowed.
        let result = Decimal::from_str_exact(&text).or_else(|error| {
            if text.contains('.') {
                Decimal::from_str_exact(text.trim_end_matches('0').trim_end_matches('.'))
            } else {
                Err(error)
            }
        });
        result
            .map(|value| value.to_string())
            .map_err(|_| decimal_overflow())
    }
}

impl Aggregate<Option<DecimalTotal>, Option<String>> for DecimalSum {
    fn init(&self, _ctx: &mut Context<'_>) -> Result<Option<DecimalTotal>> {
        Ok(None)
    }

    fn step(&self, ctx: &mut Context<'_>, sum: &mut Option<DecimalTotal>) -> Result<()> {
        let Some(text) = ctx.get::<Option<String>>(0)? else {
            return Ok(());
        };
        let value = Decimal::from_str_exact(&text).map_err(|_| invalid_decimal())?;
        match sum {
            Some(current) => current.add(value),
            None => {
                *sum = Some(DecimalTotal {
                    coefficient: BigInt::from(value.mantissa()),
                    scale: value.scale(),
                })
            }
        }
        Ok(())
    }

    fn finalize(
        &self,
        _ctx: &mut Context<'_>,
        sum: Option<Option<DecimalTotal>>,
    ) -> Result<Option<String>> {
        sum.flatten().map(DecimalTotal::finish).transpose()
    }
}

fn invalid_decimal() -> Error {
    Error::UserFunctionError(Box::new(IoError::new(
        ErrorKind::InvalidData,
        "invalid decimal text",
    )))
}

fn decimal_overflow() -> Error {
    Error::UserFunctionError(Box::new(IoError::new(
        ErrorKind::InvalidData,
        "decimal sum exceeds runtime precision",
    )))
}

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, params};

    use super::register;

    #[test]
    fn registered_decimal_storage_comparison_and_sum_are_exact() {
        let connection = Connection::open_in_memory().unwrap();
        register(&connection).unwrap();
        register(&connection).unwrap();
        connection
            .execute(
                "CREATE TABLE amounts (value DECIMAL_TEXT COLLATE TEAQL_DECIMAL \
                 CHECK (value IS NULL OR teaql_decimal_valid(value)))",
                [],
            )
            .unwrap();
        for text in ["3.50", "2", "10", "123456789012.3456789", "-0.1"] {
            connection
                .execute("INSERT INTO amounts (value) VALUES (?1)", params![text])
                .unwrap();
        }

        let stored: (String, String) = connection
            .query_row(
                "SELECT typeof(value), value FROM amounts LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, ("text".into(), "3.50".into()));
        let equal: String = connection
            .query_row(
                "SELECT value FROM amounts WHERE value = ?1",
                ["3.5"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(equal, "3.50");

        let ordered = connection
            .prepare("SELECT value FROM amounts ORDER BY value")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(ordered, ["-0.1", "2", "3.50", "10", "123456789012.3456789"]);
        let exact_sum: String = connection
            .query_row("SELECT teaql_decimal_sum(value) FROM amounts", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(exact_sum, "123456789027.7456789");
        let builtin_type: String = connection
            .query_row("SELECT typeof(SUM(value)) FROM amounts", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(builtin_type, "real");

        assert!(
            connection
                .execute("INSERT INTO amounts (value) VALUES (?1)", ["not-a-decimal"])
                .is_err()
        );
        connection
            .execute("INSERT INTO amounts (value) VALUES (NULL)", [])
            .unwrap();
        let unchanged_sum: String = connection
            .query_row("SELECT teaql_decimal_sum(value) FROM amounts", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(unchanged_sum, exact_sum);
    }

    fn sum(values: &[&str]) -> rusqlite::Result<String> {
        let connection = Connection::open_in_memory()?;
        register(&connection)?;
        connection.execute_batch("CREATE TABLE exact_sum(value TEXT)")?;
        for value in values {
            connection.execute("INSERT INTO exact_sum VALUES(?1)", [value])?;
        }
        connection.query_row(
            "SELECT teaql_decimal_sum(value) FROM exact_sum",
            [],
            |row| row.get(0),
        )
    }

    #[test]
    fn sum_preserves_nonzero_fractional_units_at_the_representation_boundary() {
        let max = rust_decimal::Decimal::MAX.to_string();
        assert!(sum(&[&max, "0.1"]).is_err());
        let min = rust_decimal::Decimal::MIN.to_string();
        assert!(sum(&[&min, "-0.1"]).is_err());
        let result = sum(&[
            "792281625142643375935439503.35",
            "792281625142643375935439503.35",
        ])
        .unwrap();
        assert_eq!(
            result.parse::<rust_decimal::Decimal>().unwrap(),
            "1584563250285286751870879006.7"
                .parse::<rust_decimal::Decimal>()
                .unwrap()
        );
        assert_eq!(sum(&["-0.05", "0.01"]).unwrap(), "-0.04");
    }

    #[test]
    fn exact_final_sum_does_not_depend_on_intermediate_overflow_or_input_order() {
        let max = rust_decimal::Decimal::MAX.to_string();
        let negative = rust_decimal::Decimal::MIN.to_string();
        for values in [
            [max.as_str(), "1", negative.as_str()],
            [max.as_str(), negative.as_str(), "1"],
            ["1", max.as_str(), negative.as_str()],
        ] {
            assert_eq!(sum(&values).unwrap(), "1");
        }
    }

    #[test]
    fn inexact_legacy_decimal_text_is_rejected_instead_of_rounded() {
        let error = sum(&["0.00000000000000000000000000001"])
            .unwrap_err()
            .to_string();
        assert!(!error.contains("00000000000000000000000000001"));
        assert!(error.contains("invalid decimal text"), "{error}");
    }
}
