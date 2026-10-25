//! Connection-local exact Decimal primitives for the SQLite provider.
//!
//! Generated SQLite schema and modeled Q aggregation use these functions.
//! Existing NUMERIC-affinity columns require explicit migration; their
//! already-rounded REAL values cannot be reinterpreted as exact Decimal.

use std::io::{Error as IoError, ErrorKind};
use std::str::FromStr;

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

impl Aggregate<Option<Decimal>, Option<String>> for DecimalSum {
    fn init(&self, _ctx: &mut Context<'_>) -> Result<Option<Decimal>> {
        Ok(None)
    }

    fn step(&self, ctx: &mut Context<'_>, sum: &mut Option<Decimal>) -> Result<()> {
        let Some(text) = ctx.get::<Option<String>>(0)? else {
            return Ok(());
        };
        let value = Decimal::from_str(&text).map_err(|_| invalid_decimal())?;
        *sum = Some(match sum {
            Some(current) => current.checked_add(value).ok_or_else(decimal_overflow)?,
            None => value,
        });
        Ok(())
    }

    fn finalize(
        &self,
        _ctx: &mut Context<'_>,
        sum: Option<Option<Decimal>>,
    ) -> Result<Option<String>> {
        Ok(sum.flatten().map(|value| value.to_string()))
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
}
