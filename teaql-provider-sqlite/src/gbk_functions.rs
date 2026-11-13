use rusqlite::{Connection, Error, Result, functions::FunctionFlags, types::ValueRef};

fn invalid(message: &'static str) -> Error {
    Error::UserFunctionError(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    )))
}

pub(crate) fn register(connection: &Connection) -> Result<()> {
    connection.create_scalar_function(
        "teaql_gbk",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| match ctx.get_raw(0) {
            ValueRef::Null => Ok(None),
            ValueRef::Text(bytes) => {
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| invalid("GBK requires valid UTF8 TEXT"))?;
                super::gbk_encoding::encode(text)
                    .map(Some)
                    .ok_or_else(|| invalid("GBK cannot encode text in the portable profile"))
            }
            _ => Err(invalid("GBK requires TEXT or NULL")),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_compatible_bytes_null_and_empty_blob() {
        let connection = Connection::open_in_memory().unwrap();
        register(&connection).unwrap();
        let (hex, null, empty_type, empty_size): (String, Option<Vec<u8>>, String, i64) = connection.query_row(
            "SELECT hex(teaql_gbk(?)),teaql_gbk(NULL),typeof(teaql_gbk('')),length(teaql_gbk(''))",
            ["€阿文中"], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).unwrap();
        assert_eq!(hex, "80B0A2CEC4D6D0");
        assert_eq!(null, None);
        assert_eq!(empty_type, "blob");
        assert_eq!(empty_size, 0);
    }

    #[test]
    fn invalid_storage_and_unmappable_values_have_sanitized_diagnostics() {
        let connection = Connection::open_in_memory().unwrap();
        register(&connection).unwrap();
        for value in [
            "PRIVATE-GBK-CANARY😀",
            "PRIVATE-GBK-CANARY\u{e000}",
            "PRIVATE-GBK-CANARY\u{1f9}",
            "PRIVATE-GBK-CANARY\0",
        ] {
            let error = connection
                .query_row("SELECT teaql_gbk(?)", [value], |r| r.get::<_, Vec<u8>>(0))
                .unwrap_err();
            assert!(error.to_string().contains("GBK cannot encode"));
            assert!(!error.to_string().contains("PRIVATE-GBK-CANARY"));
        }
        for sql in [
            "SELECT teaql_gbk(7)",
            "SELECT teaql_gbk(1.5)",
            "SELECT teaql_gbk(X'31')",
        ] {
            let error = connection
                .query_row(sql, [], |r| r.get::<_, Vec<u8>>(0))
                .unwrap_err();
            assert!(error.to_string().contains("GBK requires TEXT"));
        }
    }

    #[test]
    fn physical_connections_are_initialized_before_first_query_without_ensure_schema() {
        use teaql_sql::SqlTransport;
        for _ in 0..2 {
            let transport = crate::SqliteMutationExecutor::from_connection(
                Connection::open_in_memory().unwrap(),
            );
            let query = teaql_sql::CompiledQuery {
                sql: "SELECT hex(teaql_gbk('€')) AS encoded".into(),
                params: vec![],
                comment: None,
                log_context: Default::default(),
            };
            let rows = futures_executor::block_on(transport.fetch_all_compact_sql(&query)).unwrap();
            assert_eq!(rows[0].get("encoded"), Some(&teaql_core::Value::from("80")));
        }
    }
}
