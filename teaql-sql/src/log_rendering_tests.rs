use crate::{CompiledQuery, DatabaseKind as Db, render_sql_value, render_sql_with};
use teaql_core::Value;

#[test]
fn projected_values_share_the_debug_renderer() {
    for kind in [Db::Sqlite, Db::MySql, Db::PostgreSql] {
        let query = CompiledQuery {
            log_context: Default::default(),
            sql: if kind == Db::PostgreSql {
                "SELECT $1, $2"
            } else {
                "SELECT ?, ?"
            }
            .into(),
            params: vec![Value::Text("O'Reilly".into()), Value::I64(23)],
            comment: None,
        };
        assert_eq!(query.debug_sql(kind), "SELECT 'O''Reilly', 23");
        let safe = render_sql_with(&query.sql, kind, 2, |index| {
            if index == 0 {
                Ok("'O''****ly' /* masked */".into())
            } else {
                render_sql_value(&query.params[index], kind)
            }
        })
        .unwrap();
        assert_eq!(safe, "SELECT 'O''****ly' /* masked */, 23");
        assert_eq!(query.params[0], Value::Text("O'Reilly".into()));
    }
}

#[test]
fn quotes_comments_and_unicode_do_not_consume_bindings() {
    for kind in [Db::Sqlite, Db::MySql] {
        let sql = "SELECT '?', \"?\", `?`, 'O''?雪', ? /* ? */ -- ?\n";
        let rendered = render_sql_with(sql, kind, 1, |_| Ok("17".into())).unwrap();
        assert_eq!(
            rendered,
            "SELECT '?', \"?\", `?`, 'O''?雪', 17 /* ? */ -- ?\n"
        );
    }
    assert_eq!(
        render_sql_with("SELECT [?], ?", Db::Sqlite, 1, |_| Ok("17".into())).unwrap(),
        "SELECT [?], 17"
    );
}

#[test]
fn postgres_repeated_bindings_dollar_quotes_and_nested_comments() {
    let sql = "SELECT $2, $1, $2, '$1', $$ $2 $$, $tag$ $1 $tag$ /* outer /* $3 */ $4 */";
    let output = render_sql_with(sql, Db::PostgreSql, 2, |i| Ok((i + 10).to_string())).unwrap();
    assert_eq!(
        output,
        "SELECT 11, 10, 11, '$1', $$ $2 $$, $tag$ $1 $tag$ /* outer /* $3 */ $4 */"
    );
}

#[test]
fn incomplete_or_ambiguous_bindings_fail_closed() {
    for (sql, kind, count) in [
        ("SELECT ?", Db::Sqlite, 0),
        ("SELECT 1", Db::Sqlite, 1),
        ("SELECT ?1", Db::Sqlite, 1),
        ("SELECT :name", Db::Sqlite, 1),
        ("SELECT @name", Db::Sqlite, 1),
        ("SELECT $name", Db::Sqlite, 1),
        ("SELECT $0", Db::PostgreSql, 1),
        ("SELECT $2", Db::PostgreSql, 2),
        ("SELECT $9999999999999999999999999", Db::PostgreSql, 1),
        ("SELECT 'unterminated", Db::Sqlite, 0),
        ("SELECT /* unterminated", Db::Sqlite, 0),
        ("SELECT $tag$ unterminated", Db::PostgreSql, 0),
        ("SELECT E'\\'", Db::PostgreSql, 0),
        ("SELECT 'a\\b'", Db::MySql, 0),
        ("SELECT 1 # ?", Db::MySql, 1),
        ("SELECT /*! ? */", Db::MySql, 1),
    ] {
        assert!(
            render_sql_with(sql, kind, count, |_| Ok("safe".into())).is_err(),
            "{kind:?} {sql}"
        );
    }
}

#[test]
fn renderer_propagates_projection_failure_without_echoing_input() {
    assert_eq!(
        render_sql_with("SELECT ?", Db::Sqlite, 1, |_| Err("unsupported value")),
        Err("unsupported value")
    );
    let query = CompiledQuery {
        log_context: Default::default(),
        sql: "SELECT 'PRIVATE' /*".into(),
        params: vec![],
        comment: None,
    };
    let rendered = query.debug_sql(Db::Sqlite);
    assert!(rendered.contains("NOT REPLAYABLE"));
    assert!(!rendered.contains("PRIVATE"));
}

#[test]
fn null_arrays_and_finite_numbers_follow_dialect_rules() {
    assert_eq!(render_sql_value(&Value::Null, Db::Sqlite).unwrap(), "NULL");
    assert_eq!(
        render_sql_value(
            &Value::List(vec![Value::I64(1), Value::Null]),
            Db::PostgreSql
        )
        .unwrap(),
        "ARRAY[1, NULL]"
    );
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(render_sql_value(&Value::F64(value), Db::PostgreSql).is_err());
        assert!(render_sql_value(&Value::List(vec![Value::F64(value)]), Db::PostgreSql).is_err());
    }
    assert!(render_sql_value(&Value::Text("a\\b".into()), Db::MySql).is_err());
}
