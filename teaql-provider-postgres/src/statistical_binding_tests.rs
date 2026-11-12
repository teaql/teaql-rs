use super::*;
use tokio_postgres::types::{FromSql, ToSql, Type};

#[test]
fn integer_threshold_encodes_exact_postgres_numeric_without_a_float_round_trip() {
    for value in [i64::MIN, -1, 0, 50, 9_007_199_254_740_993, i64::MAX] {
        let mut bytes = bytes::BytesMut::new();
        assert!(PgInteger::accepts(&Type::NUMERIC));
        PgInteger(value)
            .to_sql_checked(&Type::NUMERIC, &mut bytes)
            .unwrap();
        assert_eq!(
            Decimal::from_sql(&Type::NUMERIC, &bytes).unwrap(),
            Decimal::from(value)
        );
    }
}

#[test]
fn integer_binding_still_checks_narrow_integer_ranges_and_rejects_text() {
    assert!(!PgInteger::accepts(&Type::TEXT));
    let mut bytes = bytes::BytesMut::new();
    assert!(
        PgInteger(i64::MAX)
            .to_sql_checked(&Type::INT2, &mut bytes)
            .is_err()
    );
    assert!(
        PgInteger(i64::MAX)
            .to_sql_checked(&Type::INT4, &mut bytes)
            .is_err()
    );
}

#[test]
fn result_aliases_preserve_case_and_escape_quotes_without_changing_storage_names() {
    let dialect = PostgresDialect;
    assert_eq!(dialect.quote_ident("LegacyTable"), "LegacyTable");
    assert_eq!(dialect.quote_result_alias("stddevPop"), "\"stddevPop\"");
    assert_eq!(dialect.quote_result_alias("total"), "total");
    assert_eq!(dialect.quote_result_alias("a\"b"), "\"a\"\"b\"");
    assert_eq!(dialect.quote_result_alias("\"key\""), "\"\"\"key\"\"\"");
}

#[test]
fn ordinary_mapped_projection_and_aggregate_emit_logical_result_keys() {
    let entity = EntityDescriptor::new("School")
        .table_name("legacy_school")
        .property(PropertyDescriptor::new("capacity", DataType::I64).column_name("Legacy Capacity"))
        .property(PropertyDescriptor::new("kindId", DataType::I64).column_name("kind_id"));
    let projection = PostgresDialect
        .compile_select(&entity, &SelectQuery::new("School").project("kindId"))
        .unwrap();
    assert_eq!(
        projection.sql,
        "SELECT kind_id AS \"kindId\" FROM legacy_school"
    );
    let aggregate = PostgresDialect
        .compile_select(
            &entity,
            &SelectQuery::new("School").stddev_pop("capacity", "stddevPop"),
        )
        .unwrap();
    assert_eq!(
        aggregate.sql,
        "SELECT STDDEV_POP(\"Legacy Capacity\") AS \"stddevPop\" FROM legacy_school"
    );
}
