use super::*;
use tokio_postgres::types::{FromSql, ToSql, Type};

#[test]
fn actual_postgres_bitwise_fields_are_integer_typed_in_ordinary_and_layered_plans() {
    use teaql_core::aggregation_plan::StatisticalPlan;
    use teaql_core::{Aggregate, AggregateFunction, RelationDescriptor};
    let root = EntityDescriptor::new("BitRoot")
        .property(PropertyDescriptor::new("id", DataType::I64).id())
        .property(PropertyDescriptor::new("kind", DataType::I64))
        .relation(RelationDescriptor::new("kind", "BitParent").local_key("kind"));
    for data_type in [
        DataType::Bool,
        DataType::F64,
        DataType::Decimal,
        DataType::Text,
        DataType::LargeText,
        DataType::Json,
        DataType::Date,
        DataType::Timestamp,
        DataType::I64,
        DataType::U64,
    ] {
        let descriptor = EntityDescriptor::new("BitParent")
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(
                PropertyDescriptor::new("value", data_type).column_name("Private Physical Field"),
            );
        for function in [
            AggregateFunction::BitAnd,
            AggregateFunction::BitOr,
            AggregateFunction::BitXor,
        ] {
            let parent =
                SelectQuery::new("BitParent").aggregate(Aggregate::new(function, "value", "bits"));
            let original = parent.clone();
            let ordinary = PostgresDialect.compile_select(&descriptor, &parent);
            let layered = SelectQuery::new("BitRoot")
                .count("roots")
                .group_by("kind")
                .object_group_by("kind", "kind", parent.clone());
            let plan = StatisticalPlan::build(&layered, |name| match name {
                "BitRoot" => Some(&root),
                "BitParent" => Some(&descriptor),
                _ => None,
            })
            .unwrap();
            let result = teaql_sql::compile_statistical_plan(&PostgresDialect, &plan);
            if matches!(data_type, DataType::I64 | DataType::U64) {
                assert!(ordinary.is_ok(), "{ordinary:?}");
                assert!(result.is_ok(), "{result:?}");
            } else {
                for error in [ordinary.unwrap_err(), result.unwrap_err()] {
                    let message = error.to_string();
                    assert!(
                        message.contains("bitwise aggregate requires an integer field"),
                        "{message}"
                    );
                    assert!(!message.contains("Private Physical Field"), "{message}");
                }
            }
            assert_eq!(parent, original);
        }
    }
}

#[test]
fn actual_postgres_gbk_compiler_preserves_mapping_and_parameter_privacy() {
    let entity = EntityDescriptor::new("GbkKind")
        .property(PropertyDescriptor::new("label", DataType::Text).column_name("Legacy GBK Label"))
        .audit_mask_fields(vec!["label".into()]);
    let query = SelectQuery::new("GbkKind")
        .group_by_expr("caption", Expr::identity(Expr::column("label")))
        .count("types")
        .filter(Expr::eq("label", "PRIVATE-GBK-CANARY"))
        .order_gbk_asc("caption");
    let original = query.clone();
    let result = PostgresDialect.compile_select(&entity, &query).unwrap();
    assert!(
        result
            .sql
            .ends_with("ORDER BY convert_to(\"Legacy GBK Label\", 'GBK') ASC"),
        "{}",
        result.sql
    );
    assert_eq!(result.params, [Value::from("PRIVATE-GBK-CANARY")]);
    assert_eq!(
        result.log_context.parameter_policies,
        [teaql_data_service::SqlParameterLogPolicy::Masked]
    );
    assert_eq!(query, original);
}

#[test]
fn actual_postgres_gbk_compiler_rejects_non_text_and_invalid_arity() {
    let entity = EntityDescriptor::new("GbkKind")
        .property(PropertyDescriptor::new("id", DataType::I64).id());
    for query in [
        SelectQuery::new("GbkKind")
            .group_by("id")
            .count("types")
            .order_gbk_asc("id"),
        SelectQuery::new("GbkKind")
            .count("types")
            .order_gbk_asc("types"),
    ] {
        let error = PostgresDialect
            .compile_select(&entity, &query)
            .unwrap_err()
            .to_string();
        assert!(error.contains("GBK requires a text operand"), "{error}");
    }
    let query = SelectQuery::new("GbkKind").project_expr(
        "bad",
        Expr::function(
            teaql_core::ExprFunction::Gbk,
            [Expr::value("PRIVATE-GBK-CANARY"), Expr::value("another")],
        ),
    );
    let error = PostgresDialect
        .compile_select(&entity, &query)
        .unwrap_err()
        .to_string();
    assert!(error.contains("exactly one"), "{error}");
    assert!(!error.contains("CANARY"));
}

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
