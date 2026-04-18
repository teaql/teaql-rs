mod dialect;
mod types;

pub use dialect::SqlDialect;
pub use types::{CompiledQuery, DatabaseKind, SqlCompileError};

#[cfg(test)]
mod tests {
    use teaql_core::{
        BinaryOp, DataType, DeleteCommand, EntityDescriptor, Expr, InsertCommand, OrderBy,
        PropertyDescriptor, RecoverCommand, SelectQuery, UpdateCommand, Value,
    };

    use crate::{CompiledQuery, SqlCompileError, SqlDialect};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestDialect;

    impl SqlDialect for TestDialect {
        fn kind(&self) -> crate::DatabaseKind {
            crate::DatabaseKind::PostgreSql
        }

        fn quote_ident(&self, ident: &str) -> String {
            format!("\"{}\"", ident)
        }

        fn placeholder(&self, index: usize) -> String {
            format!("${index}")
        }
    }

    fn entity() -> EntityDescriptor {
        EntityDescriptor::new("Order")
            .table_name("orders")
            .property(
                PropertyDescriptor::new("id", DataType::U64)
                    .column_name("id")
                    .id()
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .column_name("version")
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"))
    }

    fn line_entity() -> EntityDescriptor {
        EntityDescriptor::new("OrderLine")
            .table_name("orderline")
            .property(
                PropertyDescriptor::new("id", DataType::U64)
                    .column_name("id")
                    .id()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("order_id", DataType::U64).column_name("order_id"))
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"))
    }

    #[test]
    fn compiles_select_with_filters_order_and_limit() {
        let query = TestDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order")
                    .project("id")
                    .project("name")
                    .filter(Expr::eq("name", "A"))
                    .order_by(OrderBy::desc("id"))
                    .limit(10)
                    .offset(5),
            )
            .unwrap();

        assert_eq!(
            query,
            CompiledQuery {
                sql: "SELECT \"id\", \"name\" FROM \"orders\" WHERE (\"name\" = $1) ORDER BY \"id\" DESC LIMIT 10 OFFSET 5".to_owned(),
                params: vec![Value::from("A")],
            }
        );
    }

    #[test]
    fn compiles_aggregate_projection() {
        let query = TestDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order").count_field("id", "count"),
            )
            .unwrap();

        assert_eq!(
            query.sql,
            "SELECT COUNT(\"id\") AS \"count\" FROM \"orders\""
        );
    }

    #[test]
    fn compiles_grouped_aggregate_and_extended_predicates() {
        let query = TestDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order")
                    .group_by("name")
                    .count("total")
                    .sum("version", "versionSum")
                    .filter(
                        Expr::between("version", 1_i64, 9_i64)
                            .and_expr(Expr::not_like("name", "tmp%"))
                            .and_expr(Expr::not_in_list(
                                "name",
                                vec![Value::from("x"), Value::from("y")],
                            ))
                            .and_expr(Expr::is_not_null("name")),
                    )
                    .order_asc("name"),
            )
            .unwrap();

        assert_eq!(
            query.sql,
            "SELECT \"name\", COUNT(*) AS \"total\", SUM(\"version\") AS \"versionSum\" FROM \"orders\" WHERE ((\"version\" BETWEEN $1 AND $2) AND (\"name\" NOT LIKE $3) AND (\"name\" NOT IN ($4, $5)) AND (\"name\" IS NOT NULL)) GROUP BY \"name\" ORDER BY \"name\" ASC"
        );
        assert_eq!(
            query.params,
            vec![
                Value::I64(1),
                Value::I64(9),
                Value::from("tmp%"),
                Value::from("x"),
                Value::from("y"),
            ]
        );
    }

    #[test]
    fn compiles_sound_like_expression() {
        let query = TestDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order").filter(Expr::sound_like("name", "Robert")),
            )
            .unwrap();

        assert_eq!(
            query,
            CompiledQuery {
                sql: "SELECT * FROM \"orders\" WHERE (SOUNDEX(\"name\") = SOUNDEX($1))".to_owned(),
                params: vec![Value::from("Robert")],
            }
        );
    }

    #[test]
    fn compiles_java_style_string_match_builders() {
        let query = TestDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order").filter(
                    Expr::contain("name", "tea")
                        .and_expr(Expr::begin_with("name", "t"))
                        .and_expr(Expr::end_with("name", "a"))
                        .and_expr(Expr::not_contain("name", "coffee"))
                        .and_expr(Expr::not_begin_with("name", "x"))
                        .and_expr(Expr::not_end_with("name", "z")),
                ),
            )
            .unwrap();

        assert_eq!(
            query.sql,
            "SELECT * FROM \"orders\" WHERE ((\"name\" LIKE $1) AND (\"name\" LIKE $2) AND (\"name\" LIKE $3) AND (\"name\" NOT LIKE $4) AND (\"name\" NOT LIKE $5) AND (\"name\" NOT LIKE $6))"
        );
        assert_eq!(
            query.params,
            vec![
                Value::from("%tea%"),
                Value::from("t%"),
                Value::from("%a"),
                Value::from("%coffee%"),
                Value::from("x%"),
                Value::from("%z"),
            ]
        );
    }

    #[test]
    fn dialect_schema_setup_defaults_to_empty() {
        assert!(TestDialect.schema_setup_sqls().is_empty());
    }

    #[test]
    fn compiles_insert_update_delete_and_recover() {
        let insert = TestDialect
            .compile_insert(
                &entity(),
                &InsertCommand::new("Order")
                    .value("id", 1_u64)
                    .value("name", "A"),
            )
            .unwrap();
        assert_eq!(
            insert.sql,
            "INSERT INTO \"orders\" (\"id\", \"name\") VALUES ($1, $2)"
        );

        let update = TestDialect
            .compile_update(
                &entity(),
                &UpdateCommand::new("Order", 1_u64)
                    .expected_version(3)
                    .value("name", "B"),
            )
            .unwrap();
        assert_eq!(
            update.sql,
            "UPDATE \"orders\" SET \"name\" = $1, \"version\" = $2 WHERE \"id\" = $3 AND \"version\" = $4"
        );

        let delete = TestDialect
            .compile_delete(
                &entity(),
                &DeleteCommand::new("Order", 1_u64).expected_version(3),
            )
            .unwrap();
        assert_eq!(
            delete.sql,
            "UPDATE \"orders\" SET \"version\" = $1 WHERE \"id\" = $2 AND \"version\" = $3"
        );

        let recover = TestDialect
            .compile_recover(&entity(), &RecoverCommand::new("Order", 1_u64, -4))
            .unwrap();
        assert_eq!(
            recover.sql,
            "UPDATE \"orders\" SET \"version\" = $1 WHERE \"id\" = $2 AND \"version\" = $3"
        );
    }

    #[test]
    fn compiles_in_expression_and_validates_empty_list() {
        let query = TestDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order").filter(Expr::Binary {
                    left: Box::new(Expr::column("id")),
                    op: BinaryOp::In,
                    right: Box::new(Expr::Value(Value::List(vec![1_u64.into(), 2_u64.into()]))),
                }),
            )
            .unwrap();
        assert_eq!(
            query.sql,
            "SELECT * FROM \"orders\" WHERE (\"id\" IN ($1, $2))"
        );

        let err = TestDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order").filter(Expr::Binary {
                    left: Box::new(Expr::column("id")),
                    op: BinaryOp::In,
                    right: Box::new(Expr::Value(Value::List(vec![]))),
                }),
            )
            .unwrap_err();
        assert!(matches!(err, SqlCompileError::EmptyInList));
    }

    #[test]
    fn generic_dialect_expands_large_in_expressions() {
        let query = TestDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order").filter(
                    Expr::in_large("id", vec![Value::from(1_u64), Value::from(2_u64)])
                        .and_expr(Expr::not_in_large("name", vec![Value::from("archived")])),
                ),
            )
            .unwrap();

        assert_eq!(
            query.sql,
            "SELECT * FROM \"orders\" WHERE ((\"id\" IN ($1, $2)) AND (\"name\" NOT IN ($3)))"
        );
        assert_eq!(
            query.params,
            vec![
                Value::from(1_u64),
                Value::from(2_u64),
                Value::from("archived")
            ]
        );
    }

    #[test]
    fn compiles_subquery_expression_and_appends_params_in_order() {
        let query = TestDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order").filter(
                    Expr::in_subquery(
                        "id",
                        line_entity(),
                        SelectQuery::new("OrderLine")
                            .filter(Expr::eq("name", "line-1"))
                            .order_asc("id")
                            .limit(10),
                        "order_id",
                    )
                    .and_expr(Expr::eq("name", "order-1")),
                ),
            )
            .unwrap();

        assert_eq!(
            query.sql,
            "SELECT * FROM \"orders\" WHERE ((\"id\" IN (SELECT \"order_id\" FROM \"orderline\" WHERE (\"name\" = $1) ORDER BY \"id\" ASC LIMIT 10)) AND (\"name\" = $2))"
        );
        assert_eq!(
            query.params,
            vec![Value::from("line-1"), Value::from("order-1")]
        );
    }

    #[test]
    fn compiles_extended_aggregates_expression_projection_function_order_and_having() {
        let query = TestDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order")
                    .group_by("name")
                    .project_expr("nameSound", Expr::soundex(Expr::column("name")))
                    .stddev("version", "stddevVersion")
                    .stddev_pop("version", "stddevPopVersion")
                    .var_samp("version", "varSampVersion")
                    .var_pop("version", "varPopVersion")
                    .bit_and("version", "bitAndVersion")
                    .bit_or("version", "bitOrVersion")
                    .bit_xor("version", "bitXorVersion")
                    .having(Expr::binary(
                        Expr::count_all(),
                        BinaryOp::Gt,
                        Expr::value(1_i64),
                    ))
                    .order_gbk_asc("name"),
            )
            .unwrap();

        assert_eq!(
            query.sql,
            "SELECT \"name\", SOUNDEX(\"name\") AS \"nameSound\", STDDEV(\"version\") AS \"stddevVersion\", STDDEV_POP(\"version\") AS \"stddevPopVersion\", VAR_SAMP(\"version\") AS \"varSampVersion\", VAR_POP(\"version\") AS \"varPopVersion\", BIT_AND(\"version\") AS \"bitAndVersion\", BIT_OR(\"version\") AS \"bitOrVersion\", BIT_XOR(\"version\") AS \"bitXorVersion\" FROM \"orders\" GROUP BY \"name\" HAVING (COUNT(*) > $1) ORDER BY convert_to(\"name\", 'GBK') ASC"
        );
        assert_eq!(query.params, vec![Value::I64(1)]);
    }
}
