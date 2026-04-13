mod dialect;
mod types;

pub use dialect::SqlDialect;
pub use types::{CompiledQuery, DatabaseKind, SqlCompileError};

#[cfg(test)]
mod tests {
    use teaql_core::{
        Aggregate, AggregateFunction, BinaryOp, DataType, DeleteCommand, EntityDescriptor, Expr,
        InsertCommand, OrderBy, PropertyDescriptor, RecoverCommand, SelectQuery, UpdateCommand,
        Value,
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
            .property(PropertyDescriptor::new("id", DataType::U64).column_name("id").id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .column_name("version")
                    .version()
                    .not_null(),
            )
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
                &SelectQuery {
                    entity: "Order".to_owned(),
                    projection: Vec::new(),
                    filter: None,
                    order_by: Vec::new(),
                    slice: None,
                    aggregates: vec![Aggregate {
                        function: AggregateFunction::Count,
                        field: "id".to_owned(),
                        alias: "count".to_owned(),
                    }],
                    group_by: Vec::new(),
                    relations: Vec::new(),
                },
            )
            .unwrap();

        assert_eq!(query.sql, "SELECT COUNT(\"id\") AS \"count\" FROM \"orders\"");
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
            .compile_delete(&entity(), &DeleteCommand::new("Order", 1_u64).expected_version(3))
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
                &SelectQuery::new("Order")
                    .filter(Expr::Binary {
                        left: Box::new(Expr::column("id")),
                        op: BinaryOp::In,
                        right: Box::new(Expr::Value(Value::List(vec![1_u64.into(), 2_u64.into()]))),
                    }),
            )
            .unwrap();
        assert_eq!(query.sql, "SELECT * FROM \"orders\" WHERE (\"id\" IN ($1, $2))");

        let err = TestDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order")
                    .filter(Expr::Binary {
                        left: Box::new(Expr::column("id")),
                        op: BinaryOp::In,
                        right: Box::new(Expr::Value(Value::List(vec![]))),
                    }),
            )
            .unwrap_err();
        assert!(matches!(err, SqlCompileError::EmptyInList));
    }
}
