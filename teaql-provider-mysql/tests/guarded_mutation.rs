use teaql_core::{
    DataType, DeleteCommand, EntityDescriptor, Expr, PropertyDescriptor, RecoverCommand,
    UpdateCommand, Value,
};
use teaql_provider_mysql::MysqlDialect;
use teaql_sql::SqlDialect;

fn order_entity() -> EntityDescriptor {
    EntityDescriptor::new("Order")
        .table_name("orders")
        .property(PropertyDescriptor::new("id", DataType::I64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new("tenant_id", DataType::I64).not_null())
        .property(PropertyDescriptor::new("name", DataType::Text).not_null())
}

#[test]
fn mysql_guarded_mutations_bind_tenant_in_the_mutation_statement() {
    let entity = order_entity();
    let guard = Expr::eq("tenant_id", 7_i64);
    let update = MysqlDialect
        .compile_guarded_update(
            &entity,
            &UpdateCommand::new("Order", 1_i64)
                .expected_version(3)
                .value("name", "B"),
            &guard,
        )
        .expect("guarded update");
    assert_eq!(
        update.sql,
        "UPDATE orders SET name = ?, version = ? WHERE id = ? AND version = ? AND (tenant_id = ?)"
    );
    assert_eq!(update.params.last(), Some(&Value::I64(7)));

    let delete = MysqlDialect
        .compile_guarded_delete(
            &entity,
            &DeleteCommand::new("Order", 1_i64).expected_version(3),
            &guard,
        )
        .expect("guarded delete");
    assert_eq!(
        delete.sql,
        "UPDATE orders SET version = ? WHERE id = ? AND version = ? AND (tenant_id = ?)"
    );
    assert_eq!(delete.params.last(), Some(&Value::I64(7)));

    let recover = MysqlDialect
        .compile_guarded_recover(&entity, &RecoverCommand::new("Order", 1_i64, -4), &guard)
        .expect("guarded recover");
    assert_eq!(
        recover.sql,
        "UPDATE orders SET version = ? WHERE id = ? AND version = ? AND (tenant_id = ?)"
    );
    assert_eq!(recover.params.last(), Some(&Value::I64(7)));
}
