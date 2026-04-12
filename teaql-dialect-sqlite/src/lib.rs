use teaql_sql::{DatabaseKind, SqlDialect};

#[derive(Debug, Default, Clone, Copy)]
pub struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::Sqlite
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteDialect;
    use teaql_core::{DataType, DeleteCommand, EntityDescriptor, InsertCommand, PropertyDescriptor, RecoverCommand, UpdateCommand};
    use teaql_sql::SqlDialect;

    fn entity() -> EntityDescriptor {
        EntityDescriptor::new("Order")
            .table_name("orders")
            .property(PropertyDescriptor::new("id", DataType::I64).column_name("id").id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .column_name("version")
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"))
    }

    #[test]
    fn compiles_insert_with_sqlite_placeholders() {
        let query = SqliteDialect
            .compile_insert(&entity(), &InsertCommand::new("Order").value("id", 1_i64).value("name", "A"))
            .unwrap();
        assert_eq!(query.sql, "INSERT INTO \"orders\" (\"id\", \"name\") VALUES (?, ?)");
    }

    #[test]
    fn compiles_update_with_sqlite_placeholders() {
        let query = SqliteDialect
            .compile_update(
                &entity(),
                &UpdateCommand::new("Order", 1_i64)
                    .expected_version(3)
                    .value("name", "B"),
            )
            .unwrap();
        assert_eq!(
            query.sql,
            "UPDATE \"orders\" SET \"name\" = ?, \"version\" = ? WHERE \"id\" = ? AND \"version\" = ?"
        );
    }

    #[test]
    fn compiles_delete_and_recover_with_sqlite_placeholders() {
        let delete = SqliteDialect
            .compile_delete(&entity(), &DeleteCommand::new("Order", 1_i64).expected_version(3))
            .unwrap();
        let recover = SqliteDialect
            .compile_recover(&entity(), &RecoverCommand::new("Order", 1_i64, -4))
            .unwrap();
        assert_eq!(
            delete.sql,
            "UPDATE \"orders\" SET \"version\" = ? WHERE \"id\" = ? AND \"version\" = ?"
        );
        assert_eq!(
            recover.sql,
            "UPDATE \"orders\" SET \"version\" = ? WHERE \"id\" = ? AND \"version\" = ?"
        );
    }
}
