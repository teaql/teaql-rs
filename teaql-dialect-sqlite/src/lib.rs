use teaql_core::{DataType, PropertyDescriptor};
use teaql_sql::{DatabaseKind, SqlCompileError, SqlDialect};

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

    fn schema_type_sql(
        &self,
        data_type: DataType,
        property: &PropertyDescriptor,
    ) -> Result<&'static str, SqlCompileError> {
        match data_type {
            DataType::Bool => Ok("INTEGER"),
            DataType::I64 | DataType::U64 if property.is_id => Ok("INTEGER"),
            DataType::I64 | DataType::U64 => Ok("INTEGER"),
            DataType::F64 => Ok("REAL"),
            DataType::Text => Ok("TEXT"),
            DataType::Json => Ok("JSON"),
            DataType::Date => Ok("DATE"),
            DataType::Timestamp => Ok("TIMESTAMP"),
        }
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
    fn compiles_insert_with_sqlite_placeholders() {
        let query = SqliteDialect
            .compile_insert(&entity(), &InsertCommand::new("Order").value("id", 1_u64).value("name", "A"))
            .unwrap();
        assert_eq!(query.sql, "INSERT INTO \"orders\" (\"id\", \"name\") VALUES (?, ?)");
    }

    #[test]
    fn compiles_update_with_sqlite_placeholders() {
        let query = SqliteDialect
            .compile_update(
                &entity(),
                &UpdateCommand::new("Order", 1_u64)
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
            .compile_delete(&entity(), &DeleteCommand::new("Order", 1_u64).expected_version(3))
            .unwrap();
        let recover = SqliteDialect
            .compile_recover(&entity(), &RecoverCommand::new("Order", 1_u64, -4))
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

    #[test]
    fn compiles_create_table_for_sqlite() {
        let sql = SqliteDialect.compile_create_table(&entity()).unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS \"orders\" (\"id\" INTEGER PRIMARY KEY NOT NULL, \"version\" INTEGER NOT NULL, \"name\" TEXT)"
        );
    }

    #[test]
    fn compiles_add_column_for_sqlite() {
        let sql = SqliteDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("created_at", DataType::Timestamp)
                    .column_name("created_at")
                    .not_null(),
            )
            .unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE \"orders\" ADD COLUMN \"created_at\" TIMESTAMP NOT NULL"
        );
    }
}
