use teaql_core::{DataType, PropertyDescriptor};
use teaql_sql::{DatabaseKind, SqlCompileError, SqlDialect};

#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::PostgreSql
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn placeholder(&self, index: usize) -> String {
        format!("${index}")
    }

    fn schema_type_sql(
        &self,
        data_type: DataType,
        _property: &PropertyDescriptor,
    ) -> Result<&'static str, SqlCompileError> {
        match data_type {
            DataType::Bool => Ok("BOOLEAN"),
            DataType::I64 | DataType::U64 => Ok("BIGINT"),
            DataType::F64 => Ok("DOUBLE PRECISION"),
            DataType::Text => Ok("TEXT"),
            DataType::Json => Ok("JSONB"),
            DataType::Date => Ok("DATE"),
            DataType::Timestamp => Ok("TIMESTAMPTZ"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PostgresDialect;
    use teaql_core::{
        DataType, DeleteCommand, EntityDescriptor, InsertCommand, PropertyDescriptor,
        RecoverCommand, UpdateCommand,
    };
    use teaql_sql::SqlDialect;

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

    #[test]
    fn compiles_insert_with_postgres_placeholders() {
        let query = PostgresDialect
            .compile_insert(
                &entity(),
                &InsertCommand::new("Order")
                    .value("id", 1_u64)
                    .value("name", "A"),
            )
            .unwrap();
        assert_eq!(
            query.sql,
            "INSERT INTO \"orders\" (\"id\", \"name\") VALUES ($1, $2)"
        );
    }

    #[test]
    fn compiles_update_with_optimistic_lock() {
        let query = PostgresDialect
            .compile_update(
                &entity(),
                &UpdateCommand::new("Order", 1_u64)
                    .expected_version(3)
                    .value("name", "B"),
            )
            .unwrap();
        assert_eq!(
            query.sql,
            "UPDATE \"orders\" SET \"name\" = $1, \"version\" = $2 WHERE \"id\" = $3 AND \"version\" = $4"
        );
    }

    #[test]
    fn compiles_soft_delete_with_optimistic_lock() {
        let query = PostgresDialect
            .compile_delete(
                &entity(),
                &DeleteCommand::new("Order", 1_u64).expected_version(3),
            )
            .unwrap();
        assert_eq!(
            query.sql,
            "UPDATE \"orders\" SET \"version\" = $1 WHERE \"id\" = $2 AND \"version\" = $3"
        );
    }

    #[test]
    fn compiles_recover_with_optimistic_lock() {
        let query = PostgresDialect
            .compile_recover(&entity(), &RecoverCommand::new("Order", 1_u64, -4))
            .unwrap();
        assert_eq!(
            query.sql,
            "UPDATE \"orders\" SET \"version\" = $1 WHERE \"id\" = $2 AND \"version\" = $3"
        );
    }

    #[test]
    fn compiles_create_table_for_postgres() {
        let sql = PostgresDialect.compile_create_table(&entity()).unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS \"orders\" (\"id\" BIGINT PRIMARY KEY NOT NULL, \"version\" BIGINT NOT NULL, \"name\" TEXT)"
        );
    }

    #[test]
    fn compiles_add_column_for_postgres() {
        let sql = PostgresDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("payload", DataType::Json)
                    .column_name("payload")
                    .not_null(),
            )
            .unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE \"orders\" ADD COLUMN \"payload\" JSONB NOT NULL"
        );
    }
}
