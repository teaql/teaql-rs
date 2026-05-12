use teaql_core::{DataType, PropertyDescriptor};
use teaql_sql::{DatabaseKind, SqlCompileError, SqlDialect};

#[derive(Debug, Default, Clone, Copy)]
pub struct MysqlDialect;

impl SqlDialect for MysqlDialect {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::MySql
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("`{}`", ident.replace('`', "``"))
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_owned()
    }

    fn schema_type_sql(
        &self,
        data_type: DataType,
        _property: &PropertyDescriptor,
    ) -> Result<&'static str, SqlCompileError> {
        match data_type {
            DataType::Bool => Ok("BOOLEAN"),
            DataType::I64 | DataType::U64 => Ok("BIGINT"),
            DataType::F64 => Ok("DOUBLE"),
            DataType::Decimal => Ok("DECIMAL(38, 10)"),
            DataType::Text => Ok("TEXT"),
            DataType::Json => Ok("JSON"),
            DataType::Date => Ok("DATE"),
            DataType::Timestamp => Ok("DATETIME(6)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MysqlDialect;
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
    fn compiles_insert_with_mysql_placeholders() {
        let query = MysqlDialect
            .compile_insert(
                &entity(),
                &InsertCommand::new("Order")
                    .value("id", 1_u64)
                    .value("name", "A"),
            )
            .unwrap();
        assert_eq!(
            query.sql,
            "INSERT INTO `orders` (`id`, `name`) VALUES (?, ?)"
        );
    }

    #[test]
    fn compiles_update_with_mysql_placeholders() {
        let query = MysqlDialect
            .compile_update(
                &entity(),
                &UpdateCommand::new("Order", 1_u64)
                    .expected_version(3)
                    .value("name", "B"),
            )
            .unwrap();
        assert_eq!(
            query.sql,
            "UPDATE `orders` SET `name` = ?, `version` = ? WHERE `id` = ? AND `version` = ?"
        );
    }

    #[test]
    fn compiles_delete_and_recover_with_mysql_placeholders() {
        let delete = MysqlDialect
            .compile_delete(
                &entity(),
                &DeleteCommand::new("Order", 1_u64).expected_version(3),
            )
            .unwrap();
        let recover = MysqlDialect
            .compile_recover(&entity(), &RecoverCommand::new("Order", 1_u64, -4))
            .unwrap();
        assert_eq!(
            delete.sql,
            "UPDATE `orders` SET `version` = ? WHERE `id` = ? AND `version` = ?"
        );
        assert_eq!(
            recover.sql,
            "UPDATE `orders` SET `version` = ? WHERE `id` = ? AND `version` = ?"
        );
    }

    #[test]
    fn compiles_create_table_for_mysql() {
        let sql = MysqlDialect.compile_create_table(&entity()).unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS `orders` (`id` BIGINT PRIMARY KEY NOT NULL, `version` BIGINT NOT NULL, `name` TEXT)"
        );
    }

    #[test]
    fn compiles_add_column_for_mysql() {
        let sql = MysqlDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("payload", DataType::Json)
                    .column_name("payload")
                    .not_null(),
            )
            .unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE `orders` ADD COLUMN `payload` JSON NOT NULL"
        );

        let sql = MysqlDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("amount", DataType::Decimal).column_name("amount"),
            )
            .unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE `orders` ADD COLUMN `amount` DECIMAL(38, 10)"
        );
    }
}
