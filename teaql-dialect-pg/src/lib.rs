use teaql_core::{BinaryOp, DataType, EntityDescriptor, Expr, PropertyDescriptor, Value};
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

    fn schema_setup_sqls(&self) -> &'static [&'static str] {
        &[CREATE_SOUNDEX_FUNCTION]
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

    fn compile_in(
        &self,
        entity: &EntityDescriptor,
        left: &Expr,
        op: BinaryOp,
        right: &Expr,
        params: &mut Vec<Value>,
    ) -> Result<String, SqlCompileError> {
        match op {
            BinaryOp::InLarge | BinaryOp::NotInLarge => {
                let Expr::Value(Value::List(values)) = right else {
                    let lhs = self.compile_expr(entity, left, params)?;
                    let rhs = self.compile_expr(entity, right, params)?;
                    let operator = match op {
                        BinaryOp::InLarge => "= ANY",
                        BinaryOp::NotInLarge => "<> ALL",
                        _ => unreachable!(),
                    };
                    return Ok(format!("({lhs} {operator} ({rhs}))"));
                };
                if values.is_empty() {
                    return Err(SqlCompileError::EmptyInList);
                }
                let lhs = self.compile_expr(entity, left, params)?;
                params.push(Value::List(values.clone()));
                let placeholder = self.placeholder(params.len());
                let operator = match op {
                    BinaryOp::InLarge => "= ANY",
                    BinaryOp::NotInLarge => "<> ALL",
                    _ => unreachable!(),
                };
                Ok(format!("({lhs} {operator}({placeholder}))"))
            }
            _ => {
                let lhs = self.compile_expr(entity, left, params)?;
                let operator = match op {
                    BinaryOp::In => "IN",
                    BinaryOp::NotIn => "NOT IN",
                    _ => unreachable!(),
                };
                match right {
                    Expr::Value(Value::List(values)) => {
                        if values.is_empty() {
                            return Err(SqlCompileError::EmptyInList);
                        }
                        let mut placeholders = Vec::with_capacity(values.len());
                        for value in values {
                            params.push(value.clone());
                            placeholders.push(self.placeholder(params.len()));
                        }
                        Ok(format!("({lhs} {operator} ({}))", placeholders.join(", ")))
                    }
                    _ => {
                        let rhs = self.compile_expr(entity, right, params)?;
                        Ok(format!("({lhs} {operator} ({rhs}))"))
                    }
                }
            }
        }
    }
}

const CREATE_SOUNDEX_FUNCTION: &str = r#"
CREATE OR REPLACE FUNCTION soundex(input text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
    normalized text := upper(regexp_replace(input, '[^A-Za-z]', '', 'g'));
    first_char text;
    output text;
    previous_code text;
    code text;
    ch text;
    i integer;
BEGIN
    IF normalized = '' THEN
        RETURN '0000';
    END IF;

    first_char := substr(normalized, 1, 1);
    output := first_char;
    previous_code := CASE
        WHEN first_char IN ('B', 'F', 'P', 'V') THEN '1'
        WHEN first_char IN ('C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z') THEN '2'
        WHEN first_char IN ('D', 'T') THEN '3'
        WHEN first_char = 'L' THEN '4'
        WHEN first_char IN ('M', 'N') THEN '5'
        WHEN first_char = 'R' THEN '6'
        ELSE '0'
    END;

    FOR i IN 2..char_length(normalized) LOOP
        ch := substr(normalized, i, 1);
        code := CASE
            WHEN ch IN ('B', 'F', 'P', 'V') THEN '1'
            WHEN ch IN ('C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z') THEN '2'
            WHEN ch IN ('D', 'T') THEN '3'
            WHEN ch = 'L' THEN '4'
            WHEN ch IN ('M', 'N') THEN '5'
            WHEN ch = 'R' THEN '6'
            ELSE '0'
        END;

        IF code <> '0' AND code <> previous_code THEN
            output := output || code;
            IF char_length(output) = 4 THEN
                RETURN output;
            END IF;
        END IF;
        previous_code := code;
    END LOOP;

    RETURN rpad(output, 4, '0');
END;
$$
"#;

#[cfg(test)]
mod tests {
    use super::PostgresDialect;
    use teaql_core::{
        DataType, DeleteCommand, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor,
        RecoverCommand, SelectQuery, UpdateCommand, Value,
    };
    use teaql_sql::{CompiledQuery, SqlDialect};

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
    fn provides_postgres_schema_setup_for_soundex() {
        let setup_sqls = PostgresDialect.schema_setup_sqls();
        assert_eq!(setup_sqls.len(), 1);
        assert!(setup_sqls[0].contains("CREATE OR REPLACE FUNCTION soundex(input text)"));
        assert!(!setup_sqls[0].contains("fuzzystrmatch"));
    }

    #[test]
    fn compiles_large_in_with_array_bind_for_postgres() {
        let query = PostgresDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order").filter(
                    Expr::in_large("id", vec![Value::from(1_u64), Value::from(2_u64)])
                        .and_expr(Expr::not_in_large("name", vec![Value::from("archived")])),
                ),
            )
            .unwrap();

        assert_eq!(
            query,
            CompiledQuery {
                sql:
                    "SELECT * FROM \"orders\" WHERE ((\"id\" = ANY($1)) AND (\"name\" <> ALL($2)))"
                        .to_owned(),
                params: vec![
                    Value::List(vec![Value::from(1_u64), Value::from(2_u64)]),
                    Value::List(vec![Value::from("archived")]),
                ],
            }
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
