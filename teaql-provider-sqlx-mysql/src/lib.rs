use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use sqlx::types::Json;
use sqlx::{Arguments, Column, Error as SqlxError, Row, TypeInfo, ValueRef, query_with};
use std::sync::Arc;
use teaql_core::{
    DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, Record, SelectQuery,
    UpdateCommand, Value,
};
use teaql_runtime::{GraphNode, InternalIdGenerator, RuntimeError, SchemaProvider, UserContext};
use teaql_sql::{CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect};
use tokio::sync::Mutex;

pub const DEFAULT_ID_SPACE_TABLE: &str = "teaql_id_space";
use sqlx::mysql::{MySqlArguments, MySqlPool, MySqlRow};
use sqlx::{MySql, Transaction};

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

#[derive(Debug)]
pub enum MutationExecutorError {
    Sqlx(SqlxError),
    SqlCompile(SqlCompileError),
    UnsupportedValue(&'static str),
    UnsupportedColumnType(String),
    Bind(String),
}

impl std::fmt::Display for MutationExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlx(err) => err.fmt(f),
            Self::SqlCompile(err) => err.fmt(f),
            Self::UnsupportedValue(kind) => {
                write!(
                    f,
                    "unsupported sqlx bind value for mutation executor: {kind}"
                )
            }
            Self::UnsupportedColumnType(kind) => {
                write!(
                    f,
                    "unsupported sqlx column type for record decoding: {kind}"
                )
            }
            Self::Bind(message) => write!(f, "sqlx bind error: {message}"),
        }
    }
}

impl std::error::Error for MutationExecutorError {}

impl From<SqlxError> for MutationExecutorError {
    fn from(value: SqlxError) -> Self {
        Self::Sqlx(value)
    }
}

impl From<SqlCompileError> for MutationExecutorError {
    fn from(value: SqlCompileError) -> Self {
        Self::SqlCompile(value)
    }
}

#[derive(Clone)]
pub struct MysqlMutationExecutor {
    pool: MySqlPool,
}

impl MysqlMutationExecutor {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    pub async fn ensure_schema(
        &self,
        dialect: &MysqlDialect,
        entities: &[&EntityDescriptor],
    ) -> Result<(), MutationExecutorError> {
        self.ensure_id_space_table(DEFAULT_ID_SPACE_TABLE).await?;

        for entity in entities {
            if !self.table_exists(&entity.table_name).await? {
                let sql = dialect.compile_create_table(entity)?;
                sqlx::query(&sql).execute(&self.pool).await?;
                continue;
            }

            let existing_columns = self.table_columns(&entity.table_name).await?;
            for property in &entity.properties {
                if existing_columns.contains(&property.column_name) {
                    continue;
                }
                let sql = dialect.compile_add_column(entity, property)?;
                sqlx::query(&sql).execute(&self.pool).await?;
            }
        }
        Ok(())
    }

    pub async fn ensure_id_space_table(
        &self,
        table_name: &str,
    ) -> Result<(), MutationExecutorError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (type_name VARCHAR(100) PRIMARY KEY, current_level BIGINT NOT NULL)",
            mysql_quote_ident(table_name)
        );
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
        let mut args = MySqlArguments::default();
        for value in &query.params {
            bind_mysql(&mut args, value)?;
        }
        let result = query_with(&query.sql, args).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    pub async fn fetch_all(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<Record>, MutationExecutorError> {
        let mut args = MySqlArguments::default();
        for value in &query.params {
            bind_mysql(&mut args, value)?;
        }
        let rows = query_with(&query.sql, args).fetch_all(&self.pool).await?;
        rows.iter().map(decode_mysql_row).collect()
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool, MutationExecutorError> {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(1)
             FROM information_schema.tables
             WHERE table_schema = DATABASE()
               AND table_name = ?",
        )
        .bind(table_name)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists > 0)
    }

    async fn table_columns(
        &self,
        table_name: &str,
    ) -> Result<std::collections::BTreeSet<String>, MutationExecutorError> {
        let rows = sqlx::query(
            "SELECT CAST(column_name AS CHAR) AS column_name
             FROM information_schema.columns
             WHERE table_schema = DATABASE()
               AND table_name = ?",
        )
        .bind(table_name)
        .fetch_all(&self.pool)
        .await?;
        let mut columns = std::collections::BTreeSet::new();
        for row in rows {
            let name: String = row.try_get("column_name")?;
            columns.insert(name);
        }
        Ok(columns)
    }
}

async fn ensure_initial_graphs_mysql(
    executor: &MysqlMutationExecutor,
    dialect: &MysqlDialect,
    ctx: &UserContext,
) -> Result<(), MutationExecutorError> {
    for graph in ctx.initial_graphs() {
        let entity = ctx.entity(&graph.entity).ok_or_else(|| {
            MutationExecutorError::Bind(format!("missing entity: {}", graph.entity))
        })?;
        if initial_graph_exists_mysql(executor, dialect, entity, graph).await? {
            if let Some(query) = compile_initial_graph_update(dialect, entity, graph)? {
                executor.execute(&query).await?;
            }
        } else {
            let query = compile_initial_graph_insert(dialect, entity, graph)?;
            executor.execute(&query).await?;
        }
    }
    Ok(())
}

async fn initial_graph_exists_mysql(
    executor: &MysqlMutationExecutor,
    dialect: &MysqlDialect,
    entity: &EntityDescriptor,
    graph: &GraphNode,
) -> Result<bool, MutationExecutorError> {
    let Some(id) = graph.values.get("id") else {
        return Ok(false);
    };
    let query = dialect.compile_select(
        entity,
        &SelectQuery::new(&graph.entity)
            .project("id")
            .filter(Expr::eq("id", id.clone()))
            .limit(1),
    )?;
    Ok(!executor.fetch_all(&query).await?.is_empty())
}

#[derive(Clone)]
pub struct MysqlTransactionExecutor {
    transaction: Arc<Mutex<Option<Transaction<'static, MySql>>>>,
}

impl MysqlTransactionExecutor {
    pub async fn begin(pool: &MySqlPool) -> Result<Self, MutationExecutorError> {
        Ok(Self {
            transaction: Arc::new(Mutex::new(Some(pool.begin().await?))),
        })
    }

    pub async fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
        let mut transaction = self.transaction.lock().await;
        let transaction = transaction
            .as_mut()
            .ok_or_else(|| MutationExecutorError::Bind("mysql transaction is closed".to_owned()))?;
        let mut args = MySqlArguments::default();
        for value in &query.params {
            bind_mysql(&mut args, value)?;
        }
        let result = query_with(&query.sql, args)
            .execute(&mut **transaction)
            .await?;
        Ok(result.rows_affected())
    }

    pub async fn fetch_all(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<Record>, MutationExecutorError> {
        let mut transaction = self.transaction.lock().await;
        let transaction = transaction
            .as_mut()
            .ok_or_else(|| MutationExecutorError::Bind("mysql transaction is closed".to_owned()))?;
        let mut args = MySqlArguments::default();
        for value in &query.params {
            bind_mysql(&mut args, value)?;
        }
        let rows = query_with(&query.sql, args)
            .fetch_all(&mut **transaction)
            .await?;
        rows.iter().map(decode_mysql_row).collect()
    }

    pub async fn commit(&self) -> Result<(), MutationExecutorError> {
        let transaction = self.transaction.lock().await.take();
        let Some(transaction) = transaction else {
            return Err(MutationExecutorError::Bind(
                "mysql transaction is closed".to_owned(),
            ));
        };
        transaction.commit().await?;
        Ok(())
    }

    pub async fn rollback(&self) -> Result<(), MutationExecutorError> {
        let transaction = self.transaction.lock().await.take();
        let Some(transaction) = transaction else {
            return Err(MutationExecutorError::Bind(
                "mysql transaction is closed".to_owned(),
            ));
        };
        transaction.rollback().await?;
        Ok(())
    }
}

fn compile_initial_graph_insert(
    dialect: &impl SqlDialect,
    entity: &EntityDescriptor,
    graph: &GraphNode,
) -> Result<CompiledQuery, MutationExecutorError> {
    let mut command = InsertCommand::new(&graph.entity);
    for (field, value) in &graph.values {
        command = command.value(field.clone(), value.clone());
    }
    dialect.compile_insert(entity, &command).map_err(Into::into)
}

fn compile_initial_graph_update(
    dialect: &impl SqlDialect,
    entity: &EntityDescriptor,
    graph: &crate::GraphNode,
) -> Result<Option<CompiledQuery>, MutationExecutorError> {
    let Some(id) = graph.values.get("id") else {
        return Ok(None);
    };
    let mut command = UpdateCommand::new(&graph.entity, id.clone());
    for (field, value) in &graph.values {
        if field == "id" {
            continue;
        }
        command = command.value(field.clone(), value.clone());
    }
    match dialect.compile_update(entity, &command) {
        Ok(query) => Ok(Some(query)),
        Err(SqlCompileError::EmptyMutation(_)) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub trait MysqlSchemaExt {
    fn ensure_mysql_schema(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MutationExecutorError>> + '_>>;
}

async fn ensure_mysql_schema_for(ctx: &UserContext) -> Result<(), MutationExecutorError> {
    let dialect = ctx.get_resource::<MysqlDialect>().ok_or_else(|| {
        MutationExecutorError::Bind("missing typed resource: MysqlDialect".to_owned())
    })?;
    let executor = ctx.get_resource::<MysqlMutationExecutor>().ok_or_else(|| {
        MutationExecutorError::Bind("missing typed resource: MysqlMutationExecutor".to_owned())
    })?;

    let entities = ctx.all_entities();

    executor.ensure_schema(dialect, &entities).await?;
    ensure_initial_graphs_mysql(executor, dialect, ctx).await
}

impl MysqlSchemaExt for UserContext {
    fn ensure_mysql_schema(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MutationExecutorError>> + '_>> {
        Box::pin(ensure_mysql_schema_for(self))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MysqlSchemaProvider;

impl SchemaProvider for MysqlSchemaProvider {
    fn ensure_schema<'a>(
        &'a self,
        ctx: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            ensure_mysql_schema_for(ctx)
                .await
                .map_err(|err| RuntimeError::Schema(err.to_string()))
        })
    }
}

pub trait MysqlProviderExt {
    fn use_mysql_provider(&mut self, executor: MysqlMutationExecutor) -> &mut Self;
}

impl MysqlProviderExt for UserContext {
    fn use_mysql_provider(&mut self, executor: MysqlMutationExecutor) -> &mut Self {
        self.insert_resource(MysqlDialect);
        self.insert_resource(executor);
        self.set_schema_provider(MysqlSchemaProvider);
        self
    }
}

#[derive(Clone)]
pub struct MysqlIdSpaceGenerator {
    pool: MySqlPool,
    table_name: String,
}

impl MysqlIdSpaceGenerator {
    pub fn new(pool: MySqlPool) -> Self {
        Self {
            pool,
            table_name: DEFAULT_ID_SPACE_TABLE.to_owned(),
        }
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    pub async fn ensure_table(&self) -> Result<(), MutationExecutorError> {
        MysqlMutationExecutor::new(self.pool.clone())
            .ensure_id_space_table(&self.table_name)
            .await
    }

    pub async fn next_id(&self, entity: &str) -> Result<u64, MutationExecutorError> {
        self.ensure_table().await?;
        let sql = format!(
            "INSERT INTO {} (type_name, current_level) VALUES (?, LAST_INSERT_ID(1)) \
             ON DUPLICATE KEY UPDATE current_level = LAST_INSERT_ID(current_level + 1)",
            mysql_quote_ident(&self.table_name)
        );
        let mut conn = self.pool.acquire().await?;
        sqlx::query(&sql).bind(entity).execute(&mut *conn).await?;
        let id: u64 = sqlx::query_scalar("SELECT LAST_INSERT_ID()")
            .fetch_one(&mut *conn)
            .await?;
        Ok(id)
    }
}

impl InternalIdGenerator for MysqlIdSpaceGenerator {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        let generator = self.clone();
        let entity = entity.to_owned();
        block_on_id_generation(async move { generator.next_id(&entity).await })
    }
}

fn block_on_id_generation<F>(future: F) -> Result<u64, RuntimeError>
where
    F: Future<Output = Result<u64, MutationExecutorError>> + Send + 'static,
{
    let result = if tokio::runtime::Handle::try_current().is_ok() {
        let handle = tokio::runtime::Handle::current();
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| RuntimeError::IdGeneration(err.to_string()))?
            .block_on(future)
    };
    result.map_err(|err| RuntimeError::IdGeneration(err.to_string()))
}

fn mysql_quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

fn bind_mysql(args: &mut MySqlArguments, value: &Value) -> Result<(), MutationExecutorError> {
    match value {
        Value::Null => {
            let v: Option<String> = None;
            args.add(v)
                .map_err(|err| MutationExecutorError::Bind(err.to_string()))?;
        }
        Value::Bool(v) => args
            .add(*v)
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::I64(v) => args
            .add(*v)
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::U64(v) => {
            let v = i64::try_from(*v).map_err(|_| {
                MutationExecutorError::Bind(format!("u64 value {v} exceeds i64 range"))
            })?;
            args.add(v)
                .map_err(|err| MutationExecutorError::Bind(err.to_string()))?
        }
        Value::F64(v) => args
            .add(*v)
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Decimal(v) => args
            .add(*v)
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Text(v) => args
            .add(v.clone())
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Json(v) => args
            .add(Json(v.clone()))
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Date(v) => args
            .add(*v)
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Timestamp(v) => args
            .add(v.naive_utc())
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Object(_) => return Err(MutationExecutorError::UnsupportedValue("object")),
        Value::List(_) => return Err(MutationExecutorError::UnsupportedValue("list")),
    }
    Ok(())
}

fn decode_mysql_row(row: &MySqlRow) -> Result<Record, MutationExecutorError> {
    let mut record = BTreeMap::new();
    for (index, column) in row.columns().iter().enumerate() {
        let name = column.name().to_owned();
        let raw = row
            .try_get_raw(index)
            .map_err(MutationExecutorError::Sqlx)?;
        if raw.is_null() {
            record.insert(name, Value::Null);
            continue;
        }
        let type_name = column.type_info().name().to_ascii_uppercase();
        let value = match type_name.as_str() {
            "BOOL" | "BOOLEAN" => {
                Value::Bool(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
            }
            "TINYINT" => {
                let value: i8 = row.try_get(index).map_err(MutationExecutorError::Sqlx)?;
                match value {
                    0 => Value::Bool(false),
                    1 => Value::Bool(true),
                    _ => Value::I64(value as i64),
                }
            }
            "SMALLINT" | "SHORT" => Value::I64(
                row.try_get::<i16, _>(index)
                    .map_err(MutationExecutorError::Sqlx)? as i64,
            ),
            "INT" | "INTEGER" | "MEDIUMINT" | "LONG" => Value::I64(
                row.try_get::<i32, _>(index)
                    .map_err(MutationExecutorError::Sqlx)? as i64,
            ),
            "BIGINT" | "LONGLONG" | "BIGINT UNSIGNED" | "UNSIGNED BIGINT" => {
                decode_mysql_bigint(row, index)?
            }
            "FLOAT" => Value::F64(
                row.try_get::<f32, _>(index)
                    .map_err(MutationExecutorError::Sqlx)? as f64,
            ),
            "DOUBLE" => Value::F64(row.try_get(index).map_err(MutationExecutorError::Sqlx)?),
            "DECIMAL" | "NEWDECIMAL" => Value::Decimal(
                row.try_get::<Decimal, _>(index)
                    .map_err(MutationExecutorError::Sqlx)?,
            ),
            "JSON" => {
                let Json(value) = row.try_get(index).map_err(MutationExecutorError::Sqlx)?;
                Value::Json(value)
            }
            "DATE" => Value::Date(
                row.try_get::<NaiveDate, _>(index)
                    .map_err(MutationExecutorError::Sqlx)?,
            ),
            "DATETIME" | "TIMESTAMP" => {
                let value: NaiveDateTime =
                    row.try_get(index).map_err(MutationExecutorError::Sqlx)?;
                Value::Timestamp(Utc.from_utc_datetime(&value))
            }
            "TEXT" | "VARCHAR" | "CHAR" | "VAR_STRING" | "STRING" => {
                Value::Text(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
            }
            other => {
                return Err(MutationExecutorError::UnsupportedColumnType(
                    other.to_owned(),
                ));
            }
        };
        record.insert(name, value);
    }
    Ok(record)
}

fn decode_mysql_bigint(row: &MySqlRow, index: usize) -> Result<Value, MutationExecutorError> {
    if let Ok(value) = row.try_get::<i64, _>(index) {
        return Ok(Value::I64(value));
    }
    let value: u64 = row.try_get(index).map_err(MutationExecutorError::Sqlx)?;
    Ok(Value::U64(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use teaql_core::{DeleteCommand, RecoverCommand};

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
    fn mysql_dialect_compiles_mutations_with_backtick_identifiers() {
        let insert = MysqlDialect
            .compile_insert(
                &entity(),
                &InsertCommand::new("Order")
                    .value("id", 1_u64)
                    .value("name", "A"),
            )
            .unwrap();
        assert_eq!(
            insert.sql,
            "INSERT INTO `orders` (`id`, `name`) VALUES (?, ?)"
        );

        let update = MysqlDialect
            .compile_update(
                &entity(),
                &UpdateCommand::new("Order", 1_u64)
                    .expected_version(3)
                    .value("name", "B"),
            )
            .unwrap();
        assert_eq!(
            update.sql,
            "UPDATE `orders` SET `name` = ?, `version` = ? WHERE `id` = ? AND `version` = ?"
        );

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
    fn mysql_dialect_compiles_schema_types() {
        let create = MysqlDialect.compile_create_table(&entity()).unwrap();
        assert_eq!(
            create,
            "CREATE TABLE IF NOT EXISTS `orders` (`id` BIGINT PRIMARY KEY NOT NULL, `version` BIGINT NOT NULL, `name` TEXT)"
        );

        let json = MysqlDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("payload", DataType::Json)
                    .column_name("payload")
                    .not_null(),
            )
            .unwrap();
        assert_eq!(
            json,
            "ALTER TABLE `orders` ADD COLUMN `payload` JSON NOT NULL"
        );

        let decimal = MysqlDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("amount", DataType::Decimal).column_name("amount"),
            )
            .unwrap();
        assert_eq!(
            decimal,
            "ALTER TABLE `orders` ADD COLUMN `amount` DECIMAL(38, 10)"
        );
    }
}
