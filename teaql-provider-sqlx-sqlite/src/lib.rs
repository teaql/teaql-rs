use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use sqlx::{
    Arguments, Column, Error as SqlxError, Row, Sqlite, TypeInfo, ValueRef, pool::PoolConnection,
    query_with, sqlite::SqliteConnection,
};
use teaql_core::{
    DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, Record, SelectQuery,
    UpdateCommand, Value,
};
use teaql_runtime::{GraphNode, InternalIdGenerator, RuntimeError, SchemaProvider, UserContext};
use teaql_sql::{
    CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect, quote_identifier_if_needed,
};

pub const DEFAULT_ID_SPACE_TABLE: &str = "teaql_id_space";
use sqlx::sqlite::{SqliteArguments, SqlitePool, SqliteRow};
use tokio::sync::Mutex;

const SQLITE_DECIMAL_PREFIX: &str = "__teaql_decimal__:";

#[derive(Debug, Default, Clone, Copy)]
pub struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::Sqlite
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_ident(ident)
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
            DataType::Decimal => Ok("NUMERIC"),
            DataType::Text => Ok("TEXT"),
            DataType::Json => Ok("JSON"),
            DataType::Date => Ok("DATE"),
            DataType::Timestamp => Ok("TIMESTAMP"),
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
pub struct SqliteMutationExecutor {
    pool: SqlitePool,
    transaction: Arc<Mutex<Option<PoolConnection<Sqlite>>>>,
}

impl SqliteMutationExecutor {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            transaction: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn ensure_schema(
        &self,
        dialect: &SqliteDialect,
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
            quote_ident(table_name)
        );
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn begin_transaction(&self) -> Result<(), MutationExecutorError> {
        let mut transaction = self.transaction.lock().await;
        if transaction.is_some() {
            return Ok(());
        }

        let mut connection = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await?;
        *transaction = Some(connection);
        Ok(())
    }

    pub async fn commit_transaction(&self) -> Result<(), MutationExecutorError> {
        let Some(mut connection) = self.transaction.lock().await.take() else {
            return Ok(());
        };
        sqlx::query("COMMIT").execute(&mut *connection).await?;
        Ok(())
    }

    pub async fn rollback_transaction(&self) -> Result<(), MutationExecutorError> {
        let Some(mut connection) = self.transaction.lock().await.take() else {
            return Ok(());
        };
        sqlx::query("ROLLBACK").execute(&mut *connection).await?;
        Ok(())
    }

    pub async fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
        let mut args = SqliteArguments::default();
        for value in &query.params {
            bind_sqlite(&mut args, value)?;
        }
        let result = if let Some(connection) = self.transaction.lock().await.as_mut() {
            query_with(&query.sql, args)
                .execute(&mut **connection)
                .await?
        } else {
            query_with(&query.sql, args).execute(&self.pool).await?
        };
        Ok(result.rows_affected())
    }

    pub async fn fetch_all(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<Record>, MutationExecutorError> {
        let mut args = SqliteArguments::default();
        for value in &query.params {
            bind_sqlite(&mut args, value)?;
        }
        let mut transaction = self.transaction.lock().await;
        let (json_columns, rows) = if let Some(connection) = transaction.as_mut() {
            let json_columns =
                json_columns_for_query_on_connection(&query.sql, &mut **connection).await?;
            let rows = query_with(&query.sql, args)
                .fetch_all(&mut **connection)
                .await?;
            (json_columns, rows)
        } else {
            let json_columns = self.json_columns_for_query(&query.sql).await?;
            let rows = query_with(&query.sql, args).fetch_all(&self.pool).await?;
            (json_columns, rows)
        };
        rows.iter()
            .map(|row| decode_sqlite_row(row, &json_columns))
            .collect()
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool, MutationExecutorError> {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?",
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
        let pragma_sql = format!("PRAGMA table_info(\"{}\")", table_name.replace('"', "\"\""));
        let rows = sqlx::query(&pragma_sql).fetch_all(&self.pool).await?;
        let mut columns = std::collections::BTreeSet::new();
        for row in rows {
            let name: String = row.try_get("name")?;
            columns.insert(name);
        }
        Ok(columns)
    }

    async fn json_columns_for_query(
        &self,
        sql: &str,
    ) -> Result<BTreeSet<String>, MutationExecutorError> {
        let Some(table_name) = select_table_name(sql) else {
            return Ok(BTreeSet::new());
        };
        json_columns_for_table(&self.pool, &table_name).await
    }
}

async fn json_columns_for_query_on_connection(
    sql: &str,
    connection: &mut SqliteConnection,
) -> Result<BTreeSet<String>, MutationExecutorError> {
    let Some(table_name) = select_table_name(sql) else {
        return Ok(BTreeSet::new());
    };
    json_columns_for_table(connection, &table_name).await
}

async fn json_columns_for_table<'e, E>(
    executor: E,
    table_name: &str,
) -> Result<BTreeSet<String>, MutationExecutorError>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let pragma_sql = format!("PRAGMA table_info(\"{}\")", table_name.replace('"', "\"\""));
    let rows = sqlx::query(&pragma_sql).fetch_all(executor).await?;
    let mut columns = BTreeSet::new();
    for row in rows {
        let name: String = row.try_get("name")?;
        let ty: String = row.try_get("type")?;
        if declared_sqlite_type(&ty).as_deref() == Some("JSON") {
            columns.insert(name);
        }
    }
    Ok(columns)
}

async fn ensure_initial_graphs_sqlite(
    executor: &SqliteMutationExecutor,
    dialect: &SqliteDialect,
    ctx: &UserContext,
) -> Result<(), MutationExecutorError> {
    for graph in ctx.initial_graphs() {
        let entity = ctx.entity(&graph.entity).ok_or_else(|| {
            MutationExecutorError::Bind(format!("missing entity: {}", graph.entity))
        })?;
        if initial_graph_exists_sqlite(executor, dialect, entity, graph).await? {
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

async fn initial_graph_exists_sqlite(
    executor: &SqliteMutationExecutor,
    dialect: &SqliteDialect,
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

pub trait SqliteSchemaExt {
    fn ensure_sqlite_schema(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MutationExecutorError>> + '_>>;
}

async fn ensure_sqlite_schema_for(ctx: &UserContext) -> Result<(), MutationExecutorError> {
    let dialect = ctx.get_resource::<SqliteDialect>().ok_or_else(|| {
        MutationExecutorError::Bind("missing typed resource: SqliteDialect".to_owned())
    })?;
    let executor = ctx
        .get_resource::<SqliteMutationExecutor>()
        .ok_or_else(|| {
            MutationExecutorError::Bind("missing typed resource: SqliteMutationExecutor".to_owned())
        })?;

    let entities = ctx.all_entities();

    executor.ensure_schema(dialect, &entities).await?;
    ensure_initial_graphs_sqlite(executor, dialect, ctx).await
}

impl SqliteSchemaExt for UserContext {
    fn ensure_sqlite_schema(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MutationExecutorError>> + '_>> {
        Box::pin(ensure_sqlite_schema_for(self))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SqliteSchemaProvider;

impl SchemaProvider for SqliteSchemaProvider {
    fn ensure_schema<'a>(
        &'a self,
        ctx: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            ensure_sqlite_schema_for(ctx)
                .await
                .map_err(|err| RuntimeError::Schema(err.to_string()))
        })
    }
}

pub trait SqliteProviderExt {
    fn use_sqlite_provider(&mut self, executor: SqliteMutationExecutor) -> &mut Self;
}

impl SqliteProviderExt for UserContext {
    fn use_sqlite_provider(&mut self, executor: SqliteMutationExecutor) -> &mut Self {
        self.insert_resource(SqliteDialect);
        self.insert_resource(executor);
        self.set_schema_provider(SqliteSchemaProvider);
        self
    }
}

#[derive(Clone)]
pub struct SqliteIdSpaceGenerator {
    pool: SqlitePool,
    table_name: String,
}

impl SqliteIdSpaceGenerator {
    pub fn new(pool: SqlitePool) -> Self {
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
        SqliteMutationExecutor::new(self.pool.clone())
            .ensure_id_space_table(&self.table_name)
            .await
    }

    pub async fn next_id(&self, entity: &str) -> Result<u64, MutationExecutorError> {
        self.ensure_table().await?;
        let sql = format!(
            "INSERT INTO {} (type_name, current_level) VALUES (?, 1) \
             ON CONFLICT (type_name) DO UPDATE \
             SET current_level = current_level + 1 \
             RETURNING current_level",
            quote_ident(&self.table_name)
        );
        let id: i64 = sqlx::query_scalar(&sql)
            .bind(entity)
            .fetch_one(&self.pool)
            .await?;
        u64::try_from(id).map_err(|_| {
            MutationExecutorError::Bind(format!("generated id {id} cannot be represented as u64"))
        })
    }
}

impl InternalIdGenerator for SqliteIdSpaceGenerator {
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

fn quote_ident(ident: &str) -> String {
    quote_identifier_if_needed(ident, '"')
}

fn bind_sqlite(args: &mut SqliteArguments<'_>, value: &Value) -> Result<(), MutationExecutorError> {
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
            .add(format!("{SQLITE_DECIMAL_PREFIX}{v}"))
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Text(v) => args
            .add(v.clone())
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Json(v) => args
            .add(v.to_string())
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Date(v) => args
            .add(*v)
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Timestamp(v) => args
            .add(*v)
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Object(_) => return Err(MutationExecutorError::UnsupportedValue("object")),
        Value::List(_) => return Err(MutationExecutorError::UnsupportedValue("list")),
    }
    Ok(())
}

fn decode_sqlite_row(
    row: &SqliteRow,
    json_columns: &BTreeSet<String>,
) -> Result<Record, MutationExecutorError> {
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
        if json_columns.contains(&name) {
            let raw: String = row.try_get(index).map_err(MutationExecutorError::Sqlx)?;
            let value = Value::Json(serde_json::from_str(&raw).map_err(|err| {
                MutationExecutorError::Bind(format!("invalid sqlite json value: {err}"))
            })?);
            record.insert(name, value);
            continue;
        }
        let type_name = column.type_info().name().to_ascii_uppercase();
        let value = match type_name.as_str() {
            "BOOLEAN" => Value::Bool(row.try_get(index).map_err(MutationExecutorError::Sqlx)?),
            "INTEGER" | "INT8" | "INT4" | "INT2" => {
                Value::I64(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
            }
            "REAL" | "FLOAT" | "DOUBLE" => {
                Value::F64(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
            }
            "NUMERIC" | "DECIMAL" => decode_sqlite_decimal(row, index)?,
            "JSON" => {
                let raw: String = row.try_get(index).map_err(MutationExecutorError::Sqlx)?;
                Value::Json(serde_json::from_str(&raw).map_err(|err| {
                    MutationExecutorError::Bind(format!("invalid sqlite json value: {err}"))
                })?)
            }
            "DATE" => Value::Date(
                row.try_get::<NaiveDate, _>(index)
                    .map_err(MutationExecutorError::Sqlx)?,
            ),
            "TIMESTAMP" | "DATETIME" => Value::Timestamp(
                row.try_get::<DateTime<Utc>, _>(index)
                    .map_err(MutationExecutorError::Sqlx)?,
            ),
            "TEXT" | "VARCHAR" => decode_sqlite_text(row, index)?,
            "NULL" => infer_sqlite_dynamic_value(row, index)?,
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

fn select_table_name(sql: &str) -> Option<String> {
    let mut tokens = sql.split_whitespace();
    while let Some(token) = tokens.next() {
        if token.eq_ignore_ascii_case("from") {
            return tokens.next().map(clean_sqlite_identifier);
        }
    }
    None
}

fn clean_sqlite_identifier(token: &str) -> String {
    token
        .trim_matches(|ch: char| matches!(ch, '"' | '`' | '[' | ']' | ',' | ';'))
        .to_owned()
}

fn declared_sqlite_type(value: &str) -> Option<String> {
    value
        .split('(')
        .next()
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
}

fn decode_sqlite_text(row: &SqliteRow, index: usize) -> Result<Value, MutationExecutorError> {
    let value: String = row.try_get(index).map_err(MutationExecutorError::Sqlx)?;
    if let Some(decimal) = value.strip_prefix(SQLITE_DECIMAL_PREFIX) {
        return Decimal::from_str(decimal)
            .map(Value::Decimal)
            .map_err(|err| MutationExecutorError::Bind(format!("invalid sqlite decimal: {err}")));
    }
    Ok(Value::Text(value))
}

fn decode_sqlite_decimal(row: &SqliteRow, index: usize) -> Result<Value, MutationExecutorError> {
    if let Ok(value) = row.try_get::<String, _>(index) {
        if let Some(decimal) = value.strip_prefix(SQLITE_DECIMAL_PREFIX) {
            return Decimal::from_str(decimal)
                .map(Value::Decimal)
                .map_err(|err| {
                    MutationExecutorError::Bind(format!("invalid sqlite decimal: {err}"))
                });
        }
        return Decimal::from_str(&value)
            .map(Value::Decimal)
            .map_err(|err| MutationExecutorError::Bind(format!("invalid sqlite decimal: {err}")));
    }
    if let Ok(value) = row.try_get::<i64, _>(index) {
        return Ok(Value::Decimal(Decimal::from(value)));
    }
    Err(MutationExecutorError::UnsupportedColumnType(
        "NUMERIC".to_owned(),
    ))
}

fn infer_sqlite_dynamic_value(
    row: &SqliteRow,
    index: usize,
) -> Result<Value, MutationExecutorError> {
    if let Ok(value) = row.try_get::<i64, _>(index) {
        return Ok(Value::I64(value));
    }
    if let Ok(value) = row.try_get::<f64, _>(index) {
        return Ok(Value::F64(value));
    }
    if let Ok(value) = row.try_get::<String, _>(index) {
        if let Some(decimal) = value.strip_prefix(SQLITE_DECIMAL_PREFIX) {
            return Decimal::from_str(decimal)
                .map(Value::Decimal)
                .map_err(|err| {
                    MutationExecutorError::Bind(format!("invalid sqlite decimal: {err}"))
                });
        }
        if let Ok(date) = NaiveDate::parse_from_str(&value, "%Y-%m-%d") {
            return Ok(Value::Date(date));
        }
        if let Ok(timestamp) = DateTime::parse_from_rfc3339(&value) {
            return Ok(Value::Timestamp(timestamp.with_timezone(&Utc)));
        }
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S") {
            return Ok(Value::Timestamp(Utc.from_utc_datetime(&timestamp)));
        }
        return Ok(Value::Text(value));
    }
    Err(MutationExecutorError::UnsupportedColumnType(
        "NULL".to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
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
    fn sqlite_dialect_compiles_mutations_and_schema() {
        let insert = SqliteDialect
            .compile_insert(
                &entity(),
                &InsertCommand::new("Order")
                    .value("id", 1_u64)
                    .value("name", "A"),
            )
            .unwrap();
        assert_eq!(insert.sql, "INSERT INTO orders (id, name) VALUES (?, ?)");

        let update = SqliteDialect
            .compile_update(
                &entity(),
                &UpdateCommand::new("Order", 1_u64)
                    .expected_version(3)
                    .value("name", "B"),
            )
            .unwrap();
        assert_eq!(
            update.sql,
            "UPDATE orders SET name = ?, version = ? WHERE id = ? AND version = ?"
        );

        let delete = SqliteDialect
            .compile_delete(
                &entity(),
                &DeleteCommand::new("Order", 1_u64).expected_version(3),
            )
            .unwrap();
        let recover = SqliteDialect
            .compile_recover(&entity(), &RecoverCommand::new("Order", 1_u64, -4))
            .unwrap();
        assert_eq!(
            delete.sql,
            "UPDATE orders SET version = ? WHERE id = ? AND version = ?"
        );
        assert_eq!(
            recover.sql,
            "UPDATE orders SET version = ? WHERE id = ? AND version = ?"
        );

        let create = SqliteDialect.compile_create_table(&entity()).unwrap();
        assert_eq!(
            create,
            "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY NOT NULL, version INTEGER NOT NULL, name TEXT)"
        );
    }

    #[tokio::test]
    async fn sqlite_executor_ensures_schema_and_roundtrips_rows() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let executor = SqliteMutationExecutor::new(pool);
        let entity = entity();
        let mut ctx = UserContext::new()
            .with_metadata(teaql_runtime::InMemoryMetadataStore::new().with_entity(entity.clone()));

        ctx.use_sqlite_provider(executor.clone());
        ctx.ensure_schema().await.unwrap();

        let insert = SqliteDialect
            .compile_insert(
                &entity,
                &InsertCommand::new("Order")
                    .value("id", 1_u64)
                    .value("version", 1_i64)
                    .value("name", "draft"),
            )
            .unwrap();
        assert_eq!(executor.execute(&insert).await.unwrap(), 1);

        let select = SqliteDialect
            .compile_select(
                &entity,
                &SelectQuery::new("Order")
                    .filter(Expr::eq("id", 1_u64))
                    .order_asc("id"),
            )
            .unwrap();
        let rows = executor.fetch_all(&select).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("version"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("name"), Some(&Value::Text("draft".to_owned())));
    }

    #[tokio::test]
    async fn sqlite_executor_parses_json_only_for_json_columns() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let executor = SqliteMutationExecutor::new(pool);

        executor
            .execute(&CompiledQuery {
                sql: "CREATE TABLE payloads (text_payload TEXT, json_payload JSON)".to_owned(),
                params: Vec::new(),
            })
            .await
            .unwrap();
        executor
            .execute(&CompiledQuery {
                sql: "INSERT INTO payloads (text_payload, json_payload) VALUES (?, ?)".to_owned(),
                params: vec![
                    Value::Text("{\"active\":true}".to_owned()),
                    Value::Json(serde_json::json!({"active": true})),
                ],
            })
            .await
            .unwrap();

        let json_columns = executor
            .json_columns_for_query("SELECT text_payload, json_payload FROM payloads")
            .await
            .unwrap();
        assert!(json_columns.contains("json_payload"));

        let rows = executor
            .fetch_all(&CompiledQuery {
                sql: "SELECT text_payload, json_payload FROM payloads".to_owned(),
                params: Vec::new(),
            })
            .await
            .unwrap();

        assert_eq!(
            rows[0].get("text_payload"),
            Some(&Value::Text("{\"active\":true}".to_owned()))
        );
        assert_eq!(
            rows[0].get("json_payload"),
            Some(&Value::Json(serde_json::json!({"active": true})))
        );
    }

    #[tokio::test]
    async fn sqlite_transaction_reuses_one_connection_from_pool() {
        let path = temp_sqlite_path();
        let options = SqliteConnectOptions::from_str(path.to_str().unwrap())
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .unwrap();
        let executor = SqliteMutationExecutor::new(pool.clone());

        executor
            .execute(&CompiledQuery {
                sql: "CREATE TABLE payloads (id INTEGER PRIMARY KEY, json_payload JSON)".to_owned(),
                params: Vec::new(),
            })
            .await
            .unwrap();

        executor.begin_transaction().await.unwrap();
        executor
            .execute(&CompiledQuery {
                sql: "INSERT INTO payloads (id, json_payload) VALUES (?, ?)".to_owned(),
                params: vec![
                    Value::I64(1),
                    Value::Json(serde_json::json!({"active": true})),
                ],
            })
            .await
            .unwrap();
        let rows = executor
            .fetch_all(&CompiledQuery {
                sql: "SELECT id, json_payload FROM payloads WHERE id = ?".to_owned(),
                params: vec![Value::I64(1)],
            })
            .await
            .unwrap();
        executor.commit_transaction().await.unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("json_payload"),
            Some(&Value::Json(serde_json::json!({"active": true})))
        );

        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    fn temp_sqlite_path() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("teaql-sqlite-transaction-{suffix}.db"))
    }
}
