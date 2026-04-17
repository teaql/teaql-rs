use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use sqlx::postgres::{PgArguments, PgPool, PgRow};
use sqlx::sqlite::{SqliteArguments, SqlitePool, SqliteRow};
use sqlx::types::Json;
use sqlx::{Arguments, Column, Error as SqlxError, Row, TypeInfo, ValueRef, query_with};
use teaql_core::{EntityDescriptor, Record, Value};
use teaql_dialect_pg::PostgresDialect;
use teaql_dialect_sqlite::SqliteDialect;
use teaql_sql::{CompiledQuery, SqlCompileError, SqlDialect};

use crate::UserContext;

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
pub struct PgMutationExecutor {
    pool: PgPool,
}

impl PgMutationExecutor {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn ensure_schema(
        &self,
        dialect: &PostgresDialect,
        entities: &[&EntityDescriptor],
    ) -> Result<(), MutationExecutorError> {
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

    pub async fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
        let mut args = PgArguments::default();
        for value in &query.params {
            bind_pg(&mut args, value)?;
        }
        let result = query_with(&query.sql, args).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    pub async fn fetch_all(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<Record>, MutationExecutorError> {
        let mut args = PgArguments::default();
        for value in &query.params {
            bind_pg(&mut args, value)?;
        }
        let rows = query_with(&query.sql, args).fetch_all(&self.pool).await?;
        rows.iter().map(decode_pg_row).collect()
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool, MutationExecutorError> {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(1)
             FROM information_schema.tables
             WHERE table_schema = current_schema()
               AND table_name = $1",
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
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = $1",
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

#[derive(Clone)]
pub struct SqliteMutationExecutor {
    pool: SqlitePool,
}

impl SqliteMutationExecutor {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn ensure_schema(
        &self,
        dialect: &SqliteDialect,
        entities: &[&EntityDescriptor],
    ) -> Result<(), MutationExecutorError> {
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

    pub async fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
        let mut args = SqliteArguments::default();
        for value in &query.params {
            bind_sqlite(&mut args, value)?;
        }
        let result = query_with(&query.sql, args).execute(&self.pool).await?;
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
        let rows = query_with(&query.sql, args).fetch_all(&self.pool).await?;
        rows.iter().map(decode_sqlite_row).collect()
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
}

impl UserContext {
    pub async fn ensure_postgres_schema(&self) -> Result<(), MutationExecutorError> {
        let dialect = self.get_resource::<PostgresDialect>().ok_or_else(|| {
            MutationExecutorError::Bind("missing typed resource: PostgresDialect".to_owned())
        })?;
        let executor = self.get_resource::<PgMutationExecutor>().ok_or_else(|| {
            MutationExecutorError::Bind("missing typed resource: PgMutationExecutor".to_owned())
        })?;

        let entities = self
            .metadata
            .as_ref()
            .map(|metadata| metadata.all_entities())
            .unwrap_or_default();

        executor.ensure_schema(dialect, &entities).await
    }

    pub async fn ensure_sqlite_schema(&self) -> Result<(), MutationExecutorError> {
        let dialect = self.get_resource::<SqliteDialect>().ok_or_else(|| {
            MutationExecutorError::Bind("missing typed resource: SqliteDialect".to_owned())
        })?;
        let executor = self
            .get_resource::<SqliteMutationExecutor>()
            .ok_or_else(|| {
                MutationExecutorError::Bind(
                    "missing typed resource: SqliteMutationExecutor".to_owned(),
                )
            })?;

        let entities = self
            .metadata
            .as_ref()
            .map(|metadata| metadata.all_entities())
            .unwrap_or_default();

        executor.ensure_schema(dialect, &entities).await
    }
}

fn bind_pg(args: &mut PgArguments, value: &Value) -> Result<(), MutationExecutorError> {
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
            .add(*v)
            .map_err(|err| MutationExecutorError::Bind(err.to_string()))?,
        Value::Object(_) => return Err(MutationExecutorError::UnsupportedValue("object")),
        Value::List(_) => return Err(MutationExecutorError::UnsupportedValue("list")),
    }
    Ok(())
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

fn decode_pg_row(row: &PgRow) -> Result<Record, MutationExecutorError> {
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
            "INT2" => Value::I64(
                row.try_get::<i16, _>(index)
                    .map_err(MutationExecutorError::Sqlx)? as i64,
            ),
            "INT4" => Value::I64(
                row.try_get::<i32, _>(index)
                    .map_err(MutationExecutorError::Sqlx)? as i64,
            ),
            "INT8" => Value::I64(row.try_get(index).map_err(MutationExecutorError::Sqlx)?),
            "FLOAT4" => Value::F64(
                row.try_get::<f32, _>(index)
                    .map_err(MutationExecutorError::Sqlx)? as f64,
            ),
            "FLOAT8" | "NUMERIC" => {
                Value::F64(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
            }
            "JSON" | "JSONB" => {
                let Json(value) = row.try_get(index).map_err(MutationExecutorError::Sqlx)?;
                Value::Json(value)
            }
            "DATE" => Value::Date(
                row.try_get::<NaiveDate, _>(index)
                    .map_err(MutationExecutorError::Sqlx)?,
            ),
            "TIMESTAMP" | "TIMESTAMPTZ" => Value::Timestamp(
                row.try_get::<DateTime<Utc>, _>(index)
                    .map_err(MutationExecutorError::Sqlx)?,
            ),
            "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "UUID" => {
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

fn decode_sqlite_row(row: &SqliteRow) -> Result<Record, MutationExecutorError> {
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
            "BOOLEAN" => Value::Bool(row.try_get(index).map_err(MutationExecutorError::Sqlx)?),
            "INTEGER" | "INT8" | "INT4" | "INT2" => {
                Value::I64(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
            }
            "REAL" | "FLOAT" | "DOUBLE" | "NUMERIC" => {
                Value::F64(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
            }
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
            "TEXT" | "VARCHAR" => {
                Value::Text(row.try_get(index).map_err(MutationExecutorError::Sqlx)?)
            }
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
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&value) {
            if !matches!(json, serde_json::Value::String(_)) {
                return Ok(Value::Json(json));
            }
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
