use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use teaql_core::{
    DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, Record, SelectQuery,
    UpdateCommand, Value,
};
use teaql_runtime::{GraphNode, InternalIdGenerator, RuntimeError, SchemaProvider, UserContext};
use teaql_sql::{
    CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect, quote_identifier_if_needed,
};
use tokio::sync::Mutex;

pub const DEFAULT_ID_SPACE_TABLE: &str = "teaql_id_space";

use mysql_async::prelude::Queryable;
use mysql_common::constants::ColumnType;

#[derive(Debug, Default, Clone, Copy)]
pub struct MysqlDialect;

impl SqlDialect for MysqlDialect {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::MySql
    }

    fn quote_ident(&self, ident: &str) -> String {
        mysql_quote_ident(ident)
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

    fn schema_indexes_sqls(
        &self,
        entity: &EntityDescriptor,
    ) -> Result<Vec<String>, SqlCompileError> {
        let mut sqls = Vec::new();
        let table_name_upper = entity.table_name.to_uppercase();
        let quoted_table = self.quote_ident(&entity.table_name);

        if let Some(version_col) = entity.properties.iter().find(|p| p.is_version) {
            let default_id = "id".to_string();
            let id_col = entity
                .properties
                .iter()
                .find(|p| p.is_id)
                .map(|p| &p.column_name)
                .unwrap_or(&default_id);
            let idx_name = format!("PK_{}_ID_VERSION", table_name_upper);
            sqls.push(format!(
                "CREATE UNIQUE INDEX {} ON {} ({}, {})",
                self.quote_ident(&idx_name),
                quoted_table,
                self.quote_ident(id_col),
                self.quote_ident(&version_col.column_name)
            ));
        }

        for p in &entity.properties {
            if p.name.ends_with("Id")
                || p.name.ends_with("Time")
                || p.name.ends_with("_time")
                || p.name == "create_time"
                || p.name == "update_time"
            {
                let idx_name = format!("IDX_{}_{}", table_name_upper, p.column_name.to_uppercase());
                sqls.push(format!(
                    "CREATE INDEX {} ON {} ({})",
                    self.quote_ident(&idx_name),
                    quoted_table,
                    self.quote_ident(&p.column_name)
                ));
            }
        }

        Ok(sqls)
    }
}

#[derive(Debug)]
pub enum MutationExecutorError {
    MysqlAsync(mysql_async::Error),
    Pool(String),
    SqlCompile(SqlCompileError),
    UnsupportedValue(&'static str),
    UnsupportedColumnType(String),
    Bind(String),
}

impl std::fmt::Display for MutationExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MysqlAsync(err) => err.fmt(f),
            Self::Pool(msg) => write!(f, "mysql pool error: {msg}"),
            Self::SqlCompile(err) => err.fmt(f),
            Self::UnsupportedValue(kind) => {
                write!(
                    f,
                    "unsupported mysql bind value for mutation executor: {kind}"
                )
            }
            Self::UnsupportedColumnType(kind) => {
                write!(
                    f,
                    "unsupported mysql column type for record decoding: {kind}"
                )
            }
            Self::Bind(message) => write!(f, "mysql bind error: {message}"),
        }
    }
}

impl std::error::Error for MutationExecutorError {}

impl From<mysql_async::Error> for MutationExecutorError {
    fn from(value: mysql_async::Error) -> Self {
        Self::MysqlAsync(value)
    }
}

impl From<SqlCompileError> for MutationExecutorError {
    fn from(value: SqlCompileError) -> Self {
        Self::SqlCompile(value)
    }
}

#[derive(Clone)]
pub struct MysqlMutationExecutor {
    pool: mysql_async::Pool,
}

impl MysqlMutationExecutor {
    pub fn new(pool: mysql_async::Pool) -> Self {
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
                let mut conn = self.pool.get_conn().await?;
                conn.exec_drop(&sql, ()).await?;
                continue;
            }

            let existing_columns = self.table_columns(&entity.table_name).await?;
            for property in &entity.properties {
                let bare_column = strip_identifier_quotes(&property.column_name).to_lowercase();
                if existing_columns.contains(&bare_column) {
                    continue;
                }
                let sql = dialect.compile_add_column(entity, property)?;
                let mut conn = self.pool.get_conn().await?;
                conn.exec_drop(&sql, ()).await?;
            }

            for sql in dialect.schema_indexes_sqls(entity)? {
                let mut conn = self.pool.get_conn().await?;
                match conn.exec_drop(&sql, ()).await {
                    Ok(_) => {}
                    Err(mysql_async::Error::Server(err)) if err.code == 1061 => {
                        // ER_DUP_KEYNAME
                    }
                    Err(e) => return Err(MutationExecutorError::MysqlAsync(e)),
                }
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
        let mut conn = self.pool.get_conn().await?;
        conn.exec_drop(&sql, ()).await?;
        Ok(())
    }

    pub async fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
        let mut params = Vec::new();
        for value in &query.params {
            params.push(bind_mysql(value)?);
        }
        let mut conn = self.pool.get_conn().await?;
        conn.exec_drop(
            &query.sql_with_comment(),
            mysql_async::Params::Positional(params),
        )
        .await?;
        Ok(conn.affected_rows())
    }

    pub async fn fetch_all(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<Record>, MutationExecutorError> {
        let mut params = Vec::new();
        for value in &query.params {
            params.push(bind_mysql(value)?);
        }
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<mysql_async::Row> = conn
            .exec(
                &query.sql_with_comment(),
                mysql_async::Params::Positional(params),
            )
            .await?;
        let mut records = Vec::new();
        for row in rows {
            records.push(decode_mysql_row(row)?);
        }
        Ok(records)
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool, MutationExecutorError> {
        let mut conn = self.pool.get_conn().await?;
        let exists: Option<i64> = conn
            .exec_first(
                "SELECT COUNT(1)
             FROM information_schema.tables
             WHERE table_schema = DATABASE()
               AND table_name = ?",
                (table_name,),
            )
            .await?;
        Ok(exists.unwrap_or(0) > 0)
    }

    async fn table_columns(
        &self,
        table_name: &str,
    ) -> Result<std::collections::BTreeSet<String>, MutationExecutorError> {
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<String> = conn
            .exec(
                "SELECT CAST(column_name AS CHAR)
             FROM information_schema.columns
             WHERE table_schema = DATABASE()
               AND table_name = ?",
                (table_name,),
            )
            .await?;
        let mut columns = std::collections::BTreeSet::new();
        for name in rows {
            columns.insert(name.to_lowercase());
        }
        Ok(columns)
    }
}

impl teaql_sql::SqlTransport for MysqlMutationExecutor {
    type Error = MutationExecutorError;

    async fn fetch_all_sql(&self, query: &CompiledQuery) -> Result<Vec<Record>, Self::Error> {
        self.fetch_all(query).await
    }

    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        self.execute(query).await
    }
}

impl teaql_sql::SqlTransaction for MysqlTransactionExecutor {
    type Error = MutationExecutorError;

    async fn commit_sql(self) -> Result<(), Self::Error> {
        self.commit().await
    }

    async fn rollback_sql(self) -> Result<(), Self::Error> {
        self.rollback().await
    }
}

impl teaql_sql::SqlTransport for MysqlTransactionExecutor {
    type Error = MutationExecutorError;

    async fn fetch_all_sql(&self, query: &CompiledQuery) -> Result<Vec<Record>, Self::Error> {
        self.fetch_all(query).await
    }

    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        self.execute(query).await
    }
}

impl teaql_sql::SqlTransactionTransport for MysqlMutationExecutor {
    type Tx<'a>
        = MysqlTransactionExecutor
    where
        Self: 'a;

    async fn begin_sql(&self) -> Result<Self::Tx<'_>, Self::Error> {
        MysqlTransactionExecutor::begin(&self.pool).await
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
    conn: Arc<Mutex<Option<mysql_async::Conn>>>,
}

impl MysqlTransactionExecutor {
    pub async fn begin(pool: &mysql_async::Pool) -> Result<Self, MutationExecutorError> {
        let mut conn = pool.get_conn().await?;
        conn.query_drop("START TRANSACTION").await?;
        Ok(Self {
            conn: Arc::new(Mutex::new(Some(conn))),
        })
    }

    pub async fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
        let mut params = Vec::new();
        for value in &query.params {
            params.push(bind_mysql(value)?);
        }
        let mut lock = self.conn.lock().await;
        let conn = lock
            .as_mut()
            .ok_or_else(|| MutationExecutorError::Bind("mysql transaction is closed".to_owned()))?;
        conn.exec_drop(
            &query.sql_with_comment(),
            mysql_async::Params::Positional(params),
        )
        .await?;
        Ok(conn.affected_rows())
    }

    pub async fn fetch_all(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<Record>, MutationExecutorError> {
        let mut params = Vec::new();
        for value in &query.params {
            params.push(bind_mysql(value)?);
        }
        let mut lock = self.conn.lock().await;
        let conn = lock
            .as_mut()
            .ok_or_else(|| MutationExecutorError::Bind("mysql transaction is closed".to_owned()))?;
        let rows: Vec<mysql_async::Row> = conn
            .exec(
                &query.sql_with_comment(),
                mysql_async::Params::Positional(params),
            )
            .await?;
        let mut records = Vec::new();
        for row in rows {
            records.push(decode_mysql_row(row)?);
        }
        Ok(records)
    }

    pub async fn commit(&self) -> Result<(), MutationExecutorError> {
        let conn_opt = self.conn.lock().await.take();
        let Some(mut conn) = conn_opt else {
            return Err(MutationExecutorError::Bind(
                "mysql transaction is closed".to_owned(),
            ));
        };
        conn.query_drop("COMMIT").await?;
        Ok(())
    }

    pub async fn rollback(&self) -> Result<(), MutationExecutorError> {
        let conn_opt = self.conn.lock().await.take();
        let Some(mut conn) = conn_opt else {
            return Err(MutationExecutorError::Bind(
                "mysql transaction is closed".to_owned(),
            ));
        };
        conn.query_drop("ROLLBACK").await?;
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

pub async fn ensure_mysql_schema_for(ctx: &UserContext) -> Result<(), MutationExecutorError> {
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
    pool: mysql_async::Pool,
    table_name: String,
}

impl MysqlIdSpaceGenerator {
    pub fn new(pool: mysql_async::Pool) -> Self {
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
        let mut conn = self.pool.get_conn().await?;
        conn.exec_drop(&sql, (entity,)).await?;
        let id: Option<u64> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
        Ok(id.unwrap_or(0))
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
    quote_identifier_if_needed(ident, '`')
}

fn strip_identifier_quotes(ident: &str) -> &str {
    let bytes = ident.as_bytes();
    if bytes.len() >= 2 {
        let (first, last) = (bytes[0], bytes[bytes.len() - 1]);
        if (first == b'"' && last == b'"')
            || (first == b'`' && last == b'`')
            || (first == b'[' && last == b']')
        {
            return &ident[1..ident.len() - 1];
        }
    }
    ident
}

fn bind_mysql(value: &Value) -> Result<mysql_async::Value, MutationExecutorError> {
    match value {
        Value::Null => Ok(mysql_async::Value::NULL),
        Value::Bool(v) => Ok(mysql_async::Value::Int(if *v { 1 } else { 0 })),
        Value::I64(v) => Ok(mysql_async::Value::Int(*v)),
        Value::U64(v) => {
            let v = i64::try_from(*v).map_err(|_| {
                MutationExecutorError::Bind(format!("u64 value {v} exceeds i64 range"))
            })?;
            Ok(mysql_async::Value::Int(v))
        }
        Value::F64(v) => Ok(mysql_async::Value::Double(*v)),
        Value::Decimal(v) => Ok(mysql_async::Value::Bytes(v.to_string().into_bytes())),
        Value::Text(v) => Ok(mysql_async::Value::Bytes(v.clone().into_bytes())),
        Value::Json(v) => Ok(mysql_async::Value::Bytes(
            serde_json::to_string(v).unwrap_or_default().into_bytes(),
        )),
        Value::Date(v) => Ok(mysql_async::Value::Bytes(v.to_string().into_bytes())),
        Value::Timestamp(v) => Ok(mysql_async::Value::Bytes(
            v.naive_utc().to_string().into_bytes(),
        )),
        Value::Object(_) => Err(MutationExecutorError::UnsupportedValue("object")),
        Value::List(_) => Err(MutationExecutorError::UnsupportedValue("list")),
    }
}

fn decode_mysql_row(row: mysql_async::Row) -> Result<Record, MutationExecutorError> {
    let mut record = BTreeMap::new();
    for index in 0..row.len() {
        let column = row.columns_ref()[index].clone();
        let name = column.name_str().into_owned();
        let val_opt = row
            .get_opt::<mysql_async::Value, _>(index)
            .ok_or_else(|| MutationExecutorError::Bind(format!("missing col {index}")))?
            .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;

        if val_opt == mysql_async::Value::NULL {
            record.insert(name, Value::Null);
            continue;
        }

        let value = match column.column_type() {
            ColumnType::MYSQL_TYPE_TINY => {
                let v: i8 = row
                    .get_opt(index)
                    .unwrap()
                    .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
                match v {
                    0 => Value::Bool(false),
                    1 => Value::Bool(true),
                    _ => Value::I64(v as i64),
                }
            }
            ColumnType::MYSQL_TYPE_SHORT => {
                let v: i16 = row
                    .get_opt(index)
                    .unwrap()
                    .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
                Value::I64(v as i64)
            }
            ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => {
                let v: i32 = row
                    .get_opt(index)
                    .unwrap()
                    .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
                Value::I64(v as i64)
            }
            ColumnType::MYSQL_TYPE_LONGLONG => match val_opt {
                mysql_async::Value::Int(v) => Value::I64(v),
                mysql_async::Value::UInt(v) => Value::U64(v),
                mysql_async::Value::Bytes(b) => {
                    let s = String::from_utf8(b).unwrap_or_default();
                    if let Ok(v) = s.parse::<i64>() {
                        Value::I64(v)
                    } else if let Ok(v) = s.parse::<u64>() {
                        Value::U64(v)
                    } else {
                        Value::I64(0)
                    }
                }
                _ => {
                    return Err(MutationExecutorError::UnsupportedColumnType(format!(
                        "{:?}",
                        column.column_type()
                    )));
                }
            },
            ColumnType::MYSQL_TYPE_FLOAT => {
                let v: f32 = row
                    .get_opt(index)
                    .unwrap()
                    .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
                Value::F64(v as f64)
            }
            ColumnType::MYSQL_TYPE_DOUBLE => {
                let v: f64 = row
                    .get_opt(index)
                    .unwrap()
                    .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
                Value::F64(v)
            }
            ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                let s: String = row
                    .get_opt(index)
                    .unwrap()
                    .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
                Value::Decimal(Decimal::from_str(&s).unwrap_or_default())
            }
            ColumnType::MYSQL_TYPE_JSON => {
                let s: String = row
                    .get_opt(index)
                    .unwrap()
                    .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
                Value::Json(serde_json::from_str(&s).unwrap_or(serde_json::Value::Null))
            }
            ColumnType::MYSQL_TYPE_DATE => {
                let d: NaiveDate = row
                    .get_opt(index)
                    .unwrap()
                    .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
                Value::Date(d)
            }
            ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_TIMESTAMP => {
                let dt: NaiveDateTime = row
                    .get_opt(index)
                    .unwrap()
                    .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
                Value::Timestamp(Utc.from_utc_datetime(&dt))
            }
            ColumnType::MYSQL_TYPE_STRING
            | ColumnType::MYSQL_TYPE_VAR_STRING
            | ColumnType::MYSQL_TYPE_VARCHAR
            | ColumnType::MYSQL_TYPE_BLOB
            | ColumnType::MYSQL_TYPE_TINY_BLOB
            | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
            | ColumnType::MYSQL_TYPE_LONG_BLOB => {
                let s: String = row
                    .get_opt(index)
                    .unwrap()
                    .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
                Value::Text(s)
            }
            other => {
                return Err(MutationExecutorError::UnsupportedColumnType(format!(
                    "{:?}",
                    other
                )));
            }
        };
        record.insert(name, value);
    }
    Ok(record)
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
        assert_eq!(insert.sql, "INSERT INTO orders (id, name) VALUES (?, ?)");

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
            "UPDATE orders SET name = ?, version = ? WHERE id = ? AND version = ?"
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
            "UPDATE orders SET version = ? WHERE id = ? AND version = ?"
        );
        assert_eq!(
            recover.sql,
            "UPDATE orders SET version = ? WHERE id = ? AND version = ?"
        );
    }

    #[test]
    fn mysql_dialect_compiles_schema_types() {
        let create = MysqlDialect.compile_create_table(&entity()).unwrap();
        assert_eq!(
            create,
            "CREATE TABLE IF NOT EXISTS orders (id BIGINT PRIMARY KEY NOT NULL, version BIGINT NOT NULL, name TEXT)"
        );

        let json = MysqlDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("payload", DataType::Json)
                    .column_name("payload")
                    .not_null(),
            )
            .unwrap();
        assert_eq!(json, "ALTER TABLE orders ADD COLUMN payload JSON NOT NULL");

        let decimal = MysqlDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("amount", DataType::Decimal).column_name("amount"),
            )
            .unwrap();
        assert_eq!(
            decimal,
            "ALTER TABLE orders ADD COLUMN amount DECIMAL(38, 10)"
        );
    }
}
