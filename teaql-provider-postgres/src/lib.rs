#![allow(warnings)]
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use std::sync::Arc;
use teaql_core::{
    BinaryOp, DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, Record,
    SelectQuery, UpdateCommand, Value,
};
use teaql_runtime::{GraphNode, InternalIdGenerator, RuntimeError, SchemaProvider, UserContext};
use teaql_sql::{
    CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect, SqlTransport, quote_identifier_if_needed,
};
use tokio::sync::Mutex;
use deadpool_postgres::Pool;

pub const DEFAULT_ID_SPACE_TABLE: &str = "teaql_id_space";

#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::PostgreSql
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_ident(ident)
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
            DataType::Decimal => Ok("NUMERIC"),
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

#[derive(Debug)]
pub enum MutationExecutorError {
    Sqlx(tokio_postgres::Error),
    Pool(String),
    SqlCompile(SqlCompileError),
    UnsupportedValue(&'static str),
    UnsupportedColumnType(String),
    Bind(String),
}

impl std::fmt::Display for MutationExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlx(err) => err.fmt(f),
            Self::Pool(err) => write!(f, "postgres pool error: {err}"),
            Self::SqlCompile(err) => err.fmt(f),
            Self::UnsupportedValue(kind) => {
                write!(
                    f,
                    "unsupported bind value for mutation executor: {kind}"
                )
            }
            Self::UnsupportedColumnType(kind) => {
                write!(
                    f,
                    "unsupported column type for record decoding: {kind}"
                )
            }
            Self::Bind(message) => write!(f, "bind error: {message}"),
        }
    }
}

impl std::error::Error for MutationExecutorError {}

impl From<tokio_postgres::Error> for MutationExecutorError {
    fn from(value: tokio_postgres::Error) -> Self {
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
    pool: Pool,
}

impl SqlTransport for PgMutationExecutor {
    type Error = MutationExecutorError;

    async fn fetch_all_sql(&self, query: &CompiledQuery) -> Result<Vec<Record>, Self::Error> {
        self.fetch_all(query).await
    }

    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        self.execute(query).await
    }
}

impl teaql_sql::SqlTransaction for PgMutationExecutor {
    type Error = MutationExecutorError;

    async fn commit_sql(self) -> Result<(), Self::Error> {
        Err(MutationExecutorError::Bind("Transactions not supported yet".to_string()))
    }

    async fn rollback_sql(self) -> Result<(), Self::Error> {
        Err(MutationExecutorError::Bind("Transactions not supported yet".to_string()))
    }
}

impl teaql_sql::SqlTransactionTransport for PgMutationExecutor {
    type Tx<'a> = Self where Self: 'a;

    async fn begin_sql(&self) -> Result<Self::Tx<'_>, Self::Error> {
        Err(MutationExecutorError::Bind("Transactions not supported yet".to_string()))
    }
}

impl PgMutationExecutor {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> Pool {
        self.pool.clone()
    }

    pub async fn ensure_schema(
        &self,
        dialect: &PostgresDialect,
        entities: &[&EntityDescriptor],
    ) -> Result<(), MutationExecutorError> {
        let client = self.pool.get().await.map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        for sql in dialect.schema_setup_sqls() {
            client.execute(*sql, &[]).await?;
        }
        self.ensure_id_space_table(DEFAULT_ID_SPACE_TABLE).await?;

        for entity in entities {
            if !self.table_exists(&entity.table_name).await? {
                let sql = dialect.compile_create_table(entity)?;
                client.execute(&sql, &[]).await?;
                continue;
            }

            let existing_columns = self.table_columns(&entity.table_name).await?;
            for property in &entity.properties {
                let bare_column = strip_identifier_quotes(&property.column_name).to_lowercase();
                if existing_columns.contains(&bare_column) {
                    continue;
                }
                let sql = dialect.compile_add_column(entity, property)?;
                client.execute(&sql, &[]).await?;
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
        let client = self.pool.get().await.map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        client.execute(&sql, &[]).await?;
        Ok(())
    }

    pub async fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
        let mut args = PgArgs { values: Vec::new() };
        for value in &query.params {
            bind_pg(&mut args, value)?;
        }
        let client = self.pool.get().await.map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let result = client.execute(&query.sql, &args.as_refs()).await?;
        Ok(result)
    }

    pub async fn fetch_all(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<Record>, MutationExecutorError> {
        let mut args = PgArgs { values: Vec::new() };
        for value in &query.params {
            bind_pg(&mut args, value)?;
        }
        let client = self.pool.get().await.map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let rows = client.query(&query.sql, &args.as_refs()).await?;
        rows.iter().map(decode_pg_row).collect()
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool, MutationExecutorError> {
        let client = self.pool.get().await.map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let row = client.query_one(
            "SELECT COUNT(1)
             FROM information_schema.tables
             WHERE table_schema = current_schema()
               AND table_name = $1",
            &[&table_name],
        ).await?;
        let exists: i64 = row.try_get(0)?;
        Ok(exists > 0)
    }

    async fn table_columns(
        &self,
        table_name: &str,
    ) -> Result<std::collections::BTreeSet<String>, MutationExecutorError> {
        let client = self.pool.get().await.map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let rows = client.query(
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = $1",
            &[&table_name],
        ).await?;
        let mut columns = std::collections::BTreeSet::new();
        for row in rows {
            let name: String = row.try_get("column_name")?;
            columns.insert(name.to_lowercase());
        }
        Ok(columns)
    }
}

async fn ensure_initial_graphs_postgres(
    executor: &PgMutationExecutor,
    dialect: &PostgresDialect,
    ctx: &UserContext,
) -> Result<(), MutationExecutorError> {
    for graph in ctx.initial_graphs() {
        let entity = ctx.entity(&graph.entity).ok_or_else(|| {
            MutationExecutorError::Bind(format!("missing entity: {}", graph.entity))
        })?;
        if initial_graph_exists_postgres(executor, dialect, entity, graph).await? {
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

async fn initial_graph_exists_postgres(
    executor: &PgMutationExecutor,
    dialect: &PostgresDialect,
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

pub trait PostgresSchemaExt {
    fn ensure_postgres_schema(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MutationExecutorError>> + '_>>;
}

pub async fn ensure_postgres_schema_for(ctx: &UserContext) -> Result<(), MutationExecutorError> {
    let dialect = ctx.get_resource::<PostgresDialect>().ok_or_else(|| {
        MutationExecutorError::Bind("missing typed resource: PostgresDialect".to_owned())
    })?;
    let executor = ctx.get_resource::<PgMutationExecutor>().ok_or_else(|| {
        MutationExecutorError::Bind("missing typed resource: PgMutationExecutor".to_owned())
    })?;

    let entities = ctx.all_entities();

    executor.ensure_schema(dialect, &entities).await?;
    ensure_initial_graphs_postgres(executor, dialect, ctx).await
}

impl PostgresSchemaExt for UserContext {
    fn ensure_postgres_schema(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MutationExecutorError>> + '_>> {
        Box::pin(ensure_postgres_schema_for(self))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresSchemaProvider;

impl SchemaProvider for PostgresSchemaProvider {
    fn ensure_schema<'a>(
        &'a self,
        ctx: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            ensure_postgres_schema_for(ctx)
                .await
                .map_err(|err| RuntimeError::Schema(err.to_string()))
        })
    }
}

pub trait PostgresProviderExt {
    fn use_postgres_provider(&mut self, executor: PgMutationExecutor) -> &mut Self;
}

impl PostgresProviderExt for UserContext {
    fn use_postgres_provider(&mut self, executor: PgMutationExecutor) -> &mut Self {
        self.insert_resource(PostgresDialect);
        self.insert_resource(executor);
        self.set_schema_provider(PostgresSchemaProvider);
        self
    }
}

#[derive(Clone)]
pub struct PgIdSpaceGenerator {
    pool: Pool,
    table_name: String,
}

impl PgIdSpaceGenerator {
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            table_name: DEFAULT_ID_SPACE_TABLE.to_owned(),
        }
    }

    pub fn from_executor(executor: PgMutationExecutor) -> Self {
        Self::new(executor.pool())
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    pub async fn ensure_table(&self) -> Result<(), MutationExecutorError> {
        PgMutationExecutor::new(self.pool.clone())
            .ensure_id_space_table(&self.table_name)
            .await
    }

    pub async fn next_id(&self, entity: &str) -> Result<u64, MutationExecutorError> {
        self.ensure_table().await?;
        let update_sql = format!(
            "UPDATE {} SET current_level = current_level + 1 WHERE type_name = $1 RETURNING current_level",
            quote_ident(&self.table_name)
        );
        let client = self.pool.get().await.map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let row = client.query_opt(&update_sql, &[&entity]).await?;
        
        let id = match row {
            Some(r) => {
                let level: i64 = r.try_get(0)?;
                level
            },
            None => {
                let insert_sql = format!(
                    "INSERT INTO {} (type_name, current_level) VALUES ($1, 1) RETURNING current_level",
                    quote_ident(&self.table_name)
                );
                let insert_res = client.query_one(&insert_sql, &[&entity]).await;
                match insert_res {
                    Ok(r) => {
                        let level: i64 = r.try_get(0)?;
                        level
                    },
                    Err(_) => {
                        let row = client.query_one(&update_sql, &[&entity]).await?;
                        let level: i64 = row.try_get(0)?;
                        level
                    }
                }
            }
        };

        u64::try_from(id).map_err(|_| {
            MutationExecutorError::Bind(format!("generated id {id} cannot be represented as u64"))
        })
    }
}

impl InternalIdGenerator for PgIdSpaceGenerator {
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

/// Strip wrapping identifier quotes from a SQL identifier so that bare column
/// names returned by `information_schema.columns` can be compared with
/// potentially-quoted `PropertyDescriptor::column_name` values.
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

fn try_parse_datetime_from_str(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&chrono::Utc));
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc));
    }
    if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let ndt = nd.and_hms_opt(0, 0, 0)?;
        return Some(chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc));
    }
    None
}

struct PgArgs {
    values: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
}
impl PgArgs {
    fn add<T: tokio_postgres::types::ToSql + Sync + Send + 'static>(&mut self, v: T) {
        self.values.push(Box::new(v));
    }
    fn as_refs(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
        self.values.iter().map(|b| b.as_ref() as _).collect()
    }
}

fn bind_pg(args: &mut PgArgs, value: &Value) -> Result<(), MutationExecutorError> {
    match value {
        Value::Null => {
            args.add(Option::<i32>::None);
        }
        Value::Bool(v) => args.add(*v),
        Value::I64(v) => args.add(*v),
        Value::U64(v) => {
            let v = i64::try_from(*v).map_err(|_| {
                MutationExecutorError::Bind(format!("u64 value {v} exceeds i64 range"))
            })?;
            args.add(v);
        }
        Value::F64(v) => args.add(*v),
        Value::Decimal(v) => args.add(*v),
        Value::Text(v) => {
            if let Some(dt) = try_parse_datetime_from_str(v) {
                args.add(dt);
            } else {
                args.add(v.clone());
            }
        }
        Value::Json(v) => {
            let j_val: serde_json::Value = serde_json::to_value(v).map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
            args.add(j_val);
        }
        Value::Date(v) => args.add(*v),
        Value::Timestamp(v) => args.add(*v),
        Value::Object(_) => return Err(MutationExecutorError::UnsupportedValue("object")),
        Value::List(values) => bind_pg_list(args, values)?,
    }
    Ok(())
}

fn bind_pg_list(args: &mut PgArgs, values: &[Value]) -> Result<(), MutationExecutorError> {
    let Some(first) = values.first() else {
        return Err(MutationExecutorError::UnsupportedValue("empty list"));
    };
    match first {
        Value::Bool(_) => {
            let values = values
                .iter()
                .map(|value| match value {
                    Value::Bool(value) => Ok(*value),
                    _ => Err(MutationExecutorError::UnsupportedValue("mixed bool list")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            args.add(values);
        }
        Value::I64(_) => {
            let values = values
                .iter()
                .map(|value| match value {
                    Value::I64(value) => Ok(*value),
                    _ => Err(MutationExecutorError::UnsupportedValue("mixed i64 list")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            args.add(values);
        }
        Value::U64(_) => {
            let values = values
                .iter()
                .map(|value| match value {
                    Value::U64(value) => i64::try_from(*value).map_err(|_| {
                        MutationExecutorError::Bind(format!("u64 value {value} exceeds i64 range"))
                    }),
                    _ => Err(MutationExecutorError::UnsupportedValue("mixed u64 list")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            args.add(values);
        }
        Value::F64(_) => {
            let values = values
                .iter()
                .map(|value| match value {
                    Value::F64(value) => Ok(*value),
                    _ => Err(MutationExecutorError::UnsupportedValue("mixed f64 list")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            args.add(values);
        }
        Value::Decimal(_) => {
            let values = values
                .iter()
                .map(|value| match value {
                    Value::Decimal(value) => Ok(*value),
                    _ => Err(MutationExecutorError::UnsupportedValue(
                        "mixed decimal list",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            args.add(values);
        }
        Value::Text(_) => {
            let values = values
                .iter()
                .map(|value| match value {
                    Value::Text(value) => Ok(value.clone()),
                    _ => Err(MutationExecutorError::UnsupportedValue("mixed text list")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            args.add(values);
        }
        Value::Date(_) => {
            let values = values
                .iter()
                .map(|value| match value {
                    Value::Date(value) => Ok(*value),
                    _ => Err(MutationExecutorError::UnsupportedValue("mixed date list")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            args.add(values);
        }
        Value::Timestamp(_) => {
            let values = values
                .iter()
                .map(|value| match value {
                    Value::Timestamp(value) => Ok(*value),
                    _ => Err(MutationExecutorError::UnsupportedValue(
                        "mixed timestamp list",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            args.add(values);
        }
        Value::Null => return Err(MutationExecutorError::UnsupportedValue("null list")),
        Value::Json(_) => return Err(MutationExecutorError::UnsupportedValue("json list")),
        Value::Object(_) => return Err(MutationExecutorError::UnsupportedValue("object list")),
        Value::List(_) => return Err(MutationExecutorError::UnsupportedValue("nested list")),
    }
    Ok(())
}

fn decode_pg_row(row: &tokio_postgres::Row) -> Result<Record, MutationExecutorError> {
    let mut record = BTreeMap::new();
    for (index, column) in row.columns().iter().enumerate() {
        let name = column.name().to_owned();
        let type_name = column.type_().name().to_ascii_uppercase();
        
        let value = match type_name.as_str() {
            "BOOL" | "BOOLEAN" => {
                let v: Option<bool> = row.try_get(index)?;
                match v {
                    Some(v) => Value::Bool(v),
                    None => Value::Null,
                }
            }
            "INT2" => {
                let v: Option<i16> = row.try_get(index)?;
                match v {
                    Some(v) => Value::I64(v as i64),
                    None => Value::Null,
                }
            }
            "INT4" => {
                let v: Option<i32> = row.try_get(index)?;
                match v {
                    Some(v) => Value::I64(v as i64),
                    None => Value::Null,
                }
            }
            "INT8" => {
                let v: Option<i64> = row.try_get(index)?;
                match v {
                    Some(v) => Value::I64(v),
                    None => Value::Null,
                }
            }
            "FLOAT4" => {
                let v: Option<f32> = row.try_get(index)?;
                match v {
                    Some(v) => Value::F64(v as f64),
                    None => Value::Null,
                }
            }
            "FLOAT8" => {
                let v: Option<f64> = row.try_get(index)?;
                match v {
                    Some(v) => Value::F64(v),
                    None => Value::Null,
                }
            }
            "NUMERIC" => {
                let v: Option<Decimal> = row.try_get(index)?;
                match v {
                    Some(v) => Value::Decimal(v),
                    None => Value::Null,
                }
            }
            "JSON" | "JSONB" => {
                let v: Option<serde_json::Value> = row.try_get(index)?;
                match v {
                    Some(j) => Value::Json(j.into()),
                    None => Value::Null,
                }
            }
            "DATE" => {
                let v: Option<NaiveDate> = row.try_get(index)?;
                match v {
                    Some(v) => Value::Date(v),
                    None => Value::Null,
                }
            }
            "TIMESTAMP" | "TIMESTAMPTZ" => {
                let v: Option<DateTime<Utc>> = row.try_get(index)?;
                match v {
                    Some(v) => Value::Timestamp(v),
                    None => Value::Null,
                }
            }
            "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "UUID" => {
                let v: Option<String> = row.try_get(index)?;
                match v {
                    Some(v) => Value::Text(v),
                    None => Value::Null,
                }
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
    fn postgres_dialect_compiles_mutations_with_numbered_placeholders() {
        let insert = PostgresDialect
            .compile_insert(
                &entity(),
                &InsertCommand::new("Order")
                    .value("id", 1_u64)
                    .value("name", "A"),
            )
            .unwrap();
        assert_eq!(
            insert.sql,
            "INSERT INTO orders (id, name) VALUES ($1, $2)"
        );

        let update = PostgresDialect
            .compile_update(
                &entity(),
                &UpdateCommand::new("Order", 1_u64)
                    .expected_version(3)
                    .value("name", "B"),
            )
            .unwrap();
        assert_eq!(
            update.sql,
            "UPDATE orders SET name = $1, version = $2 WHERE id = $3 AND version = $4"
        );

        let delete = PostgresDialect
            .compile_delete(
                &entity(),
                &DeleteCommand::new("Order", 1_u64).expected_version(3),
            )
            .unwrap();
        let recover = PostgresDialect
            .compile_recover(&entity(), &RecoverCommand::new("Order", 1_u64, -4))
            .unwrap();
        assert_eq!(
            delete.sql,
            "UPDATE orders SET version = $1 WHERE id = $2 AND version = $3"
        );
        assert_eq!(
            recover.sql,
            "UPDATE orders SET version = $1 WHERE id = $2 AND version = $3"
        );
    }

    #[test]
    fn postgres_dialect_compiles_schema_and_large_in_array_binds() {
        let create = PostgresDialect.compile_create_table(&entity()).unwrap();
        assert_eq!(
            create,
            "CREATE TABLE IF NOT EXISTS orders (id BIGINT PRIMARY KEY NOT NULL, version BIGINT NOT NULL, name TEXT)"
        );
        assert!(
            PostgresDialect
                .schema_setup_sqls()
                .iter()
                .any(|sql| sql.contains("CREATE OR REPLACE FUNCTION soundex"))
        );

        let query = PostgresDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order")
                    .filter(Expr::in_large(
                        "id",
                        vec![Value::from(1_u64), Value::from(2_u64)],
                    ))
                    .order_asc("id"),
            )
            .unwrap();
        assert_eq!(
            query.sql,
            "SELECT id, version, name FROM orders WHERE (id = ANY($1)) ORDER BY id ASC"
        );
        assert_eq!(
            query.params,
            vec![Value::List(vec![Value::U64(1), Value::U64(2)])]
        );
    }
}
