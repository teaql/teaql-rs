use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use rusqlite::types::{Value as SqliteValue, ValueRef};
use rusqlite::{Connection, Row, params_from_iter};
use rust_decimal::Decimal;
use teaql_core::{
    DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, Record, SelectQuery,
    UpdateCommand, Value,
};
use teaql_runtime::{
    EntityEvent, GraphNode, InternalIdGenerator,
    RuntimeError, SchemaProvider, UserContext,
};
use teaql_sql::{
    CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect,
    SqlTransport, quote_identifier_if_needed,
};

pub const DEFAULT_ID_SPACE_TABLE: &str = "teaql_id_space";

const SQLITE_DECIMAL_PREFIX: &str = "__teaql_decimal__:";

#[derive(Debug, Default, Clone, Copy)]
pub struct RusqliteDialect;

impl SqlDialect for RusqliteDialect {
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
    Rusqlite(rusqlite::Error),
    SqlCompile(SqlCompileError),
    UnsupportedValue(&'static str),
    UnsupportedColumnType(String),
    Bind(String),
    Lock(String),
}

impl std::fmt::Display for MutationExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rusqlite(err) => err.fmt(f),
            Self::SqlCompile(err) => err.fmt(f),
            Self::UnsupportedValue(kind) => {
                write!(
                    f,
                    "unsupported rusqlite bind value for mutation executor: {kind}"
                )
            }
            Self::UnsupportedColumnType(kind) => {
                write!(
                    f,
                    "unsupported rusqlite column type for record decoding: {kind}"
                )
            }
            Self::Bind(message) => write!(f, "rusqlite bind error: {message}"),
            Self::Lock(message) => write!(f, "rusqlite connection lock error: {message}"),
        }
    }
}

impl std::error::Error for MutationExecutorError {}

impl From<rusqlite::Error> for MutationExecutorError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Rusqlite(value)
    }
}

impl From<SqlCompileError> for MutationExecutorError {
    fn from(value: SqlCompileError) -> Self {
        Self::SqlCompile(value)
    }
}

#[derive(Clone)]
pub struct RusqliteMutationExecutor {
    connection: Arc<Mutex<Connection>>,
}

impl RusqliteMutationExecutor {
    pub fn new(connection: Connection) -> Self {
        Self::from_shared_connection(Arc::new(Mutex::new(connection)))
    }

    pub fn from_shared_connection(connection: Arc<Mutex<Connection>>) -> Self {
        Self { connection }
    }

    pub fn connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.connection)
    }

    pub fn ensure_schema(
        &self,
        dialect: &RusqliteDialect,
        entities: &[&EntityDescriptor],
    ) -> Result<(), MutationExecutorError> {
        self.ensure_id_space_table(DEFAULT_ID_SPACE_TABLE)?;

        for entity in entities {
            if !self.table_exists(&entity.table_name)? {
                let sql = dialect.compile_create_table(entity)?;
                self.lock()?.execute(&sql, [])?;
                continue;
            }

            let existing_columns = self.table_columns(&entity.table_name)?;
            for property in &entity.properties {
                let bare_column = strip_identifier_quotes(&property.column_name).to_lowercase();
                if existing_columns.contains(&bare_column) {
                    continue;
                }
                let sql = dialect.compile_add_column(entity, property)?;
                self.lock()?.execute(&sql, [])?;
            }
        }
        Ok(())
    }

    pub fn ensure_id_space_table(&self, table_name: &str) -> Result<(), MutationExecutorError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (type_name VARCHAR(100) PRIMARY KEY, current_level BIGINT NOT NULL)",
            quote_ident(table_name)
        );
        self.lock()?.execute(&sql, [])?;
        Ok(())
    }

    pub fn begin_transaction(&self) -> Result<(), MutationExecutorError> {
        self.lock()?.execute("BEGIN IMMEDIATE", [])?;
        Ok(())
    }

    pub fn commit_transaction(&self) -> Result<(), MutationExecutorError> {
        self.lock()?.execute("COMMIT", [])?;
        Ok(())
    }

    pub fn rollback_transaction(&self) -> Result<(), MutationExecutorError> {
        self.lock()?.execute("ROLLBACK", [])?;
        Ok(())
    }

    pub fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
        let params = bind_values(&query.params)?;
        let rows = self
            .lock()?
            .execute(&query.sql_with_comment(), params_from_iter(params.iter()))?;
        Ok(rows as u64)
    }

    pub fn fetch_all(&self, query: &CompiledQuery) -> Result<Vec<Record>, MutationExecutorError> {
        let params = bind_values(&query.params)?;
        let connection = self.lock()?;
        let mut statement = connection.prepare(&query.sql_with_comment())?;
        let columns = statement_columns(&statement);
        let mut rows = statement.query(params_from_iter(params.iter()))?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            records.push(decode_sqlite_row(row, &columns)?);
        }
        Ok(records)
    }

    pub fn table_exists(&self, table_name: &str) -> Result<bool, MutationExecutorError> {
        let exists: i64 = self.lock()?.query_row(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?",
            [table_name],
            |row| row.get(0),
        )?;
        Ok(exists > 0)
    }

    pub fn table_columns(&self, table_name: &str) -> Result<BTreeSet<String>, MutationExecutorError> {
        let pragma_sql = format!("PRAGMA table_info({})", quote_ident(table_name));
        let connection = self.lock()?;
        let mut statement = connection.prepare(&pragma_sql)?;
        let rows = statement.query_map([], |row| row.get::<_, String>("name"))?;
        let mut columns = BTreeSet::new();
        for row in rows {
            columns.insert(row?.to_lowercase());
        }
        Ok(columns)
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>, MutationExecutorError> {
        self.connection
            .lock()
            .map_err(|err| MutationExecutorError::Lock(err.to_string()))
    }
}


impl SqlTransport for RusqliteMutationExecutor {
    type Error = MutationExecutorError;

    async fn fetch_all_sql(&self, query: &CompiledQuery) -> Result<Vec<Record>, Self::Error> {
        RusqliteMutationExecutor::fetch_all(self, query)
    }

    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        RusqliteMutationExecutor::execute(self, query)
    }
}

impl teaql_sql::SqlTransaction for RusqliteMutationExecutor {
    type Error = MutationExecutorError;

    async fn commit_sql(self) -> Result<(), Self::Error> {
        self.commit_transaction()
    }

    async fn rollback_sql(self) -> Result<(), Self::Error> {
        self.rollback_transaction()
    }
}

impl teaql_sql::SqlTransactionTransport for RusqliteMutationExecutor {
    type Tx<'a> = Self where Self: 'a;

    async fn begin_sql(&self) -> Result<Self::Tx<'_>, Self::Error> {
        self.begin_transaction()?;
        Ok(self.clone())
    }
}



fn initial_graph_exists_rusqlite(
    executor: &RusqliteMutationExecutor,
    dialect: &RusqliteDialect,
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
    Ok(!executor.fetch_all(&query)?.is_empty())
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
    graph: &GraphNode,
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

pub trait RusqliteSchemaExt {
    fn ensure_rusqlite_schema(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MutationExecutorError>> + Send + '_>>;
}

pub fn ensure_rusqlite_schema_for(ctx: &UserContext) -> Result<(), MutationExecutorError> {
    let dialect = ctx.get_resource::<RusqliteDialect>().ok_or_else(|| {
        MutationExecutorError::Bind("missing typed resource: RusqliteDialect".to_owned())
    })?;
    let executor = ctx
        .get_resource::<RusqliteMutationExecutor>()
        .ok_or_else(|| {
            MutationExecutorError::Bind(
                "missing typed resource: RusqliteMutationExecutor".to_owned(),
            )
        })?;

    let entities = ctx.all_entities();

    // Ensure id space table exists
    executor.ensure_id_space_table(DEFAULT_ID_SPACE_TABLE)?;

    // Process each entity table individually with granular events
    for entity in &entities {
        let field_count = entity.properties.len();
        if !executor.table_exists(&entity.table_name)? {
            // New table: create it
            let sql = dialect.compile_create_table(entity)?;
            executor.lock()?.execute(&sql, [])?;
            let _ = ctx.send_event(EntityEvent::schema_created(
                &entity.name,
                &entity.table_name,
                field_count,
            ));
        } else {
            // Existing table: check for missing columns
            let existing_columns = executor.table_columns(&entity.table_name)?;
            let mut fields_added = 0;
            for property in &entity.properties {
                let bare_column = strip_identifier_quotes(&property.column_name).to_lowercase();
                if existing_columns.contains(&bare_column) {
                    continue;
                }
                let sql = dialect.compile_add_column(entity, property)?;
                executor.lock()?.execute(&sql, [])?;
                let _ = ctx.send_event(EntityEvent::field_added(
                    &entity.name,
                    &entity.table_name,
                    &property.column_name,
                ));
                fields_added += 1;
            }
            let _ = ctx.send_event(EntityEvent::schema_verified(
                &entity.name,
                &entity.table_name,
                field_count,
            ));
            let _ = fields_added; // used above for FieldAdded events
        }
    }

    // Seed initial data, tracking insert vs update counts per entity
    let mut seed_counts: BTreeMap<String, (usize, usize)> = BTreeMap::new(); // (inserted, updated)
    for graph in ctx.initial_graphs() {
        let entity = ctx.entity(&graph.entity).ok_or_else(|| {
            MutationExecutorError::Bind(format!("missing entity: {}", graph.entity))
        })?;
        let counts = seed_counts.entry(graph.entity.clone()).or_insert((0, 0));
        if initial_graph_exists_rusqlite(executor, dialect, entity, graph)? {
            if let Some(query) = compile_initial_graph_update(dialect, entity, graph)? {
                executor.execute(&query)?;
            }
            counts.1 += 1; // updated
        } else {
            let query = compile_initial_graph_insert(dialect, entity, graph)?;
            executor.execute(&query)?;
            counts.0 += 1; // inserted
        }
    }

    // Fire DataSeeded events per entity type
    for (entity_name, (inserted, updated)) in &seed_counts {
        let entity = ctx.entity(entity_name).ok_or_else(|| {
            MutationExecutorError::Bind(format!("missing entity: {}", entity_name))
        })?;
        let _ = ctx.send_event(EntityEvent::data_seeded(
            entity_name,
            &entity.table_name,
            *inserted,
            *updated,
        ));
    }

    Ok(())
}

impl RusqliteSchemaExt for UserContext {
    fn ensure_rusqlite_schema(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MutationExecutorError>> + Send + '_>> {
        Box::pin(async move { ensure_rusqlite_schema_for(self) })
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RusqliteSchemaProvider;

impl SchemaProvider for RusqliteSchemaProvider {
    fn ensure_schema<'a>(
        &'a self,
        ctx: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            ensure_rusqlite_schema_for(ctx).map_err(|err| RuntimeError::Schema(err.to_string()))
        })
    }
}

pub trait RusqliteProviderExt {
    fn use_rusqlite_provider(&mut self, executor: RusqliteMutationExecutor) -> &mut Self;
}

impl RusqliteProviderExt for UserContext {
    fn use_rusqlite_provider(&mut self, executor: RusqliteMutationExecutor) -> &mut Self {
        self.insert_resource(RusqliteDialect);
        self.insert_resource(executor);
        self.set_schema_provider(RusqliteSchemaProvider);
        self
    }
}

#[derive(Clone)]
pub struct RusqliteIdSpaceGenerator {
    executor: RusqliteMutationExecutor,
    table_name: String,
}

impl RusqliteIdSpaceGenerator {
    pub fn new(connection: Connection) -> Self {
        Self::from_executor(RusqliteMutationExecutor::new(connection))
    }

    pub fn from_executor(executor: RusqliteMutationExecutor) -> Self {
        Self {
            executor,
            table_name: DEFAULT_ID_SPACE_TABLE.to_owned(),
        }
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    pub fn ensure_table(&self) -> Result<(), MutationExecutorError> {
        self.executor.ensure_id_space_table(&self.table_name)
    }

    pub fn next_id(&self, entity: &str) -> Result<u64, MutationExecutorError> {
        self.ensure_table()?;
        let sql = format!(
            "INSERT INTO {} (type_name, current_level) VALUES (?, 1) \
             ON CONFLICT (type_name) DO UPDATE \
             SET current_level = current_level + 1 \
             RETURNING current_level",
            quote_ident(&self.table_name)
        );
        let id: i64 = self
            .executor
            .lock()?
            .query_row(&sql, [entity], |row| row.get(0))?;
        u64::try_from(id).map_err(|_| {
            MutationExecutorError::Bind(format!("generated id {id} cannot be represented as u64"))
        })
    }
}

impl InternalIdGenerator for RusqliteIdSpaceGenerator {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        self.next_id(entity)
            .map_err(|err| RuntimeError::IdGeneration(err.to_string()))
    }
}

fn quote_ident(ident: &str) -> String {
    quote_identifier_if_needed(ident, '"')
}

/// Strip wrapping identifier quotes from a SQL identifier.
///
/// SQLite `PRAGMA table_info` returns bare column names (e.g. `description`),
/// but generated `PropertyDescriptor::column_name` may carry quotes
/// (e.g. `"description"`) when the name is a reserved keyword.  This helper
/// normalises the column name so the two can be compared correctly during
/// schema migration.
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

fn bind_values(values: &[Value]) -> Result<Vec<SqliteValue>, MutationExecutorError> {
    values.iter().map(bind_sqlite_value).collect()
}

fn bind_sqlite_value(value: &Value) -> Result<SqliteValue, MutationExecutorError> {
    match value {
        Value::Null => Ok(SqliteValue::Null),
        Value::Bool(v) => Ok(SqliteValue::Integer(i64::from(*v))),
        Value::I64(v) => Ok(SqliteValue::Integer(*v)),
        Value::U64(v) => i64::try_from(*v)
            .map(SqliteValue::Integer)
            .map_err(|_| MutationExecutorError::Bind(format!("u64 value {v} exceeds i64 range"))),
        Value::F64(v) => Ok(SqliteValue::Real(*v)),
        Value::Decimal(v) => Ok(SqliteValue::Text(format!("{SQLITE_DECIMAL_PREFIX}{v}"))),
        Value::Text(v) => Ok(SqliteValue::Text(v.clone())),
        Value::Json(v) => Ok(SqliteValue::Text(v.to_string())),
        Value::Date(v) => Ok(SqliteValue::Text(v.format("%Y-%m-%d").to_string())),
        Value::Timestamp(v) => Ok(SqliteValue::Text(v.to_rfc3339())),
        Value::Object(_) => Err(MutationExecutorError::UnsupportedValue("object")),
        Value::List(_) => Err(MutationExecutorError::UnsupportedValue("list")),
    }
}

#[derive(Debug, Clone)]
struct ColumnInfo {
    name: String,
    decl_type: Option<String>,
}

fn statement_columns(statement: &rusqlite::Statement<'_>) -> Vec<ColumnInfo> {
    statement
        .columns()
        .into_iter()
        .map(|column| ColumnInfo {
            name: column.name().to_owned(),
            decl_type: column.decl_type().map(|value| value.to_ascii_uppercase()),
        })
        .collect()
}

fn decode_sqlite_row(
    row: &Row<'_>,
    columns: &[ColumnInfo],
) -> Result<Record, MutationExecutorError> {
    let mut record = BTreeMap::new();
    for (index, column) in columns.iter().enumerate() {
        let value_ref = row.get_ref(index)?;
        let value = match value_ref {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(value) => decode_sqlite_integer(value, column),
            ValueRef::Real(value) => Value::F64(value),
            ValueRef::Text(value) => decode_sqlite_text(value, column)?,
            ValueRef::Blob(_) => {
                return Err(MutationExecutorError::UnsupportedColumnType(
                    "BLOB".to_owned(),
                ));
            }
        };
        record.insert(column.name.clone(), value);
    }
    Ok(record)
}

fn decode_sqlite_integer(value: i64, column: &ColumnInfo) -> Value {
    match column_decl_type(column).as_deref() {
        Some("BOOLEAN") | Some("BOOL") => Value::Bool(value != 0),
        _ => Value::I64(value),
    }
}

fn decode_sqlite_text(value: &[u8], column: &ColumnInfo) -> Result<Value, MutationExecutorError> {
    let value = std::str::from_utf8(value)
        .map_err(|err| MutationExecutorError::Bind(format!("invalid sqlite text: {err}")))?;
    if let Some(decimal) = value.strip_prefix(SQLITE_DECIMAL_PREFIX) {
        return Decimal::from_str(decimal)
            .map(Value::Decimal)
            .map_err(|err| MutationExecutorError::Bind(format!("invalid sqlite decimal: {err}")));
    }

    match column_decl_type(column).as_deref() {
        Some("NUMERIC") | Some("DECIMAL") => Decimal::from_str(value)
            .map(Value::Decimal)
            .map_err(|err| MutationExecutorError::Bind(format!("invalid sqlite decimal: {err}"))),
        Some("JSON") => serde_json::from_str(value).map(Value::Json).map_err(|err| {
            MutationExecutorError::Bind(format!("invalid sqlite json value: {err}"))
        }),
        Some("DATE") => NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|err| MutationExecutorError::Bind(format!("invalid sqlite date: {err}"))),
        Some("TIMESTAMP") | Some("DATETIME") => parse_sqlite_timestamp(value),
        _ => infer_sqlite_text(value),
    }
}

fn infer_sqlite_text(value: &str) -> Result<Value, MutationExecutorError> {
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(Value::Date(date));
    }
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(Value::Timestamp(timestamp.with_timezone(&Utc)));
    }
    if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(Value::Timestamp(Utc.from_utc_datetime(&timestamp)));
    }
    Ok(Value::Text(value.to_owned()))
}

fn parse_sqlite_timestamp(value: &str) -> Result<Value, MutationExecutorError> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(Value::Timestamp(timestamp.with_timezone(&Utc)));
    }
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .map(|timestamp| Value::Timestamp(Utc.from_utc_datetime(&timestamp)))
        .map_err(|err| MutationExecutorError::Bind(format!("invalid sqlite timestamp: {err}")))
}

fn column_decl_type(column: &ColumnInfo) -> Option<String> {
    column
        .decl_type
        .as_ref()
        .map(|value| value.split('(').next().unwrap_or(value).trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use teaql_core::{DeleteCommand, RecoverCommand};
    use teaql_runtime::InMemoryMetadataStore;

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
    fn rusqlite_dialect_compiles_mutations_and_schema() {
        let insert = RusqliteDialect
            .compile_insert(
                &entity(),
                &InsertCommand::new("Order")
                    .value("id", 1_u64)
                    .value("name", "A"),
            )
            .unwrap();
        assert_eq!(insert.sql, "INSERT INTO orders (id, name) VALUES (?, ?)");

        let update = RusqliteDialect
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

        let delete = RusqliteDialect
            .compile_delete(
                &entity(),
                &DeleteCommand::new("Order", 1_u64).expected_version(3),
            )
            .unwrap();
        let recover = RusqliteDialect
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

        let create = RusqliteDialect.compile_create_table(&entity()).unwrap();
        assert_eq!(
            create,
            "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY NOT NULL, version INTEGER NOT NULL, name TEXT)"
        );
    }

    #[test]
    fn rusqlite_executor_ensures_schema_and_roundtrips_rows() {
        let executor = RusqliteMutationExecutor::new(Connection::open_in_memory().unwrap());
        let entity = entity();
        let mut ctx = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity.clone()));

        ctx.use_rusqlite_provider(executor.clone());
        ensure_rusqlite_schema_for(&ctx).unwrap();

        let insert = RusqliteDialect
            .compile_insert(
                &entity,
                &InsertCommand::new("Order")
                    .value("id", 1_u64)
                    .value("version", 1_i64)
                    .value("name", "draft"),
            )
            .unwrap();
        assert_eq!(executor.execute(&insert).unwrap(), 1);

        let select = RusqliteDialect
            .compile_select(
                &entity,
                &SelectQuery::new("Order")
                    .filter(Expr::eq("id", 1_u64))
                    .order_asc("id"),
            )
            .unwrap();
        let rows = executor.fetch_all(&select).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("version"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("name"), Some(&Value::Text("draft".to_owned())));
    }

    #[test]
    fn rusqlite_executor_parses_json_only_for_json_columns() {
        let executor = RusqliteMutationExecutor::new(Connection::open_in_memory().unwrap());

        executor
            .execute(&CompiledQuery {
                sql: "CREATE TABLE payloads (text_payload TEXT, json_payload JSON)".to_owned(),
                params: Vec::new(),
                comment: None,
            })
            .unwrap();
        executor
            .execute(&CompiledQuery {
                sql: "INSERT INTO payloads (text_payload, json_payload) VALUES (?, ?)".to_owned(),
                params: vec![
                    Value::Text("{\"active\":true}".to_owned()),
                    Value::Json(serde_json::json!({"active": true})),
                ],
                comment: None,
            })
            .unwrap();

        let rows = executor
            .fetch_all(&CompiledQuery {
                sql: "SELECT text_payload, json_payload FROM payloads".to_owned(),
                params: Vec::new(),
                comment: None,
            })
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

    #[test]
    fn rusqlite_id_space_generator_increments_ids() {
        let executor = RusqliteMutationExecutor::new(Connection::open_in_memory().unwrap());
        let generator = RusqliteIdSpaceGenerator::from_executor(executor);
        assert_eq!(generator.next_id("Order").unwrap(), 1);
        assert_eq!(generator.next_id("Order").unwrap(), 2);
    }
}
