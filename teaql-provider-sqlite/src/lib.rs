use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use rusqlite::types::{Value as SqliteValue, ValueRef};
use rusqlite::{Connection, OptionalExtension, Row, params, params_from_iter};
use rust_decimal::Decimal;
use teaql_core::{
    CompactRow, DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, SelectQuery,
    UpdateCommand, Value,
};
use teaql_runtime::{
    GraphNode, InternalIdGenerator, RawAuditEvent, RuntimeError, SchemaProvider, UserContext,
};
use teaql_sql::{
    CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect, SqlTransport,
    quote_identifier_if_needed,
};

pub const DEFAULT_ID_SPACE_TABLE: &str = "teaql_id_space";

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
            DataType::Bool => Ok("BOOLEAN"),
            DataType::I64 | DataType::U64 if property.is_id => Ok("INTEGER"),
            DataType::I64 | DataType::U64 => Ok("INTEGER"),
            DataType::F64 => Ok("REAL"),
            DataType::Decimal => Ok("NUMERIC"),
            DataType::Text => Ok("VARCHAR(255)"),
            DataType::LargeText => Ok("TEXT"),
            DataType::Json => Ok("JSON"),
            DataType::Date => Ok("DATE"),
            DataType::Timestamp => Ok("TIMESTAMP"),
        }
    }

    fn compile_add_column(
        &self,
        entity: &EntityDescriptor,
        property: &PropertyDescriptor,
    ) -> Result<String, SqlCompileError> {
        // SQLite does not support adding NOT NULL columns without a DEFAULT.
        // Since TeaQL enforces nullability at the application layer, we can safely
        // strip the NOT NULL constraint when adding columns to existing tables.
        let def = self.column_definition_sql(property)?;
        let def_without_not_null = def.replace(" NOT NULL", "");

        Ok(format!(
            "ALTER TABLE {} ADD COLUMN {}",
            self.quote_ident(&entity.table_name),
            def_without_not_null
        ))
    }
}

#[derive(Debug)]
pub enum MutationExecutorError {
    Sqlite(rusqlite::Error),
    SqlCompile(SqlCompileError),
    UnsupportedValue(&'static str),
    UnsupportedColumnType(String),
    Bind(String),
    Lock(String),
}

impl std::fmt::Display for MutationExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(err) => err.fmt(f),
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
        Self::Sqlite(value)
    }
}

impl From<SqlCompileError> for MutationExecutorError {
    fn from(value: SqlCompileError) -> Self {
        Self::SqlCompile(value)
    }
}

#[derive(Clone)]
pub struct SqliteMutationExecutor {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteMutationExecutor {
    pub fn new(connection: Arc<Mutex<Connection>>) -> Self {
        Self { connection }
    }

    pub fn from_connection(connection: Connection) -> Self {
        Self::new(Arc::new(Mutex::new(connection)))
    }

    pub fn connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.connection)
    }

    pub fn ensure_schema(
        &self,
        dialect: &SqliteDialect,
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

            for sql in dialect.schema_indexes_sqls(entity)? {
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

    pub fn fetch_all_compact(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, MutationExecutorError> {
        let params = bind_values(&query.params)?;
        let connection = self.lock()?;
        let mut statement = connection.prepare(&query.sql_with_comment())?;
        let columns = statement_columns(&statement);
        let column_names: Arc<[String]> = columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
            .into();
        let mut rows = statement.query(params_from_iter(params.iter()))?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            result.push(CompactRow::new(
                column_names.clone(),
                decode_sqlite_values(row, &columns)?,
            ));
        }
        Ok(result)
    }

    /// Fetch rows in streaming mode (chunked).
    /// Returns a Vec of StreamChunk, each containing up to `chunk_size` rows.
    pub fn fetch_stream(
        &self,
        query: &CompiledQuery,
        chunk_size: usize,
    ) -> Result<Vec<teaql_data_service::StreamChunk>, MutationExecutorError> {
        let params = bind_values(&query.params)?;
        let connection = self.lock()?;
        let mut statement = connection.prepare(&query.sql_with_comment())?;
        let columns = statement_columns(&statement);
        let column_names: Arc<[String]> = columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
            .into();
        let mut rows = statement.query(params_from_iter(params.iter()))?;

        let mut chunks = Vec::new();
        let mut current_chunk = Vec::new();
        let mut chunk_index = 0;

        while let Some(row) = rows.next()? {
            current_chunk.push(CompactRow::new(
                column_names.clone(),
                decode_sqlite_values(row, &columns)?,
            ));
            if current_chunk.len() >= chunk_size {
                chunks.push(teaql_data_service::StreamChunk {
                    rows: current_chunk,
                    chunk_index,
                    is_last: false,
                });
                current_chunk = Vec::new();
                chunk_index += 1;
            }
        }

        // Push the final chunk (may be empty if exactly aligned)
        chunks.push(teaql_data_service::StreamChunk {
            rows: current_chunk,
            chunk_index,
            is_last: true,
        });

        Ok(chunks)
    }

    pub fn table_exists(&self, table_name: &str) -> Result<bool, MutationExecutorError> {
        let exists: i64 = self.lock()?.query_row(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?",
            [table_name],
            |row| row.get(0),
        )?;
        Ok(exists > 0)
    }

    pub fn table_columns(
        &self,
        table_name: &str,
    ) -> Result<BTreeSet<String>, MutationExecutorError> {
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

impl teaql_data_service::DataServiceExecutor for SqliteMutationExecutor {
    type Error = MutationExecutorError;

    fn capabilities(&self) -> teaql_data_service::DataServiceCapabilities {
        teaql_data_service::DataServiceCapabilities {
            query: true,
            mutation: true,
            transaction: true,
            schema: true,
            id_generation: true,
            ..Default::default()
        }
    }
}

impl SqlTransport for SqliteMutationExecutor {
    type Error = MutationExecutorError;

    async fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, Self::Error> {
        SqliteMutationExecutor::fetch_all_compact(self, query)
    }

    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        SqliteMutationExecutor::execute(self, query)
    }
}

impl teaql_sql::StreamingSqlTransport for SqliteMutationExecutor {
    // A rusqlite statement and its rows borrow the guarded connection for the
    // lifetime of the stream. This stream is intentionally local/non-Send, so
    // retaining the synchronous guard across yields is required and safe.
    #[allow(clippy::await_holding_lock)]
    fn stream_sql(
        &self,
        query: CompiledQuery,
        chunk_size: usize,
    ) -> teaql_data_service::QueryStream<'_, Self::Error> {
        let connection = self.connection.clone();
        Box::pin(async_stream::try_stream! {
            let params = bind_values(&query.params)?;
            let guard = connection.lock().map_err(|err| MutationExecutorError::Lock(err.to_string()))?;
            let mut statement = guard.prepare(&query.sql_with_comment())?;
            let columns = statement_columns(&statement);
            let column_names: Arc<[String]> = columns.iter().map(|column| column.name.clone()).collect::<Vec<_>>().into();
            let mut rows = statement.query(params_from_iter(params.iter()))?;
            let mut chunk = Vec::with_capacity(chunk_size); let mut index = 0;
            while let Some(row) = rows.next()? {
                chunk.push(CompactRow::new(column_names.clone(), decode_sqlite_values(row, &columns)?));
                if chunk.len() == chunk_size { yield teaql_data_service::StreamChunk { rows: std::mem::take(&mut chunk), chunk_index: index, is_last: false }; index += 1; }
            }
            if !chunk.is_empty() { yield teaql_data_service::StreamChunk { rows: chunk, chunk_index: index, is_last: true }; }
        })
    }
}

impl teaql_data_service::StreamQueryExecutor for SqliteMutationExecutor {
    fn query_stream(
        &self,
        request: teaql_data_service::QueryRequest,
        chunk_size: usize,
    ) -> teaql_data_service::QueryStream<'_, Self::Error> {
        let dialect = SqliteDialect;
        // Use a dummy entity descriptor for compilation
        let entity_desc = teaql_core::EntityDescriptor::new(&request.query.entity);
        match dialect.compile_select(&entity_desc, &request.query) {
            Ok(compiled) => {
                teaql_sql::StreamingSqlTransport::stream_sql(self, compiled, chunk_size)
            }
            Err(error) => Box::pin(futures_util::stream::once(async {
                Err(MutationExecutorError::SqlCompile(error))
            })),
        }
    }
}

impl teaql_sql::SqlTransaction for SqliteMutationExecutor {
    type Error = MutationExecutorError;

    async fn commit_sql(self) -> Result<(), Self::Error> {
        self.commit_transaction()
    }

    async fn rollback_sql(self) -> Result<(), Self::Error> {
        self.rollback_transaction()
    }
}

impl teaql_sql::SqlTransactionTransport for SqliteMutationExecutor {
    type Tx<'a>
        = Self
    where
        Self: 'a;

    async fn begin_sql(&self) -> Result<Self::Tx<'_>, Self::Error> {
        self.begin_transaction()?;
        Ok(self.clone())
    }
}

fn initial_graph_exists_sqlite(
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
    Ok(!executor.fetch_all_compact(&query)?.is_empty())
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
        if field != "id" {
            command = command.value(field.clone(), value.clone());
        }
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
    ) -> Pin<Box<dyn Future<Output = Result<(), MutationExecutorError>> + Send + '_>>;
}

pub fn ensure_sqlite_schema_for(context: &UserContext) -> Result<(), MutationExecutorError> {
    let dialect = context.get_resource::<SqliteDialect>().ok_or_else(|| {
        MutationExecutorError::Bind("missing typed resource: SqliteDialect".to_owned())
    })?;
    let executor = context
        .get_resource::<SqliteMutationExecutor>()
        .ok_or_else(|| {
            MutationExecutorError::Bind("missing typed resource: SqliteMutationExecutor".to_owned())
        })?;

    let entities = context.all_entities();

    // Ensure id space table exists
    executor.ensure_id_space_table(DEFAULT_ID_SPACE_TABLE)?;

    // Process each entity table individually with granular events
    for entity in &entities {
        let field_count = entity.properties.len();
        if !executor.table_exists(&entity.table_name)? {
            // New table: create it
            let sql = dialect.compile_create_table(entity)?;
            executor.lock()?.execute(&sql, [])?;
            let _ = context.send_event(RawAuditEvent::schema_created(
                &entity.name,
                &entity.table_name,
                field_count,
            ));
            continue;
        }
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
            let _ = context.send_event(RawAuditEvent::field_added(
                &entity.name,
                &entity.table_name,
                &property.column_name,
            ));
            fields_added += 1;
        }
        let _ = context.send_event(RawAuditEvent::schema_verified(
            &entity.name,
            &entity.table_name,
            field_count,
        ));
        let _ = fields_added; // used above for FieldAdded events
    }

    // Constant graphs are reconciled so model changes are propagated.
    let id_generator = SqliteIdSpaceGenerator::from_executor(executor.clone());
    let mut seed_counts: BTreeMap<String, (usize, usize)> = BTreeMap::new(); // (inserted, updated)
    for graph in context.initial_graphs() {
        let entity = context.entity(&graph.entity).ok_or_else(|| {
            MutationExecutorError::Bind(format!("missing entity: {}", graph.entity))
        })?;
        let counts = seed_counts.entry(graph.entity.clone()).or_insert((0, 0));
        if initial_graph_exists_sqlite(executor, dialect, entity, graph)? {
            if let Some(query) = compile_initial_graph_update(dialect, entity, graph)? {
                executor.execute(&query)?;
            }
            counts.1 += 1;
            if let Some(id) = graph.values.get("id").and_then(Value::try_u64) {
                id_generator.ensure_floor(&graph.entity, id)?;
            }
            continue;
        }
        let query = compile_initial_graph_insert(dialect, entity, graph)?;
        executor.execute(&query)?;
        counts.0 += 1; // inserted
        if let Some(id) = graph.values.get("id").and_then(Value::try_u64) {
            id_generator.ensure_floor(&graph.entity, id)?;
        }
    }

    // Roots are create-if-absent. Once present, application-owned values win.
    for graph in context.root_graphs() {
        let entity = context.entity(&graph.entity).ok_or_else(|| {
            MutationExecutorError::Bind(format!("missing entity: {}", graph.entity))
        })?;
        if initial_graph_exists_sqlite(executor, dialect, entity, graph)? {
            if let Some(id) = graph.values.get("id").and_then(Value::try_u64) {
                id_generator.ensure_floor(&graph.entity, id)?;
            }
            continue;
        }
        let query = compile_initial_graph_insert(dialect, entity, graph)?;
        executor.execute(&query)?;
        seed_counts.entry(graph.entity.clone()).or_insert((0, 0)).0 += 1;
        if let Some(id) = graph.values.get("id").and_then(Value::try_u64) {
            id_generator.ensure_floor(&graph.entity, id)?;
        }
    }

    // Fire DataSeeded events per entity type
    for (entity_name, (inserted, updated)) in &seed_counts {
        let entity = context.entity(entity_name).ok_or_else(|| {
            MutationExecutorError::Bind(format!("missing entity: {}", entity_name))
        })?;
        let _ = context.send_event(RawAuditEvent::data_seeded(
            entity_name,
            &entity.table_name,
            *inserted,
            *updated,
        ));
    }

    Ok(())
}

impl SqliteSchemaExt for UserContext {
    fn ensure_sqlite_schema(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), MutationExecutorError>> + Send + '_>> {
        Box::pin(async move { ensure_sqlite_schema_for(self) })
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SqliteSchemaProvider;

impl SchemaProvider for SqliteSchemaProvider {
    fn ensure_schema<'a>(
        &'a self,
        context: &'a UserContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            ensure_sqlite_schema_for(context).map_err(|err| RuntimeError::Schema(err.to_string()))
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
    executor: SqliteMutationExecutor,
    table_name: String,
}

impl SqliteIdSpaceGenerator {
    pub fn new(connection: Connection) -> Self {
        Self::from_executor(SqliteMutationExecutor::from_connection(connection))
    }

    pub fn from_executor(executor: SqliteMutationExecutor) -> Self {
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
        let table = quote_ident(&self.table_name);
        let select_sql = format!("SELECT current_level FROM {table} WHERE type_name = ?");
        let insert_sql = format!(
            "INSERT INTO {table} (type_name, current_level) VALUES (?, 1)"
        );
        let update_sql = format!(
            "UPDATE {table} SET current_level = ? WHERE type_name = ? AND current_level = ?"
        );
        for attempt in 1..=100 {
            let connection = self.executor.lock()?;
            let current = connection
                .query_row(&select_sql, [entity], |row| row.get::<_, i64>(0))
                .optional()?;
            if let Some(current) = current {
                let next = current.checked_add(1).ok_or_else(|| {
                    MutationExecutorError::Bind(format!(
                        "ID space overflow for {entity} on optimistic-lock attempt {attempt}"
                    ))
                })?;
                if connection.execute(&update_sql, params![next, entity, current])? == 1 {
                    return u64::try_from(next).map_err(|_| {
                        MutationExecutorError::Bind(format!(
                            "generated id {next} cannot be represented as u64"
                        ))
                    });
                }
            } else {
                match connection.execute(&insert_sql, params![entity]) {
                    Ok(1) => return Ok(1),
                    Ok(changed) => {
                        return Err(MutationExecutorError::Bind(format!(
                            "ID space insert for {entity} changed {changed} rows"
                        )))
                    }
                    Err(error) if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Err(MutationExecutorError::Bind(format!(
            "Unable to allocate ID for {entity} after 100 optimistic-lock attempts"
        )))
    }

    pub fn ensure_floor(&self, entity: &str, floor: u64) -> Result<(), MutationExecutorError> {
        self.ensure_table()?;
        let floor = i64::try_from(floor).map_err(|_| {
            MutationExecutorError::Bind(format!("ID space floor {floor} for {entity} exceeds i64"))
        })?;
        let table = quote_ident(&self.table_name);
        for _ in 1..=100 {
            let connection = self.executor.lock()?;
            let current = connection
                .query_row(
                    &format!("SELECT current_level FROM {table} WHERE type_name = ?"),
                    [entity],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            match current {
                Some(current) if current >= floor => return Ok(()),
                Some(current) => {
                    if connection.execute(
                        &format!("UPDATE {table} SET current_level = ? WHERE type_name = ? AND current_level = ?"),
                        params![floor, entity, current],
                    )? == 1 { return Ok(()); }
                }
                None => match connection.execute(
                    &format!("INSERT INTO {table}(type_name, current_level) VALUES (?, ?)"),
                    params![entity, floor],
                ) {
                    Ok(1) => return Ok(()),
                    Ok(_) => {}
                    Err(error) if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) => {}
                    Err(error) => return Err(error.into()),
                },
            }
        }
        Err(MutationExecutorError::Bind(format!(
            "Unable to synchronize ID space floor for {entity} after 100 optimistic-lock attempts"
        )))
    }
}

impl InternalIdGenerator for SqliteIdSpaceGenerator {
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
        // Bind the canonical numeric spelling. SQLite NUMERIC affinity keeps
        // predicates and aggregates numeric; an application-only text prefix
        // makes range comparisons silently return the wrong result.
        Value::Decimal(v) => Ok(SqliteValue::Text(v.to_string())),
        Value::Text(v) => Ok(SqliteValue::Text(v.clone())),
        Value::Json(v) => Ok(SqliteValue::Text(v.to_string())),
        Value::Date(v) => Ok(SqliteValue::Text(v.format("%Y-%m-%d").to_string())),
        Value::Timestamp(v) => Ok(SqliteValue::Integer(v.0)),
        Value::Object(_) => Err(MutationExecutorError::UnsupportedValue("object")),
        Value::List(_) => Err(MutationExecutorError::UnsupportedValue("list")),
        Value::TypedNull(_) => Ok(SqliteValue::Null),
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

fn decode_sqlite_values(
    row: &Row<'_>,
    columns: &[ColumnInfo],
) -> Result<Vec<Value>, MutationExecutorError> {
    let mut values = Vec::with_capacity(columns.len());
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
        values.push(value);
    }
    Ok(values)
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
        return Ok(Value::Timestamp(teaql_core::time::Timestamp(
            timestamp.timestamp_millis(),
        )));
    }
    if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(Value::Timestamp(teaql_core::time::Timestamp(
            timestamp.and_utc().timestamp_millis(),
        )));
    }
    Ok(Value::Text(value.to_owned()))
}

fn parse_sqlite_timestamp(value: &str) -> Result<Value, MutationExecutorError> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(Value::Timestamp(teaql_core::time::Timestamp(
            timestamp.timestamp_millis(),
        )));
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(Value::Timestamp(teaql_core::time::Timestamp(
            date.and_hms_opt(0, 0, 0)
                .unwrap_or_default()
                .and_utc()
                .timestamp_millis(),
        )));
    }
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .map(|timestamp| {
            Value::Timestamp(teaql_core::time::Timestamp(
                timestamp.and_utc().timestamp_millis(),
            ))
        })
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
    use futures_util::StreamExt;
    use teaql_core::{DeleteCommand, Record, RecoverCommand};
    use teaql_macros::TeaqlEntity;
    use teaql_runtime::InMemoryMetadataStore;

    #[test]
    fn streaming_sql_yields_bounded_chunks_and_releases_cursor_on_drop() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE stream_fixture(id INTEGER);\
                 INSERT INTO stream_fixture VALUES (1), (2), (3), (4), (5);",
            )
            .unwrap();
        let executor = SqliteMutationExecutor::from_connection(connection);
        let query = CompiledQuery {
            sql: "SELECT id FROM stream_fixture ORDER BY id".to_owned(),
            params: vec![],
            comment: None,
        };
        let mut stream = teaql_sql::StreamingSqlTransport::stream_sql(&executor, query.clone(), 2);
        let sizes = futures_executor::block_on(async {
            let mut result = Vec::new();
            while let Some(chunk) = stream.next().await {
                result.push(chunk.unwrap().rows.len());
            }
            result
        });
        assert_eq!(sizes, vec![2, 2, 1]);

        let mut early = teaql_sql::StreamingSqlTransport::stream_sql(&executor, query, 2);
        assert_eq!(
            futures_executor::block_on(early.next())
                .unwrap()
                .unwrap()
                .rows
                .len(),
            2
        );
        drop(early);
        let count: i64 = executor
            .connection()
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM stream_fixture", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 5);
    }

    #[test]
    fn decimal_bind_is_numeric_and_comparable() {
        let value =
            bind_sqlite_value(&Value::Decimal(Decimal::from_str("123.450").unwrap())).unwrap();
        assert_eq!(value, SqliteValue::Text("123.450".to_owned()));
        let connection = Connection::open_in_memory().unwrap();
        let matches: i64 = connection
            .query_row(
                "SELECT 1 WHERE CAST(? AS NUMERIC) BETWEEN 120 AND 130",
                [value],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(matches, 1);
    }

    #[test]
    fn temporal_debug_sql_is_executable_and_matches_prepared_storage() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE temporal_fixture (id INTEGER PRIMARY KEY, d DATE, t TIMESTAMP)",
            )
            .unwrap();
        let query = CompiledQuery {
            sql: "INSERT INTO temporal_fixture VALUES (?, ?, ?)".to_owned(),
            params: vec![
                Value::I64(1),
                Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()),
                Value::Timestamp(teaql_core::time::Timestamp(1_787_110_200_123)),
            ],
            comment: None,
        };
        let values = bind_values(&query.params).unwrap();
        connection
            .execute(&query.sql, rusqlite::params_from_iter(values))
            .unwrap();
        connection
            .execute(
                &query
                    .debug_sql(teaql_sql::DatabaseKind::Sqlite)
                    .replace("VALUES (1,", "VALUES (2,"),
                [],
            )
            .unwrap();

        let equal_count: i64 = connection.query_row(
            "SELECT count(*) FROM temporal_fixture a JOIN temporal_fixture b ON a.d=b.d AND a.t=b.t WHERE a.id=1 AND b.id=2",
            [], |row| row.get(0),
        ).unwrap();
        let storage_type: String = connection
            .query_row(
                "SELECT typeof(t) FROM temporal_fixture WHERE id=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(equal_count, 1);
        assert_eq!(storage_type, "integer");
    }

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

    fn order_line_entity() -> EntityDescriptor {
        EntityDescriptor::new("OrderLine")
            .table_name("order_line")
            .property(
                PropertyDescriptor::new("id", DataType::U64)
                    .column_name("id")
                    .id()
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("order_id", DataType::U64)
                    .column_name("order_id")
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"))
    }

    #[allow(dead_code)]
    #[derive(Debug, PartialEq, TeaqlEntity)]
    #[teaql(entity = "FeatureFlag", table = "feature_flags")]
    struct FeatureFlagRow {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        enabled: bool,
        optional_enabled: Option<bool>,
    }

    fn feature_flag_record(enabled: Value, optional_enabled: Value) -> Record {
        Record::from([
            ("id".to_owned(), Value::U64(1)),
            ("version".to_owned(), Value::I64(1)),
            ("enabled".to_owned(), enabled),
            ("optional_enabled".to_owned(), optional_enabled),
        ])
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
            "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY NOT NULL, version INTEGER NOT NULL, name VARCHAR(255))"
        );
    }

    #[test]
    fn sqlite_executor_ensures_schema_and_roundtrips_rows() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let entity = entity();
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity.clone()));

        context.use_sqlite_provider(executor.clone());
        ensure_sqlite_schema_for(&context).unwrap();

        let insert = SqliteDialect
            .compile_insert(
                &entity,
                &InsertCommand::new("Order")
                    .value("id", 1_u64)
                    .value("version", 1_i64)
                    .value("name", "draft"),
            )
            .unwrap();
        assert_eq!(executor.execute(&insert).unwrap(), 1);

        let select = SqliteDialect
            .compile_select(
                &entity,
                &SelectQuery::new("Order")
                    .filter(Expr::eq("id", 1_u64))
                    .order_asc("id"),
            )
            .unwrap();
        let rows = executor.fetch_all_compact(&select).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("version"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("name"), Some(&Value::Text("draft".to_owned())));
    }

    #[test]
    fn repeated_schema_ensure_does_not_overwrite_existing_initial_graph() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let entity = entity();
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity.clone()));
        context.set_root_graphs(vec![
            GraphNode::new("Order")
                .value("id", 1_u64)
                .value("version", 1_i64)
                .value("name", "module seed"),
        ]);
        context.use_sqlite_provider(executor.clone());

        ensure_sqlite_schema_for(&context).unwrap();
        let customize = SqliteDialect
            .compile_update(
                &entity,
                &UpdateCommand::new("Order", 1_u64).value("name", "application value"),
            )
            .unwrap();
        assert_eq!(executor.execute(&customize).unwrap(), 1);

        ensure_sqlite_schema_for(&context).unwrap();

        let select = SqliteDialect
            .compile_select(
                &entity,
                &SelectQuery::new("Order").filter(Expr::eq("id", 1_u64)),
            )
            .unwrap();
        let rows = executor.fetch_all_compact(&select).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("name"),
            Some(&Value::Text("application value".to_owned()))
        );
    }

    #[test]
    fn repeated_schema_ensure_reconciles_changed_constant_graph() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let entity = entity();
        let mut context = UserContext::new()
            .with_metadata(InMemoryMetadataStore::new().with_entity(entity.clone()));
        context.set_initial_graphs(vec![
            GraphNode::new("Order")
                .value("id", 1001_u64)
                .value("version", 1_i64)
                .value("name", "red"),
        ]);
        context.use_sqlite_provider(executor.clone());
        ensure_sqlite_schema_for(&context).unwrap();

        context.set_initial_graphs(vec![
            GraphNode::new("Order")
                .value("id", 1001_u64)
                .value("version", 1_i64)
                .value("name", "crimson"),
        ]);
        ensure_sqlite_schema_for(&context).unwrap();

        let select = SqliteDialect
            .compile_select(
                &entity,
                &SelectQuery::new("Order").filter(Expr::eq("id", 1001_u64)),
            )
            .unwrap();
        let rows = executor.fetch_all_compact(&select).unwrap();
        assert_eq!(
            rows[0].get("name"),
            Some(&Value::Text("crimson".to_owned()))
        );
        let generator = SqliteIdSpaceGenerator::from_executor(executor);
        assert_eq!(generator.next_id("Order").unwrap(), 1002);
    }

    #[test]
    fn sqlite_executes_partitioned_relation_limit_per_parent() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let entity = order_line_entity();
        executor.ensure_schema(&SqliteDialect, &[&entity]).unwrap();

        for order_id in [11_u64, 12_u64] {
            for index in 1_u64..=5 {
                let id = order_id * 100 + index;
                let insert = SqliteDialect
                    .compile_insert(
                        &entity,
                        &InsertCommand::new("OrderLine")
                            .value("id", id)
                            .value("order_id", order_id)
                            .value("name", format!("line-{id}")),
                    )
                    .unwrap();
                executor.execute(&insert).unwrap();
            }
        }

        let query = SelectQuery::new("OrderLine")
            .project("id")
            .project("order_id")
            .order_desc("id")
            .limit(3)
            .partition_by("order_id");
        let compiled = SqliteDialect.compile_select(&entity, &query).unwrap();
        let rows = executor.fetch_all_compact(&compiled).unwrap();

        assert_eq!(rows.len(), 6);
        for order_id in [11_i64, 12_i64] {
            let ids = rows
                .iter()
                .filter(|row| row.get("order_id") == Some(&Value::I64(order_id)))
                .filter_map(|row| row.get("id").cloned())
                .collect::<Vec<_>>();
            assert_eq!(
                ids,
                vec![
                    Value::I64(order_id * 100 + 5),
                    Value::I64(order_id * 100 + 4),
                    Value::I64(order_id * 100 + 3),
                ]
            );
        }
    }

    #[test]
    fn sqlite_boolean_new_schema_roundtrips_as_bool() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let entity = <FeatureFlagRow as teaql_core::TeaqlEntity>::entity_descriptor();
        let ddl = SqliteDialect.compile_create_table(&entity).unwrap();
        assert!(ddl.contains("enabled BOOLEAN NOT NULL"), "{ddl}");
        assert!(ddl.contains("optional_enabled BOOLEAN"), "{ddl}");
        assert!(!ddl.contains("enabled INTEGER"), "{ddl}");

        executor.ensure_schema(&SqliteDialect, &[&entity]).unwrap();
        for (id, enabled, optional_enabled) in [(1_u64, false, true), (2_u64, true, false)] {
            let insert = SqliteDialect
                .compile_insert(
                    &entity,
                    &InsertCommand::new("FeatureFlag")
                        .value("id", id)
                        .value("version", 1_i64)
                        .value("enabled", enabled)
                        .value("optional_enabled", optional_enabled),
                )
                .unwrap();
            assert_eq!(executor.execute(&insert).unwrap(), 1);
        }

        let select = SqliteDialect
            .compile_select(&entity, &SelectQuery::new("FeatureFlag").order_asc("id"))
            .unwrap();
        let rows = executor.fetch_all_compact(&select).unwrap();
        assert_eq!(rows[0].get("enabled"), Some(&Value::Bool(false)));
        assert_eq!(rows[0].get("optional_enabled"), Some(&Value::Bool(true)));
        assert_eq!(rows[1].get("enabled"), Some(&Value::Bool(true)));
        assert_eq!(rows[1].get("optional_enabled"), Some(&Value::Bool(false)));

        let first = <FeatureFlagRow as teaql_core::Entity>::from_compact_row(rows[0].clone()).unwrap();
        let second = <FeatureFlagRow as teaql_core::Entity>::from_compact_row(rows[1].clone()).unwrap();
        assert!(!first.enabled);
        assert_eq!(first.optional_enabled, Some(true));
        assert!(second.enabled);
        assert_eq!(second.optional_enabled, Some(false));
    }

    #[test]
    fn sqlite_boolean_legacy_integer_schema_maps_only_binary_values() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let entity = <FeatureFlagRow as teaql_core::TeaqlEntity>::entity_descriptor();
        executor
            .execute(&CompiledQuery {
                sql: "CREATE TABLE feature_flags (id INTEGER PRIMARY KEY, version INTEGER NOT NULL, enabled INTEGER NOT NULL, optional_enabled INTEGER)"
                    .to_owned(),
                params: Vec::new(),
                comment: None,
            })
            .unwrap();

        let insert = SqliteDialect
            .compile_insert(
                &entity,
                &InsertCommand::new("FeatureFlag")
                    .value("id", 1_u64)
                    .value("version", 1_i64)
                    .value("enabled", true)
                    .value("optional_enabled", false),
            )
            .unwrap();
        executor.execute(&insert).unwrap();
        executor
            .execute(&CompiledQuery {
                sql: "INSERT INTO feature_flags (id, version, enabled, optional_enabled) VALUES (?, ?, ?, ?)"
                    .to_owned(),
                params: vec![
                    Value::U64(2),
                    Value::I64(1),
                    Value::I64(2),
                    Value::Null,
                ],
                comment: None,
            })
            .unwrap();
        let select = SqliteDialect
            .compile_select(&entity, &SelectQuery::new("FeatureFlag").order_asc("id"))
            .unwrap();
        let rows = executor.fetch_all_compact(&select).unwrap();
        assert_eq!(rows[0].get("version"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("enabled"), Some(&Value::I64(1)));
        assert_eq!(rows[0].get("optional_enabled"), Some(&Value::I64(0)));

        let decoded = <FeatureFlagRow as teaql_core::Entity>::from_compact_row(rows[0].clone()).unwrap();
        assert!(decoded.enabled);
        assert_eq!(decoded.optional_enabled, Some(false));
        assert_eq!(rows[1].get("enabled"), Some(&Value::I64(2)));
        let error =
            <FeatureFlagRow as teaql_core::Entity>::from_compact_row(rows[1].clone()).unwrap_err();
        assert!(error.message.contains("invalid field enabled"));

        for (value, expected) in [
            (Value::I64(0), false),
            (Value::I64(1), true),
            (Value::U64(0), false),
            (Value::U64(1), true),
        ] {
            let decoded = <FeatureFlagRow as teaql_core::Entity>::from_compact_row(
                teaql_core::CompactRow::from_record(feature_flag_record(value, Value::Null)),
            )
            .unwrap();
            assert_eq!(decoded.enabled, expected);
            assert_eq!(decoded.optional_enabled, None);
        }

        for invalid in [Value::I64(-1), Value::I64(2), Value::U64(2)] {
            let error = <FeatureFlagRow as teaql_core::Entity>::from_compact_row(
                teaql_core::CompactRow::from_record(feature_flag_record(invalid, Value::Null)),
            )
            .unwrap_err();
            assert!(error.message.contains("invalid field enabled"));
        }
        let error = <FeatureFlagRow as teaql_core::Entity>::from_compact_row(
            teaql_core::CompactRow::from_record(feature_flag_record(
                Value::Bool(true),
                Value::U64(2),
            )),
        )
        .unwrap_err();
        assert!(error.message.contains("invalid field optional_enabled"));
    }

    #[test]
    fn sqlite_executor_parses_json_only_for_json_columns() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());

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
            .fetch_all_compact(&CompiledQuery {
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
    fn sqlite_id_space_generator_increments_ids() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let generator = SqliteIdSpaceGenerator::from_executor(executor);
        assert_eq!(generator.next_id("Order").unwrap(), 1);
        assert_eq!(generator.next_id("Order").unwrap(), 2);
    }

    #[test]
    fn sqlite_id_space_generator_is_safe_across_connections() {
        let path = std::env::temp_dir().join(format!(
            "teaql-id-space-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut workers = Vec::new();
        for _ in 0..4 {
            let path = path.clone();
            workers.push(std::thread::spawn(move || {
                let connection = Connection::open(path).unwrap();
                connection
                    .busy_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                let generator = SqliteIdSpaceGenerator::new(connection);
                (0..25)
                    .map(|_| generator.next_id("Order").unwrap())
                    .collect::<Vec<_>>()
            }));
        }
        let mut ids = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, (1..=100).collect::<Vec<_>>());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sqlite_fetch_stream_returns_chunked_rows() {
        let executor = SqliteMutationExecutor::new(Arc::new(Mutex::new(
            Connection::open_in_memory().unwrap(),
        )));
        let entity = entity();

        // Create table and insert 25 rows
        executor
            .execute(&CompiledQuery {
                sql: "CREATE TABLE orders (id INTEGER PRIMARY KEY, version INTEGER, name VARCHAR(255))"
                    .to_owned(),
                params: Vec::new(),
                comment: None,
            })
            .unwrap();

        for i in 1..=25 {
            let insert = SqliteDialect
                .compile_insert(
                    &entity,
                    &InsertCommand::new("Order")
                        .value("id", i as u64)
                        .value("version", 1_i64)
                        .value("name", format!("order-{i}")),
                )
                .unwrap();
            executor.execute(&insert).unwrap();
        }

        // Stream with chunk_size = 10
        let query = SelectQuery::new("Order")
            .filter(Expr::gt("version", 0_i64))
            .order_asc("id")
            .stream(10);

        let compiled = SqliteDialect.compile_select(&entity, &query).unwrap();

        let chunks = executor.fetch_stream(&compiled, 10).unwrap();

        // 25 rows / 10 per chunk = 3 chunks
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].rows.len(), 10);
        assert_eq!(chunks[0].chunk_index, 0);
        assert!(!chunks[0].is_last);

        assert_eq!(chunks[1].rows.len(), 10);
        assert_eq!(chunks[1].chunk_index, 1);
        assert!(!chunks[1].is_last);

        assert_eq!(chunks[2].rows.len(), 5);
        assert_eq!(chunks[2].chunk_index, 2);
        assert!(chunks[2].is_last);

        // Verify first and last row
        assert_eq!(
            chunks[0].rows[0].get("name"),
            Some(&Value::Text("order-1".to_owned()))
        );
        assert_eq!(
            chunks[2].rows[4].get("name"),
            Some(&Value::Text("order-25".to_owned()))
        );
    }

    #[test]
    fn sqlite_fetch_stream_handles_empty_result() {
        let executor = SqliteMutationExecutor::new(Arc::new(Mutex::new(
            Connection::open_in_memory().unwrap(),
        )));

        executor
            .execute(&CompiledQuery {
                sql: "CREATE TABLE orders (id INTEGER PRIMARY KEY, version INTEGER, name VARCHAR(255))"
                    .to_owned(),
                params: Vec::new(),
                comment: None,
            })
            .unwrap();

        let entity = entity();
        let query = SelectQuery::new("Order")
            .filter(Expr::gt("version", 0_i64))
            .stream(10);

        let compiled = SqliteDialect.compile_select(&entity, &query).unwrap();

        let chunks = executor.fetch_stream(&compiled, 10).unwrap();

        // Empty result = 1 chunk with 0 rows, marked as last
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].rows.len(), 0);
        assert!(chunks[0].is_last);
    }

    #[test]
    fn sqlite_fetch_stream_exact_chunk_boundary() {
        let executor = SqliteMutationExecutor::new(Arc::new(Mutex::new(
            Connection::open_in_memory().unwrap(),
        )));
        let entity = entity();

        executor
            .execute(&CompiledQuery {
                sql: "CREATE TABLE orders (id INTEGER PRIMARY KEY, version INTEGER, name VARCHAR(255))"
                    .to_owned(),
                params: Vec::new(),
                comment: None,
            })
            .unwrap();

        // Insert exactly 20 rows
        for i in 1..=20 {
            let insert = SqliteDialect
                .compile_insert(
                    &entity,
                    &InsertCommand::new("Order")
                        .value("id", i as u64)
                        .value("version", 1_i64)
                        .value("name", format!("order-{i}")),
                )
                .unwrap();
            executor.execute(&insert).unwrap();
        }

        let query = SelectQuery::new("Order")
            .filter(Expr::gt("version", 0_i64))
            .order_asc("id")
            .stream(10);

        let compiled = SqliteDialect.compile_select(&entity, &query).unwrap();

        let chunks = executor.fetch_stream(&compiled, 10).unwrap();

        // 20 rows / 10 per chunk = 2 full chunks + 1 empty final chunk
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].rows.len(), 10);
        assert!(!chunks[0].is_last);
        assert_eq!(chunks[1].rows.len(), 10);
        assert!(!chunks[1].is_last);
        assert_eq!(chunks[2].rows.len(), 0);
        assert!(chunks[2].is_last);
    }

    #[test]
    fn test_parse_sqlite_timestamp() {
        let ts1 = parse_sqlite_timestamp("2023-01-01 12:30:45").unwrap();
        assert!(matches!(ts1, Value::Timestamp(_)));

        let ts2 = parse_sqlite_timestamp("2023-01-01").unwrap();
        assert!(matches!(ts2, Value::Timestamp(_)));

        let ts3 = parse_sqlite_timestamp("2023-01-01T12:30:45Z").unwrap();
        assert!(matches!(ts3, Value::Timestamp(_)));

        assert!(parse_sqlite_timestamp("invalid").is_err());
    }
}
