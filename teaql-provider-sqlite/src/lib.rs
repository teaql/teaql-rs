use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone};
use rusqlite::types::{Value as SqliteValue, ValueRef};
use rusqlite::{
    Connection, OptionalExtension, Row, functions::FunctionFlags, params, params_from_iter,
};
use rust_decimal::Decimal;
use teaql_core::{
    CompactRow, DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, SelectQuery,
    UpdateCommand, Value,
};
use teaql_runtime::{
    GraphNode, InternalIdGenerator, RawAuditEvent, RuntimeError, SchemaProvider, UserContext,
    canonical_id_space_entity,
};
use teaql_sql::{
    CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect, SqlTransport,
    quote_identifier_if_needed,
};

pub const DEFAULT_ID_SPACE_TABLE: &str = "teaql_id_space";
pub const DEFAULT_PREPARED_STATEMENT_CACHE_CAPACITY: usize = 64;
pub const DEFAULT_COLUMN_LAYOUT_CACHE_CAPACITY: usize = 64;

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

    fn prefers_small_parent_relation_probes(&self) -> bool {
        true
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
    column_layout_cache: Arc<Mutex<HashMap<String, Arc<ColumnLayout>>>>,
}

impl SqliteMutationExecutor {
    pub fn new(connection: Arc<Mutex<Connection>>) -> Self {
        if let Ok(connection) = connection.lock() {
            connection
                .set_prepared_statement_cache_capacity(DEFAULT_PREPARED_STATEMENT_CACHE_CAPACITY);
        }
        Self {
            connection,
            column_layout_cache: Arc::new(Mutex::new(HashMap::new())),
        }
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
        self.ensure_soundex_function()?;
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
        self.clear_query_caches();
        Ok(())
    }

    fn ensure_soundex_function(&self) -> Result<(), MutationExecutorError> {
        self.lock()?.create_scalar_function(
            "soundex",
            1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |ctx| {
                let input = ctx.get_raw(0).as_str().ok();
                Ok(sqlite_compatible_soundex(input))
            },
        )?;
        Ok(())
    }

    fn clear_query_caches(&self) {
        if let Ok(connection) = self.connection.lock() {
            connection.flush_prepared_statement_cache();
        }
        if let Ok(mut cache) = self.column_layout_cache.lock() {
            cache.clear();
        }
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
        let sql = query.sql_with_comment();
        let mut statement = connection.prepare_cached(&sql)?;
        let layout = cached_column_layout(&self.column_layout_cache, &query.sql, &statement);
        let mut rows = statement.query(params_from_iter(params.iter()))?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            result.push(CompactRow::new(
                layout.names.clone(),
                decode_sqlite_values(row, &layout.columns)?,
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
        let sql = query.sql_with_comment();
        let mut statement = connection.prepare_cached(&sql)?;
        let layout = cached_column_layout(&self.column_layout_cache, &query.sql, &statement);
        let mut rows = statement.query(params_from_iter(params.iter()))?;

        let mut chunks = Vec::new();
        let mut current_chunk = Vec::new();
        let mut chunk_index = 0;

        while let Some(row) = rows.next()? {
            current_chunk.push(CompactRow::new(
                layout.names.clone(),
                decode_sqlite_values(row, &layout.columns)?,
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

fn sqlite_compatible_soundex(input: Option<&str>) -> String {
    fn code(byte: u8) -> u8 {
        match byte.to_ascii_uppercase() {
            b'B' | b'F' | b'P' | b'V' => 1,
            b'C' | b'G' | b'J' | b'K' | b'Q' | b'S' | b'X' | b'Z' => 2,
            b'D' | b'T' => 3,
            b'L' => 4,
            b'M' | b'N' => 5,
            b'R' => 6,
            _ => 0,
        }
    }
    let Some(input) = input else {
        return "?000".to_owned();
    };
    let Some((first_index, first)) = input
        .bytes()
        .enumerate()
        .find(|(_, byte)| byte.is_ascii_alphabetic())
    else {
        return "?000".to_owned();
    };
    let mut result = String::with_capacity(4);
    result.push(char::from(first.to_ascii_uppercase()));
    let mut previous = code(first);
    for byte in input.bytes().skip(first_index + 1) {
        if !byte.is_ascii_alphabetic() {
            continue;
        }
        let current = code(byte);
        if current != 0 && current != previous {
            result.push(char::from(b'0' + current));
            if result.len() == 4 {
                break;
            }
        }
        previous = current;
    }
    while result.len() < 4 {
        result.push('0');
    }
    result
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

    async fn fetch_repeated_compact_sql(
        &self,
        template: &CompiledQuery,
        param_index: usize,
        values: &[Value],
    ) -> Result<Vec<CompactRow>, Self::Error> {
        let connection = self.lock()?;
        let sql = template.sql_with_comment();
        let mut statement = connection.prepare_cached(&sql)?;
        let layout = cached_column_layout(&self.column_layout_cache, &template.sql, &statement);
        let mut result = Vec::new();
        let mut query_params = template.params.clone();
        for value in values {
            query_params[param_index] = value.clone();
            let params = bind_values(&query_params)?;
            let mut rows = statement.query(params_from_iter(params.iter()))?;
            while let Some(row) = rows.next()? {
                result.push(CompactRow::new(
                    layout.names.clone(),
                    decode_sqlite_values(row, &layout.columns)?,
                ));
            }
        }
        Ok(result)
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
        let column_layout_cache = self.column_layout_cache.clone();
        Box::pin(async_stream::try_stream! {
            let params = bind_values(&query.params)?;
            let guard = connection.lock().map_err(|err| MutationExecutorError::Lock(err.to_string()))?;
            let sql = query.sql_with_comment();
            let mut statement = guard.prepare_cached(&sql)?;
            let layout = cached_column_layout(&column_layout_cache, &query.sql, &statement);
            let mut rows = statement.query(params_from_iter(params.iter()))?;
            let mut chunk = Vec::with_capacity(chunk_size); let mut index = 0;
            while let Some(row) = rows.next()? {
                chunk.push(CompactRow::new(layout.names.clone(), decode_sqlite_values(row, &layout.columns)?));
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

fn initial_graph_row_sqlite(
    executor: &SqliteMutationExecutor,
    dialect: &SqliteDialect,
    entity: &EntityDescriptor,
    graph: &GraphNode,
) -> Result<Option<teaql_core::CompactRow>, MutationExecutorError> {
    let Some(id) = graph.values.get("id") else {
        return Ok(None);
    };
    let mut select = SelectQuery::new(&graph.entity)
        .filter(Expr::eq("id", id.clone()))
        .limit(1);
    for field in graph.values.keys() {
        select = select.project(field);
    }
    if let Some(version) = entity
        .version_property()
        .filter(|version| !graph.values.contains_key(&version.name))
    {
        select = select.project(&version.name);
    }
    let query = dialect.compile_select(entity, &select)?;
    Ok(executor.fetch_all_compact(&query)?.into_iter().next())
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
    current: &teaql_core::CompactRow,
) -> Result<Option<CompiledQuery>, MutationExecutorError> {
    let Some(id) = graph.values.get("id") else {
        return Ok(None);
    };
    let mut command = UpdateCommand::new(&graph.entity, id.clone());
    for (field, value) in &graph.values {
        if field != "id"
            && field != "version"
            && !bootstrap_values_equal(current.get(field), Some(value))
        {
            command = command.value(field.clone(), value.clone());
        }
    }
    if command.values.is_empty() {
        return Ok(None);
    }
    if let Some(version) = entity
        .version_property()
        .and_then(|property| current.get(&property.name))
        .and_then(Value::try_i64)
    {
        command = command.expected_version(version);
    }
    match dialect.compile_update(entity, &command) {
        Ok(query) => Ok(Some(query)),
        Err(SqlCompileError::EmptyMutation(_)) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn bootstrap_values_equal(left: Option<&Value>, right: Option<&Value>) -> bool {
    let (Some(left), Some(right)) = (left, right) else {
        return left.is_none() && right.is_none();
    };
    if left == right {
        return true;
    }
    matches!((left.try_decimal(), right.try_decimal()), (Some(a), Some(b)) if a == b)
}

pub(crate) fn ensure_sqlite_physical_schema_for(
    context: &UserContext,
) -> Result<(), MutationExecutorError> {
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

    executor.clear_query_caches();
    Ok(())
}

pub(crate) fn ensure_sqlite_schema_for(context: &UserContext) -> Result<(), MutationExecutorError> {
    ensure_sqlite_physical_schema_for(context)?;
    let dialect = context.get_resource::<SqliteDialect>().ok_or_else(|| {
        MutationExecutorError::Bind("missing typed resource: SqliteDialect".to_owned())
    })?;
    let executor = context
        .get_resource::<SqliteMutationExecutor>()
        .ok_or_else(|| {
            MutationExecutorError::Bind("missing typed resource: SqliteMutationExecutor".to_owned())
        })?;

    // Constant graphs are reconciled so model changes are propagated.
    let id_generator = SqliteIdSpaceGenerator::from_executor(executor.clone());
    let mut seed_counts: BTreeMap<String, (usize, usize)> = BTreeMap::new(); // (inserted, updated)
    for graph in context.initial_graphs() {
        let entity = context.entity(&graph.entity).ok_or_else(|| {
            MutationExecutorError::Bind(format!("missing entity: {}", graph.entity))
        })?;
        let counts = seed_counts.entry(graph.entity.clone()).or_insert((0, 0));
        if let Some(current) = initial_graph_row_sqlite(executor, dialect, entity, graph)? {
            if let Some(query) = compile_initial_graph_update(dialect, entity, graph, &current)? {
                executor.execute(&query)?;
                counts.1 += 1;
            }
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
        if initial_graph_row_sqlite(executor, dialect, entity, graph)?.is_some() {
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

    executor.clear_query_caches();
    Ok(())
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SqliteSchemaProvider;

impl SchemaProvider for SqliteSchemaProvider {
    fn ensure_schema<'a>(
        &'a self,
        context: &'a UserContext,
        _invocation: &'a teaql_runtime::SchemaInvocation,
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
        let entity = canonical_id_space_entity(entity);
        let entity = entity.as_str();
        self.ensure_table()?;
        let table = quote_ident(&self.table_name);
        let select_sql = format!("SELECT current_level FROM {table} WHERE type_name = ?");
        let insert_sql = format!("INSERT INTO {table} (type_name, current_level) VALUES (?, 1)");
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
                        )));
                    }
                    Err(error)
                        if error.sqlite_error_code()
                            == Some(rusqlite::ErrorCode::ConstraintViolation) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Err(MutationExecutorError::Bind(format!(
            "Unable to allocate ID for {entity} after 100 optimistic-lock attempts"
        )))
    }

    pub fn ensure_floor(&self, entity: &str, floor: u64) -> Result<(), MutationExecutorError> {
        let entity = canonical_id_space_entity(entity);
        let entity = entity.as_str();
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
    decode_kind: SqliteDecodeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteDecodeKind {
    Infer,
    Bool,
    Decimal,
    Json,
    Date,
    Timestamp,
    Text,
}

#[derive(Debug)]
struct ColumnLayout {
    columns: Arc<[ColumnInfo]>,
    names: Arc<[String]>,
}

fn cached_column_layout(
    cache: &Mutex<HashMap<String, Arc<ColumnLayout>>>,
    sql: &str,
    statement: &rusqlite::Statement<'_>,
) -> Arc<ColumnLayout> {
    if let Ok(cache) = cache.lock()
        && let Some(layout) = cache.get(sql)
    {
        return layout.clone();
    }

    let columns: Arc<[ColumnInfo]> = statement_columns(statement).into();
    let names = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>()
        .into();
    let layout = Arc::new(ColumnLayout { columns, names });
    if let Ok(mut cache) = cache.lock() {
        if cache.len() >= DEFAULT_COLUMN_LAYOUT_CACHE_CAPACITY {
            cache.clear();
        }
        cache.insert(sql.to_owned(), layout.clone());
    }
    layout
}

fn statement_columns(statement: &rusqlite::Statement<'_>) -> Vec<ColumnInfo> {
    statement
        .columns()
        .into_iter()
        .map(|column| ColumnInfo {
            name: column.name().to_owned(),
            decode_kind: sqlite_decode_kind(column.decl_type()),
        })
        .collect()
}

fn sqlite_decode_kind(decl_type: Option<&str>) -> SqliteDecodeKind {
    let Some(decl_type) = decl_type else {
        return SqliteDecodeKind::Infer;
    };
    let base = decl_type.split('(').next().unwrap_or(decl_type).trim();
    if base.eq_ignore_ascii_case("BOOLEAN") || base.eq_ignore_ascii_case("BOOL") {
        SqliteDecodeKind::Bool
    } else if base.eq_ignore_ascii_case("NUMERIC") || base.eq_ignore_ascii_case("DECIMAL") {
        SqliteDecodeKind::Decimal
    } else if base.eq_ignore_ascii_case("JSON") {
        SqliteDecodeKind::Json
    } else if base.eq_ignore_ascii_case("DATE") {
        SqliteDecodeKind::Date
    } else if base.eq_ignore_ascii_case("TIMESTAMP") || base.eq_ignore_ascii_case("DATETIME") {
        SqliteDecodeKind::Timestamp
    } else if ["TEXT", "VARCHAR", "CHAR", "CLOB"]
        .iter()
        .any(|v| base.eq_ignore_ascii_case(v))
    {
        SqliteDecodeKind::Text
    } else {
        SqliteDecodeKind::Infer
    }
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
    match column.decode_kind {
        SqliteDecodeKind::Bool => Value::Bool(value != 0),
        _ => Value::I64(value),
    }
}

fn decode_sqlite_text(value: &[u8], column: &ColumnInfo) -> Result<Value, MutationExecutorError> {
    let value = std::str::from_utf8(value)
        .map_err(|err| MutationExecutorError::Bind(format!("invalid sqlite text: {err}")))?;
    match column.decode_kind {
        SqliteDecodeKind::Decimal => Decimal::from_str(value)
            .map(Value::Decimal)
            .map_err(|err| MutationExecutorError::Bind(format!("invalid sqlite decimal: {err}"))),
        SqliteDecodeKind::Json => serde_json::from_str(value).map(Value::Json).map_err(|err| {
            MutationExecutorError::Bind(format!("invalid sqlite json value: {err}"))
        }),
        SqliteDecodeKind::Date => NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|err| MutationExecutorError::Bind(format!("invalid sqlite date: {err}"))),
        SqliteDecodeKind::Timestamp => parse_sqlite_timestamp(value),
        SqliteDecodeKind::Text | SqliteDecodeKind::Bool => Ok(Value::Text(value.to_owned())),
        SqliteDecodeKind::Infer => infer_sqlite_text(value),
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
    if let Some(timestamp) = parse_fixed_sqlite_timestamp(value) {
        return Ok(Value::Timestamp(teaql_core::time::Timestamp(timestamp)));
    }
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(Value::Timestamp(teaql_core::time::Timestamp(
            timestamp.timestamp_millis(),
        )));
    }
    if let Ok(timestamp) = DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%#z") {
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
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .map(|timestamp| {
            Value::Timestamp(teaql_core::time::Timestamp(
                timestamp.and_utc().timestamp_millis(),
            ))
        })
        .map_err(|err| MutationExecutorError::Bind(format!("invalid sqlite timestamp: {err}")))
}

fn parse_fixed_sqlite_timestamp(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.len() < 19
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || !matches!(bytes.get(10), Some(b' ') | Some(b'T'))
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return None;
    }
    let digits = |start: usize, len: usize| -> Option<u32> {
        bytes
            .get(start..start + len)?
            .iter()
            .try_fold(0_u32, |value, byte| {
                byte.is_ascii_digit()
                    .then_some(value * 10 + u32::from(*byte - b'0'))
            })
    };
    let date = NaiveDate::from_ymd_opt(
        i32::try_from(digits(0, 4)?).ok()?,
        digits(5, 2)?,
        digits(8, 2)?,
    )?;
    let hour = digits(11, 2)?;
    let minute = digits(14, 2)?;
    let second = digits(17, 2)?;
    let mut cursor = 19;
    let mut nanos = 0_u32;
    if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let fraction_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            if cursor - fraction_start < 9 {
                nanos = nanos * 10 + u32::from(bytes[cursor] - b'0');
            }
            cursor += 1;
        }
        let kept = (cursor - fraction_start).min(9);
        if kept == 0 {
            return None;
        }
        nanos *= 10_u32.pow(u32::try_from(9 - kept).ok()?);
    }
    let datetime = date.and_hms_nano_opt(hour, minute, second, nanos)?;
    let offset_seconds = match bytes.get(cursor..) {
        Some([]) | Some([b'Z']) | Some([b'z']) => 0,
        Some([sign @ (b'+' | b'-'), hour_1, hour_2]) => {
            signed_offset(*sign, [*hour_1, *hour_2], *b"00")?
        }
        Some([sign @ (b'+' | b'-'), hour_1, hour_2, minute_1, minute_2]) => {
            signed_offset(*sign, [*hour_1, *hour_2], [*minute_1, *minute_2])?
        }
        Some(
            [
                sign @ (b'+' | b'-'),
                hour_1,
                hour_2,
                b':',
                minute_1,
                minute_2,
            ],
        ) => signed_offset(*sign, [*hour_1, *hour_2], [*minute_1, *minute_2])?,
        _ => return None,
    };
    FixedOffset::east_opt(offset_seconds)?
        .from_local_datetime(&datetime)
        .single()
        .map(|timestamp| timestamp.timestamp_millis())
}

fn signed_offset(sign: u8, hours: [u8; 2], minutes: [u8; 2]) -> Option<i32> {
    let pair = |digits: [u8; 2]| {
        digits
            .iter()
            .all(u8::is_ascii_digit)
            .then_some(i32::from(digits[0] - b'0') * 10 + i32::from(digits[1] - b'0'))
    };
    let hours = pair(hours)?;
    let minutes = pair(minutes)?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    let seconds = hours * 3600 + minutes * 60;
    Some(if sign == b'-' { -seconds } else { seconds })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use teaql_core::{DeleteCommand, Record, RecoverCommand};
    use teaql_macros::TeaqlEntity;
    use teaql_runtime::InMemoryMetadataStore;

    #[test]
    fn ensure_schema_registers_soundex_idempotently() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        executor.ensure_schema(&SqliteDialect, &[]).unwrap();
        executor.ensure_schema(&SqliteDialect, &[]).unwrap();
        let connection = executor.connection();
        let guard = connection.lock().unwrap();
        let encoded: String = guard
            .query_row("SELECT soundex('Robert')", [], |row| row.get(0))
            .unwrap();
        let matches: i64 = guard
            .query_row("SELECT soundex('Robert') = soundex('Rupert')", [], |row| {
                row.get(0)
            })
            .unwrap();
        let empty: String = guard
            .query_row("SELECT soundex(NULL)", [], |row| row.get(0))
            .unwrap();
        assert_eq!(encoded, "R163");
        assert_eq!(matches, 1);
        assert_eq!(empty, "?000");
    }

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

    fn complete_query_record_entity() -> EntityDescriptor {
        EntityDescriptor::new("QueryRecord")
            .table_name("query_record_scalar")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("required_text", DataType::Text))
            .property(PropertyDescriptor::new("optional_text", DataType::Text))
            .property(PropertyDescriptor::new("required_integer", DataType::I64))
            .property(PropertyDescriptor::new("optional_long", DataType::I64))
            .property(PropertyDescriptor::new(
                "required_decimal",
                DataType::Decimal,
            ))
            .property(PropertyDescriptor::new("required_float", DataType::F64))
            .property(PropertyDescriptor::new("required_double", DataType::F64))
            .property(PropertyDescriptor::new("required_date", DataType::Date))
            .property(PropertyDescriptor::new("required_time", DataType::I64))
            .property(PropertyDescriptor::new(
                "required_timestamp",
                DataType::Timestamp,
            ))
            .property(PropertyDescriptor::new("active", DataType::Bool))
            .property(PropertyDescriptor::new("reviewed", DataType::Bool))
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
    }

    #[test]
    fn complete_scalar_fixture_including_nullable_boolean_executes_on_sqlite() {
        let executor = SqliteMutationExecutor::from_connection(
            Connection::open_in_memory().expect("open SQLite fixture"),
        );
        executor
            .connection()
            .lock()
            .expect("lock SQLite fixture")
            .execute_batch("CREATE TABLE query_record_scalar (\
                    id INTEGER PRIMARY KEY, required_text TEXT, optional_text TEXT,\
                    required_integer INTEGER, optional_long INTEGER, required_decimal NUMERIC,\
                    required_float REAL, required_double REAL, required_date DATE,\
                    required_time INTEGER, required_timestamp TIMESTAMP,\
                    active BOOLEAN, reviewed BOOLEAN, version INTEGER);\
                    INSERT INTO query_record_scalar VALUES \
                    (1,'Alpha','optional',42,42000000000,42.125,42.5,42.75,'2026-08-29',34200000,1777632600000,1,0,1),\
                    (2,'Beta',NULL,7,NULL,7.500,7.5,7.75,'2026-08-30',36000000,1777720400000,0,NULL,1),\
                    (3,'Gamma','tail',99,99000000000,99.875,99.5,99.75,'2026-08-31',37800000,1777808200000,1,1,1)")
            .expect("seed complete scalar fixture");
        let entity = complete_query_record_entity();
        let ids = |expr: Expr| {
            let query = SelectQuery::new("QueryRecord")
                .project("id")
                .filter(expr)
                .order_asc("id");
            executor
                .fetch_all_compact(&SqliteDialect.compile_select(&entity, &query).unwrap())
                .expect("execute scalar predicate")
                .into_iter()
                .map(|row| row.get("id").cloned().expect("projected id"))
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(Expr::eq("required_text", "Alpha")), vec![Value::I64(1)]);
        assert_eq!(
            ids(Expr::ne("required_text", "Alpha")),
            vec![Value::I64(2), Value::I64(3)]
        );
        assert_eq!(
            ids(Expr::in_list(
                "required_text",
                [Value::from("Alpha"), Value::from("Gamma")]
            )),
            vec![Value::I64(1), Value::I64(3)]
        );
        assert_eq!(
            ids(Expr::contain("required_text", "et")),
            vec![Value::I64(2)]
        );
        assert_eq!(
            ids(Expr::between("required_integer", 40_i64, 100_i64)),
            vec![Value::I64(1), Value::I64(3)]
        );
        assert_eq!(
            ids(Expr::gt("required_decimal", Decimal::from(50))),
            vec![Value::I64(3)]
        );
        assert_eq!(
            ids(Expr::lte("required_float", 7.5_f64)),
            vec![Value::I64(2)]
        );
        assert_eq!(
            ids(Expr::gte("required_double", 99.75_f64)),
            vec![Value::I64(3)]
        );
        assert_eq!(
            ids(Expr::between(
                "required_date",
                NaiveDate::from_ymd_opt(2026, 8, 30).unwrap(),
                NaiveDate::from_ymd_opt(2026, 8, 31).unwrap(),
            )),
            vec![Value::I64(2), Value::I64(3)]
        );
        assert_eq!(
            ids(Expr::gt("required_time", 36_000_000_i64)),
            vec![Value::I64(3)]
        );
        assert_eq!(
            ids(Expr::lt(
                "required_timestamp",
                teaql_core::time::Timestamp(1_777_750_000_000)
            )),
            vec![Value::I64(1), Value::I64(2)]
        );
        assert_eq!(ids(Expr::is_null("optional_text")), vec![Value::I64(2)]);
        assert_eq!(
            ids(Expr::is_not_null("optional_long")),
            vec![Value::I64(1), Value::I64(3)]
        );
        assert_eq!(ids(Expr::eq("active", false)), vec![Value::I64(2)]);
        assert_eq!(ids(Expr::eq("reviewed", true)), vec![Value::I64(3)]);
        assert_eq!(ids(Expr::eq("reviewed", false)), vec![Value::I64(1)]);
        assert_eq!(ids(Expr::is_null("reviewed")), vec![Value::I64(2)]);
    }

    #[test]
    fn relation_subqueries_execute_positive_and_negative_predicates_on_sqlite() {
        let executor = SqliteMutationExecutor::from_connection(
            Connection::open_in_memory().expect("open SQLite fixture"),
        );
        executor
            .connection()
            .lock()
            .expect("lock SQLite fixture")
            .execute_batch(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, version INTEGER, name TEXT);\
                 CREATE TABLE order_line (id INTEGER PRIMARY KEY, order_id INTEGER, name TEXT);\
                 INSERT INTO orders VALUES (1, 1, 'first'), (2, 1, 'second'), (3, 1, 'third');\
                 INSERT INTO order_line VALUES\
                    (10, 1, 'priority'), (11, 1, 'ordinary'), (12, 2, 'ordinary'),\
                    (13, NULL, 'orphan');",
            )
            .expect("seed relation fixture");

        let matching_lines = SelectQuery::new("OrderLine").filter(Expr::eq("name", "priority"));
        let positive = SelectQuery::new("Order")
            .project("id")
            .filter(Expr::in_subquery(
                "id",
                order_line_entity(),
                matching_lines.clone(),
                "order_id",
            ))
            .order_asc("id");
        let negative = SelectQuery::new("Order")
            .project("id")
            .filter(Expr::not_in_subquery(
                "id",
                order_line_entity(),
                matching_lines,
                "order_id",
            ))
            .order_asc("id");

        let ids = |rows: Vec<CompactRow>| {
            rows.into_iter()
                .map(|row| row.get("id").cloned().expect("projected id"))
                .collect::<Vec<_>>()
        };
        let order_ids = |query: SelectQuery| {
            ids(executor
                .fetch_all_compact(&SqliteDialect.compile_select(&entity(), &query).unwrap())
                .expect("execute order relation predicate"))
        };
        let line_ids = |query: SelectQuery| {
            ids(executor
                .fetch_all_compact(
                    &SqliteDialect
                        .compile_select(&order_line_entity(), &query)
                        .unwrap(),
                )
                .expect("execute line relation predicate"))
        };

        // Reverse relation: typed child matching and its negative form.
        assert_eq!(order_ids(positive), vec![Value::I64(1)]);
        assert_eq!(order_ids(negative), vec![Value::I64(2), Value::I64(3)]);

        // Forward relation identity state keeps NULL distinct from a known FK.
        assert_eq!(
            line_ids(
                SelectQuery::new("OrderLine")
                    .project("id")
                    .filter(Expr::is_not_null("order_id"))
                    .order_asc("id")
            ),
            vec![Value::I64(10), Value::I64(11), Value::I64(12)]
        );
        assert_eq!(
            line_ids(
                SelectQuery::new("OrderLine")
                    .project("id")
                    .filter(Expr::is_null("order_id"))
                    .order_asc("id")
            ),
            vec![Value::I64(13)]
        );

        // Forward nested matching. SQL NOT IN deliberately excludes the NULL
        // foreign key; callers use IsUnknown when they want orphan rows.
        let first_order = SelectQuery::new("Order").filter(Expr::eq("name", "first"));
        assert_eq!(
            line_ids(
                SelectQuery::new("OrderLine")
                    .project("id")
                    .filter(Expr::in_subquery(
                        "order_id",
                        entity(),
                        first_order.clone(),
                        "id",
                    ))
                    .order_asc("id")
            ),
            vec![Value::I64(10), Value::I64(11)]
        );
        assert_eq!(
            line_ids(
                SelectQuery::new("OrderLine")
                    .project("id")
                    .filter(Expr::not_in_subquery(
                        "order_id",
                        entity(),
                        first_order,
                        "id",
                    ))
                    .order_asc("id")
            ),
            vec![Value::I64(12)]
        );

        // Reverse existence/non-existence without an additional child filter.
        let all_lines = SelectQuery::new("OrderLine");
        assert_eq!(
            order_ids(
                SelectQuery::new("Order")
                    .project("id")
                    .filter(Expr::in_subquery(
                        "id",
                        order_line_entity(),
                        all_lines.clone(),
                        "order_id",
                    ))
                    .order_asc("id")
            ),
            vec![Value::I64(1), Value::I64(2)]
        );
        assert_eq!(
            order_ids(
                SelectQuery::new("Order")
                    .project("id")
                    .filter(Expr::not_in_subquery(
                        "id",
                        order_line_entity(),
                        all_lines,
                        "order_id",
                    ))
                    .order_asc("id")
            ),
            vec![Value::I64(3)]
        );
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
        assert!(SqliteDialect.prefers_small_parent_relation_probes());
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
    fn column_layout_cache_uses_parameterized_sql_not_comments() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute("CREATE TABLE sample (id INTEGER, enabled BOOLEAN)", [])
            .unwrap();
        connection
            .execute("INSERT INTO sample (id, enabled) VALUES (1, 1)", [])
            .unwrap();
        let executor = SqliteMutationExecutor::from_connection(connection);
        let mut first = CompiledQuery {
            sql: "SELECT id, enabled FROM sample WHERE id = ?".to_owned(),
            params: vec![Value::I64(1)],
            comment: Some("first purpose".to_owned()),
        };
        let rows = executor.fetch_all_compact(&first).unwrap();
        assert_eq!(rows[0].get("enabled"), Some(&Value::Bool(true)));

        first.comment = Some("different purpose".to_owned());
        executor.fetch_all_compact(&first).unwrap();

        assert_eq!(executor.column_layout_cache.lock().unwrap().len(), 1);
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
        ensure_sqlite_physical_schema_for(&context).unwrap();
        let before_bootstrap = SqliteDialect
            .compile_select(
                &entity,
                &SelectQuery::new("Order").filter(Expr::eq("id", 1001_u64)),
            )
            .unwrap();
        assert!(executor.fetch_all_compact(&before_bootstrap).unwrap().is_empty());
        ensure_sqlite_schema_for(&context).unwrap();
        ensure_sqlite_schema_for(&context).unwrap();

        let unchanged = SqliteDialect
            .compile_select(
                &entity,
                &SelectQuery::new("Order").filter(Expr::eq("id", 1001_u64)),
            )
            .unwrap();
        let rows = executor.fetch_all_compact(&unchanged).unwrap();
        assert_eq!(rows[0].get("version"), Some(&Value::I64(1)));

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
        assert_eq!(rows[0].get("version"), Some(&Value::I64(2)));
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
    fn topn_005_007_window_and_probes_preserve_results_and_predicates() {
        futures_executor::block_on(async {
            #[derive(Clone)]
            struct FixedSchema(Arc<EntityDescriptor>);

            impl teaql_data_service::SchemaProvider for FixedSchema {
                fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
                    (name == self.0.name).then(|| self.0.clone())
                }
            }

            let transport =
                SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
            let entity = Arc::new(order_line_entity());
            transport
                .ensure_schema(&SqliteDialect, &[entity.as_ref()])
                .unwrap();

            for order_id in [11_u64, 12_u64, 13_u64] {
                for index in 1_u64..=5 {
                    let id = order_id * 100 + index;
                    let name = if index == 4 { "excluded" } else { "visible" };
                    let insert = SqliteDialect
                        .compile_insert(
                            &entity,
                            &InsertCommand::new("OrderLine")
                                .value("id", id)
                                .value("order_id", order_id)
                                .value("name", name),
                        )
                        .unwrap();
                    transport.execute(&insert).unwrap();
                }
            }

            let executor = teaql_sql::SqlDataServiceExecutor::new(
                SqliteDialect,
                transport,
                FixedSchema(entity),
            );
            let base = SelectQuery::new("OrderLine")
                .project("id")
                .project("order_id")
                .project("name")
                .filter(Expr::in_list("order_id", [Value::U64(11), Value::U64(12)]))
                .and_filter(Expr::eq("name", "visible"))
                .order_desc("id")
                .limit(3)
                .partition_by("order_id");
            let execute = |query| {
                teaql_data_service::QueryExecutor::query(
                    &executor,
                    teaql_data_service::QueryRequest {
                        query,
                        trace_chain: Vec::new(),
                        comment: Some("TOPN plan equivalence".to_owned()),
                        capture_debug_query: false,
                        capture_execution_metadata: false,
                    },
                )
            };

            let probes = execute(base.clone()).await.unwrap().rows;
            let window = execute(base.top_n_probe_parent_threshold(0))
                .await
                .unwrap()
                .rows;
            let children_of = |rows: &[CompactRow], parent: i64| {
                rows.iter()
                    .filter(|row| row.get("order_id") == Some(&Value::I64(parent)))
                    .map(|row| (row.get("id").cloned(), row.get("name").cloned()))
                    .collect::<Vec<_>>()
            };

            for parent in [11_i64, 12_i64] {
                assert_eq!(children_of(&probes, parent), children_of(&window, parent));
            }
            assert_eq!(
                children_of(&window, 11),
                vec![
                    (Some(Value::I64(1105)), Some(Value::Text("visible".into()))),
                    (Some(Value::I64(1103)), Some(Value::Text("visible".into()))),
                    (Some(Value::I64(1102)), Some(Value::Text("visible".into()))),
                ]
            );
            assert_eq!(
                children_of(&window, 12),
                vec![
                    (Some(Value::I64(1205)), Some(Value::Text("visible".into()))),
                    (Some(Value::I64(1203)), Some(Value::Text("visible".into()))),
                    (Some(Value::I64(1202)), Some(Value::Text("visible".into()))),
                ]
            );
            assert!(children_of(&probes, 13).is_empty());
            assert!(children_of(&window, 13).is_empty());
        });
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

        let first =
            <FeatureFlagRow as teaql_core::Entity>::from_compact_row(rows[0].clone()).unwrap();
        let second =
            <FeatureFlagRow as teaql_core::Entity>::from_compact_row(rows[1].clone()).unwrap();
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

        let decoded =
            <FeatureFlagRow as teaql_core::Entity>::from_compact_row(rows[0].clone()).unwrap();
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
                teaql_core::CompactRow::from_map(feature_flag_record(value, Value::Null)),
            )
            .unwrap();
            assert_eq!(decoded.enabled, expected);
            assert_eq!(decoded.optional_enabled, None);
        }

        for invalid in [Value::I64(-1), Value::I64(2), Value::U64(2)] {
            let error = <FeatureFlagRow as teaql_core::Entity>::from_compact_row(
                teaql_core::CompactRow::from_map(feature_flag_record(invalid, Value::Null)),
            )
            .unwrap_err();
            assert!(error.message.contains("invalid field enabled"));
        }
        let error = <FeatureFlagRow as teaql_core::Entity>::from_compact_row(
            teaql_core::CompactRow::from_map(feature_flag_record(Value::Bool(true), Value::U64(2))),
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

        let ts4 = parse_sqlite_timestamp("2026-08-23 10:43:16.152546+00").unwrap();
        assert!(matches!(ts4, Value::Timestamp(_)));

        let ts5 = parse_sqlite_timestamp("2026-08-23 10:43:16.152546").unwrap();
        assert!(matches!(ts5, Value::Timestamp(_)));

        assert_eq!(
            parse_fixed_sqlite_timestamp("2024-01-01 00:00:00+00"),
            Some(1_704_067_200_000)
        );
        assert_eq!(
            parse_fixed_sqlite_timestamp("2024-01-01T08:00:00.123+08:00"),
            Some(1_704_067_200_123)
        );
        assert_eq!(
            parse_fixed_sqlite_timestamp("2023-12-31 19:00:00-0500"),
            Some(1_704_067_200_000)
        );
        assert_eq!(parse_fixed_sqlite_timestamp("2024-13-01 00:00:00Z"), None);
        assert_eq!(parse_fixed_sqlite_timestamp("2024-01-01 00:00:00+24"), None);

        assert!(parse_sqlite_timestamp("invalid").is_err());
    }

    #[test]
    fn declared_text_does_not_infer_timestamp_from_content() {
        for decl_type in ["TEXT", "VARCHAR(255)", "CHAR(32)", "CLOB"] {
            let column = ColumnInfo {
                name: "external_timestamp".to_owned(),
                decode_kind: sqlite_decode_kind(Some(decl_type)),
            };

            assert_eq!(
                decode_sqlite_text(b"2024-01-01 00:57:55", &column).unwrap(),
                Value::Text("2024-01-01 00:57:55".to_owned())
            );
        }
    }

    #[test]
    fn declared_column_types_compile_to_decode_kinds() {
        assert_eq!(sqlite_decode_kind(Some("BOOLEAN")), SqliteDecodeKind::Bool);
        assert_eq!(
            sqlite_decode_kind(Some("decimal(20, 4)")),
            SqliteDecodeKind::Decimal
        );
        assert_eq!(
            sqlite_decode_kind(Some(" VARCHAR(255) ")),
            SqliteDecodeKind::Text
        );
        assert_eq!(
            sqlite_decode_kind(Some("datetime")),
            SqliteDecodeKind::Timestamp
        );
        assert_eq!(sqlite_decode_kind(Some("custom")), SqliteDecodeKind::Infer);
        assert_eq!(sqlite_decode_kind(None), SqliteDecodeKind::Infer);
    }
}
