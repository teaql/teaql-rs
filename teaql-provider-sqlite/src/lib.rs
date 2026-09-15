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
use teaql_core::{CompactRow, DataType, EntityDescriptor, PropertyDescriptor, Value};
use teaql_runtime::{
    InternalIdGenerator, RawAuditEvent, RuntimeError, SchemaProvider, UserContext,
    canonical_id_space_entity,
};
use teaql_sql::{
    CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect, SqlTransport,
    quote_identifier_if_needed, schema_index_specs,
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
    ) -> Result<String, SqlCompileError> {
        match data_type {
            DataType::Bool => Ok("BOOLEAN".to_owned()),
            DataType::I64 | DataType::U64 if property.is_id => Ok("INTEGER".to_owned()),
            DataType::I64 | DataType::U64 => Ok("INTEGER".to_owned()),
            DataType::F64 => Ok("REAL".to_owned()),
            DataType::Decimal => match (property.numeric_precision, property.numeric_scale) {
                (Some(precision), Some(scale)) => Ok(format!("NUMERIC({precision},{scale})")),
                _ => Ok("NUMERIC".to_owned()),
            },
            DataType::Text => Ok(format!("VARCHAR({})", property.max_length.unwrap_or(255))),
            DataType::LargeText => Ok("TEXT".to_owned()),
            DataType::Json => Ok("JSON".to_owned()),
            DataType::Date => Ok("DATE".to_owned()),
            DataType::Timestamp => Ok("TIMESTAMP".to_owned()),
        }
    }

    fn compile_add_column(
        &self,
        entity: &EntityDescriptor,
        property: &PropertyDescriptor,
    ) -> Result<String, SqlCompileError> {
        let def = self.column_definition_sql(property)?;
        Ok(format!(
            "ALTER TABLE {} ADD COLUMN {}",
            self.quote_ident(&entity.table_name),
            def
        ))
    }
}

fn sqlite_foreign_keys_for(
    source_entity: &EntityDescriptor,
    entities: &[&EntityDescriptor],
) -> Result<Vec<(String, String, String)>, MutationExecutorError> {
    let mut foreign_keys = BTreeSet::new();
    for entity in entities {
        for relation in &entity.relations {
            let Some(target) = entities
                .iter()
                .copied()
                .find(|candidate| candidate.name == relation.target_entity)
            else {
                continue;
            };
            if entity.data_service != target.data_service {
                continue;
            }
            let (source, source_key, referenced, referenced_key) = if relation.many {
                (target, &relation.foreign_key, *entity, &relation.local_key)
            } else {
                (*entity, &relation.local_key, target, &relation.foreign_key)
            };
            if source.name != source_entity.name {
                continue;
            }
            let source_property = source.property_by_name(source_key).ok_or_else(|| {
                MutationExecutorError::Bind(format!(
                    "cannot ensure relation {}.{}: source key {}.{} does not exist",
                    entity.name, relation.name, source.name, source_key
                ))
            })?;
            let referenced_property =
                referenced.property_by_name(referenced_key).ok_or_else(|| {
                    MutationExecutorError::Bind(format!(
                        "cannot ensure relation {}.{}: referenced key {}.{} does not exist",
                        entity.name, relation.name, referenced.name, referenced_key
                    ))
                })?;
            foreign_keys.insert((
                source_property.column_name.clone(),
                referenced.table_name.clone(),
                referenced_property.column_name.clone(),
            ));
        }
    }
    Ok(foreign_keys.into_iter().collect())
}

struct SqliteForeignKeyRow {
    id: i64,
    seq: i64,
    source_column: String,
    referenced_table: String,
    referenced_column: String,
    update_action: String,
    delete_action: String,
}

fn compile_sqlite_create_table(
    dialect: &SqliteDialect,
    entity: &EntityDescriptor,
    entities: &[&EntityDescriptor],
) -> Result<String, MutationExecutorError> {
    let mut definitions = entity
        .properties
        .iter()
        .map(|property| dialect.column_definition_sql(property))
        .collect::<Result<Vec<_>, _>>()?;
    for (source_column, referenced_table, referenced_column) in
        sqlite_foreign_keys_for(entity, entities)?
    {
        definitions.push(format!(
            "FOREIGN KEY ({}) REFERENCES {} ({})",
            dialect.quote_ident(&source_column),
            dialect.quote_ident(&referenced_table),
            dialect.quote_ident(&referenced_column),
        ));
    }
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        dialect.quote_ident(&entity.table_name),
        definitions.join(", ")
    ))
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

    #[cfg(test)]
    fn ensure_schema(
        &self,
        dialect: &SqliteDialect,
        entities: &[&EntityDescriptor],
    ) -> Result<(), MutationExecutorError> {
        let connection = self.lock()?;
        Self::ensure_soundex_function(&connection)?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.execute("BEGIN IMMEDIATE", [])?;
        let result = Self::ensure_schema_with_connection(&connection, dialect, entities);
        match result {
            Ok(()) => {
                if let Err(error) = connection.execute("COMMIT", []) {
                    let _ = connection.execute("ROLLBACK", []);
                    return Err(error.into());
                }
            }
            Err(error) => {
                let _ = connection.execute("ROLLBACK", []);
                return Err(error);
            }
        }
        drop(connection);
        self.clear_query_caches();
        Ok(())
    }

    #[cfg(test)]
    fn ensure_schema_with_connection(
        connection: &Connection,
        dialect: &SqliteDialect,
        entities: &[&EntityDescriptor],
    ) -> Result<(), MutationExecutorError> {
        Self::ensure_id_space_table_with_connection(connection, DEFAULT_ID_SPACE_TABLE)?;

        for entity in entities {
            if !Self::table_exists_with_connection(connection, &entity.table_name)? {
                let sql = compile_sqlite_create_table(dialect, entity, entities)?;
                connection.execute(&sql, [])?;
            } else {
                let existing_columns =
                    Self::table_columns_with_connection(connection, &entity.table_name)?;
                for property in &entity.properties {
                    let bare_column = strip_identifier_quotes(&property.column_name).to_lowercase();
                    if let Some((actual_type, actual_nullable)) = existing_columns.get(&bare_column)
                    {
                        ensure_sqlite_column_compatibility(
                            entity,
                            property,
                            actual_type,
                            *actual_nullable,
                        )?;
                        continue;
                    }
                    Self::ensure_required_column_can_be_added(connection, entity, property)?;
                    let sql = dialect.compile_add_column(entity, property)?;
                    connection.execute(&sql, [])?;
                }
            }

            for sql in dialect.schema_indexes_sqls(entity)? {
                connection.execute(&sql, [])?;
            }
            ensure_sqlite_declared_index_shapes(connection, entity)?;
        }
        Self::ensure_foreign_keys_with_connection(connection, entities)?;
        Ok(())
    }

    fn ensure_foreign_keys_with_connection(
        connection: &Connection,
        entities: &[&EntityDescriptor],
    ) -> Result<(), MutationExecutorError> {
        for entity in entities {
            let expected = sqlite_foreign_keys_for(entity, entities)?;
            if expected.is_empty() {
                continue;
            }
            let pragma = format!(
                "PRAGMA foreign_key_list({})",
                quote_ident(&entity.table_name)
            );
            let mut statement = connection.prepare(&pragma)?;
            let rows = statement.query_map([], |row| {
                Ok(SqliteForeignKeyRow {
                    id: row.get("id")?,
                    seq: row.get("seq")?,
                    source_column: row.get("from")?,
                    referenced_table: row.get("table")?,
                    referenced_column: row.get("to")?,
                    update_action: row.get("on_update")?,
                    delete_action: row.get("on_delete")?,
                })
            })?;
            let actual = rows.collect::<Result<Vec<_>, _>>()?;
            for (source_column, referenced_table, referenced_column) in expected {
                let restricts = |action: &str| {
                    action.eq_ignore_ascii_case("NO ACTION")
                        || action.eq_ignore_ascii_case("RESTRICT")
                };
                let mut found = false;
                let mut incompatible = None;
                for candidate in actual.iter().filter(|candidate| {
                    candidate.source_column.eq_ignore_ascii_case(&source_column)
                        && candidate
                            .referenced_table
                            .eq_ignore_ascii_case(&referenced_table)
                        && candidate
                            .referenced_column
                            .eq_ignore_ascii_case(&referenced_column)
                }) {
                    found = true;
                    let key_count = actual.iter().filter(|row| row.id == candidate.id).count();
                    if candidate.seq == 0
                        && key_count == 1
                        && restricts(&candidate.update_action)
                        && restricts(&candidate.delete_action)
                    {
                        incompatible = None;
                        break;
                    }
                    incompatible.get_or_insert_with(|| format!(
                        "ensure schema incompatible SQLite foreign-key shape: table={}, column={}, referenced_table={}, referenced_column={}, expected=one full-column key with RESTRICT actions, installed_fk_id={}, installed_seq={}, installed_key_count={}, installed_update={}, installed_delete={}; rebuild/migrate the table explicitly before retrying",
                        entity.table_name,
                        source_column,
                        referenced_table,
                        referenced_column,
                        candidate.id,
                        candidate.seq,
                        key_count,
                        candidate.update_action,
                        candidate.delete_action
                    ));
                }
                if !found {
                    return Err(MutationExecutorError::Bind(format!(
                        "ensure schema cannot add missing SQLite foreign key in place: table={}, column={}, referenced_table={}, referenced_column={}; rebuild/migrate the table explicitly before retrying",
                        entity.table_name, source_column, referenced_table, referenced_column
                    )));
                }
                if let Some(message) = incompatible {
                    return Err(MutationExecutorError::Bind(message));
                }
            }
        }
        Ok(())
    }

    fn ensure_soundex_function(connection: &Connection) -> Result<(), MutationExecutorError> {
        connection.create_scalar_function(
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
        let connection = self.lock()?;
        Self::ensure_id_space_table_with_connection(&connection, table_name)
    }

    fn ensure_id_space_table_with_connection(
        connection: &Connection,
        table_name: &str,
    ) -> Result<(), MutationExecutorError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (type_name VARCHAR(100) PRIMARY KEY, current_level BIGINT NOT NULL)",
            quote_ident(table_name)
        );
        connection.execute(&sql, [])?;
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
        let connection = self.lock()?;
        Self::table_exists_with_connection(&connection, table_name)
    }

    fn table_exists_with_connection(
        connection: &Connection,
        table_name: &str,
    ) -> Result<bool, MutationExecutorError> {
        let exists: i64 = connection.query_row(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?",
            [table_name],
            |row| row.get(0),
        )?;
        Ok(exists > 0)
    }

    pub fn table_columns(
        &self,
        table_name: &str,
    ) -> Result<BTreeMap<String, (String, bool)>, MutationExecutorError> {
        let connection = self.lock()?;
        Self::table_columns_with_connection(&connection, table_name)
    }

    fn table_columns_with_connection(
        connection: &Connection,
        table_name: &str,
    ) -> Result<BTreeMap<String, (String, bool)>, MutationExecutorError> {
        let pragma_sql = format!("PRAGMA table_info({})", quote_ident(table_name));
        let mut statement = connection.prepare(&pragma_sql)?;
        let rows = statement.query_map([], |row| {
            let not_null = row.get::<_, i64>("notnull")? != 0;
            let primary_key = row.get::<_, i64>("pk")? != 0;
            Ok((
                row.get::<_, String>("name")?,
                row.get::<_, String>("type")?,
                !(not_null || primary_key),
            ))
        })?;
        let mut columns = BTreeMap::new();
        for row in rows {
            let (name, data_type, nullable) = row?;
            columns.insert(name.to_lowercase(), (data_type, nullable));
        }
        Ok(columns)
    }

    fn ensure_required_column_can_be_added(
        connection: &Connection,
        entity: &EntityDescriptor,
        property: &PropertyDescriptor,
    ) -> Result<(), MutationExecutorError> {
        if property.nullable {
            return Ok(());
        }
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM {} LIMIT 1)",
            quote_ident(&entity.table_name)
        );
        let has_rows: bool = connection.query_row(&sql, [], |row| row.get(0))?;
        if !has_rows {
            return Ok(());
        }
        Err(MutationExecutorError::Bind(format!(
            "ensure schema cannot add required column without a deterministic backfill: entity={}, table={}, column={}; the table contains rows, so migrate/backfill explicitly before retrying",
            entity.name, entity.table_name, property.column_name
        )))
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>, MutationExecutorError> {
        self.connection
            .lock()
            .map_err(|err| MutationExecutorError::Lock(err.to_string()))
    }
}

fn ensure_sqlite_column_compatibility(
    entity: &EntityDescriptor,
    property: &PropertyDescriptor,
    actual: &str,
    actual_nullable: bool,
) -> Result<(), MutationExecutorError> {
    let actual_family = sqlite_type_family(actual);
    let compatible = match property.data_type {
        DataType::Bool => matches!(actual_family.as_str(), "boolean" | "integer"),
        DataType::I64 | DataType::U64 => actual_family == "integer",
        DataType::F64 => actual_family == "real",
        DataType::Decimal => actual_family == "numeric",
        DataType::Text | DataType::LargeText => actual_family == "text",
        DataType::Json => matches!(actual_family.as_str(), "json" | "text"),
        DataType::Date => matches!(actual_family.as_str(), "date" | "text"),
        DataType::Timestamp => matches!(actual_family.as_str(), "timestamp" | "integer"),
    };
    if !compatible {
        return Err(MutationExecutorError::Bind(format!(
            "ensure schema incompatible column type: entity={}, table={}, column={}, expected={:?}, actual={actual}",
            entity.name, entity.table_name, property.column_name, property.data_type
        )));
    }
    if property.nullable != actual_nullable {
        return Err(MutationExecutorError::Bind(format!(
            "ensure schema incompatible column nullability: entity={}, table={}, column={}, expected_nullable={}, actual_nullable={actual_nullable}",
            entity.name, entity.table_name, property.column_name, property.nullable
        )));
    }
    let declared_args = sqlite_declared_type_args(actual);
    if let Some(expected) = property.max_length {
        let actual_length = declared_args.first().copied();
        if !teaql_sql::storage_length_covers(expected, actual_length) {
            return Err(MutationExecutorError::Bind(format!(
                "ensure schema existing column is too narrow: entity={}, table={}, column={}, required_max_length={expected}, actual_max_length={actual_length:?}",
                entity.name, entity.table_name, property.column_name
            )));
        }
    }
    if let (Some(expected_precision), Some(expected_scale)) =
        (property.numeric_precision, property.numeric_scale)
    {
        let actual_precision = declared_args.first().copied();
        let actual_scale = declared_args.get(1).copied();
        if !teaql_sql::storage_numeric_covers(
            expected_precision,
            expected_scale,
            actual_precision,
            actual_scale,
        ) {
            return Err(MutationExecutorError::Bind(format!(
                "ensure schema existing numeric column does not cover the model value domain: entity={}, table={}, column={}, required_precision={expected_precision}, required_scale={expected_scale}, actual_precision={actual_precision:?}, actual_scale={actual_scale:?}",
                entity.name, entity.table_name, property.column_name
            )));
        }
    }
    Ok(())
}

fn sqlite_declared_type_args(data_type: &str) -> Vec<u32> {
    let Some(start) = data_type.find('(') else {
        return Vec::new();
    };
    let Some(end) = data_type[start + 1..].find(')') else {
        return Vec::new();
    };
    data_type[start + 1..start + 1 + end]
        .split(',')
        .filter_map(|part| part.trim().parse().ok())
        .collect()
}

fn sqlite_type_family(data_type: &str) -> String {
    let normalized = data_type.trim().to_ascii_uppercase();
    if normalized.contains("INT") {
        "integer".to_owned()
    } else if normalized.contains("CHAR")
        || normalized.contains("CLOB")
        || normalized.contains("TEXT")
    {
        "text".to_owned()
    } else if normalized.contains("REAL")
        || normalized.contains("FLOA")
        || normalized.contains("DOUB")
    {
        "real".to_owned()
    } else if normalized.contains("NUM") || normalized.contains("DEC") {
        "numeric".to_owned()
    } else if normalized.contains("BOOL") {
        "boolean".to_owned()
    } else if normalized.contains("JSON") {
        "json".to_owned()
    } else if normalized.contains("TIMESTAMP") || normalized.contains("DATETIME") {
        "timestamp".to_owned()
    } else if normalized.contains("DATE") {
        "date".to_owned()
    } else {
        normalized.to_ascii_lowercase()
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
    let connection = executor.lock()?;
    SqliteMutationExecutor::ensure_soundex_function(&connection)?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.execute("BEGIN IMMEDIATE", [])?;
    let result = (|| -> Result<(), MutationExecutorError> {
        SqliteMutationExecutor::ensure_id_space_table_with_connection(
            &connection,
            DEFAULT_ID_SPACE_TABLE,
        )?;

        // Process each entity table individually with granular events.
        for entity in &entities {
            let field_count = entity.properties.len();
            if !SqliteMutationExecutor::table_exists_with_connection(
                &connection,
                &entity.table_name,
            )? {
                let sql = compile_sqlite_create_table(dialect, entity, &entities)?;
                connection.execute(&sql, [])?;
                let _ = context.send_event(RawAuditEvent::schema_created(
                    &entity.name,
                    &entity.table_name,
                    field_count,
                ));
            } else {
                let existing_columns = SqliteMutationExecutor::table_columns_with_connection(
                    &connection,
                    &entity.table_name,
                )?;
                for property in &entity.properties {
                    let bare_column = strip_identifier_quotes(&property.column_name).to_lowercase();
                    if let Some((actual_type, actual_nullable)) = existing_columns.get(&bare_column)
                    {
                        ensure_sqlite_column_compatibility(
                            entity,
                            property,
                            actual_type,
                            *actual_nullable,
                        )?;
                        continue;
                    }
                    SqliteMutationExecutor::ensure_required_column_can_be_added(
                        &connection,
                        entity,
                        property,
                    )?;
                    let sql = dialect.compile_add_column(entity, property)?;
                    connection.execute(&sql, [])?;
                    let _ = context.send_event(RawAuditEvent::field_added(
                        &entity.name,
                        &entity.table_name,
                        &property.column_name,
                    ));
                }
            }

            for sql in dialect.schema_indexes_sqls(entity)? {
                connection.execute(&sql, [])?;
            }
            ensure_sqlite_declared_index_shapes(&connection, entity)?;
            let _ = context.send_event(RawAuditEvent::schema_verified(
                &entity.name,
                &entity.table_name,
                field_count,
            ));
        }
        SqliteMutationExecutor::ensure_foreign_keys_with_connection(&connection, &entities)?;
        Ok(())
    })();
    match result {
        Ok(()) => {
            if let Err(error) = connection.execute("COMMIT", []) {
                let _ = connection.execute("ROLLBACK", []);
                return Err(error.into());
            }
        }
        Err(error) => {
            let _ = connection.execute("ROLLBACK", []);
            return Err(error);
        }
    }
    drop(connection);
    executor.clear_query_caches();
    Ok(())
}

pub(crate) fn ensure_sqlite_schema_for(context: &UserContext) -> Result<(), MutationExecutorError> {
    ensure_sqlite_physical_schema_for(context)?;
    if !context.initial_graphs().is_empty() || !context.root_graphs().is_empty() {
        return Err(MutationExecutorError::Bind(
            "generated root/constant bootstrap must use the typed RuntimeModule callback"
                .to_owned(),
        ));
    }
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

/// Installs the SQLite provider; schema changes must then go through
/// `UserContext::ensure_schema`, not the executor's private physical DDL path.
///
/// ```compile_fail
/// let _ = teaql_provider_sqlite::SqliteMutationExecutor::ensure_schema;
/// ```
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
        for _ in 1..=100 {
            match self.executor.ensure_id_space_table(&self.table_name) {
                Ok(()) => return Ok(()),
                Err(MutationExecutorError::Sqlite(error))
                    if retryable_sqlite_id_space_lock(&error) =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(error) => return Err(error),
            }
        }
        Err(MutationExecutorError::Bind(format!(
            "SQLite ID provider was unable to ensure ID-space table {} after 100 lock-contention attempts",
            self.table_name
        )))
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
            let result = (|| -> Result<Option<u64>, MutationExecutorError> {
                let connection = self.executor.lock()?;
                let current = connection
                    .query_row(&select_sql, [entity], |row| row.get::<_, i64>(0))
                    .optional()?;
                if let Some(current) = current {
                    let next = current.checked_add(1).ok_or_else(|| {
                        MutationExecutorError::Bind(format!(
                            "SQLite ID provider overflow for ID space {entity} in table {} on optimistic-lock attempt {attempt}",
                            self.table_name
                        ))
                    })?;
                    if connection.execute(&update_sql, params![next, entity, current])? == 1 {
                        return u64::try_from(next).map(Some).map_err(|_| {
                            MutationExecutorError::Bind(format!(
                                "SQLite ID provider generated id {next} for ID space {entity} in table {} that cannot be represented as u64",
                                self.table_name
                            ))
                        });
                    }
                } else {
                    match connection.execute(&insert_sql, params![entity]) {
                        Ok(1) => return Ok(Some(1)),
                        Ok(changed) => {
                            return Err(MutationExecutorError::Bind(format!(
                                "SQLite ID provider insert for ID space {entity} in table {} changed {changed} rows on optimistic-lock attempt {attempt}",
                                self.table_name
                            )));
                        }
                        Err(error)
                            if error.sqlite_error_code()
                                == Some(rusqlite::ErrorCode::ConstraintViolation) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                Ok(None)
            })();
            match result {
                Ok(Some(id)) => return Ok(id),
                Ok(None) => {}
                Err(MutationExecutorError::Sqlite(error))
                    if retryable_sqlite_id_space_lock(&error) => {}
                Err(error) => return Err(error),
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Err(MutationExecutorError::Bind(format!(
            "SQLite ID provider was unable to allocate ID space {entity} in table {} after 100 optimistic-lock attempts",
            self.table_name
        )))
    }

    pub fn ensure_floor(&self, entity: &str, floor: u64) -> Result<(), MutationExecutorError> {
        let entity = canonical_id_space_entity(entity);
        let entity = entity.as_str();
        self.ensure_table()?;
        let floor = i64::try_from(floor).map_err(|_| {
            MutationExecutorError::Bind(format!(
                "SQLite ID provider floor {floor} for ID space {entity} in table {} exceeds i64",
                self.table_name
            ))
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
            "SQLite ID provider was unable to synchronize floor for ID space {entity} in table {} after 100 optimistic-lock attempts",
            self.table_name
        )))
    }
}

fn retryable_sqlite_id_space_lock(error: &rusqlite::Error) -> bool {
    matches!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

impl InternalIdGenerator for SqliteIdSpaceGenerator {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        self.next_id(entity)
            .map_err(|err| RuntimeError::IdGeneration(err.to_string()))
    }

    fn ensure_floor(&self, entity: &str, floor: u64) -> Result<(), RuntimeError> {
        SqliteIdSpaceGenerator::ensure_floor(self, entity, floor)
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

fn ensure_sqlite_declared_index_shapes(
    connection: &Connection,
    entity: &EntityDescriptor,
) -> Result<(), MutationExecutorError> {
    let table = strip_identifier_quotes(&entity.table_name);
    for spec in schema_index_specs(entity, None) {
        let name = &spec.name;
        let expected_unique = spec.unique;
        let expected_columns = &spec.columns;
        let installed_table: Option<String> = connection
            .query_row(
                "SELECT tbl_name FROM sqlite_master WHERE type='index' AND name=?1",
                [&name],
                |row| row.get(0),
            )
            .optional()?;
        let Some(installed_table) = installed_table else {
            return Err(MutationExecutorError::Bind(format!(
                "SQLite declared index missing: table={table} index={name}"
            )));
        };
        let list_sql = format!("PRAGMA index_list({})", quote_ident(table));
        let mut list = connection.prepare(&list_sql)?;
        let rows = list.query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        let actual_flags = rows
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .find(|(index_name, _, _)| index_name == name);

        let xinfo_sql = format!("PRAGMA index_xinfo({})", quote_ident(name));
        let mut xinfo = connection.prepare(&xinfo_sql)?;
        let columns = xinfo
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let actual_key_columns = columns
            .iter()
            .filter(|column| column.5 == 1)
            .collect::<Vec<_>>();
        let keys_match = actual_key_columns.len() == expected_columns.len()
            && actual_key_columns
                .iter()
                .zip(expected_columns)
                .all(|(actual, expected)| {
                    actual.1 >= 0
                        && actual.2.as_deref().is_some_and(|column| {
                            column.eq_ignore_ascii_case(strip_identifier_quotes(expected))
                        })
                        && actual.3 == 0
                        && actual.4.eq_ignore_ascii_case("BINARY")
                });
        let flags_match = actual_flags.as_ref().is_some_and(|(_, unique, partial)| {
            *unique == i64::from(expected_unique) && *partial == 0
        });
        if !installed_table.eq_ignore_ascii_case(table) || !flags_match || !keys_match {
            return Err(MutationExecutorError::Bind(format!(
                "SQLite declared index shape mismatch: table={table} index={name}; expected={} on ({}) with ascending BINARY full-column keys; installed table={installed_table}, flags={actual_flags:?}, keys={actual_key_columns:?}; drop/rename the colliding index or migrate the table explicitly",
                if expected_unique {
                    "unique"
                } else {
                    "non-unique"
                },
                expected_columns.join(", ")
            )));
        }
    }
    Ok(())
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
    use teaql_core::{
        DeleteCommand, Entity, Expr, InsertCommand, Record, RecoverCommand, RelationDescriptor,
        SelectQuery, TeaqlEntity as _, UpdateCommand,
    };
    use teaql_macros::{TeaqlEntity, teaql_entity};
    use teaql_runtime::{GraphNode, InMemoryMetadataStore};

    #[teaql_entity]
    #[derive(Debug, TeaqlEntity)]
    #[teaql(entity = "TransactionSchool", table = "transaction_school")]
    struct TransactionSchool {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        name: String,
    }

    #[derive(Debug, PartialEq, TeaqlEntity)]
    #[teaql(
        entity = "PaymentChannelCollision",
        table = "payment_channel_collision"
    )]
    struct PaymentChannelCollision {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        label: String,
    }

    #[derive(Debug, PartialEq, TeaqlEntity)]
    #[teaql(entity = "PaymentOrderCollision", table = "payment_order_collision")]
    struct PaymentOrderCollision {
        #[teaql(id)]
        id: u64,
        #[teaql(version)]
        version: i64,
        #[teaql(column = "channel")]
        channel_id: u64,
        name: String,
        #[teaql(relation(
            target = "PaymentChannelCollision",
            local_key = "channel_id",
            foreign_key = "id"
        ))]
        channel: Option<PaymentChannelCollision>,
    }

    impl TransactionSchool {
        fn new(id: u64, name: &str) -> (Self, teaql_runtime::EntityRuntimeState) {
            let state = teaql_runtime::EntityRuntimeState::default();
            let key = teaql_runtime::EntityKey::new("TransactionSchool", id);
            state.mark_as_new(key.clone());
            state.set(key.clone(), "id", id);
            state.set(key, "name", name);
            (
                Self {
                    id,
                    version: 0,
                    name: name.to_owned(),
                    __teaql_runtime_state: state.clone(),
                },
                state,
            )
        }
    }

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
    fn installing_runtime_module_does_not_create_schema() {
        futures_executor::block_on(async {
            let executor =
                SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
            let entity = order_line_entity();
            let table = entity.table_name.clone();
            let mut context = UserContext::new()
                .with_module(teaql_runtime::RuntimeModule::new().descriptor(entity));
            context.use_sqlite_provider(executor.clone());

            assert!(!executor.table_exists(&table).unwrap());
            assert!(!executor.table_exists(DEFAULT_ID_SPACE_TABLE).unwrap());

            context.ensure_schema().await.unwrap();
            assert!(executor.table_exists(&table).unwrap());
            assert!(executor.table_exists(DEFAULT_ID_SPACE_TABLE).unwrap());

            context.ensure_schema().await.unwrap();
            assert!(executor.table_exists(&table).unwrap());
        });
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

    #[test]
    fn schema_compatibility_covers_boolean_and_temporal_storage_contracts() {
        let entity = EntityDescriptor::new("HighRiskFixture").table_name("high_risk_fixture");
        let bool_property = PropertyDescriptor::new("enabled", DataType::Bool);
        let date_property = PropertyDescriptor::new("business_date", DataType::Date);
        let timestamp_property = PropertyDescriptor::new("occurred_at", DataType::Timestamp);

        assert!(
            ensure_sqlite_column_compatibility(&entity, &bool_property, "BOOLEAN", true).is_ok()
        );
        assert!(
            ensure_sqlite_column_compatibility(&entity, &bool_property, "INTEGER", true).is_ok()
        );
        assert!(ensure_sqlite_column_compatibility(&entity, &bool_property, "TEXT", true).is_err());
        assert!(ensure_sqlite_column_compatibility(&entity, &date_property, "DATE", true).is_ok());
        assert!(ensure_sqlite_column_compatibility(&entity, &date_property, "TEXT", true).is_ok());
        assert!(
            ensure_sqlite_column_compatibility(&entity, &date_property, "INTEGER", true).is_err()
        );
        assert!(
            ensure_sqlite_column_compatibility(&entity, &timestamp_property, "TIMESTAMP", true)
                .is_ok()
        );
        assert!(
            ensure_sqlite_column_compatibility(&entity, &timestamp_property, "INTEGER", true)
                .is_ok()
        );
        assert!(
            ensure_sqlite_column_compatibility(&entity, &timestamp_property, "TEXT", true).is_err()
        );
    }

    #[test]
    fn ensure_schema_rejects_incompatible_existing_column_type() {
        let executor = SqliteMutationExecutor::from_connection(
            Connection::open_in_memory().expect("open schema mismatch fixture"),
        );
        executor
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, version INTEGER NOT NULL, name INTEGER)",
            )
            .unwrap();

        let error = executor
            .ensure_schema(&SqliteDialect, &[&entity()])
            .expect_err("incompatible storage type must fail schema ensure");
        let message = error.to_string();
        assert!(message.contains("entity=Order"), "{message}");
        assert!(message.contains("table=orders"), "{message}");
        assert!(message.contains("column=name"), "{message}");
        assert!(message.contains("expected=Text"), "{message}");
        assert!(message.contains("actual=INTEGER"), "{message}");
    }

    #[test]
    fn ensure_schema_accepts_covering_shapes_and_rejects_narrower_sqlite_columns() {
        let executor = SqliteMutationExecutor::from_connection(
            Connection::open_in_memory().expect("open shape mismatch fixture"),
        );
        executor
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE shape_fixture (
                   id INTEGER PRIMARY KEY,
                   version INTEGER NOT NULL,
                   name VARCHAR(255),
                   amount NUMERIC(38,10)
                 )",
            )
            .unwrap();
        let length_model = EntityDescriptor::new("ShapeFixture")
            .table_name("shape_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).max_length(100));
        executor
            .ensure_schema(&SqliteDialect, &[&length_model])
            .expect("wider VARCHAR storage must cover the model");

        let numeric_model = EntityDescriptor::new("ShapeFixture")
            .table_name("shape_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("amount", DataType::Decimal)
                    .numeric_precision(19)
                    .numeric_scale(7),
            );
        executor
            .ensure_schema(&SqliteDialect, &[&numeric_model])
            .expect("wider NUMERIC storage must cover the model");

        executor
            .lock()
            .unwrap()
            .execute_batch(
                "DROP TABLE shape_fixture;
                 CREATE TABLE shape_fixture (
                   id INTEGER PRIMARY KEY,
                   version INTEGER NOT NULL,
                   name VARCHAR(32),
                   amount NUMERIC(18,2)
                 )",
            )
            .unwrap();
        let message = executor
            .ensure_schema(&SqliteDialect, &[&length_model])
            .expect_err("narrower VARCHAR storage must fail")
            .to_string();
        assert!(message.contains("required_max_length=100"), "{message}");
        assert!(message.contains("actual_max_length=Some(32)"), "{message}");

        let message = executor
            .ensure_schema(&SqliteDialect, &[&numeric_model])
            .expect_err("narrower numeric storage must fail")
            .to_string();
        assert!(message.contains("required_precision=19"), "{message}");
        assert!(message.contains("required_scale=7"), "{message}");
        assert!(message.contains("actual_precision=Some(18)"), "{message}");
        assert!(message.contains("actual_scale=Some(2)"), "{message}");
    }

    #[test]
    fn sqlite_schema_ddl_uses_declared_length_and_numeric_shape() {
        let shaped = EntityDescriptor::new("ShapeFixture")
            .table_name("shape_fixture")
            .property(PropertyDescriptor::new("name", DataType::Text).max_length(100))
            .property(
                PropertyDescriptor::new("amount", DataType::Decimal)
                    .numeric_precision(19)
                    .numeric_scale(7),
            );
        assert_eq!(
            SqliteDialect.compile_create_table(&shaped).unwrap(),
            "CREATE TABLE IF NOT EXISTS shape_fixture (name VARCHAR(100), amount NUMERIC(19,7))"
        );
    }

    #[test]
    fn ensure_schema_rejects_incompatible_existing_column_nullability() {
        let executor = SqliteMutationExecutor::from_connection(
            Connection::open_in_memory().expect("open nullability mismatch fixture"),
        );
        executor
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, version INTEGER NOT NULL, name TEXT)",
            )
            .unwrap();
        let required_name = entity().property(
            PropertyDescriptor::new("required_name", DataType::Text)
                .column_name("name")
                .not_null(),
        );

        let error = executor
            .ensure_schema(&SqliteDialect, &[&required_name])
            .expect_err("nullable storage must not satisfy a required model field");
        let message = error.to_string();
        assert!(message.contains("entity=Order"), "{message}");
        assert!(message.contains("column=name"), "{message}");
        assert!(message.contains("expected_nullable=false"), "{message}");
        assert!(message.contains("actual_nullable=true"), "{message}");
    }

    #[test]
    fn ensure_schema_adds_required_column_to_empty_sqlite_table_idempotently() {
        let executor = SqliteMutationExecutor::from_connection(
            Connection::open_in_memory().expect("open empty required-column fixture"),
        );
        executor
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, version INTEGER NOT NULL, name TEXT)",
            )
            .unwrap();
        let evolved = entity().property(
            PropertyDescriptor::new("code", DataType::Text)
                .column_name("code")
                .not_null(),
        );

        executor.ensure_schema(&SqliteDialect, &[&evolved]).unwrap();
        executor.ensure_schema(&SqliteDialect, &[&evolved]).unwrap();
        let columns = executor.table_columns("orders").unwrap();
        assert_eq!(
            columns.get("code"),
            Some(&("VARCHAR(255)".to_owned(), false))
        );
    }

    #[test]
    fn ensure_schema_rejects_required_column_on_populated_sqlite_table_without_partial_change() {
        let executor = SqliteMutationExecutor::from_connection(
            Connection::open_in_memory().expect("open populated required-column fixture"),
        );
        executor
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, version INTEGER NOT NULL, name TEXT);
                 INSERT INTO orders(id, version, name) VALUES (1, 1, 'existing');",
            )
            .unwrap();
        let evolved = entity().property(
            PropertyDescriptor::new("code", DataType::Text)
                .column_name("code")
                .not_null(),
        );

        let message = executor
            .ensure_schema(&SqliteDialect, &[&evolved])
            .expect_err("required column without a backfill must fail before ALTER TABLE")
            .to_string();
        assert!(message.contains("entity=Order"), "{message}");
        assert!(message.contains("table=orders"), "{message}");
        assert!(message.contains("column=code"), "{message}");
        assert!(message.contains("migrate/backfill explicitly"), "{message}");
        assert!(
            !executor
                .table_columns("orders")
                .unwrap()
                .contains_key("code")
        );
    }

    #[test]
    fn concurrent_schema_evolution_is_idempotent_across_connections() {
        let path = std::env::temp_dir().join(format!(
            "teaql-concurrent-schema-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE teaql_concurrent_schema_fixture (
                    id INTEGER PRIMARY KEY NOT NULL,
                    version INTEGER NOT NULL
                 );",
            )
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                let executor = SqliteMutationExecutor::from_connection(
                    Connection::open(path).expect("open concurrent schema fixture"),
                );
                let entity = EntityDescriptor::new("ConcurrentSchemaFixture")
                    .table_name("teaql_concurrent_schema_fixture")
                    .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
                    .property(
                        PropertyDescriptor::new("version", DataType::I64)
                            .version()
                            .not_null(),
                    )
                    .property(PropertyDescriptor::new("name", DataType::Text))
                    .property(PropertyDescriptor::new("code", DataType::Text));
                barrier.wait();
                executor.ensure_schema(&SqliteDialect, &[&entity])
            }));
        }
        for thread in threads {
            thread.join().unwrap().unwrap();
        }
        let connection = Connection::open(&path).unwrap();
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('teaql_concurrent_schema_fixture')
                  WHERE name IN ('name', 'code')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
        drop(connection);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ensure_schema_creates_and_enforces_sqlite_foreign_keys() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let parent = EntityDescriptor::new("SqliteFkParentFixture")
            .table_name("teaql_sqlite_fk_parent_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .relation(
                RelationDescriptor::new("children", "SqliteFkChildFixture")
                    .local_key("id")
                    .foreign_key("parent_id")
                    .many(),
            );
        let child = EntityDescriptor::new("SqliteFkChildFixture")
            .table_name("teaql_sqlite_fk_child_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("parent", "SqliteFkParentFixture")
                    .local_key("parent_id")
                    .foreign_key("id"),
            );
        executor
            .ensure_schema(&SqliteDialect, &[&child, &parent])
            .unwrap();
        executor
            .ensure_schema(&SqliteDialect, &[&parent, &child])
            .unwrap();
        let connection = executor.lock().unwrap();
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_foreign_key_list('teaql_sqlite_fk_child_fixture')
                  WHERE `from`='parent_id'
                    AND `table`='teaql_sqlite_fk_parent_fixture'
                    AND `to`='id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        let violation = connection.execute(
            "INSERT INTO teaql_sqlite_fk_child_fixture(id, parent_id) VALUES (1, 999)",
            [],
        );
        assert!(violation.is_err());
    }

    #[test]
    fn ensure_schema_rejects_existing_sqlite_table_without_required_foreign_key() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        executor
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE teaql_sqlite_fk_parent_fixture (id INTEGER PRIMARY KEY NOT NULL);
                 CREATE TABLE teaql_sqlite_fk_child_fixture (
                   id INTEGER PRIMARY KEY NOT NULL,
                   parent_id INTEGER NOT NULL
                 );",
            )
            .unwrap();
        let parent = EntityDescriptor::new("SqliteFkParentFixture")
            .table_name("teaql_sqlite_fk_parent_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null());
        let child = EntityDescriptor::new("SqliteFkChildFixture")
            .table_name("teaql_sqlite_fk_child_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("parent", "SqliteFkParentFixture")
                    .local_key("parent_id")
                    .foreign_key("id"),
            );
        let message = executor
            .ensure_schema(&SqliteDialect, &[&child, &parent])
            .expect_err("SQLite cannot silently accept a missing physical foreign key")
            .to_string();
        assert!(message.contains("table=teaql_sqlite_fk_child_fixture"));
        assert!(message.contains("column=parent_id"));
        assert!(message.contains("rebuild/migrate the table explicitly"));
    }

    #[test]
    fn ensure_schema_rejects_composite_fk_that_matches_only_first_key() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let parent_table = "teaql_sqlite_fk_composite_parent";
        let child_table = "teaql_sqlite_fk_composite_child";
        executor
            .lock()
            .unwrap()
            .execute_batch(&format!(
                "CREATE TABLE {parent_table}(
                   id INTEGER PRIMARY KEY NOT NULL, scope_id INTEGER NOT NULL,
                   UNIQUE(id, scope_id));
                 CREATE TABLE {child_table}(
                   id INTEGER PRIMARY KEY NOT NULL, parent_id INTEGER NOT NULL,
                   scope_id INTEGER NOT NULL,
                   FOREIGN KEY(parent_id, scope_id) REFERENCES {parent_table}(id, scope_id));"
            ))
            .unwrap();
        let parent = EntityDescriptor::new("SqliteFkCompositeParent")
            .table_name(parent_table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null());
        let child = EntityDescriptor::new("SqliteFkCompositeChild")
            .table_name(child_table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("parent", "SqliteFkCompositeParent")
                    .local_key("parent_id")
                    .foreign_key("id"),
            );
        let message = executor
            .ensure_schema(&SqliteDialect, &[&child, &parent])
            .expect_err("a composite FK must not stand in for a single-key FK")
            .to_string();
        assert!(message.contains("installed_key_count=2"), "{message}");
        executor
            .lock()
            .unwrap()
            .execute_batch(&format!(
                "DROP TABLE {child_table};
                 CREATE TABLE {child_table}(
                   id INTEGER PRIMARY KEY NOT NULL, parent_id INTEGER NOT NULL,
                   FOREIGN KEY(parent_id) REFERENCES {parent_table}(id));"
            ))
            .unwrap();
        executor
            .ensure_schema(&SqliteDialect, &[&child, &parent])
            .expect("a full single-key FK must qualify after explicit rebuild");
        executor
            .ensure_schema(&SqliteDialect, &[&child, &parent])
            .expect("the rebuilt FK must remain idempotent");
        executor
            .lock()
            .unwrap()
            .execute_batch(&format!(
                "DROP TABLE {child_table};
                 CREATE TABLE {child_table}(
                   id INTEGER PRIMARY KEY NOT NULL, parent_id INTEGER NOT NULL,
                   FOREIGN KEY(parent_id) REFERENCES {parent_table}(id) ON DELETE CASCADE);"
            ))
            .unwrap();
        let message = executor
            .ensure_schema(&SqliteDialect, &[&child, &parent])
            .expect_err("CASCADE must not stand in for the generated RESTRICT/NO ACTION FK")
            .to_string();
        assert!(message.contains("installed_delete=CASCADE"), "{message}");
    }

    #[test]
    fn first_schema_install_creates_declared_indexes() {
        let executor = SqliteMutationExecutor::from_connection(
            Connection::open_in_memory().expect("open first-install index fixture"),
        );
        executor
            .ensure_schema(&SqliteDialect, &[&entity()])
            .unwrap();

        let index_count: i64 = executor
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(1) FROM sqlite_master WHERE type='index' AND name='PK_ORDERS_ID_VERSION'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(index_count, 1);
    }

    #[test]
    fn ensure_schema_rejects_colliding_declared_index_with_wrong_shape() {
        let executor = SqliteMutationExecutor::from_connection(
            Connection::open_in_memory().expect("open index-collision fixture"),
        );
        {
            let connection = executor.lock().unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE orders (id INTEGER NOT NULL, version INTEGER NOT NULL, name TEXT); \
                     CREATE INDEX PK_ORDERS_ID_VERSION ON orders (version, id);",
                )
                .unwrap();
        }
        let error = executor
            .ensure_schema(&SqliteDialect, &[&entity()])
            .expect_err("an existing index with reversed keys must not be accepted");
        let message = error.to_string();
        assert!(message.contains("PK_ORDERS_ID_VERSION"), "{message}");
        assert!(message.contains("orders"), "{message}");
        for definition in [
            "CREATE UNIQUE INDEX PK_ORDERS_ID_VERSION ON orders (id, version) WHERE version > 0",
            "CREATE UNIQUE INDEX PK_ORDERS_ID_VERSION ON orders (id DESC, version)",
            "CREATE UNIQUE INDEX PK_ORDERS_ID_VERSION ON orders (id, version COLLATE NOCASE)",
            "CREATE UNIQUE INDEX PK_ORDERS_ID_VERSION ON orders (id, (version + 1))",
        ] {
            {
                let connection = executor.lock().unwrap();
                connection
                    .execute_batch(&format!("DROP INDEX PK_ORDERS_ID_VERSION; {definition};"))
                    .unwrap();
            }
            let error = executor
                .ensure_schema(&SqliteDialect, &[&entity()])
                .expect_err("an incompatible index definition must be rejected");
            assert!(
                error.to_string().contains("PK_ORDERS_ID_VERSION"),
                "definition={definition}, error={error}"
            );
        }
    }

    #[test]
    fn user_context_transaction_scope_commits_and_rolls_back_on_one_connection() {
        let executor = SqliteMutationExecutor::from_connection(
            Connection::open_in_memory().expect("open transaction fixture"),
        );
        executor
            .connection()
            .lock()
            .expect("lock transaction fixture")
            .execute_batch(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, version INTEGER NOT NULL, name TEXT)",
            )
            .expect("create transaction fixture");

        let metadata = InMemoryMetadataStore::new().with_entity(entity());
        let data_service = teaql_sql::SqlDataServiceExecutor::new(
            SqliteDialect,
            executor.clone(),
            metadata.clone(),
        );
        let mut context = UserContext::new().with_metadata(metadata);
        context.register_executor(data_service);

        type Executor = teaql_sql::SqlDataServiceExecutor<
            SqliteDialect,
            SqliteMutationExecutor,
            InMemoryMetadataStore,
        >;

        futures_executor::block_on(context.execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                scope
                    .mutate(teaql_data_service::MutationRequest::Insert(
                        teaql_core::InsertCommand::new("Order")
                            .value("id", 1_u64)
                            .value("version", 1_i64)
                            .value("name", "first"),
                    ))
                    .await?;
                scope
                    .mutate(teaql_data_service::MutationRequest::Insert(
                        teaql_core::InsertCommand::new("Order")
                            .value("id", 2_u64)
                            .value("version", 1_i64)
                            .value("name", "second"),
                    ))
                    .await?;
                Ok(())
            })
        }))
        .expect("commit transaction scope");

        let failed =
            futures_executor::block_on(context.execute_in_transaction::<Executor, _, _>(|scope| {
                Box::pin(async move {
                    scope
                        .mutate(teaql_data_service::MutationRequest::Insert(
                            teaql_core::InsertCommand::new("Order")
                                .value("id", 3_u64)
                                .value("version", 1_i64)
                                .value("name", "must roll back"),
                        ))
                        .await?;
                    scope
                        .mutate(teaql_data_service::MutationRequest::Insert(
                            teaql_core::InsertCommand::new("Order")
                                .value("id", 1_u64)
                                .value("version", 1_i64)
                                .value("name", "duplicate"),
                        ))
                        .await?;
                    Ok(())
                })
            }));
        assert!(failed.is_err(), "duplicate key must fail the scope");

        let connection = executor.connection();
        let guard = connection.lock().expect("lock committed fixture");
        let ids = guard
            .prepare("SELECT id FROM orders ORDER BY id")
            .expect("prepare committed ids")
            .query_map([], |row| row.get::<_, i64>(0))
            .expect("query committed ids")
            .collect::<Result<Vec<_>, _>>()
            .expect("read committed ids");
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn typed_audited_saves_share_commit_and_rollback_boundary() {
        let executor = SqliteMutationExecutor::from_connection(
            Connection::open_in_memory().expect("open audited transaction fixture"),
        );
        executor
            .connection()
            .lock()
            .expect("lock audited transaction fixture")
            .execute_batch(
                "CREATE TABLE transaction_school (id INTEGER PRIMARY KEY, version INTEGER NOT NULL, name TEXT NOT NULL)",
            )
            .expect("create audited transaction fixture");

        let metadata = InMemoryMetadataStore::new()
            .with_entity(TransactionSchool::entity_descriptor().clone());
        let data_service = teaql_sql::SqlDataServiceExecutor::new(
            SqliteDialect,
            executor.clone(),
            metadata.clone(),
        );
        let mut context = UserContext::new().with_metadata(metadata);
        context.register_executor(data_service);

        type Executor = teaql_sql::SqlDataServiceExecutor<
            SqliteDialect,
            SqliteMutationExecutor,
            InMemoryMetadataStore,
        >;

        futures_executor::block_on(context.execute_in_transaction::<Executor, _, _>(|scope| {
            Box::pin(async move {
                let (first, _) = TransactionSchool::new(1, "first");
                let (second, _) = TransactionSchool::new(2, "second");
                let first = scope
                    .save_audited(first.audit_as("create first school"))
                    .await?;
                let second = scope
                    .save_audited(second.audit_as("create second school"))
                    .await?;
                assert_eq!(first.version, 1);
                assert_eq!(second.version, 1);
                Ok(())
            })
        }))
        .expect("commit audited transaction scope");

        let (third, third_ledger) = TransactionSchool::new(3, "must roll back");
        let failed =
            futures_executor::block_on(context.execute_in_transaction::<Executor, _, _>(|scope| {
                Box::pin(async move {
                    scope
                        .save_audited(third.audit_as("create third school"))
                        .await?;
                    let (duplicate, _) = TransactionSchool::new(1, "duplicate");
                    scope
                        .save_audited(duplicate.audit_as("force duplicate failure"))
                        .await?;
                    Ok(())
                })
            }));
        assert!(
            failed.is_err(),
            "duplicate audited save must fail the scope"
        );
        assert!(
            !third_ledger.new_keys().is_empty(),
            "rolled-back ledger must retain retryable mutation intent"
        );

        let connection = executor.connection();
        let guard = connection.lock().expect("lock audited committed fixture");
        let rows = guard
            .prepare("SELECT id, name FROM transaction_school ORDER BY id")
            .expect("prepare audited committed rows")
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .expect("query audited committed rows")
            .collect::<Result<Vec<_>, _>>()
            .expect("read audited committed rows");
        assert_eq!(
            rows,
            vec![(1, "first".to_owned()), (2, "second".to_owned())]
        );
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
    fn sqlite_relation_name_collision_hydrates_only_fk_and_updates_existing_parent() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let channel = PaymentChannelCollision::entity_descriptor();
        let order = PaymentOrderCollision::entity_descriptor();
        executor
            .ensure_schema(&SqliteDialect, &[&channel, &order])
            .unwrap();
        executor
            .execute(
                &SqliteDialect
                    .compile_insert(
                        &channel,
                        &InsertCommand::new("PaymentChannelCollision")
                            .value("id", 1002_u64)
                            .value("version", 1_i64)
                            .value("label", "Primary"),
                    )
                    .unwrap(),
            )
            .unwrap();
        executor
            .execute(
                &SqliteDialect
                    .compile_insert(
                        &order,
                        &InsertCommand::new("PaymentOrderCollision")
                            .value("id", 9_u64)
                            .value("version", 1_i64)
                            .value("channel_id", 1002_u64)
                            .value("name", "before"),
                    )
                    .unwrap(),
            )
            .unwrap();

        let select = SqliteDialect
            .compile_select(
                &order,
                &SelectQuery::new("PaymentOrderCollision").filter(Expr::eq("id", 9_u64)),
            )
            .unwrap();
        assert!(
            select.sql.contains("channel AS channel_id"),
            "{}",
            select.sql
        );
        let rows = executor.fetch_all_compact(&select).unwrap();
        assert_eq!(rows.len(), 1);
        let loaded = <PaymentOrderCollision as Entity>::from_compact_row(rows[0].clone()).unwrap();
        assert_eq!(loaded.channel_id, 1002);
        assert_eq!(
            loaded.channel, None,
            "scalar FK must not synthesize a child"
        );
        assert_eq!(loaded.version, 1);

        let affected = executor
            .execute(
                &SqliteDialect
                    .compile_update(
                        &order,
                        &UpdateCommand::new("PaymentOrderCollision", loaded.id)
                            .expected_version(loaded.version)
                            .value("name", "after"),
                    )
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(affected, 1);
        let rows = executor.fetch_all_compact(&select).unwrap();
        let reloaded =
            <PaymentOrderCollision as Entity>::from_compact_row(rows[0].clone()).unwrap();
        assert_eq!(reloaded.channel_id, 1002);
        assert_eq!(reloaded.channel, None);
        assert_eq!(reloaded.version, 2);
        assert_eq!(reloaded.name, "after");
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
    fn physical_schema_never_interprets_generated_bootstrap_graphs() {
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

        ensure_sqlite_physical_schema_for(&context).unwrap();

        let select = SqliteDialect
            .compile_select(
                &entity,
                &SelectQuery::new("Order").filter(Expr::eq("id", 1_u64)),
            )
            .unwrap();
        let rows = executor.fetch_all_compact(&select).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn public_schema_boundary_rejects_legacy_provider_owned_bootstrap_graphs() {
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
        let error = ensure_sqlite_schema_for(&context).unwrap_err();
        assert!(error.to_string().contains(
            "generated root/constant bootstrap must use the typed RuntimeModule callback"
        ));
        let count: i64 = executor
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
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
    fn sqlite_id_space_overflow_reports_safe_actionable_context() {
        let executor =
            SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
        let table = "teaql_id_space_diagnostic";
        let generator =
            SqliteIdSpaceGenerator::from_executor(executor.clone()).with_table_name(table);
        generator.ensure_table().unwrap();
        executor
            .execute(&CompiledQuery {
                sql: format!("INSERT INTO {table}(type_name, current_level) VALUES (?, ?)"),
                params: vec![Value::Text("order".to_owned()), Value::I64(i64::MAX)],
                comment: None,
            })
            .unwrap();

        let message = generator.next_id("Order").unwrap_err().to_string();
        assert!(message.contains("SQLite ID provider"), "{message}");
        assert!(message.contains("ID space order"), "{message}");
        assert!(message.contains(table), "{message}");
        assert!(message.contains("attempt 1"), "{message}");
        assert!(!message.contains("sqlite:"), "{message}");
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
