use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;

use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use teaql_core::{CompactRow, DataType, EntityDescriptor, PropertyDescriptor, Value};
#[cfg(test)]
use teaql_core::{InsertCommand, UpdateCommand};
#[cfg(test)]
use teaql_runtime::GraphNode;
use teaql_runtime::{
    InternalIdGenerator, RuntimeError, SchemaProvider, UserContext, canonical_id_space_entity,
};
use teaql_sql::{
    CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect, quote_identifier_if_needed,
    schema_foreign_key_name, schema_index_specs, validate_schema_identifier_lengths,
};
use tokio::sync::Mutex;

pub const DEFAULT_ID_SPACE_TABLE: &str = "teaql_id_space";

use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;

#[derive(Debug, Default, Clone, Copy)]
pub struct MysqlDialect;

fn mysql_foreign_key_specs(
    entities: &[&EntityDescriptor],
) -> Result<Vec<(String, String, String, String)>, MutationExecutorError> {
    let mut specs = BTreeSet::new();
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
            specs.insert((
                source.table_name.clone(),
                source_property.column_name.clone(),
                referenced.table_name.clone(),
                referenced_property.column_name.clone(),
            ));
        }
    }
    Ok(specs.into_iter().collect())
}

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
        property: &PropertyDescriptor,
    ) -> Result<String, SqlCompileError> {
        match data_type {
            DataType::Bool => Ok("BOOLEAN".to_owned()),
            DataType::I64 | DataType::U64 => Ok("BIGINT".to_owned()),
            DataType::F64 => Ok("DOUBLE".to_owned()),
            DataType::Decimal => match (property.numeric_precision, property.numeric_scale) {
                (Some(precision), Some(scale)) => Ok(format!("DECIMAL({precision}, {scale})")),
                _ => Ok("DECIMAL(38, 10)".to_owned()),
            },
            DataType::Text => Ok(format!("VARCHAR({})", property.max_length.unwrap_or(255))),
            DataType::LargeText => Ok("LONGTEXT".to_owned()),
            DataType::Json => Ok("JSON".to_owned()),
            DataType::Date => Ok("DATE".to_owned()),
            DataType::Timestamp => Ok("DATETIME(6)".to_owned()),
        }
    }

    fn compile_add_column(
        &self,
        entity: &EntityDescriptor,
        property: &PropertyDescriptor,
    ) -> Result<String, SqlCompileError> {
        Ok(format!(
            "ALTER TABLE {} ADD COLUMN {}",
            self.quote_ident(&entity.table_name),
            self.column_definition_sql(property)?
        ))
    }

    fn schema_indexes_sqls(
        &self,
        entity: &EntityDescriptor,
    ) -> Result<Vec<String>, SqlCompileError> {
        Ok(schema_index_specs(entity, Some(64))
            .into_iter()
            .map(|spec| {
                let columns = spec
                    .columns
                    .iter()
                    .map(|column| self.quote_ident(column))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "{} {} ON {} ({columns})",
                    if spec.unique {
                        "CREATE UNIQUE INDEX"
                    } else {
                        "CREATE INDEX"
                    },
                    self.quote_ident(&spec.name),
                    self.quote_ident(&spec.table),
                )
            })
            .collect())
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

#[derive(Debug, Clone)]
struct MysqlColumnMetadata {
    data_type: String,
    nullable: bool,
    max_length: Option<u32>,
    numeric_precision: Option<u32>,
    numeric_scale: Option<u32>,
}

type MysqlColumnMetadataRow = (
    String,
    String,
    String,
    Option<u64>,
    Option<u64>,
    Option<u64>,
);

impl MysqlMutationExecutor {
    pub fn new(pool: mysql_async::Pool) -> Self {
        Self { pool }
    }

    async fn ensure_schema(
        &self,
        dialect: &MysqlDialect,
        entities: &[&EntityDescriptor],
    ) -> Result<(), MutationExecutorError> {
        validate_schema_identifier_lengths(entities, 64)?;
        const SCHEMA_LOCK: &str = "SHA2(CONCAT('teaql-schema:', COALESCE(DATABASE(), '')), 256)";
        let mut conn = self.pool.get_conn().await?;
        let acquired: Option<i64> = conn
            .query_first(format!("SELECT GET_LOCK({SCHEMA_LOCK}, 30)"))
            .await?;
        if acquired != Some(1) {
            return Err(MutationExecutorError::Bind(
                "MySQL ensure schema could not acquire the database schema lock within 30 seconds"
                    .to_owned(),
            ));
        }

        let result = self
            .ensure_schema_with_connection(dialect, entities, &mut conn)
            .await;
        let released: Result<Option<i64>, mysql_async::Error> = conn
            .query_first(format!("SELECT RELEASE_LOCK({SCHEMA_LOCK})"))
            .await;
        match (result, released) {
            (Err(error), _) => Err(error),
            (Ok(()), Ok(Some(1))) => Ok(()),
            (Ok(()), Ok(other)) => Err(MutationExecutorError::Bind(format!(
                "MySQL ensure schema completed but did not release the database schema lock: result={other:?}"
            ))),
            (Ok(()), Err(error)) => Err(error.into()),
        }
    }

    async fn ensure_schema_with_connection(
        &self,
        dialect: &MysqlDialect,
        entities: &[&EntityDescriptor],
        conn: &mut mysql_async::Conn,
    ) -> Result<(), MutationExecutorError> {
        Self::preflight_schema_with_connection(dialect, entities, conn).await?;
        let id_space_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (type_name VARCHAR(100) PRIMARY KEY, current_level BIGINT NOT NULL)",
            mysql_quote_ident(DEFAULT_ID_SPACE_TABLE)
        );
        conn.exec_drop(&id_space_sql, ()).await?;

        for entity in entities {
            if !Self::table_exists_with_connection(conn, &entity.table_name).await? {
                let sql = dialect.compile_create_table(entity)?;
                conn.exec_drop(&sql, ()).await?;
            } else {
                let existing_columns =
                    Self::table_columns_with_connection(conn, &entity.table_name).await?;
                for property in &entity.properties {
                    let bare_column = strip_identifier_quotes(&property.column_name).to_lowercase();
                    if let Some(actual) = existing_columns.get(&bare_column) {
                        ensure_mysql_column_compatibility(entity, property, actual)?;
                        continue;
                    }
                    Self::ensure_required_column_can_be_added(conn, entity, property).await?;
                    let sql = dialect.compile_add_column(entity, property)?;
                    conn.exec_drop(&sql, ()).await?;
                }
            }

            for sql in dialect.schema_indexes_sqls(entity)? {
                match conn.exec_drop(&sql, ()).await {
                    Ok(_) => {}
                    Err(mysql_async::Error::Server(err)) if err.code == 1061 => {
                        // ER_DUP_KEYNAME
                    }
                    Err(e) => return Err(MutationExecutorError::MysqlAsync(e)),
                }
            }
            ensure_mysql_declared_index_shapes(conn, entity, false).await?;
        }

        // Install constraints only after all participating tables and columns
        // exist. Registration order must not change the physical schema.
        for (source_table, source_column, referenced_table, referenced_column) in
            mysql_foreign_key_specs(entities)?
        {
            Self::ensure_foreign_key_with_connection(
                conn,
                &source_table,
                &source_column,
                &referenced_table,
                &referenced_column,
                true,
            )
            .await?;
        }
        Ok(())
    }

    async fn preflight_schema_with_connection(
        dialect: &MysqlDialect,
        entities: &[&EntityDescriptor],
        conn: &mut mysql_async::Conn,
    ) -> Result<(), MutationExecutorError> {
        let foreign_keys = mysql_foreign_key_specs(entities)?;
        for entity in entities {
            // Compile all model-owned DDL before any statement can autocommit.
            dialect.compile_create_table(entity)?;
            dialect.schema_indexes_sqls(entity)?;
            if !Self::table_exists_with_connection(conn, &entity.table_name).await? {
                continue;
            }
            let existing_columns =
                Self::table_columns_with_connection(conn, &entity.table_name).await?;
            for property in &entity.properties {
                let bare_column = strip_identifier_quotes(&property.column_name).to_lowercase();
                if let Some(actual) = existing_columns.get(&bare_column) {
                    ensure_mysql_column_compatibility(entity, property, actual)?;
                } else {
                    Self::ensure_required_column_can_be_added(conn, entity, property).await?;
                }
            }
            ensure_mysql_declared_index_shapes(conn, entity, true).await?;
        }
        for (source_table, source_column, referenced_table, referenced_column) in foreign_keys {
            let installed = Self::ensure_foreign_key_with_connection(
                conn,
                &source_table,
                &source_column,
                &referenced_table,
                &referenced_column,
                false,
            )
            .await?;
            if !installed {
                Self::preflight_foreign_key_rows(
                    conn,
                    &source_table,
                    &source_column,
                    &referenced_table,
                    &referenced_column,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn preflight_foreign_key_rows(
        conn: &mut mysql_async::Conn,
        source_table: &str,
        source_column: &str,
        referenced_table: &str,
        referenced_column: &str,
    ) -> Result<(), MutationExecutorError> {
        if !Self::table_exists_with_connection(conn, source_table).await? {
            return Ok(());
        }
        let source_columns = Self::table_columns_with_connection(conn, source_table).await?;
        if !source_columns.contains_key(&strip_identifier_quotes(source_column).to_lowercase()) {
            // A newly added optional FK column starts NULL. Required additions
            // to populated tables already fail in the column preflight.
            return Ok(());
        }
        let target_exists = Self::table_exists_with_connection(conn, referenced_table).await?;
        let target_has_column = if target_exists {
            Self::table_columns_with_connection(conn, referenced_table)
                .await?
                .contains_key(&strip_identifier_quotes(referenced_column).to_lowercase())
        } else {
            false
        };
        let source = mysql_quote_ident(source_table);
        let source_key = mysql_quote_ident(source_column);
        let sql = if target_has_column {
            let target = mysql_quote_ident(referenced_table);
            let target_key = mysql_quote_ident(referenced_column);
            format!(
                "SELECT EXISTS(
                   SELECT 1 FROM {source} teaql_source
                   LEFT JOIN {target} teaql_target
                     ON teaql_source.{source_key} = teaql_target.{target_key}
                  WHERE teaql_source.{source_key} IS NOT NULL
                    AND teaql_target.{target_key} IS NULL LIMIT 1)"
            )
        } else {
            format!(
                "SELECT EXISTS(SELECT 1 FROM {source}
                  WHERE {source_key} IS NOT NULL LIMIT 1)"
            )
        };
        let has_orphans: Option<bool> = conn.query_first(sql).await?;
        if has_orphans.unwrap_or(false) {
            return Err(MutationExecutorError::Bind(format!(
                "MySQL ensure schema cannot install foreign key with orphan rows: table={source_table}, column={source_column}, referenced_table={referenced_table}, referenced_column={referenced_column}; repair/backfill the existing rows before retrying"
            )));
        }
        Ok(())
    }

    async fn ensure_foreign_key_with_connection(
        conn: &mut mysql_async::Conn,
        source_table: &str,
        source_column: &str,
        referenced_table: &str,
        referenced_column: &str,
        install_missing: bool,
    ) -> Result<bool, MutationExecutorError> {
        let existing: Vec<(String, String, String, u64)> = conn
            .exec(
                "SELECT CAST(k.CONSTRAINT_NAME AS CHAR), CAST(rc.UPDATE_RULE AS CHAR),
                        CAST(rc.DELETE_RULE AS CHAR),
                        (SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE k2
                          WHERE k2.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
                            AND k2.TABLE_NAME = k.TABLE_NAME
                            AND k2.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                            AND k2.REFERENCED_TABLE_NAME IS NOT NULL) AS key_count
                   FROM information_schema.KEY_COLUMN_USAGE k
                   JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                     ON rc.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
                    AND rc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                    AND rc.TABLE_NAME = k.TABLE_NAME
                  WHERE k.CONSTRAINT_SCHEMA = DATABASE()
                    AND k.TABLE_NAME = ?
                    AND k.COLUMN_NAME = ?
                    AND k.REFERENCED_TABLE_NAME = ?
                    AND k.REFERENCED_COLUMN_NAME = ?",
                (
                    strip_identifier_quotes(source_table),
                    strip_identifier_quotes(source_column),
                    strip_identifier_quotes(referenced_table),
                    strip_identifier_quotes(referenced_column),
                ),
            )
            .await?;
        let restricts = |rule: &str| {
            rule.eq_ignore_ascii_case("NO ACTION") || rule.eq_ignore_ascii_case("RESTRICT")
        };
        let mut incompatible = None;
        for (name, update_rule, delete_rule, key_count) in existing {
            if key_count == 1 && restricts(&update_rule) && restricts(&delete_rule) {
                return Ok(true);
            }
            incompatible.get_or_insert_with(|| format!(
                "MySQL ensure schema incompatible existing foreign key: table={source_table}, column={source_column}, referenced_table={referenced_table}, referenced_column={referenced_column}, expected=one full-column key with RESTRICT actions, installed_constraint={name}, installed_key_count={key_count}, installed_update={update_rule}, installed_delete={delete_rule}; migrate the constraint explicitly before retrying"
            ));
        }
        if let Some(message) = incompatible {
            return Err(MutationExecutorError::Bind(message));
        }

        let constraint_name = schema_foreign_key_name(
            source_table,
            source_column,
            referenced_table,
            referenced_column,
            64,
        );
        let collision: Option<(String, String)> = conn
            .exec_first(
                "SELECT CAST(CONSTRAINT_TYPE AS CHAR), CAST(TABLE_NAME AS CHAR)
                   FROM information_schema.TABLE_CONSTRAINTS
                  WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = ?
                  LIMIT 1",
                (&constraint_name,),
            )
            .await?;
        if let Some((kind, table)) = collision {
            return Err(MutationExecutorError::Bind(format!(
                "MySQL ensure schema foreign-key name collision: table={source_table}, constraint={constraint_name}, expected=FOREIGN KEY ({source_column}) REFERENCES {referenced_table}({referenced_column}) with RESTRICT, installed_kind={kind}, installed_table={table}; rename/drop the colliding constraint or migrate explicitly before retrying"
            )));
        }
        if !install_missing {
            return Ok(false);
        }
        let sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
            mysql_quote_ident(source_table),
            mysql_quote_ident(&constraint_name),
            mysql_quote_ident(source_column),
            mysql_quote_ident(referenced_table),
            mysql_quote_ident(referenced_column),
        );
        conn.exec_drop(sql, ()).await?;
        Ok(true)
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
            query.sql_with_comment(),
            mysql_async::Params::Positional(params),
        )
        .await?;
        Ok(conn.affected_rows())
    }

    pub async fn fetch_all_compact(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, MutationExecutorError> {
        let params = query
            .params
            .iter()
            .map(bind_mysql)
            .collect::<Result<Vec<_>, _>>()?;
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<mysql_async::Row> = conn
            .exec(
                query.sql_with_comment(),
                mysql_async::Params::Positional(params),
            )
            .await?;
        decode_mysql_compact_rows(rows)
    }

    async fn table_exists_with_connection(
        conn: &mut mysql_async::Conn,
        table_name: &str,
    ) -> Result<bool, MutationExecutorError> {
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

    async fn table_columns_with_connection(
        conn: &mut mysql_async::Conn,
        table_name: &str,
    ) -> Result<std::collections::BTreeMap<String, MysqlColumnMetadata>, MutationExecutorError>
    {
        let rows: Vec<MysqlColumnMetadataRow> = conn
            .exec(
                "SELECT CAST(column_name AS CHAR), CAST(data_type AS CHAR), CAST(is_nullable AS CHAR),
                        character_maximum_length, numeric_precision, numeric_scale
             FROM information_schema.columns
             WHERE table_schema = DATABASE()
               AND table_name = ?",
                (table_name,),
            )
            .await?;
        let mut columns = std::collections::BTreeMap::new();
        for (name, data_type, is_nullable, max_length, numeric_precision, numeric_scale) in rows {
            columns.insert(
                name.to_lowercase(),
                MysqlColumnMetadata {
                    data_type,
                    nullable: is_nullable.eq_ignore_ascii_case("YES"),
                    max_length: max_length.and_then(|value| value.try_into().ok()),
                    numeric_precision: numeric_precision.and_then(|value| value.try_into().ok()),
                    numeric_scale: numeric_scale.and_then(|value| value.try_into().ok()),
                },
            );
        }
        Ok(columns)
    }

    async fn ensure_required_column_can_be_added(
        conn: &mut mysql_async::Conn,
        entity: &EntityDescriptor,
        property: &PropertyDescriptor,
    ) -> Result<(), MutationExecutorError> {
        if property.nullable {
            return Ok(());
        }
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM {} LIMIT 1)",
            mysql_quote_ident(&entity.table_name)
        );
        let has_rows: Option<bool> = conn.query_first(sql).await?;
        if !has_rows.unwrap_or(false) {
            return Ok(());
        }
        Err(MutationExecutorError::Bind(format!(
            "ensure schema cannot add required column without a deterministic backfill: entity={}, table={}, column={}; the table contains rows, so migrate/backfill explicitly before retrying",
            entity.name, entity.table_name, property.column_name
        )))
    }
}

fn ensure_mysql_column_compatibility(
    entity: &EntityDescriptor,
    property: &PropertyDescriptor,
    metadata: &MysqlColumnMetadata,
) -> Result<(), MutationExecutorError> {
    let actual = metadata.data_type.trim().to_ascii_lowercase();
    let compatible = match property.data_type {
        DataType::Bool => matches!(actual.as_str(), "tinyint" | "boolean" | "bool"),
        DataType::I64 | DataType::U64 => actual == "bigint",
        DataType::F64 => matches!(actual.as_str(), "double" | "float"),
        DataType::Decimal => matches!(actual.as_str(), "decimal" | "numeric"),
        DataType::Text => matches!(actual.as_str(), "varchar" | "text"),
        DataType::LargeText => matches!(actual.as_str(), "text" | "mediumtext" | "longtext"),
        DataType::Json => actual == "json",
        DataType::Date => actual == "date",
        DataType::Timestamp => matches!(actual.as_str(), "datetime" | "timestamp"),
    };
    if !compatible {
        return Err(MutationExecutorError::Bind(format!(
            "ensure schema incompatible column type: entity={}, table={}, column={}, expected={:?}, actual={actual}",
            entity.name, entity.table_name, property.column_name, property.data_type
        )));
    }
    if property.nullable != metadata.nullable {
        return Err(MutationExecutorError::Bind(format!(
            "ensure schema incompatible column nullability: entity={}, table={}, column={}, expected_nullable={}, actual_nullable={}",
            entity.name,
            entity.table_name,
            property.column_name,
            property.nullable,
            metadata.nullable
        )));
    }
    if let Some(expected) = property.max_length
        && !teaql_sql::storage_length_covers(expected, metadata.max_length)
    {
        return Err(MutationExecutorError::Bind(format!(
            "ensure schema existing column is too narrow: entity={}, table={}, column={}, required_max_length={expected}, actual_max_length={:?}",
            entity.name, entity.table_name, property.column_name, metadata.max_length
        )));
    }
    if let (Some(expected_precision), Some(expected_scale)) =
        (property.numeric_precision, property.numeric_scale)
        && !teaql_sql::storage_numeric_covers(
            expected_precision,
            expected_scale,
            metadata.numeric_precision,
            metadata.numeric_scale,
        )
    {
        return Err(MutationExecutorError::Bind(format!(
            "ensure schema existing numeric column does not cover the model value domain: entity={}, table={}, column={}, required_precision={expected_precision}, required_scale={expected_scale}, actual_precision={:?}, actual_scale={:?}",
            entity.name,
            entity.table_name,
            property.column_name,
            metadata.numeric_precision,
            metadata.numeric_scale
        )));
    }
    Ok(())
}

impl teaql_sql::SqlTransport for MysqlMutationExecutor {
    type Error = MutationExecutorError;

    async fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, Self::Error> {
        self.fetch_all_compact(query).await
    }

    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        self.execute(query).await
    }
}

impl teaql_sql::StreamingSqlTransport for MysqlMutationExecutor {
    fn stream_sql(
        &self,
        query: CompiledQuery,
        chunk_size: usize,
    ) -> teaql_data_service::QueryStream<'_, Self::Error> {
        let pool = self.pool.clone();
        Box::pin(async_stream::try_stream! {
            use futures_util::StreamExt;
            let params = query.params.iter().map(bind_mysql).collect::<Result<Vec<_>, _>>()?;
            let mut conn = pool.get_conn().await?;
            let mut stream = conn
                .exec_stream::<mysql_async::Row, _, _>(query.sql_with_comment(), params)
                .await?;
            let mut columns: Option<Arc<[String]>> = None;
            let mut chunk=Vec::with_capacity(chunk_size); let mut index=0;
            while let Some(row) = stream.next().await {
                let row = row?;
                let shared_columns = columns.get_or_insert_with(|| row.columns_ref().iter().map(|column| column.name_str().into_owned()).collect::<Vec<_>>().into()).clone();
                chunk.push(CompactRow::new(shared_columns, decode_mysql_values(row)?));
                if chunk.len()==chunk_size { yield teaql_data_service::StreamChunk { rows:std::mem::take(&mut chunk), chunk_index:index, is_last:false }; index+=1; }
            }
            if !chunk.is_empty() { yield teaql_data_service::StreamChunk { rows:chunk, chunk_index:index, is_last:true }; }
        })
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

    async fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, Self::Error> {
        self.fetch_all_compact(query).await
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
            query.sql_with_comment(),
            mysql_async::Params::Positional(params),
        )
        .await?;
        Ok(conn.affected_rows())
    }

    pub async fn fetch_all_compact(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, MutationExecutorError> {
        let params = query
            .params
            .iter()
            .map(bind_mysql)
            .collect::<Result<Vec<_>, _>>()?;
        let mut lock = self.conn.lock().await;
        let conn = lock
            .as_mut()
            .ok_or_else(|| MutationExecutorError::Bind("mysql transaction is closed".to_owned()))?;
        let rows: Vec<mysql_async::Row> = conn
            .exec(
                query.sql_with_comment(),
                mysql_async::Params::Positional(params),
            )
            .await?;
        decode_mysql_compact_rows(rows)
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

pub(crate) async fn ensure_mysql_schema_for(
    context: &UserContext,
) -> Result<(), MutationExecutorError> {
    let dialect = context.get_resource::<MysqlDialect>().ok_or_else(|| {
        MutationExecutorError::Bind("missing typed resource: MysqlDialect".to_owned())
    })?;
    let executor = context
        .get_resource::<MysqlMutationExecutor>()
        .ok_or_else(|| {
            MutationExecutorError::Bind("missing typed resource: MysqlMutationExecutor".to_owned())
        })?;

    let entities = context.all_entities();

    executor.ensure_schema(dialect, &entities).await?;
    if !context.initial_graphs().is_empty() || !context.root_graphs().is_empty() {
        return Err(MutationExecutorError::Bind(
            "generated root/constant bootstrap must use the typed RuntimeModule callback"
                .to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use futures_util::StreamExt;
    use teaql_core::RelationDescriptor;
    use teaql_sql::{SqlTransport, StreamingSqlTransport};

    fn schema_entity(name: &str, table: &str) -> EntityDescriptor {
        EntityDescriptor::new(name)
            .table_name(table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text))
    }

    #[tokio::test]
    async fn context_schema_rejects_provider_owned_bootstrap_graphs_when_configured() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let pool = mysql_async::Pool::new(url.as_str());
        let executor = MysqlMutationExecutor::new(pool.clone());
        let entity = schema_entity("LegacyBootstrapProbe", "teaql_gabm_legacy_mysql_rejection");
        let mut context = UserContext::new()
            .with_metadata(teaql_runtime::InMemoryMetadataStore::new().with_entity(entity));
        context.set_initial_graphs(vec![
            GraphNode::new("LegacyBootstrapProbe")
                .value("id", 1001_u64)
                .value("version", 1_i64)
                .value("name", "provider-owned bootstrap must not run"),
        ]);
        context.use_mysql_provider(executor);
        let error = context.ensure_schema().await.unwrap_err();
        assert!(error.to_string().contains(
            "generated root/constant bootstrap must use the typed RuntimeModule callback"
        ));
        context.set_initial_graphs(Vec::new());
        context.set_root_graphs(vec![
            GraphNode::new("LegacyBootstrapProbe")
                .value("id", 1_u64)
                .value("version", 1_i64)
                .value("name", "provider-owned root must not run"),
        ]);
        let root_error = context.ensure_schema().await.unwrap_err();
        assert!(root_error.to_string().contains(
            "generated root/constant bootstrap must use the typed RuntimeModule callback"
        ));
        let mut conn = pool.get_conn().await.unwrap();
        let count: Option<u64> = conn
            .query_first("SELECT COUNT(*) FROM teaql_gabm_legacy_mysql_rejection")
            .await
            .unwrap();
        assert_eq!(count, Some(0));
    }

    #[tokio::test]
    async fn streams_from_real_mysql_when_configured() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let executor = MysqlMutationExecutor::new(mysql_async::Pool::new(url.as_str()));
        let query = CompiledQuery { sql: "SELECT id FROM (SELECT 1 id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) fixture ORDER BY id".to_owned(), params: vec![], comment: None };
        let mut stream = executor.stream_sql(query, 2);
        let mut sizes = Vec::new();
        while let Some(chunk) = stream.next().await {
            sizes.push(chunk.unwrap().rows.len());
        }
        assert_eq!(sizes, vec![2, 2, 1]);
    }

    #[tokio::test]
    async fn boolean_roundtrips_real_mysql_when_configured() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let executor = MysqlMutationExecutor::new(mysql_async::Pool::new(url.as_str()));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE IF EXISTS teaql_boolean_runtime_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: "CREATE TABLE teaql_boolean_runtime_fixture(id BIGINT, required_flag BOOLEAN NOT NULL, optional_flag BOOLEAN)".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        for (id, required_flag, optional_flag) in [
            (1_i64, Value::Bool(false), Value::Bool(true)),
            (2_i64, Value::Bool(true), Value::Bool(false)),
            (3_i64, Value::Bool(true), Value::Null),
        ] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: "INSERT INTO teaql_boolean_runtime_fixture VALUES (?, ?, ?)".to_owned(),
                    params: vec![Value::I64(id), required_flag, optional_flag],
                    comment: None,
                })
                .await
                .unwrap();
        }
        let rows = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT required_flag, optional_flag FROM teaql_boolean_runtime_fixture ORDER BY id".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        assert_eq!(rows[0].get("required_flag"), Some(&Value::Bool(false)));
        assert_eq!(rows[0].get("optional_flag"), Some(&Value::Bool(true)));
        assert_eq!(rows[1].get("required_flag"), Some(&Value::Bool(true)));
        assert_eq!(rows[1].get("optional_flag"), Some(&Value::Bool(false)));
        assert_eq!(rows[2].get("optional_flag"), Some(&Value::Null));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_boolean_runtime_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_rejects_incompatible_existing_column_type() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let executor = MysqlMutationExecutor::new(mysql_async::Pool::new(url.as_str()));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE IF EXISTS teaql_schema_type_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: "CREATE TABLE teaql_schema_type_fixture (id BIGINT PRIMARY KEY, version BIGINT NOT NULL, name BIGINT)".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let entity = schema_entity("SchemaTypeFixture", "teaql_schema_type_fixture");
        let error = executor
            .ensure_schema(&MysqlDialect, &[&entity])
            .await
            .expect_err("incompatible storage type must fail schema ensure");
        let message = error.to_string();
        assert!(message.contains("entity=SchemaTypeFixture"), "{message}");
        assert!(
            message.contains("table=teaql_schema_type_fixture"),
            "{message}"
        );
        assert!(message.contains("column=name"), "{message}");
        assert!(message.contains("expected=Text"), "{message}");
        assert!(message.contains("actual=bigint"), "{message}");
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_schema_type_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_preflights_later_entity_before_earlier_ddl() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let pool = mysql_async::Pool::new(url.as_str());
        let executor = MysqlMutationExecutor::new(pool.clone());
        let fresh_table = "teaql_mysql_preflight_fresh";
        let incompatible_table = "teaql_mysql_preflight_incompatible";
        for table in [fresh_table, incompatible_table] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: format!("DROP TABLE IF EXISTS {table}"),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        executor
            .execute_sql(&CompiledQuery {
                sql: format!(
                    "CREATE TABLE {incompatible_table}(
                       id BIGINT PRIMARY KEY, version BIGINT NOT NULL, name BIGINT)"
                ),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let fresh = schema_entity("MysqlPreflightFresh", fresh_table);
        let incompatible = schema_entity("MysqlPreflightIncompatible", incompatible_table);
        let message = executor
            .ensure_schema(&MysqlDialect, &[&fresh, &incompatible])
            .await
            .expect_err("the later incompatible entity must reject the full ensure")
            .to_string();
        assert!(message.contains(incompatible_table), "{message}");
        let mut conn = pool.get_conn().await.unwrap();
        let present: Option<i64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.tables
                  WHERE table_schema=DATABASE() AND table_name=?",
                (fresh_table,),
            )
            .await
            .unwrap();
        assert_eq!(
            present,
            Some(0),
            "failed ensure must not create earlier table"
        );
        drop(conn);
        executor
            .execute_sql(&CompiledQuery {
                sql: format!("DROP TABLE {incompatible_table}"),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_preflights_orphan_rows_before_unrelated_ddl() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let pool = mysql_async::Pool::new(url.as_str());
        let executor = MysqlMutationExecutor::new(pool.clone());
        let fresh_table = "teaql_mysql_orphan_preflight_fresh";
        let parent_table = "teaql_mysql_orphan_preflight_parent";
        let child_table = "teaql_mysql_orphan_preflight_child";
        for table in [child_table, parent_table, fresh_table] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: format!("DROP TABLE IF EXISTS {table}"),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        for sql in [
            format!("CREATE TABLE {parent_table}(id BIGINT PRIMARY KEY)"),
            format!("CREATE TABLE {child_table}(id BIGINT PRIMARY KEY, parent_id BIGINT NOT NULL)"),
            format!("INSERT INTO {child_table}(id, parent_id) VALUES (1, 999)"),
        ] {
            executor
                .execute_sql(&CompiledQuery {
                    sql,
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        let fresh = schema_entity("MysqlOrphanPreflightFresh", fresh_table);
        let parent = EntityDescriptor::new("MysqlOrphanPreflightParent")
            .table_name(parent_table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null());
        let child = EntityDescriptor::new("MysqlOrphanPreflightChild")
            .table_name(child_table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("parent", "MysqlOrphanPreflightParent")
                    .local_key("parent_id")
                    .foreign_key("id"),
            );
        let message = executor
            .ensure_schema(&MysqlDialect, &[&fresh, &child, &parent])
            .await
            .expect_err("orphan rows must reject FK installation")
            .to_string();
        assert!(
            message.contains("orphan") && message.contains(child_table),
            "{message}"
        );
        let mut conn = pool.get_conn().await.unwrap();
        let present: Option<i64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.tables
                  WHERE table_schema=DATABASE() AND table_name=?",
                (fresh_table,),
            )
            .await
            .unwrap();
        assert_eq!(
            present,
            Some(0),
            "orphan preflight must precede earlier DDL"
        );
        drop(conn);
        executor
            .execute_sql(&CompiledQuery {
                sql: format!("INSERT INTO {parent_table}(id) VALUES (999)"),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .ensure_schema(&MysqlDialect, &[&fresh, &child, &parent])
            .await
            .expect("repairing orphan rows must allow schema installation");
        executor
            .ensure_schema(&MysqlDialect, &[&fresh, &child, &parent])
            .await
            .expect("the installed FK must remain idempotent");
        for table in [child_table, parent_table, fresh_table] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: format!("DROP TABLE {table}"),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_accepts_covering_shapes_and_rejects_narrower_mysql_columns() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let executor = MysqlMutationExecutor::new(mysql_async::Pool::new(url.as_str()));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE IF EXISTS teaql_schema_shape_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: "CREATE TABLE teaql_schema_shape_fixture (id BIGINT PRIMARY KEY, version BIGINT NOT NULL, name VARCHAR(255), amount DECIMAL(38,10))".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let base = || schema_entity("SchemaShapeFixture", "teaql_schema_shape_fixture");
        let length_model = base().property(
            PropertyDescriptor::new("limited_name", DataType::Text)
                .column_name("name")
                .max_length(100),
        );
        executor
            .ensure_schema(&MysqlDialect, &[&length_model])
            .await
            .expect("wider VARCHAR storage must cover the model");

        let numeric_model = base().property(
            PropertyDescriptor::new("amount", DataType::Decimal)
                .numeric_precision(19)
                .numeric_scale(7),
        );
        executor
            .ensure_schema(&MysqlDialect, &[&numeric_model])
            .await
            .expect("wider DECIMAL storage must cover the model");

        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_schema_shape_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: "CREATE TABLE teaql_schema_shape_fixture (id BIGINT PRIMARY KEY, version BIGINT NOT NULL, name VARCHAR(32), amount DECIMAL(18,2))".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let message = executor
            .ensure_schema(&MysqlDialect, &[&length_model])
            .await
            .expect_err("narrower VARCHAR storage must fail")
            .to_string();
        assert!(message.contains("required_max_length=100"), "{message}");
        assert!(message.contains("actual_max_length=Some(32)"), "{message}");

        let message = executor
            .ensure_schema(&MysqlDialect, &[&numeric_model])
            .await
            .expect_err("narrower numeric storage must fail")
            .to_string();
        assert!(message.contains("required_precision=19"), "{message}");
        assert!(message.contains("required_scale=7"), "{message}");
        assert!(message.contains("actual_precision=Some(18)"), "{message}");
        assert!(message.contains("actual_scale=Some(2)"), "{message}");
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_schema_shape_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_rejects_incompatible_existing_column_nullability() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let executor = MysqlMutationExecutor::new(mysql_async::Pool::new(url.as_str()));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE IF EXISTS teaql_schema_nullability_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: "CREATE TABLE teaql_schema_nullability_fixture (id BIGINT PRIMARY KEY, version BIGINT NOT NULL, name VARCHAR(255))".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let entity = schema_entity(
            "SchemaNullabilityFixture",
            "teaql_schema_nullability_fixture",
        )
        .property(
            PropertyDescriptor::new("required_name", DataType::Text)
                .column_name("name")
                .not_null(),
        );

        let message = executor
            .ensure_schema(&MysqlDialect, &[&entity])
            .await
            .expect_err("nullable storage must not satisfy a required model field")
            .to_string();
        assert!(
            message.contains("entity=SchemaNullabilityFixture"),
            "{message}"
        );
        assert!(message.contains("column=name"), "{message}");
        assert!(message.contains("expected_nullable=false"), "{message}");
        assert!(message.contains("actual_nullable=true"), "{message}");
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_schema_nullability_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn required_column_evolution_has_no_mysql_magic_default() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let executor = MysqlMutationExecutor::new(mysql_async::Pool::new(url.as_str()));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE IF EXISTS teaql_required_evolution_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: "CREATE TABLE teaql_required_evolution_fixture (id BIGINT PRIMARY KEY, version BIGINT NOT NULL, name VARCHAR(255))".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: "INSERT INTO teaql_required_evolution_fixture(id, version, name) VALUES (1, 1, 'existing')".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let entity = schema_entity(
            "RequiredEvolutionFixture",
            "teaql_required_evolution_fixture",
        )
        .property(PropertyDescriptor::new("code", DataType::Text).not_null());

        let message = executor
            .ensure_schema(&MysqlDialect, &[&entity])
            .await
            .expect_err("required column without a backfill must fail before ALTER TABLE")
            .to_string();
        assert!(
            message.contains("entity=RequiredEvolutionFixture"),
            "{message}"
        );
        assert!(message.contains("column=code"), "{message}");
        assert!(message.contains("migrate/backfill explicitly"), "{message}");
        let rows = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT COUNT(*) AS column_count FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='teaql_required_evolution_fixture' AND column_name='code'".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        assert_eq!(rows[0].get("column_count"), Some(&Value::I64(0)));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_required_evolution_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: "CREATE TABLE teaql_required_evolution_fixture (id BIGINT PRIMARY KEY, version BIGINT NOT NULL, name VARCHAR(255))".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .ensure_schema(&MysqlDialect, &[&entity])
            .await
            .unwrap();
        executor
            .ensure_schema(&MysqlDialect, &[&entity])
            .await
            .unwrap();
        executor
            .ensure_schema(&MysqlDialect, &[&entity])
            .await
            .expect("normal installed indexes must remain idempotent");
        let rows = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT CAST(is_nullable AS CHAR) AS is_nullable, column_default IS NULL AS has_no_default FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='teaql_required_evolution_fixture' AND column_name='code'".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        assert_eq!(
            rows[0].get("is_nullable"),
            Some(&Value::Text("NO".to_owned()))
        );
        assert_eq!(rows[0].get("has_no_default"), Some(&Value::I64(1)));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_required_evolution_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn concurrent_schema_evolution_is_idempotent_when_configured() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let pool = mysql_async::Pool::new(url.as_str());
        let executor = MysqlMutationExecutor::new(pool.clone());
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE IF EXISTS teaql_concurrent_schema_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: "CREATE TABLE teaql_concurrent_schema_fixture (id BIGINT PRIMARY KEY, version BIGINT NOT NULL)".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let entity = Arc::new(
            schema_entity("ConcurrentSchemaFixture", "teaql_concurrent_schema_fixture")
                .property(PropertyDescriptor::new("code", DataType::Text)),
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let executor = executor.clone();
            let entity = Arc::clone(&entity);
            let barrier = Arc::clone(&barrier);
            tasks.spawn(async move {
                barrier.wait().await;
                executor
                    .ensure_schema(&MysqlDialect, &[entity.as_ref()])
                    .await
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
        let rows = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT COUNT(*) AS column_count FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='teaql_concurrent_schema_fixture' AND column_name='code'".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        assert_eq!(rows[0].get("column_count"), Some(&Value::I64(1)));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_concurrent_schema_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_creates_foreign_key_once_by_semantics() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let pool = mysql_async::Pool::new(url.as_str());
        let executor = MysqlMutationExecutor::new(pool.clone());
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE IF EXISTS teaql_mysql_fk_child_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE IF EXISTS teaql_mysql_fk_parent_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();

        let parent = EntityDescriptor::new("MysqlFkParentFixture")
            .table_name("teaql_mysql_fk_parent_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .relation(
                RelationDescriptor::new("children", "MysqlFkChildFixture")
                    .local_key("id")
                    .foreign_key("parent_id")
                    .many(),
            );
        let child = EntityDescriptor::new("MysqlFkChildFixture")
            .table_name("teaql_mysql_fk_child_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("parent", "MysqlFkParentFixture")
                    .local_key("parent_id")
                    .foreign_key("id"),
            );
        executor
            .ensure_schema(&MysqlDialect, &[&child, &parent])
            .await
            .unwrap();
        executor
            .ensure_schema(&MysqlDialect, &[&parent, &child])
            .await
            .unwrap();

        let rows = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT COUNT(*) AS constraint_count
                        FROM information_schema.KEY_COLUMN_USAGE
                       WHERE CONSTRAINT_SCHEMA=DATABASE()
                         AND TABLE_NAME='teaql_mysql_fk_child_fixture'
                         AND COLUMN_NAME='parent_id'
                         AND REFERENCED_TABLE_NAME='teaql_mysql_fk_parent_fixture'
                         AND REFERENCED_COLUMN_NAME='id'"
                    .to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        assert_eq!(rows[0].get("constraint_count"), Some(&Value::I64(1)));

        let violation = executor
            .execute_sql(&CompiledQuery {
                sql: "INSERT INTO teaql_mysql_fk_child_fixture(id, parent_id) VALUES (1, 999)"
                    .to_owned(),
                params: vec![],
                comment: None,
            })
            .await;
        assert!(violation.is_err());

        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_mysql_fk_child_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_mysql_fk_parent_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_reports_colliding_foreign_key_name_before_add_constraint() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let pool = mysql_async::Pool::new(url.as_str());
        let executor = MysqlMutationExecutor::new(pool.clone());
        let target = "teaql_mysql_fk_name_target";
        let wrong = "teaql_mysql_fk_name_wrong";
        let child_table = "teaql_mysql_fk_name_child";
        let fresh_table = "teaql_mysql_fk_name_fresh";
        for table in [child_table, target, wrong, fresh_table] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: format!("DROP TABLE IF EXISTS {table}"),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        let name = schema_foreign_key_name(child_table, "parent_id", target, "id", 64);
        for sql in [
            format!("CREATE TABLE {target}(id BIGINT PRIMARY KEY)"),
            format!("CREATE TABLE {wrong}(id BIGINT PRIMARY KEY)"),
            format!(
                "CREATE TABLE {child_table}(
                   id BIGINT PRIMARY KEY, parent_id BIGINT NOT NULL,
                   CONSTRAINT {} FOREIGN KEY(parent_id) REFERENCES {wrong}(id)
                 )",
                mysql_quote_ident(&name)
            ),
        ] {
            executor
                .execute_sql(&CompiledQuery {
                    sql,
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        let parent = EntityDescriptor::new("MysqlFkNameTarget")
            .table_name(target)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null());
        let child = EntityDescriptor::new("MysqlFkNameChild")
            .table_name(child_table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("parent", "MysqlFkNameTarget")
                    .local_key("parent_id")
                    .foreign_key("id"),
            );
        let fresh = schema_entity("MysqlFkNameFresh", fresh_table);
        let message = executor
            .ensure_schema(&MysqlDialect, &[&fresh, &child, &parent])
            .await
            .expect_err("a same-name wrong-target constraint must fail before ADD CONSTRAINT")
            .to_string();
        assert!(message.contains("foreign-key name collision"), "{message}");
        assert!(message.contains(&name), "{message}");
        assert!(message.contains(child_table), "{message}");
        let mut conn = pool.get_conn().await.unwrap();
        let present: Option<i64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.tables
                  WHERE table_schema=DATABASE() AND table_name=?",
                (fresh_table,),
            )
            .await
            .unwrap();
        assert_eq!(
            present,
            Some(0),
            "FK mismatch must preflight before earlier DDL"
        );
        drop(conn);
        for table in [child_table, target, wrong] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: format!("DROP TABLE {table}"),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_rejects_composite_fk_that_matches_only_first_key() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let pool = mysql_async::Pool::new(url.as_str());
        let executor = MysqlMutationExecutor::new(pool.clone());
        let parent_table = "teaql_mysql_fk_composite_parent";
        let child_table = "teaql_mysql_fk_composite_child";
        for table in [child_table, parent_table] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: format!("DROP TABLE IF EXISTS {table}"),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        for sql in [
            format!(
                "CREATE TABLE {parent_table}(
                   id BIGINT PRIMARY KEY, scope_id BIGINT NOT NULL,
                   UNIQUE KEY uq_parent_scope(id, scope_id))"
            ),
            format!(
                "CREATE TABLE {child_table}(
                   id BIGINT PRIMARY KEY, parent_id BIGINT NOT NULL, scope_id BIGINT NOT NULL,
                   CONSTRAINT teaql_mysql_fk_composite_old FOREIGN KEY(parent_id, scope_id)
                   REFERENCES {parent_table}(id, scope_id))"
            ),
        ] {
            executor
                .execute_sql(&CompiledQuery {
                    sql,
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        let parent = EntityDescriptor::new("MysqlFkCompositeParent")
            .table_name(parent_table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null());
        let child = EntityDescriptor::new("MysqlFkCompositeChild")
            .table_name(child_table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("parent", "MysqlFkCompositeParent")
                    .local_key("parent_id")
                    .foreign_key("id"),
            );
        let message = executor
            .ensure_schema(&MysqlDialect, &[&child, &parent])
            .await
            .expect_err("a composite FK must not stand in for a single-column FK")
            .to_string();
        assert!(message.contains("installed_key_count=2"), "{message}");
        executor
            .execute_sql(&CompiledQuery {
                sql: format!(
                    "ALTER TABLE {child_table} DROP FOREIGN KEY teaql_mysql_fk_composite_old"
                ),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .ensure_schema(&MysqlDialect, &[&child, &parent])
            .await
            .expect("a single-column FK must install after explicit migration");
        executor
            .ensure_schema(&MysqlDialect, &[&child, &parent])
            .await
            .expect("the migrated single-column FK must remain idempotent");
        for table in [child_table, parent_table] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: format!("DROP TABLE {table}"),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_keeps_cross_datasource_and_external_relations_logical() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let pool = mysql_async::Pool::new(url.as_str());
        let executor = MysqlMutationExecutor::new(pool.clone());
        for table in [
            "teaql_mysql_fk_logical_child_fixture",
            "teaql_mysql_fk_logical_parent_fixture",
        ] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: format!("DROP TABLE IF EXISTS {}", mysql_quote_ident(table)),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        let parent = EntityDescriptor::new("MysqlLogicalParentFixture")
            .table_name("teaql_mysql_fk_logical_parent_fixture")
            .data_service("customer_db")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null());
        let child = EntityDescriptor::new("MysqlLogicalChildFixture")
            .table_name("teaql_mysql_fk_logical_child_fixture")
            .data_service("order_db")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64))
            .property(PropertyDescriptor::new("external_id", DataType::U64))
            .relation(
                RelationDescriptor::new("parent", "MysqlLogicalParentFixture")
                    .local_key("parent_id")
                    .foreign_key("id"),
            )
            .relation(
                RelationDescriptor::new("external", "MysqlNotInstalledFixture")
                    .local_key("external_id")
                    .foreign_key("id"),
            );
        executor
            .ensure_schema(&MysqlDialect, &[&child, &parent])
            .await
            .unwrap();
        let rows = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT COUNT(*) AS constraint_count
                        FROM information_schema.KEY_COLUMN_USAGE
                       WHERE CONSTRAINT_SCHEMA=DATABASE()
                         AND TABLE_NAME='teaql_mysql_fk_logical_child_fixture'
                         AND REFERENCED_TABLE_NAME IS NOT NULL"
                    .to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        assert_eq!(rows[0].get("constraint_count"), Some(&Value::I64(0)));
        for table in [
            "teaql_mysql_fk_logical_child_fixture",
            "teaql_mysql_fk_logical_parent_fixture",
        ] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: format!("DROP TABLE {}", mysql_quote_ident(table)),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn first_schema_install_creates_declared_indexes() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let executor = MysqlMutationExecutor::new(mysql_async::Pool::new(url.as_str()));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE IF EXISTS teaql_first_index_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let entity = schema_entity("FirstIndexFixture", "teaql_first_index_fixture");
        executor
            .ensure_schema(&MysqlDialect, &[&entity])
            .await
            .unwrap();
        let rows = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT COUNT(DISTINCT index_name) AS index_count FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'teaql_first_index_fixture' AND index_name = 'PK_TEAQL_FIRST_INDEX_FIXTURE_ID_VERSION'".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        assert_eq!(rows[0].get("index_count"), Some(&Value::I64(1)));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_first_index_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_rejects_wrong_shape_behind_declared_index_name() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let pool = mysql_async::Pool::new(url.as_str());
        let executor = MysqlMutationExecutor::new(pool.clone());
        let table = "teaql_index_shape_fixture";
        let fresh_table = "teaql_index_shape_preflight_fresh";
        executor
            .execute_sql(&CompiledQuery {
                sql: format!("DROP TABLE IF EXISTS {fresh_table}"),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: format!("DROP TABLE IF EXISTS {table}"),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: format!(
                    "CREATE TABLE {table} (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, version BIGINT NOT NULL, name VARCHAR(255))"
                ),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: format!(
                    "CREATE INDEX PK_TEAQL_INDEX_SHAPE_FIXTURE_ID_VERSION ON {table} (version, id)"
                ),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let entity = schema_entity("IndexShapeFixture", table);
        let fresh = schema_entity("IndexShapePreflightFresh", fresh_table);
        let error = executor
            .ensure_schema(&MysqlDialect, &[&fresh, &entity])
            .await
            .expect_err("a colliding index with reversed keys must be rejected");
        let message = error.to_string();
        assert!(message.contains(table), "{message}");
        assert!(
            message.contains("PK_TEAQL_INDEX_SHAPE_FIXTURE_ID_VERSION"),
            "{message}"
        );
        let mut conn = pool.get_conn().await.unwrap();
        let present: Option<i64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.tables
                  WHERE table_schema=DATABASE() AND table_name=?",
                (fresh_table,),
            )
            .await
            .unwrap();
        assert_eq!(present, Some(0), "index mismatch must preflight before DDL");
        drop(conn);
        for definition in [
            format!(
                "CREATE UNIQUE INDEX PK_TEAQL_INDEX_SHAPE_FIXTURE_ID_VERSION ON {table} (id DESC, version)"
            ),
            format!(
                "CREATE UNIQUE INDEX PK_TEAQL_INDEX_SHAPE_FIXTURE_ID_VERSION ON {table} (id, version) INVISIBLE"
            ),
            format!(
                "CREATE UNIQUE INDEX PK_TEAQL_INDEX_SHAPE_FIXTURE_ID_VERSION ON {table} (id, ((version + 1)))"
            ),
        ] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: format!("DROP INDEX PK_TEAQL_INDEX_SHAPE_FIXTURE_ID_VERSION ON {table}"),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
            executor
                .execute_sql(&CompiledQuery {
                    sql: definition.clone(),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
            let error = executor
                .ensure_schema(&MysqlDialect, &[&entity])
                .await
                .expect_err("a malformed installed index must be rejected");
            assert!(
                error
                    .to_string()
                    .contains("PK_TEAQL_INDEX_SHAPE_FIXTURE_ID_VERSION"),
                "definition={definition}, error={error}"
            );
        }
        executor
            .execute_sql(&CompiledQuery {
                sql: format!("DROP TABLE {table}"),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_uses_bounded_stable_declared_index_names() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let pool = mysql_async::Pool::new(url.as_str());
        let executor = MysqlMutationExecutor::new(pool.clone());
        let table = "teaql_long_declared_index_fixture_aaaaaaaaaaaaaaaaaaaaa";
        executor
            .execute_sql(&CompiledQuery {
                sql: format!("DROP TABLE IF EXISTS {table}"),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let entity = schema_entity("LongDeclaredIndexFixture", table);
        executor
            .ensure_schema(&MysqlDialect, &[&entity])
            .await
            .expect("long declared index names must fit MySQL's limit");
        executor
            .ensure_schema(&MysqlDialect, &[&entity])
            .await
            .expect("the bounded declared index must remain stable");
        executor
            .execute_sql(&CompiledQuery {
                sql: format!("DROP TABLE {table}"),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_rejects_overlong_model_identifiers_before_connecting() {
        let pool = mysql_async::Pool::new("mysql://root@127.0.0.1:1/teaql_no_database");
        let executor = MysqlMutationExecutor::new(pool);
        let long_table = EntityDescriptor::new("School")
            .table_name("a".repeat(65))
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null());
        let error = executor
            .ensure_schema(&MysqlDialect, &[&long_table])
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MutationExecutorError::SqlCompile(SqlCompileError::SchemaIdentifierTooLong {
                actual_bytes: 65,
                max_bytes: 64,
                ..
            })
        ));

        let long_column = EntityDescriptor::new("School")
            .table_name("school_data")
            .property(
                PropertyDescriptor::new("contactPhone", DataType::Text).column_name("b".repeat(65)),
            );
        let message = executor
            .ensure_schema(&MysqlDialect, &[&long_column])
            .await
            .unwrap_err()
            .to_string();
        assert!(message.contains("column for entity School property contactPhone"));
        assert!(message.contains("at most 64 bytes"));
    }

    #[tokio::test]
    async fn mysql_id_space_overflow_reports_safe_actionable_context() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let pool = mysql_async::Pool::new(url.as_str());
        let executor = MysqlMutationExecutor::new(pool.clone());
        let table = format!(
            "teaql_id_space_diagnostic_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        executor
            .execute_sql(&CompiledQuery {
                sql: format!(
                    "CREATE TABLE {} (type_name VARCHAR(100) PRIMARY KEY, current_level BIGINT UNSIGNED NOT NULL)",
                    mysql_quote_ident(&table)
                ),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        // The portable binder deliberately rejects values above i64::MAX.
        // Seed the MySQL-specific unsigned boundary directly so this test can
        // exercise the generator's own overflow diagnostic.
        let mut connection = pool.get_conn().await.unwrap();
        connection
            .query_drop(format!(
                "INSERT INTO {}(type_name, current_level) VALUES ('order', {})",
                mysql_quote_ident(&table),
                u64::MAX
            ))
            .await
            .unwrap();
        drop(connection);
        let generator = MysqlIdSpaceGenerator::new(pool).with_table_name(&table);

        let message = generator.next_id("Order").await.unwrap_err().to_string();
        assert!(message.contains("MySQL ID provider"), "{message}");
        assert!(message.contains("ID space order"), "{message}");
        assert!(message.contains(&table), "{message}");
        assert!(message.contains("attempt 1"), "{message}");
        assert!(!message.contains("mysql://"), "{message}");
        executor
            .execute_sql(&CompiledQuery {
                sql: format!("DROP TABLE {}", mysql_quote_ident(&table)),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn temporal_debug_sql_matches_real_mysql_when_configured() {
        let Ok(url) = std::env::var("TEAQL_TEST_MYSQL_URL") else {
            return;
        };
        let executor = MysqlMutationExecutor::new(mysql_async::Pool::new(url.as_str()));
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE IF EXISTS teaql_temporal_runtime_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql:
                    "CREATE TABLE teaql_temporal_runtime_fixture(id INTEGER, d DATE, t DATETIME(3))"
                        .to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let prepared = CompiledQuery {
            sql: "INSERT INTO teaql_temporal_runtime_fixture VALUES (?, ?, ?)".to_owned(),
            params: vec![
                Value::I64(1),
                Value::Date("2024-02-29".parse().unwrap()),
                Value::Timestamp(teaql_core::time::Timestamp(-315_521_754_322)),
            ],
            comment: Some("teaql source=temporal.verify ?".to_owned()),
        };
        executor.execute_sql(&prepared).await.unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: prepared
                    .debug_sql(DatabaseKind::MySql)
                    .replace("VALUES (1,", "VALUES (2,"),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let rows = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT d, t FROM teaql_temporal_runtime_fixture ORDER BY id".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        assert_eq!(rows[0], rows[1]);
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_temporal_runtime_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MysqlSchemaProvider;

impl SchemaProvider for MysqlSchemaProvider {
    fn ensure_schema<'a>(
        &'a self,
        context: &'a UserContext,
        _invocation: &'a teaql_runtime::SchemaInvocation,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            ensure_mysql_schema_for(context)
                .await
                .map_err(|err| RuntimeError::Schema(err.to_string()))
        })
    }
}

/// Installs the MySQL provider; schema changes require the context-owned
/// invocation path rather than direct executor DDL calls.
///
/// ```compile_fail
/// let _ = teaql_provider_mysql::MysqlMutationExecutor::ensure_schema;
/// ```
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
        let entity = canonical_id_space_entity(entity);
        let entity = entity.as_str();
        self.ensure_table().await?;
        let table = mysql_quote_ident(&self.table_name);
        let mut conn = self.pool.get_conn().await?;
        let select_sql = format!("SELECT current_level FROM {table} WHERE type_name = ?");
        let insert_sql = format!("INSERT INTO {table}(type_name, current_level) VALUES (?, 1)");
        let update_sql = format!(
            "UPDATE {table} SET current_level = ? WHERE type_name = ? AND current_level = ?"
        );
        for attempt in 1..=100 {
            let current: Option<u64> = conn.exec_first(&select_sql, (entity,)).await?;
            if let Some(current) = current {
                let next = current.checked_add(1).ok_or_else(|| {
                    MutationExecutorError::Bind(format!(
                        "MySQL ID provider overflow for ID space {entity} in table {} on optimistic-lock attempt {attempt}",
                        self.table_name
                    ))
                })?;
                conn.exec_drop(&update_sql, (next, entity, current)).await?;
                if conn.affected_rows() == 1 {
                    return Ok(next);
                }
            } else {
                match conn.exec_drop(&insert_sql, (entity,)).await {
                    Ok(()) if conn.affected_rows() == 1 => return Ok(1),
                    Ok(()) => {
                        return Err(MutationExecutorError::Bind(format!(
                            "MySQL ID provider insert for ID space {entity} in table {} changed {} rows on optimistic-lock attempt {attempt}",
                            self.table_name,
                            conn.affected_rows()
                        )));
                    }
                    Err(error) => {
                        let winner: Option<u64> = conn.exec_first(&select_sql, (entity,)).await?;
                        if winner.is_none() {
                            return Err(error.into());
                        }
                    }
                }
            }
        }
        Err(MutationExecutorError::Bind(format!(
            "MySQL ID provider was unable to allocate ID space {entity} in table {} after 100 optimistic-lock attempts",
            self.table_name
        )))
    }

    pub async fn ensure_floor(
        &self,
        entity: &str,
        floor: u64,
    ) -> Result<(), MutationExecutorError> {
        let entity = canonical_id_space_entity(entity);
        let entity = entity.as_str();
        self.ensure_table().await?;
        if floor > i64::MAX as u64 {
            return Err(MutationExecutorError::Bind(format!(
                "MySQL ID provider floor {floor} for ID space {entity} in table {} exceeds BIGINT",
                self.table_name
            )));
        }
        let table = mysql_quote_ident(&self.table_name);
        let mut conn = self.pool.get_conn().await?;
        let select = format!("SELECT current_level FROM {table} WHERE type_name = ?");
        let insert = format!("INSERT INTO {table}(type_name, current_level) VALUES (?, ?)");
        let update = format!(
            "UPDATE {table} SET current_level = ? WHERE type_name = ? AND current_level = ?"
        );
        for _ in 1..=100 {
            let current: Option<u64> = conn.exec_first(&select, (entity,)).await?;
            match current {
                Some(current) if current >= floor => return Ok(()),
                Some(current) => {
                    conn.exec_drop(&update, (floor, entity, current)).await?;
                    if conn.affected_rows() == 1 {
                        return Ok(());
                    }
                }
                None => match conn.exec_drop(&insert, (entity, floor)).await {
                    Ok(()) if conn.affected_rows() == 1 => return Ok(()),
                    Ok(()) => {}
                    Err(error) => {
                        let winner: Option<u64> = conn.exec_first(&select, (entity,)).await?;
                        if winner.is_none() {
                            return Err(error.into());
                        }
                    }
                },
            }
        }
        Err(MutationExecutorError::Bind(format!(
            "MySQL ID provider was unable to synchronize floor for ID space {entity} in table {} after 100 optimistic-lock attempts",
            self.table_name
        )))
    }
}

impl InternalIdGenerator for MysqlIdSpaceGenerator {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        let generator = self.clone();
        let entity = entity.to_owned();
        block_on_id_generation(async move { generator.next_id(&entity).await })
    }

    fn ensure_floor(&self, entity: &str, floor: u64) -> Result<(), RuntimeError> {
        let generator = self.clone();
        let entity = entity.to_owned();
        block_on_id_generation(async move { generator.ensure_floor(&entity, floor).await })
    }
}

fn block_on_id_generation<T, F>(future: F) -> Result<T, RuntimeError>
where
    T: Send + 'static,
    F: Future<Output = Result<T, MutationExecutorError>> + Send + 'static,
{
    let result = match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| RuntimeError::IdGeneration(err.to_string()))?
            .block_on(future),
    };
    result.map_err(|err| RuntimeError::IdGeneration(err.to_string()))
}

fn mysql_quote_ident(ident: &str) -> String {
    quote_identifier_if_needed(ident, '`')
}

async fn ensure_mysql_declared_index_shapes(
    conn: &mut mysql_async::Conn,
    entity: &EntityDescriptor,
    allow_missing: bool,
) -> Result<(), MutationExecutorError> {
    let table = strip_identifier_quotes(&entity.table_name);
    for spec in schema_index_specs(entity, Some(64)) {
        let name = spec.name.as_str();
        let unique = spec.unique;
        let expected_columns = &spec.columns;
        type IndexRow = (
            i64,
            u64,
            Option<String>,
            Option<String>,
            Option<u64>,
            String,
            Option<String>,
            String,
        );
        let installed: Vec<IndexRow> = conn
            .exec(
                "SELECT NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, COLLATION, SUB_PART, INDEX_TYPE, EXPRESSION, IS_VISIBLE
                   FROM information_schema.statistics
                  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?
                  ORDER BY SEQ_IN_INDEX",
                (table, name),
            )
            .await?;
        if allow_missing && installed.is_empty() {
            continue;
        }
        let correct = installed.len() == expected_columns.len()
            && installed.iter().zip(expected_columns).enumerate().all(
                |(position, (actual, expected))| {
                    actual.0 == i64::from(!unique)
                        && actual.1 == (position + 1) as u64
                        && actual.2.as_deref().is_some_and(|column| {
                            column.eq_ignore_ascii_case(strip_identifier_quotes(expected))
                        })
                        && actual.3.as_deref() == Some("A")
                        && actual.4.is_none()
                        && actual.5.eq_ignore_ascii_case("BTREE")
                        && actual.6.is_none()
                        && actual.7.eq_ignore_ascii_case("YES")
                },
            );
        if !correct {
            return Err(MutationExecutorError::Bind(format!(
                "MySQL declared index shape mismatch: table={table}, index={name}; expected={} full visible BTREE on ({}) with ascending keys; installed={installed:?}; drop/rename the colliding index or migrate the table explicitly",
                if unique { "unique" } else { "non-unique" },
                expected_columns.join(", ")
            )));
        }
    }
    Ok(())
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
        Value::Bool(v) => Ok(mysql_async::Value::Int(i64::from(*v))),
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
            v.to_datetime().naive_utc().to_string().into_bytes(),
        )),
        Value::Object(_) => Err(MutationExecutorError::UnsupportedValue("object")),
        Value::List(_) => Err(MutationExecutorError::UnsupportedValue("list")),
        Value::TypedNull(_) => Ok(mysql_async::Value::NULL),
    }
}

fn decode_mysql_compact_rows(
    rows: Vec<mysql_async::Row>,
) -> Result<Vec<CompactRow>, MutationExecutorError> {
    let Some(first) = rows.first() else {
        return Ok(Vec::new());
    };
    let columns: Arc<[String]> = first
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect::<Vec<_>>()
        .into();
    rows.into_iter()
        .map(|row| Ok(CompactRow::new(columns.clone(), decode_mysql_values(row)?)))
        .collect()
}

fn decode_mysql_values(row: mysql_async::Row) -> Result<Vec<Value>, MutationExecutorError> {
    let mut values = Vec::with_capacity(row.len());
    for index in 0..row.len() {
        let column = row.columns_ref()[index].clone();
        let val_opt = row
            .get_opt::<mysql_async::Value, _>(index)
            .ok_or_else(|| MutationExecutorError::Bind(format!("missing col {index}")))?
            .map_err(|e| MutationExecutorError::Bind(e.to_string()))?;

        if val_opt == mysql_async::Value::NULL {
            values.push(Value::Null);
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
                    s.parse::<i64>()
                        .map(Value::I64)
                        .or_else(|_| s.parse::<u64>().map(Value::U64))
                        .unwrap_or(Value::I64(0))
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
                Value::Timestamp(teaql_core::time::Timestamp(
                    Utc.from_utc_datetime(&dt).timestamp_millis(),
                ))
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
        values.push(value);
    }
    Ok(values)
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

    fn mysql_metadata(data_type: &str) -> MysqlColumnMetadata {
        MysqlColumnMetadata {
            data_type: data_type.to_owned(),
            nullable: true,
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
        }
    }

    #[test]
    fn schema_compatibility_covers_boolean_and_temporal_storage_contracts() {
        let entity = EntityDescriptor::new("HighRiskFixture").table_name("high_risk_fixture");
        let bool_property = PropertyDescriptor::new("enabled", DataType::Bool);
        let date_property = PropertyDescriptor::new("business_date", DataType::Date);
        let timestamp_property = PropertyDescriptor::new("occurred_at", DataType::Timestamp);

        assert!(
            ensure_mysql_column_compatibility(&entity, &bool_property, &mysql_metadata("tinyint"))
                .is_ok()
        );
        assert!(
            ensure_mysql_column_compatibility(&entity, &bool_property, &mysql_metadata("varchar"))
                .is_err()
        );
        assert!(
            ensure_mysql_column_compatibility(&entity, &date_property, &mysql_metadata("date"))
                .is_ok()
        );
        assert!(
            ensure_mysql_column_compatibility(&entity, &date_property, &mysql_metadata("datetime"))
                .is_err()
        );
        assert!(
            ensure_mysql_column_compatibility(
                &entity,
                &timestamp_property,
                &mysql_metadata("datetime")
            )
            .is_ok()
        );
        assert!(
            ensure_mysql_column_compatibility(
                &entity,
                &timestamp_property,
                &mysql_metadata("timestamp")
            )
            .is_ok()
        );
        assert!(
            ensure_mysql_column_compatibility(
                &entity,
                &timestamp_property,
                &mysql_metadata("date")
            )
            .is_err()
        );
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
            "CREATE TABLE IF NOT EXISTS orders (id BIGINT PRIMARY KEY NOT NULL, version BIGINT NOT NULL, name VARCHAR(255))"
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

        let shaped_text = MysqlDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("short_code", DataType::Text).max_length(32),
            )
            .unwrap();
        assert_eq!(
            shaped_text,
            "ALTER TABLE orders ADD COLUMN short_code VARCHAR(32)"
        );

        let shaped_decimal = MysqlDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("precise_amount", DataType::Decimal)
                    .numeric_precision(19)
                    .numeric_scale(7),
            )
            .unwrap();
        assert_eq!(
            shaped_decimal,
            "ALTER TABLE orders ADD COLUMN precise_amount DECIMAL(19, 7)"
        );
    }
}
