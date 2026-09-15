use std::future::Future;
use std::pin::Pin;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use deadpool_postgres::{GenericClient, Pool};
use rust_decimal::Decimal;
use std::collections::HashSet;
use std::sync::Arc;
use teaql_core::{
    BinaryOp, DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, SelectQuery,
    UpdateCommand, Value,
};
use teaql_runtime::{
    GraphNode, InternalIdGenerator, RuntimeError, SchemaProvider, UserContext,
    canonical_id_space_entity,
};
use teaql_sql::{
    CompiledQuery, DatabaseKind, SchemaIndexSpec, SqlCompileError, SqlDialect, SqlTransport,
    bounded_sql_identifier, quote_identifier_if_needed, schema_foreign_key_name,
    schema_index_specs, validate_schema_identifier_lengths,
};

pub const DEFAULT_ID_SPACE_TABLE: &str = "teaql_id_space";

#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresDialect;

#[derive(Debug)]
struct PostgresRelationIndexSpec {
    name: String,
    table: String,
    foreign_key: String,
    id: String,
}

impl PostgresDialect {
    /// Indexes supporting the common "recent children for each parent" access
    /// pattern.  This deliberately uses a full index: PostgreSQL cannot use a
    /// partial `WHERE version > 0` index for a generic prepared plan whose
    /// version predicate is parameterized.
    fn relation_index_specs(&self, entity: &EntityDescriptor) -> Vec<PostgresRelationIndexSpec> {
        let Some(id_property) = entity.id_property() else {
            return Vec::new();
        };
        let mut indexed_columns = HashSet::new();
        let mut specs = Vec::new();

        for relation in &entity.relations {
            // A to-one relation whose local key is not the entity ID represents
            // a foreign-key property on this table.  `(foreign_key, id DESC)`
            // serves both equality lookup and top-N/recent-child queries.
            if relation.many || relation.local_key == id_property.name {
                continue;
            }
            let Some(foreign_key_property) = entity.property_by_name(&relation.local_key) else {
                continue;
            };
            if !indexed_columns.insert(foreign_key_property.column_name.as_str()) {
                continue;
            }

            let index_name = postgres_index_name(
                &entity.table_name,
                &foreign_key_property.column_name,
                &id_property.column_name,
            );
            specs.push(PostgresRelationIndexSpec {
                name: index_name,
                table: entity.table_name.clone(),
                foreign_key: foreign_key_property.column_name.clone(),
                id: id_property.column_name.clone(),
            });
        }
        specs
    }

    #[cfg(test)]
    fn relation_indexes_sqls(&self, entity: &EntityDescriptor) -> Vec<String> {
        self.relation_index_specs(entity)
            .into_iter()
            .map(|spec| self.relation_index_sql(&spec))
            .collect()
    }

    fn relation_index_sql(&self, spec: &PostgresRelationIndexSpec) -> String {
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} ({}, {} DESC)",
            self.quote_ident(&spec.name),
            self.quote_ident(&spec.table),
            self.quote_ident(&spec.foreign_key),
            self.quote_ident(&spec.id),
        )
    }
}

fn postgres_index_name(table: &str, foreign_key: &str, id: &str) -> String {
    let full = format!("IDX_{table}_{foreign_key}_{id}_DESC").to_uppercase();
    bounded_sql_identifier(&full, 63)
}

impl SqlDialect for PostgresDialect {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::PostgreSql
    }

    fn large_in_uses_array_param(&self) -> bool {
        true
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

    fn schema_indexes_sqls(
        &self,
        entity: &EntityDescriptor,
    ) -> Result<Vec<String>, SqlCompileError> {
        Ok(schema_index_specs(entity, Some(63))
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
                        "CREATE UNIQUE INDEX IF NOT EXISTS"
                    } else {
                        "CREATE INDEX IF NOT EXISTS"
                    },
                    self.quote_ident(&spec.name),
                    self.quote_ident(&spec.table)
                )
            })
            .collect())
    }

    fn schema_type_sql(
        &self,
        data_type: DataType,
        property: &PropertyDescriptor,
    ) -> Result<String, SqlCompileError> {
        match data_type {
            DataType::Bool => Ok("BOOLEAN".to_owned()),
            DataType::I64 | DataType::U64 => Ok("BIGINT".to_owned()),
            DataType::F64 => Ok("DOUBLE PRECISION".to_owned()),
            DataType::Decimal => match (property.numeric_precision, property.numeric_scale) {
                (Some(precision), Some(scale)) => Ok(format!("NUMERIC({precision},{scale})")),
                _ => Ok("NUMERIC".to_owned()),
            },
            DataType::Text => Ok(format!("VARCHAR({})", property.max_length.unwrap_or(255))),
            DataType::LargeText => Ok("TEXT".to_owned()),
            DataType::Json => Ok("JSONB".to_owned()),
            DataType::Date => Ok("DATE".to_owned()),
            DataType::Timestamp => Ok("TIMESTAMPTZ".to_owned()),
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
    Driver(tokio_postgres::Error),
    Pool(String),
    SqlCompile(SqlCompileError),
    UnsupportedValue(&'static str),
    UnsupportedColumnType(String),
    Bind(String),
}

impl std::fmt::Display for MutationExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Driver(err) => {
                if let Some(db_error) = err.as_db_error() {
                    write!(
                        f,
                        "postgres SQLSTATE {}: {}",
                        db_error.code().code(),
                        db_error.message()
                    )?;
                    if let Some(constraint) = db_error.constraint() {
                        write!(f, "; constraint={constraint}")?;
                    }
                    Ok(())
                } else {
                    err.fmt(f)
                }
            }
            Self::Pool(err) => write!(f, "postgres pool error: {err}"),
            Self::SqlCompile(err) => err.fmt(f),
            Self::UnsupportedValue(kind) => {
                write!(f, "unsupported bind value for mutation executor: {kind}")
            }
            Self::UnsupportedColumnType(kind) => {
                write!(f, "unsupported column type for record decoding: {kind}")
            }
            Self::Bind(message) => write!(f, "bind error: {message}"),
        }
    }
}

impl std::error::Error for MutationExecutorError {}

impl From<tokio_postgres::Error> for MutationExecutorError {
    fn from(value: tokio_postgres::Error) -> Self {
        Self::Driver(value)
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

#[derive(Debug, Clone)]
struct PostgresColumnMetadata {
    data_type: String,
    nullable: bool,
    max_length: Option<u32>,
    numeric_precision: Option<u32>,
    numeric_scale: Option<u32>,
}

/// A transaction owns one checked-out pooled connection for its complete
/// lifetime. Using SQL BEGIN/COMMIT avoids a self-referential wrapper around
/// `tokio_postgres::Transaction` while preserving connection affinity.
pub struct PgTransactionExecutor {
    client: deadpool_postgres::Object,
}

impl SqlTransport for PgMutationExecutor {
    type Error = MutationExecutorError;

    async fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<teaql_core::CompactRow>, Self::Error> {
        let mut args = PgArgs { values: Vec::new() };
        for value in &query.params {
            bind_pg(&mut args, value)?;
        }
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let statement = client.prepare_cached(&query.sql).await?;
        let rows = client.query(&statement, &args.as_refs()).await?;
        let columns: std::sync::Arc<[String]> = statement
            .columns()
            .iter()
            .map(|column| column.name().to_owned())
            .collect::<Vec<_>>()
            .into();
        rows.iter()
            .map(|row| {
                Ok(teaql_core::CompactRow::new(
                    columns.clone(),
                    decode_pg_values(row)?,
                ))
            })
            .collect()
    }

    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        self.execute(query).await
    }
}

impl teaql_sql::StreamingSqlTransport for PgMutationExecutor {
    fn stream_sql(
        &self,
        query: CompiledQuery,
        chunk_size: usize,
    ) -> teaql_data_service::QueryStream<'_, Self::Error> {
        let pool = self.pool.clone();
        Box::pin(async_stream::try_stream! {
            use futures_util::TryStreamExt;
            let mut args = PgArgs { values: Vec::new() }; for value in &query.params { bind_pg(&mut args, value)?; }
            let client = pool.get().await.map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
            let params = args.as_refs();
            let statement = client.prepare_cached(&query.sql).await?;
            let columns: std::sync::Arc<[String]> = statement.columns().iter().map(|column| column.name().to_owned()).collect::<Vec<_>>().into();
            let rows = client.query_raw(&statement, params).await?;
            futures_util::pin_mut!(rows);
            let mut chunk = Vec::with_capacity(chunk_size); let mut index = 0;
            while let Some(row) = rows.try_next().await? { chunk.push(teaql_core::CompactRow::new(columns.clone(), decode_pg_values(&row)?)); if chunk.len()==chunk_size { yield teaql_data_service::StreamChunk { rows: std::mem::take(&mut chunk), chunk_index:index, is_last:false }; index+=1; } }
            if !chunk.is_empty() { yield teaql_data_service::StreamChunk { rows:chunk, chunk_index:index, is_last:true }; }
        })
    }
}

impl SqlTransport for PgTransactionExecutor {
    type Error = MutationExecutorError;

    async fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<teaql_core::CompactRow>, Self::Error> {
        let mut args = PgArgs { values: Vec::new() };
        for value in &query.params {
            bind_pg(&mut args, value)?;
        }
        let statement = self.client.prepare_cached(&query.sql).await?;
        let rows = self.client.query(&statement, &args.as_refs()).await?;
        let columns: Arc<[String]> = statement
            .columns()
            .iter()
            .map(|column| column.name().to_owned())
            .collect::<Vec<_>>()
            .into();
        rows.iter()
            .map(|row| {
                Ok(teaql_core::CompactRow::new(
                    columns.clone(),
                    decode_pg_values(row)?,
                ))
            })
            .collect()
    }

    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        let mut args = PgArgs { values: Vec::new() };
        for value in &query.params {
            bind_pg(&mut args, value)?;
        }
        let statement = self.client.prepare_cached(&query.sql).await?;
        Ok(self.client.execute(&statement, &args.as_refs()).await?)
    }
}

impl teaql_sql::SqlTransaction for PgTransactionExecutor {
    type Error = MutationExecutorError;

    async fn commit_sql(self) -> Result<(), Self::Error> {
        self.client.batch_execute("COMMIT").await?;
        Ok(())
    }

    async fn rollback_sql(self) -> Result<(), Self::Error> {
        self.client.batch_execute("ROLLBACK").await?;
        Ok(())
    }
}

impl teaql_sql::SqlTransactionTransport for PgMutationExecutor {
    type Tx<'a>
        = PgTransactionExecutor
    where
        Self: 'a;

    async fn begin_sql(&self) -> Result<Self::Tx<'_>, Self::Error> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| MutationExecutorError::Pool(error.to_string()))?;
        client.batch_execute("BEGIN").await?;
        Ok(PgTransactionExecutor { client })
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
        validate_schema_identifier_lengths(entities, 63)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let transaction = client.transaction().await?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended('teaql-schema-setup', 0))",
                &[],
            )
            .await?;
        let result = self
            .ensure_schema_with_client(&transaction, dialect, entities)
            .await;
        match result {
            Ok(()) => transaction.commit().await?,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        }
        Ok(())
    }

    async fn ensure_schema_with_client<C>(
        &self,
        client: &C,
        dialect: &PostgresDialect,
        entities: &[&EntityDescriptor],
    ) -> Result<(), MutationExecutorError>
    where
        C: GenericClient + Sync,
    {
        for sql in dialect.schema_setup_sqls() {
            client.execute(*sql, &[]).await?;
        }
        let id_space_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (type_name VARCHAR(100) PRIMARY KEY, current_level BIGINT NOT NULL)",
            quote_ident(DEFAULT_ID_SPACE_TABLE)
        );
        client.execute(&id_space_sql, &[]).await?;

        for entity in entities {
            if !Self::table_exists(client, &entity.table_name).await? {
                let sql = dialect.compile_create_table(entity)?;
                client.execute(&sql, &[]).await?;
            } else {
                let existing_columns = Self::table_columns(client, &entity.table_name).await?;
                for property in &entity.properties {
                    let bare_column = strip_identifier_quotes(&property.column_name).to_lowercase();
                    if let Some(actual) = existing_columns.get(&bare_column) {
                        ensure_postgres_column_compatibility(entity, property, actual)?;
                        continue;
                    }
                    Self::ensure_required_column_can_be_added(client, entity, property).await?;
                    let sql = dialect.compile_add_column(entity, property)?;
                    client.execute(&sql, &[]).await?;
                }
            }

            for sql in dialect.schema_indexes_sqls(entity)? {
                client.execute(&sql, &[]).await?;
            }
            for spec in schema_index_specs(entity, Some(63)) {
                Self::ensure_declared_index_shape(client, &spec).await?;
            }
            for spec in dialect.relation_index_specs(entity) {
                client
                    .execute(&dialect.relation_index_sql(&spec), &[])
                    .await?;
                Self::ensure_relation_index_shape(client, &spec).await?;
            }
        }

        // Install constraints only after every table and column exists, so
        // descriptor registration order does not affect schema creation.
        for entity in entities {
            for relation in &entity.relations {
                let Some(target) = entities
                    .iter()
                    .copied()
                    .find(|candidate| candidate.name == relation.target_entity)
                else {
                    // A module may intentionally reference an entity supplied by
                    // another module or service. That remains a logical relation.
                    continue;
                };
                if entity.data_service != target.data_service {
                    // Cross-data-source relations cannot be represented by a
                    // database-local foreign-key constraint.
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
                Self::ensure_foreign_key(
                    client,
                    &source.table_name,
                    &source_property.column_name,
                    &referenced.table_name,
                    &referenced_property.column_name,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn ensure_declared_index_shape<C>(
        client: &C,
        spec: &SchemaIndexSpec,
    ) -> Result<(), MutationExecutorError>
    where
        C: GenericClient + Sync,
    {
        let table = postgres_stored_ident(&spec.table);
        let index = postgres_stored_ident(&spec.name);
        let rows = client
            .query(
                "SELECT i.indisvalid, i.indisready, i.indisunique, am.amname,
                        i.indnkeyatts::int, i.indnatts::int,
                        i.indpred IS NULL, i.indexprs IS NULL,
                        first_column.attname, second_column.attname,
                        COALESCE((i.indoption[0]::int & 1) <> 0, false),
                        COALESCE((i.indoption[1]::int & 1) <> 0, false),
                        COALESCE((i.indoption[0]::int & 2) <> 0, false),
                        COALESCE((i.indoption[1]::int & 2) <> 0, false),
                        first_opclass.opcdefault, second_opclass.opcdefault,
                        i.indcollation[0] = first_column.attcollation,
                        i.indcollation[1] = second_column.attcollation
                 FROM pg_index i
                 JOIN pg_class table_class ON table_class.oid = i.indrelid
                 JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
                 JOIN pg_class index_class ON index_class.oid = i.indexrelid
                 JOIN pg_am am ON am.oid = index_class.relam
                 LEFT JOIN pg_attribute first_column
                   ON first_column.attrelid = table_class.oid
                  AND first_column.attnum = i.indkey[0]
                 LEFT JOIN pg_attribute second_column
                   ON second_column.attrelid = table_class.oid
                  AND second_column.attnum = i.indkey[1]
                 LEFT JOIN pg_opclass first_opclass ON first_opclass.oid = i.indclass[0]
                 LEFT JOIN pg_opclass second_opclass ON second_opclass.oid = i.indclass[1]
                 WHERE namespace.nspname = current_schema()
                   AND table_class.relname = $1 AND index_class.relname = $2",
                &[&table, &index],
            )
            .await?;
        let Some(row) = rows.first() else {
            return Err(MutationExecutorError::Bind(format!(
                "PostgreSQL declared index missing after installation: table={}, index={}",
                spec.table, spec.name
            )));
        };
        let valid: bool = row.try_get(0)?;
        let ready: bool = row.try_get(1)?;
        let unique: bool = row.try_get(2)?;
        let method: String = row.try_get(3)?;
        let key_count: i32 = row.try_get(4)?;
        let total_columns: i32 = row.try_get(5)?;
        let unfiltered: bool = row.try_get(6)?;
        let direct: bool = row.try_get(7)?;
        let first: Option<String> = row.try_get(8)?;
        let second: Option<String> = row.try_get(9)?;
        let first_desc: bool = row.try_get(10)?;
        let second_desc: bool = row.try_get(11)?;
        let first_nulls_first: bool = row.try_get(12)?;
        let second_nulls_first: bool = row.try_get(13)?;
        let first_default_opclass: Option<bool> = row.try_get(14)?;
        let second_default_opclass: Option<bool> = row.try_get(15)?;
        let first_default_collation: Option<bool> = row.try_get(16)?;
        let second_default_collation: Option<bool> = row.try_get(17)?;
        let installed = [first.as_deref(), second.as_deref()];
        let expected = spec
            .columns
            .iter()
            .map(|column| postgres_stored_ident(column))
            .collect::<Vec<_>>();
        let column_match = expected
            .iter()
            .enumerate()
            .all(|(position, column)| installed[position] == Some(column.as_str()));
        let flags_match = !first_desc
            && !first_nulls_first
            && first_default_opclass == Some(true)
            && first_default_collation == Some(true)
            && (expected.len() == 1
                || (!second_desc
                    && !second_nulls_first
                    && second_default_opclass == Some(true)
                    && second_default_collation == Some(true)));
        if valid
            && ready
            && unique == spec.unique
            && method == "btree"
            && key_count == expected.len() as i32
            && total_columns == key_count
            && unfiltered
            && direct
            && column_match
            && flags_match
        {
            return Ok(());
        }
        Err(MutationExecutorError::Bind(format!(
            "PostgreSQL declared index has incompatible shape: table={}, index={}, expected={} full/valid btree({}) with ascending default keys, actual=unique={unique}, method={method}, keys={key_count}/{total_columns}, columns={first:?}/{second:?}, desc={first_desc}/{second_desc}, nulls_first={first_nulls_first}/{second_nulls_first}, default_opclass={first_default_opclass:?}/{second_default_opclass:?}, default_collation={first_default_collation:?}/{second_default_collation:?}, full={unfiltered}, direct={direct}, valid={valid}, ready={ready}; drop or rename the conflicting index before retrying",
            spec.table,
            spec.name,
            if spec.unique { "unique" } else { "non-unique" },
            spec.columns.join(", ")
        )))
    }

    async fn ensure_relation_index_shape<C>(
        client: &C,
        spec: &PostgresRelationIndexSpec,
    ) -> Result<(), MutationExecutorError>
    where
        C: GenericClient + Sync,
    {
        let table = postgres_stored_ident(&spec.table);
        let index = postgres_stored_ident(&spec.name);
        let rows = client
            .query(
                "SELECT i.indisvalid, i.indisready, am.amname, i.indnkeyatts::int,
                        i.indpred IS NULL, i.indexprs IS NULL,
                        first_column.attname, second_column.attname,
                        (i.indoption[0]::int & 1) <> 0,
                        (i.indoption[1]::int & 1) <> 0,
                        i.indisunique, i.indnatts::int,
                        (i.indoption[0]::int & 2) <> 0,
                        (i.indoption[1]::int & 2) <> 0,
                        first_opclass.opcdefault, second_opclass.opcdefault,
                        i.indcollation[0] = first_column.attcollation,
                        i.indcollation[1] = second_column.attcollation
                 FROM pg_index i
                 JOIN pg_class table_class ON table_class.oid = i.indrelid
                 JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
                 JOIN pg_class index_class ON index_class.oid = i.indexrelid
                 JOIN pg_am am ON am.oid = index_class.relam
                 LEFT JOIN pg_attribute first_column
                   ON first_column.attrelid = table_class.oid
                  AND first_column.attnum = i.indkey[0]
                 LEFT JOIN pg_attribute second_column
                   ON second_column.attrelid = table_class.oid
                  AND second_column.attnum = i.indkey[1]
                 LEFT JOIN pg_opclass first_opclass ON first_opclass.oid = i.indclass[0]
                 LEFT JOIN pg_opclass second_opclass ON second_opclass.oid = i.indclass[1]
                 WHERE namespace.nspname = current_schema()
                   AND table_class.relname = $1 AND index_class.relname = $2",
                &[&table, &index],
            )
            .await?;
        let Some(row) = rows.first() else {
            return Err(MutationExecutorError::Bind(format!(
                "ensure schema relation index missing after installation: table={}, index={}",
                spec.table, spec.name
            )));
        };
        let valid: bool = row.try_get(0)?;
        let ready: bool = row.try_get(1)?;
        let method: String = row.try_get(2)?;
        let key_count: i32 = row.try_get(3)?;
        let unfiltered: bool = row.try_get(4)?;
        let direct_columns: bool = row.try_get(5)?;
        let first: Option<String> = row.try_get(6)?;
        let second: Option<String> = row.try_get(7)?;
        let first_desc: bool = row.try_get(8)?;
        let second_desc: bool = row.try_get(9)?;
        let unique: bool = row.try_get(10)?;
        let total_columns: i32 = row.try_get(11)?;
        let first_nulls_first: bool = row.try_get(12)?;
        let second_nulls_first: bool = row.try_get(13)?;
        let first_default_opclass: Option<bool> = row.try_get(14)?;
        let second_default_opclass: Option<bool> = row.try_get(15)?;
        let first_default_collation: Option<bool> = row.try_get(16)?;
        let second_default_collation: Option<bool> = row.try_get(17)?;
        let expected_first = postgres_stored_ident(&spec.foreign_key);
        let expected_second = postgres_stored_ident(&spec.id);

        if valid
            && ready
            && method == "btree"
            && key_count == 2
            && total_columns == key_count
            && !unique
            && unfiltered
            && direct_columns
            && first.as_deref() == Some(expected_first.as_str())
            && second.as_deref() == Some(expected_second.as_str())
            && !first_desc
            && second_desc
            && !first_nulls_first
            && second_nulls_first
            && first_default_opclass == Some(true)
            && second_default_opclass == Some(true)
            && first_default_collation == Some(true)
            && second_default_collation == Some(true)
        {
            return Ok(());
        }
        Err(MutationExecutorError::Bind(format!(
            "ensure schema relation index has incompatible shape: table={}, index={}, expected=non-unique btree({}, {} DESC) with two full/valid default keys, actual=method={method}, unique={unique}, keys={key_count}/{total_columns}, columns={first:?}/{second:?}, desc={first_desc}/{second_desc}, nulls_first={first_nulls_first}/{second_nulls_first}, default_opclass={first_default_opclass:?}/{second_default_opclass:?}, default_collation={first_default_collation:?}/{second_default_collation:?}, full={unfiltered}, direct={direct_columns}, valid={valid}, ready={ready}; drop or rename the conflicting index before retrying",
            spec.table, spec.name, spec.foreign_key, spec.id
        )))
    }

    async fn ensure_foreign_key<C>(
        client: &C,
        source_table: &str,
        source_column: &str,
        referenced_table: &str,
        referenced_column: &str,
    ) -> Result<(), MutationExecutorError>
    where
        C: GenericClient + Sync,
    {
        let candidates = client
            .query(
                "SELECT CAST(c.conname AS TEXT), c.convalidated, c.condeferrable,
                        c.condeferred, CAST(c.confupdtype AS TEXT),
                        CAST(c.confdeltype AS TEXT), CAST(c.confmatchtype AS TEXT),
                        pg_get_constraintdef(c.oid)
                   FROM pg_constraint c
                   JOIN pg_class st ON st.oid = c.conrelid
                   JOIN pg_namespace sn ON sn.oid = st.relnamespace
                   JOIN pg_class rt ON rt.oid = c.confrelid
                   JOIN pg_namespace rn ON rn.oid = rt.relnamespace
                   JOIN pg_attribute sc ON sc.attrelid = c.conrelid AND sc.attnum = c.conkey[1]
                   JOIN pg_attribute rc ON rc.attrelid = c.confrelid AND rc.attnum = c.confkey[1]
                  WHERE c.contype = 'f'
                    AND sn.nspname = current_schema()
                    AND rn.nspname = current_schema()
                    AND st.relname = $1 AND sc.attname = $2
                    AND rt.relname = $3 AND rc.attname = $4
                    AND cardinality(c.conkey) = 1 AND cardinality(c.confkey) = 1",
                &[
                    &postgres_stored_ident(source_table),
                    &postgres_stored_ident(source_column),
                    &postgres_stored_ident(referenced_table),
                    &postgres_stored_ident(referenced_column),
                ],
            )
            .await?;
        let mut incompatible = None;
        for row in candidates {
            let name: String = row.try_get(0)?;
            let validated: bool = row.try_get(1)?;
            let deferrable: bool = row.try_get(2)?;
            let initially_deferred: bool = row.try_get(3)?;
            let update_action: String = row.try_get(4)?;
            let delete_action: String = row.try_get(5)?;
            let match_type: String = row.try_get(6)?;
            let definition: String = row.try_get(7)?;
            if validated
                && !deferrable
                && !initially_deferred
                && update_action == "a"
                && delete_action == "a"
                && match_type == "s"
            {
                return Ok(());
            }
            incompatible.get_or_insert_with(|| format!(
                "PostgreSQL ensure schema incompatible existing foreign key: table={source_table}, column={source_column}, referenced_table={referenced_table}, referenced_column={referenced_column}, expected=validated non-deferrable MATCH SIMPLE with NO ACTION, installed_constraint={name}, installed_validated={validated}, installed_deferrable={deferrable}, installed_initially_deferred={initially_deferred}, installed_update={update_action}, installed_delete={delete_action}, installed_match={match_type}, installed_definition={definition}; validate or migrate the constraint explicitly before retrying"
            ));
        }
        if let Some(message) = incompatible {
            return Err(MutationExecutorError::Bind(message));
        }
        {
            let constraint_name = schema_foreign_key_name(
                source_table,
                source_column,
                referenced_table,
                referenced_column,
                63,
            );
            let collision = client
                .query_opt(
                    "SELECT CAST(c.contype AS TEXT), pg_get_constraintdef(c.oid)
                       FROM pg_constraint c
                       JOIN pg_class t ON t.oid = c.conrelid
                       JOIN pg_namespace n ON n.oid = t.relnamespace
                      WHERE n.nspname = current_schema()
                        AND t.relname = $1 AND c.conname = $2
                      LIMIT 1",
                    &[
                        &postgres_stored_ident(source_table),
                        &postgres_stored_ident(&constraint_name),
                    ],
                )
                .await?;
            if let Some(row) = collision {
                let kind: String = row.try_get(0)?;
                let definition: String = row.try_get(1)?;
                return Err(MutationExecutorError::Bind(format!(
                    "PostgreSQL ensure schema foreign-key name collision: table={source_table}, constraint={constraint_name}, expected=FOREIGN KEY ({source_column}) REFERENCES {referenced_table}({referenced_column}) with NO ACTION, installed_kind={kind}, installed_definition={definition}; rename/drop the colliding constraint or migrate explicitly before retrying"
                )));
            }
            let sql = format!(
                "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
                quote_ident(source_table),
                quote_ident(&constraint_name),
                quote_ident(source_column),
                quote_ident(referenced_table),
                quote_ident(referenced_column),
            );
            client.execute(&sql, &[]).await?;
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        client.execute(&sql, &[]).await?;
        Ok(())
    }

    pub async fn execute(&self, query: &CompiledQuery) -> Result<u64, MutationExecutorError> {
        let mut args = PgArgs { values: Vec::new() };
        for value in &query.params {
            bind_pg(&mut args, value)?;
        }
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let statement = client.prepare_cached(&query.sql).await?;
        let result = client.execute(&statement, &args.as_refs()).await?;
        Ok(result)
    }

    async fn table_exists<C>(client: &C, table_name: &str) -> Result<bool, MutationExecutorError>
    where
        C: GenericClient + Sync,
    {
        let row = client
            .query_one(
                "SELECT COUNT(1)
             FROM information_schema.tables
             WHERE table_schema = current_schema()
               AND table_name = $1",
                &[&table_name],
            )
            .await?;
        let exists: i64 = row.try_get(0)?;
        Ok(exists > 0)
    }

    async fn table_columns<C>(
        client: &C,
        table_name: &str,
    ) -> Result<std::collections::BTreeMap<String, PostgresColumnMetadata>, MutationExecutorError>
    where
        C: GenericClient + Sync,
    {
        let rows = client
            .query(
                "SELECT column_name, data_type, is_nullable,
                        character_maximum_length, numeric_precision, numeric_scale
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = $1",
                &[&table_name],
            )
            .await?;
        let mut columns = std::collections::BTreeMap::new();
        for row in rows {
            let name: String = row.try_get("column_name")?;
            let data_type: String = row.try_get("data_type")?;
            let is_nullable: String = row.try_get("is_nullable")?;
            let max_length: Option<i32> = row.try_get("character_maximum_length")?;
            let numeric_precision: Option<i32> = row.try_get("numeric_precision")?;
            let numeric_scale: Option<i32> = row.try_get("numeric_scale")?;
            columns.insert(
                name.to_lowercase(),
                PostgresColumnMetadata {
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

    async fn ensure_required_column_can_be_added<C>(
        client: &C,
        entity: &EntityDescriptor,
        property: &PropertyDescriptor,
    ) -> Result<(), MutationExecutorError>
    where
        C: GenericClient + Sync,
    {
        if property.nullable {
            return Ok(());
        }
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM {} LIMIT 1)",
            quote_ident(&entity.table_name)
        );
        let has_rows: bool = client.query_one(&sql, &[]).await?.try_get(0)?;
        if !has_rows {
            return Ok(());
        }
        Err(MutationExecutorError::Bind(format!(
            "ensure schema cannot add required column without a deterministic backfill: entity={}, table={}, column={}; the table contains rows, so migrate/backfill explicitly before retrying",
            entity.name, entity.table_name, property.column_name
        )))
    }
}

fn ensure_postgres_column_compatibility(
    entity: &EntityDescriptor,
    property: &PropertyDescriptor,
    metadata: &PostgresColumnMetadata,
) -> Result<(), MutationExecutorError> {
    let actual = metadata.data_type.trim().to_ascii_lowercase();
    let compatible = match property.data_type {
        DataType::Bool => actual == "boolean",
        DataType::I64 | DataType::U64 => actual == "bigint",
        DataType::F64 => matches!(actual.as_str(), "double precision" | "real"),
        DataType::Decimal => matches!(actual.as_str(), "numeric" | "decimal"),
        DataType::Text => matches!(actual.as_str(), "character varying" | "text"),
        DataType::LargeText => matches!(actual.as_str(), "text" | "character varying"),
        DataType::Json => matches!(actual.as_str(), "jsonb" | "json"),
        DataType::Date => actual == "date",
        DataType::Timestamp => actual == "timestamp with time zone",
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

async fn ensure_initial_graphs_postgres(
    executor: &PgMutationExecutor,
    dialect: &PostgresDialect,
    context: &UserContext,
) -> Result<(), MutationExecutorError> {
    for graph in context.initial_graphs() {
        let entity = context.entity(&graph.entity).ok_or_else(|| {
            MutationExecutorError::Bind(format!("missing entity: {}", graph.entity))
        })?;
        if initial_graph_exists_postgres(executor, dialect, entity, graph).await? {
            if let Some(query) = compile_initial_graph_update(dialect, entity, graph)? {
                executor.execute(&query).await?;
            }
            continue;
        }
        let query = compile_initial_graph_insert(dialect, entity, graph)?;
        executor.execute(&query).await?;
    }
    for graph in context.root_graphs() {
        let entity = context.entity(&graph.entity).ok_or_else(|| {
            MutationExecutorError::Bind(format!("missing entity: {}", graph.entity))
        })?;
        if initial_graph_exists_postgres(executor, dialect, entity, graph).await? {
            continue;
        }
        let query = compile_initial_graph_insert(dialect, entity, graph)?;
        executor.execute(&query).await?;
    }
    let generator = PgIdSpaceGenerator::from_executor(executor.clone());
    for graph in context.initial_graphs().iter().chain(context.root_graphs()) {
        if let Some(id) = graph.values.get("id").and_then(Value::try_u64) {
            generator.ensure_floor(&graph.entity, id).await?;
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
    Ok(!executor.fetch_all_compact_sql(&query).await?.is_empty())
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

pub(crate) async fn ensure_postgres_schema_for(
    context: &UserContext,
) -> Result<(), MutationExecutorError> {
    let dialect = context.get_resource::<PostgresDialect>().ok_or_else(|| {
        MutationExecutorError::Bind("missing typed resource: PostgresDialect".to_owned())
    })?;
    let executor = context
        .get_resource::<PgMutationExecutor>()
        .ok_or_else(|| {
            MutationExecutorError::Bind("missing typed resource: PgMutationExecutor".to_owned())
        })?;

    let entities = context.all_entities();

    executor.ensure_schema(dialect, &entities).await?;
    ensure_initial_graphs_postgres(executor, dialect, context).await
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use futures_util::StreamExt;
    use teaql_core::RelationDescriptor;
    use teaql_sql::{SqlTransaction, SqlTransactionTransport, SqlTransport, StreamingSqlTransport};

    // Live ensure_schema tests share the provider's teaql_id_space table.
    // Serialize only those schema-mutating fixtures, not the whole test suite.
    static LIVE_SCHEMA_FIXTURE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn configured_pool(url: String) -> Pool {
        let mut config = deadpool_postgres::Config::new();
        config.url = Some(url);
        config
            .create_pool(
                Some(deadpool_postgres::Runtime::Tokio1),
                tokio_postgres::NoTls,
            )
            .unwrap()
    }

    #[tokio::test]
    async fn streams_from_real_postgres_when_configured() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let executor = PgMutationExecutor::new(pool);
        let query = CompiledQuery {
            sql: "SELECT id FROM (VALUES (1), (2), (3), (4), (5)) AS fixture(id) ORDER BY id"
                .to_owned(),
            params: vec![],
            comment: None,
        };
        let mut stream = executor.stream_sql(query, 2);
        let mut sizes = Vec::new();
        while let Some(chunk) = stream.next().await {
            sizes.push(chunk.unwrap().rows.len());
        }
        assert_eq!(sizes, vec![2, 2, 1]);
    }

    #[tokio::test]
    async fn transaction_commit_and_rollback_use_one_connection_when_configured() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let executor = PgMutationExecutor::new(configured_pool(url));
        for sql in [
            "DROP TABLE IF EXISTS teaql_transaction_runtime_fixture",
            "CREATE TABLE teaql_transaction_runtime_fixture(id BIGINT PRIMARY KEY)",
        ] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: sql.to_owned(),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }

        let rolled_back = executor.begin_sql().await.unwrap();
        rolled_back
            .execute_sql(&CompiledQuery {
                sql: "INSERT INTO teaql_transaction_runtime_fixture(id) VALUES ($1)".to_owned(),
                params: vec![Value::I64(1)],
                comment: None,
            })
            .await
            .unwrap();
        rolled_back.rollback_sql().await.unwrap();

        let committed = executor.begin_sql().await.unwrap();
        committed
            .execute_sql(&CompiledQuery {
                sql: "INSERT INTO teaql_transaction_runtime_fixture(id) VALUES ($1)".to_owned(),
                params: vec![Value::I64(2)],
                comment: None,
            })
            .await
            .unwrap();
        committed.commit_sql().await.unwrap();

        let rows = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT id FROM teaql_transaction_runtime_fixture ORDER BY id".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::I64(2)));
    }

    #[tokio::test]
    async fn boolean_roundtrips_real_postgres_when_configured() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let mut config = deadpool_postgres::Config::new();
        config.url = Some(url);
        let pool = config
            .create_pool(
                Some(deadpool_postgres::Runtime::Tokio1),
                tokio_postgres::NoTls,
            )
            .unwrap();
        let executor = PgMutationExecutor::new(pool);
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
                    sql: "INSERT INTO teaql_boolean_runtime_fixture VALUES ($1, $2, $3)".to_owned(),
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
    async fn generic_null_binds_across_postgres_column_types_when_configured() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let executor = PgMutationExecutor::new(configured_pool(url));
        for sql in [
            "DROP TABLE IF EXISTS teaql_generic_null_runtime_fixture",
            "CREATE TABLE teaql_generic_null_runtime_fixture(id BIGINT PRIMARY KEY, paid_at TIMESTAMPTZ, established_date DATE, note TEXT, amount NUMERIC, active BOOLEAN)",
        ] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: sql.to_owned(),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        executor.execute_sql(&CompiledQuery {
            sql: "INSERT INTO teaql_generic_null_runtime_fixture(id, paid_at, established_date, note, amount, active) VALUES ($1, $2, $3, $4, $5, $6)".to_owned(),
            params: vec![Value::I64(1), Value::Null, Value::Null, Value::Null, Value::Null, Value::Null],
            comment: None,
        }).await.unwrap();
        let rows = executor.fetch_all_compact_sql(&CompiledQuery {
            sql: "SELECT paid_at, established_date, note, amount, active FROM teaql_generic_null_runtime_fixture WHERE id = $1".to_owned(),
            params: vec![Value::I64(1)],
            comment: None,
        }).await.unwrap();
        assert_eq!(rows.len(), 1);
        for column in ["paid_at", "established_date", "note", "amount", "active"] {
            assert_eq!(rows[0].get(column), Some(&Value::Null), "column {column}");
        }
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_generic_null_runtime_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn teaql_long_binds_to_legacy_postgres_int4_scalars_and_arrays() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let executor = PgMutationExecutor::new(pool);
        for sql in [
            "DROP TABLE IF EXISTS teaql_int4_binding_fixture",
            "CREATE TABLE teaql_int4_binding_fixture(id INTEGER PRIMARY KEY)",
        ] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: sql.to_owned(),
                    params: vec![],
                    comment: None,
                })
                .await
                .unwrap();
        }
        for id in [1_i64, i64::from(i32::MAX)] {
            executor
                .execute_sql(&CompiledQuery {
                    sql: "INSERT INTO teaql_int4_binding_fixture(id) VALUES ($1)".to_owned(),
                    params: vec![Value::I64(id)],
                    comment: None,
                })
                .await
                .unwrap();
        }
        let rows = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT id FROM teaql_int4_binding_fixture WHERE id = ANY($1) ORDER BY id"
                    .to_owned(),
                params: vec![Value::List(vec![
                    Value::U64(1),
                    Value::U64(i32::MAX as u64),
                ])],
                comment: None,
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&Value::I64(1)));

        let overflow = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT id FROM teaql_int4_binding_fixture WHERE id = $1".to_owned(),
                params: vec![Value::I64(i64::from(i32::MAX) + 1)],
                comment: None,
            })
            .await;
        assert!(overflow.is_err());
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE teaql_int4_binding_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn topn_012_ensure_schema_creates_relation_index_idempotently() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let _schema_guard = LIVE_SCHEMA_FIXTURE_LOCK.lock().await;
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        client
            .batch_execute("DROP TABLE IF EXISTS teaql_relation_index_fixture")
            .await
            .unwrap();

        let entity = EntityDescriptor::new("RelationIndexFixture")
            .table_name("teaql_relation_index_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("vendor_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("vendor", "Vendor")
                    .local_key("vendor_id")
                    .foreign_key("id"),
            );
        let executor = PgMutationExecutor::new(pool.clone());

        executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .unwrap();
        executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .unwrap();

        let rows = client
            .query(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'teaql_relation_index_fixture' AND indexdef LIKE '%(vendor_id, id DESC)%'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);

        client
            .batch_execute("DROP TABLE teaql_relation_index_fixture")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_rejects_wrong_shape_behind_declared_index_name() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        let table = "teaql_declared_index_fixture";
        let index = "PK_TEAQL_DECLARED_INDEX_FIXTURE_ID_VERSION";
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table} (id BIGINT NOT NULL, version BIGINT NOT NULL, name TEXT); \
                 CREATE INDEX {index} ON {table} (version, id)"
            ))
            .await
            .unwrap();
        let entity = EntityDescriptor::new("DeclaredIndexFixture")
            .table_name(table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text));
        let executor = PgMutationExecutor::new(pool.clone());
        let error = executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .expect_err("colliding declared index must fail");
        let message = error.to_string();
        assert!(message.contains(table), "{message}");
        assert!(message.contains(index), "{message}");
        for definition in [
            format!("CREATE UNIQUE INDEX {index} ON {table} (id, version) WHERE version > 0"),
            format!("CREATE UNIQUE INDEX {index} ON {table} (id DESC, version)"),
            format!("CREATE UNIQUE INDEX {index} ON {table} (id, version) INCLUDE (name)"),
            format!("CREATE UNIQUE INDEX {index} ON {table} (id, (version + 1))"),
            format!("CREATE UNIQUE INDEX {index} ON {table} (id NULLS FIRST, version)"),
        ] {
            client
                .batch_execute(&format!("DROP INDEX {index}; {definition}"))
                .await
                .unwrap();
            let error = executor
                .ensure_schema(&PostgresDialect, &[&entity])
                .await
                .expect_err("a wrong-shape declared index must be rejected");
            assert!(
                error.to_string().contains(index),
                "definition={definition}, error={error}"
            );
        }
        client
            .batch_execute(&format!(
                "DROP INDEX {index}; CREATE UNIQUE INDEX {index} ON {table} (id, version)"
            ))
            .await
            .unwrap();
        executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .expect("a matching declared index must be accepted");
        executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .expect("a matching declared index must remain idempotent");
        let other_table = "teaql_declared_index_wrong_table_fixture";
        client
            .batch_execute(&format!(
                "DROP INDEX {index}; CREATE TABLE {other_table} (id BIGINT); \
                 CREATE INDEX {index} ON {other_table} (id)"
            ))
            .await
            .unwrap();
        let error = executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .expect_err("an index name on the wrong table cannot satisfy schema");
        let message = error.to_string();
        assert!(message.contains(table), "{message}");
        assert!(message.contains(index), "{message}");
        client
            .batch_execute(&format!("DROP TABLE {other_table}; DROP TABLE {table}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_uses_bounded_stable_declared_index_names() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        let table = "teaql_long_declared_index_fixture_aaaaaaaaaaaaaaaaaaaaa";
        client
            .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
            .await
            .unwrap();
        let entity = EntityDescriptor::new("LongDeclaredIndexFixture")
            .table_name(table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            );
        let executor = PgMutationExecutor::new(pool.clone());
        executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .expect("long declared index names must install without truncation drift");
        executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .expect("the bounded declared index must remain stable");
        client
            .batch_execute(&format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_rejects_overlong_model_identifiers_before_connecting() {
        let pool = configured_pool("postgres://127.0.0.1:1/teaql_no_database".to_owned());
        let executor = PgMutationExecutor::new(pool);
        let long_table = EntityDescriptor::new("School")
            .table_name("a".repeat(64))
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null());
        let error = executor
            .ensure_schema(&PostgresDialect, &[&long_table])
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MutationExecutorError::SqlCompile(SqlCompileError::SchemaIdentifierTooLong {
                actual_bytes: 64,
                max_bytes: 63,
                ..
            })
        ));

        let long_column = EntityDescriptor::new("School")
            .table_name("school_data")
            .property(
                PropertyDescriptor::new("contactPhone", DataType::Text).column_name("b".repeat(64)),
            );
        let message = executor
            .ensure_schema(&PostgresDialect, &[&long_column])
            .await
            .unwrap_err()
            .to_string();
        assert!(message.contains("column for entity School property contactPhone"));
        assert!(message.contains("at most 63 bytes"));
    }

    #[tokio::test]
    async fn ensure_schema_rejects_wrong_shape_behind_relation_index_name() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        client
            .batch_execute("DROP TABLE IF EXISTS teaql_relation_index_shape_fixture")
            .await
            .unwrap();

        let entity = EntityDescriptor::new("RelationIndexShapeFixture")
            .table_name("teaql_relation_index_shape_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("vendor_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("vendor", "Vendor")
                    .local_key("vendor_id")
                    .foreign_key("id"),
            );
        let executor = PgMutationExecutor::new(pool.clone());
        executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .unwrap();

        let relation_index =
            postgres_index_name("teaql_relation_index_shape_fixture", "vendor_id", "id");
        client
            .batch_execute(&format!(
                "DROP INDEX {relation_index}; CREATE INDEX {relation_index} ON teaql_relation_index_shape_fixture (id, vendor_id)"
            ))
            .await
            .unwrap();

        let result = executor.ensure_schema(&PostgresDialect, &[&entity]).await;
        assert!(
            result.is_err(),
            "a colliding index with reversed columns must fail"
        );
        let error = result.unwrap_err().to_string();
        assert!(error.contains("teaql_relation_index_shape_fixture"));
        assert!(error.contains(&relation_index));

        client
            .batch_execute(&format!(
                "DROP INDEX {relation_index}; CREATE INDEX {relation_index} ON teaql_relation_index_shape_fixture (vendor_id, id DESC) WHERE version > 0"
            ))
            .await
            .unwrap();
        let result = executor.ensure_schema(&PostgresDialect, &[&entity]).await;
        assert!(
            result.is_err(),
            "a partial colliding index must not qualify"
        );
        assert!(result.unwrap_err().to_string().contains("full=false"));

        client
            .batch_execute(&format!(
                "DROP INDEX {relation_index}; CREATE INDEX {relation_index} ON teaql_relation_index_shape_fixture (vendor_id, id)"
            ))
            .await
            .unwrap();
        let result = executor.ensure_schema(&PostgresDialect, &[&entity]).await;
        assert!(result.is_err(), "an ascending ID index must not qualify");
        assert!(result.unwrap_err().to_string().contains("desc=false/false"));

        client
            .batch_execute(&format!(
                "DROP INDEX {relation_index}; CREATE UNIQUE INDEX {relation_index} ON teaql_relation_index_shape_fixture (vendor_id, id DESC)"
            ))
            .await
            .unwrap();
        let result = executor.ensure_schema(&PostgresDialect, &[&entity]).await;
        assert!(
            result.is_err(),
            "a unique colliding index must not qualify as the owned non-unique relation index"
        );

        client
            .batch_execute(&format!(
                "DROP INDEX {relation_index}; CREATE INDEX {relation_index} ON teaql_relation_index_shape_fixture (vendor_id, id DESC) INCLUDE (version)"
            ))
            .await
            .unwrap();
        let result = executor.ensure_schema(&PostgresDialect, &[&entity]).await;
        assert!(
            result.is_err(),
            "an INCLUDE column must not qualify as the exact owned relation index"
        );

        client
            .batch_execute(&format!(
                "DROP INDEX {relation_index}; CREATE INDEX {relation_index} ON teaql_relation_index_shape_fixture (vendor_id, id DESC NULLS LAST)"
            ))
            .await
            .unwrap();
        let result = executor.ensure_schema(&PostgresDialect, &[&entity]).await;
        assert!(
            result.is_err(),
            "non-default ID null ordering must not qualify as the owned relation index"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("nulls_first=false/false")
        );

        client
            .batch_execute(&format!(
                "DROP INDEX {relation_index}; CREATE INDEX {relation_index} ON teaql_relation_index_shape_fixture (vendor_id, id DESC)"
            ))
            .await
            .unwrap();
        executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .unwrap();
        executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .unwrap();

        client
            .batch_execute("DROP TABLE teaql_relation_index_shape_fixture")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_rejects_incompatible_existing_column_type() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS teaql_schema_type_fixture;
                 CREATE TABLE teaql_schema_type_fixture (
                   id BIGINT PRIMARY KEY,
                   version BIGINT NOT NULL,
                   name BIGINT
                 )",
            )
            .await
            .unwrap();
        let entity = EntityDescriptor::new("SchemaTypeFixture")
            .table_name("teaql_schema_type_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text));
        let executor = PgMutationExecutor::new(pool.clone());

        let error = executor
            .ensure_schema(&PostgresDialect, &[&entity])
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

        client
            .batch_execute("DROP TABLE teaql_schema_type_fixture")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_accepts_covering_shapes_and_rejects_narrower_postgres_columns() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS teaql_schema_shape_fixture;
                 CREATE TABLE teaql_schema_shape_fixture (
                   id BIGINT PRIMARY KEY,
                   version BIGINT NOT NULL,
                   name VARCHAR(255),
                   amount NUMERIC(38,10)
                 )",
            )
            .await
            .unwrap();
        let base = || {
            EntityDescriptor::new("SchemaShapeFixture")
                .table_name("teaql_schema_shape_fixture")
                .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
                .property(
                    PropertyDescriptor::new("version", DataType::I64)
                        .version()
                        .not_null(),
                )
        };
        let executor = PgMutationExecutor::new(pool.clone());
        let length_model =
            base().property(PropertyDescriptor::new("name", DataType::Text).max_length(100));
        executor
            .ensure_schema(&PostgresDialect, &[&length_model])
            .await
            .expect("wider VARCHAR storage must cover the model");

        let numeric_model = base().property(
            PropertyDescriptor::new("amount", DataType::Decimal)
                .numeric_precision(19)
                .numeric_scale(7),
        );
        executor
            .ensure_schema(&PostgresDialect, &[&numeric_model])
            .await
            .expect("wider NUMERIC storage must cover the model");

        client
            .batch_execute(
                "DROP TABLE teaql_schema_shape_fixture;
                 CREATE TABLE teaql_schema_shape_fixture (
                   id BIGINT PRIMARY KEY,
                   version BIGINT NOT NULL,
                   name VARCHAR(32),
                   amount NUMERIC(18,2)
                 )",
            )
            .await
            .unwrap();
        let message = executor
            .ensure_schema(&PostgresDialect, &[&length_model])
            .await
            .expect_err("narrower VARCHAR storage must fail")
            .to_string();
        assert!(message.contains("required_max_length=100"), "{message}");
        assert!(message.contains("actual_max_length=Some(32)"), "{message}");

        let message = executor
            .ensure_schema(&PostgresDialect, &[&numeric_model])
            .await
            .expect_err("narrower numeric storage must fail")
            .to_string();
        assert!(message.contains("required_precision=19"), "{message}");
        assert!(message.contains("required_scale=7"), "{message}");
        assert!(message.contains("actual_precision=Some(18)"), "{message}");
        assert!(message.contains("actual_scale=Some(2)"), "{message}");
        client
            .batch_execute("DROP TABLE teaql_schema_shape_fixture")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_rejects_incompatible_existing_column_nullability() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS teaql_schema_nullability_fixture;
                 CREATE TABLE teaql_schema_nullability_fixture (
                   id BIGINT PRIMARY KEY,
                   version BIGINT NOT NULL,
                   name VARCHAR(255)
                 )",
            )
            .await
            .unwrap();
        let entity = EntityDescriptor::new("SchemaNullabilityFixture")
            .table_name("teaql_schema_nullability_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text).not_null());
        let executor = PgMutationExecutor::new(pool.clone());

        let message = executor
            .ensure_schema(&PostgresDialect, &[&entity])
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
        client
            .batch_execute("DROP TABLE teaql_schema_nullability_fixture")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn required_column_evolution_has_no_postgres_magic_default() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS teaql_required_evolution_fixture;
                 CREATE TABLE teaql_required_evolution_fixture (
                   id BIGINT PRIMARY KEY,
                   version BIGINT NOT NULL,
                   name VARCHAR(255)
                 );
                 INSERT INTO teaql_required_evolution_fixture(id, version, name)
                 VALUES (1, 1, 'existing');",
            )
            .await
            .unwrap();
        let entity = EntityDescriptor::new("RequiredEvolutionFixture")
            .table_name("teaql_required_evolution_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(
                PropertyDescriptor::new("version", DataType::I64)
                    .version()
                    .not_null(),
            )
            .property(PropertyDescriptor::new("name", DataType::Text))
            .property(PropertyDescriptor::new("code", DataType::Text).not_null());
        let executor = PgMutationExecutor::new(pool.clone());

        let message = executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .expect_err("required column without a backfill must fail before ALTER TABLE")
            .to_string();
        assert!(
            message.contains("entity=RequiredEvolutionFixture"),
            "{message}"
        );
        assert!(message.contains("column=code"), "{message}");
        assert!(message.contains("migrate/backfill explicitly"), "{message}");
        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM information_schema.columns
                  WHERE table_schema=current_schema()
                    AND table_name='teaql_required_evolution_fixture'
                    AND column_name='code'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 0);
        client
            .batch_execute("DROP TABLE teaql_required_evolution_fixture")
            .await
            .unwrap();
        client
            .batch_execute(
                "CREATE TABLE teaql_required_evolution_fixture (
                   id BIGINT PRIMARY KEY,
                   version BIGINT NOT NULL,
                   name VARCHAR(255)
                 )",
            )
            .await
            .unwrap();
        executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .unwrap();
        executor
            .ensure_schema(&PostgresDialect, &[&entity])
            .await
            .unwrap();
        let row = client
            .query_one(
                "SELECT is_nullable, column_default FROM information_schema.columns
                  WHERE table_schema=current_schema()
                    AND table_name='teaql_required_evolution_fixture'
                    AND column_name='code'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>("is_nullable"), "NO");
        assert_eq!(row.get::<_, Option<String>>("column_default"), None);
        client
            .batch_execute("DROP TABLE teaql_required_evolution_fixture")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn concurrent_schema_evolution_is_idempotent_when_configured() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS teaql_concurrent_schema_fixture;
                 CREATE TABLE teaql_concurrent_schema_fixture (
                   id BIGINT PRIMARY KEY,
                   version BIGINT NOT NULL
                 );",
            )
            .await
            .unwrap();
        let entity = Arc::new(
            EntityDescriptor::new("ConcurrentSchemaFixture")
                .table_name("teaql_concurrent_schema_fixture")
                .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
                .property(
                    PropertyDescriptor::new("version", DataType::I64)
                        .version()
                        .not_null(),
                )
                .property(PropertyDescriptor::new("code", DataType::Text)),
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let executor = PgMutationExecutor::new(pool.clone());
            let entity = Arc::clone(&entity);
            let barrier = Arc::clone(&barrier);
            tasks.spawn(async move {
                barrier.wait().await;
                executor
                    .ensure_schema(&PostgresDialect, &[entity.as_ref()])
                    .await
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM information_schema.columns
                  WHERE table_schema=current_schema()
                    AND table_name='teaql_concurrent_schema_fixture'
                    AND column_name='code'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 1);
        client
            .batch_execute("DROP TABLE teaql_concurrent_schema_fixture")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_creates_foreign_key_once_by_semantics() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let _schema_guard = LIVE_SCHEMA_FIXTURE_LOCK.lock().await;
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS teaql_fk_child_fixture;
                 DROP TABLE IF EXISTS teaql_fk_parent_fixture;",
            )
            .await
            .unwrap();

        let parent = EntityDescriptor::new("FkParentFixture")
            .table_name("teaql_fk_parent_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .relation(
                RelationDescriptor::new("children", "FkChildFixture")
                    .local_key("id")
                    .foreign_key("parent_id")
                    .many(),
            );
        let child = EntityDescriptor::new("FkChildFixture")
            .table_name("teaql_fk_child_fixture")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("parent", "FkParentFixture")
                    .local_key("parent_id")
                    .foreign_key("id"),
            );
        let executor = PgMutationExecutor::new(pool.clone());

        executor
            .ensure_schema(&PostgresDialect, &[&child, &parent])
            .await
            .unwrap();
        executor
            .ensure_schema(&PostgresDialect, &[&parent, &child])
            .await
            .unwrap();

        let count: i64 = client
            .query_one(
                "SELECT COUNT(*)
                   FROM pg_constraint c
                   JOIN pg_class t ON t.oid = c.conrelid
                  WHERE c.contype = 'f'
                    AND t.relname = 'teaql_fk_child_fixture'",
                &[],
            )
            .await
            .unwrap()
            .try_get(0)
            .unwrap();
        assert_eq!(count, 1);

        let violation = client
            .execute(
                "INSERT INTO teaql_fk_child_fixture(id, parent_id) VALUES (1, 999)",
                &[],
            )
            .await;
        assert!(violation.is_err());

        client
            .batch_execute(
                "DROP TABLE teaql_fk_child_fixture;
                 DROP TABLE teaql_fk_parent_fixture;",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_reports_colliding_foreign_key_name_before_add_constraint() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        let target = "teaql_fk_name_target_pg";
        let wrong = "teaql_fk_name_wrong_pg";
        let child_table = "teaql_fk_name_child_pg";
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {child_table}; DROP TABLE IF EXISTS {target}; DROP TABLE IF EXISTS {wrong};"
            ))
            .await
            .unwrap();
        let name = schema_foreign_key_name(child_table, "parent_id", target, "id", 63);
        client
            .batch_execute(&format!(
                "CREATE TABLE {target}(id BIGINT PRIMARY KEY);
                 CREATE TABLE {wrong}(id BIGINT PRIMARY KEY);
                 CREATE TABLE {child_table}(
                   id BIGINT PRIMARY KEY, parent_id BIGINT NOT NULL,
                   CONSTRAINT {} FOREIGN KEY(parent_id) REFERENCES {wrong}(id)
                 );",
                quote_ident(&name)
            ))
            .await
            .unwrap();
        let parent = EntityDescriptor::new("FkNameTargetPg")
            .table_name(target)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null());
        let child = EntityDescriptor::new("FkNameChildPg")
            .table_name(child_table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("parent", "FkNameTargetPg")
                    .local_key("parent_id")
                    .foreign_key("id"),
            );
        let executor = PgMutationExecutor::new(pool.clone());
        let error = executor
            .ensure_schema(&PostgresDialect, &[&child, &parent])
            .await
            .expect_err("a same-name wrong-target constraint must fail before ADD CONSTRAINT");
        let message = error.to_string();
        assert!(message.contains("foreign-key name collision"), "{error:?}");
        assert!(message.contains(&name), "{message}");
        assert!(message.contains(wrong), "{message}");
        client
            .batch_execute(&format!(
                "DROP TABLE {child_table}; DROP TABLE {target}; DROP TABLE {wrong};"
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_rejects_unvalidated_or_deferrable_existing_foreign_key() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        let parent_table = "teaql_fk_state_parent_pg";
        let child_table = "teaql_fk_state_child_pg";
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {child_table}; DROP TABLE IF EXISTS {parent_table};
                 CREATE TABLE {parent_table}(id BIGINT PRIMARY KEY);
                 CREATE TABLE {child_table}(id BIGINT PRIMARY KEY, parent_id BIGINT NOT NULL);"
            ))
            .await
            .unwrap();
        let parent = EntityDescriptor::new("FkStateParentPg")
            .table_name(parent_table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null());
        let child = EntityDescriptor::new("FkStateChildPg")
            .table_name(child_table)
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64).not_null())
            .relation(
                RelationDescriptor::new("parent", "FkStateParentPg")
                    .local_key("parent_id")
                    .foreign_key("id"),
            );
        let executor = PgMutationExecutor::new(pool.clone());
        let fk_sql = format!(
            "ALTER TABLE {child_table} ADD CONSTRAINT teaql_fk_state_old
             FOREIGN KEY(parent_id) REFERENCES {parent_table}(id)"
        );
        client
            .batch_execute(&format!("{fk_sql} NOT VALID"))
            .await
            .unwrap();
        let message = executor
            .ensure_schema(&PostgresDialect, &[&child, &parent])
            .await
            .expect_err("NOT VALID must not count as installed schema")
            .to_string();
        assert!(message.contains("installed_validated=false"), "{message}");
        client
            .batch_execute(&format!(
                "ALTER TABLE {child_table} DROP CONSTRAINT teaql_fk_state_old;
                 {fk_sql} DEFERRABLE INITIALLY DEFERRED"
            ))
            .await
            .unwrap();
        let message = executor
            .ensure_schema(&PostgresDialect, &[&child, &parent])
            .await
            .expect_err("DEFERRABLE must not count as immediate FK governance")
            .to_string();
        assert!(message.contains("installed_deferrable=true"), "{message}");
        client
            .batch_execute(&format!(
                "ALTER TABLE {child_table} DROP CONSTRAINT teaql_fk_state_old;
                 {fk_sql}"
            ))
            .await
            .unwrap();
        executor
            .ensure_schema(&PostgresDialect, &[&child, &parent])
            .await
            .expect("a validated non-deferrable equivalent FK must be accepted");
        executor
            .ensure_schema(&PostgresDialect, &[&child, &parent])
            .await
            .expect("an equivalent FK must remain idempotent");
        client
            .batch_execute(&format!(
                "DROP TABLE {child_table}; DROP TABLE {parent_table};"
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ensure_schema_keeps_cross_datasource_and_external_relations_logical() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let client = pool.get().await.unwrap();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS teaql_fk_logical_child_fixture;
                 DROP TABLE IF EXISTS teaql_fk_logical_parent_fixture;",
            )
            .await
            .unwrap();

        let parent = EntityDescriptor::new("LogicalParentFixture")
            .table_name("teaql_fk_logical_parent_fixture")
            .data_service("customer_db")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null());
        let child = EntityDescriptor::new("LogicalChildFixture")
            .table_name("teaql_fk_logical_child_fixture")
            .data_service("order_db")
            .property(PropertyDescriptor::new("id", DataType::U64).id().not_null())
            .property(PropertyDescriptor::new("parent_id", DataType::U64))
            .property(PropertyDescriptor::new("external_id", DataType::U64))
            .relation(
                RelationDescriptor::new("parent", "LogicalParentFixture")
                    .local_key("parent_id")
                    .foreign_key("id"),
            )
            .relation(
                RelationDescriptor::new("external", "NotInstalledFixture")
                    .local_key("external_id")
                    .foreign_key("id"),
            );

        let executor = PgMutationExecutor::new(pool.clone());
        executor
            .ensure_schema(&PostgresDialect, &[&child, &parent])
            .await
            .unwrap();
        executor
            .ensure_schema(&PostgresDialect, &[&parent, &child])
            .await
            .unwrap();
        let foreign_key_count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM pg_constraint c
                   JOIN pg_class t ON t.oid = c.conrelid
                  WHERE c.contype = 'f'
                    AND t.relname = 'teaql_fk_logical_child_fixture'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(foreign_key_count, 0);

        client
            .batch_execute(
                "DROP TABLE teaql_fk_logical_child_fixture;
                 DROP TABLE teaql_fk_logical_parent_fixture;",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn postgres_id_space_is_safe_across_concurrent_generators() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let table = format!(
            "teaql_id_space_deep_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let generator = PgIdSpaceGenerator::new(pool.clone()).with_table_name(&table);
        generator.ensure_table().await.unwrap();
        generator.ensure_floor("SchoolType", 1002).await.unwrap();
        assert_eq!(generator.next_id("SchoolType").await.unwrap(), 1003);

        let mut jobs = Vec::new();
        for _ in 0..4 {
            let independent = PgIdSpaceGenerator::new(pool.clone()).with_table_name(&table);
            jobs.push(tokio::spawn(async move {
                let mut ids = Vec::new();
                for _ in 0..20 {
                    ids.push(independent.next_id("Order").await.unwrap());
                }
                ids
            }));
        }
        let mut ids = Vec::new();
        for job in jobs {
            ids.extend(job.await.unwrap());
        }
        ids.sort_unstable();
        assert_eq!(ids, (1_u64..=80).collect::<Vec<_>>());

        let restarted = PgIdSpaceGenerator::new(pool.clone()).with_table_name(&table);
        assert_eq!(restarted.next_id("Order").await.unwrap(), 81);
        let client = pool.get().await.unwrap();
        client
            .batch_execute(&format!("DROP TABLE {}", quote_ident(&table)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn postgres_id_space_overflow_reports_safe_actionable_context() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let pool = configured_pool(url);
        let table = format!(
            "teaql_id_space_diagnostic_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let generator = PgIdSpaceGenerator::new(pool.clone()).with_table_name(&table);
        generator.ensure_table().await.unwrap();
        let client = pool.get().await.unwrap();
        client
            .execute(
                &format!(
                    "INSERT INTO {}(type_name, current_level) VALUES ($1, $2)",
                    quote_ident(&table)
                ),
                &[&"order", &i64::MAX],
            )
            .await
            .unwrap();

        let message = generator.next_id("Order").await.unwrap_err().to_string();
        assert!(message.contains("PostgreSQL ID provider"), "{message}");
        assert!(message.contains("ID space order"), "{message}");
        assert!(message.contains(&table), "{message}");
        assert!(message.contains("attempt 1"), "{message}");
        assert!(!message.contains("postgres://"), "{message}");
        client
            .batch_execute(&format!("DROP TABLE {}", quote_ident(&table)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn temporal_debug_sql_matches_real_postgres_when_configured() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
        let mut config = deadpool_postgres::Config::new();
        config.url = Some(url);
        let pool = config
            .create_pool(
                Some(deadpool_postgres::Runtime::Tokio1),
                tokio_postgres::NoTls,
            )
            .unwrap();
        let executor = PgMutationExecutor::new(pool);
        executor
            .execute_sql(&CompiledQuery {
                sql: "DROP TABLE IF EXISTS teaql_temporal_runtime_fixture".to_owned(),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        executor.execute_sql(&CompiledQuery { sql: "CREATE TABLE teaql_temporal_runtime_fixture(id BIGINT, d DATE, t TIMESTAMPTZ(3), t_local TIMESTAMP(3))".to_owned(), params: vec![], comment: None }).await.unwrap();
        let prepared = CompiledQuery {
            sql: "INSERT INTO teaql_temporal_runtime_fixture VALUES ($1, $2, $3, TIMESTAMP '1960-01-02 03:04:05.678')".to_owned(),
            params: vec![
                Value::I64(1),
                Value::Date("2024-02-29".parse().unwrap()),
                Value::Timestamp(teaql_core::time::Timestamp(-315_521_754_322)),
            ],
            comment: Some("teaql source=temporal.verify $1".to_owned()),
        };
        executor.execute_sql(&prepared).await.unwrap();
        executor
            .execute_sql(&CompiledQuery {
                sql: prepared
                    .debug_sql(DatabaseKind::PostgreSql)
                    .replace("VALUES (1,", "VALUES (2,"),
                params: vec![],
                comment: None,
            })
            .await
            .unwrap();
        let rows = executor
            .fetch_all_compact_sql(&CompiledQuery {
                sql: "SELECT d, t, t_local FROM teaql_temporal_runtime_fixture ORDER BY id"
                    .to_owned(),
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
pub struct PostgresSchemaProvider;

impl SchemaProvider for PostgresSchemaProvider {
    fn ensure_schema<'a>(
        &'a self,
        context: &'a UserContext,
        _invocation: &'a teaql_runtime::SchemaInvocation,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            ensure_postgres_schema_for(context)
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
        let entity = canonical_id_space_entity(entity);
        let entity = entity.as_str();
        self.ensure_table().await?;
        let table = quote_ident(&self.table_name);
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let select_sql = format!("SELECT current_level FROM {table} WHERE type_name = $1");
        let insert_sql = format!("INSERT INTO {table}(type_name, current_level) VALUES ($1, 1)");
        let update_sql = format!(
            "UPDATE {table} SET current_level = $1 WHERE type_name = $2 AND current_level = $3"
        );
        for attempt in 1..=100 {
            let current = client
                .query_opt(&select_sql, &[&entity])
                .await?
                .map(|row| row.try_get::<_, i64>(0))
                .transpose()?;
            if let Some(current) = current {
                let next = current.checked_add(1).ok_or_else(|| {
                    MutationExecutorError::Bind(format!(
                        "PostgreSQL ID provider overflow for ID space {entity} in table {} on optimistic-lock attempt {attempt}",
                        self.table_name
                    ))
                })?;
                if client
                    .execute(&update_sql, &[&next, &entity, &current])
                    .await?
                    == 1
                {
                    return u64::try_from(next).map_err(|_| {
                        MutationExecutorError::Bind(format!(
                            "PostgreSQL ID provider generated id {next} for ID space {entity} in table {} that cannot be represented as u64",
                            self.table_name
                        ))
                    });
                }
            } else {
                match client.execute(&insert_sql, &[&entity]).await {
                    Ok(1) => return Ok(1),
                    Ok(changed) => {
                        return Err(MutationExecutorError::Bind(format!(
                            "PostgreSQL ID provider insert for ID space {entity} in table {} changed {changed} rows on optimistic-lock attempt {attempt}",
                            self.table_name
                        )));
                    }
                    Err(error) => {
                        if client.query_opt(&select_sql, &[&entity]).await?.is_none() {
                            return Err(error.into());
                        }
                    }
                }
            }
        }
        Err(MutationExecutorError::Bind(format!(
            "PostgreSQL ID provider was unable to allocate ID space {entity} in table {} after 100 optimistic-lock attempts",
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
        let floor = i64::try_from(floor).map_err(|_| {
            MutationExecutorError::Bind(format!(
                "PostgreSQL ID provider floor {floor} for ID space {entity} in table {} exceeds BIGINT",
                self.table_name
            ))
        })?;
        let table = quote_ident(&self.table_name);
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let select = format!("SELECT current_level FROM {table} WHERE type_name = $1");
        let insert = format!("INSERT INTO {table}(type_name, current_level) VALUES ($1, $2)");
        let update = format!(
            "UPDATE {table} SET current_level = $1 WHERE type_name = $2 AND current_level = $3"
        );
        for _ in 1..=100 {
            let current = client
                .query_opt(&select, &[&entity])
                .await?
                .map(|row| row.try_get::<_, i64>(0))
                .transpose()?;
            match current {
                Some(current) if current >= floor => return Ok(()),
                Some(current) => {
                    if client
                        .execute(&update, &[&floor, &entity, &current])
                        .await?
                        == 1
                    {
                        return Ok(());
                    }
                }
                None => match client.execute(&insert, &[&entity, &floor]).await {
                    Ok(1) => return Ok(()),
                    Ok(_) => {}
                    Err(error) => {
                        if client.query_opt(&select, &[&entity]).await?.is_none() {
                            return Err(error.into());
                        }
                    }
                },
            }
        }
        Err(MutationExecutorError::Bind(format!(
            "PostgreSQL ID provider was unable to synchronize floor for ID space {entity} in table {} after 100 optimistic-lock attempts",
            self.table_name
        )))
    }
}

impl InternalIdGenerator for PgIdSpaceGenerator {
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

fn quote_ident(ident: &str) -> String {
    quote_identifier_if_needed(ident, '"')
}

fn postgres_stored_ident(ident: &str) -> String {
    let quoted = quote_ident(ident);
    if quoted.starts_with('"') {
        strip_identifier_quotes(&quoted).to_owned()
    } else {
        strip_identifier_quotes(&quoted).to_ascii_lowercase()
    }
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
        return Some(chrono::DateTime::from_naive_utc_and_offset(
            ndt,
            chrono::Utc,
        ));
    }
    if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let ndt = nd.and_hms_opt(0, 0, 0)?;
        return Some(chrono::DateTime::from_naive_utc_and_offset(
            ndt,
            chrono::Utc,
        ));
    }
    None
}

#[derive(Debug, Clone, Copy)]
struct PgNull;

impl tokio_postgres::types::ToSql for PgNull {
    fn to_sql(
        &self,
        _ty: &tokio_postgres::types::Type,
        _out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(tokio_postgres::types::IsNull::Yes)
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        _ty: &tokio_postgres::types::Type,
        _out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(tokio_postgres::types::IsNull::Yes)
    }
}

#[derive(Debug, Clone, Copy)]
struct PgTimestamp(DateTime<Utc>);

impl tokio_postgres::types::ToSql for PgTimestamp {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if *ty == tokio_postgres::types::Type::TIMESTAMP {
            self.0.naive_utc().to_sql(ty, out)
        } else {
            self.0.to_sql(ty, out)
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::TIMESTAMP
            || *ty == tokio_postgres::types::Type::TIMESTAMPTZ
    }

    tokio_postgres::types::to_sql_checked!();
}

#[derive(Debug, Clone, Copy)]
struct PgInteger(i64);

impl tokio_postgres::types::ToSql for PgInteger {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            tokio_postgres::types::Type::INT2 => i16::try_from(self.0)?.to_sql(ty, out),
            tokio_postgres::types::Type::INT4 => i32::try_from(self.0)?.to_sql(ty, out),
            tokio_postgres::types::Type::INT8 => self.0.to_sql(ty, out),
            _ => Err(format!("integer cannot be encoded as PostgreSQL type {ty}").into()),
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(
            *ty,
            tokio_postgres::types::Type::INT2
                | tokio_postgres::types::Type::INT4
                | tokio_postgres::types::Type::INT8
        )
    }

    tokio_postgres::types::to_sql_checked!();
}

#[derive(Debug, Clone)]
struct PgIntegerList(Vec<i64>);

impl tokio_postgres::types::ToSql for PgIntegerList {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            tokio_postgres::types::Type::INT2_ARRAY => self
                .0
                .iter()
                .copied()
                .map(i16::try_from)
                .collect::<Result<Vec<_>, _>>()?
                .to_sql(ty, out),
            tokio_postgres::types::Type::INT4_ARRAY => self
                .0
                .iter()
                .copied()
                .map(i32::try_from)
                .collect::<Result<Vec<_>, _>>()?
                .to_sql(ty, out),
            tokio_postgres::types::Type::INT8_ARRAY => self.0.to_sql(ty, out),
            _ => Err(format!("integer list cannot be encoded as PostgreSQL type {ty}").into()),
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(
            *ty,
            tokio_postgres::types::Type::INT2_ARRAY
                | tokio_postgres::types::Type::INT4_ARRAY
                | tokio_postgres::types::Type::INT8_ARRAY
        )
    }

    tokio_postgres::types::to_sql_checked!();
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
            args.add(PgNull);
        }
        Value::Bool(v) => args.add(*v),
        Value::I64(v) => args.add(PgInteger(*v)),
        Value::U64(v) => {
            let v = i64::try_from(*v).map_err(|_| {
                MutationExecutorError::Bind(format!("u64 value {v} exceeds i64 range"))
            })?;
            args.add(PgInteger(v));
        }
        Value::F64(v) => args.add(*v),
        Value::Decimal(v) => args.add(*v),
        Value::Text(v) => match try_parse_datetime_from_str(v) {
            Some(dt) => args.add(dt),
            None => args.add(v.clone()),
        },
        Value::Json(v) => {
            let j_val: serde_json::Value =
                serde_json::to_value(v).map_err(|e| MutationExecutorError::Bind(e.to_string()))?;
            args.add(j_val);
        }
        Value::Date(v) => args.add(*v),
        Value::Timestamp(v) => args.add(PgTimestamp(v.to_datetime())),
        Value::Object(_) => return Err(MutationExecutorError::UnsupportedValue("object")),
        Value::List(values) => bind_pg_list(args, values)?,
        Value::TypedNull(dt) => match dt {
            DataType::Bool => args.add(Option::<bool>::None),
            DataType::I64 | DataType::U64 => args.add(Option::<i64>::None),
            DataType::F64 => args.add(Option::<f64>::None),
            DataType::Decimal => args.add(Option::<Decimal>::None),
            DataType::Text | DataType::LargeText => args.add(Option::<String>::None),
            DataType::Json => args.add(Option::<serde_json::Value>::None),
            DataType::Date => args.add(Option::<NaiveDate>::None),
            DataType::Timestamp => args.add(PgNull),
        },
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
            args.add(PgIntegerList(values));
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
            args.add(PgIntegerList(values));
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
                    Value::Timestamp(value) => Ok(value.to_datetime()),
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
        Value::TypedNull(_) => return Err(MutationExecutorError::UnsupportedValue("null list")),
    }
    Ok(())
}

fn decode_pg_values(row: &tokio_postgres::Row) -> Result<Vec<Value>, MutationExecutorError> {
    let mut values = Vec::with_capacity(row.len());
    for (index, column) in row.columns().iter().enumerate() {
        let type_name = column.type_().name();

        let value = match type_name {
            "bool" | "boolean" => {
                let v: Option<bool> = row.try_get(index)?;
                match v {
                    Some(v) => Value::Bool(v),
                    None => Value::Null,
                }
            }
            "int2" => {
                let v: Option<i16> = row.try_get(index)?;
                match v {
                    Some(v) => Value::I64(v as i64),
                    None => Value::Null,
                }
            }
            "int4" => {
                let v: Option<i32> = row.try_get(index)?;
                match v {
                    Some(v) => Value::I64(v as i64),
                    None => Value::Null,
                }
            }
            "int8" => {
                let v: Option<i64> = row.try_get(index)?;
                match v {
                    Some(v) => Value::I64(v),
                    None => Value::Null,
                }
            }
            "float4" => {
                let v: Option<f32> = row.try_get(index)?;
                match v {
                    Some(v) => Value::F64(v as f64),
                    None => Value::Null,
                }
            }
            "float8" => {
                let v: Option<f64> = row.try_get(index)?;
                match v {
                    Some(v) => Value::F64(v),
                    None => Value::Null,
                }
            }
            "numeric" => {
                let v: Option<Decimal> = row.try_get(index)?;
                match v {
                    Some(v) => Value::Decimal(v),
                    None => Value::Null,
                }
            }
            "json" | "jsonb" => {
                let v: Option<serde_json::Value> = row.try_get(index)?;
                match v {
                    Some(j) => Value::Json(j),
                    None => Value::Null,
                }
            }
            "date" => {
                let v: Option<NaiveDate> = row.try_get(index)?;
                match v {
                    Some(v) => Value::Date(v),
                    None => Value::Null,
                }
            }
            "timestamp" => {
                let v: Option<NaiveDateTime> = row.try_get(index)?;
                match v {
                    Some(v) => Value::Timestamp(teaql_core::time::Timestamp(
                        v.and_utc().timestamp_millis(),
                    )),
                    None => Value::Null,
                }
            }
            "timestamptz" => {
                let v: Option<DateTime<Utc>> = row.try_get(index)?;
                match v {
                    Some(v) => Value::Timestamp(teaql_core::time::Timestamp(v.timestamp_millis())),
                    None => Value::Null,
                }
            }
            "text" | "varchar" | "bpchar" | "name" | "uuid" => {
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
        values.push(value);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use teaql_core::{DeleteCommand, RecoverCommand, RelationDescriptor};

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

    fn postgres_metadata(data_type: &str) -> PostgresColumnMetadata {
        PostgresColumnMetadata {
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
            ensure_postgres_column_compatibility(
                &entity,
                &bool_property,
                &postgres_metadata("boolean")
            )
            .is_ok()
        );
        assert!(
            ensure_postgres_column_compatibility(
                &entity,
                &bool_property,
                &postgres_metadata("bigint")
            )
            .is_err()
        );
        assert!(
            ensure_postgres_column_compatibility(
                &entity,
                &date_property,
                &postgres_metadata("date")
            )
            .is_ok()
        );
        assert!(
            ensure_postgres_column_compatibility(
                &entity,
                &date_property,
                &postgres_metadata("timestamp with time zone")
            )
            .is_err()
        );
        assert!(
            ensure_postgres_column_compatibility(
                &entity,
                &timestamp_property,
                &postgres_metadata("timestamp with time zone")
            )
            .is_ok()
        );
        assert!(
            ensure_postgres_column_compatibility(
                &entity,
                &timestamp_property,
                &postgres_metadata("timestamp without time zone")
            )
            .is_err()
        );
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
        assert_eq!(insert.sql, "INSERT INTO orders (id, name) VALUES ($1, $2)");

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
            "CREATE TABLE IF NOT EXISTS orders (id BIGINT PRIMARY KEY NOT NULL, version BIGINT NOT NULL, name VARCHAR(255))"
        );
        let required = PostgresDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("code", DataType::Text).not_null(),
            )
            .unwrap();
        assert_eq!(
            required,
            "ALTER TABLE orders ADD COLUMN code VARCHAR(255) NOT NULL"
        );
        let shaped_text = PostgresDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("short_code", DataType::Text).max_length(32),
            )
            .unwrap();
        assert_eq!(
            shaped_text,
            "ALTER TABLE orders ADD COLUMN short_code VARCHAR(32)"
        );
        let shaped_decimal = PostgresDialect
            .compile_add_column(
                &entity(),
                &PropertyDescriptor::new("amount", DataType::Decimal)
                    .numeric_precision(19)
                    .numeric_scale(7),
            )
            .unwrap();
        assert_eq!(
            shaped_decimal,
            "ALTER TABLE orders ADD COLUMN amount NUMERIC(19,7)"
        );
        assert!(
            PostgresDialect
                .schema_setup_sqls()
                .iter()
                .any(|sql| sql.contains("CREATE OR REPLACE FUNCTION soundex"))
        );

        let values = (1_u64..=21).map(Value::from).collect::<Vec<_>>();
        let query = PostgresDialect
            .compile_select(
                &entity(),
                &SelectQuery::new("Order")
                    .filter(Expr::in_list("id", values.clone()))
                    .order_asc("id"),
            )
            .unwrap();
        assert_eq!(
            query.sql,
            "SELECT id, version, name FROM orders WHERE (id = ANY($1)) ORDER BY id ASC"
        );
        assert_eq!(query.params, vec![Value::List(values)]);
    }

    #[test]
    fn topn_012_postgres_schema_adds_full_foreign_key_id_desc_index() {
        let trip = EntityDescriptor::new("Trip")
            .table_name("trip_data")
            .property(
                PropertyDescriptor::new("id", DataType::U64)
                    .column_name("id")
                    .id()
                    .not_null(),
            )
            .property(
                PropertyDescriptor::new("vendor_id", DataType::U64)
                    .column_name("vendor")
                    .not_null(),
            )
            .relation(
                RelationDescriptor::new("vendor", "Vendor")
                    .local_key("vendor_id")
                    .foreign_key("id"),
            )
            // A second relation through the same key must not duplicate DDL.
            .relation(
                RelationDescriptor::new("billing_vendor", "Vendor")
                    .local_key("vendor_id")
                    .foreign_key("id"),
            )
            // Reverse relations belong to the target table and are ignored here.
            .relation(
                RelationDescriptor::new("items", "TripItem")
                    .local_key("id")
                    .foreign_key("trip_id")
                    .many(),
            );

        assert_eq!(
            PostgresDialect.relation_indexes_sqls(&trip),
            vec![
                "CREATE INDEX IF NOT EXISTS IDX_TRIP_DATA_VENDOR_ID_DESC ON trip_data (vendor, id DESC)"
            ]
        );
    }

    #[test]
    fn postgres_relation_index_name_is_stable_and_within_identifier_limit() {
        let name = postgres_index_name(
            "an_extremely_long_generated_transaction_history_table_name",
            "an_equally_long_business_owner_reference_identifier",
            "id",
        );
        assert!(name.len() <= 63);
        assert_eq!(
            name,
            postgres_index_name(
                "an_extremely_long_generated_transaction_history_table_name",
                "an_equally_long_business_owner_reference_identifier",
                "id",
            )
        );
        assert!(name.ends_with("_889B21BBED38CC82"));
    }
}
