#![allow(warnings)]
use std::future::Future;
use std::pin::Pin;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use deadpool_postgres::Pool;
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
    CompiledQuery, DatabaseKind, SqlCompileError, SqlDialect, SqlTransport,
    quote_identifier_if_needed,
};
use tokio::sync::Mutex;

pub const DEFAULT_ID_SPACE_TABLE: &str = "teaql_id_space";

#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresDialect;

impl PostgresDialect {
    /// Indexes supporting the common "recent children for each parent" access
    /// pattern.  This deliberately uses a full index: PostgreSQL cannot use a
    /// partial `WHERE version > 0` index for a generic prepared plan whose
    /// version predicate is parameterized.
    fn relation_indexes_sqls(&self, entity: &EntityDescriptor) -> Vec<String> {
        let Some(id_property) = entity.id_property() else {
            return Vec::new();
        };
        let mut indexed_columns = HashSet::new();
        let mut sqls = Vec::new();

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
            sqls.push(format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} ({}, {} DESC)",
                self.quote_ident(&index_name),
                self.quote_ident(&entity.table_name),
                self.quote_ident(&foreign_key_property.column_name),
                self.quote_ident(&id_property.column_name),
            ));
        }
        sqls
    }
}

fn postgres_index_name(table: &str, foreign_key: &str, id: &str) -> String {
    let full = format!("IDX_{table}_{foreign_key}_{id}_DESC").to_uppercase();
    if full.len() <= 63 {
        return full;
    }

    // PostgreSQL silently truncates identifiers to 63 bytes. Add a stable hash
    // ourselves so two long generated names cannot collapse to the same index.
    let hash = full.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    let suffix = format!("_{hash:016X}");
    let prefix_bytes = 63 - suffix.len();
    let mut end = prefix_bytes.min(full.len());
    while !full.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &full[..end], suffix)
}

fn postgres_foreign_key_name(
    source_table: &str,
    source_column: &str,
    referenced_table: &str,
    referenced_column: &str,
) -> String {
    let full = format!("FK_{source_table}_{source_column}_{referenced_table}_{referenced_column}")
        .to_uppercase();
    if full.len() <= 63 {
        return full;
    }
    let hash = full.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    let suffix = format!("_{hash:016X}");
    let mut end = (63 - suffix.len()).min(full.len());
    while !full.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &full[..end], suffix)
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
            DataType::Text => Ok("VARCHAR(255)"),
            DataType::LargeText => Ok("TEXT"),
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
            Self::Driver(err) => err.fmt(f),
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

impl teaql_sql::SqlTransaction for PgMutationExecutor {
    type Error = MutationExecutorError;

    async fn commit_sql(self) -> Result<(), Self::Error> {
        Err(MutationExecutorError::Bind(
            "Transactions not supported yet".to_string(),
        ))
    }

    async fn rollback_sql(self) -> Result<(), Self::Error> {
        Err(MutationExecutorError::Bind(
            "Transactions not supported yet".to_string(),
        ))
    }
}

impl teaql_sql::SqlTransactionTransport for PgMutationExecutor {
    type Tx<'a>
        = Self
    where
        Self: 'a;

    async fn begin_sql(&self) -> Result<Self::Tx<'_>, Self::Error> {
        Err(MutationExecutorError::Bind(
            "Transactions not supported yet".to_string(),
        ))
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
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        {
            let transaction = client.transaction().await?;
            transaction
                .query_one(
                    "SELECT pg_advisory_xact_lock(hashtextextended('teaql-schema-setup', 0))",
                    &[],
                )
                .await?;
            for sql in dialect.schema_setup_sqls() {
                transaction.execute(*sql, &[]).await?;
            }
            transaction.commit().await?;
        }
        self.ensure_id_space_table(DEFAULT_ID_SPACE_TABLE).await?;

        for entity in entities {
            if !self.table_exists(&entity.table_name).await? {
                let sql = dialect.compile_create_table(entity)?;
                client.execute(&sql, &[]).await?;
            } else {
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

            for sql in dialect.schema_indexes_sqls(entity)? {
                client.execute(&sql, &[]).await?;
            }
            for sql in dialect.relation_indexes_sqls(entity) {
                client.execute(&sql, &[]).await?;
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
                self.ensure_foreign_key(
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

    async fn ensure_foreign_key(
        &self,
        source_table: &str,
        source_column: &str,
        referenced_table: &str,
        referenced_column: &str,
    ) -> Result<(), MutationExecutorError> {
        let semantic_key = format!(
            "teaql-fk:{source_table}:{source_column}:{referenced_table}:{referenced_column}:a:a"
        );
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let transaction = client.transaction().await?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&semantic_key],
            )
            .await?;
        let exists: bool = transaction
            .query_one(
                "SELECT EXISTS (
                    SELECT 1
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
                       AND cardinality(c.conkey) = 1 AND cardinality(c.confkey) = 1
                       AND c.confupdtype = 'a' AND c.confdeltype = 'a'
                )",
                &[
                    &strip_identifier_quotes(source_table),
                    &strip_identifier_quotes(source_column),
                    &strip_identifier_quotes(referenced_table),
                    &strip_identifier_quotes(referenced_column),
                ],
            )
            .await?
            .try_get(0)?;
        if !exists {
            let constraint_name = postgres_foreign_key_name(
                source_table,
                source_column,
                referenced_table,
                referenced_column,
            );
            let sql = format!(
                "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
                quote_ident(source_table),
                quote_ident(&constraint_name),
                quote_ident(source_column),
                quote_ident(referenced_table),
                quote_ident(referenced_column),
            );
            transaction.execute(&sql, &[]).await?;
        }
        transaction.commit().await?;
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

    async fn table_exists(&self, table_name: &str) -> Result<bool, MutationExecutorError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
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

    async fn table_columns(
        &self,
        table_name: &str,
    ) -> Result<std::collections::BTreeSet<String>, MutationExecutorError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| MutationExecutorError::Pool(e.to_string()))?;
        let rows = client
            .query(
                "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = $1",
                &[&table_name],
            )
            .await?;
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
    use teaql_sql::{SqlTransport, StreamingSqlTransport};

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
    async fn ensure_schema_creates_foreign_key_once_by_semantics() {
        let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
            return;
        };
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
        for _ in 1..=100 {
            let current = client
                .query_opt(&select_sql, &[&entity])
                .await?
                .map(|row| row.try_get::<_, i64>(0))
                .transpose()?;
            if let Some(current) = current {
                let next = current.checked_add(1).ok_or_else(|| {
                    MutationExecutorError::Bind(format!("ID space overflow for {entity}"))
                })?;
                if client
                    .execute(&update_sql, &[&next, &entity, &current])
                    .await?
                    == 1
                {
                    return u64::try_from(next).map_err(|_| {
                        MutationExecutorError::Bind(format!(
                            "generated id {next} cannot be represented as u64"
                        ))
                    });
                }
            } else {
                match client.execute(&insert_sql, &[&entity]).await {
                    Ok(1) => return Ok(1),
                    Ok(changed) => {
                        return Err(MutationExecutorError::Bind(format!(
                            "ID space insert for {entity} changed {changed} rows"
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
            "Unable to allocate ID for {entity} after 100 optimistic-lock attempts"
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
                "ID space floor {floor} for {entity} exceeds BIGINT"
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
            "Unable to synchronize ID space floor for {entity} after 100 optimistic-lock attempts"
        )))
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
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(tokio_postgres::types::IsNull::Yes)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
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
                    Some(j) => Value::Json(j.into()),
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
