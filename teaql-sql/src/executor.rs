#![allow(clippy::manual_async_fn)]
#![allow(async_fn_in_trait)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use teaql_core::{Expr, Record, SelectQuery};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
};

use crate::{CompiledQuery, SqlCompileError, SqlDialect};

pub trait SqlTransport: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn fetch_all_sql(
        &self,
        query: &CompiledQuery,
    ) -> impl std::future::Future<Output = Result<Vec<Record>, Self::Error>> + Send;
    fn execute_sql(
        &self,
        query: &CompiledQuery,
    ) -> impl std::future::Future<Output = Result<u64, Self::Error>> + Send;
}

pub trait StreamingSqlTransport: SqlTransport {
    fn stream_sql(
        &self,
        query: CompiledQuery,
        chunk_size: usize,
    ) -> teaql_data_service::QueryStream<'_, Self::Error>;
}

pub trait SqlTransactionTransport: SqlTransport {
    type Tx<'a>: SqlTransport<Error = Self::Error>
        + SqlTransaction<Error = Self::Error>
        + Send
        + Sync
        + 'a
    where
        Self: 'a;

    fn begin_sql(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::Tx<'_>, Self::Error>> + Send;
}

pub trait SqlTransaction {
    type Error: std::error::Error + Send + Sync + 'static;
    fn commit_sql(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
    fn rollback_sql(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Debug)]
pub enum SqlExecutorError<E: std::error::Error + Send + Sync + 'static> {
    Compile(SqlCompileError),
    Transport(E),
    PersistedRecord(String),
}

impl<E: std::error::Error + Send + Sync + 'static> std::fmt::Display for SqlExecutorError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlExecutorError::Compile(e) => write!(f, "SQL compile error: {}", e),
            SqlExecutorError::Transport(e) => write!(f, "Transport error: {}", e),
            SqlExecutorError::PersistedRecord(e) => write!(f, "Persisted record error: {}", e),
        }
    }
}

impl<E: std::error::Error + Send + Sync + 'static> std::error::Error for SqlExecutorError<E> {}

#[derive(Clone)]
pub struct SqlDataServiceExecutor<D, T, S> {
    pub dialect: D,
    pub transport: T,
    pub schema_provider: S,
    descriptor_cache: Arc<RwLock<HashMap<String, Arc<teaql_core::EntityDescriptor>>>>,
}

impl<D, T, S> SqlDataServiceExecutor<D, T, S> {
    pub fn new(dialect: D, transport: T, schema_provider: S) -> Self {
        Self {
            dialect,
            transport,
            schema_provider,
            descriptor_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<D, T, S> SqlDataServiceExecutor<D, T, S>
where
    S: teaql_data_service::SchemaProvider,
{
    fn entity_descriptor(&self, name: &str) -> Option<Arc<teaql_core::EntityDescriptor>> {
        if let Ok(cache) = self.descriptor_cache.read() {
            if let Some(descriptor) = cache.get(name) {
                return Some(descriptor.clone());
            }
        }
        let descriptor = self.schema_provider.get_entity(name)?;
        if let Ok(mut cache) = self.descriptor_cache.write() {
            return Some(
                cache
                    .entry(name.to_owned())
                    .or_insert_with(|| descriptor.clone())
                    .clone(),
            );
        }
        Some(descriptor)
    }
}

impl<
    D: SqlDialect + Send + Sync,
    T: SqlTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> DataServiceExecutor for SqlDataServiceExecutor<D, T, S>
{
    type Error = SqlExecutorError<T::Error>;

    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceCapabilities {
            query: true,
            mutation: true,
            transaction: false, // Override if T implements SqlTransactionTransport
            schema: false,
            id_generation: false,
            batch_mutation: true,
            returning: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};

    #[derive(Clone, Copy)]
    struct TestDialect;

    impl SqlDialect for TestDialect {
        fn kind(&self) -> crate::DatabaseKind {
            crate::DatabaseKind::PostgreSql
        }

        fn quote_ident(&self, ident: &str) -> String {
            format!("\"{ident}\"")
        }

        fn placeholder(&self, index: usize) -> String {
            format!("${index}")
        }
    }

    #[derive(Clone, Copy)]
    struct EmptyTransport;

    impl SqlTransport for EmptyTransport {
        type Error = std::io::Error;

        async fn fetch_all_sql(&self, _query: &CompiledQuery) -> Result<Vec<Record>, Self::Error> {
            Ok(Vec::new())
        }

        async fn execute_sql(&self, _query: &CompiledQuery) -> Result<u64, Self::Error> {
            Ok(0)
        }
    }

    #[derive(Clone)]
    struct CountingSchemaProvider {
        lookups: Arc<AtomicUsize>,
    }

    impl teaql_data_service::SchemaProvider for CountingSchemaProvider {
        fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            (name == "Order").then(|| {
                Arc::new(
                    EntityDescriptor::new("Order")
                        .property(PropertyDescriptor::new("id", DataType::U64).id().not_null()),
                )
            })
        }
    }

    fn query_request() -> QueryRequest {
        QueryRequest {
            query: SelectQuery::new("Order"),
            trace_chain: Vec::new(),
            comment: None,
        }
    }

    #[tokio::test]
    async fn caches_entity_descriptors_across_executor_clones() {
        let lookups = Arc::new(AtomicUsize::new(0));
        let executor = SqlDataServiceExecutor::new(
            TestDialect,
            EmptyTransport,
            CountingSchemaProvider {
                lookups: lookups.clone(),
            },
        );

        executor.query(query_request()).await.unwrap();
        executor.clone().query(query_request()).await.unwrap();

        assert_eq!(lookups.load(Ordering::Relaxed), 1);
    }
}

impl<
    D: SqlDialect + Send + Sync,
    T: SqlTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> QueryExecutor for SqlDataServiceExecutor<D, T, S>
{
    fn query(
        &self,
        request: QueryRequest,
    ) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send {
        async move {
            let entity_desc = self
                .entity_descriptor(&request.query.entity)
                .ok_or_else(|| {
                    SqlExecutorError::Compile(SqlCompileError::UnknownEntity(
                        request.query.entity.clone(),
                    ))
                })?;

            let compiled = self
                .dialect
                .compile_select(&entity_desc, &request.query)
                .map_err(SqlExecutorError::Compile)?;
            let start = SystemTime::now();
            let rows = self
                .transport
                .fetch_all_sql(&compiled)
                .await
                .map_err(SqlExecutorError::Transport)?;
            let end = SystemTime::now();
            let debug_query = compiled.debug_sql(self.dialect.kind());
            let CompiledQuery { sql, params, .. } = compiled;

            let metadata = ExecutionMetadata {
                backend: "sql".to_string(),
                operation: DataServiceOperation::Query,
                started_at: start,
                ended_at: end,
                affected_rows: None,
                result_count: Some(rows.len()),
                trace_chain: request.trace_chain,
                comment: request.comment,
                backend_request_id: None,
                parameterized_query: Some(sql),
                params,
                debug_query: Some(debug_query),
            };

            Ok(QueryResult { rows, metadata })
        }
    }
}

impl<
    D: SqlDialect + Send + Sync,
    T: SqlTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> MutationExecutor for SqlDataServiceExecutor<D, T, S>
{
    fn mutate(
        &self,
        request: MutationRequest,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send {
        async move {
            let entity_name = match &request {
                MutationRequest::Insert(cmd) => &cmd.entity,
                MutationRequest::Update(cmd) => &cmd.entity,
                MutationRequest::Delete(cmd) => &cmd.entity,
                MutationRequest::Recover(cmd) => &cmd.entity,
                MutationRequest::Batch(mutations) => {
                    let mut total_affected = 0;
                    let mut parameterized_queries = Vec::new();
                    let mut params = Vec::new();
                    let mut debug_queries = Vec::new();
                    let start = SystemTime::now();
                    for req in mutations {
                        let res = Box::pin(self.mutate(req.clone())).await?;
                        total_affected += res.affected_rows;
                        if let Some(query) = res.metadata.parameterized_query {
                            parameterized_queries.push(query);
                        }
                        params.extend(res.metadata.params);
                        if let Some(query) = res.metadata.debug_query {
                            debug_queries.push(query);
                        }
                    }
                    let end = SystemTime::now();
                    return Ok(MutationResult {
                        affected_rows: total_affected,
                        generated_values: Record::default(),
                        persisted_record: None,
                        metadata: ExecutionMetadata {
                            backend: "sql".to_string(),
                            operation: DataServiceOperation::Batch,
                            started_at: start,
                            ended_at: end,
                            affected_rows: Some(total_affected),
                            result_count: None,
                            trace_chain: Vec::new(),
                            comment: None,
                            backend_request_id: None,
                            parameterized_query: (!parameterized_queries.is_empty())
                                .then(|| parameterized_queries.join("; ")),
                            params,
                            debug_query: (!debug_queries.is_empty())
                                .then(|| debug_queries.join("; ")),
                        },
                    });
                }
            };

            let entity_desc = self.entity_descriptor(entity_name).ok_or_else(|| {
                SqlExecutorError::Compile(SqlCompileError::UnknownEntity(entity_name.clone()))
            })?;

            let compiled = match &request {
                MutationRequest::Insert(cmd) => self
                    .dialect
                    .compile_insert(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Update(cmd) => self
                    .dialect
                    .compile_update(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Delete(cmd) => self
                    .dialect
                    .compile_delete(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Recover(cmd) => self
                    .dialect
                    .compile_recover(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Batch(_) => unreachable!(),
            };

            let start = SystemTime::now();
            let affected_rows = self
                .transport
                .execute_sql(&compiled)
                .await
                .map_err(SqlExecutorError::Transport)?;
            let end = SystemTime::now();

            let operation = match &request {
                MutationRequest::Insert(_) => DataServiceOperation::Insert,
                MutationRequest::Update(_) => DataServiceOperation::Update,
                MutationRequest::Delete(_) => DataServiceOperation::Delete,
                MutationRequest::Recover(_) => DataServiceOperation::Recover,
                MutationRequest::Batch(_) => DataServiceOperation::Batch,
            };

            let metadata = ExecutionMetadata {
                backend: "sql".to_string(),
                operation,
                started_at: start,
                ended_at: end,
                affected_rows: Some(affected_rows),
                result_count: None,
                trace_chain: request.trace_chain().to_vec(),
                comment: request.comment().map(|s| s.to_owned()),
                backend_request_id: None,
                parameterized_query: Some(compiled.sql.clone()),
                params: compiled.params.clone(),
                debug_query: Some(compiled.debug_sql(self.dialect.kind())),
            };

            Ok(MutationResult {
                affected_rows,
                generated_values: Record::default(),
                persisted_record: None,
                metadata,
            })
        }
    }
}

#[derive(Clone)]
pub struct SqlDataServiceTransaction<'a, D, Tx: SqlTransport + SqlTransaction, S> {
    pub dialect: &'a D,
    pub transport: Tx,
    pub schema_provider: &'a S,
    descriptor_cache: Arc<RwLock<HashMap<String, Arc<teaql_core::EntityDescriptor>>>>,
}

impl<'a, D, Tx: SqlTransport + SqlTransaction, S> SqlDataServiceTransaction<'a, D, Tx, S>
where
    S: teaql_data_service::SchemaProvider,
{
    fn entity_descriptor(&self, name: &str) -> Option<Arc<teaql_core::EntityDescriptor>> {
        if let Ok(cache) = self.descriptor_cache.read() {
            if let Some(descriptor) = cache.get(name) {
                return Some(descriptor.clone());
            }
        }
        let descriptor = self.schema_provider.get_entity(name)?;
        if let Ok(mut cache) = self.descriptor_cache.write() {
            return Some(
                cache
                    .entry(name.to_owned())
                    .or_insert_with(|| descriptor.clone())
                    .clone(),
            );
        }
        Some(descriptor)
    }
}

impl<
    'a,
    D: SqlDialect + Send + Sync,
    Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> DataServiceExecutor for SqlDataServiceTransaction<'a, D, Tx, S>
{
    type Error = SqlExecutorError<<Tx as SqlTransport>::Error>;

    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceCapabilities {
            query: true,
            mutation: true,
            transaction: false,
            schema: false,
            id_generation: false,
            batch_mutation: true,
            returning: false,
        }
    }
}

impl<
    'a,
    D: SqlDialect + Send + Sync,
    Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> QueryExecutor for SqlDataServiceTransaction<'a, D, Tx, S>
{
    fn query(
        &self,
        request: QueryRequest,
    ) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send {
        async move {
            let entity_desc = self
                .entity_descriptor(&request.query.entity)
                .ok_or_else(|| {
                    SqlExecutorError::Compile(SqlCompileError::UnknownEntity(
                        request.query.entity.clone(),
                    ))
                })?;

            let compiled = self
                .dialect
                .compile_select(&entity_desc, &request.query)
                .map_err(SqlExecutorError::Compile)?;
            let start = SystemTime::now();
            let rows = self
                .transport
                .fetch_all_sql(&compiled)
                .await
                .map_err(SqlExecutorError::Transport)?;
            let end = SystemTime::now();

            let metadata = ExecutionMetadata {
                backend: "sql".to_string(),
                operation: DataServiceOperation::Query,
                started_at: start,
                ended_at: end,
                affected_rows: None,
                result_count: Some(rows.len()),
                trace_chain: request.trace_chain,
                comment: request.comment,
                backend_request_id: None,
                parameterized_query: Some(compiled.sql.clone()),
                params: compiled.params.clone(),
                debug_query: Some(compiled.debug_sql(self.dialect.kind())),
            };

            Ok(QueryResult { rows, metadata })
        }
    }
}

impl<
    'a,
    D: SqlDialect + Send + Sync,
    Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> MutationExecutor for SqlDataServiceTransaction<'a, D, Tx, S>
{
    fn mutate(
        &self,
        request: MutationRequest,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send {
        async move {
            let entity_name = match &request {
                MutationRequest::Insert(cmd) => &cmd.entity,
                MutationRequest::Update(cmd) => &cmd.entity,
                MutationRequest::Delete(cmd) => &cmd.entity,
                MutationRequest::Recover(cmd) => &cmd.entity,
                MutationRequest::Batch(mutations) => {
                    let mut total_affected = 0;
                    let mut parameterized_queries = Vec::new();
                    let mut params = Vec::new();
                    let mut debug_queries = Vec::new();
                    let start = SystemTime::now();
                    for req in mutations {
                        let res = Box::pin(self.mutate(req.clone())).await?;
                        total_affected += res.affected_rows;
                        if let Some(query) = res.metadata.parameterized_query {
                            parameterized_queries.push(query);
                        }
                        params.extend(res.metadata.params);
                        if let Some(query) = res.metadata.debug_query {
                            debug_queries.push(query);
                        }
                    }
                    let end = SystemTime::now();
                    return Ok(MutationResult {
                        affected_rows: total_affected,
                        generated_values: Record::default(),
                        persisted_record: None,
                        metadata: ExecutionMetadata {
                            backend: "sql".to_string(),
                            operation: DataServiceOperation::Batch,
                            started_at: start,
                            ended_at: end,
                            affected_rows: Some(total_affected),
                            result_count: None,
                            trace_chain: Vec::new(),
                            comment: None,
                            backend_request_id: None,
                            parameterized_query: (!parameterized_queries.is_empty())
                                .then(|| parameterized_queries.join("; ")),
                            params,
                            debug_query: (!debug_queries.is_empty())
                                .then(|| debug_queries.join("; ")),
                        },
                    });
                }
            };

            let entity_desc = self.entity_descriptor(entity_name).ok_or_else(|| {
                SqlExecutorError::Compile(SqlCompileError::UnknownEntity(entity_name.clone()))
            })?;

            let compiled = match &request {
                MutationRequest::Insert(cmd) => self
                    .dialect
                    .compile_insert(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Update(cmd) => self
                    .dialect
                    .compile_update(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Delete(cmd) => self
                    .dialect
                    .compile_delete(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Recover(cmd) => self
                    .dialect
                    .compile_recover(&entity_desc, cmd)
                    .map_err(SqlExecutorError::Compile)?,
                MutationRequest::Batch(_) => unreachable!("batch handled above"),
            };

            let start = SystemTime::now();
            let affected_rows = self
                .transport
                .execute_sql(&compiled)
                .await
                .map_err(SqlExecutorError::Transport)?;
            let end = SystemTime::now();

            let operation = match &request {
                MutationRequest::Insert(_) => DataServiceOperation::Insert,
                MutationRequest::Update(_) => DataServiceOperation::Update,
                MutationRequest::Delete(_) => DataServiceOperation::Delete,
                MutationRequest::Recover(_) => DataServiceOperation::Recover,
                MutationRequest::Batch(_) => DataServiceOperation::Batch,
            };

            let persisted_id = match &request {
                MutationRequest::Insert(cmd) => cmd.values.get("id").cloned(),
                MutationRequest::Update(cmd) => Some(cmd.id.clone()),
                MutationRequest::Delete(cmd) if cmd.soft_delete => Some(cmd.id.clone()),
                MutationRequest::Recover(cmd) => Some(cmd.id.clone()),
                MutationRequest::Delete(_) | MutationRequest::Batch(_) => None,
            };
            let persisted_record = if affected_rows == 1 {
                if let Some(id) = persisted_id {
                    let query = SelectQuery::new(entity_name.clone()).filter(Expr::eq("id", id));
                    let compiled_readback = self
                        .dialect
                        .compile_select(&entity_desc, &query)
                        .map_err(SqlExecutorError::Compile)?;
                    let mut rows = self
                        .transport
                        .fetch_all_sql(&compiled_readback)
                        .await
                        .map_err(SqlExecutorError::Transport)?;
                    if rows.len() != 1 {
                        return Err(SqlExecutorError::PersistedRecord(format!(
                            "persisted {entity_name} record could not be read back"
                        )));
                    }
                    rows.pop()
                } else {
                    None
                }
            } else {
                None
            };

            let metadata = ExecutionMetadata {
                backend: "sql".to_string(),
                operation,
                started_at: start,
                ended_at: end,
                affected_rows: Some(affected_rows),
                result_count: None,
                trace_chain: request.trace_chain().to_vec(),
                comment: request.comment().map(|s| s.to_owned()),
                backend_request_id: None,
                parameterized_query: Some(compiled.sql.clone()),
                params: compiled.params.clone(),
                debug_query: Some(compiled.debug_sql(self.dialect.kind())),
            };

            Ok(MutationResult {
                affected_rows,
                generated_values: Record::default(),
                persisted_record,
                metadata,
            })
        }
    }
}

impl<
    'a,
    D: SqlDialect + Send + Sync,
    Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> teaql_data_service::Transaction for SqlDataServiceTransaction<'a, D, Tx, S>
{
    type Error = SqlExecutorError<<Tx as SqlTransport>::Error>;

    fn commit(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            self.transport
                .commit_sql()
                .await
                .map_err(SqlExecutorError::Transport)
        }
    }

    fn rollback(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            self.transport
                .rollback_sql()
                .await
                .map_err(SqlExecutorError::Transport)
        }
    }
}

impl<
    D: SqlDialect + Send + Sync,
    T: SqlTransactionTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> teaql_data_service::TransactionExecutor for SqlDataServiceExecutor<D, T, S>
{
    type Tx<'a>
        = SqlDataServiceTransaction<'a, D, T::Tx<'a>, S>
    where
        Self: 'a;

    fn begin(&self) -> impl std::future::Future<Output = Result<Self::Tx<'_>, Self::Error>> + Send {
        async move {
            let tx = self
                .transport
                .begin_sql()
                .await
                .map_err(SqlExecutorError::Transport)?;
            Ok(SqlDataServiceTransaction {
                dialect: &self.dialect,
                transport: tx,
                schema_provider: &self.schema_provider,
                descriptor_cache: self.descriptor_cache.clone(),
            })
        }
    }
}

impl<
    D: SqlDialect + Send + Sync,
    T: StreamingSqlTransport + Send + Sync,
    S: teaql_data_service::SchemaProvider + Send + Sync,
> teaql_data_service::StreamQueryExecutor for SqlDataServiceExecutor<D, T, S>
{
    fn query_stream(
        &self,
        request: teaql_data_service::QueryRequest,
        chunk_size: usize,
    ) -> teaql_data_service::QueryStream<'_, Self::Error> {
        use futures_util::StreamExt;
        let entity = match self.entity_descriptor(&request.query.entity) {
            Some(entity) => entity,
            None => {
                return Box::pin(futures_util::stream::once(async {
                    Err(SqlExecutorError::Compile(SqlCompileError::UnknownEntity(
                        request.query.entity,
                    )))
                }));
            }
        };
        match self.dialect.compile_select(&entity, &request.query) {
            Ok(compiled) => Box::pin(
                self.transport
                    .stream_sql(compiled, chunk_size)
                    .map(|r| r.map_err(SqlExecutorError::Transport)),
            ),
            Err(error) => Box::pin(futures_util::stream::once(async {
                Err(SqlExecutorError::Compile(error))
            })),
        }
    }
}
