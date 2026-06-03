#![allow(async_fn_in_trait)]

use std::time::SystemTime;
use teaql_core::Record;
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
};

use crate::{CompiledQuery, SqlCompileError, SqlDialect};

pub trait SqlTransport: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn fetch_all_sql(&self, query: &CompiledQuery) -> impl std::future::Future<Output = Result<Vec<Record>, Self::Error>> + Send;
    fn execute_sql(&self, query: &CompiledQuery) -> impl std::future::Future<Output = Result<u64, Self::Error>> + Send;
}

pub trait SqlTransactionTransport: SqlTransport {
    type Tx<'a>: SqlTransport<Error = Self::Error> + SqlTransaction<Error = Self::Error> + Send + Sync + 'a
    where
        Self: 'a;

    fn begin_sql(&self) -> impl std::future::Future<Output = Result<Self::Tx<'_>, Self::Error>> + Send;
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
}

impl<E: std::error::Error + Send + Sync + 'static> std::fmt::Display for SqlExecutorError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlExecutorError::Compile(e) => write!(f, "SQL compile error: {}", e),
            SqlExecutorError::Transport(e) => write!(f, "Transport error: {}", e),
        }
    }
}

impl<E: std::error::Error + Send + Sync + 'static> std::error::Error for SqlExecutorError<E> {}

#[derive(Clone)]
pub struct SqlDataServiceExecutor<D, T, S> {
    pub dialect: D,
    pub transport: T,
    pub schema_provider: S,
}

impl<D, T, S> SqlDataServiceExecutor<D, T, S> {
    pub fn new(dialect: D, transport: T, schema_provider: S) -> Self {
        Self { dialect, transport, schema_provider }
    }
}

impl<D: SqlDialect + Send + Sync, T: SqlTransport + Send + Sync, S: teaql_data_service::SchemaProvider + Send + Sync> DataServiceExecutor
    for SqlDataServiceExecutor<D, T, S>
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

impl<D: SqlDialect + Send + Sync, T: SqlTransport + Send + Sync, S: teaql_data_service::SchemaProvider + Send + Sync> QueryExecutor
    for SqlDataServiceExecutor<D, T, S>
{
    fn query(&self, request: QueryRequest) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send { async move {
        let entity_desc = self.schema_provider.get_entity(&request.query.entity)
            .ok_or_else(|| SqlExecutorError::Compile(SqlCompileError::UnknownEntity(request.query.entity.clone())))?;

        let compiled = self.dialect.compile_select(&entity_desc, &request.query).map_err(SqlExecutorError::Compile)?;
        let start = SystemTime::now();
        let rows = self.transport.fetch_all_sql(&compiled).await.map_err(SqlExecutorError::Transport)?;
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
            debug_query: Some(compiled.debug_sql(self.dialect.kind())),
        };

        Ok(QueryResult { rows, metadata })
    } }
}

impl<D: SqlDialect + Send + Sync, T: SqlTransport + Send + Sync, S: teaql_data_service::SchemaProvider + Send + Sync> MutationExecutor
    for SqlDataServiceExecutor<D, T, S>
{
    fn mutate(&self, request: MutationRequest) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send { async move {
        let entity_name = match &request {
            MutationRequest::Insert(cmd) => &cmd.entity,
            MutationRequest::Update(cmd) => &cmd.entity,
            MutationRequest::Delete(cmd) => &cmd.entity,
            MutationRequest::Recover(cmd) => &cmd.entity,
            MutationRequest::Batch(mutations) => {
                let mut total_affected = 0;
                let start = SystemTime::now();
                for req in mutations {
                    let res = Box::pin(self.mutate(req.clone())).await?;
                    total_affected += res.affected_rows;
                }
                let end = SystemTime::now();
                return Ok(MutationResult {
                    affected_rows: total_affected,
                    generated_values: Record::default(),
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
                        debug_query: None,
                    },
                });
            }
        };
        
        let entity_desc = self.schema_provider.get_entity(entity_name)
            .ok_or_else(|| SqlExecutorError::Compile(SqlCompileError::UnknownEntity(entity_name.clone())))?;

        let compiled = match &request {
            MutationRequest::Insert(cmd) => self.dialect.compile_insert(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Update(cmd) => self.dialect.compile_update(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Delete(cmd) => self.dialect.compile_delete(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Recover(cmd) => self.dialect.compile_recover(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Batch(_) => unreachable!(),
        };

        let start = SystemTime::now();
        let affected_rows = self.transport.execute_sql(&compiled).await.map_err(SqlExecutorError::Transport)?;
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
            debug_query: Some(compiled.debug_sql(self.dialect.kind())),
        };

        Ok(MutationResult {
            affected_rows,
            generated_values: Record::default(),
            metadata,
        })
    } }
}

#[derive(Clone)]
pub struct SqlDataServiceTransaction<'a, D, Tx: SqlTransport + SqlTransaction, S> {
    pub dialect: &'a D,
    pub transport: Tx,
    pub schema_provider: &'a S,
}

impl<'a, D: SqlDialect + Send + Sync, Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync, S: teaql_data_service::SchemaProvider + Send + Sync> DataServiceExecutor
    for SqlDataServiceTransaction<'a, D, Tx, S>
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

impl<'a, D: SqlDialect + Send + Sync, Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync, S: teaql_data_service::SchemaProvider + Send + Sync> QueryExecutor
    for SqlDataServiceTransaction<'a, D, Tx, S>
{
    fn query(&self, request: QueryRequest) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send { async move {
        let entity_desc = self.schema_provider.get_entity(&request.query.entity)
            .ok_or_else(|| SqlExecutorError::Compile(SqlCompileError::UnknownEntity(request.query.entity.clone())))?;

        let compiled = self.dialect.compile_select(&entity_desc, &request.query).map_err(SqlExecutorError::Compile)?;
        let start = SystemTime::now();
        let rows = self.transport.fetch_all_sql(&compiled).await.map_err(SqlExecutorError::Transport)?;
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
            debug_query: Some(compiled.debug_sql(self.dialect.kind())),
        };

        Ok(QueryResult { rows, metadata })
    } }
}

impl<'a, D: SqlDialect + Send + Sync, Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync, S: teaql_data_service::SchemaProvider + Send + Sync> MutationExecutor
    for SqlDataServiceTransaction<'a, D, Tx, S>
{
    fn mutate(&self, request: MutationRequest) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send { async move {
        let entity_name = match &request {
            MutationRequest::Insert(cmd) => &cmd.entity,
            MutationRequest::Update(cmd) => &cmd.entity,
            MutationRequest::Delete(cmd) => &cmd.entity,
            MutationRequest::Recover(cmd) => &cmd.entity,
            MutationRequest::Batch(_) => unreachable!(), // Similar to above, implement properly if needed
        };
        
        let entity_desc = self.schema_provider.get_entity(entity_name)
            .ok_or_else(|| SqlExecutorError::Compile(SqlCompileError::UnknownEntity(entity_name.clone())))?;

        let compiled = match &request {
            MutationRequest::Insert(cmd) => self.dialect.compile_insert(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Update(cmd) => self.dialect.compile_update(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Delete(cmd) => self.dialect.compile_delete(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Recover(cmd) => self.dialect.compile_recover(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Batch(_) => unreachable!(),
        };

        let start = SystemTime::now();
        let affected_rows = self.transport.execute_sql(&compiled).await.map_err(SqlExecutorError::Transport)?;
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
            debug_query: Some(compiled.debug_sql(self.dialect.kind())),
        };

        Ok(MutationResult {
            affected_rows,
            generated_values: Record::default(),
            metadata,
        })
    } }
}

impl<'a, D: SqlDialect + Send + Sync, Tx: SqlTransport + SqlTransaction<Error = <Tx as SqlTransport>::Error> + Send + Sync, S: teaql_data_service::SchemaProvider + Send + Sync> teaql_data_service::Transaction
    for SqlDataServiceTransaction<'a, D, Tx, S>
{
    type Error = SqlExecutorError<<Tx as SqlTransport>::Error>;

    fn commit(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send { async move {
        self.transport.commit_sql().await.map_err(SqlExecutorError::Transport)
    } }

    fn rollback(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send { async move {
        self.transport.rollback_sql().await.map_err(SqlExecutorError::Transport)
    } }
}

impl<D: SqlDialect + Send + Sync, T: SqlTransactionTransport + Send + Sync, S: teaql_data_service::SchemaProvider + Send + Sync> teaql_data_service::TransactionExecutor
    for SqlDataServiceExecutor<D, T, S>
{
    type Tx<'a> = SqlDataServiceTransaction<'a, D, T::Tx<'a>, S> where Self: 'a;

    fn begin(&self) -> impl std::future::Future<Output = Result<Self::Tx<'_>, Self::Error>> + Send { async move {
        let tx = self.transport.begin_sql().await.map_err(SqlExecutorError::Transport)?;
        Ok(SqlDataServiceTransaction {
            dialect: &self.dialect,
            transport: tx,
            schema_provider: &self.schema_provider,
        })
    } }
}
