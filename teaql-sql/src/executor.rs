use std::time::SystemTime;
use teaql_core::Record;
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
};

use crate::{CompiledQuery, SqlCompileError, SqlDialect};

pub trait SqlTransport: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn fetch_all_sql(&self, query: &CompiledQuery) -> Result<Vec<Record>, Self::Error>;
    fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error>;
}

pub trait SqlTransactionTransport: SqlTransport {
    type Tx<'a>: SqlTransport<Error = Self::Error> + SqlTransaction<Error = Self::Error>
    where
        Self: 'a;

    fn begin_sql(&self) -> Result<Self::Tx<'_>, Self::Error>;
}

pub trait SqlTransaction {
    type Error: std::error::Error + Send + Sync + 'static;
    fn commit_sql(self) -> Result<(), Self::Error>;
    fn rollback_sql(self) -> Result<(), Self::Error>;
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
    fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
        let entity_desc = self.schema_provider.get_entity(&request.query.entity)
            .ok_or_else(|| SqlExecutorError::Compile(SqlCompileError::UnknownEntity(request.query.entity.clone())))?;

        let compiled = self.dialect.compile_select(&entity_desc, &request.query).map_err(SqlExecutorError::Compile)?;
        let start = SystemTime::now();
        let rows = self.transport.fetch_all_sql(&compiled).map_err(SqlExecutorError::Transport)?;
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
        };

        Ok(QueryResult { rows, metadata })
    }
}

impl<D: SqlDialect + Send + Sync, T: SqlTransport + Send + Sync, S: teaql_data_service::SchemaProvider + Send + Sync> MutationExecutor
    for SqlDataServiceExecutor<D, T, S>
{
    fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
        let entity_name = match &request {
            MutationRequest::Insert(cmd) => &cmd.entity,
            MutationRequest::Update(cmd) => &cmd.entity,
            MutationRequest::Delete(cmd) => &cmd.entity,
            MutationRequest::Recover(cmd) => &cmd.entity,
            MutationRequest::Batch(_) => return Err(SqlExecutorError::Compile(SqlCompileError::UnknownEntity("Batch".to_string()))),
        };
        
        let entity_desc = self.schema_provider.get_entity(entity_name)
            .ok_or_else(|| SqlExecutorError::Compile(SqlCompileError::UnknownEntity(entity_name.clone())))?;

        let compiled = match &request {
            MutationRequest::Insert(cmd) => self.dialect.compile_insert(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Update(cmd) => self.dialect.compile_update(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Delete(cmd) => self.dialect.compile_delete(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Recover(cmd) => self.dialect.compile_recover(&entity_desc, cmd).map_err(SqlExecutorError::Compile)?,
            MutationRequest::Batch(_) => return Err(SqlExecutorError::Compile(SqlCompileError::UnknownEntity("Batch".to_string()))),
        };

        let start = SystemTime::now();
        let affected_rows = self.transport.execute_sql(&compiled).map_err(SqlExecutorError::Transport)?;
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
            trace_chain: Vec::new(), 
            comment: None,
            backend_request_id: None,
        };

        Ok(MutationResult {
            affected_rows,
            generated_values: Record::default(),
            metadata,
        })
    }
}
