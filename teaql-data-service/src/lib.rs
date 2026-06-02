use std::time::SystemTime;
use teaql_core::{
    DeleteCommand, InsertCommand, RecoverCommand, UpdateCommand, SelectQuery, TraceNode, Record,
};

#[derive(Debug, Clone, Default)]
pub struct DataServiceCapabilities {
    pub query: bool,
    pub mutation: bool,
    pub transaction: bool,
    pub schema: bool,
    pub id_generation: bool,
    pub batch_mutation: bool,
    pub returning: bool,
}

#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub query: SelectQuery,
    pub trace_chain: Vec<TraceNode>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<Record>,
    pub metadata: ExecutionMetadata,
}

#[derive(Debug, Clone)]
pub enum MutationRequest {
    Insert(InsertCommand),
    Update(UpdateCommand),
    Delete(DeleteCommand),
    Recover(RecoverCommand),
    Batch(Vec<MutationRequest>),
}

#[derive(Debug, Clone)]
pub struct MutationResult {
    pub affected_rows: u64,
    pub generated_values: Record,
    pub metadata: ExecutionMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataServiceOperation {
    Query,
    Insert,
    Update,
    Delete,
    Recover,
    Batch,
    Schema,
}

#[derive(Debug, Clone)]
pub struct ExecutionMetadata {
    pub backend: String,
    pub operation: DataServiceOperation,
    pub started_at: SystemTime,
    pub ended_at: SystemTime,
    pub affected_rows: Option<u64>,
    pub result_count: Option<usize>,
    pub trace_chain: Vec<TraceNode>,
    pub comment: Option<String>,
    pub backend_request_id: Option<String>,
}

pub trait DataServiceExecutor {
    type Error: std::error::Error + Send + Sync + 'static;

    fn capabilities(&self) -> DataServiceCapabilities;
}

pub trait QueryExecutor: DataServiceExecutor {
    fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error>;
}

pub trait MutationExecutor: DataServiceExecutor {
    fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error>;
}

pub trait TransactionExecutor: DataServiceExecutor {
    type Tx<'a>: QueryExecutor<Error = Self::Error>
        + MutationExecutor<Error = Self::Error>
        + Transaction<Error = Self::Error>
    where
        Self: 'a;

    fn begin(&self) -> Result<Self::Tx<'_>, Self::Error>;
}

pub trait Transaction {
    type Error: std::error::Error + Send + Sync + 'static;

    fn commit(self) -> Result<(), Self::Error>;
    fn rollback(self) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone)]
pub struct SchemaRequest {
    pub entity_name: String,
}

#[derive(Debug, Clone)]
pub struct SchemaResult {
    pub changed: bool,
}

pub trait SchemaExecutor: DataServiceExecutor {
    fn ensure_schema(&self, request: SchemaRequest) -> Result<SchemaResult, Self::Error>;
}

pub trait IdGeneratorExecutor: DataServiceExecutor {
    fn next_id(&self, entity: &str) -> Result<u64, Self::Error>;
}

pub trait SchemaProvider: Send + Sync {
    fn get_entity(&self, name: &str) -> Option<std::sync::Arc<teaql_core::EntityDescriptor>>;
}


