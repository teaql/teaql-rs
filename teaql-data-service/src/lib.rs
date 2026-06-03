#![allow(async_fn_in_trait)]

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

impl MutationRequest {
    pub fn trace_chain(&self) -> &[teaql_core::TraceNode] {
        match self {
            MutationRequest::Insert(cmd) => &cmd.trace_chain,
            MutationRequest::Update(cmd) => &cmd.trace_chain,
            MutationRequest::Delete(cmd) => &cmd.trace_chain,
            MutationRequest::Recover(cmd) => &cmd.trace_chain,
            MutationRequest::Batch(_) => &[], // Batch traces are per-item
        }
    }

    pub fn comment(&self) -> Option<&str> {
        match self {
            MutationRequest::Insert(cmd) => cmd.trace_chain.last().map(|n| n.comment.as_str()),
            MutationRequest::Update(cmd) => cmd.trace_chain.last().map(|n| n.comment.as_str()),
            MutationRequest::Delete(cmd) => cmd.trace_chain.last().map(|n| n.comment.as_str()),
            MutationRequest::Recover(cmd) => cmd.trace_chain.last().map(|n| n.comment.as_str()),
            MutationRequest::Batch(_) => None,
        }
    }
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
    pub debug_query: Option<String>,
}

pub trait DataServiceExecutor {
    type Error: std::error::Error + Send + Sync + 'static;

    fn capabilities(&self) -> DataServiceCapabilities;
}

pub trait QueryExecutor: DataServiceExecutor {
    fn query(&self, request: QueryRequest) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send;
}

pub trait MutationExecutor: DataServiceExecutor {
    fn mutate(&self, request: MutationRequest) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send;
}

pub trait TransactionExecutor: DataServiceExecutor {
    type Tx<'a>: QueryExecutor<Error = Self::Error>
        + MutationExecutor<Error = Self::Error>
        + Transaction<Error = Self::Error>
    where
        Self: 'a;

    fn begin(&self) -> impl std::future::Future<Output = Result<Self::Tx<'_>, Self::Error>> + Send;
}

pub trait Transaction {
    type Error: std::error::Error + Send + Sync + 'static;

    fn commit(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
    fn rollback(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
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
    fn ensure_schema(&self, request: SchemaRequest) -> impl std::future::Future<Output = Result<SchemaResult, Self::Error>> + Send;
}

pub trait IdGeneratorExecutor: DataServiceExecutor {
    fn next_id(&self, entity: &str) -> impl std::future::Future<Output = Result<u64, Self::Error>> + Send;
}

pub trait SchemaProvider: Send + Sync {
    fn get_entity(&self, name: &str) -> Option<std::sync::Arc<teaql_core::EntityDescriptor>>;
}
