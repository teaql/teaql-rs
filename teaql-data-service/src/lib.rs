#![allow(async_fn_in_trait)]

use std::time::SystemTime;
use teaql_core::{
    CompactRow, DeleteCommand, InsertCommand, Record, RecoverCommand, SelectQuery, TraceNode,
    UpdateCommand,
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
    /// Build a copy-paste representation with bind values interpolated.
    /// Runtimes should disable this when query logging is disabled.
    pub capture_debug_query: bool,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<CompactRow>,
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
    pub persisted_record: Option<Record>,
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
    /// Provider-native parameterized request text. For SQL this contains
    /// placeholders and never interpolated bind values.
    pub parameterized_query: Option<String>,
    /// Structured bind values corresponding to `parameterized_query`.
    pub params: Vec<teaql_core::Value>,
    pub debug_query: Option<String>,
}

pub trait DataServiceExecutor {
    type Error: std::error::Error + Send + Sync + 'static;

    fn capabilities(&self) -> DataServiceCapabilities;
}

pub trait QueryExecutor: DataServiceExecutor {
    fn query(
        &self,
        request: QueryRequest,
    ) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send;
}

/// Result of a single streaming chunk.
#[derive(Debug, Clone)]
pub struct StreamChunk {
    pub rows: Vec<CompactRow>,
    pub chunk_index: usize,
    pub is_last: bool,
}

pub type QueryStream<'a, E> =
    std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<StreamChunk, E>> + 'a>>;

/// Streaming query executor. Returns rows in chunks rather than all at once.
pub trait StreamQueryExecutor: DataServiceExecutor {
    fn query_stream(
        &self,
        request: QueryRequest,
        chunk_size: usize,
    ) -> QueryStream<'_, Self::Error>;
}

pub trait MutationExecutor: DataServiceExecutor {
    fn mutate(
        &self,
        request: MutationRequest,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send;
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
    fn ensure_schema(
        &self,
        request: SchemaRequest,
    ) -> impl std::future::Future<Output = Result<SchemaResult, Self::Error>> + Send;
}

pub trait IdGeneratorExecutor: DataServiceExecutor {
    fn next_id(
        &self,
        entity: &str,
    ) -> impl std::future::Future<Output = Result<u64, Self::Error>> + Send;
}

pub trait SchemaProvider: Send + Sync {
    fn get_entity(&self, name: &str) -> Option<std::sync::Arc<teaql_core::EntityDescriptor>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mutation_request_trace_and_comment_accessors() {
        let trace1 = TraceNode {
            entity_type: "User".to_string(),
            entity_id: Some(1),
            comment: "Create User".to_string(),
        };
        let trace2 = TraceNode {
            entity_type: "Profile".to_string(),
            entity_id: None,
            comment: "Create Profile".to_string(),
        };
        let trace_chain = vec![trace1.clone(), trace2.clone()];

        // Test Insert
        let insert_cmd = InsertCommand {
            entity: "User".to_string(),
            values: Record::new().into(),
            trace_chain: trace_chain.clone(),
        };
        let req_insert = MutationRequest::Insert(insert_cmd);
        assert_eq!(req_insert.trace_chain().len(), 2);
        assert_eq!(req_insert.trace_chain()[1], trace2);
        assert_eq!(req_insert.comment(), Some("Create Profile"));

        // Test Update
        let update_cmd = UpdateCommand {
            entity: "User".to_string(),
            id: teaql_core::Value::I64(1),
            values: Record::new().into(),
            expected_version: None,
            old_values: None,
            trace_chain: trace_chain.clone(),
        };
        let req_update = MutationRequest::Update(update_cmd);
        assert_eq!(req_update.trace_chain().len(), 2);
        assert_eq!(req_update.comment(), Some("Create Profile"));

        // Test Delete
        let delete_cmd = DeleteCommand {
            entity: "User".to_string(),
            id: teaql_core::Value::I64(1),
            expected_version: None,
            soft_delete: true,
            trace_chain: trace_chain.clone(),
        };
        let req_delete = MutationRequest::Delete(delete_cmd);
        assert_eq!(req_delete.trace_chain().len(), 2);
        assert_eq!(req_delete.comment(), Some("Create Profile"));

        // Test Recover
        let recover_cmd = RecoverCommand {
            entity: "User".to_string(),
            id: teaql_core::Value::I64(1),
            expected_version: 1,
            trace_chain: trace_chain.clone(),
        };
        let req_recover = MutationRequest::Recover(recover_cmd);
        assert_eq!(req_recover.trace_chain().len(), 2);
        assert_eq!(req_recover.comment(), Some("Create Profile"));

        // Test Batch
        let req_batch = MutationRequest::Batch(vec![req_insert, req_update]);
        assert_eq!(req_batch.trace_chain().len(), 0);
        assert_eq!(req_batch.comment(), None);

        // Test empty trace chain
        let insert_empty = InsertCommand {
            entity: "User".to_string(),
            values: Record::new().into(),
            trace_chain: vec![],
        };
        let req_empty = MutationRequest::Insert(insert_empty);
        assert_eq!(req_empty.trace_chain().len(), 0);
        assert_eq!(req_empty.comment(), None);
    }
}
