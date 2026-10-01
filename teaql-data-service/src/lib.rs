#![allow(async_fn_in_trait)]

mod sql_log;
pub use sql_log::{
    SqlExecutionOutcome, SqlIntentRedactions, SqlLogContext, SqlParameterLogPolicy,
    SqlProjectionState, is_credential_log_name,
};

use std::time::SystemTime;
use teaql_core::{
    CompactRow, DeleteCommand, EntitySnapshot, Expr, GeneratedValues, InsertCommand,
    MutationIntent, QueryIntent, RecoverCommand, RequestIntentError, SelectQuery, TraceNode,
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
    /// A small number of per-parent indexed relation queries is preferable to
    /// a partitioned batch query for this provider (for example embedded SQLite).
    pub small_parent_relation_probes: bool,
}

#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub query: SelectQuery,
    pub trace_chain: Vec<TraceNode>,
    /// Required, validated intent, independent of trace frames and logging flags.
    pub intent: QueryIntent,
    /// Request a diagnostic SQL representation. SQL executors retain bindings;
    /// the runtime must project their privacy policies before interpolation.
    /// This flag does not authorize plaintext rendering in an executor.
    pub capture_debug_query: bool,
    /// Retain timings, parameterized text, bind values, trace and comment in the result.
    /// Disable only when the caller will discard execution metadata.
    pub capture_execution_metadata: bool,
}

impl QueryRequest {
    pub fn new(query: SelectQuery, intent: QueryIntent) -> Self {
        Self {
            trace_chain: query.trace_chain.clone(),
            query,
            intent,
            capture_debug_query: false,
            capture_execution_metadata: true,
        }
    }

    pub fn from_query(query: SelectQuery) -> Result<Self, RequestIntentError> {
        let intent =
            QueryIntent::from_optional(query.comment.as_deref(), query.purpose.as_deref())?;
        Ok(Self::new(query, intent))
    }
    pub fn comment(&self) -> &str {
        self.intent.comment()
    }
    pub fn purpose(&self) -> &str {
        self.intent.purpose()
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<CompactRow>,
    pub metadata: ExecutionMetadata,
}

#[derive(Debug, Clone)]
pub enum MutationCommand {
    Insert(InsertCommand),
    Update(UpdateCommand),
    Delete(DeleteCommand),
    Recover(RecoverCommand),
    Batch(Vec<MutationRequest>),
}

impl MutationCommand {
    pub fn request(
        self,
        comment: impl Into<String>,
    ) -> Result<MutationRequest, RequestIntentError> {
        MutationRequest::new(self, comment)
    }
}

/// A complete mutation envelope. Commands cannot reach an executor without an
/// explicit validated root comment, including a batch of annotated children.
#[derive(Debug, Clone)]
pub struct MutationRequest {
    pub command: MutationCommand,
    intent: MutationIntent,
}

impl MutationRequest {
    pub fn new(
        command: MutationCommand,
        comment: impl Into<String>,
    ) -> Result<Self, RequestIntentError> {
        Ok(Self::with_intent(command, MutationIntent::new(comment)?))
    }

    pub fn with_intent(command: MutationCommand, intent: MutationIntent) -> Self {
        Self { command, intent }
    }

    pub fn intent(&self) -> &MutationIntent {
        &self.intent
    }

    pub fn trace_chain(&self) -> &[teaql_core::TraceNode] {
        match &self.command {
            MutationCommand::Insert(cmd) => &cmd.trace_chain,
            MutationCommand::Update(cmd) => &cmd.trace_chain,
            MutationCommand::Delete(cmd) => &cmd.trace_chain,
            MutationCommand::Recover(cmd) => &cmd.trace_chain,
            MutationCommand::Batch(_) => &[], // Batch traces are per-item
        }
    }

    pub fn comment(&self) -> &str {
        self.intent.comment()
    }
}

#[derive(Debug, Clone)]
pub struct MutationResult {
    pub affected_rows: u64,
    pub generated_values: GeneratedValues,
    pub persisted_snapshot: Option<EntitySnapshot>,
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
    /// Batch statements keep independent binding positions and policies.
    pub statements: Vec<ExecutionMetadata>,
    pub sql_log: SqlLogContext,
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

impl ExecutionMetadata {
    pub fn unrecorded_query(result_count: usize) -> Self {
        Self {
            statements: Vec::new(),
            sql_log: SqlLogContext::default(),
            backend: String::new(),
            operation: DataServiceOperation::Query,
            started_at: SystemTime::UNIX_EPOCH,
            ended_at: SystemTime::UNIX_EPOCH,
            affected_rows: None,
            result_count: Some(result_count),
            trace_chain: Vec::new(),
            comment: None,
            backend_request_id: None,
            parameterized_query: None,
            params: Vec::new(),
            debug_query: None,
        }
    }
}

pub trait DataServiceExecutor {
    type Error: std::error::Error + Send + Sync + 'static;

    fn capabilities(&self) -> DataServiceCapabilities;
}

pub trait QueryExecutor: DataServiceExecutor {
    /// Provider diagnostic SPI for a cached/rewritten query whose original
    /// statement will not execute. Must not perform I/O or emit a SQL log.
    /// Providers can derive precise policies from compilation. The default
    /// treats query literals as unknown, including in explicit debug mode.
    #[doc(hidden)]
    fn query_log_intent(&self, query: &SelectQuery) -> SqlIntentRedactions {
        SqlIntentRedactions::from_unclassified_query(query)
    }

    fn query(
        &self,
        request: QueryRequest,
    ) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send;

    /// Runtime diagnostic SPI. Success metadata remains in QueryResult; the
    /// observer receives only failures/cancellation that cannot return a result.
    #[doc(hidden)]
    fn query_observed<'a>(
        &'a self,
        request: QueryRequest,
        _observer: Option<ExecutionObserver<'a>>,
    ) -> impl std::future::Future<Output = Result<QueryResult, Self::Error>> + Send + 'a {
        self.query(request)
    }
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

/// Provider SPI: runtime-owned diagnostic projection, never part of a serialized request.
#[doc(hidden)]
pub type ExecutionObserver<'a> = std::sync::Arc<dyn Fn(ExecutionMetadata) + Send + Sync + 'a>;

/// Streaming query executor. Returns rows in chunks rather than all at once.
pub trait StreamQueryExecutor: DataServiceExecutor {
    fn query_stream(
        &self,
        request: QueryRequest,
        chunk_size: usize,
    ) -> QueryStream<'_, Self::Error>;

    /// Optional provider diagnostic hook. The default preserves third-party executors;
    /// it cannot invent SQL or claim diagnostics for an executor that supplies none.
    #[doc(hidden)]
    fn query_stream_observed<'a>(
        &'a self,
        request: QueryRequest,
        chunk_size: usize,
        _observer: ExecutionObserver<'a>,
    ) -> QueryStream<'a, Self::Error> {
        self.query_stream(request, chunk_size)
    }
}

pub trait MutationExecutor: DataServiceExecutor {
    fn mutate(
        &self,
        request: MutationRequest,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send;

    /// Runtime-only diagnostic SPI for results lost to failure or cancellation.
    #[doc(hidden)]
    fn mutate_observed<'a>(
        &'a self,
        request: MutationRequest,
        _observer: Option<ExecutionObserver<'a>>,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send + 'a {
        self.mutate(request)
    }
}

/// A mutation whose target must also satisfy a trusted, datastore-enforced guard.
///
/// The guard is deliberately kept outside [`MutationRequest`]: ordinary domain
/// mutations do not own authorization policy, while boundary adapters such as a
/// federated endpoint must be able to require one atomic mutation predicate.
#[derive(Debug, Clone)]
pub struct GuardedMutationRequest {
    pub mutation: MutationRequest,
    pub guard: Expr,
}

impl GuardedMutationRequest {
    pub fn new(mutation: MutationRequest, guard: Expr) -> Self {
        Self { mutation, guard }
    }
}

/// Executes a mutation only when its target also satisfies a trusted guard.
///
/// Implementations must apply the guard atomically in the same datastore
/// statement as the mutation. A separate read-before-write check does not
/// satisfy this contract.
pub trait GuardedMutationExecutor: MutationExecutor {
    fn mutate_guarded(
        &self,
        request: GuardedMutationRequest,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send;

    #[doc(hidden)]
    fn mutate_guarded_observed<'a>(
        &'a self,
        request: GuardedMutationRequest,
        _observer: Option<ExecutionObserver<'a>>,
    ) -> impl std::future::Future<Output = Result<MutationResult, Self::Error>> + Send + 'a {
        self.mutate_guarded(request)
    }
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
    fn mutation_comment_is_owned_and_never_derived_from_trace_tail() {
        let trace = TraceNode::typed(teaql_core::TraceKind::Entity, "User", Some(1), "");
        let commands = [
            MutationCommand::Insert(InsertCommand {
                entity: "User".into(),
                values: Default::default(),
                trace_chain: vec![trace.clone()],
            }),
            MutationCommand::Update(UpdateCommand::new("User", 1_u64)),
            MutationCommand::Delete(DeleteCommand::new("User", 1_u64)),
            MutationCommand::Recover(RecoverCommand::new("User", 1_u64, -1)),
        ];
        for command in commands {
            let request = command.request("review user changes").unwrap();
            assert_eq!(request.comment(), "review user changes");
        }
    }

    #[test]
    fn annotated_children_do_not_supply_a_batch_root_comment() {
        let child = MutationCommand::Insert(InsertCommand::new("User"))
            .request("create user")
            .unwrap();
        for comment in ["", " \t\n", "\u{2003}"] {
            let error = MutationCommand::Batch(vec![child.clone()])
                .request(comment)
                .unwrap_err();
            assert_eq!(error.code(), "REQUEST_COMMENT_REQUIRED");
        }
        let batch = MutationCommand::Batch(vec![child])
            .request("import reviewed users")
            .unwrap();
        assert_eq!(batch.comment(), "import reviewed users");
    }

    #[test]
    fn trace_comment_cannot_supply_missing_request_comment_or_purpose() {
        let mut query = SelectQuery::new("User");
        query.trace_chain.push(TraceNode::typed(
            teaql_core::TraceKind::Purpose,
            "User",
            None,
            "fabricated purpose",
        ));
        assert_eq!(
            QueryRequest::from_query(query.clone()).unwrap_err().code(),
            "REQUEST_COMMENT_REQUIRED"
        );
        query.comment = Some("load user".into());
        assert_eq!(
            QueryRequest::from_query(query.clone()).unwrap_err().code(),
            "QUERY_PURPOSE_REQUIRED"
        );
        query.purpose = Some("render user".into());
        let mut request = QueryRequest::from_query(query).unwrap();
        request.capture_execution_metadata = false;
        request.capture_debug_query = false;
        assert_eq!(request.comment(), "load user");
        assert_eq!(request.purpose(), "render user");
    }
}
