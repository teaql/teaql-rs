use async_trait::async_trait;
use teaql_core::{EntityDescriptor, Record, Value};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
};

#[derive(Debug)]
pub struct MeilisearchError(pub String);

impl std::fmt::Display for MeilisearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Meilisearch Error: {}", self.0)
    }
}

impl std::error::Error for MeilisearchError {}

/// Meilisearch Provider for TeaQL.
/// Functions as a fully fledged DataService for specific entities.
pub struct MeilisearchProvider {
    client: reqwest::Client,
    host: String,
    api_key: Option<String>,
}

impl MeilisearchProvider {
    pub fn new(host: impl Into<String>, api_key: Option<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            host: host.into(),
            api_key,
        }
    }

    /// Setup the schema on Meilisearch based on the entity descriptor
    pub async fn sync_schema(&self, descriptor: &EntityDescriptor) -> Result<(), MeilisearchError> {
        // TODO: Map descriptor properties to filterableAttributes and sortableAttributes
        // TODO: Set primary key
        Ok(())
    }
}

impl DataServiceExecutor for MeilisearchProvider {
    type Error = MeilisearchError;

    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceCapabilities::default()
    }
}

impl QueryExecutor for MeilisearchProvider {
    async fn query(&self, _request: QueryRequest) -> Result<QueryResult, Self::Error> {
        let started_at = std::time::SystemTime::now();
        
        // TODO: Translate request.query (SelectQuery) into Meilisearch JSON
        // Handle search_for_text mapping to `q`
        // Handle filter mapping to Meilisearch filter string

        Ok(QueryResult {
            rows: Vec::new(),
            metadata: ExecutionMetadata {
                debug_query: Some("TODO: Meilisearch Query".to_owned()),
                backend: "meilisearch".to_owned(),
                operation: DataServiceOperation::Query,
                started_at,
                ended_at: std::time::SystemTime::now(),
                affected_rows: None,
                result_count: Some(0),
                trace_chain: Vec::new(),
                comment: None,
                backend_request_id: None,
            },
        })
    }
}

impl MutationExecutor for MeilisearchProvider {
    async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
        let started_at = std::time::SystemTime::now();

        // TODO: Translate request (InsertCommand / UpdateCommand / DeleteCommand)
        // Fire-and-forget logic: submit task to Meilisearch and return Ok()

        Ok(MutationResult {
            affected_rows: 0,
            generated_values: Record::new(),
            metadata: ExecutionMetadata {
                debug_query: Some("TODO: Meilisearch Mutation".to_owned()),
                backend: "meilisearch".to_owned(),
                operation: DataServiceOperation::Update,
                started_at,
                ended_at: std::time::SystemTime::now(),
                affected_rows: Some(0),
                result_count: None,
                trace_chain: Vec::new(),
                comment: None,
                backend_request_id: None,
            },
        })
    }
}
