use std::collections::HashMap;
use std::time::SystemTime;

use teaql_core::{EntityDescriptor, Record};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
    StreamChunk, StreamQueryExecutor, Transaction, TransactionExecutor,
};
use teaql_runtime::{InMemoryMetadataStore, MemoryDataService};

use crate::collector::{Collector, ProcessCollector, SystemInfoCollector, ThreadCollector};
use crate::error::LinuxProviderError;

/// A `DataServiceExecutor` backed by Linux /proc collectors.
///
/// Routes queries to the appropriate collector by entity name, collects all records,
/// then delegates in-memory query processing (filter, sort, project, aggregate) to
/// `MemoryDataService`.
pub struct LinuxDataServiceExecutor {
    collectors: HashMap<String, Box<dyn Collector>>,
}

impl Default for LinuxDataServiceExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl LinuxDataServiceExecutor {
    /// Create a new executor with the default set of collectors.
    pub fn new() -> Self {
        let mut collectors: HashMap<String, Box<dyn Collector>> = HashMap::new();
        collectors.insert(
            "SystemInfo".to_owned(),
            Box::new(SystemInfoCollector),
        );
        collectors.insert("Process".to_owned(), Box::new(ProcessCollector));
        collectors.insert("Thread".to_owned(), Box::new(ThreadCollector));
        Self { collectors }
    }

    /// Register an additional collector.
    pub fn with_collector(mut self, collector: Box<dyn Collector>) -> Self {
        self.collectors
            .insert(collector.entity_name().to_owned(), collector);
        self
    }

    fn collect_records(&self, entity: &str) -> Result<Vec<Record>, LinuxProviderError> {
        let collector = self
            .collectors
            .get(entity)
            .ok_or_else(|| LinuxProviderError::UnknownEntity(entity.to_owned()))?;
        collector.collect_all()
    }
}

impl DataServiceExecutor for LinuxDataServiceExecutor {
    type Error = LinuxProviderError;

    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceCapabilities {
            query: true,
            mutation: false,
            transaction: false,
            schema: false,
            id_generation: false,
            batch_mutation: false,
            returning: false,
        }
    }
}

impl QueryExecutor for LinuxDataServiceExecutor {
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
        let started_at = SystemTime::now();
        let entity = &request.query.entity;

        // Collect raw records from the matching collector.
        let rows = self.collect_records(entity)?;

        // Build a MemoryDataService with the entity registered so fetch_all succeeds.
        let metadata = InMemoryMetadataStore::new()
            .with_entity(EntityDescriptor::new(entity));
        let mut mem = MemoryDataService::new(metadata);
        mem.seed(entity.clone(), rows);

        let result_rows = mem
            .fetch_all(&request.query)
            .map_err(|e| LinuxProviderError::ProcFs(e.to_string()))?;

        let ended_at = SystemTime::now();
        let count = result_rows.len();

        Ok(QueryResult {
            rows: result_rows,
            metadata: ExecutionMetadata {
                backend: "linux-proc".to_owned(),
                operation: DataServiceOperation::Query,
                started_at,
                ended_at,
                affected_rows: None,
                result_count: Some(count),
                trace_chain: request.trace_chain,
                comment: request.comment,
                backend_request_id: None,
                debug_query: None,
            },
        })
    }
}

impl MutationExecutor for LinuxDataServiceExecutor {
    async fn mutate(
        &self,
        _request: MutationRequest,
    ) -> Result<MutationResult, Self::Error> {
        Err(LinuxProviderError::ProcFs("Linux provider is read-only".to_owned()))
    }
}

impl Transaction for LinuxDataServiceExecutor {
    type Error = LinuxProviderError;

    async fn commit(self) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn rollback(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl TransactionExecutor for LinuxDataServiceExecutor {
    type Tx<'a> = LinuxDataServiceExecutor;

    async fn begin(&self) -> Result<Self::Tx<'_>, Self::Error> {
        Err(LinuxProviderError::ProcFs("Linux provider does not support transactions".to_owned()))
    }
}

impl StreamQueryExecutor for LinuxDataServiceExecutor {
    async fn query_stream(&self, _request: QueryRequest, _chunk_size: usize) -> Result<Vec<StreamChunk>, Self::Error> {
        Err(LinuxProviderError::ProcFs("Linux provider does not support streaming".to_owned()))
    }
}
