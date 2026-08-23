use std::collections::HashMap;
use std::time::SystemTime;

use teaql_core::Record;
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, MutationExecutor, MutationRequest,
    MutationResult, QueryExecutor, QueryRequest, QueryResult, QueryStream, StreamQueryExecutor,
    Transaction, TransactionExecutor,
};
use teaql_runtime::InMemoryQueryEngine;

use crate::collector::{Collector, ProcessCollector, SystemInfoCollector, ThreadCollector};
use crate::error::LinuxProviderError;

/// A `DataServiceExecutor` backed by Linux /proc collectors.
///
/// Routes queries to the appropriate collector by entity name, collects all records,
/// then delegates in-memory query processing (filter, sort, project, aggregate) to
/// `InMemoryQueryEngine`.
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
        collectors.insert("SystemInfo".to_owned(), Box::new(SystemInfoCollector));
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

        let mut result = InMemoryQueryEngine::execute(&request.query, rows);
        result.metadata.backend = "linux-proc".to_owned();
        result.metadata.started_at = started_at;
        result.metadata.ended_at = SystemTime::now();
        result.metadata.trace_chain = request.trace_chain;
        result.metadata.comment = request.comment;
        Ok(result)
    }
}

impl MutationExecutor for LinuxDataServiceExecutor {
    async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
        Err(LinuxProviderError::ProcFs(
            "Linux provider is read-only".to_owned(),
        ))
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
        Err(LinuxProviderError::ProcFs(
            "Linux provider does not support transactions".to_owned(),
        ))
    }
}

impl StreamQueryExecutor for LinuxDataServiceExecutor {
    fn query_stream(
        &self,
        _request: QueryRequest,
        _chunk_size: usize,
    ) -> QueryStream<'_, Self::Error> {
        Box::pin(futures_util::stream::once(async {
            Err(LinuxProviderError::ProcFs(
                "Linux provider does not support streaming".to_owned(),
            ))
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use teaql_core::{Aggregate, Expr, OrderBy, SelectQuery, TraceNode, Value};
    use teaql_data_service::DataServiceOperation;

    struct StaticCollector {
        rows: Vec<Record>,
    }

    impl Collector for StaticCollector {
        fn entity_name(&self) -> &str {
            "Fixture"
        }

        fn collect_all(&self) -> Result<Vec<Record>, LinuxProviderError> {
            Ok(self.rows.clone())
        }
    }

    fn row(name: &str, score: i64) -> Record {
        [
            ("name".to_owned(), Value::Text(name.to_owned())),
            ("score".to_owned(), Value::I64(score)),
        ]
        .into_iter()
        .collect()
    }

    fn executor() -> LinuxDataServiceExecutor {
        LinuxDataServiceExecutor::new().with_collector(Box::new(StaticCollector {
            rows: vec![row("low", 5), row("middle", 15), row("high", 25)],
        }))
    }

    #[tokio::test]
    async fn query_processes_rows_and_preserves_request_metadata() {
        let trace = TraceNode::new("Fixture", None, "Inspect fixture rows");
        let request = QueryRequest {
            query: SelectQuery::new("Fixture")
                .filter(Expr::gt("score", 10_i64))
                .order_by(OrderBy::desc("score"))
                .page(1, 1)
                .projects(["name"]),
            trace_chain: vec![trace.clone()],
            comment: Some("Load the second matching fixture".to_owned()),
            capture_debug_query: true,
        };

        let result = executor().query(request).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("name"),
            Some(&Value::Text("middle".to_owned()))
        );
        assert_eq!(result.rows[0].len(), 1);
        assert_eq!(result.metadata.backend, "linux-proc");
        assert_eq!(result.metadata.operation, DataServiceOperation::Query);
        assert_eq!(result.metadata.result_count, Some(1));
        assert_eq!(result.metadata.trace_chain, vec![trace]);
        assert_eq!(
            result.metadata.comment.as_deref(),
            Some("Load the second matching fixture")
        );
        assert!(result.metadata.started_at <= result.metadata.ended_at);
    }

    #[tokio::test]
    async fn query_supports_aggregates() {
        let request = QueryRequest {
            query: SelectQuery::new("Fixture")
                .filter(Expr::gt("score", 10_i64))
                .aggregate(Aggregate::count("matching")),
            trace_chain: Vec::new(),
            comment: None,
            capture_debug_query: true,
        };

        let result = executor().query(request).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("matching"), Some(&Value::I64(2)));
        assert_eq!(result.metadata.result_count, Some(1));
    }
}
