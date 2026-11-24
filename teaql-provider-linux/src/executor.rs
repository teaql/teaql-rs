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
            small_parent_relation_probes: false,
        }
    }
}

impl QueryExecutor for LinuxDataServiceExecutor {
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
        let started_at = SystemTime::now();
        let entity = &request.query.entity;

        // Collect raw records from the matching collector.
        let rows = self.collect_records(entity)?;

        let mut result = InMemoryQueryEngine::execute(
            &request.query,
            rows.into_iter()
                .map(teaql_core::CompactRow::from_map)
                .collect(),
        )?;
        result.metadata.backend = "linux-proc".to_owned();
        result.metadata.started_at = started_at;
        result.metadata.ended_at = SystemTime::now();
        result.metadata.trace_chain = request.trace_chain;
        result.metadata.comment = Some(request.intent.comment().to_owned());
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
            intent: teaql_core::QueryIntent::new(
                "Load the second matching fixture".to_owned(),
                "verify provider query behavior",
            )
            .unwrap(),
            capture_debug_query: true,
            capture_execution_metadata: true,
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
    async fn query_preserves_exact_integer_sum_above_float_precision() {
        let executor = LinuxDataServiceExecutor::new().with_collector(Box::new(StaticCollector {
            rows: vec![row("one", 9_007_199_254_740_993), row("two", 2)],
        }));
        let request = QueryRequest::new(
            SelectQuery::new("Fixture")
                .aggregate(Aggregate::sum("score", "total"))
                .limit(1),
            teaql_core::QueryIntent::new("sum exact fixture scores", "verify provider precision")
                .unwrap(),
        );
        let result = executor.query(request).await.unwrap();
        assert_eq!(
            result.rows[0].get("total"),
            Some(&Value::I64(9_007_199_254_740_995))
        );
        assert_eq!(result.metadata.backend, "linux-proc");
        assert_eq!(result.metadata.result_count, Some(1));
    }

    #[tokio::test]
    async fn query_retains_structured_aggregate_error_and_source() {
        let executor = LinuxDataServiceExecutor::new().with_collector(Box::new(StaticCollector {
            rows: vec![row("one", i64::MAX), row("two", 1)],
        }));
        let request = QueryRequest::new(
            SelectQuery::new("Fixture")
                .aggregate(Aggregate::sum("score", "total"))
                .limit(1),
            teaql_core::QueryIntent::new("inspect sum overflow", "verify typed provider errors")
                .unwrap(),
        );
        let error = executor.query(request).await.unwrap_err();
        let LinuxProviderError::Query(aggregate) = &error else {
            panic!("aggregate error type erased")
        };
        assert_eq!(
            aggregate.kind,
            teaql_runtime::InMemoryQueryErrorKind::NumericOverflow
        );
        assert_eq!(aggregate.field, "score");
        assert_eq!(aggregate.alias, "total");
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<teaql_runtime::InMemoryQueryError>()
                .is_some()
        );
        assert!(!format!("{error}").contains(&i64::MAX.to_string()));
    }

    #[tokio::test]
    async fn query_supports_aggregates() {
        let request = QueryRequest {
            query: SelectQuery::new("Fixture")
                .filter(Expr::gt("score", 10_i64))
                .aggregate(Aggregate::count("matching")),
            trace_chain: Vec::new(),
            intent: teaql_core::QueryIntent::new(
                "verify bounded provider query",
                "verify provider query behavior",
            )
            .unwrap(),
            capture_debug_query: true,
            capture_execution_metadata: true,
        };

        let result = executor().query(request).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("matching"), Some(&Value::I64(2)));
        assert_eq!(result.metadata.result_count, Some(1));
    }

    #[tokio::test]
    async fn aggregate_having_order_and_slice_preserve_provider_trace_metadata() {
        let executor = LinuxDataServiceExecutor::new().with_collector(Box::new(StaticCollector {
            rows: vec![
                row("A", 4),
                row("B", 2),
                row("A", 6),
                row("C", 3),
                row("C", 5),
            ],
        }));
        let trace = TraceNode::new("Fixture", None, "Load qualifying resource groups");
        let request = QueryRequest {
            query: SelectQuery::new("Fixture")
                .filter(Expr::gt("score", 1_i64))
                .group_by("name")
                .aggregate(Aggregate::sum("score", "total"))
                .having(Expr::gt("total", 5_i64))
                .order_by(OrderBy::desc("total"))
                .page(1, 1),
            trace_chain: vec![trace.clone()],
            intent: teaql_core::QueryIntent::new(
                "Load qualifying resource groups",
                "verify bounded in-memory aggregate execution",
            )
            .unwrap(),
            capture_debug_query: true,
            capture_execution_metadata: true,
        };
        let result = executor.query(request).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("name"), Some(&Value::from("C")));
        assert_eq!(
            result.rows[0].get("total").and_then(Value::try_f64),
            Some(8.0)
        );
        assert_eq!(result.metadata.result_count, Some(1));
        assert_eq!(result.metadata.backend, "linux-proc");
        assert_eq!(result.metadata.trace_chain, vec![trace]);
        assert_eq!(
            result.metadata.comment.as_deref(),
            Some("Load qualifying resource groups")
        );
        assert_eq!(result.metadata.operation, DataServiceOperation::Query);
        assert!(result.metadata.statements.is_empty());
        assert!(result.metadata.started_at <= result.metadata.ended_at);
    }
}
