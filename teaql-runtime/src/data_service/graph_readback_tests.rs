//! Runtime-derived readback intent; no test-authored runtime trace frames.
use super::EntityDataService;
use crate::{InMemoryMetadataStore, UserContext};
use teaql_core::{DataType, EntityDescriptor, Expr, MutationIntent, PropertyDescriptor, Value};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, ExecutionMetadata, ExecutionObserver,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
    SqlIntentRedactions, SqlParameterLogPolicy,
};

struct ReadbackExecutor {
    fail: bool,
}

impl DataServiceExecutor for ReadbackExecutor {
    type Error = std::io::Error;
    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceCapabilities::default()
    }
}

impl ReadbackExecutor {
    fn result(
        &self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'_>>,
    ) -> Result<QueryResult, std::io::Error> {
        assert_eq!(request.comment(), "save fixture 7123");
        assert_eq!(request.query.filter, Some(Expr::eq("record_key", 7123_u64)));
        let mut metadata = ExecutionMetadata::unrecorded_query(0);
        metadata.backend = "readback-spy".into();
        metadata.parameterized_query =
            Some("SELECT record_key FROM fixture WHERE record_key = ?".into());
        metadata.params = vec![Value::U64(7123)];
        metadata.trace_chain = request.execution_trace_chain();
        metadata.comment = Some(request.comment().into());
        metadata.sql_log.generated_sql = true;
        metadata.sql_log.database_kind = Some("sqlite".into());
        metadata.sql_log.parameter_policies = vec![SqlParameterLogPolicy::Plain];
        if self.fail {
            if let Some(observer) = observer {
                observer(metadata);
            }
            Err(std::io::Error::other("simulated readback failure"))
        } else {
            Ok(QueryResult {
                rows: vec![],
                metadata,
            })
        }
    }
}

impl QueryExecutor for ReadbackExecutor {
    fn query_log_intent(&self, _: &teaql_core::SelectQuery) -> SqlIntentRedactions {
        // Emulate the real SQL compiler: the declared ID binding is plain.
        SqlIntentRedactions::default()
    }
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
        self.result(request, None)
    }
    async fn query_observed<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<QueryResult, Self::Error> {
        self.result(request, observer)
    }
}

impl MutationExecutor for ReadbackExecutor {
    async fn mutate(&self, _: MutationRequest) -> Result<MutationResult, Self::Error> {
        panic!("readback must not write")
    }
}

#[tokio::test]
async fn readback_masks_target_in_intent_on_success_and_failure_without_polluting_parent() {
    for logging in [true, false] {
        for fail in [false, true] {
            let mut context = UserContext::new().with_metadata(
                InMemoryMetadataStore::new().with_entity(
                    EntityDescriptor::new("Fixture")
                        .property(PropertyDescriptor::new("record_key", DataType::U64).id())
                        .audit_mask_fields(vec![]),
                ),
            );
            if !logging {
                context.disable_sql_log();
            }
            let executor = ReadbackExecutor { fail };
            let parent = EntityDataService::for_executor(&context, "Fixture", &executor)
                .with_mutation_intent(MutationIntent::new("save fixture 7123").unwrap());
            let result = parent
                .fetch_graph_current_row_internal(
                    "Fixture",
                    "record_key",
                    &Value::U64(7123),
                    vec![],
                )
                .await;
            assert_eq!(result.is_err(), fail);
            let logs = context.sql_logs();
            assert_eq!(logs.len(), usize::from(logging));
            if logging {
                assert_eq!(logs[0].comment.as_deref(), Some("save fixture [REDACTED]"));
                assert_eq!(
                    logs[0].params,
                    vec![Value::U64(7123)],
                    "ID binding remains plain"
                );
            }
            assert!(
                parent.query_intent_snapshot().is_none(),
                "readback provenance escaped its scope"
            );
            assert_eq!(
                parent.request_intent.as_ref().unwrap().comment(),
                "save fixture 7123"
            );
        }
    }
}
