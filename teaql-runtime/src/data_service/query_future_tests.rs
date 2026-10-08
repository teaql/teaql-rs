//! Size and lazy-observation regression for the ordinary typed query path.
use super::*;
use teaql_core::{CompactRow, TeaqlEntity as _, Value};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, ExecutionMetadata, MutationExecutor,
    MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
};

#[derive(Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "FutureProbe")]
struct Probe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
}
struct Executor;
impl DataServiceExecutor for Executor {
    type Error = std::io::Error;
    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceCapabilities::default()
    }
}
impl QueryExecutor for Executor {
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
        assert_eq!(
            request.intent.comment(),
            "what: measure ordinary query future"
        );
        Ok(QueryResult {
            rows: vec![CompactRow::new(
                vec!["id".into(), "version".into(), "name".into()].into(),
                vec![Value::U64(1), Value::I64(1), Value::Text("probe".into())],
            )],
            metadata: ExecutionMetadata::unrecorded_query(1),
        })
    }
}
impl MutationExecutor for Executor {
    async fn mutate(&self, _: MutationRequest) -> Result<MutationResult, Self::Error> {
        panic!("read-only future regression")
    }
}

#[tokio::test]
async fn ordinary_typed_query_future_has_bounded_heap_frame() {
    let mut context = crate::UserContext::new()
        .with_metadata(crate::InMemoryMetadataStore::new().with_entity(Probe::entity_descriptor()));
    context.disable_sql_log();
    context.observe_id_set("UNEXECUTED", Some(77));
    let executor = Executor;
    let mut service = EntityDataService::for_executor(&context, "FutureProbe", &executor);
    service.request_intent = Some(
        teaql_core::QueryIntent::new(
            "what: measure ordinary query future",
            "why: avoid complex graph frame per ordinary query",
        )
        .unwrap(),
    );
    let query = SelectQuery::new("FutureProbe")
        .projects(vec!["id", "version", "name"])
        .limit(1)
        .comment("what: measure ordinary query future");
    let future =
        service.fetch_enhanced_entities_with_relation_aggregates_prepared::<Probe>(query, &[]);
    let bytes = std::mem::size_of_val(future.as_ref().get_ref());
    println!("ORDINARY_QUERY_HEAP_FRAME_BYTES={bytes}");
    assert_eq!(
        context.id_set_plan().as_deref(),
        Some("UNEXECUTED"),
        "unpolled future must not emit execution observation"
    );
    assert_eq!(context.id_set_count(), Some(77));
    let rows = future.await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "probe");
    assert_eq!(context.id_set_plan().as_deref(), Some("ID_SET_DISABLED"));
    assert_eq!(context.id_set_count(), None);
    assert!(
        bytes <= 8192,
        "ordinary query must not carry the large graph branch: {bytes}"
    );
}
