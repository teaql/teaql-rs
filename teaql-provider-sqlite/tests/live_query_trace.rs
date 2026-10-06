//! TC-REQ-06: real SQLite results remain held while two queries share one Context.
//! #239. The observer delegates unchanged requests; it never supplies trace nodes.
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use teaql_core::{
    DataType, EntityDescriptor, InsertCommand, PropertyDescriptor, RelationDescriptor, SelectQuery,
    TraceKind, TraceNode, Value,
};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, ExecutionMetadata, MutationCommand,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
    SchemaProvider, SqlIntentRedactions,
};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, PurposedSelectQuery, UserContext};
use teaql_sql::SqlDataServiceExecutor;

#[derive(Clone)]
struct Schema(Vec<Arc<EntityDescriptor>>);
impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        self.0.iter().find(|entity| entity.name == name).cloned()
    }
}
type Executor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;

struct Observation {
    entity: String,
    comment: String,
    purpose: String,
    trace: Vec<TraceNode>,
    metadata: ExecutionMetadata,
}
#[derive(Default)]
struct Gate {
    held_roots: usize,
    released: bool,
}
struct ObservedExecutor {
    inner: Executor,
    facts: Mutex<Vec<Observation>>,
    gate: (Mutex<Gate>, Condvar),
}
impl ObservedExecutor {
    fn release(&self) {
        self.gate.0.lock().unwrap().released = true;
        self.gate.1.notify_all();
    }
}
impl DataServiceExecutor for ObservedExecutor {
    type Error = <Executor as DataServiceExecutor>::Error;
    fn capabilities(&self) -> DataServiceCapabilities {
        self.inner.capabilities()
    }
}
impl QueryExecutor for ObservedExecutor {
    fn query_log_intent(&self, query: &SelectQuery) -> SqlIntentRedactions {
        self.inner.query_log_intent(query)
    }
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
        let entity = request.query.entity.clone();
        let comment = request.intent.comment().to_owned();
        let purpose = request.intent.purpose().to_owned();
        let trace = request.execution_trace_chain();
        let result = self.inner.query(request).await?;
        self.facts.lock().unwrap().push(Observation {
            entity: entity.clone(),
            comment,
            purpose,
            trace,
            metadata: result.metadata.clone(),
        });
        if entity == "LiveRoot" {
            let mut gate = self.gate.0.lock().unwrap();
            gate.held_roots += 1;
            self.gate.1.notify_all();
            let (gate, timeout) = self
                .gate
                .1
                .wait_timeout_while(gate, Duration::from_secs(5), |gate| !gate.released)
                .unwrap();
            assert!(
                !timeout.timed_out() && gate.released,
                "query release timed out"
            );
        }
        Ok(result)
    }
}
impl MutationExecutor for ObservedExecutor {
    async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
        self.inner.mutate(request).await
    }
}
struct ReleaseOnDrop<'a>(&'a ObservedExecutor);
impl Drop for ReleaseOnDrop<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn fixture(logging: bool) -> UserContext {
    let names = ["LiveRoot", "LiveChild", "LiveDetail", "LiveLeaf"];
    let mut metadata = InMemoryMetadataStore::new();
    let mut schema = Vec::new();
    for (i, name) in names.iter().enumerate() {
        let mut entity = EntityDescriptor::new(*name)
            .table_name(name.to_ascii_lowercase())
            .property(PropertyDescriptor::new("id", DataType::I64).id())
            .property(PropertyDescriptor::new("version", DataType::I64).version())
            .property(PropertyDescriptor::new("parent_id", DataType::I64));
        if i < 3 {
            entity = entity.relation(
                RelationDescriptor::new("children", names[i + 1])
                    .many()
                    .local_key("id")
                    .foreign_key("parent_id"),
            );
        }
        metadata = metadata.with_entity(entity.clone());
        schema.push(Arc::new(entity));
    }
    let mut context = UserContext::new().with_metadata(metadata);
    let transport =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open_in_memory().unwrap());
    context.use_sqlite_provider(transport.clone());
    context.ensure_schema().await.unwrap();
    let executor = Executor::new(SqliteDialect, transport, Schema(schema));
    for name in names {
        executor
            .mutate(
                MutationCommand::Insert(
                    InsertCommand::new(name)
                        .value("id", 1_i64)
                        .value("version", 1_i64)
                        .value("parent_id", 1_i64),
                )
                .request("seed native live-query fixture")
                .unwrap(),
            )
            .await
            .unwrap();
    }
    context.insert_resource(ObservedExecutor {
        inner: executor,
        facts: Mutex::new(Vec::new()),
        gate: (Mutex::new(Gate::default()), Condvar::new()),
    });
    context.clear_sql_logs();
    if !logging {
        context.disable_sql_log();
    }
    context
}

#[test]
fn two_live_queries_on_one_context_keep_intent_and_real_relation_paths_isolated() {
    for logging in [false, true] {
        let context = futures_executor::block_on(fixture(logging));
        let observer = context.require_resource::<ObservedExecutor>().unwrap();
        std::thread::scope(|threads| {
            // Drop releases waiting workers before scoped thread joining on failure.
            let _release = ReleaseOnDrop(observer);
            let handles = ["alpha", "beta"].map(|label| {
                let context = &context;
                // Recursive native query futures exceed the default worker
                // stack in this unoptimized fixture; do not hide that cost.
                std::thread::Builder::new()
                    .name(format!("live-query-{label}"))
                    .stack_size(32 * 1024 * 1024)
                    .spawn_scoped(threads, move || {
                        futures_executor::block_on(async {
                            let query = PurposedSelectQuery::new(
                                SelectQuery::new("LiveRoot")
                                    .limit(1)
                                    .comment(format!("load {label} graph"))
                                    .relation_query(
                                        "children",
                                        SelectQuery::new("LiveChild").limit(1).relation_query(
                                            "children",
                                            SelectQuery::new("LiveDetail").limit(1).relation_query(
                                                "children",
                                                SelectQuery::new("LiveLeaf").limit(1),
                                            ),
                                        ),
                                    ),
                                format!("render {label} graph"),
                            );
                            context
                                .entity_data_service::<ObservedExecutor>("LiveRoot")
                                .unwrap()
                                .fetch_all(&query)
                                .await
                                .unwrap()
                        })
                    })
                    .unwrap()
            });
            let (live, timeout) = observer
                .gate
                .1
                .wait_timeout_while(
                    observer.gate.0.lock().unwrap(),
                    Duration::from_secs(5),
                    |gate| gate.held_roots != 2,
                )
                .unwrap();
            assert!(
                !timeout.timed_out() && live.held_roots == 2,
                "both real root reads must overlap before either query finishes"
            );
            assert!(!live.released);
            drop(live);
            assert!(handles.iter().all(|handle| !handle.is_finished()));
            assert!(context.sql_logs().is_empty());
            assert_eq!(observer.facts.lock().unwrap().len(), 2);
            println!(
                "LIVE_QUERY_BARRIER {}",
                serde_json::json!({
                    "logging": logging,
                    "heldRoots": observer.gate.0.lock().unwrap().held_roots,
                    "unfinishedWorkers": handles.iter().filter(|handle| !handle.is_finished()).count(),
                    "observedPhysicalRoots": observer.facts.lock().unwrap().len(),
                    "contextLogCount": context.sql_logs().len(),
                })
            );
            observer.release();
            for handle in handles {
                let mut rows = handle.join().unwrap();
                for _ in 0..3 {
                    assert_eq!(rows.len(), 1);
                    let Value::List(children) = rows.remove(0).remove("children").unwrap() else {
                        panic!("missing real child list");
                    };
                    rows = children
                        .into_iter()
                        .map(|value| {
                            let Value::Object(row) = value else {
                                panic!("missing real child object")
                            };
                            teaql_core::CompactRow::from_map(row)
                        })
                        .collect();
                }
                assert_eq!(rows[0].get("id").and_then(Value::try_i64), Some(1));
            }
        });
        let facts = observer.facts.lock().unwrap();
        assert_eq!(facts.len(), 8);
        for label in ["alpha", "beta"] {
            let own = facts
                .iter()
                .filter(|fact| fact.comment == format!("load {label} graph"))
                .collect::<Vec<_>>();
            assert_eq!(own.len(), 4);
            for (depth, fact) in own.iter().enumerate() {
                assert_eq!(fact.purpose, format!("render {label} graph"));
                assert_eq!(
                    fact.entity,
                    ["LiveRoot", "LiveChild", "LiveDetail", "LiveLeaf"][depth]
                );
                let relations = fact
                    .trace
                    .iter()
                    .filter(|node| node.kind == TraceKind::Relation)
                    .collect::<Vec<_>>();
                // Logging-off disables diagnostic route capture, not the
                // independently validated request intent or physical query.
                assert_eq!(relations.len(), if logging { depth } else { 0 });
                for (edge, node) in relations.iter().enumerate() {
                    assert_eq!(node.entity_type, "children");
                    assert_eq!(
                        node.comment,
                        format!("{}.children", ["LiveRoot", "LiveChild", "LiveDetail"][edge])
                    );
                }
                if logging {
                    assert_eq!(fact.metadata.result_count, Some(1));
                }
            }
        }
        let logs = context.sql_logs();
        println!(
            "LIVE_QUERY_RESULT {}",
            serde_json::json!({
                "logging": logging,
                "workerStackBytes": 32 * 1024 * 1024,
                "requests": facts.iter().map(|fact| serde_json::json!({
                    "entity": fact.entity, "comment": fact.comment, "purpose": fact.purpose,
                    "relations": fact.trace.iter().filter(|node| node.kind == TraceKind::Relation)
                        .map(|node| vec![node.entity_type.clone(), node.comment.clone()]).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
                "sql": logs.iter().map(|entry| serde_json::json!({
                    "comment": entry.comment, "purpose": entry.purpose,
                    "path": entry.trace_path.iter().map(|node|
                        vec![format!("{:?}", node.kind), node.entity_type.clone(), node.comment.clone()])
                        .collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
            })
        );
        assert_eq!(logs.len(), if logging { 8 } else { 0 });
        for label in ["alpha", "beta"] {
            let own = logs
                .iter()
                .filter(|entry| {
                    entry.comment.as_deref() == Some(format!("load {label} graph").as_str())
                })
                .collect::<Vec<_>>();
            assert_eq!(own.len(), if logging { 4 } else { 0 });
            for (depth, entry) in own.iter().enumerate() {
                assert_eq!(
                    entry.purpose.as_deref(),
                    Some(format!("render {label} graph").as_str())
                );
                let kinds = entry
                    .trace_path
                    .iter()
                    .map(|node| node.kind)
                    .collect::<Vec<_>>();
                let mut expected = vec![TraceKind::Operation, TraceKind::Request];
                expected.extend(std::iter::repeat_n(TraceKind::Relation, depth));
                expected.extend([TraceKind::Provider, TraceKind::Sql]);
                assert_eq!(kinds, expected);
                assert_eq!(entry.trace_path[0].entity_type, "LiveRoot");
                assert_eq!(entry.trace_path[1].entity_type, "LiveRoot");
                assert_eq!(entry.trace_path[depth + 2].entity_type, "sqlite");
                assert_eq!(entry.trace_path[depth + 3].entity_type, "select");
            }
        }
    }
}
