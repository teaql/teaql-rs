use crate::{
    PurposedSelectQuery, RuntimeOperation, RuntimeTelemetry, RuntimeTelemetryScope, UserContext,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use teaql_core::request::QueryOptions;
use teaql_core::{Expr, QueryOperation, QueryOperationKind, SelectQuery, SmartList};

fn options(alias: Option<&str>) -> QueryOptions {
    let mut operation = QueryOperation::new(QueryOperationKind::NamedCount, "count");
    if let Some(alias) = alias {
        operation = operation.with_alias(alias);
    }
    QueryOptions {
        page_count_operation: Some(operation),
        ..Default::default()
    }
}

fn authorize(query: SelectQuery) -> PurposedSelectQuery {
    PurposedSelectQuery::new(query.comment("load matching rows"), "render bounded list")
}

#[test]
fn explicit_count_splits_rows_and_total_without_changing_membership() {
    let source = SelectQuery::new("Probe")
        .filter(Expr::eq("status", "ACTIVE"))
        .projects(vec!["id", "name"])
        .count("total")
        .page(2, 3);
    let mut query = authorize(source);
    let (alias, count) = query
        .take_requested_list_count(&options(Some("total")))
        .unwrap();
    assert_eq!(alias, "total");
    assert!(query.as_query().aggregates.is_empty());
    assert_eq!(query.as_query().slice.unwrap().offset, 2);
    assert_eq!(query.as_query().filter, count.as_query().filter);
    assert!(count.as_query().slice.is_none());
    assert!(count.as_query().projection.is_empty());
    assert_eq!(count.as_query().aggregates[0].alias, "total");
    assert_eq!(count.as_query().purpose, query.as_query().purpose);
}

#[test]
fn ordinary_list_does_not_invent_count() {
    let mut query = authorize(SelectQuery::new("Probe").limit(2));
    assert!(
        query
            .take_requested_list_count(&QueryOptions::default())
            .is_none()
    );
    assert!(query.as_query().aggregates.is_empty());
    // Metadata alone cannot reinterpret an unrelated aggregate or an alias.
    assert!(query.take_requested_list_count(&options(None)).is_none());
    let mut unrelated = authorize(SelectQuery::new("Probe").count("different"));
    assert!(
        unrelated
            .take_requested_list_count(&options(None))
            .is_none()
    );
}

#[test]
fn grouped_and_mixed_aggregates_keep_their_semantics() {
    let queries = [
        SelectQuery::new("Probe").count("count").group_by("status"),
        SelectQuery::new("Probe")
            .count("count")
            .sum("amount", "sum"),
        SelectQuery::new("Probe").count_field("name", "count"),
    ];
    for source in queries {
        let mut query = authorize(source);
        let before = query.as_query().clone();
        assert!(query.take_requested_list_count(&options(None)).is_none());
        assert_eq!(query.as_query(), &before);
    }
}

#[derive(Default)]
struct Recorder(Mutex<Vec<String>>);
struct Scope(Arc<Recorder>);
impl RuntimeTelemetry for Arc<Recorder> {
    fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
        self.0.lock().unwrap().push(operation.name);
        Box::new(Scope(self.clone()))
    }
}
impl RuntimeTelemetryScope for Scope {
    fn success(&mut self, attributes: BTreeMap<String, crate::RuntimeAttributeValue>) {
        if let Some(value) = attributes.get("teaql.result.cardinality") {
            self.0.0.lock().unwrap().push(format!("rows:{value:?}"));
        }
    }
    fn failure(&mut self, _: &str) {
        self.0.0.lock().unwrap().push("failure".into());
    }
}

#[tokio::test]
async fn generated_list_owns_children_without_leaking_to_other_context_or_later_query() {
    let recorder = Arc::new(Recorder::default());
    let context = UserContext::new().with_runtime_telemetry(Arc::new(recorder.clone()));
    let other = UserContext::new().with_runtime_telemetry(Arc::new(recorder.clone()));
    let future = context.observe_generated_list("Probe", async {
        context
            .observe_query_request("Probe", async { Ok::<_, ()>(()) })
            .await?;
        tokio::task::yield_now().await;
        context
            .observe_query_request("Facet", async { Ok::<_, ()>(()) })
            .await?;
        other
            .observe_query_request("Independent", async { Ok::<_, ()>(()) })
            .await?;
        Ok::<_, ()>(SmartList::<u64>::new(vec![1, 2]))
    });
    assert!(
        recorder.0.lock().unwrap().is_empty(),
        "unpolled future must not start a root"
    );
    assert_eq!(future.await.unwrap().len(), 2);
    context
        .observe_query_request("After", async { Ok::<_, ()>(()) })
        .await
        .unwrap();
    assert_eq!(
        *recorder.0.lock().unwrap(),
        [
            "Probe.list",
            "Independent.request",
            "rows:Integer(2)",
            "After.request"
        ]
    );
}

#[tokio::test]
async fn concurrent_lists_get_independent_roots_and_failure_keeps_original_error() {
    let recorder = Arc::new(Recorder::default());
    let context = UserContext::new().with_runtime_telemetry(Arc::new(recorder.clone()));
    let first = context.observe_generated_list("First", async {
        tokio::task::yield_now().await;
        context
            .observe_query_request("FirstCount", async { Ok::<_, &str>(()) })
            .await?;
        Ok::<_, &str>(SmartList::<u64>::new(vec![1]))
    });
    let second = context.observe_generated_list("Second", async {
        tokio::task::yield_now().await;
        context
            .observe_query_request("SecondCount", async { Ok::<_, &str>(()) })
            .await?;
        Err::<SmartList<u64>, _>("original business error")
    });
    let (first, second) = tokio::join!(first, second);
    assert_eq!(first.unwrap().len(), 1);
    assert_eq!(second.unwrap_err(), "original business error");
    let events = recorder.0.lock().unwrap();
    assert_eq!(events.iter().filter(|e| e.ends_with(".list")).count(), 2);
    assert!(!events.iter().any(|e| e.ends_with(".request")));
    assert_eq!(events.iter().filter(|e| *e == "failure").count(), 1);
}
