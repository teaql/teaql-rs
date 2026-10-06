//! Regression contract for request intent and graph lineage (teaql-rs #239).
//! No expected trace is injected into the planner or provider.
use teaql_core::{
    CompactRow, DataType, Entity, EntityDescriptor, PropertyDescriptor, RelationDescriptor,
    TraceKind, TraceNode, Value,
};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, MutationExecutor, MutationRequest,
    MutationResult, QueryExecutor, QueryRequest, QueryResult,
};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_runtime::{
    EntityDataService, EntityRuntimeState, GraphMutationKind, GraphNode, GraphOperation,
    InMemoryMetadataStore, LedgerEntity, UserContext,
};

struct NoIo;

struct NoPolicy;
impl teaql_runtime::RequestPolicy for NoPolicy {
    fn enforce_select(
        &self,
        _: &UserContext,
        _: &mut teaql_core::SelectQuery,
    ) -> Result<(), teaql_runtime::RuntimeError> {
        panic!("missing request intent must fail before policy callbacks");
    }
}

impl DataServiceExecutor for NoIo {
    type Error = std::io::Error;
    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceCapabilities::default()
    }
}

impl QueryExecutor for NoIo {
    async fn query(&self, _request: QueryRequest) -> Result<QueryResult, Self::Error> {
        panic!("a create-only graph or rejected request must not query the provider");
    }
}

impl MutationExecutor for NoIo {
    async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
        panic!("planning must not mutate the provider");
    }
}

fn entity(name: &str) -> EntityDescriptor {
    EntityDescriptor::new(name)
        .property(PropertyDescriptor::new("id", DataType::U64).id())
        .property(PropertyDescriptor::new("version", DataType::I64).version())
}

fn context() -> UserContext {
    UserContext::default().with_metadata(
        InMemoryMetadataStore::new()
            .with_entity(
                entity("Order")
                    .relation(RelationDescriptor::new("items", "OrderItem").many())
                    .relation(RelationDescriptor::new("payment", "Payment"))
                    .relation(RelationDescriptor::new("shipment", "Shipment")),
            )
            .with_entity(entity("OrderItem"))
            .with_entity(
                entity("Payment")
                    .relation(RelationDescriptor::new("attempts", "PaymentAttempt").many()),
            )
            .with_entity(entity("PaymentAttempt"))
            .with_entity(entity("Shipment")),
    )
}

fn create(entity: &str, id: u64) -> GraphNode {
    GraphNode::new(entity)
        .operation(GraphOperation::Create)
        .value("id", id)
}

fn assert_intent_error(
    error: teaql_runtime::DataServiceError<std::io::Error>,
    kind: teaql_core::RequestKind,
    field: &str,
) {
    let teaql_runtime::DataServiceError::Runtime(teaql_runtime::RuntimeError::RequestIntent(error)) =
        error
    else {
        panic!("expected a structured request intent error, got {error:?}");
    };
    assert_eq!(error.request_kind, kind);
    assert_eq!(error.field, field);
    assert_eq!(
        error.code(),
        if field == "purpose" {
            "QUERY_PURPOSE_REQUIRED"
        } else {
            "REQUEST_COMMENT_REQUIRED"
        }
    );
}

#[tokio::test]
async fn query_missing_comment_cannot_be_repaired_by_trace_or_disabled_logs() {
    for disable_logs in [false, true] {
        let mut context = context().with_request_policy(NoPolicy);
        if disable_logs {
            context.disable_sql_log();
        }
        let executor = NoIo;
        for comment in [None, Some(""), Some(" \t\n"), Some("\u{2003}")] {
            let mut query = teaql_core::SelectQuery::new("Order").limit(10);
            query.comment = comment.map(str::to_owned);
            // Neither a Purpose frame nor ambient trace ancestry is the
            // explicit request comment. Do not reach policy/provider.
            let query = teaql_runtime::PurposedSelectQuery::new(query, "render orders");
            let service = EntityDataService::for_executor(&context, "Order", &executor)
                .with_trace_context(vec![TraceNode::typed(
                    TraceKind::Comment,
                    "Order",
                    None,
                    "fabricated root comment",
                )]);
            let error = service.fetch_all(&query).await.unwrap_err();
            assert_intent_error(error, teaql_core::RequestKind::Query, "comment");
        }
    }
}

fn lineage(nodes: &[TraceNode]) -> Vec<(&str, Option<u64>, &str)> {
    nodes
        .iter()
        .map(|node| {
            assert_eq!(node.kind, TraceKind::AuditReason);
            (
                node.entity_type.as_str(),
                node.entity_id,
                node.comment.as_str(),
            )
        })
        .collect()
}

#[tokio::test]
async fn graph_planner_rejects_missing_or_blank_root_comment_before_io() {
    let context = context();
    let executor = NoIo;
    let service = EntityDataService::for_executor(&context, "Order", &executor);
    for comment in [None, Some(""), Some(" \t\n"), Some("\u{2003}")] {
        let mut node = create("Order", 100);
        node.comment = comment.map(str::to_owned);
        // A child's reason must not repair the missing root request property.
        node = node.relation("items", create("OrderItem", 201).comment("add item"));
        let error = service
            .plan_graph(node)
            .await
            .expect_err("the root request must own a non-blank comment");
        assert_intent_error(error, teaql_core::RequestKind::Mutation, "comment");
    }
}

#[tokio::test]
async fn planner_retains_deleted_child_reason_and_isolates_sibling_branches() {
    let context = context();
    let executor = NoIo;
    let service = EntityDataService::for_executor(&context, "Order", &executor);
    let graph = create("Order", 100)
        .comment("submit order")
        .relation("items", create("OrderItem", 201))
        .relation(
            "items",
            GraphNode::new("OrderItem")
                .value("id", 202_u64)
                .value("version", 1_i64)
                .remove()
                .comment("remove unavailable item"),
        )
        .relation(
            "payment",
            create("Payment", 301)
                .comment("authorize payment")
                .relation("attempts", create("PaymentAttempt", 401)),
        )
        .relation(
            "shipment",
            create("Shipment", 501).comment("dispatch shipment"),
        );
    let plan = service.plan_graph(graph).await.unwrap();
    assert_eq!(plan.items.len(), 6);
    for item in &plan.items {
        let trace = item.scope_token.as_ref().unwrap().recover_trace_chain();
        let mut expected = vec![("Order", Some(100), "submit order")];
        match (
            item.entity.as_str(),
            item.values.get("id").and_then(Value::try_u64),
        ) {
            ("OrderItem", Some(202)) => {
                assert_eq!(item.kind, GraphMutationKind::Delete);
                expected.push(("OrderItem", Some(202), "remove unavailable item"));
            }
            ("Payment", _) | ("PaymentAttempt", _) => {
                expected.push(("Payment", Some(301), "authorize payment"));
            }
            ("Shipment", _) => {
                expected.push(("Shipment", Some(501), "dispatch shipment"));
            }
            _ => {}
        }
        assert_eq!(lineage(&trace), expected, "{}", item.entity);
    }
}

#[tokio::test]
async fn blank_descendant_comment_inherits_without_adding_a_scope() {
    let context = context();
    let executor = NoIo;
    let plan = EntityDataService::for_executor(&context, "Order", &executor)
        .plan_graph(
            create("Order", 100)
                .comment("submit order")
                .relation("items", create("OrderItem", 201).comment(" \n")),
        )
        .await
        .unwrap();
    let trace = plan.items[1]
        .scope_token
        .as_ref()
        .unwrap()
        .recover_trace_chain();
    assert_eq!(lineage(&trace), [("Order", Some(100), "submit order")]);
}

#[tokio::test]
async fn newly_allocated_ids_and_same_id_different_types_are_retained() {
    let context = context();
    let executor = NoIo;
    let plan = EntityDataService::for_executor(&context, "Order", &executor)
        .plan_graph(
            create("Order", 100)
                .comment("submit order")
                .relation(
                    "payment",
                    create("Payment", 100).comment("authorize payment"),
                )
                .relation(
                    "shipment",
                    GraphNode::new("Shipment")
                        .operation(GraphOperation::Create)
                        .comment("dispatch shipment"),
                ),
        )
        .await
        .unwrap();
    assert_eq!(plan.items.len(), 3);
    for item in &plan.items {
        let trace = item.scope_token.as_ref().unwrap().recover_trace_chain();
        let tail = trace.last().unwrap();
        assert_eq!(tail.entity_type, item.entity);
        assert_eq!(
            tail.entity_id,
            item.values.get("id").and_then(Value::try_u64)
        );
        assert!(tail.entity_id.is_some_and(|id| id > 0));
    }
}

#[teaql_entity]
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "CommentProbe")]
struct CommentProbe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
}

fn probe(id: u64) -> CommentProbe {
    CommentProbe::from_compact_row(CompactRow::from_map(std::collections::BTreeMap::from([
        ("id".to_owned(), Value::U64(id)),
        ("version".to_owned(), Value::I64(1)),
        ("name".to_owned(), Value::Text("probe".to_owned())),
    ])))
    .unwrap()
}

#[test]
fn macro_entity_comments_are_keyed_even_when_sharing_one_ledger() {
    let shared = EntityRuntimeState::default();
    let mut root = probe(100);
    let mut child = probe(201);
    root.__teaql_replace_runtime_state(shared.clone());
    child.__teaql_replace_runtime_state(shared);
    root.set_comment("submit order");
    child.set_comment("add item");
    assert_eq!(root.get_comment().as_deref(), Some("submit order"));
    assert_eq!(child.get_comment().as_deref(), Some("add item"));
}

#[test]
fn child_composition_must_not_supply_a_missing_root_comment() {
    let root = probe(100);
    let mut child = probe(201);
    child.set_comment("add item");
    root.include_pending_mutations_from(&child).unwrap();
    assert_eq!(root.get_comment(), None);
    assert_eq!(child.get_comment().as_deref(), Some("add item"));
}
