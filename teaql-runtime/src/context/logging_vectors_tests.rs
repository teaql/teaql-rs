//! Pure shared-vector gate against the linked runtime, not source extraction.
//! Fixture-authored nodes here are NOT generated-query/provider evidence.
use super::{SqlLogOperation, canonical_sql_trace_path, trace_value};
use serde_json::Value;
use teaql_core::{TraceKind, TraceNode};

fn nodes(value: &Value) -> Vec<TraceNode> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|node| {
            let kind = match node["kind"].as_str().unwrap() {
                "Operation" => TraceKind::Operation,
                "Request" => TraceKind::Request,
                "Relation" => TraceKind::Relation,
                "Entity" => TraceKind::Entity,
                "Provider" => TraceKind::Provider,
                "Sql" => TraceKind::Sql,
                "Comment" => TraceKind::Comment,
                "Purpose" => TraceKind::Purpose,
                "AuditReason" => TraceKind::AuditReason,
                other => panic!("unknown fixture trace kind {other}"),
            };
            TraceNode::typed(
                kind,
                node["name"].as_str().unwrap(),
                node["entityId"].as_u64(),
                node["detail"].as_str().unwrap(),
            )
        })
        .collect()
}

#[test]
fn shared_sql_trace_vectors_are_canonical_idempotent_and_keep_intent_separate() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../tests/fixtures/sql-trace-path-v1.json")).unwrap();
    assert_eq!(fixture["contract"], "teaql.sql-trace-path.v1");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 12);
    let mut ids = std::collections::BTreeSet::new();
    for case in cases {
        let id = case["id"].as_str().unwrap();
        assert!(ids.insert(id), "duplicate vector {id}");
        let operation = match case["operation"].as_str().unwrap() {
            "select" => SqlLogOperation::Select,
            "insert" => SqlLogOperation::Insert,
            "update" => SqlLogOperation::Update,
            "delete" => SqlLogOperation::Delete,
            "recover" => SqlLogOperation::Recover,
            other => panic!("unsupported fixture operation {other}"),
        };
        let source = nodes(&case["source"]);
        let before = source.clone();
        let backend = case["backend"].as_str().unwrap();
        let actual = canonical_sql_trace_path(operation, backend, &source);
        assert_eq!(actual, nodes(&case["expectedPath"]), "{id}: path");
        assert_eq!(
            canonical_sql_trace_path(operation, backend, &actual),
            actual,
            "{id}: idempotence"
        );
        for (key, kind) in [
            ("comment", TraceKind::Comment),
            ("purpose", TraceKind::Purpose),
            ("auditReason", TraceKind::AuditReason),
        ] {
            assert_eq!(
                trace_value(&source, kind).as_deref(),
                case["expectedIntent"][key].as_str(),
                "{id}: {key}"
            );
        }
        assert_eq!(source, before, "{id}: preserve input nodes");
        println!("PASS shared SQL vector {id}");
    }
}

#[test]
fn executed_sql_projects_owned_root_reason_without_changing_local_lineage_or_pure_fold() {
    use teaql_data_service::{DataServiceOperation, ExecutionMetadata};
    let context = crate::UserContext::new();
    let lineage = vec![
        TraceNode::typed(TraceKind::AuditReason, "Order", Some(100), "submit order"),
        TraceNode::typed(
            TraceKind::AuditReason,
            "Payment",
            Some(201),
            "authorize payment",
        ),
    ];
    assert_eq!(
        trace_value(&lineage, TraceKind::AuditReason).as_deref(),
        Some("authorize payment")
    );
    for operation in [
        DataServiceOperation::Insert,
        DataServiceOperation::Update,
        DataServiceOperation::Delete,
        DataServiceOperation::Recover,
        DataServiceOperation::Query,
    ] {
        context.clear_sql_logs();
        let mut metadata = ExecutionMetadata::unrecorded_query(1);
        metadata.backend = "sqlite".into();
        metadata.operation = operation;
        metadata.trace_chain = lineage.clone();
        metadata.comment = Some("submit order".into());
        context.record_metadata_log(&metadata);
        let logs = context.sql_logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].audit_reason.as_deref(), Some("submit order"));
        assert_eq!(
            metadata.trace_chain, lineage,
            "projection must not mutate per-entity responsibility"
        );
        assert!(
            logs[0]
                .trace_path
                .iter()
                .all(|node| node.kind != TraceKind::AuditReason)
        );
    }
    // Request-owned intent is invocation-local, not remembered on Context.
    context.clear_sql_logs();
    let mut independent = ExecutionMetadata::unrecorded_query(1);
    independent.backend = "sqlite".into();
    independent.comment = Some("independent query".into());
    independent.trace_chain = vec![TraceNode::typed(
        TraceKind::Comment,
        "Order",
        None,
        "independent query",
    )];
    context.record_metadata_log(&independent);
    assert_eq!(context.sql_logs()[0].audit_reason, None);
}
