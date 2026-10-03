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
