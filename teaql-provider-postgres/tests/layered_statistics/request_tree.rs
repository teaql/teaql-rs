use super::*;

#[tokio::test]
async fn live_combined_statement_keeps_multihop_and_independent_request_branches() {
    use teaql_core::{QueryOperation, QueryOperationKind, QueryRequestTraceNode};
    use teaql_runtime::log_formatter::{HumanReaderFormatter, LogFormatter};
    let Some((context, _pool)) = fixture().await else {
        return;
    };
    let descriptors = descriptors();
    let kind = descriptors
        .iter()
        .find(|entity| entity.name == "Kind")
        .unwrap()
        .clone();
    let platform = descriptors
        .iter()
        .find(|entity| entity.name == "Platform")
        .unwrap()
        .clone();
    let eligible = SelectQuery::new("Platform")
        .group_by("id")
        .count("eligible")
        .limit(10)
        .comment("Read independently eligible platforms");
    let wrapper = SelectQuery::new("Kind")
        .project("id")
        .limit(10)
        .filter(Expr::in_subquery("platform_id", platform, eligible, "id"))
        .comment("Route through ordinary type identities");
    let grandparent = SelectQuery::new("Platform")
        .group_by("name")
        .count("platforms")
        .comment("Group visible platform population");
    let mut parent = SelectQuery::new("Kind")
        .count("types")
        .object_group_by("platform", "platform_id", grandparent)
        .comment("Group visible type population");
    parent.object_group_bys[0].operation = Some(QueryOperation::new(
        QueryOperationKind::Aggregate,
        "roll_up_to_platform",
    ));
    let mut query = SelectQuery::new("School")
        .filter(Expr::lte("id", 5_i64))
        .and_filter(Expr::in_subquery("kind_id", kind, wrapper, "id"))
        .count("schools")
        .sum("capacity", "total")
        .object_group_by("kind", "kind_id", parent)
        .limit(10)
        .comment("Count source observations through two parent layers");
    query.object_group_bys[0].operation = Some(QueryOperation::new(
        QueryOperationKind::Aggregate,
        "roll_up_to_kind",
    ));
    for round in 1..=2 {
        context.clear_sql_logs();
        let rows = fetch(&context, query.clone()).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("schools").and_then(Value::try_i64), Some(5));
        assert_eq!(rows[0].get("total").and_then(Value::try_i64), Some(3300));
        assert_eq!(
            object(&rows[0], "kind")
                .get("types")
                .and_then(Value::try_i64),
            Some(2)
        );
        let logs = context.sql_logs();
        assert_eq!(logs.len(), 1, "one actual combined SQL statement");
        let nodes = logs[0].log_context.request_tree.as_deref().unwrap();
        let find = |detail| nodes.iter().find(|node| node.detail == detail).unwrap();
        let parent = find("Group visible type population");
        let grandparent = find("Group visible platform population");
        let wrapper = find("Route through ordinary type identities");
        let eligible = find("Read independently eligible platforms");
        assert_eq!(
            nodes[parent.parent_id.unwrap()].method.as_deref(),
            Some("roll_up_to_kind")
        );
        let grand_operation = &nodes[grandparent.parent_id.unwrap()];
        assert_eq!(
            grand_operation.method.as_deref(),
            Some("roll_up_to_platform")
        );
        assert_eq!(grand_operation.parent_id, Some(parent.id));
        assert_eq!(wrapper.parent_id, Some(1));
        assert_eq!(
            nodes[eligible.parent_id.unwrap()].parent_id,
            Some(wrapper.id)
        );
        for (index, node) in nodes.iter().enumerate() {
            assert_eq!(node.id, index);
            assert!(node.parent_id.is_none_or(|parent| parent < index));
        }
        let text = HumanReaderFormatter.format_sql_log(&logs[0].trace_path, &logs[0]);
        for expected in [
            "Group visible type population",
            "Group visible platform population",
            "Route through ordinary type identities",
            "Read independently eligible platforms",
            "roll_up_to_kind()",
            "roll_up_to_platform()",
        ] {
            assert!(text.contains(expected), "Missing {expected}");
        }
        println!(
            "\nCOMBINED_POSTGRES_TREE={}",
            json!({"round":round,"physicalStatements":logs.len(),
            "schools":5,"total":3300,"requestTree":QueryRequestTraceNode::to_json_value(nodes),"text":text})
        );
    }
}
