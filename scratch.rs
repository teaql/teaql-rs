use std::collections::BTreeMap;
fn convert(node: teaql_core::EntityGraphNode) -> teaql_runtime::GraphNode {
    let mut relations = BTreeMap::new();
    for (rel_name, child) in node.children {
        relations.entry(rel_name).or_insert_with(Vec::new).push(convert(child));
    }
    teaql_runtime::GraphNode {
        entity: node.entity_type,
        values: node.record.into_inner(),
        relations,
        operation: match node.operation {
            teaql_core::EntityGraphOperation::Save => teaql_runtime::GraphOperation::Upsert,
            teaql_core::EntityGraphOperation::Delete => teaql_runtime::GraphOperation::Remove,
        },
        comment: node.comment,
    }
}
