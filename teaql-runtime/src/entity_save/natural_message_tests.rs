use super::*;
use crate::{InMemoryMetadataStore, checker_messages::CheckerPresentationModel};
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};

#[test]
fn graph_preflight_resolves_child_errors_against_graph_origin_not_declaring_type() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/natural-messages/cases-v1.json"
    ))
    .unwrap();
    let model = CheckerPresentationModel::from_json(
        &serde_json::json!({
            "schema":"teaql.checker-model/v1", "revision":fixture["revision"],
            "entities":fixture["entities"], "currentObject":fixture["currentObject"]
        })
        .to_string(),
    )
    .unwrap();
    let mut context = UserContext::new().with_metadata(
        InMemoryMetadataStore::new()
            .with_entity(EntityDescriptor::new("Catalog"))
            .with_entity(
                EntityDescriptor::new("Entry")
                    .property(PropertyDescriptor::new("name", DataType::Text).not_null()),
            ),
    );
    context.set_checker_presentation_model(Arc::new(model));
    let mut node = GraphNode::new("Catalog")
        .relation(
            "entry_list",
            GraphNode::new("Entry")
                .operation(GraphOperation::Create)
                .value("name", "valid"),
        )
        .relation(
            "entry_list",
            GraphNode::new("Entry").operation(GraphOperation::Create),
        );
    let Err(RuntimeError::Check(results)) =
        preflight_graph(&context, &mut node, &ObjectLocation::root(), None)
    else {
        panic!("must reject missing child name")
    };
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entity_type.as_deref(), Some("Entry"));
    assert_eq!(results[0].location.model_path(), "entry_list[1].name");
    assert_eq!(
        results[0].message.as_deref(),
        Some("The name of the 2nd entry is required.")
    );
    // A later independent invocation must not reuse Catalog as its origin.
    let mut root = vec![crate::CheckResult::required(ObjectLocation::root())];
    context.translate_check_results(&mut root);
    assert_eq!(
        root[0].message.as_deref(),
        Some("The current object is required.\nLocation: current object")
    );
}
