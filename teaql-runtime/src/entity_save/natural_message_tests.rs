use super::*;
use crate::{InMemoryMetadataStore, checker_messages::CheckerPresentationModel};
use std::sync::atomic::{AtomicUsize, Ordering};
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "Entry")]
struct SelectedEntry {
    #[teaql(id)]
    id: u64,
}

struct OnceChecker(Arc<AtomicUsize>);
impl crate::Checker for OnceChecker {
    fn entity(&self) -> &str {
        "Entry"
    }
    fn check_and_fix(
        &self,
        _: &UserContext,
        _: &mut crate::EntityValues,
        location: &ObjectLocation,
        results: &mut crate::CheckResults,
    ) {
        self.0.fetch_add(1, Ordering::SeqCst);
        results.push(
            crate::CheckResult::required(location.clone().member("name"))
                .with_source_instance_path("/originalInput/entryName"),
        );
    }
}

#[test]
fn selected_view_positions_override_sparse_or_identity_sorted_preflight_order() {
    for (selection, expected_path, expected_message) in [
        (
            "descending",
            "entry_list[1].name",
            "The name of the 2nd entry is required.",
        ),
        (
            "sparse",
            "entry_list[2].name",
            "The name of the 3rd entry is required.",
        ),
        (
            "forward",
            "selected_entry.name",
            "The name of the selected entry is required.",
        ),
        (
            "absent",
            "entry_list[0].name",
            "The name of the 1st entry is required.",
        ),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut context = UserContext::new()
            .with_metadata(
                InMemoryMetadataStore::new()
                    .with_entity(EntityDescriptor::new("Catalog"))
                    .with_entity(
                        EntityDescriptor::new("Entry")
                            .property(PropertyDescriptor::new("name", DataType::Text)),
                    ),
            )
            .with_checker_registry(
                crate::InMemoryCheckerRegistry::new().with_checker(OnceChecker(calls.clone())),
            );
        context.set_checker_presentation_model(Arc::new(
            CheckerPresentationModel::from_json(
                r#"{
            "schema":"teaql.checker-model/v1","revision":"selected-location-v1","entities":{
              "Catalog":{"labels":{"en":"catalog"},"fields":[
                {"name":"entry_list","targetType":"Entry","labels":{"en":"entry"}},
                {"name":"selected_entry","targetType":"Entry","labels":{"en":"selected entry"}}]},
              "Entry":{"labels":{"en":"entry"},"fields":[{"name":"name","labels":{"en":"name"}}]}}}
            "#,
            )
            .unwrap(),
        ));
        let root = crate::EntityRuntimeState::default();
        let mut graph = crate::EntityGraphBuilder::default();
        match selection {
            "descending" | "sparse" => graph.install_typed_relation_list(
                "Catalog",
                1,
                "entry_list",
                teaql_core::SmartList::from(if selection == "sparse" {
                    vec![
                        SelectedEntry { id: 30 },
                        SelectedEntry { id: 20 },
                        SelectedEntry { id: 10 },
                    ]
                } else {
                    vec![SelectedEntry { id: 20 }, SelectedEntry { id: 10 }]
                }),
            ),
            "forward" => graph.install_typed_relation_option(
                0,
                "Catalog",
                1,
                "selected_entry",
                Some(SelectedEntry { id: 10 }),
            ),
            _ => (),
        }
        root.freeze_graph(graph).unwrap();
        let mut node = GraphNode::new("Catalog")
            .value("id", 1_u64)
            .operation(GraphOperation::Reference)
            .relation(
                "entry_list",
                GraphNode::new("Entry")
                    .value("id", 10_u64)
                    .value("name", "unchanged"),
            );
        let Err(RuntimeError::Check(results)) =
            preflight_graph(&context, &mut node, &ObjectLocation::root(), Some(&root))
        else {
            panic!("the test Checker must reject");
        };
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "never replay customer Fix/Checker to relocate an error"
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].entity_type.as_deref(), Some("Entry"));
        assert_eq!(
            results[0].source_instance_path.as_deref(),
            Some("/originalInput/entryName")
        );
        assert_eq!(
            results[0].location.model_path(),
            expected_path,
            "{selection}"
        );
        assert_eq!(
            results[0].message.as_deref(),
            Some(expected_message),
            "{selection}"
        );
        assert!(
            root.current_change_set().is_empty(),
            "selected path lookup does not create mutations"
        );
    }
}

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
