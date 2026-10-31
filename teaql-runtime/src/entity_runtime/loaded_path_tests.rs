use super::*;

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
struct LoadedPathParent {
    #[teaql(id)]
    id: u64,
}
#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
struct LoadedPathLeaf {
    #[teaql(id)]
    id: u64,
}
fn leaf(id: u64) -> EntityKey {
    EntityKey::new("LoadedPathLeaf", id)
}

#[test]
fn sparse_ancestor_paths_keep_actual_list_indices_without_mutation_state() {
    let state = EntityRuntimeState::default();
    let mut graph = EntityGraphBuilder::default();
    graph.install_typed_relation_list(
        "Owner",
        1,
        "purchase_order_list",
        SmartList::from(vec![
            LoadedPathParent { id: 10 },
            LoadedPathParent { id: 20 },
        ]),
    );
    graph.install_typed_relation_list(
        "LoadedPathParent",
        20,
        "order_item_list",
        SmartList::from(vec![LoadedPathLeaf { id: 9 }]),
    );
    state.freeze_graph(graph).unwrap();
    let paths =
        state.loaded_object_locations(&EntityKey::new("Owner", 1_u64), &BTreeSet::from([leaf(9)]));
    assert_eq!(
        paths[&leaf(9)].instance_path(),
        "/purchaseOrderList/1/orderItemList/0"
    );
    assert_eq!(paths.len(), 1);
    assert!(!state.has_mutation_context());
}

#[test]
fn loaded_paths_do_not_union_same_owner_json_views() {
    let state = EntityRuntimeState::default().with_json_view(7);
    let mut graph = EntityGraphBuilder::default();
    graph.install_json_relation_list(
        7,
        "Owner",
        1,
        "items",
        SmartList::from(vec![LoadedPathLeaf { id: 8 }]),
    );
    graph.install_json_relation_list(
        8,
        "Owner",
        1,
        "items",
        SmartList::from(vec![LoadedPathLeaf { id: 9 }]),
    );
    state.freeze_graph(graph).unwrap();
    let paths = state.loaded_object_locations(
        &EntityKey::new("Owner", 1_u64),
        &BTreeSet::from([leaf(8), leaf(9)]),
    );
    assert_eq!(paths[&leaf(8)].instance_path(), "/items/0");
    assert!(!paths.contains_key(&leaf(9)));
}

#[test]
fn loaded_path_tie_breaking_is_stable_and_absence_is_not_invented() {
    let state = EntityRuntimeState::default();
    let mut graph = EntityGraphBuilder::default();
    for relation in ["z_items", "a_items"] {
        graph.install_typed_relation_list(
            "Owner",
            1,
            relation,
            SmartList::from(vec![LoadedPathLeaf { id: 9 }]),
        );
    }
    graph.install_typed_relation_list::<LoadedPathLeaf>("Owner", 1, "empty", SmartList::empty());
    state.freeze_graph(graph).unwrap();
    let paths = state.loaded_object_locations(
        &EntityKey::new("Owner", 1_u64),
        &BTreeSet::from([leaf(9), leaf(99)]),
    );
    assert_eq!(paths[&leaf(9)].instance_path(), "/aItems/0");
    assert!(!paths.contains_key(&leaf(99)));
}

#[test]
fn loaded_to_one_paths_terminate_cycles_without_a_list_index() {
    let state = EntityRuntimeState::default();
    let mut graph = EntityGraphBuilder::default();
    graph.install_typed_relation_option(
        0,
        "LoadedPathParent",
        1,
        "selected_leaf",
        Some(LoadedPathLeaf { id: 2 }),
    );
    graph.install_typed_relation_option(
        0,
        "LoadedPathLeaf",
        2,
        "back",
        Some(LoadedPathParent { id: 1 }),
    );
    state.freeze_graph(graph).unwrap();
    let paths = state.loaded_object_locations(
        &EntityKey::new("LoadedPathParent", 1_u64),
        &BTreeSet::from([leaf(2), leaf(99)]),
    );
    assert_eq!(paths[&leaf(2)].instance_path(), "/selectedLeaf");
    assert!(!paths.contains_key(&leaf(99)));
}

#[test]
fn same_numeric_id_different_types_keep_separate_loaded_paths() {
    let state = EntityRuntimeState::default();
    let mut graph = EntityGraphBuilder::default();
    graph.install_typed_relation_list(
        "LoadedPathParent",
        1,
        "items",
        SmartList::from(vec![LoadedPathLeaf { id: 1 }]),
    );
    state.freeze_graph(graph).unwrap();
    let parent = EntityKey::new("LoadedPathParent", 1_u64);
    let paths = state.loaded_object_locations(&parent, &BTreeSet::from([parent.clone(), leaf(1)]));
    assert!(paths[&parent].is_root());
    assert_eq!(paths[&leaf(1)].instance_path(), "/items/0");
    assert_eq!(paths.len(), 2);
}
