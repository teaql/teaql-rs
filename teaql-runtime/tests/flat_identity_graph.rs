use teaql_core::{Entity as _, SmartList, Value};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_runtime::{EntityGraphBuilder, EntityKey, EntityRuntimeState, LedgerEntity as _};

#[derive(Debug, PartialEq, Eq)]
struct Vendor {
    id: u64,
    name: String,
}

#[teaql_entity]
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "LoadedOrder")]
struct LoadedOrder {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
}

#[test]
fn macro_hydration_shares_identity_graph_without_reusing_mutation_ownership() {
    let graph = EntityRuntimeState::default();
    let mut builder = EntityGraphBuilder::default();
    builder.install(
        1,
        Vendor {
            id: 1,
            name: "Read-only Vendor".to_owned(),
        },
    );
    graph.freeze_graph(builder).unwrap();
    let decode = |id, version| {
        LoadedOrder::from_compact_row_with_context(
            teaql_core::CompactRow::from_map(std::collections::BTreeMap::from([
                ("id".to_owned(), Value::U64(id)),
                ("version".to_owned(), Value::I64(version)),
                ("name".to_owned(), Value::from("loaded order")),
            ])),
            &graph,
        )
        .unwrap()
    };
    let mut left = decode(1, 3);
    let mut right = decode(2, 8);
    let left_state = left.entity_runtime_state().unwrap();
    let right_state = right.entity_runtime_state().unwrap();
    assert_ne!(
        left_state, right_state,
        "hydration must allocate independent ledger ownership"
    );
    assert!(std::ptr::eq(
        left_state.resolve_entity::<Vendor>(1).unwrap(),
        right_state.resolve_entity::<Vendor>(1).unwrap(),
    ));
    left.set_comment("revise order A".to_owned());
    right.set_comment("revise order B".to_owned());
    let left_key = EntityKey::new("LoadedOrder", 1_u64);
    let right_key = EntityKey::new("LoadedOrder", 2_u64);
    left_state.set(left_key.clone(), "name", Value::from("A"));
    right_state.set(right_key.clone(), "name", Value::from("B"));
    assert_eq!(left_state.get(&right_key, "name"), None);
    assert_eq!(right_state.get(&left_key, "name"), None);
    assert_eq!(left_state.get_entity_comment(&right_key), None);
    assert_eq!(right_state.get_entity_comment(&left_key), None);
    assert_eq!(left_state.get_original_version(&left_key), Some(3));
    assert_eq!(right_state.get_original_version(&right_key), Some(8));
    assert!(graph.current_change_set().changes().is_empty());
    assert_eq!(graph.get_entity_comment(&left_key), None);
    assert_eq!(graph.get_entity_comment(&right_key), None);
}

#[test]
fn roots_share_flat_entities_without_sharing_mutation_ledgers() {
    let graph_root = EntityRuntimeState::default();
    let first_trip_root = EntityRuntimeState::default().with_shared_graph(&graph_root);
    let second_trip_root = EntityRuntimeState::default().with_shared_graph(&graph_root);

    let mut builder = EntityGraphBuilder::default();
    builder.install(
        7,
        Vendor {
            id: 7,
            name: "Vendor A".to_owned(),
        },
    );
    graph_root.freeze_graph(builder).expect("freeze graph");

    let first_vendor = first_trip_root
        .resolve_entity::<Vendor>(7)
        .expect("first trip resolves vendor");
    let second_vendor = second_trip_root
        .resolve_entity::<Vendor>(7)
        .expect("second trip resolves vendor");
    assert!(std::ptr::eq(first_vendor, second_vendor));

    let trip_key = EntityKey::new("NycYellowTrip", 1_u64);
    first_trip_root.set(trip_key.clone(), "total_amount", Value::I64(100));
    assert_eq!(
        first_trip_root.get(&trip_key, "total_amount"),
        Some(Value::I64(100))
    );
    assert_eq!(second_trip_root.get(&trip_key, "total_amount"), None);
}

#[test]
fn shared_read_only_reference_preserves_snapshot_version_and_root_local_intent() {
    #[derive(Debug)]
    struct Platform {
        state: EntityRuntimeState,
    }

    let graph_root = EntityRuntimeState::default();
    let mut left = EntityRuntimeState::default().with_shared_graph(&graph_root);
    let mut right = EntityRuntimeState::default().with_shared_graph(&graph_root);
    for (state, id, version) in [(&mut left, 1_u64, 3_i64), (&mut right, 2, 8)] {
        state.set_original_compact_row(
            "CustomerOrder",
            teaql_core::CompactRow::from_map(std::collections::BTreeMap::from([
                ("id".to_owned(), Value::U64(id)),
                ("version".to_owned(), Value::I64(version)),
                ("platform_id".to_owned(), Value::U64(1)),
            ])),
        );
    }
    let mut reference = EntityRuntimeState::default();
    reference.set_original_compact_row(
        "Platform",
        teaql_core::CompactRow::from_map(std::collections::BTreeMap::from([
            ("id".to_owned(), Value::U64(1)),
            ("version".to_owned(), Value::I64(11)),
            (
                "name".to_owned(),
                Value::Text("Unmodified Platform".to_owned()),
            ),
        ])),
    );
    let reference_snapshot = reference.original_snapshot();
    let reference_state = reference.clone();
    let mut builder = EntityGraphBuilder::default();
    builder.install(1, Platform { state: reference });
    graph_root.freeze_graph(builder).unwrap();
    let left_reference = left.resolve_entity::<Platform>(1).unwrap();
    let right_reference = right.resolve_entity::<Platform>(1).unwrap();
    assert!(std::ptr::eq(left_reference, right_reference));
    assert_ne!(left, right);
    assert_ne!(left, reference_state);
    assert_ne!(right, reference_state);

    let left_key = EntityKey::new("CustomerOrder", 1_u64);
    let right_key = EntityKey::new("CustomerOrder", 2_u64);
    let reference_key = EntityKey::new("Platform", 1_u64);
    left.set(left_key.clone(), "description", Value::from("graph A"));
    right.set(right_key.clone(), "description", Value::from("graph B"));
    left.set_comment("revise graph A");
    right.set_comment("revise graph B");
    left.set_entity_comment(left_key.clone(), "local graph A");
    right.set_entity_comment(right_key.clone(), "local graph B");

    // This is the boundary that graph composition must preserve: same snapshot
    // pointer, but neither the reference nor the other root acquires this intent.
    assert_eq!(left.get(&right_key, "description"), None);
    assert_eq!(right.get(&left_key, "description"), None);
    assert_eq!(left.get_entity_comment(&right_key), None);
    assert_eq!(right.get_entity_comment(&left_key), None);
    assert_eq!(left.get_original_version(&left_key), Some(3));
    assert_eq!(right.get_original_version(&right_key), Some(8));
    assert_eq!(left.get_original_version(&reference_key), None);
    assert_eq!(
        reference_state.get_original_version(&reference_key),
        Some(11)
    );
    assert!(reference_state.current_change_set().changes().is_empty());
    assert_eq!(reference_state.get_comment(), None);
    assert!(reference_state.new_keys().is_empty());
    assert!(reference_state.deleted_keys().is_empty());
    assert_eq!(reference_state.original_snapshot(), reference_snapshot);
    assert_eq!(left_reference.state, reference_state);
    assert_eq!(right_reference.state, reference_state);
}

#[test]
fn the_same_u64_id_is_namespaced_by_entity_type() {
    let root = EntityRuntimeState::default();
    let mut builder = EntityGraphBuilder::default();
    builder.install(
        7,
        Vendor {
            id: 7,
            name: "Vendor".to_owned(),
        },
    );
    builder.install(7, String::from("not a vendor"));
    root.freeze_graph(builder).expect("freeze graph");

    assert_eq!(root.resolve_entity::<Vendor>(7).unwrap().name, "Vendor");
    assert_eq!(
        root.resolve_entity::<String>(7).unwrap().as_str(),
        "not a vendor"
    );
}

#[test]
fn frozen_graph_exposes_a_stable_typed_to_many_view() {
    let root = EntityRuntimeState::default();
    let mut builder = EntityGraphBuilder::default();
    builder.install_relation_list(
        "Vendor",
        7,
        "trips",
        SmartList::new(vec![
            Vendor {
                id: 11,
                name: "first".to_owned(),
            },
            Vendor {
                id: 12,
                name: "second".to_owned(),
            },
        ]),
    );
    root.freeze_graph(builder).expect("freeze graph");

    let first = root
        .resolve_relation_list::<Vendor>("Vendor", 7, "trips")
        .expect("typed relation list");
    let second = root
        .resolve_relation_list::<Vendor>("Vendor", 7, "trips")
        .expect("same typed relation list");
    assert_eq!(first.data.len(), 2);
    assert_eq!(first.data[1].name, "second");
    assert!(std::ptr::eq(first, second));
    assert!(
        root.resolve_relation_list::<String>("Vendor", 7, "trips")
            .is_none()
    );
}

#[test]
fn frozen_graph_distinguishes_loaded_null_from_missing_to_one_view() {
    let root = EntityRuntimeState::default();
    let mut builder = EntityGraphBuilder::default();
    builder.install_relation_option::<Vendor>("Garage", 7, "primary_vehicle", None);
    root.freeze_graph(builder).expect("freeze graph");

    assert_eq!(
        root.resolve_relation_option::<Vendor>("Garage", 7, "primary_vehicle"),
        Some(&None)
    );
    assert!(root.has_relation_view("Garage", 7, "primary_vehicle"));
    assert!(!root.has_relation_view("Garage", 7, "backup_vehicle"));
}
