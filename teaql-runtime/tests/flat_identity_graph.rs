use teaql_core::{SmartList, Value};
use teaql_runtime::{EntityGraphBuilder, EntityKey, EntityRoot};

#[derive(Debug, PartialEq, Eq)]
struct Vendor {
    id: u64,
    name: String,
}

#[test]
fn roots_share_flat_entities_without_sharing_mutation_ledgers() {
    let graph_root = EntityRoot::default();
    let first_trip_root = EntityRoot::default().with_shared_graph(&graph_root);
    let second_trip_root = EntityRoot::default().with_shared_graph(&graph_root);

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
fn the_same_u64_id_is_namespaced_by_entity_type() {
    let root = EntityRoot::default();
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
    let root = EntityRoot::default();
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
