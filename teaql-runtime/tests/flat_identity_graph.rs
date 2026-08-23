use teaql_core::Value;
use teaql_runtime::{EntityKey, EntityRoot};

#[derive(Debug, PartialEq, Eq)]
struct Vendor {
    id: u64,
    name: String,
}

#[test]
fn roots_share_flat_entities_without_sharing_mutation_ledgers() {
    let graph_root = EntityRoot::default();
    let first_trip_root = EntityRoot::default();
    let second_trip_root = EntityRoot::default();
    first_trip_root.share_graph_from(&graph_root);
    second_trip_root.share_graph_from(&graph_root);

    graph_root.install_entity(
        7,
        Vendor {
            id: 7,
            name: "Vendor A".to_owned(),
        },
    );

    let first_vendor = first_trip_root
        .resolve_entity::<Vendor>(7)
        .expect("first trip resolves vendor");
    let second_vendor = second_trip_root
        .resolve_entity::<Vendor>(7)
        .expect("second trip resolves vendor");
    assert!(std::sync::Arc::ptr_eq(&first_vendor, &second_vendor));

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
    root.install_entity(7, Vendor { id: 7, name: "Vendor".to_owned() });
    root.install_entity(7, String::from("not a vendor"));

    assert_eq!(root.resolve_entity::<Vendor>(7).unwrap().name, "Vendor");
    assert_eq!(root.resolve_entity::<String>(7).unwrap().as_str(), "not a vendor");
}
