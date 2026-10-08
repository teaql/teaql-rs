use super::*;
use crate::LedgerEntity;
use std::collections::BTreeMap;
use std::sync::Arc;
use teaql_core::{TeaqlEntity, eval::LoadState};

#[teaql_macros::teaql_entity]
#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "JsonProbe", indexed_layout)]
struct JsonProbe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: Option<String>,
    address: Option<String>,
    count: i64,
    active: bool,
    ratio: f64,
    price: Decimal,
    birthday: chrono::NaiveDate,
    happened_at: teaql_core::time::Timestamp,
    raw: serde_json::Value,
    #[teaql(column = "base_url")]
    display_name: Option<String>,
    #[teaql(relation(target = "JsonProbe", local_key = "id", foreign_key = "id"))]
    parent: Option<Box<JsonProbe>>,
    #[teaql(dynamic)]
    properties: BTreeMap<String, Value>,
    #[teaql(skip)]
    __load_state: LoadState,
}

impl JsonProbe {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "json-probe-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("version", 1),
        ("name", 2),
        ("address", 3),
        ("count", 4),
        ("active", 5),
        ("ratio", 6),
        ("price", 7),
        ("birthday", 8),
        ("happened_at", 9),
        ("raw", 10),
        ("base_url", 11),
    ];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("name", "name", "name"),
        ("address", "address", "address"),
        ("count", "count", "count"),
        ("active", "active", "active"),
        ("ratio", "ratio", "ratio"),
        ("price", "price", "price"),
        ("birthday", "birthday", "birthday"),
        ("happened_at", "happened_at", "happened_at"),
        ("raw", "raw", "raw"),
        ("base_url", "display_name", "base_url"),
    ];
}

#[teaql_macros::teaql_entity]
#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(
    entity = "JsonParent",
    indexed_layout,
    reverse_relation(
        name = "child_list",
        target = "JsonChild",
        local_key = "id",
        foreign_key = "parent_id",
        many,
        json_type = "GraphChild"
    )
)]
struct GraphParent {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: Option<String>,
    #[teaql(skip)]
    __load_state: LoadState,
}
impl GraphParent {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "json-parent-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("name", 2)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("name", "name", "name"),
    ];
}
#[teaql_macros::teaql_entity]
#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "JsonChild", indexed_layout)]
struct GraphChild {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: Option<String>,
    #[teaql(column = "parent")]
    parent_id: Option<u64>,
    #[teaql(relation(target = "JsonParent", local_key = "parent_id", foreign_key = "id"))]
    parent: Option<Box<GraphParent>>,
    #[teaql(column = "backup_parent")]
    backup_parent_id: Option<u64>,
    #[teaql(relation(
        target = "JsonParent",
        local_key = "backup_parent_id",
        foreign_key = "id"
    ))]
    backup_parent: Option<Box<GraphParent>>,
    #[teaql(skip)]
    __load_state: LoadState,
}
impl GraphChild {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "json-child-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("version", 1),
        ("name", 2),
        ("parent", 3),
        ("backup_parent", 4),
    ];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("name", "name", "name"),
        ("parent", "parent_id", "parent"),
        ("backup_parent", "backup_parent_id", "backup_parent"),
    ];
}
fn graph_context() -> UserContext {
    crate::RuntimeModule::new()
        .entity::<GraphParent>()
        .entity::<GraphChild>()
        .into_context()
}

#[test]
fn graph_json_restores_typed_children_without_widening_repeated_identity_views() {
    use crate::LedgerEntity;
    let input = serde_json::json!({"id":1,"version":7,"name":"parent","child_list":[
        {"id":1,"version":7,"name":"first","parent_id":1,"parent":{"id":1,"version":7,"name":"parent"}},
        {"id":2,"version":7,"name":null,"parent_id":1}
    ]});
    let ctx = graph_context();
    let root = ctx.decode_json_entity::<GraphParent>(&input).unwrap();
    let state = root.entity_runtime_state().unwrap();
    let list = state
        .resolve_relation_list::<GraphChild>("JsonParent", 1, "child_list")
        .unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].name.as_deref(), Some("first"));
    assert!(list[1].name.is_none());
    assert!(list[0].is_field_loaded("parent"));
    assert!(!list[1].is_field_loaded("parent"));
    let child_state = list[0].entity_runtime_state().unwrap();
    let parent = child_state
        .resolve_relation_option::<GraphParent>("JsonChild", 1, "parent")
        .unwrap()
        .as_ref()
        .unwrap();
    assert_eq!(parent.name.as_deref(), Some("parent"));
    assert!(
        std::ptr::eq(
            parent,
            child_state.resolve_entity::<GraphParent>(1).unwrap()
        ),
        "identity lookup must borrow the existing option entity, not a duplicate"
    );
    assert!(!parent.is_field_loaded("child_list"));
    assert!(root.dirty_fields().is_none());
    assert!(list.iter().all(|child| child.dirty_fields().is_none()));
    assert_eq!(root.clone().into_json(), input);
    let other = ctx.decode_json_entity::<GraphParent>(&input).unwrap();
    state.set(
        crate::EntityKey::new("JsonParent", 1),
        "name",
        Value::Text("one root".into()),
    );
    assert!(root.dirty_fields().is_some());
    assert!(other.dirty_fields().is_none());
}

#[test]
fn graph_json_preserves_empty_omitted_null_and_fk_identity() {
    let ctx = graph_context();
    for input in [
        serde_json::json!({"id":1,"child_list":[]}),
        serde_json::json!({"id":1}),
    ] {
        let root = ctx.decode_json_entity::<GraphParent>(&input).unwrap();
        assert_eq!(
            root.is_field_loaded("child_list"),
            input.get("child_list").is_some()
        );
        assert_eq!(root.into_json(), input);
    }
    for input in [
        serde_json::json!({"id":2,"parent_id":null,"parent":null}),
        serde_json::json!({"id":2,"parent_id":1}),
        serde_json::json!({"id":2,"parent":{"id":1}}),
    ] {
        let root = ctx.decode_json_entity::<GraphChild>(&input).unwrap();
        assert!(root.is_field_loaded("parent_id"));
        assert_eq!(
            root.is_field_loaded("parent"),
            input.get("parent").is_some()
        );
        let output = root.into_json();
        assert_eq!(output.get("parent"), input.get("parent"));
    }
}

#[test]
fn graph_input_rejects_conflicting_keys_malformed_edges_and_missing_decoders() {
    let ctx = graph_context();
    for input in [
        serde_json::json!({"id":2,"parent_id":1,"parent":{"id":3}}),
        serde_json::json!({"id":2,"parent_id":1,"parent":null}),
        serde_json::json!({"id":2,"parent":[]}),
        serde_json::json!({"id":2,"parent":{"name":"PRIVATE_LITERAL"}}),
    ] {
        let err = ctx.decode_json_entity::<GraphChild>(&input).unwrap_err();
        assert!(!err.to_string().contains("PRIVATE_LITERAL"));
    }
    for input in [
        serde_json::json!({"id":1,"child_list":null}),
        serde_json::json!({"id":1,"child_list":[null]}),
        serde_json::json!({"child_list":[]}),
        serde_json::json!({"id":1,"child_list":[{"id":1,"parent_id":999}]}),
        serde_json::json!({"id":1,"child_list":[{"id":1,"name":123}]}),
    ] {
        assert!(ctx.decode_json_entity::<GraphParent>(&input).is_err());
    }
    let missing = UserContext::new().with_metadata(
        crate::InMemoryMetadataStore::new()
            .with_entity(GraphParent::entity_descriptor())
            .with_entity(GraphChild::entity_descriptor()),
    );
    assert!(
        missing
            .decode_json_entity::<GraphParent>(&serde_json::json!({"id":1,"child_list":[]}))
            .is_err()
    );
    let mut bad = GraphChild::entity_descriptor();
    bad.properties[0].data_type = DataType::Text;
    let mut bad_context = graph_context();
    bad_context.set_metadata(
        crate::InMemoryMetadataStore::new()
            .with_entity(GraphParent::entity_descriptor())
            .with_entity(bad),
    );
    assert!(
        bad_context
            .decode_json_entity::<GraphParent>(&serde_json::json!({"id":1,"child_list":[]}))
            .is_err()
    );
}

#[test]
fn independently_decoded_root_graphs_with_equal_ids_do_not_share_child_payloads() {
    use crate::LedgerEntity;
    let roots = graph_context()
        .decode_json_entities::<GraphParent>(&serde_json::json!([
            {"id":1,"child_list":[{"id":1,"name":"first"}]},
            {"id":1,"child_list":[{"id":1,"name":"second"}]}
        ]))
        .unwrap();
    assert!(Arc::ptr_eq(
        &roots[0].loaded_state_snapshot().unwrap(),
        &roots[1].loaded_state_snapshot().unwrap()
    ));
    for (root, name) in roots.iter().zip(["first", "second"]) {
        let state = root.entity_runtime_state().unwrap();
        let rows = state
            .resolve_relation_list::<GraphChild>("JsonParent", 1, "child_list")
            .unwrap();
        assert_eq!(rows[0].name.as_deref(), Some(name));
        assert!(rows[0].dirty_fields().is_none());
    }
}

#[test]
fn incompatible_forward_target_views_do_not_silently_choose_one_payload() {
    let value = serde_json::json!({"id":1,"child_list":[
        {"id":1,"parent_id":1,"parent":{"id":1,"name":"first"}},
        {"id":2,"parent_id":1,"parent":{"id":1,"name":"second"}}
    ]});
    let error = graph_context()
        .decode_json_entity::<GraphParent>(&value)
        .unwrap_err();
    assert!(error.to_string().contains("conflicting native values"));
    assert!(!error.to_string().contains("first"));
    assert!(!error.to_string().contains("second"));
}

#[test]
fn one_graph_preserves_forward_projection_and_reverse_list_views_of_equal_ids() {
    let input = serde_json::json!({"id":1,"version":7,"name":"shared","child_list":[
        {"id":1,"parent_id":1,"parent":{"id":1,"version":7,"name":"shared","child_list":[]}},
        {"id":2,"parent_id":1,"parent":{"id":1,"version":7}}
    ]});
    let root = graph_context()
        .decode_json_entity::<GraphParent>(&input)
        .unwrap();
    let root_state = root.entity_runtime_state().unwrap();
    let list = root_state
        .resolve_relation_list::<GraphChild>("JsonParent", 1, "child_list")
        .unwrap();
    let first_state = list[0].entity_runtime_state().unwrap();
    let second_state = list[1].entity_runtime_state().unwrap();
    let first = first_state.resolve_entity::<GraphParent>(1).unwrap();
    let second = second_state.resolve_entity::<GraphParent>(1).unwrap();
    assert_eq!(first.name.as_deref(), Some("shared"));
    assert!(first.is_field_loaded("name"));
    assert!(!second.is_field_loaded("name"));
    assert!(second.name.is_none());
    assert!(
        first
            .entity_runtime_state()
            .unwrap()
            .resolve_relation_list::<GraphChild>("JsonParent", 1, "child_list")
            .unwrap()
            .is_empty()
    );
    assert!(
        second
            .entity_runtime_state()
            .unwrap()
            .resolve_relation_list::<GraphChild>("JsonParent", 1, "child_list")
            .is_none()
    );
    assert!(Arc::ptr_eq(
        &root.loaded_state_snapshot().unwrap(),
        &first.loaded_state_snapshot().unwrap()
    ));
    assert!(!Arc::ptr_eq(
        &first.loaded_state_snapshot().unwrap(),
        &second.loaded_state_snapshot().unwrap()
    ));
    assert!(list.iter().all(|child| child.dirty_fields().is_none()));
    let detached = first.clone();
    assert_eq!(root.clone().into_json(), input);
    drop(first_state);
    drop(second_state);
    drop(root_state);
    drop(root);
    assert_eq!(detached.name.as_deref(), Some("shared"));
    assert!(
        detached
            .entity_runtime_state()
            .unwrap()
            .resolve_relation_list::<GraphChild>("JsonParent", 1, "child_list")
            .unwrap()
            .is_empty()
    );
    assert_eq!(detached.into_json(), input["child_list"][0]["parent"]);
}

#[test]
fn two_relations_to_equal_identity_keep_distinct_views_and_refuse_ambiguous_lookup() {
    let input = serde_json::json!({"id":1,"parent_id":7,"parent":{"id":7,"name":"shared"},
        "backup_parent_id":7,"backup_parent":{"id":7}});
    let row = graph_context()
        .decode_json_entity::<GraphChild>(&input)
        .unwrap();
    let state = row.entity_runtime_state().unwrap();
    let first = state
        .resolve_relation_option::<GraphParent>("JsonChild", 1, "parent")
        .unwrap()
        .as_ref()
        .unwrap();
    let second = state
        .resolve_relation_option::<GraphParent>("JsonChild", 1, "backup_parent")
        .unwrap()
        .as_ref()
        .unwrap();
    assert!(first.is_field_loaded("name"));
    assert!(!second.is_field_loaded("name"));
    assert!(
        state.resolve_entity::<GraphParent>(7).is_none(),
        "identity alone cannot select an ambiguous projection"
    );
    assert_eq!(row.into_json(), input);
}

#[test]
fn inferred_foreign_keys_cannot_conflict_across_views_of_one_identity() {
    let value = serde_json::json!({"id":1,"child_list":[
        {"id":2,"parent_id":1,"backup_parent":{"id":7}},
        {"id":2,"parent_id":1,"backup_parent":{"id":8}}
    ]});
    let error = graph_context()
        .decode_json_entity::<GraphParent>(&value)
        .unwrap_err();
    assert!(error.to_string().contains("conflicting native values"));
}

#[test]
fn graph_depth_is_bounded_before_hydration() {
    let mut value = serde_json::json!({"id":1});
    for _ in 0..66 {
        value = serde_json::json!({"id":1,"parent":value});
    }
    let err = context()
        .decode_json_entity::<JsonProbe>(&value)
        .unwrap_err();
    assert!(err.to_string().contains("depth or node limit"));
}

#[test]
fn overlarge_relation_rejects_before_allocating_or_decoding_child_views() {
    let value = serde_json::json!({"id":1,"child_list":
        vec![serde_json::json!({"unknown_child_field":"must not be decoded"}); 100_000]});
    let error = graph_context()
        .decode_json_entity::<GraphParent>(&value)
        .unwrap_err();
    assert!(error.to_string().contains("depth or node limit"));
    assert!(!error.to_string().contains("unknown_child_field"));
    assert!(!error.to_string().contains("must not be decoded"));
}

fn context() -> UserContext {
    crate::RuntimeModule::new()
        .entity::<JsonProbe>()
        .into_context()
}

#[test]
fn native_json_keeps_null_omission_falsy_dates_decimal_and_no_mutation() {
    let input = serde_json::json!({"id":1,"version":7,"name":null,"count":0,"active":false,
        "ratio":3.5,"price":"123.450","birthday":"2024-02-29","happened_at":1700000000123_i64,
        "raw":{"literal":"_comment"},"base_url":"","_name":"derived","_nil":null,"_count":0});
    let row = context().decode_json_entity::<JsonProbe>(&input).unwrap();
    assert!(row.is_field_loaded("name"));
    assert!(row.name.is_none());
    assert!(!row.is_field_loaded("address"));
    assert!(row.address.is_none());
    assert_eq!(row.count, 0);
    assert!(!row.active);
    assert_eq!(row.ratio, 3.5);
    assert_eq!(row.price.to_string(), "123.450");
    assert_eq!(row.birthday.to_string(), "2024-02-29");
    assert_eq!(row.happened_at.0, 1700000000123);
    assert_eq!(row.display_name.as_deref(), Some(""));
    assert!(row.dirty_fields().is_none());
    assert!(!row.is_new());
    assert!(!row.is_marked_as_delete());
    assert!(!row.has_pending_dynamic_mutations());
    let json = row.clone().into_json();
    assert_eq!(json["_name"], "derived");
    assert!(json["_nil"].is_null());
    assert_eq!(json["_count"], 0);
    assert!(json.get("address").is_none());
    assert_eq!(json["name"], serde_json::Value::Null);
    let restored = context().decode_json_entity::<JsonProbe>(&json).unwrap();
    assert_eq!(json, restored.into_json());
}

#[test]
fn batch_actual_shapes_share_snapshots_and_keep_values_and_ledgers_private() {
    let rows = context().decode_json_entities::<JsonProbe>(&serde_json::json!([
        {"id":1,"version":7,"name":null},{"version":7,"name":"private","id":1},{"id":2,"version":7}
    ])).unwrap();
    assert!(Arc::ptr_eq(
        &rows[0].loaded_state_snapshot().unwrap(),
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    assert!(!Arc::ptr_eq(
        &rows[0].loaded_state_snapshot().unwrap(),
        &rows[2].loaded_state_snapshot().unwrap()
    ));
    assert!(rows[0].name.is_none());
    assert_eq!(rows[1].name.as_deref(), Some("private"));
    assert!(!rows[2].is_field_loaded("name"));
    let state = rows[0]
        .__teaql_runtime_state_any()
        .unwrap()
        .downcast_ref::<crate::EntityRuntimeState>()
        .unwrap();
    state.set(
        crate::EntityKey::new("JsonProbe", 1),
        "name",
        Value::Text("one private mutation".into()),
    );
    assert!(rows[0].dirty_fields().is_some());
    assert!(rows[1].dirty_fields().is_none());
    assert!(rows[2].dirty_fields().is_none());
}

#[test]
fn aliases_and_unsupported_input_reject_without_exposing_values() {
    let ctx = context();
    for input in [
        serde_json::json!({"unknown":"PRIVATE_LITERAL"}),
        serde_json::json!({"name":42}),
        serde_json::json!({"active":null}),
        serde_json::json!({"count":1.5}),
        serde_json::json!({"id":-1}),
        serde_json::json!({"birthday":"PRIVATE_LITERAL"}),
        serde_json::json!({"_original_values":{}}),
        serde_json::json!({"#name":"PRIVATE_LITERAL"}),
        serde_json::json!({"display_name":"A","base_url":"B"}),
        serde_json::json!({"parent":{"id":1,"name":"PRIVATE_LITERAL"}}),
    ] {
        let error = ctx.decode_json_entity::<JsonProbe>(&input).unwrap_err();
        assert!(!error.to_string().contains("PRIVATE_LITERAL"));
    }
    assert!(
        UserContext::new()
            .decode_json_entity::<JsonProbe>(&serde_json::json!({"id":1}))
            .is_err()
    );
    let mut descriptor = JsonProbe::entity_descriptor();
    descriptor.properties[0].data_type = DataType::Text;
    let wrong = UserContext::new()
        .with_metadata(crate::InMemoryMetadataStore::new().with_entity(descriptor));
    assert!(
        wrong
            .decode_json_entity::<JsonProbe>(&serde_json::json!({"id":"PRIVATE_LITERAL"}))
            .is_err()
    );
}

#[test]
fn native_batches_construct_geometry_per_shape_not_per_row() {
    let descriptor = JsonProbe::entity_descriptor();
    let null = serde_json::json!({"id":1,"version":7,"display_name":null});
    let value = serde_json::json!({"base_url":"private","version":7,"id":2});
    let minimal = serde_json::json!({"id":3,"version":7});
    for count in [1, 100, 10_000] {
        let mut shapes = NativeJsonShapes::default();
        let mut rows = Vec::with_capacity(count);
        for index in 0..count {
            let row = native_row::<JsonProbe>(
                &descriptor,
                if index % 2 == 0 { &null } else { &value },
                &mut shapes,
            )
            .unwrap();
            if let Some(first) = rows.first() {
                assert!(Arc::ptr_eq(
                    &row.shared_layout(),
                    &CompactRow::shared_layout(first)
                ));
            }
            rows.push(row);
        }
        assert_eq!(shapes.layouts.len(), 1);
        let other = native_row::<JsonProbe>(&descriptor, &minimal, &mut shapes).unwrap();
        assert_eq!(shapes.layouts.len(), 2);
        assert!(!Arc::ptr_eq(
            &rows[0].shared_layout(),
            &other.shared_layout()
        ));
        drop(shapes);
        let first = JsonProbe::from_compact_row(rows.remove(0)).unwrap();
        assert!(first.is_field_loaded("display_name"));
        assert!(first.display_name.is_none());
        assert!(
            !JsonProbe::from_compact_row(other)
                .unwrap()
                .is_field_loaded("display_name")
        );
    }
}
