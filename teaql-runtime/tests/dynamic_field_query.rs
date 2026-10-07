//! Native context/provider/typed-carrier bridge. No generated domain code is patched.
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use teaql_core::dynamic_fields::DynamicFieldValues;
use teaql_core::dynamic_fields::{
    DynamicFieldDefinitions, DynamicFieldError, DynamicFieldSelection, DynamicFieldState,
};
use teaql_core::{CompactRow, DataType, Entity, QueryIntent, SelectQuery, TeaqlEntity as _, Value};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, ExecutionMetadata, MutationExecutor,
    MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult, QueryStream,
    StreamQueryExecutor,
};
use teaql_macros::{TeaqlEntity, TeaqlReverseRelations, teaql_entity};
use teaql_runtime::dynamic_fields::{
    DynamicFieldBatch, DynamicFieldsProvider, InMemoryDynamicFieldsProvider,
};
use teaql_runtime::{InMemoryMetadataStore, LedgerEntity, PurposedSelectQuery, UserContext};

#[teaql_entity]
#[derive(Clone, Debug, PartialEq, TeaqlEntity)]
#[teaql(entity = "School", indexed_layout)]
struct School {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, Value>,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}

#[test]
fn graph_decoders_retain_dynamic_read_metadata_without_promoting_it_to_columns() {
    let definitions = DynamicFieldDefinitions::new(
        "School",
        "graph-dynamic-v1",
        [("note".into(), DataType::Text)],
    )
    .unwrap();
    let values = DynamicFieldValues::from_batch(
        definitions,
        vec![
            HashMap::from([("note".into(), "private value".into())]),
            HashMap::from([("note".into(), Value::Null)]),
            HashMap::new(),
        ],
    )
    .unwrap();
    let layout = teaql_core::CompactRowLayout::new(Arc::from([
        "id".into(),
        "version".into(),
        "name".into(),
    ]));
    let rows: Vec<_> = values
        .into_iter()
        .enumerate()
        .map(|(index, fields)| {
            let mut row = CompactRow::with_layout(
                layout.clone(),
                vec![
                    (index as u64 + 1).into(),
                    1_i64.into(),
                    format!("row-{index}").into(),
                ],
            );
            row.set_loaded_dynamic_fields(fields);
            assert_eq!(row.len(), 3);
            assert!(!row.clone().into_map().contains_key("#note"));
            row
        })
        .collect();
    let root = teaql_runtime::EntityRuntimeState::default();
    let mut registry = teaql_runtime::InMemoryEntityGraphDecoderRegistry::default();
    registry.register::<School>();
    let mut builder = teaql_runtime::EntityGraphBuilder::default();
    registry
        .decode_compact_batch("School", rows.clone(), &root, &mut builder)
        .unwrap();
    registry
        .decode_compact_list(
            "School",
            rows.clone(),
            &root,
            &mut builder,
            "JsonGraphRow",
            77,
            "children",
        )
        .unwrap();
    registry
        .decode_compact_option(
            "School",
            vec![rows[0].clone()],
            &root,
            &mut builder,
            "JsonGraphRow",
            77,
            "single_child",
        )
        .unwrap();
    registry
        .decode_compact("School", rows[0].clone(), &root, &mut builder)
        .unwrap();
    root.freeze_graph(builder).unwrap();
    let list = root
        .resolve_relation_list::<School>("JsonGraphRow", 77, "children")
        .unwrap();
    assert_eq!(
        list[0]
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .value(),
        Some(&Value::Text("private value".into()))
    );
    assert_eq!(
        list[1]
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .state(),
        DynamicFieldState::Null
    );
    assert_eq!(
        list[2]
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .state(),
        DynamicFieldState::NotLoaded
    );
    assert!(Arc::ptr_eq(
        &list[0].loaded_state_snapshot().unwrap(),
        &list[1].loaded_state_snapshot().unwrap()
    ));
    assert!(!Arc::ptr_eq(
        &list[0].loaded_state_snapshot().unwrap(),
        &list[2].loaded_state_snapshot().unwrap()
    ));
    for entity in [
        root.resolve_entity::<School>(1).unwrap(),
        root.resolve_relation_option::<School>("JsonGraphRow", 77, "single_child")
            .unwrap()
            .as_ref()
            .unwrap(),
    ] {
        assert_eq!(
            entity
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&Value::Text("private value".into()))
        );
        assert_eq!(entity.clone().into_json()["#note"], "private value");
    }
    let mut cleared = rows[0].clone();
    cleared.clear_loaded_relations();
    assert!(cleared.take_loaded_dynamic_fields().is_none());
}

// Intentionally not Clone: presentation must borrow graph nodes, never copy them.
#[teaql_entity]
#[derive(Debug, TeaqlEntity)]
// Independent reverse edges legitimately repeat target/key metadata.
#[allow(clippy::duplicated_attributes)]
#[teaql(entity = "JsonGraphRow", indexed_layout)]
#[teaql(reverse_relation(
    name = "children",
    target = "JsonGraphRow",
    json_type = "JsonGraphRow",
    local_key = "id",
    foreign_key = "peer_id",
    many
))]
#[teaql(reverse_relation(
    name = "single_child",
    target = "JsonGraphRow",
    json_type = "JsonGraphRow",
    local_key = "id",
    foreign_key = "peer_id"
))]
struct JsonGraphRow {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    peer_id: Option<u64>,
    #[teaql(relation(target = "JsonGraphRow", local_key = "peer_id", foreign_key = "id"))]
    peer: Option<Box<JsonGraphRow>>,
    #[teaql(dynamic)]
    dynamic: BTreeMap<String, Value>,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}

impl JsonGraphRow {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "json-graph-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("name", 2), ("peer_id", 3)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("name", "name", "name"),
        ("peer_id", "peer_id", "peer_id"),
    ];
}

#[test]
fn borrowed_flat_graph_json_is_cycle_bounded_and_never_clones_entities() {
    let root = teaql_runtime::EntityRuntimeState::default();
    let make = |id: u64, peer: u64, state: &teaql_runtime::EntityRuntimeState| {
        let mut entity = JsonGraphRow::from_compact_row_with_context(
            CompactRow::from_map(BTreeMap::from([
                ("id".into(), Value::U64(id)),
                ("version".into(), Value::I64(1)),
                ("name".into(), format!("node-{id}").into()),
                ("peer_id".into(), peer.into()),
            ])),
            state as &dyn std::any::Any,
        )
        .unwrap();
        entity.__load_state.mark_loaded("peer").unwrap();
        entity
    };
    let mut builder = teaql_runtime::EntityGraphBuilder::default();
    let mut registry = teaql_runtime::InMemoryEntityGraphDecoderRegistry::default();
    registry.register::<JsonGraphRow>();
    for (id, peer) in [(1_u64, 2_u64), (2, 1)] {
        registry
            .decode_compact(
                "JsonGraphRow",
                CompactRow::from_map(BTreeMap::from([
                    ("id".into(), id.into()),
                    ("version".into(), 1_i64.into()),
                    ("name".into(), format!("node-{id}").into()),
                    ("peer_id".into(), peer.into()),
                    ("peer".into(), Value::Null),
                ])),
                &root,
                &mut builder,
            )
            .unwrap();
    }
    root.freeze_graph(builder).unwrap();
    let mut entity = make(1, 2, &root);
    // Live typed values, not stale original snapshots, are authoritative.
    entity.name = "live-name".into();
    let loaded = entity.loaded_state_snapshot().unwrap();
    let first = entity.borrowed_json(&mut Default::default()).unwrap();
    assert_eq!(first["name"], "live-name");
    assert_eq!(first["peer"]["name"], "node-2");
    assert_eq!(first["peer"]["peer"]["name"], "node-1");
    assert_eq!(first["peer"]["peer"]["peer"]["id"], 2);
    assert_eq!(first["peer"]["peer"]["peer"]["version"], 1);
    assert!(first["peer"]["peer"]["peer"].get("name").is_none());
    assert!(first["peer"]["peer"]["peer"].get("peer").is_none());
    assert!(Arc::ptr_eq(
        &loaded,
        &entity.loaded_state_snapshot().unwrap()
    ));
    assert_eq!(entity.into_json(), first);
}

#[test]
fn borrowed_reverse_graph_json_distinguishes_empty_and_unselected_lists() {
    let root = teaql_runtime::EntityRuntimeState::default();
    let mut registry = teaql_runtime::InMemoryEntityGraphDecoderRegistry::default();
    registry.register::<JsonGraphRow>();
    let mut builder = teaql_runtime::EntityGraphBuilder::default();
    let row = |id: u64| {
        CompactRow::from_map(BTreeMap::from([
            ("id".into(), id.into()),
            ("version".into(), 1_i64.into()),
            ("name".into(), format!("node-{id}").into()),
        ]))
    };
    registry
        .decode_compact_list(
            "JsonGraphRow",
            vec![row(11), row(12)],
            &root,
            &mut builder,
            "JsonGraphRow",
            1,
            "children",
        )
        .unwrap();
    registry
        .decode_compact_list(
            "JsonGraphRow",
            vec![],
            &root,
            &mut builder,
            "JsonGraphRow",
            2,
            "children",
        )
        .unwrap();
    registry
        .decode_compact_option(
            "JsonGraphRow",
            vec![],
            &root,
            &mut builder,
            "JsonGraphRow",
            2,
            "single_child",
        )
        .unwrap();
    root.freeze_graph(builder).unwrap();
    let json = |id, selected| {
        let mut projected = row(id);
        if selected {
            // Installing an edge in the graph does not select it in every view
            // of the same identity. Preserve this view's requested projection.
            projected.insert("children".into(), Value::Null);
            if id == 2 {
                projected.insert("single_child".into(), Value::Null);
            }
        }
        JsonGraphRow::from_compact_row_with_context(projected, &root as &dyn std::any::Any)
            .unwrap()
            .into_json()
    };
    let parent = json(1, true);
    assert_eq!(parent["children"][0]["name"], "node-11");
    assert_eq!(parent["children"][1]["name"], "node-12");
    assert_eq!(parent["children"].as_array().unwrap().len(), 2);
    assert_eq!(json(2, true)["children"], serde_json::json!([]));
    assert!(json(2, true).get("single_child").unwrap().is_null());
    for id in [1, 2, 3] {
        assert!(json(id, false).get("children").is_none());
        assert!(json(id, false).get("single_child").is_none());
    }
    assert!(parent["children"][0].get("_original_values").is_none());
}

#[test]
fn flat_json_does_not_leak_sibling_detail_into_an_unselected_or_filtered_edge() {
    let root = teaql_runtime::EntityRuntimeState::default();
    let mut registry = teaql_runtime::InMemoryEntityGraphDecoderRegistry::default();
    registry.register::<JsonGraphRow>();
    let mut builder = teaql_runtime::EntityGraphBuilder::default();
    registry
        .decode_compact(
            "JsonGraphRow",
            CompactRow::from_map(BTreeMap::from([
                ("id".into(), 7_u64.into()),
                ("version".into(), 1_i64.into()),
                ("name".into(), "visible-sibling".into()),
            ])),
            &root,
            &mut builder,
        )
        .unwrap();
    registry
        .decode_compact_option(
            "JsonGraphRow",
            vec![CompactRow::from_map(BTreeMap::from([(
                "id".into(),
                7_u64.into(),
            )]))],
            &root,
            &mut builder,
            "JsonGraphRow",
            2,
            "peer",
        )
        .unwrap();
    root.freeze_graph(builder).unwrap();
    let row = |id| {
        JsonGraphRow::from_compact_row_with_context(
            CompactRow::from_map(BTreeMap::from([
                ("id".into(), Value::U64(id)),
                ("version".into(), 1_i64.into()),
                ("peer_id".into(), 7_u64.into()),
            ])),
            &root as &dyn std::any::Any,
        )
        .unwrap()
    };
    assert!(row(1).into_json().get("peer").is_none());
    assert!(row(2).into_json().get("peer").is_none());
    let mut selected = row(1);
    selected.__load_state.mark_loaded("peer").unwrap();
    assert_eq!(selected.into_json()["peer"]["name"], "visible-sibling");
    let mut null = JsonGraphRow::from_compact_row_with_context(
        CompactRow::from_map(BTreeMap::from([
            ("id".into(), 3_u64.into()),
            ("version".into(), 1_i64.into()),
            ("peer_id".into(), Value::Null),
        ])),
        &root as &dyn std::any::Any,
    )
    .unwrap();
    null.__load_state.mark_loaded("peer").unwrap();
    assert!(null.into_json().get("peer").unwrap().is_null());
}

#[test]
fn serialization_preserves_fixed_derived_and_persistent_names_without_promoting_not_loaded() {
    let mut row = School::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(1)),
        ("version".into(), Value::I64(1)),
        ("name".into(), "native".into()),
        ("_name".into(), "derived".into()),
        ("_nil".into(), Value::Null),
    ])))
    .unwrap();
    let definitions = DynamicFieldDefinitions::new(
        "School",
        "serialization-v1",
        [
            ("name".into(), DataType::Text),
            ("nil".into(), DataType::Text),
            ("missing".into(), DataType::Text),
            ("zero".into(), DataType::I64),
            ("false".into(), DataType::Bool),
            ("empty".into(), DataType::Text),
        ],
    )
    .unwrap();
    let fields = DynamicFieldValues::from_values(
        definitions,
        HashMap::from([
            ("name".into(), "persistent".into()),
            ("nil".into(), Value::Null),
            ("zero".into(), Value::I64(0)),
            ("false".into(), Value::Bool(false)),
            ("empty".into(), "".into()),
        ]),
    )
    .unwrap();
    let state = teaql_core::LoadedSnapshot::with_dynamic_fields(
        &row.loaded_state_snapshot().unwrap(),
        &fields,
    )
    .unwrap();
    row.install_loaded_dynamic_fields(fields, state).unwrap();
    let json = row.into_json();
    assert_eq!(json["name"], "native");
    assert_eq!(json["_name"], "derived");
    assert_eq!(json["#name"], "persistent");
    assert_eq!(json["#zero"], 0);
    assert_eq!(json["#false"], false);
    assert_eq!(json["#empty"], "");
    assert!(json.get("#nil").unwrap().is_null());
    assert!(json.get("_nil").unwrap().is_null());
    assert!(json.get("#missing").is_none());
    assert!(json.get("__load_state").is_none());
    assert!(json.get("storage_binding").is_none());
}

#[test]
fn sparse_serialization_omits_absent_fixed_fields_and_internal_mutation_metadata() {
    let mut row = School::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(1)),
        ("version".into(), Value::I64(1)),
        ("_name".into(), "derived".into()),
    ])))
    .unwrap();
    assert!(!row.is_field_loaded("name"));
    row.set_comment("private mutation reason");
    row.mark_as_new();
    // MutationValues remains the governed persistence carrier. JSON is not.
    let values = row.clone().into_values();
    assert!(values.contains_key("_comment"));
    assert!(values.contains_key("_is_new"));
    let json = row.into_json();
    assert_eq!(json["_name"], "derived");
    assert!(json.get("name").is_none());
    for internal in [
        "_comment",
        "_is_new",
        "_is_deleted",
        "_dirty_fields",
        "_original_values",
    ] {
        assert!(json.get(internal).is_none(), "leaked {internal}");
    }
}

impl School {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "school-query-test-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("name", 2)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("name", "name", "name"),
    ];
}

#[teaql_entity]
#[derive(Clone, Debug, TeaqlEntity)]
#[teaql(entity = "SerializationParent")]
struct SerializationParent {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    #[teaql(relation(target = "School", local_key = "id", foreign_key = "id"))]
    child: Option<Box<School>>,
    #[teaql(relation(target = "School", local_key = "id", foreign_key = "id", many))]
    children: teaql_core::SmartList<School>,
    #[teaql(boxed_relations)]
    boxed: Box<SerializationRelations>,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}

#[derive(Clone, Debug, TeaqlReverseRelations)]
struct SerializationRelations {
    #[teaql(relation(target = "School", local_key = "id", foreign_key = "id"))]
    boxed_child: Option<Box<School>>,
    #[teaql(relation(target = "School", local_key = "id", foreign_key = "id", many))]
    boxed_children: teaql_core::SmartList<School>,
}

#[test]
fn nested_entity_json_keeps_extensions_and_does_not_serialize_mutation_internals() {
    let mut child = School::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(1)),
        ("version".into(), Value::I64(1)),
        ("name".into(), "native".into()),
        ("_name".into(), "derived".into()),
        (
            "_payload".into(),
            Value::Object(BTreeMap::from([(
                "_comment".into(),
                "literal business data".into(),
            )])),
        ),
    ])))
    .unwrap();
    let values = DynamicFieldValues::from_values(
        DynamicFieldDefinitions::new(
            "School",
            "nested-json-v1",
            [
                ("name".into(), DataType::Text),
                ("nil".into(), DataType::Text),
                ("missing".into(), DataType::Text),
            ],
        )
        .unwrap(),
        HashMap::from([
            ("name".into(), "persistent".into()),
            ("nil".into(), Value::Null),
        ]),
    )
    .unwrap();
    let state = teaql_core::LoadedSnapshot::with_dynamic_fields(
        &child.loaded_state_snapshot().unwrap(),
        &values,
    )
    .unwrap();
    child.install_loaded_dynamic_fields(values, state).unwrap();
    child.set_comment("private child mutation reason");
    child.mark_as_new();
    let mut parent = SerializationParent::from_compact_row(CompactRow::from_map(BTreeMap::from([
        ("id".into(), Value::U64(2)),
        ("version".into(), Value::I64(1)),
        ("child".into(), Value::Null),
        ("children".into(), Value::List(vec![])),
        ("boxed_child".into(), Value::Null),
        ("boxed_children".into(), Value::List(vec![])),
    ])))
    .unwrap();
    parent.child = Some(Box::new(child.clone()));
    parent.children = vec![child.clone()].into();
    parent.boxed.boxed_child = Some(Box::new(child.clone()));
    parent.boxed.boxed_children = vec![child].into();
    let mutations = parent.clone().into_values();
    assert!(
        matches!(mutations.get("child"), Some(Value::Object(fields)) if fields.contains_key("_comment"))
    );
    let json = parent.into_json();
    for child_json in [
        &json["child"],
        &json["children"][0],
        &json["boxed_child"],
        &json["boxed_children"][0],
    ] {
        for key in [
            "_comment",
            "_is_new",
            "_is_deleted",
            "_dirty_fields",
            "_original_values",
        ] {
            assert!(
                child_json.get(key).is_none(),
                "private nested mutation metadata leaked: {key}"
            );
        }
        assert_eq!(child_json["name"], "native");
        assert_eq!(child_json["_name"], "derived");
        assert_eq!(child_json["#name"], "persistent");
        assert!(child_json.get("#nil").unwrap().is_null());
        assert!(child_json.get("#missing").is_none());
        assert_eq!(child_json["_payload"]["_comment"], "literal business data");
    }
}

#[test]
fn relation_json_distinguishes_selected_null_empty_and_not_loaded() {
    let row = |selected| {
        let mut fields = BTreeMap::from([
            ("id".into(), Value::U64(1)),
            ("version".into(), Value::I64(1)),
        ]);
        if selected {
            fields.insert("child".into(), Value::Null);
            fields.insert("children".into(), Value::List(vec![]));
            fields.insert("boxed_child".into(), Value::Null);
            fields.insert("boxed_children".into(), Value::List(vec![]));
        }
        SerializationParent::from_compact_row(CompactRow::from_map(fields)).unwrap()
    };
    let missing = row(false).into_json();
    assert!(missing.get("child").is_none());
    assert!(missing.get("children").is_none());
    assert!(missing.get("boxed_child").is_none());
    assert!(missing.get("boxed_children").is_none());
    let loaded = row(true).into_json();
    assert!(loaded.get("child").unwrap().is_null());
    assert_eq!(loaded["children"], serde_json::json!([]));
    assert!(loaded.get("boxed_child").unwrap().is_null());
    assert_eq!(loaded["boxed_children"], serde_json::json!([]));
}

struct FixedRows {
    rows: Vec<CompactRow>,
    queries: Arc<Mutex<Vec<QueryRequest>>>,
}
impl DataServiceExecutor for FixedRows {
    type Error = std::io::Error;
    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceCapabilities::default()
    }
}
impl QueryExecutor for FixedRows {
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
        self.queries.lock().unwrap().push(request);
        Ok(QueryResult {
            rows: self.rows.clone(),
            metadata: ExecutionMetadata::unrecorded_query(self.rows.len()),
        })
    }
}
impl MutationExecutor for FixedRows {
    async fn mutate(&self, _: MutationRequest) -> Result<MutationResult, Self::Error> {
        panic!("loading must not mutate")
    }
}
impl StreamQueryExecutor for FixedRows {
    fn query_stream(&self, _: QueryRequest, _: usize) -> QueryStream<'_, Self::Error> {
        panic!("unsupported enhancement must reject before the cursor")
    }
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    OmittedOwner,
    UnexpectedOwner,
    UnselectedValue,
    WrongType,
    WrongOwner,
}
struct ContextMarker;
struct ObservedProvider {
    memory: InMemoryDynamicFieldsProvider,
    calls: Arc<Mutex<Vec<(usize, String, String)>>>,
    fault: Fault,
}
#[async_trait::async_trait]
impl DynamicFieldsProvider for ObservedProvider {
    async fn load_values(
        &self,
        context: &UserContext,
        owner: &str,
        ids: &[u64],
        selection: &DynamicFieldSelection,
        intent: &QueryIntent,
    ) -> Result<DynamicFieldBatch, DynamicFieldError> {
        assert!(context.get_resource::<ContextMarker>().is_some());
        self.calls.lock().unwrap().push((
            ids.len(),
            intent.comment().into(),
            intent.purpose().into(),
        ));
        let mut result = self
            .memory
            .load_values(context, owner, ids, selection, intent)
            .await?;
        match self.fault {
            Fault::None => {}
            Fault::OmittedOwner => {
                result.rows.remove(&1);
            }
            Fault::UnexpectedOwner => {
                result.rows.insert(999, HashMap::new());
            }
            Fault::UnselectedValue => {
                result
                    .rows
                    .get_mut(&1)
                    .unwrap()
                    .insert("extra".into(), Value::Text("PRIVATE-CANARY".into()));
            }
            Fault::WrongType => {
                result
                    .rows
                    .get_mut(&1)
                    .unwrap()
                    .insert("note".into(), Value::Bool(false));
            }
            Fault::WrongOwner => {
                result.definitions = definitions("Platform");
            }
        }
        Ok(result)
    }
}

fn definitions(owner: &str) -> Arc<DynamicFieldDefinitions> {
    DynamicFieldDefinitions::new(
        owner,
        "extension-v1",
        [
            ("note".into(), DataType::Text),
            ("extra".into(), DataType::Text),
        ],
    )
    .unwrap()
}
type Calls = Arc<Mutex<Vec<(usize, String, String)>>>;
type Queries = Arc<Mutex<Vec<QueryRequest>>>;
fn context(fault: Option<Fault>, duplicate: bool) -> (UserContext, Calls, Queries) {
    context_with_extra(fault, duplicate, false)
}

fn context_with_extra(
    fault: Option<Fault>,
    duplicate: bool,
    extra: bool,
) -> (UserContext, Calls, Queries) {
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(School::entity_descriptor()));
    let mut rows = (1..=3)
        .map(|id| {
            CompactRow::from_map(BTreeMap::from([
                ("id".into(), Value::U64(id)),
                ("version".into(), Value::I64(1)),
                ("name".into(), Value::Text(format!("school-{id}"))),
            ]))
        })
        .collect::<Vec<_>>();
    if duplicate {
        rows.push(rows[0].clone());
    }
    let queries = Arc::default();
    context.insert_resource(FixedRows {
        rows,
        queries: Arc::clone(&queries),
    });
    context.insert_resource(ContextMarker);
    let calls = Arc::default();
    if let Some(fault) = fault {
        let memory = InMemoryDynamicFieldsProvider::from_owners([
            (
                definitions("School"),
                HashMap::from([
                    (
                        1,
                        if extra {
                            HashMap::from([
                                ("note".into(), "private-school".into()),
                                ("extra".into(), "root-extra".into()),
                            ])
                        } else {
                            HashMap::from([("note".into(), Value::Text("private-school".into()))])
                        },
                    ),
                    (
                        2,
                        if extra {
                            HashMap::from([
                                ("note".into(), Value::Null),
                                ("extra".into(), Value::Null),
                            ])
                        } else {
                            HashMap::from([("note".into(), Value::Null)])
                        },
                    ),
                ]),
            ),
            (
                definitions("Platform"),
                HashMap::from([(
                    1,
                    HashMap::from([("note".into(), Value::Text("wrong-owner".into()))]),
                )]),
            ),
        ])
        .unwrap();
        context.set_dynamic_fields_provider(Arc::new(ObservedProvider {
            memory,
            calls: Arc::clone(&calls),
            fault,
        }));
    }
    (context, calls, queries)
}

fn query(selection: bool) -> PurposedSelectQuery {
    let mut query = SelectQuery::new("School")
        .projects(["id", "version", "name"])
        .limit(4);
    query.comment = Some("load dynamic state counterexamples".into());
    if selection {
        query = query.select_dynamic_fields(
            DynamicFieldSelection::fields([("note".into(), DataType::Text)]).unwrap(),
        );
    }
    PurposedSelectQuery::new(query, "verify shared selection with private payloads")
}

#[tokio::test]
async fn child_enhancement_dynamic_selection_reaches_typed_views_and_survives_root_loading() {
    for root_selection in [false, true] {
        let (context, calls, queries) = context_with_extra(Some(Fault::None), false, true);
        let child = SelectQuery::new("School")
            .projects(["id", "version", "name"])
            .limit(4)
            .comment("load dynamic state counterexamples")
            .select_dynamic_fields(
                DynamicFieldSelection::fields([("note".into(), DataType::Text)]).unwrap(),
            );
        let mut root = SelectQuery::new("School")
            .projects(["id", "version", "name"])
            .limit(4)
            .comment("load dynamic state counterexamples")
            .child_enhancement(child);
        if root_selection {
            root = root.select_dynamic_fields(
                DynamicFieldSelection::fields([("extra".into(), DataType::Text)]).unwrap(),
            );
        }
        let request = PurposedSelectQuery::new(root, "verify dynamic enhancement read metadata");
        let result = context
            .entity_data_service::<FixedRows>("School")
            .unwrap()
            .fetch_enhanced_entities::<School>(&request)
            .await
            .unwrap()
            .data;
        assert_eq!(queries.lock().unwrap().len(), 2);
        assert_eq!(
            calls.lock().unwrap().len(),
            if root_selection { 2 } else { 1 }
        );
        assert_eq!(result.len(), 3);
        for (index, row) in result.iter().enumerate() {
            let fields = row.dynamic_field_values().unwrap();
            assert_eq!(
                fields.field("note").unwrap().state(),
                [
                    DynamicFieldState::Value,
                    DynamicFieldState::Null,
                    DynamicFieldState::NotLoaded
                ][index]
            );
            assert_eq!(
                fields.field("extra").unwrap().state(),
                if root_selection {
                    [
                        DynamicFieldState::Value,
                        DynamicFieldState::Null,
                        DynamicFieldState::NotLoaded,
                    ][index]
                } else {
                    DynamicFieldState::NotLoaded
                }
            );
            assert!(!row.has_pending_dynamic_mutations());
        }
        assert!(Arc::ptr_eq(
            &result[0].loaded_state_snapshot().unwrap(),
            &result[1].loaded_state_snapshot().unwrap()
        ));
        let json = result[0].clone().into_json();
        assert_eq!(json["#note"], "private-school");
        if root_selection {
            assert_eq!(json["#extra"], "root-extra");
        } else {
            assert!(json.get("#extra").is_none());
        }
    }
}

#[tokio::test]
async fn malformed_dynamic_child_enhancement_fails_without_returning_partial_entities() {
    for fault in [
        Fault::OmittedOwner,
        Fault::UnexpectedOwner,
        Fault::UnselectedValue,
        Fault::WrongType,
        Fault::WrongOwner,
    ] {
        let (context, calls, _) = context(Some(fault), false);
        let child = SelectQuery::new("School")
            .projects(["id", "version", "name"])
            .limit(4)
            .comment("load dynamic state counterexamples")
            .select_dynamic_fields(
                DynamicFieldSelection::fields([("note".into(), DataType::Text)]).unwrap(),
            );
        let request = PurposedSelectQuery::new(
            SelectQuery::new("School")
                .projects(["id", "version", "name"])
                .limit(4)
                .comment("load dynamic state counterexamples")
                .child_enhancement(child),
            "reject malformed child dynamic evidence",
        );
        let error = context
            .entity_data_service::<FixedRows>("School")
            .unwrap()
            .fetch_enhanced_entities::<School>(&request)
            .await
            .unwrap_err();
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert!(!error.to_string().contains("PRIVATE-CANARY"));
    }
}

#[tokio::test]
async fn typed_query_batches_once_and_preserves_private_null_missing_and_ledgers() {
    let (context, calls, queries) = context(Some(Fault::None), false);
    let repo = context.entity_data_service::<FixedRows>("School").unwrap();
    let mut rows = repo
        .fetch_enhanced_entities::<School>(&query(true))
        .await
        .unwrap()
        .data;
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        &[(
            3,
            "load dynamic state counterexamples".into(),
            "verify shared selection with private payloads".into()
        )]
    );
    assert_eq!(
        queries.lock().unwrap()[0].query.projection,
        ["id", "version", "name"]
    );
    assert!(Arc::ptr_eq(
        &rows[0].loaded_state_snapshot().unwrap(),
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    assert!(!Arc::ptr_eq(
        &rows[1].loaded_state_snapshot().unwrap(),
        &rows[2].loaded_state_snapshot().unwrap()
    ));
    assert_eq!(
        rows[0]
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .value(),
        Some(&Value::Text("private-school".into()))
    );
    assert_eq!(
        rows[1]
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .state(),
        DynamicFieldState::Null
    );
    assert_eq!(
        rows[2]
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .state(),
        DynamicFieldState::NotLoaded
    );
    assert_eq!(
        rows[0]
            .dynamic_field_values()
            .unwrap()
            .field("extra")
            .unwrap()
            .state(),
        DynamicFieldState::NotLoaded
    );
    assert!(rows.iter().all(|row| row.dirty_fields().is_none()));
    let state = rows[0].entity_runtime_state().unwrap();
    let sibling = rows[1].entity_runtime_state().unwrap();
    assert_ne!(state, sibling);
    assert!(state.current_change_set().changes().is_empty());
    state.set(
        teaql_runtime::EntityKey::new("School", 1_u64),
        "name",
        Value::Text("local-change".into()),
    );
    assert!(sibling.current_change_set().changes().is_empty());
    let original = rows[1].loaded_state_snapshot().unwrap();
    let augmented = DynamicFieldValues::from_values(
        definitions("School"),
        HashMap::from([
            ("note".into(), Value::Text("private-school".into())),
            ("extra".into(), Value::Text("private-extra".into())),
        ]),
    )
    .unwrap();
    let augmented_state =
        teaql_core::LoadedSnapshot::with_dynamic_fields(&original, &augmented).unwrap();
    rows[0]
        .install_loaded_dynamic_fields(augmented, augmented_state)
        .unwrap();
    assert!(Arc::ptr_eq(
        &original,
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    assert!(!rows[1].is_field_loaded("#extra"));
    assert!(rows[0].is_field_loaded("name"));
    assert!(rows[1].is_field_loaded("#note"));
    assert!(!rows[2].is_field_loaded("#note"));
    // Readonly / loaded extensions never become database write intent just because they were selected.
    assert!(!rows.remove(0).into_values().contains_key("#note"));
}

#[tokio::test]
async fn basic_typed_query_duplicates_and_result_lifetime_keep_the_same_contract() {
    let (context, calls, _) = context(Some(Fault::None), true);
    let rows = context
        .entity_data_service::<FixedRows>("School")
        .unwrap()
        .fetch_entities::<School>(&query(true))
        .await
        .unwrap()
        .data;
    assert_eq!(rows.len(), 4);
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert!(Arc::ptr_eq(
        &rows[0].loaded_state_snapshot().unwrap(),
        &rows[3].loaded_state_snapshot().unwrap()
    ));
    assert!(!std::ptr::eq(
        rows[0].dynamic_field_values().unwrap(),
        rows[3].dynamic_field_values().unwrap()
    ));
    let clone = rows[0].clone();
    drop(rows);
    drop(context);
    assert_eq!(
        clone
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .value(),
        Some(&Value::Text("private-school".into()))
    );
}

#[tokio::test]
async fn malformed_provider_batches_fail_closed_without_returning_partial_rows_or_values_in_errors()
{
    for (fault, code) in [
        (Fault::OmittedOwner, "DYNAMIC_FIELD_BATCH_OMITTED_OWNER"),
        (Fault::UnexpectedOwner, "DYNAMIC_FIELD_UNREQUESTED_OWNER"),
        (Fault::UnselectedValue, "DYNAMIC_FIELD_UNSELECTED_VALUE"),
        (Fault::WrongType, "DYNAMIC_FIELD_TYPE_MISMATCH"),
        (Fault::WrongOwner, "DYNAMIC_FIELD_OWNER_MISMATCH"),
    ] {
        let (context, _, _) = context(Some(fault), false);
        let error = context
            .entity_data_service::<FixedRows>("School")
            .unwrap()
            .fetch_enhanced_entities::<School>(&query(true))
            .await
            .unwrap_err();
        assert!(error.to_string().contains(code), "{error}");
        assert!(!error.to_string().contains("PRIVATE-CANARY"));
    }
}

#[tokio::test]
async fn no_selection_needs_no_provider_and_streaming_cannot_silently_drop_a_selection() {
    let (context, calls, queries) = context(None, false);
    let repo = context.entity_data_service::<FixedRows>("School").unwrap();
    let rows = repo
        .fetch_enhanced_entities::<School>(&query(false))
        .await
        .unwrap();
    assert!(
        rows.data
            .iter()
            .all(|row| row.dynamic_field_values().is_none())
    );
    assert!(calls.lock().unwrap().is_empty());
    let error = repo
        .fetch_enhanced_entities::<School>(&query(true))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("DYNAMIC_FIELD_PROVIDER_MISSING"));
    let count = queries.lock().unwrap().len();
    assert!(repo.fetch_stream(&query(true)).await.is_err());
    assert_eq!(queries.lock().unwrap().len(), count);
}

#[tokio::test]
async fn composing_a_ledger_never_replaces_child_dynamic_payload_with_parent_values() {
    let (context, _, _) = context(Some(Fault::None), false);
    let mut rows = context
        .entity_data_service::<FixedRows>("School")
        .unwrap()
        .fetch_enhanced_entities::<School>(&query(true))
        .await
        .unwrap()
        .data;
    let parent = rows[0].entity_runtime_state().unwrap();
    let null_state = rows[1].loaded_state_snapshot().unwrap();
    let missing_state = rows[2].loaded_state_snapshot().unwrap();
    rows[1].__teaql_replace_runtime_state(parent.clone());
    rows[2].__teaql_replace_runtime_state(parent);
    assert_eq!(
        rows[1]
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .state(),
        DynamicFieldState::Null
    );
    assert_eq!(
        rows[2]
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .state(),
        DynamicFieldState::NotLoaded
    );
    assert!(Arc::ptr_eq(
        &null_state,
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &missing_state,
        &rows[2].loaded_state_snapshot().unwrap()
    ));
    assert_eq!(
        rows[1]
            .entity_runtime_state()
            .unwrap()
            .original_snapshot()
            .unwrap()
            .get("id"),
        Some(&Value::U64(2))
    );
    assert_eq!(
        rows[2]
            .entity_runtime_state()
            .unwrap()
            .original_snapshot()
            .unwrap()
            .get("id"),
        Some(&Value::U64(3))
    );
    assert_eq!(
        rows[0]
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .value(),
        Some(&Value::Text("private-school".into()))
    );
}

#[tokio::test]
async fn dynamic_mutation_intent_is_explicit_row_local_and_cannot_be_silently_saved() {
    use teaql_core::dynamic_fields::DynamicFieldMutation;
    let (context, _, queries) = context(Some(Fault::None), false);
    let mut rows = context
        .entity_data_service::<FixedRows>("School")
        .unwrap()
        .fetch_enhanced_entities::<School>(&query(true))
        .await
        .unwrap()
        .data;
    let sibling = rows[1].loaded_state_snapshot().unwrap();
    let own = rows[0].loaded_state_snapshot().unwrap();
    let foreign = teaql_core::FieldLayout::from_generated(
        "School",
        "incompatible-revision",
        &[("id", 0), ("version", 1), ("name", 2)],
        &[
            ("id", "id", "id"),
            ("version", "version", "version"),
            ("name", "name", "name"),
        ],
        &[],
        &["id", "version", "name"],
    )
    .unwrap();
    rows[0].__load_state = teaql_core::eval::LoadState::Indexed(
        teaql_core::LoadedSnapshot::fully_loaded(foreign).into_shared(),
    );
    assert_eq!(
        rows[0]
            .update_dynamic_field("note", Value::Null)
            .unwrap_err()
            .code,
        "DYNAMIC_FIELD_LAYOUT_MISMATCH"
    );
    assert_eq!(
        rows[0].delete_dynamic_field("note").unwrap_err().code,
        "DYNAMIC_FIELD_LAYOUT_MISMATCH"
    );
    assert!(!rows[0].has_pending_dynamic_mutations());
    assert_eq!(
        rows[0]
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .value(),
        Some(&Value::Text("private-school".into()))
    );
    rows[0].__load_state = teaql_core::eval::LoadState::Indexed(own);
    rows[0].update_dynamic_field("note", Value::Null).unwrap();
    assert_eq!(
        rows[0].dirty_fields().unwrap(),
        std::collections::BTreeSet::from(["#note".to_owned()])
    );
    assert!(Arc::ptr_eq(
        &sibling,
        &rows[0].loaded_state_snapshot().unwrap()
    ));
    let state = rows[0].entity_runtime_state().unwrap();
    assert_eq!(
        state.current_change_set().dynamic_changes().unwrap()
            [&teaql_runtime::EntityKey::new("School", 1_u64)]["note"],
        DynamicFieldMutation::Set(Value::Null)
    );
    assert!(state.current_change_set().changes().is_empty());
    assert!(
        rows[1]
            .entity_runtime_state()
            .unwrap()
            .current_change_set()
            .is_empty()
    );
    let before = state.current_change_set();
    assert!(
        rows[0]
            .update_dynamic_field("note", Value::Bool(false))
            .is_err()
    );
    assert_eq!(state.current_change_set(), before);
    rows[0].delete_dynamic_field("note").unwrap();
    assert_eq!(
        state.current_change_set().dynamic_changes().unwrap()
            [&teaql_runtime::EntityKey::new("School", 1_u64)]["note"],
        DynamicFieldMutation::Delete
    );
    assert_eq!(
        rows[0]
            .dynamic_field_values()
            .unwrap()
            .field("note")
            .unwrap()
            .state(),
        DynamicFieldState::NotLoaded
    );
    assert!(Arc::ptr_eq(
        &rows[2].loaded_state_snapshot().unwrap(),
        &rows[0].loaded_state_snapshot().unwrap()
    ));
    let query_count = queries.lock().unwrap().len();
    let error = teaql_runtime::graph_node_from_entity(&context, rows[0].clone()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED")
    );
    assert_eq!(queries.lock().unwrap().len(), query_count);
    use teaql_runtime::AuditedSaveExt;
    let error = rows[0]
        .clone()
        .audit_as("reject an unbound dynamic-field write")
        .save(&context)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED")
    );
    let error = teaql_runtime::save_audited_ledger_entity(
        rows[0]
            .clone()
            .audit_as("retain dynamic intent on failed save"),
        &context,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("DYNAMIC_FIELD_TRANSACTION_BINDING_REQUIRED")
    );
    assert_eq!(queries.lock().unwrap().len(), query_count);
    assert_eq!(
        state.current_change_set().dynamic_changes().unwrap()
            [&teaql_runtime::EntityKey::new("School", 1_u64)]["note"],
        DynamicFieldMutation::Delete
    );
    // A failed save gate keeps intent retryable. Explicit graph composition must also retain it.
    let composed = teaql_runtime::EntityRuntimeState::default();
    composed.adopt_mutations_from(&state);
    assert_eq!(
        composed.current_change_set().dynamic_changes(),
        state.current_change_set().dynamic_changes()
    );
    assert!(composed.has_pending_dynamic_mutations());
    state.push_change_set();
    rows[0].update_dynamic_field("note", Value::Null).unwrap();
    assert_eq!(
        state.pop_change_set().unwrap().dynamic_changes().unwrap()
            [&teaql_runtime::EntityKey::new("School", 1_u64)]["note"],
        DynamicFieldMutation::Set(Value::Null)
    );
    assert_eq!(
        state.current_change_set().dynamic_changes().unwrap()
            [&teaql_runtime::EntityKey::new("School", 1_u64)]["note"],
        DynamicFieldMutation::Delete
    );
    state.clear_committed();
    assert!(!state.has_pending_dynamic_mutations());
    assert!(state.current_change_set().is_empty());
    assert!(composed.has_pending_dynamic_mutations());
}
