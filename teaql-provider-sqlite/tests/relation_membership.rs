//! Native SQLite probe for relation membership and execution-owned trace paths (#239).
//! No generated source or manually inserted trace frames are used.
use std::{collections::BTreeMap, sync::Arc};

use teaql_core::{
    DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor, RelationDescriptor,
    SelectQuery, Value,
};
use teaql_data_service::{MutationCommand, SchemaProvider};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, PurposedSelectQuery, UserContext};
use teaql_sql::SqlDataServiceExecutor;

#[derive(Clone)]
struct Schema(Vec<Arc<EntityDescriptor>>);
impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        self.0.iter().find(|entity| entity.name == name).cloned()
    }
}
type Executor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;
const PRIVATE_NAME: &str = "MEMBERSHIP-PRIVATE-NAME";

// Handwritten native adapters exercise the same typed hydration boundary without
// reading or changing any generated library.
macro_rules! graph_entity {
    ($name:ident, $table:literal) => {
        struct $name {
            row: teaql_core::CompactRow,
            state: teaql_runtime::EntityRuntimeState,
        }
        impl teaql_core::TeaqlEntity for $name {
            const ENTITY_NAME: &'static str = stringify!($name);
            fn entity_descriptor() -> EntityDescriptor {
                entity(stringify!($name), $table)
            }
        }
        impl teaql_core::Entity for $name {
            fn is_field_loaded(&self, field: &str) -> bool {
                self.row.contains_key(field)
            }
            fn from_compact_row(
                row: teaql_core::CompactRow,
            ) -> Result<Self, teaql_core::EntityError> {
                Ok(Self {
                    row,
                    state: Default::default(),
                })
            }
            fn into_values(self) -> teaql_core::MutationValues {
                self.row.into_map().into()
            }
            fn on_loaded(&mut self, context: &dyn std::any::Any) {
                self.state = context
                    .downcast_ref::<teaql_runtime::EntityRuntimeState>()
                    .unwrap()
                    .clone();
            }
        }
        impl teaql_core::IdentifiableEntity for $name {
            fn id_value(&self) -> Value {
                self.row.get("id").unwrap().clone()
            }
        }
    };
}
graph_entity!(MembershipParent, "membership_parent");
graph_entity!(MembershipChild, "membership_child");

fn entity(name: &str, table: &str) -> EntityDescriptor {
    EntityDescriptor::new(name)
        .table_name(table)
        .property(PropertyDescriptor::new("id", DataType::I64).id())
        .property(PropertyDescriptor::new("version", DataType::I64).version())
        .property(PropertyDescriptor::new("name", DataType::Text))
}

async fn setup(aliased: bool) -> UserContext {
    let parent = entity("MembershipParent", "membership_parent")
        .audit_mask_fields(vec!["name".into()])
        .relation(
            RelationDescriptor::new("children", "MembershipChild")
                .many()
                .local_key("id")
                .foreign_key("parent_id"),
        );
    let child = entity("MembershipChild", "membership_child")
        .property(PropertyDescriptor::new("parent_id", DataType::I64))
        .relation(
            RelationDescriptor::new(
                if aliased { "parent_id" } else { "parent" },
                "MembershipParent",
            )
            .local_key("parent_id")
            .foreign_key("id"),
        )
        .relation(
            RelationDescriptor::new("second_parent", "MembershipParent")
                .local_key("parent_id")
                .foreign_key("id"),
        );
    let mut context = UserContext::new().with_metadata(
        InMemoryMetadataStore::new()
            .with_entity(parent.clone())
            .with_entity(child.clone()),
    );
    let mut decoders = teaql_runtime::InMemoryEntityGraphDecoderRegistry::default();
    decoders.register::<MembershipParent>();
    decoders.register::<MembershipChild>();
    context.set_entity_graph_decoder_registry(decoders);
    let transport =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open_in_memory().unwrap());
    context.use_sqlite_provider(transport.clone());
    context.ensure_schema().await.unwrap();
    context.insert_resource(Executor::new(
        SqliteDialect,
        transport,
        Schema(vec![Arc::new(parent), Arc::new(child)]),
    ));
    context
        .execute_in_transaction::<Executor, _, _>(|transaction| {
            Box::pin(async move {
                for command in [
                    InsertCommand::new("MembershipParent")
                        .value("id", 100_i64)
                        .value("name", PRIVATE_NAME),
                    InsertCommand::new("MembershipChild")
                        .value("id", 101_i64)
                        .value("name", "first")
                        .value("parent_id", 100_i64),
                    InsertCommand::new("MembershipChild")
                        .value("id", 102_i64)
                        .value("name", "second")
                        .value("parent_id", 100_i64),
                ] {
                    transaction
                        .mutate(
                            MutationCommand::Insert(command.value("version", 1_i64))
                                .request("seed relation membership fixture")?,
                        )
                        .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    context.clear_sql_logs();
    context
}

async fn verify(filtered: bool, threshold: usize, aliased: bool, sibling: bool, logging: bool) {
    let mut context = setup(aliased).await;
    if !logging {
        context.disable_sql_log();
    }
    let forward = if aliased { "parent_id" } else { "parent" };
    let mut children_query = SelectQuery::new("MembershipChild")
        .limit(10)
        .top_n_probe_parent_threshold(threshold)
        .relation_query(
            forward,
            SelectQuery::new("MembershipParent").filter(Expr::eq(
                "name",
                if filtered { "ABSENT" } else { PRIVATE_NAME },
            )),
        );
    if sibling {
        children_query =
            children_query.relation_query("second_parent", SelectQuery::new("MembershipParent"));
    }
    let query = PurposedSelectQuery::new(
        SelectQuery::new("MembershipParent")
            .limit(10)
            .relation_query("children", children_query)
            .comment(format!(
                "load child membership and filtered parent {PRIVATE_NAME}"
            )),
        format!("verify nested trace execution and predicate preservation {PRIVATE_NAME}"),
    );
    let rows = context
        .entity_data_service::<Executor>("MembershipParent")
        .unwrap()
        .fetch_all(&query)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(Value::List(children)) = rows[0].get("children") else {
        panic!("children must be loaded");
    };
    assert_eq!(
        children.len(),
        2,
        "forward filtering must not remove children"
    );
    for child in children {
        let Value::Object(child) = child else {
            panic!("expected child object")
        };
        if !aliased {
            assert_eq!(child.get("parent_id").and_then(Value::try_i64), Some(100));
        }
        if filtered {
            assert_eq!(
                child.get(forward),
                Some(&Value::object(BTreeMap::from([(
                    "id".into(),
                    Value::I64(100)
                )]))),
                "filtered detail retains the real identity without loading its fields"
            );
        } else {
            assert!(matches!(child.get(forward), Some(Value::Object(_))));
        }
        if sibling {
            assert!(
                matches!(child.get("second_parent"), Some(Value::Object(_))),
                "sibling must use original FK"
            );
        }
    }
    let logs = context.sql_logs();
    // The nested query still owns sensitive provenance after the planner strips
    // child loads from its single-layer SQL request.
    if !filtered {
        assert!(!format!("{logs:?}").contains(PRIVATE_NAME));
    }
    assert_eq!(
        logs.len(),
        if logging { 3 + usize::from(sibling) } else { 0 },
        "one root, one child batch and one nested forward query; actual logs: {logs:#?}"
    );
    if logging {
        let paths = logs
            .iter()
            .map(|log| {
                log.trace_path
                    .iter()
                    .filter(|node| node.kind == teaql_core::TraceKind::Relation)
                    .map(|node| node.entity_type.as_str())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut expected = vec![vec![], vec!["children"], vec!["children", forward]];
        if sibling {
            expected.push(vec!["children", "second_parent"]);
        }
        assert_eq!(
            paths, expected,
            "trace must describe actual unique branches"
        );
    }
    context.clear_sql_logs();
    let independent = context
        .entity_data_service::<Executor>("MembershipChild")
        .unwrap()
        .fetch_all(&PurposedSelectQuery::new(
            SelectQuery::new("MembershipChild")
                .limit(10)
                .comment("verify original foreign key"),
            "check query scope isolation",
        ))
        .await
        .unwrap();
    assert_eq!(independent.len(), 2);
    assert!(
        independent
            .iter()
            .all(|row| row.get("parent_id").and_then(Value::try_i64) == Some(100))
    );
    assert_eq!(context.sql_logs().len(), usize::from(logging));
    println!(
        "PASS Rust relation membership: filtered={filtered} threshold={threshold} aliased={aliased} sibling={sibling} logging={logging}"
    );
}

async fn verify_matrix(filtered: bool, threshold: usize) {
    for aliased in [false, true] {
        for sibling in [false, true] {
            for logging in [false, true] {
                verify(filtered, threshold, aliased, sibling, logging).await;
            }
        }
    }
}

#[test]
fn filtered_forward_window_keeps_membership() {
    futures_executor::block_on(verify_matrix(true, 0));
}

#[test]
fn visible_forward_window_is_loaded_once() {
    futures_executor::block_on(verify_matrix(false, 0));
}

#[test]
fn filtered_forward_probe_keeps_membership() {
    futures_executor::block_on(verify_matrix(true, 32));
}

#[test]
fn visible_forward_probe_is_loaded_once() {
    futures_executor::block_on(verify_matrix(false, 32));
}

async fn verify_aggregate(aliased: bool, filtered: bool, typed: bool) {
    for logging in [false, true] {
        let mut context = setup(aliased).await;
        if !logging {
            context.disable_sql_log();
        }
        let forward = if aliased { "parent_id" } else { "parent" };
        let query = PurposedSelectQuery::new(
            SelectQuery::new("MembershipChild")
                .limit(10)
                .relation_query(
                    forward,
                    SelectQuery::new("MembershipParent").filter(Expr::eq(
                        "name",
                        if filtered { "ABSENT" } else { PRIVATE_NAME },
                    )),
                )
                .comment("load children and their parent count"),
            "verify aggregation after filtered forward hydration",
        );
        let aggregates = [teaql_core::RelationAggregate::new(
            "second_parent",
            "parent_count",
            SelectQuery::new("MembershipParent").count("value"),
            true,
        )];
        let repo = context
            .entity_data_service::<Executor>("MembershipChild")
            .unwrap();
        let rows: Vec<_> = if typed {
            repo.fetch_enhanced_entities_with_relation_aggregates::<MembershipChild>(
                &query,
                &aggregates,
            )
            .await
            .unwrap()
            .into_iter()
            .map(|child| {
                assert_eq!(
                    child
                        .state
                        .resolve_entity::<MembershipParent>(100)
                        .is_none(),
                    filtered
                );
                child.row
            })
            .collect()
        } else {
            repo.fetch_smart_list_with_relation_aggregates(&query, &aggregates)
                .await
                .unwrap()
                .into_iter()
                .collect()
        };
        assert_eq!(rows.len(), 2);
        for row in rows.iter() {
            assert_eq!(
                row.get("parent_count").and_then(Value::try_i64),
                Some(1),
                "aggregate membership must use the original scalar FK"
            );
            if filtered && !typed {
                assert_eq!(
                    row.get(forward),
                    Some(&Value::object(BTreeMap::from([(
                        "id".into(),
                        Value::I64(100)
                    ),])))
                );
            }
        }
        let logs = context.sql_logs();
        assert_eq!(logs.len(), if logging { 3 } else { 0 });
        if logging {
            let paths = logs
                .iter()
                .map(|log| {
                    log.trace_path
                        .iter()
                        .filter(|node| node.kind == teaql_core::TraceKind::Relation)
                        .map(|node| node.entity_type.as_str())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                paths,
                if typed {
                    vec![vec![], vec!["second_parent"], vec![forward]]
                } else {
                    vec![vec![], vec![forward], vec!["second_parent"]]
                }
            );
            assert!(
                logs.iter()
                    .all(|log| log.trace_path.first().unwrap().entity_type == "MembershipChild")
            );
        }
        println!(
            "PASS Rust aggregate membership: aliased={aliased} filtered={filtered} logging={logging} typed={typed}"
        );
    }
}

#[test]
fn aggregate_with_distinct_forward_field() {
    futures_executor::block_on(async {
        verify_aggregate(false, false, false).await;
        verify_aggregate(false, true, false).await;
    });
}

#[test]
fn aggregate_with_visible_overlapping_forward_field() {
    futures_executor::block_on(verify_aggregate(true, false, false));
}

#[test]
fn aggregate_with_filtered_overlapping_forward_field() {
    futures_executor::block_on(verify_aggregate(true, true, false));
}

#[test]
fn aggregate_with_typed_flat_hydration() {
    futures_executor::block_on(async {
        for aliased in [false, true] {
            for filtered in [false, true] {
                verify_aggregate(aliased, filtered, true).await;
            }
        }
    });
}

#[test]
fn aggregate_alias_cannot_change_later_aggregate_membership() {
    futures_executor::block_on(async {
        let context = setup(false).await;
        let query = PurposedSelectQuery::new(
            SelectQuery::new("MembershipChild")
                .limit(10)
                .comment("project two independent counts"),
            "verify aliases cannot change subsequent aggregation keys",
        );
        let count = |alias| {
            teaql_core::RelationAggregate::new(
                "second_parent",
                alias,
                SelectQuery::new("MembershipParent").count("value"),
                true,
            )
        };
        let rows = context
            .entity_data_service::<Executor>("MembershipChild")
            .unwrap()
            .fetch_smart_list_with_relation_aggregates(
                &query,
                &[count("parent_id"), count("parent_count")],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        for row in rows.iter() {
            assert_eq!(row.get("parent_id").and_then(Value::try_i64), Some(1));
            assert_eq!(row.get("parent_count").and_then(Value::try_i64), Some(1));
        }
        assert_eq!(context.sql_logs().len(), 3);
    });
}

#[test]
fn typed_filtered_reference_has_an_edge_owned_identity_only_view() {
    futures_executor::block_on(async {
        for aliased in [false, true] {
            for optimized in [false, true] {
                let context = setup(aliased).await;
                let forward = if aliased { "parent_id" } else { "parent" };
                let query = PurposedSelectQuery::new(
                    SelectQuery::new("MembershipChild")
                        .relation_query(
                            forward,
                            SelectQuery::new("MembershipParent").filter(Expr::eq("name", "ABSENT")),
                        )
                        .relation_query("second_parent", SelectQuery::new("MembershipParent"))
                        .limit(10)
                        .comment("load filtered and visible references to the same parent"),
                    "preserve identity without leaking sibling detail",
                );
                let repo = context
                    .entity_data_service::<Executor>("MembershipChild")
                    .unwrap();
                let rows = if optimized {
                    repo.fetch_enhanced_entities::<MembershipChild>(&query)
                        .await
                        .unwrap()
                } else {
                    // A relation aggregate selects the general flat graph path;
                    // fetch_entities intentionally constructs no identity graph.
                    let aggregates = [teaql_core::RelationAggregate::new(
                        "second_parent",
                        "parent_count",
                        SelectQuery::new("MembershipParent").count("value"),
                        true,
                    )];
                    repo.fetch_enhanced_entities_with_relation_aggregates::<MembershipChild>(
                        &query,
                        &aggregates,
                    )
                    .await
                    .unwrap()
                };
                assert_eq!(rows.len(), 2);
                for child in rows {
                    let id = child.row.get("id").and_then(Value::try_u64).unwrap();
                    let view = child
                        .state
                        .resolve_relation_option::<MembershipParent>("MembershipChild", id, forward)
                        .expect("filtered edge must not fall back to the shared identity table")
                        .as_ref()
                        .expect("real FK cannot become a null relationship");
                    assert_eq!(view.row.get("id").and_then(Value::try_i64), Some(100));
                    assert!(!teaql_core::Entity::is_field_loaded(view, "name"));
                    assert!(!view.row.contains_key("version"));
                    assert_eq!(
                        child.row.get("parent_id").and_then(Value::try_i64),
                        Some(100)
                    );
                    let visible = child.state.resolve_entity::<MembershipParent>(100).unwrap();
                    assert_eq!(
                        visible.row.get("name"),
                        Some(&Value::Text(PRIVATE_NAME.into()))
                    );
                }
            }
        }
    });
}

#[test]
fn explicit_forward_projection_survives_reverse_inverse_wiring_with_aggregates() {
    futures_executor::block_on(async {
        for aliased in [false, true] {
            for filtered in [false, true] {
                for aggregated in [false, true] {
                    for logging in [false, true] {
                        let mut context = setup(aliased).await;
                        if !logging {
                            context.disable_sql_log();
                        }
                        let forward = if aliased { "parent_id" } else { "parent" };
                        let query = PurposedSelectQuery::new(
                            SelectQuery::new("MembershipParent")
                                .project("id")
                                .project("version")
                                .limit(10)
                                .relation_query(
                                    "children",
                                    SelectQuery::new("MembershipChild")
                                        .relation_query(
                                            forward,
                                            SelectQuery::new("MembershipParent")
                                                .filter(Expr::eq(
                                                    "id",
                                                    if filtered { 0_i64 } else { 100_i64 },
                                                ))
                                                .limit(10),
                                        )
                                        .limit(10),
                                )
                                .comment("inspect explicitly selected parent detail"),
                            "inverse convenience must not replace an explicit projection",
                        );
                        let repo = context
                            .entity_data_service::<Executor>("MembershipParent")
                            .unwrap();
                        let aggregates = [teaql_core::RelationAggregate::new(
                            "children",
                            "selected_count",
                            SelectQuery::new("MembershipChild")
                                .filter(Expr::eq("name", "first"))
                                .count("value"),
                            true,
                        )];
                        let rows = if aggregated {
                            repo.fetch_enhanced_entities_with_relation_aggregates::<MembershipParent>(&query, &aggregates).await.unwrap()
                        } else {
                            repo.fetch_enhanced_entities::<MembershipParent>(&query)
                                .await
                                .unwrap()
                        };
                        assert_eq!(rows.len(), 1);
                        let parent = &rows.data[0];
                        assert!(!teaql_core::Entity::is_field_loaded(parent, "name"));
                        if aggregated {
                            assert_eq!(
                                parent.row.get("selected_count").and_then(Value::try_i64),
                                Some(1)
                            );
                        }
                        let children = parent
                            .state
                            .resolve_relation_list::<MembershipChild>(
                                "MembershipParent",
                                100,
                                "children",
                            )
                            .unwrap();
                        assert_eq!(children.len(), 2);
                        for child in &children.data {
                            let id = child.row.get("id").and_then(Value::try_u64).unwrap();
                            assert_eq!(
                                child.row.get("parent_id").and_then(Value::try_i64),
                                Some(100)
                            );
                            let owned = child.state.resolve_relation_option::<MembershipParent>(
                                "MembershipChild",
                                id,
                                forward,
                            );
                            let shared = child.state.resolve_entity::<MembershipParent>(100);
                            let view = owned
                                .as_ref()
                                .and_then(|v| v.as_ref())
                                .or(shared.as_deref())
                                .unwrap();
                            assert_eq!(view.row.get("id").and_then(Value::try_i64), Some(100));
                            assert_eq!(
                                teaql_core::Entity::is_field_loaded(view, "name"),
                                !filtered,
                                "explicit forward detail must survive inverse convenience wiring"
                            );
                            if !filtered {
                                assert_eq!(
                                    view.row.get("name"),
                                    Some(&Value::Text(PRIVATE_NAME.into()))
                                );
                            }
                        }
                        println!(
                            "EXPLICIT_FORWARD_PROJECTION aliased={aliased} filtered={filtered} aggregated={aggregated} logging={logging}"
                        );
                    }
                }
            }
        }
    });
}

#[test]
fn future_aggregate_bindings_are_masked_before_parent_sql_for_all_result_shapes() {
    futures_executor::block_on(async {
        for logging in [false, true] {
            for shape in ["rows", "typed", "owned"] {
                let mut context = setup(false).await;
                if !logging {
                    context.disable_sql_log();
                }
                let query = PurposedSelectQuery::new(
                    SelectQuery::new("MembershipChild")
                        .limit(10)
                        .comment(format!("inspect {PRIVATE_NAME}")),
                    format!("verify aggregate {PRIVATE_NAME}"),
                );
                let aggregates = [teaql_core::RelationAggregate::new(
                    "parent",
                    "private_count",
                    SelectQuery::new("MembershipParent")
                        .filter(Expr::eq("name", PRIVATE_NAME))
                        .count("value"),
                    true,
                )];
                let repo = context
                    .entity_data_service::<Executor>("MembershipChild")
                    .unwrap();
                let values: Vec<_> = match shape {
                    "rows" => {
                        repo.fetch_smart_list_with_relation_aggregates(&query, &aggregates)
                            .await
                            .unwrap()
                            .data
                    }
                    "typed" => repo
                        .fetch_enhanced_entities_with_relation_aggregates::<MembershipChild>(
                            &query,
                            &aggregates,
                        )
                        .await
                        .unwrap()
                        .data
                        .into_iter()
                        .map(|row| row.row)
                        .collect(),
                    "owned" => repo
                        .fetch_enhanced_entities_with_relation_aggregates_owned::<MembershipChild>(
                            query,
                            &aggregates,
                        )
                        .await
                        .unwrap()
                        .data
                        .into_iter()
                        .map(|row| row.row)
                        .collect(),
                    _ => unreachable!(),
                };
                assert_eq!(values.len(), 2);
                for row in values {
                    assert_eq!(row.get("private_count").and_then(Value::try_i64), Some(1));
                }
                let logs = context.sql_logs();
                assert_eq!(logs.len(), if logging { 2 } else { 0 });
                for entry in &logs {
                    assert_eq!(entry.comment.as_deref(), Some("inspect [REDACTED]"));
                    assert_eq!(
                        entry.purpose.as_deref(),
                        Some("verify aggregate [REDACTED]")
                    );
                    assert!(!format!("{entry:?}").contains(PRIVATE_NAME));
                }
                context.clear_sql_logs();
                let independent = PurposedSelectQuery::new(
                    SelectQuery::new("MembershipChild")
                        .limit(10)
                        .comment(format!("independent {PRIVATE_NAME}")),
                    "no aggregate in this request",
                );
                assert_eq!(
                    repo.fetch_enhanced_entities::<MembershipChild>(&independent)
                        .await
                        .unwrap()
                        .len(),
                    2
                );
                if logging {
                    assert_eq!(
                        context.sql_logs()[0].comment.as_deref(),
                        Some(format!("independent {PRIVATE_NAME}").as_str())
                    );
                }
                println!("FUTURE_AGGREGATE_PRIVACY shape={shape} logging={logging}");
            }
        }
    });
}

#[test]
fn nested_typed_selection_executes_its_related_counts_once() {
    futures_executor::block_on(async {
        for logging in [false, true] {
            for cyclic in [false, true] {
                let mut context = setup(false).await;
                if !logging {
                    context.disable_sql_log();
                }
                let mut selected = SelectQuery::new("MembershipParent")
                    .project("id")
                    .project("version")
                    .limit(10);
                if cyclic {
                    selected = selected.relation_query(
                        "children",
                        SelectQuery::new("MembershipChild")
                            .limit(10)
                            .relation_query(
                                "parent",
                                SelectQuery::new("MembershipParent").limit(10),
                            ),
                    );
                }
                let mut nested = teaql_core::request::QuerySelection::new(selected);
                nested.query_options.relation_aggregates.push(
                    teaql_core::request::RelationAggregate::new(
                        "children",
                        "selected_count",
                        SelectQuery::new("MembershipChild")
                            .filter(Expr::eq("name", "first"))
                            .count("value"),
                        true,
                    ),
                );
                let query = PurposedSelectQuery::new(
                    SelectQuery::new("MembershipChild")
                        .limit(10)
                        .relation_query("parent", nested.into_query())
                        .comment("load nested related count"),
                    "typed selections must preserve related metrics",
                );
                let repo = context
                    .entity_data_service::<Executor>("MembershipChild")
                    .unwrap();
                let rows = repo
                    .fetch_enhanced_entities::<MembershipChild>(&query)
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 2);
                for child in &rows.data {
                    let owner_id = child.row.get("id").and_then(Value::try_u64).unwrap();
                    let owned = child.state.resolve_relation_option::<MembershipParent>(
                        "MembershipChild",
                        owner_id,
                        "parent",
                    );
                    let shared = child.state.resolve_entity::<MembershipParent>(100);
                    let parent = owned
                        .as_ref()
                        .and_then(|value| value.as_ref())
                        .or(shared)
                        .unwrap();
                    assert_eq!(
                        parent.row.get("selected_count").and_then(Value::try_i64),
                        Some(1)
                    );
                    assert!(!teaql_core::Entity::is_field_loaded(parent, "name"));
                    if cyclic {
                        let members = parent
                            .state
                            .resolve_relation_list::<MembershipChild>(
                                "MembershipParent",
                                100,
                                "children",
                            )
                            .unwrap();
                        assert_eq!(members.len(), 2);
                        for member in &members.data {
                            let detail = member
                                .state
                                .resolve_entity::<MembershipParent>(100)
                                .unwrap();
                            assert_eq!(
                                detail.row.get("name"),
                                Some(&Value::Text(PRIVATE_NAME.into()))
                            );
                        }
                    }
                }
                let logs = context.sql_logs();
                assert_eq!(
                    logs.len(),
                    if logging {
                        if cyclic { 5 } else { 3 }
                    } else {
                        0
                    }
                );
                if logging {
                    assert_eq!(
                        logs.iter()
                            .filter(|entry| entry.sql.to_ascii_uppercase().contains("COUNT("))
                            .count(),
                        1
                    );
                    let count = logs
                        .iter()
                        .find(|entry| entry.sql.to_ascii_uppercase().contains("COUNT("))
                        .unwrap();
                    let edges: Vec<_> = count
                        .trace_path
                        .iter()
                        .filter(|node| node.kind == teaql_core::TraceKind::Relation)
                        .map(|node| node.comment.as_str())
                        .collect();
                    assert_eq!(
                        edges,
                        ["MembershipChild.parent", "MembershipParent.children"]
                    );
                }
                println!(
                    "NESTED_TYPED_AGGREGATE logging={logging} cyclic={cyclic} children=2 count=1"
                );
            }
        }
    });
}
