//! Native SQLite probe for relation membership and execution-owned trace paths (#239).
//! No generated source or manually inserted trace frames are used.
use std::sync::Arc;

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
                Some(&Value::Null),
                "inverse attachment must not undo the explicit filter"
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
                assert_eq!(row.get(forward), Some(&Value::Null));
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
