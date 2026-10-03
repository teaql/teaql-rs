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
