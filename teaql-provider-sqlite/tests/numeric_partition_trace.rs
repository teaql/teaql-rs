//! TC-SQL-10: scalar grouping/partitioning is not a model relation (#239).
//! Actual SQLite rows and safe SQL logs are the oracle; no trace frames are injected.
use std::sync::Arc;

use teaql_core::{
    BinaryOp, DataType, EntityDescriptor, Expr, InsertCommand, OrderBy, PropertyDescriptor,
    RelationDescriptor, SelectQuery, TraceKind, Value,
};
use teaql_data_service::{MutationCommand, SchemaProvider};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, PurposedSelectQuery, UserContext};
use teaql_sql::SqlDataServiceExecutor;

const SECRET: &str = "PRIVATE-NUMERIC-PARTITION";

#[derive(Clone)]
struct Schema(Vec<Arc<EntityDescriptor>>);
impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        self.0.iter().find(|entity| entity.name == name).cloned()
    }
}
type Executor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Shape {
    Root,
    RelationWindow,
    RelationProbe,
}

async fn setup() -> UserContext {
    let parent = EntityDescriptor::new("GroupOwner")
        .table_name("group_owner")
        .property(PropertyDescriptor::new("id", DataType::I64).id())
        .property(PropertyDescriptor::new("version", DataType::I64).version())
        .relation(
            RelationDescriptor::new("samples", "MetricSample")
                .many()
                .local_key("id")
                .foreign_key("owner_id"),
        );
    let child = EntityDescriptor::new("MetricSample")
        .table_name("metric_sample")
        .property(PropertyDescriptor::new("id", DataType::I64).id())
        .property(PropertyDescriptor::new("version", DataType::I64).version())
        .property(PropertyDescriptor::new("owner_id", DataType::I64))
        .property(PropertyDescriptor::new("bucket", DataType::I64))
        .property(PropertyDescriptor::new("name", DataType::Text))
        .audit_mask_fields(vec!["name".into()]);
    assert!(child.relation_by_name("bucket").is_none());
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
                transaction
                    .mutate(
                        MutationCommand::Insert(
                            InsertCommand::new("GroupOwner")
                                .value("id", 1_i64)
                                .value("version", 1_i64),
                        )
                        .request("seed numeric partition owner")?,
                    )
                    .await?;
                for index in 0..4_i64 {
                    transaction
                        .mutate(
                            MutationCommand::Insert(
                                InsertCommand::new("MetricSample")
                                    .value("id", index + 1)
                                    .value("version", 1_i64)
                                    .value("owner_id", 1_i64)
                                    .value("bucket", if index < 2 { 10_i64 } else { 20 })
                                    .value("name", if index == 3 { "excluded" } else { SECRET }),
                            )
                            .request("seed numeric partition sample")?,
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

async fn verify(shape: Shape, logging: bool, having: bool) {
    let mut context = setup().await;
    if !logging {
        context.disable_sql_log();
    }
    let mut grouped = SelectQuery::new("MetricSample")
        .group_by("owner_id")
        .group_by("bucket")
        .count("n")
        .filter(Expr::eq("name", SECRET))
        .order_by(OrderBy::asc("bucket"))
        .limit(10);
    if having {
        grouped = grouped.having(Expr::binary(
            Expr::count_all(),
            BinaryOp::Gt,
            Expr::value(1_i64),
        ));
    }
    let root = if shape == Shape::Root {
        "MetricSample"
    } else {
        "GroupOwner"
    };
    let query = match shape {
        Shape::Root => grouped.partition_by("bucket"),
        Shape::RelationWindow | Shape::RelationProbe => SelectQuery::new("GroupOwner")
            .project("id")
            .limit(1)
            .relation_query(
                "samples",
                grouped.top_n_probe_parent_threshold(if shape == Shape::RelationWindow {
                    0
                } else {
                    32
                }),
            ),
    };
    let comment = format!("count numeric groups {SECRET}");
    let purpose = format!("render numeric partitions {SECRET}");
    let query = PurposedSelectQuery::new(query.comment(&comment), &purpose);
    let original = query.as_query().clone();
    let result = context
        .entity_data_service::<Executor>(root)
        .unwrap()
        .fetch_all(&query)
        .await
        .unwrap();
    let mut actual = if shape == Shape::Root {
        result
            .iter()
            .map(|row| {
                (
                    row.get("bucket").and_then(Value::try_i64).unwrap(),
                    row.get("n").and_then(Value::try_i64).unwrap(),
                )
            })
            .collect::<Vec<_>>()
    } else {
        assert_eq!(result.len(), 1);
        let Some(Value::List(samples)) = result[0].get("samples") else {
            panic!("loaded sample list missing")
        };
        samples
            .iter()
            .map(|sample| {
                let Value::Object(row) = sample else {
                    panic!("grouped sample is not an object")
                };
                (
                    row["bucket"].try_i64().unwrap(),
                    row["n"].try_i64().unwrap(),
                )
            })
            .collect::<Vec<_>>()
    };
    actual.sort();
    assert_eq!(
        actual,
        if having {
            vec![(10, 2)]
        } else {
            vec![(10, 2), (20, 1)]
        },
        "{shape:?}, logging={logging}, having={having}"
    );
    assert_eq!(
        query.as_query(),
        &original,
        "caller intent and bindings remain unchanged"
    );
    let logs = context.sql_logs();
    assert_eq!(
        logs.len(),
        if logging {
            if shape == Shape::Root { 1 } else { 2 }
        } else {
            0
        }
    );
    for (index, log) in logs.iter().enumerate() {
        let relations = if shape != Shape::Root && index > 0 {
            vec!["samples"]
        } else {
            vec![]
        };
        let names = [
            vec![root, root],
            relations.clone(),
            vec!["sqlite", "select"],
        ]
        .concat();
        let kinds = [
            vec![TraceKind::Operation, TraceKind::Request],
            vec![TraceKind::Relation; relations.len()],
            vec![TraceKind::Provider, TraceKind::Sql],
        ]
        .concat();
        assert_eq!(
            log.trace_path
                .iter()
                .map(|node| node.entity_type.as_str())
                .collect::<Vec<_>>(),
            names
        );
        assert_eq!(
            log.trace_path
                .iter()
                .map(|node| node.kind)
                .collect::<Vec<_>>(),
            kinds
        );
        assert!(
            !format!("{log:?}").contains(SECRET),
            "including first root statement before the private child bind"
        );
        assert!(
            log.comment
                .as_deref()
                .unwrap()
                .contains("count numeric groups")
        );
        assert!(
            log.purpose
                .as_deref()
                .unwrap()
                .contains("render numeric partitions")
        );
        assert_eq!(
            log.log_context.execution_outcome,
            Some(teaql_data_service::SqlExecutionOutcome::Success)
        );
        if log.sql.contains("COUNT(") {
            assert!(log.sql.contains("GROUP BY"));
            assert_eq!(log.sql.contains("HAVING"), having);
            assert_eq!(
                log.sql.contains("PARTITION BY"),
                shape != Shape::RelationProbe
            );
            assert!(
                !log.sql.contains(", id ASC"),
                "grouped Top-N must not order by an ungrouped source-row id: {}",
                log.sql
            );
        }
        println!(
            "NUMERIC_SQL shape={shape:?} having={having} sql={} path={:?}",
            log.debug_sql, log.trace_path
        );
    }
    println!("NUMERIC_RESULT shape={shape:?} logging={logging} having={having} counts={actual:?}");
    context.clear_sql_logs();
    context
        .entity_data_service::<Executor>("GroupOwner")
        .unwrap()
        .fetch_all(&PurposedSelectQuery::new(
            SelectQuery::new("GroupOwner").limit(1).comment(SECRET),
            "independent query",
        ))
        .await
        .unwrap();
    if logging {
        assert_eq!(
            context.sql_logs()[0].comment.as_deref(),
            Some(SECRET),
            "no inherited redaction in independent request"
        );
        assert_eq!(context.sql_logs()[0].trace_path.len(), 4);
    } else {
        assert!(context.sql_logs().is_empty());
    }
}

#[test]
fn scalar_partition_keeps_groups_and_does_not_invent_relation_edges() {
    futures_executor::block_on(async {
        for logging in [true, false] {
            for having in [false, true] {
                verify(Shape::Root, logging, having).await;
            }
        }
    });
}

#[test]
fn loaded_scalar_groups_preserve_only_the_real_relation_edge_window() {
    futures_executor::block_on(async {
        for logging in [true, false] {
            for having in [false, true] {
                verify(Shape::RelationWindow, logging, having).await;
            }
        }
    });
}

#[test]
fn loaded_scalar_groups_preserve_only_the_real_relation_edge_probes() {
    futures_executor::block_on(async {
        for logging in [true, false] {
            for having in [false, true] {
                verify(Shape::RelationProbe, logging, having).await;
            }
        }
    });
}
