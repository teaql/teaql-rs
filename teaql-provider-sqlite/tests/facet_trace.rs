//! Root/nested Facet execution through handwritten metadata and real SQLite (#239).
//! Relation frames are produced by the runtime, never supplied by this fixture.
use std::sync::Arc;

use teaql_core::request::{FacetRequest, QueryOptions, QuerySelection, RelationAggregate};
use teaql_core::{
    CompactRow, DataType, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor,
    RelationDescriptor, SelectQuery, SmartList, TraceKind, TraceNode, Value,
};
use teaql_data_service::{MutationCommand, SchemaProvider};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{
    InMemoryMetadataStore, PurposedSelectQuery, RuntimeError, TeaqlRuntime, UserContext,
    execute_facets,
};
use teaql_sql::SqlDataServiceExecutor;

#[derive(Clone)]
struct Schema(Vec<Arc<EntityDescriptor>>);
impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        self.0.iter().find(|entity| entity.name == name).cloned()
    }
}
type Executor = SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;
const SECRET: &str = "FACET-PRIVATE-SCHOOL";

struct Runtime(UserContext);
impl TeaqlRuntime for Runtime {
    fn user_context(&self) -> &UserContext {
        &self.0
    }

    async fn fetch_facet_smart_list(
        &self,
        entity: &str,
        query: &PurposedSelectQuery,
        aggregates: &[teaql_core::RelationAggregate],
        trace_context: Vec<TraceNode>,
    ) -> Result<SmartList<CompactRow>, RuntimeError> {
        self.0
            .entity_data_service::<Executor>(entity)
            .unwrap()
            .with_trace_context(trace_context)
            .fetch_smart_list_with_relation_aggregates(query, aggregates)
            .await
            .map_err(|error| RuntimeError::Graph(error.to_string()))
    }
}

fn entity(name: &str, table: &str) -> EntityDescriptor {
    EntityDescriptor::new(name)
        .table_name(table)
        .property(PropertyDescriptor::new("id", DataType::I64).id())
        .property(PropertyDescriptor::new("version", DataType::I64).version())
        .property(PropertyDescriptor::new("name", DataType::Text))
}

async fn setup() -> Runtime {
    let school = entity("School", "facet_school")
        .audit_mask_fields(vec!["name".into()])
        .property(PropertyDescriptor::new("type_id", DataType::I64))
        .relation(
            RelationDescriptor::new("school_type", "SchoolType")
                .local_key("type_id")
                .foreign_key("id"),
        );
    let school_type = entity("SchoolType", "facet_school_type")
        .property(PropertyDescriptor::new("platform_id", DataType::I64))
        .relation(
            RelationDescriptor::new("schools", "School")
                .many()
                .local_key("id")
                .foreign_key("type_id"),
        )
        .relation(
            RelationDescriptor::new("platform", "Platform")
                .local_key("platform_id")
                .foreign_key("id"),
        );
    let platform = entity("Platform", "facet_platform").relation(
        RelationDescriptor::new("school_types", "SchoolType")
            .many()
            .local_key("id")
            .foreign_key("platform_id"),
    );
    let mut context = UserContext::new().with_metadata(
        InMemoryMetadataStore::new()
            .with_entity(school.clone())
            .with_entity(school_type.clone())
            .with_entity(platform.clone()),
    );
    let transport =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open_in_memory().unwrap());
    context.use_sqlite_provider(transport.clone());
    context.ensure_schema().await.unwrap();
    context.insert_resource(Executor::new(
        SqliteDialect,
        transport,
        Schema(vec![
            Arc::new(school),
            Arc::new(school_type),
            Arc::new(platform),
        ]),
    ));
    context
        .execute_in_transaction::<Executor, _, _>(|transaction| {
            Box::pin(async move {
                for command in [
                    InsertCommand::new("Platform")
                        .value("id", 1_i64)
                        .value("name", "used"),
                    InsertCommand::new("Platform")
                        .value("id", 2_i64)
                        .value("name", "empty"),
                    InsertCommand::new("SchoolType")
                        .value("id", 10_i64)
                        .value("name", "primary")
                        .value("platform_id", 1_i64),
                    InsertCommand::new("SchoolType")
                        .value("id", 20_i64)
                        .value("name", "secondary")
                        .value("platform_id", 1_i64),
                    InsertCommand::new("SchoolType")
                        .value("id", 30_i64)
                        .value("name", "unused")
                        .value("platform_id", 1_i64),
                    InsertCommand::new("School")
                        .value("id", 100_i64)
                        .value("name", SECRET)
                        .value("type_id", 10_i64),
                    InsertCommand::new("School")
                        .value("id", 101_i64)
                        .value("name", SECRET)
                        .value("type_id", 10_i64),
                    InsertCommand::new("School")
                        .value("id", 102_i64)
                        .value("name", SECRET)
                        .value("type_id", 20_i64),
                    InsertCommand::new("School")
                        .value("id", 103_i64)
                        .value("name", "excluded")
                        .value("type_id", 20_i64),
                ] {
                    transaction
                        .mutate(
                            MutationCommand::Insert(command.value("version", 1_i64))
                                .request("seed nested facet fixture")?,
                        )
                        .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    context.clear_sql_logs();
    Runtime(context)
}

fn options(nested: bool, include_all: bool) -> QueryOptions {
    let mut types = QuerySelection::new(SelectQuery::new("SchoolType").limit(10));
    types
        .query_options
        .relation_aggregates
        .push(RelationAggregate::new(
            "schools",
            "school_count",
            SelectQuery::new("School"),
            true,
        ));
    if nested {
        let mut platforms = QuerySelection::new(SelectQuery::new("Platform").limit(10));
        platforms
            .query_options
            .relation_aggregates
            .push(RelationAggregate::new(
                "school_types",
                "type_count",
                SelectQuery::new("SchoolType"),
                true,
            ));
        // Display aliases intentionally differ from both relation and entity names.
        types.query_options.facets.push(FacetRequest::new(
            "platformChoices",
            "platform",
            platforms,
            include_all,
        ));
    }
    QueryOptions {
        facets: vec![FacetRequest::new(
            "typeChoices",
            "school_type",
            types,
            include_all,
        )],
        ..Default::default()
    }
}

fn counts(rows: &SmartList<CompactRow>, alias: &str) -> Vec<(i64, i64)> {
    let mut values = rows
        .data
        .iter()
        .map(|row| {
            (
                row.get("id").and_then(Value::try_i64).unwrap(),
                row.get(alias).and_then(Value::try_i64).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    values.sort();
    values
}

async fn verify(nested: bool, include_all: bool, logging: bool) {
    let mut runtime = setup().await;
    if !logging {
        runtime.0.disable_sql_log();
    }
    let query = PurposedSelectQuery::new(
        SelectQuery::new("School")
            .limit(10)
            .filter(Expr::eq("name", SECRET))
            .comment(format!("load school facets {SECRET}")),
        format!("render choices {SECRET}"),
    );
    let root = runtime
        .0
        .entity_data_service::<Executor>("School")
        .unwrap()
        .fetch_all(&query)
        .await
        .unwrap();
    assert_eq!(root.len(), 3, "private bind must reach SQLite unchanged");
    let options = options(nested, include_all);
    let facets = execute_facets(&runtime, query.as_query(), &options)
        .await
        .unwrap();
    let types = facets.get("typeChoices").unwrap();
    let mut expected = vec![(10, 2), (20, 1)];
    if include_all {
        expected.push((30, 0));
    }
    assert_eq!(counts(types, "school_count"), expected);
    if nested {
        let platforms = types
            .facets
            .get("platformChoices")
            .expect("nested Facet options must execute and materialize");
        let mut expected = vec![(1, if include_all { 3 } else { 2 })];
        if include_all {
            expected.push((2, 0));
        }
        assert_eq!(counts(platforms, "type_count"), expected);
    }
    let logs = runtime.0.sql_logs();
    assert_eq!(
        logs.len(),
        if logging {
            if nested { 5 } else { 3 }
        } else {
            0
        }
    );
    if logging {
        let mut expected_edges = vec![
            vec![],
            vec!["School.school_type"],
            vec!["School.school_type", "SchoolType.schools"],
        ];
        if nested {
            expected_edges.push(vec!["School.school_type", "SchoolType.platform"]);
            expected_edges.push(vec![
                "School.school_type",
                "SchoolType.platform",
                "Platform.school_types",
            ]);
        }
        for (log, edges) in logs.iter().zip(expected_edges) {
            let path = &log.trace_path;
            assert_eq!(
                (path[0].kind, path[0].entity_type.as_str()),
                (TraceKind::Operation, "School")
            );
            assert_eq!(
                (path[1].kind, path[1].entity_type.as_str()),
                (TraceKind::Request, "School")
            );
            assert_eq!(
                path.len(),
                edges.len() + 4,
                "no duplicate ancestors: {path:?}"
            );
            assert_eq!(
                path.iter()
                    .filter(|node| node.kind == TraceKind::Relation)
                    .map(|node| node.comment.as_str())
                    .collect::<Vec<_>>(),
                edges
            );
            for edge in path.iter().filter(|node| node.kind == TraceKind::Relation) {
                assert_eq!(
                    edge.comment.rsplit('.').next(),
                    Some(edge.entity_type.as_str())
                );
            }
            assert_eq!(
                (
                    path[path.len() - 2].kind,
                    path[path.len() - 2].entity_type.as_str()
                ),
                (TraceKind::Provider, "sqlite")
            );
            assert_eq!(
                (
                    path[path.len() - 1].kind,
                    path[path.len() - 1].entity_type.as_str()
                ),
                (TraceKind::Sql, "select")
            );
            assert!(
                log.comment
                    .as_deref()
                    .unwrap()
                    .contains("load school facets")
            );
            assert!(log.purpose.as_deref().unwrap().contains("render choices"));
        }
        assert!(
            !format!("{logs:?}").contains(SECRET),
            "inherited root intent must be safe even without its bind"
        );
    }
    assert!(
        query
            .as_query()
            .comment
            .as_deref()
            .unwrap()
            .contains(SECRET)
    );
    runtime.0.clear_sql_logs();
    // Reusing Context does not retain the previous request's route or redactions.
    runtime
        .0
        .entity_data_service::<Executor>("Platform")
        .unwrap()
        .fetch_all(&PurposedSelectQuery::new(
            SelectQuery::new("Platform")
                .limit(10)
                .comment(format!("independent public literal {SECRET}")),
            "verify isolation",
        ))
        .await
        .unwrap();
    let independent = runtime.0.sql_logs();
    assert_eq!(independent.len(), usize::from(logging));
    if logging {
        assert_eq!(independent[0].trace_path.len(), 4);
        assert_eq!(independent[0].trace_path[0].entity_type, "Platform");
        assert!(independent[0].comment.as_deref().unwrap().contains(SECRET));
    }
    println!(
        "PASS SQLite root/nested facets: nested={nested} include_all={include_all} logging={logging}"
    );
}

#[test]
fn root_facets_keep_original_root_and_field_edges() {
    futures_executor::block_on(verify(false, true, true));
}

#[test]
fn nested_facets_preserve_counts_routes_privacy_and_log_off() {
    futures_executor::block_on(async {
        for include_all in [true, false] {
            for logging in [true, false] {
                verify(true, include_all, logging).await;
            }
        }
    });
}

struct SchoolRow(CompactRow);
impl teaql_core::TeaqlEntity for SchoolRow {
    const ENTITY_NAME: &'static str = "School";
    fn entity_descriptor() -> EntityDescriptor {
        entity("School", "facet_school")
    }
}
impl teaql_core::Entity for SchoolRow {
    fn from_compact_row(row: CompactRow) -> Result<Self, teaql_core::EntityError> {
        Ok(Self(row))
    }
    fn into_values(self) -> teaql_core::MutationValues {
        self.0.into_map().into()
    }
}

fn future_private_options(secret: &str) -> QueryOptions {
    let mut options = options(false, true);
    options.facets[0].query.query_options.relation_aggregates[0]
        .query
        .query = SelectQuery::new("School").filter(Expr::eq("name", secret));
    options
}

#[test]
fn future_facet_bindings_are_safe_before_root_rows_entities_and_stream() {
    futures_executor::block_on(async {
        use futures_util::StreamExt;
        let runtime = setup().await;
        let query = PurposedSelectQuery::new(
            SelectQuery::new("School")
                .limit(10)
                .comment(format!("future facets {SECRET}")),
            format!("render future choices {SECRET}"),
        )
        .with_facet_diagnostics(&future_private_options(SECRET));
        assert!(query.as_query().child_enhancements.is_empty());
        let repository = runtime.0.entity_data_service::<Executor>("School").unwrap();
        assert_eq!(repository.fetch_all(&query).await.unwrap().len(), 4);
        assert_eq!(
            repository
                .fetch_all_owned(query.clone())
                .await
                .unwrap()
                .len(),
            4
        );
        assert_eq!(
            repository
                .fetch_smart_list(&query)
                .await
                .unwrap()
                .data
                .len(),
            4
        );
        assert_eq!(
            repository
                .fetch_smart_list_with_relation_aggregates(&query, &[])
                .await
                .unwrap()
                .data
                .len(),
            4
        );
        assert_eq!(
            repository
                .fetch_entities::<SchoolRow>(&query)
                .await
                .unwrap()
                .data
                .len(),
            4
        );
        assert_eq!(
            repository
                .fetch_enhanced_entities::<SchoolRow>(&query)
                .await
                .unwrap()
                .data
                .len(),
            4
        );
        assert_eq!(
            repository
                .fetch_enhanced_entities_with_relation_aggregates::<SchoolRow>(&query, &[])
                .await
                .unwrap()
                .data
                .len(),
            4
        );
        assert_eq!(
            repository
                .fetch_enhanced_entities_with_relation_aggregates_owned::<SchoolRow>(
                    query.clone(),
                    &[]
                )
                .await
                .unwrap()
                .data
                .len(),
            4
        );
        let mut stream = repository.fetch_stream(&query).await.unwrap();
        let mut rows = 0;
        while let Some(chunk) = stream.next().await {
            rows += chunk.unwrap().rows.len();
        }
        assert_eq!(rows, 4);
        let logs = runtime.0.sql_logs();
        assert_eq!(
            logs.len(),
            9,
            "diagnostic future queries must never execute"
        );
        for (index, log) in logs.iter().enumerate() {
            assert!(
                !format!("{log:?}").contains(SECRET),
                "root entry {index}: {log:#?}"
            );
        }
        assert!(
            logs.iter()
                .all(|log| log.trace_path.len() == 4 && log.params.is_empty())
        );
        assert!(
            query
                .as_query()
                .comment
                .as_deref()
                .unwrap()
                .contains(SECRET)
        );
    });
}

#[test]
fn repeated_facet_diagnostics_merge_with_removed_count_sources() {
    futures_executor::block_on(async {
        let runtime = setup().await;
        let first = "PRIVATE-FUTURE-A";
        let second = "PRIVATE-FUTURE-B";
        let removed = "PRIVATE-REMOVED-COUNT-RELATION";
        let query = PurposedSelectQuery::new(
            SelectQuery::new("School")
                .relation_query(
                    "unused",
                    SelectQuery::new("School").filter(Expr::eq("name", removed)),
                )
                .comment(format!("count with {first} {second} {removed}")),
            format!("page with {first} {second} {removed}"),
        )
        .for_exact_count("count")
        .with_facet_diagnostics(&future_private_options(first))
        .with_facet_diagnostics(&future_private_options(second))
        .with_facet_diagnostics(&QueryOptions::default());
        assert!(query.as_query().relations.is_empty());
        assert!(query.as_query().child_enhancements.is_empty());
        let rows = runtime
            .0
            .entity_data_service::<Executor>("School")
            .unwrap()
            .fetch_all(&query)
            .await
            .unwrap();
        assert_eq!(rows[0].get("count").and_then(Value::try_i64), Some(4));
        let logs = runtime.0.sql_logs();
        assert_eq!(logs.len(), 1);
        let rendered = format!("{logs:?}");
        for secret in [first, second, removed] {
            assert!(!rendered.contains(secret));
        }
    });
}
