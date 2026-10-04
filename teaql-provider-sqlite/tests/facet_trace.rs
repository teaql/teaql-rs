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
        .audit_mask_fields(vec!["name".into()])
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

macro_rules! graph_view {
    ($view:ident, $entity:literal, $table:literal) => {
        struct $view {
            row: CompactRow,
            state: teaql_runtime::EntityRuntimeState,
        }
        impl teaql_core::TeaqlEntity for $view {
            const ENTITY_NAME: &'static str = $entity;
            fn entity_descriptor() -> EntityDescriptor {
                entity($entity, $table)
            }
        }
        impl teaql_core::Entity for $view {
            fn from_compact_row(row: CompactRow) -> Result<Self, teaql_core::EntityError> {
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
        impl teaql_core::IdentifiableEntity for $view {
            fn id_value(&self) -> Value {
                self.row.get("id").unwrap().clone()
            }
        }
    };
}
graph_view!(PlatformView, "Platform", "facet_platform");
graph_view!(SchoolTypeView, "SchoolType", "facet_school_type");

async fn loaded_setup() -> Runtime {
    let mut runtime = setup().await;
    runtime
        .0
        .execute_in_transaction::<Executor, _, _>(|transaction| {
            Box::pin(async move {
                for command in [
                    InsertCommand::new("Platform")
                        .value("id", 3_i64)
                        .value("name", "no types"),
                    InsertCommand::new("SchoolType")
                        .value("id", 40_i64)
                        .value("name", "primary")
                        .value("platform_id", 2_i64),
                ] {
                    transaction
                        .mutate(
                            MutationCommand::Insert(command.value("version", 1_i64))
                                .request("seed second loaded Facet owner")?,
                        )
                        .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    let mut decoders = teaql_runtime::InMemoryEntityGraphDecoderRegistry::default();
    decoders.register::<PlatformView>();
    decoders.register::<SchoolTypeView>();
    runtime.0.set_entity_graph_decoder_registry(decoders);
    runtime.0.clear_sql_logs();
    runtime
}

fn loaded_query(include_all: bool, threshold: usize) -> PurposedSelectQuery {
    let mut choices = QuerySelection::new(SelectQuery::new("Platform").limit(10));
    choices
        .query_options
        .relation_aggregates
        .push(RelationAggregate::new(
            "school_types",
            "type_count",
            SelectQuery::new("SchoolType"),
            true,
        ));
    // This binding exists only in a future loaded-relation Facet aggregate.
    choices
        .query_options
        .relation_aggregates
        .push(RelationAggregate::new(
            "school_types",
            "matching_count",
            SelectQuery::new("SchoolType").filter(Expr::eq("name", "primary")),
            true,
        ));
    let mut types = QuerySelection::new(
        SelectQuery::new("SchoolType")
            .filter(Expr::ne("name", "unused"))
            .order_asc("id")
            .limit(1)
            .top_n_probe_parent_threshold(threshold),
    );
    types.query_options.facets.push(FacetRequest::new(
        "platformChoices",
        "platform",
        choices,
        include_all,
    ));
    let mut root = QuerySelection::new(SelectQuery::new("Platform").order_asc("id").limit(10));
    root.relation_selections
        .push(teaql_core::request::RelationSelection::new(
            "school_types",
            types,
        ));
    PurposedSelectQuery::new(
        root.into_query()
            .comment("load primary choices by platform"),
        "render primary counts independently",
    )
    .with_facet_diagnostics(&QueryOptions::default())
}

#[test]
fn loaded_relation_facets_keep_per_parent_counts_and_empty_typed_lists() {
    futures_executor::block_on(async {
        for include_all in [true, false] {
            for logging in [true, false] {
                for threshold in [0, 32] {
                    let mut runtime = loaded_setup().await;
                    if !logging {
                        runtime.0.disable_sql_log();
                    }
                    let query = loaded_query(include_all, threshold);
                    let rows = runtime
                        .0
                        .entity_data_service::<Executor>("Platform")
                        .unwrap()
                        .fetch_enhanced_entities::<PlatformView>(&query)
                        .await
                        .unwrap();
                    assert_eq!(rows.len(), 3);
                    for parent in &rows.data {
                        let id = parent.row.get("id").and_then(Value::try_u64).unwrap();
                        let handle = parent.state.relation_list::<SchoolTypeView>(
                            "Platform",
                            id,
                            "school_types",
                        );
                        let types = handle
                            .value()
                            .expect("selected empty relation still has a SmartList");
                        assert_eq!(
                            types.len(),
                            usize::from(id != 3),
                            "TopN page still has at most one row"
                        );
                        let choices = types.facet("platformChoices").expect(
                            "loaded relation SmartList must retain per-parent Facet metadata",
                        );
                        let expected_count = if id == 1 {
                            2
                        } else if id == 2 {
                            1
                        } else {
                            0
                        };
                        let expected = if include_all {
                            (1..=3)
                                .map(|choice| {
                                    (
                                        choice,
                                        if choice == id as i64 {
                                            expected_count
                                        } else {
                                            0
                                        },
                                    )
                                })
                                .collect()
                        } else if id == 3 {
                            Vec::new()
                        } else {
                            vec![(id as i64, expected_count)]
                        };
                        assert_eq!(
                            counts(choices, "type_count"),
                            expected,
                            "facet membership is full filtered collection, not TopN rows"
                        );
                        let expected_matching = if include_all {
                            (1..=3)
                                .map(|choice| (choice, i64::from(id != 3 && choice == id as i64)))
                                .collect()
                        } else if id == 3 {
                            Vec::new()
                        } else {
                            vec![(id as i64, 1)]
                        };
                        assert_eq!(counts(choices, "matching_count"), expected_matching);
                        assert!(parent.row.keys().all(|key| key != "platformChoices"));
                        assert!(
                            types
                                .data
                                .iter()
                                .all(|row| row.row.keys().all(|key| key != "platformChoices"))
                        );
                        assert_eq!(
                            handle.state(),
                            if id == 3 {
                                teaql_runtime::LoadedRelation::Empty
                            } else {
                                teaql_runtime::LoadedRelation::Loaded
                            }
                        );
                    }
                    let logs = runtime.0.sql_logs();
                    if logging {
                        assert_eq!(
                            logs.len(),
                            1 + if threshold == 0 { 1 } else { 3 }
                                + if include_all { 9 } else { 7 },
                            "bounded one relation batch/probe and one Facet tree per owner"
                        );
                        assert!(
                            !format!("{logs:?}").contains("primary"),
                            "future loaded Facet bind must mask the first root SQL too"
                        );
                        assert!(
                            logs.iter()
                                .all(|log| log.trace_path[0].entity_type == "Platform"
                                    && log.trace_path[1].entity_type == "Platform")
                        );
                        assert!(logs.iter().any(|log| {
                            log.trace_path
                                .iter()
                                .filter(|node| node.kind == TraceKind::Relation)
                                .map(|node| node.comment.as_str())
                                .collect::<Vec<_>>()
                                == vec![
                                    "Platform.school_types",
                                    "SchoolType.platform",
                                    "Platform.school_types",
                                ]
                        }));
                    } else {
                        assert!(logs.is_empty());
                    }
                }
            }
        }
    });
}

#[test]
fn loaded_relation_future_binding_masks_first_root_sql() {
    futures_executor::block_on(async {
        let runtime = loaded_setup().await;
        let query = loaded_query(true, 0);
        runtime
            .0
            .entity_data_service::<Executor>("Platform")
            .unwrap()
            .fetch_enhanced_entities::<PlatformView>(&query)
            .await
            .unwrap();
        let logs = runtime.0.sql_logs();
        assert!(
            !format!("{:?}", logs[0]).contains("primary"),
            "future relation Facets must be captured before first SQL"
        );
    });
}

#[test]
fn loaded_relation_compact_sidecars_never_enter_persistent_values() {
    futures_executor::block_on(async {
        let runtime = loaded_setup().await;
        let rows = runtime
            .0
            .entity_data_service::<Executor>("Platform")
            .unwrap()
            .fetch_all(&loaded_query(true, 0))
            .await
            .unwrap();
        for row in rows {
            let id = row.get("id").and_then(Value::try_i64).unwrap();
            let list = row
                .loaded_relation("school_types")
                .expect("compact result retains list metadata");
            assert_eq!(list.len(), usize::from(id != 3));
            let expected = (1..=3)
                .map(|choice| {
                    (
                        choice,
                        if choice == id {
                            if id == 1 {
                                2
                            } else if id == 2 {
                                1
                            } else {
                                0
                            }
                        } else {
                            0
                        },
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                counts(list.facet("platformChoices").unwrap(), "type_count"),
                expected
            );
            assert!(
                !teaql_core::compact_row_to_json_value(&row)
                    .to_string()
                    .contains("platformChoices")
            );
            assert!(!format!("{:?}", row.into_map()).contains("platformChoices"));
        }
    });
}

#[test]
fn loaded_relation_facets_survive_slow_typed_hydration_with_root_aggregate() {
    futures_executor::block_on(async {
        let runtime = loaded_setup().await;
        let aggregates = [teaql_core::RelationAggregate::new(
            "school_types",
            "whole_count",
            SelectQuery::new("SchoolType"),
            true,
        )];
        let rows = runtime
            .0
            .entity_data_service::<Executor>("Platform")
            .unwrap()
            .fetch_enhanced_entities_with_relation_aggregates::<PlatformView>(
                &loaded_query(true, 0),
                &aggregates,
            )
            .await
            .unwrap();
        for parent in rows.data {
            let id = parent.row.get("id").and_then(Value::try_u64).unwrap();
            let types =
                parent
                    .state
                    .relation_list::<SchoolTypeView>("Platform", id, "school_types");
            let expected = if id == 1 {
                2
            } else if id == 2 {
                1
            } else {
                0
            };
            assert_eq!(
                types
                    .value()
                    .unwrap()
                    .facet("platformChoices")
                    .unwrap()
                    .data
                    .iter()
                    .find(|row| row.get("id").and_then(Value::try_u64) == Some(id))
                    .unwrap()
                    .get("type_count")
                    .and_then(Value::try_i64),
                Some(expected)
            );
            assert_eq!(
                parent.row.get("whole_count").and_then(Value::try_i64),
                Some(if id == 1 {
                    3
                } else if id == 2 {
                    1
                } else {
                    0
                })
            );
        }
        assert_eq!(runtime.0.sql_logs().len(), 12);
        assert!(!format!("{:?}", runtime.0.sql_logs()).contains("primary"));
    });
}

#[test]
fn loaded_to_one_facets_remain_query_metadata_for_shared_and_filtered_targets() {
    futures_executor::block_on(async {
        let runtime = loaded_setup().await;
        let mut choices = QuerySelection::new(SelectQuery::new("SchoolType").limit(10));
        choices
            .query_options
            .relation_aggregates
            .push(RelationAggregate::new(
                "platform",
                "platform_count",
                SelectQuery::new("Platform"),
                true,
            ));
        let mut target =
            QuerySelection::new(SelectQuery::new("Platform").filter(Expr::ne("name", "empty")));
        target.query_options.facets.push(FacetRequest::new(
            "typeChoices",
            "school_types",
            choices,
            false,
        ));
        let mut source = QuerySelection::new(
            SelectQuery::new("SchoolType")
                .projects(["id", "version", "name", "platform_id"])
                .order_asc("id")
                .limit(10),
        );
        source
            .relation_selections
            .push(teaql_core::request::RelationSelection::new(
                "platform", target,
            ));
        let query = PurposedSelectQuery::new(
            source.into_query().comment("load selected platform facets"),
            "retain read-only relation metadata",
        );
        let repository = runtime
            .0
            .entity_data_service::<Executor>("SchoolType")
            .unwrap();
        let rows = repository
            .fetch_enhanced_entities::<SchoolTypeView>(&query)
            .await
            .unwrap();
        assert_eq!(rows.len(), 4);
        for row in rows.data {
            let id = row.row.get("id").and_then(Value::try_u64).unwrap();
            let choices = row
                .state
                .relation_facet("SchoolType", id, "platform", "typeChoices")
                .expect("to-one cardinality must not discard Facet results");
            assert_eq!(
                counts(choices, "platform_count"),
                if id == 40 {
                    vec![]
                } else {
                    vec![(10, 1), (20, 1), (30, 1)]
                }
            );
            assert!(!format!("{:?}", row.row.into_map()).contains("typeChoices"));
        }
        let compact = repository.fetch_all(&query).await.unwrap();
        for row in compact {
            let id = row.get("id").and_then(Value::try_u64).unwrap();
            let target = row.loaded_relation("platform").unwrap();
            assert_eq!(target.len(), 1);
            if id == 40 {
                // The filter hides details, not SchoolType's real platform FK.
                assert_eq!(target[0].len(), 1);
                assert!(target[0].contains_key("id"));
                assert!(!target[0].contains_key("name"));
            }
            assert_eq!(
                counts(target.facet("typeChoices").unwrap(), "platform_count"),
                if id == 40 {
                    vec![]
                } else {
                    vec![(10, 1), (20, 1), (30, 1)]
                }
            );
            assert!(
                !teaql_core::compact_row_to_json_value(&row)
                    .to_string()
                    .contains("typeChoices")
            );
        }
    });
}

#[test]
fn loaded_to_one_null_and_missing_keys_retain_empty_facets_without_orphan_counts() {
    futures_executor::block_on(async {
        for missing_key in [false, true] {
            for include_all in [false, true] {
                for logging in [false, true] {
                    let mut runtime = loaded_setup().await;
                    runtime
                        .0
                        .execute_in_transaction::<Executor, _, _>(|transaction| {
                            Box::pin(async move {
                                transaction
                                    .mutate(
                                        MutationCommand::Insert(
                                            InsertCommand::new("SchoolType")
                                                .value("id", 50_i64)
                                                .value("version", 1_i64)
                                                .value("name", "unassigned")
                                                .value("platform_id", Value::Null),
                                        )
                                        .request("seed nullable relation owner")?,
                                    )
                                    .await?;
                                Ok(())
                            })
                        })
                        .await
                        .unwrap();
                    runtime.0.clear_sql_logs();
                    if !logging {
                        runtime.0.disable_sql_log();
                    }
                    let mut choices = QuerySelection::new(SelectQuery::new("SchoolType").limit(10));
                    choices
                        .query_options
                        .relation_aggregates
                        .push(RelationAggregate::new(
                            "platform",
                            "platform_count",
                            SelectQuery::new("Platform"),
                            true,
                        ));
                    let mut target = QuerySelection::new(SelectQuery::new("Platform"));
                    target.query_options.facets.push(FacetRequest::new(
                        "typeChoices",
                        "school_types",
                        choices,
                        include_all,
                    ));
                    let mut root = SelectQuery::new("SchoolType")
                        .projects(["id", "version", "name", "platform_id"])
                        .limit(10);
                    if missing_key {
                        // Native SQLite DTO projection intentionally omits the local key.
                        // No fabricated rows or traces are passed to the runtime.
                        root = root
                            .raw_sql("SELECT id, version, name FROM facet_school_type ORDER BY id");
                    }
                    let mut source = QuerySelection::new(root);
                    // apply_runtime_metadata owns the raw SQL option, matching the builder boundary.
                    source.query_options.raw_sql = source.query.raw_sql.clone();
                    source
                        .relation_selections
                        .push(teaql_core::request::RelationSelection::new(
                            "platform", target,
                        ));
                    let query = PurposedSelectQuery::new(
                        source
                            .into_query()
                            .comment("load nullable relation choices"),
                        "preserve empty relation Facets",
                    );
                    let rows = runtime
                        .0
                        .entity_data_service::<Executor>("SchoolType")
                        .unwrap()
                        .fetch_enhanced_entities::<SchoolTypeView>(&query)
                        .await
                        .unwrap();
                    assert_eq!(rows.len(), 5);
                    for row in rows.data {
                        let id = row.row.get("id").and_then(Value::try_u64).unwrap();
                        let empty = missing_key || id == 50;
                        let choices = row
                            .state
                            .relation_facet("SchoolType", id, "platform", "typeChoices")
                            .unwrap();
                        let ids: Vec<i64> = if include_all {
                            vec![10, 20, 30, 40, 50]
                        } else if empty {
                            vec![]
                        } else if id == 40 {
                            vec![40]
                        } else {
                            vec![10, 20, 30]
                        };
                        let expected = ids
                            .into_iter()
                            .map(|choice| {
                                (
                                    choice,
                                    i64::from(
                                        !empty && choice != 50 && (choice == 40) == (id == 40),
                                    ),
                                )
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(counts(choices, "platform_count"), expected);
                    }
                    assert!(
                        runtime.0.sql_logs().len() <= 12,
                        "bounded Facet work per selected owner"
                    );
                    if !logging {
                        assert!(runtime.0.sql_logs().is_empty());
                    }
                }
            }
        }
    });
}
