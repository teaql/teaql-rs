use super::*;
use teaql_core::aggregation_plan::StatisticalPlan;
use teaql_core::{PropertyDescriptor as P, RelationDescriptor as R, SelectQuery};

fn descriptors() -> Vec<EntityDescriptor> {
    vec![
        EntityDescriptor::new("School")
            .table_name("legacy_school")
            .property(P::new("id", DataType::I64).id())
            .property(P::new("capacity", DataType::I64).column_name("Legacy Capacity"))
            .property(P::new("kind_id", DataType::I64).column_name("Legacy Kind Key"))
            .relation(R::new("kind", "Kind").local_key("kind_id")),
        EntityDescriptor::new("Kind")
            .table_name("legacy_kind")
            .property(P::new("id", DataType::I64).id())
            .property(P::new("label", DataType::Text).column_name("Legacy Kind Label"))
            .property(P::new("ordinal", DataType::I64))
            .property(P::new("platform_id", DataType::I64))
            .relation(R::new("platform", "Platform").local_key("platform_id")),
        EntityDescriptor::new("Platform")
            .table_name("legacy_platform")
            .property(P::new("id", DataType::I64).id())
            .property(P::new("name", DataType::Text)),
    ]
}

fn executor() -> SqliteMutationExecutor {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch(
        "CREATE TABLE legacy_school(id INTEGER PRIMARY KEY, \"Legacy Capacity\" INTEGER, \"Legacy Kind Key\" INTEGER);
         CREATE TABLE legacy_kind(id INTEGER PRIMARY KEY, \"Legacy Kind Label\" TEXT, ordinal INTEGER, platform_id INTEGER);
         CREATE TABLE legacy_platform(id INTEGER PRIMARY KEY, name TEXT);
         INSERT INTO legacy_platform VALUES(1,'Campus');
         INSERT INTO legacy_kind VALUES(1001,'shared',1,1),(1002,'shared',2,1);
         INSERT INTO legacy_school VALUES(1,800,1001),(2,800,1001),(3,800,1001),(4,800,1001),(5,100,1002);"
    ).unwrap();
    SqliteMutationExecutor::from_connection(connection)
}

fn execute(executor: &SqliteMutationExecutor, query: SelectQuery) -> Vec<CompactRow> {
    let descriptors = descriptors();
    let plan = StatisticalPlan::build(&query, |name| {
        descriptors
            .iter()
            .find(|descriptor| descriptor.name == name)
    })
    .unwrap();
    let compiled = teaql_sql::compile_statistical_plan(&SqliteDialect, &plan).unwrap();
    futures_executor::block_on(executor.fetch_all_compact_sql(&compiled)).unwrap()
}

fn source(parent: SelectQuery) -> SelectQuery {
    SelectQuery::new("School")
        .aggregate(Aggregate::count("schools"))
        .aggregate(Aggregate::sum("capacity", "total"))
        .aggregate(Aggregate::avg("capacity", "mean"))
        .group_by("kind_id")
        .object_group_by("kind", "kind_id", parent)
        .comment("Count school observations at the final parent dimension")
        .purpose("Validate metadata-bound roll-up SQL against independent totals")
}

#[test]
fn layered_rollup_grouped_parent_merges_source_counts_and_weighted_average() {
    let rows = execute(
        &executor(),
        source(SelectQuery::new("Kind").group_by("label")),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("__teaql_d0"), Some(&Value::from("shared")));
    assert_eq!(rows[0].get("__teaql_m0"), Some(&Value::I64(5)));
    assert_eq!(rows[0].get("__teaql_m1"), Some(&Value::I64(3300)));
    // Mean of the two per-kind averages is 450, not the correct 660.
    assert_eq!(rows[0].get("__teaql_m2"), Some(&Value::F64(660.0)));
}

#[test]
fn layered_rollup_parent_filter_restricts_source_population() {
    let rows = execute(
        &executor(),
        source(
            SelectQuery::new("Kind")
                .group_by("label")
                .and_filter(Expr::eq("ordinal", 1)),
        ),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("__teaql_m0"), Some(&Value::I64(4)));
    assert_eq!(rows[0].get("__teaql_m1"), Some(&Value::I64(3200)));
}

#[test]
fn layered_rollup_multihop_deduplicates_parent_measure_at_its_owner_identity() {
    let parent = SelectQuery::new("Kind")
        .aggregate(Aggregate::count("kinds"))
        .aggregate(Aggregate::sum("ordinal", "ordinalTotal"))
        .group_by("platform_id")
        .object_group_by(
            "platform",
            "platform_id",
            SelectQuery::new("Platform").group_by("name"),
        );
    let rows = execute(&executor(), source(parent));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("__teaql_d0"), Some(&Value::from("Campus")));
    assert_eq!(rows[0].get("__teaql_m0"), Some(&Value::I64(5)));
    assert_eq!(rows[0].get("__teaql_m1"), Some(&Value::I64(3300)));
    assert_eq!(rows[0].get("__teaql_m3"), Some(&Value::I64(2)));
    // Repeating kind 1001 once per school incorrectly produces 6.
    assert_eq!(rows[0].get("__teaql_m4"), Some(&Value::I64(3)));
}

#[test]
fn layered_rollup_contradictory_layer_filters_return_no_group() {
    let query = source(
        SelectQuery::new("Kind")
            .group_by("label")
            .and_filter(Expr::eq("ordinal", 1)),
    )
    .and_filter(Expr::lt("capacity", 500));
    assert!(execute(&executor(), query).is_empty());
}

#[test]
fn layered_rollup_empty_scalar_population_keeps_count_zero_sum_and_average_null() {
    let query = source(SelectQuery::new("Kind").aggregate(Aggregate::count("kinds")))
        .and_filter(Expr::lt("capacity", 0));
    let rows = execute(&executor(), query);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("__teaql_m0"), Some(&Value::I64(0)));
    assert_eq!(rows[0].get("__teaql_m1"), Some(&Value::Null));
    assert_eq!(rows[0].get("__teaql_m2"), Some(&Value::Null));
    assert_eq!(rows[0].get("__teaql_m3"), Some(&Value::I64(0)));
}

#[test]
fn layered_rollup_null_dimension_joins_its_measure_cell() {
    let executor = executor();
    executor
        .connection()
        .lock()
        .unwrap()
        .execute("UPDATE legacy_kind SET \"Legacy Kind Label\" = NULL", [])
        .unwrap();
    let rows = execute(
        &executor,
        source(SelectQuery::new("Kind").group_by("label")),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("__teaql_d0"), Some(&Value::Null));
    assert_eq!(rows[0].get("__teaql_m0"), Some(&Value::I64(5)));
}
