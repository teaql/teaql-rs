use super::*;
use teaql_core::aggregation_plan::StatisticalPlan;
use teaql_core::{BinaryOp, PropertyDescriptor as P, RelationDescriptor as R, SelectQuery};

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
fn layered_rollup_distinct_counts_use_final_population_not_intermediate_totals() {
    let transport = executor();
    transport
        .connection()
        .lock()
        .unwrap()
        .execute_batch("INSERT INTO legacy_school VALUES(6,800,1002),(7,NULL,1002);")
        .unwrap();
    let context = runtime_context(transport, policy(false, false));
    let parent = SelectQuery::new("Kind")
        .group_by("label")
        .count_distinct("label", "labels")
        .having(Expr::eq("labels", 1));
    let query = source(parent)
        .count_distinct("capacity", "capacities")
        .having(Expr::eq("capacities", 2))
        .order_by(teaql_core::OrderBy::desc("capacities"));
    for _ in 0..2 {
        let result = fetch(&context, query.clone()).unwrap();
        assert_eq!(result.len(), 1);
        let row = &result[0];
        assert_eq!(row.get("schools").and_then(Value::try_i64), Some(7));
        assert_eq!(row.get("capacities").and_then(Value::try_i64), Some(2));
        let Some(Value::Object(parent)) = row.get("kind") else {
            panic!("missing parent measures")
        };
        assert_eq!(parent.get("labels").and_then(Value::try_i64), Some(1));
    }
}

#[test]
fn layered_rollup_distinct_count_empty_null_and_ordinary_having() {
    let transport = executor();
    transport
        .connection()
        .lock()
        .unwrap()
        .execute_batch("INSERT INTO legacy_school VALUES(7,NULL,1002);")
        .unwrap();
    let context = runtime_context(transport, policy(false, false));
    for (id, expected_parent) in [(7, 1), (999, 0)] {
        let parent = SelectQuery::new("Kind").count_distinct("label", "labels");
        let query = source(parent)
            .count_distinct("capacity", "capacities")
            .and_filter(Expr::eq("id", id));
        let result = fetch(&context, query).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].get("capacities").and_then(Value::try_i64),
            Some(0)
        );
        let Some(Value::Object(parent)) = result[0].get("kind") else {
            panic!("missing parent measures")
        };
        assert_eq!(
            parent.get("labels").and_then(Value::try_i64),
            Some(expected_parent)
        );
    }
    let ordinary = SelectQuery::new("School")
        .count_distinct("capacity", "capacities")
        .having(Expr::eq("capacities", 2))
        .comment("Count distinct mapped capacities")
        .purpose("Verify ordinary statistical HAVING");
    let result = fetch(&context, ordinary.clone()).unwrap();
    assert_eq!(
        result[0].get("capacities").and_then(Value::try_i64),
        Some(2)
    );
    assert!(
        fetch(&context, ordinary.having(Expr::gt("capacities", 2)))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn layered_rollup_function_dimension_merges_distinct_source_values() {
    let transport = executor();
    transport.connection().lock().unwrap().execute_batch(
        "UPDATE legacy_kind SET \"Legacy Kind Label\"=CASE id WHEN 1001 THEN 'Robert' ELSE 'Rupert' END;"
    ).unwrap();
    let context = runtime_context(transport, policy(false, false));
    let parent = SelectQuery::new("Kind")
        .group_by("phonetic")
        .project_expr("phonetic", Expr::soundex(Expr::column("label")))
        .count("kinds")
        .sum("ordinal", "ordinalTotal")
        .having(Expr::eq("phonetic", "R163"))
        .order_asc("phonetic")
        .limit(1);
    for _ in 0..2 {
        let rows = fetch(&context, source(parent.clone())).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("schools").and_then(Value::try_i64), Some(5));
        assert_eq!(rows[0].get("total").and_then(Value::try_i64), Some(3300));
        let Some(Value::Object(kind)) = rows[0].get("kind") else {
            panic!("missing parent")
        };
        assert_eq!(kind.get("phonetic"), Some(&Value::from("R163")));
        assert_eq!(kind.get("kinds").and_then(Value::try_i64), Some(2));
        assert_eq!(kind.get("ordinalTotal").and_then(Value::try_i64), Some(3));
        assert!(!kind.contains_key("label"));
    }
}

#[test]
fn layered_rollup_multihop_function_dimension_keeps_owner_grains() {
    let transport = executor();
    transport
        .connection()
        .lock()
        .unwrap()
        .execute_batch(
            "UPDATE legacy_platform SET name='Robert' WHERE id=1;
         INSERT INTO legacy_platform VALUES(2,'Rupert');
         UPDATE legacy_kind SET platform_id=2 WHERE id=1002;",
        )
        .unwrap();
    let context = runtime_context(transport, policy(false, false));
    let platform = SelectQuery::new("Platform")
        .group_by("phonetic")
        .project_expr("phonetic", Expr::soundex(Expr::column("name")))
        .count("platforms");
    let parent = SelectQuery::new("Kind")
        .count("kinds")
        .sum("ordinal", "ordinalTotal")
        .group_by("platform_id")
        .object_group_by("platform", "platform_id", platform);
    let rows = fetch(&context, source(parent)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools").and_then(Value::try_i64), Some(5));
    let Some(Value::Object(kind)) = rows[0].get("kind") else {
        panic!("missing parent")
    };
    assert_eq!(kind.get("kinds").and_then(Value::try_i64), Some(2));
    assert_eq!(kind.get("ordinalTotal").and_then(Value::try_i64), Some(3));
    let Some(Value::Object(platform)) = kind.get("platform") else {
        panic!("missing grandparent")
    };
    assert_eq!(platform.get("phonetic"), Some(&Value::from("R163")));
    assert_eq!(platform.get("platforms").and_then(Value::try_i64), Some(2));
}

#[test]
fn ordinary_function_grouping_uses_the_transformed_key_and_having() {
    let transport = executor();
    transport.connection().lock().unwrap().execute_batch(
        "UPDATE legacy_kind SET \"Legacy Kind Label\"=CASE id WHEN 1001 THEN 'Robert' ELSE 'Rupert' END;"
    ).unwrap();
    let context = runtime_context(transport, policy(false, false));
    let request = SelectQuery::new("Kind")
        .group_by_expr("phonetic", Expr::soundex(Expr::column("label")))
        .count("kinds")
        .having(Expr::eq("phonetic", "R163"))
        .order_asc("phonetic")
        .limit(1)
        .comment("Group transformed parent labels without a roll-up");
    let rows = fetch(&context, request).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("phonetic"), Some(&Value::from("R163")));
    assert_eq!(rows[0].get("kinds").and_then(Value::try_i64), Some(2));
    assert!(!rows[0].contains_key("label"));
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

fn rollup_object<'a>(
    row: &'a CompactRow,
    key: &str,
) -> &'a std::collections::BTreeMap<String, Value> {
    let Some(Value::Object(object)) = row.get(key) else {
        panic!("missing {key}");
    };
    object
}

#[test]
fn missing_parent_references_are_not_an_existing_null_dimension() {
    let transport = executor();
    transport
        .connection()
        .lock()
        .unwrap()
        .execute_batch(
            "INSERT INTO legacy_school VALUES(6,50,NULL),(7,700,9999);
         UPDATE legacy_kind SET \"Legacy Kind Label\"=NULL WHERE id=1001;",
        )
        .unwrap();
    let oracle: (i64,i64) = transport.connection().lock().unwrap().query_row(
        "SELECT COUNT(*),SUM(s.\"Legacy Capacity\") FROM legacy_school s JOIN legacy_kind k ON k.id=s.\"Legacy Kind Key\"", [],
        |row| Ok((row.get(0)?,row.get(1)?))
    ).unwrap();
    assert_eq!(oracle, (5, 3300));
    let context = runtime_context(transport, policy(false, false));
    let raw = SelectQuery::new("School")
        .group_by("kind_id")
        .count("schools")
        .sum("capacity", "total")
        .comment("Retain null and orphan identities when grouping the raw foreign key");
    let raw_rows = fetch(&context, raw).unwrap();
    assert_eq!(raw_rows.len(), 4);
    assert_eq!(
        raw_rows
            .iter()
            .map(|row| row.get("schools").and_then(Value::try_i64).unwrap())
            .sum::<i64>(),
        7
    );
    assert!(
        raw_rows
            .iter()
            .any(|row| row.get("kind_id") == Some(&Value::Null)
                && row.get("total").and_then(Value::try_i64) == Some(50))
    );
    assert!(
        raw_rows
            .iter()
            .any(|row| row.get("kind_id").and_then(Value::try_i64) == Some(9999))
    );
    let parent = SelectQuery::new("Kind")
        .group_by("label")
        .count("types")
        .sum("ordinal", "ordinalTotal");
    let rows = fetch(&context, source(parent)).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| row.get("schools").and_then(Value::try_i64).unwrap())
            .sum::<i64>(),
        oracle.0
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.get("total").and_then(Value::try_i64).unwrap())
            .sum::<i64>(),
        oracle.1
    );
    let cell = rows
        .iter()
        .find(|row| rollup_object(row, "kind").get("label") == Some(&Value::Null))
        .unwrap();
    assert_eq!(cell.get("schools").and_then(Value::try_i64), Some(4));
    assert_eq!(cell.get("total").and_then(Value::try_i64), Some(3200));
    assert_eq!(
        rollup_object(cell, "kind")
            .get("types")
            .and_then(Value::try_i64),
        Some(1)
    );
}

#[test]
fn every_rollup_hop_requires_an_existing_parent_not_a_non_null_label() {
    let transport = executor();
    transport
        .connection()
        .lock()
        .unwrap()
        .execute_batch(
            "INSERT INTO legacy_kind VALUES(1003,'no root',3,NULL),(1004,'orphan root',4,9999);
         INSERT INTO legacy_school VALUES(6,50,1003),(7,700,1004);
         UPDATE legacy_platform SET name=NULL;",
        )
        .unwrap();
    let context = runtime_context(transport, policy(false, false));
    let one = fetch(&context, source(SelectQuery::new("Kind").group_by("label"))).unwrap();
    assert_eq!(one.len(), 3);
    assert_eq!(
        one.iter()
            .map(|row| row.get("schools").and_then(Value::try_i64).unwrap())
            .sum::<i64>(),
        7
    );
    assert_eq!(
        one.iter()
            .map(|row| row.get("total").and_then(Value::try_i64).unwrap())
            .sum::<i64>(),
        4050
    );
    let platform = SelectQuery::new("Platform")
        .group_by("name")
        .count("platforms");
    let parent = SelectQuery::new("Kind")
        .count("types")
        .sum("ordinal", "ordinalTotal")
        .group_by("platform_id")
        .object_group_by("platform", "platform_id", platform);
    let rows = fetch(&context, source(parent)).unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(5));
    assert_eq!(row.get("total").and_then(Value::try_i64), Some(3300));
    let kind = rollup_object(row, "kind");
    assert_eq!(kind.get("types").and_then(Value::try_i64), Some(2));
    assert_eq!(kind.get("ordinalTotal").and_then(Value::try_i64), Some(3));
    let Some(Value::Object(platform)) = kind.get("platform") else {
        panic!("missing platform");
    };
    assert_eq!(platform.get("name"), Some(&Value::Null));
    assert_eq!(platform.get("platforms").and_then(Value::try_i64), Some(1));
}

#[test]
fn filtered_and_missing_parents_keep_empty_group_and_scalar_semantics_distinct() {
    let transport = executor();
    transport
        .connection()
        .lock()
        .unwrap()
        .execute_batch(
            "INSERT INTO legacy_school VALUES(6,50,NULL),(7,700,9999);
         UPDATE legacy_kind SET \"Legacy Kind Label\"=NULL WHERE id=1002;",
        )
        .unwrap();
    let context = runtime_context(transport, policy(false, false));
    let parent = SelectQuery::new("Kind")
        .group_by("label")
        .count("types")
        .filter(Expr::eq("ordinal", 2));
    let rows = fetch(&context, source(parent)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools").and_then(Value::try_i64), Some(1));
    assert_eq!(rows[0].get("total").and_then(Value::try_i64), Some(100));
    assert_eq!(
        rollup_object(&rows[0], "kind").get("label"),
        Some(&Value::Null)
    );
    assert!(
        fetch(
            &context,
            source(SelectQuery::new("Kind").group_by("label")).and_filter(Expr::gte("id", 6))
        )
        .unwrap()
        .is_empty()
    );
    let scalar = SelectQuery::new("Kind")
        .count("types")
        .sum("ordinal", "ordinalTotal");
    let rows = fetch(&context, source(scalar).and_filter(Expr::gte("id", 6))).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools").and_then(Value::try_i64), Some(0));
    assert_eq!(rows[0].get("total"), Some(&Value::Null));
    assert_eq!(rows[0].get("mean"), Some(&Value::Null));
    let kind = rollup_object(&rows[0], "kind");
    assert_eq!(kind.get("types").and_then(Value::try_i64), Some(0));
    assert_eq!(kind.get("ordinalTotal"), Some(&Value::Null));
}

type StatisticalExecutor = teaql_sql::SqlDataServiceExecutor<
    SqliteDialect,
    SqliteMutationExecutor,
    teaql_runtime::InMemoryMetadataStore,
>;

#[derive(Clone)]
struct LayerPolicy {
    restrict_kind: bool,
    deny_platform: bool,
    seen: Arc<Mutex<Vec<(String, String)>>>,
}

impl teaql_runtime::RequestPolicy for LayerPolicy {
    fn enforce_select(
        &self,
        _context: &UserContext,
        query: &mut SelectQuery,
    ) -> Result<(), RuntimeError> {
        self.seen.lock().unwrap().push((
            query.entity.clone(),
            query.request_scope().unwrap().comment().to_owned(),
        ));
        if query.entity == "Kind" && self.restrict_kind {
            query.filter = Some(match query.filter.take() {
                Some(filter) => filter.and_expr(Expr::eq("ordinal", 1)),
                None => Expr::eq("ordinal", 1),
            });
        }
        if query.entity == "Platform" && self.deny_platform {
            return Err(RuntimeError::Policy("STATISTICS_PLATFORM_DENIED".into()));
        }
        Ok(())
    }
}

fn runtime_context(transport: SqliteMutationExecutor, policy: LayerPolicy) -> UserContext {
    let metadata = descriptors().into_iter().fold(
        teaql_runtime::InMemoryMetadataStore::new(),
        |metadata, descriptor| metadata.with_entity(descriptor),
    );
    let service =
        teaql_sql::SqlDataServiceExecutor::new(SqliteDialect, transport, metadata.clone());
    let mut context = UserContext::new()
        .with_metadata(metadata)
        .with_request_policy(policy);
    context.register_executor(service);
    context
}

fn policy(restrict_kind: bool, deny_platform: bool) -> LayerPolicy {
    LayerPolicy {
        restrict_kind,
        deny_platform,
        seen: Arc::new(Mutex::new(Vec::new())),
    }
}

fn fetch(context: &UserContext, query: SelectQuery) -> Result<Vec<CompactRow>, String> {
    let request = teaql_runtime::PurposedSelectQuery::new(
        query,
        "Validate Context-scoped layered statistics",
    );
    let service = context
        .entity_data_service::<StatisticalExecutor>("School")
        .unwrap();
    futures_executor::block_on(service.fetch_all(&request)).map_err(|error| error.to_string())
}

fn extended(query: SelectQuery, field: &str) -> SelectQuery {
    query
        .stddev(field, "stddev")
        .stddev_pop(field, "stddevPop")
        .var_samp(field, "variance")
        .var_pop(field, "variancePop")
        .bit_and(field, "bitAnd")
        .bit_or(field, "bitOr")
        .bit_xor(field, "bitXor")
}

#[test]
fn layered_rollup_extended_functions_preserve_source_and_parent_owner_grain() {
    let context = runtime_context(executor(), policy(false, false));
    let parent = extended(
        SelectQuery::new("Kind").group_by("label").count("kinds"),
        "ordinal",
    );
    let rows = fetch(&context, extended(source(parent), "capacity")).unwrap();
    assert_eq!(rows.len(), 1);
    let root = &rows[0];
    let Some(Value::Object(kind)) = root.get("kind") else {
        panic!("missing kind")
    };
    for (name, expected) in [
        ("variance", 98000.0),
        ("variancePop", 78400.0),
        ("stddev", 98000_f64.sqrt()),
        ("stddevPop", 280.0),
    ] {
        assert!(
            (root.get(name).and_then(Value::try_f64).unwrap() - expected).abs() < 1e-8,
            "{name}"
        );
    }
    for (name, expected) in [
        ("variance", 0.5),
        ("variancePop", 0.25),
        ("stddev", 0.5_f64.sqrt()),
        ("stddevPop", 0.5),
    ] {
        assert!(
            (kind.get(name).and_then(Value::try_f64).unwrap() - expected).abs() < 1e-12,
            "parent {name}"
        );
    }
    for (name, expected) in [("bitAnd", 32), ("bitOr", 868), ("bitXor", 100)] {
        assert_eq!(root.get(name).and_then(Value::try_i64), Some(expected));
    }
    for (name, expected) in [("bitAnd", 0), ("bitOr", 3), ("bitXor", 3)] {
        assert_eq!(kind.get(name).and_then(Value::try_i64), Some(expected));
    }
    assert_eq!(kind.get("kinds").and_then(Value::try_i64), Some(2));
}

#[test]
fn layered_rollup_extended_empty_population_keeps_null_not_identity_values() {
    let context = runtime_context(executor(), policy(false, false));
    let parent = extended(SelectQuery::new("Kind").count("kinds"), "ordinal");
    let rows = fetch(
        &context,
        extended(source(parent), "capacity").and_filter(Expr::lt("id", 0_i64)),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(Value::Object(kind)) = rows[0].get("kind") else {
        panic!("missing kind")
    };
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(0)));
    assert_eq!(kind.get("kinds"), Some(&Value::I64(0)));
    for name in [
        "variance",
        "variancePop",
        "stddev",
        "stddevPop",
        "bitAnd",
        "bitOr",
        "bitXor",
    ] {
        assert_eq!(rows[0].get(name), Some(&Value::Null));
        assert_eq!(kind.get(name), Some(&Value::Null));
    }
}

#[test]
fn layered_rollup_all_null_source_and_singleton_parent_have_distinct_variance_semantics() {
    let transport = executor();
    transport
        .connection()
        .lock()
        .unwrap()
        .execute(
            "UPDATE legacy_school SET \"Legacy Capacity\"=NULL WHERE id=5",
            [],
        )
        .unwrap();
    let context = runtime_context(transport, policy(false, false));
    let parent = extended(
        SelectQuery::new("Kind").group_by("label").count("kinds"),
        "ordinal",
    );
    let rows = fetch(
        &context,
        extended(source(parent), "capacity").and_filter(Expr::eq("id", 5_i64)),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    let Some(Value::Object(kind)) = rows[0].get("kind") else {
        panic!("missing kind")
    };
    for name in [
        "variance",
        "variancePop",
        "stddev",
        "stddevPop",
        "bitAnd",
        "bitOr",
        "bitXor",
    ] {
        assert_eq!(rows[0].get(name), Some(&Value::Null));
    }
    assert_eq!(kind.get("variance"), Some(&Value::Null));
    assert_eq!(kind.get("stddev"), Some(&Value::Null));
    assert_eq!(kind.get("variancePop").and_then(Value::try_f64), Some(0.0));
    assert_eq!(kind.get("stddevPop").and_then(Value::try_f64), Some(0.0));
}

#[test]
fn layered_rollup_context_applies_each_population_policy_and_restores_logical_paths() {
    let policy = policy(true, false);
    let seen = policy.seen.clone();
    let context = runtime_context(executor(), policy);
    let parent = SelectQuery::new("Kind")
        .group_by("label")
        .comment("Group eligible school types");
    let rows = fetch(
        &context,
        source(parent).limit(10).having(Expr::gt("schools", 2)),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(4)));
    assert_eq!(rows[0].get("total"), Some(&Value::I64(3200)));
    let Some(Value::Object(kind)) = rows[0].get("kind") else {
        panic!("missing typed dimension object");
    };
    assert_eq!(kind.get("label"), Some(&Value::from("shared")));
    assert!(!rows[0].contains_key("kind_id"));
    assert!(!rows[0].contains_key("__teaql_m0"));
    assert_eq!(
        seen.lock()
            .unwrap()
            .iter()
            .map(|(entity, _)| entity.as_str())
            .collect::<Vec<_>>(),
        ["School", "Kind"]
    );
    assert_eq!(seen.lock().unwrap()[1].1, "Group eligible school types");
}

#[test]
fn layered_rollup_context_orders_and_pages_final_cells_not_source_rows() {
    let context = runtime_context(executor(), policy(false, false));
    let query = source(SelectQuery::new("Kind").group_by("ordinal"))
        .order_desc("schools")
        .limit(1)
        .offset(1);
    let rows = fetch(&context, query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(1)));
    assert_eq!(rows[0].get("total"), Some(&Value::I64(100)));
    let Some(Value::Object(kind)) = rows[0].get("kind") else {
        panic!("missing dimension");
    };
    assert_eq!(kind.get("ordinal"), Some(&Value::I64(2)));
}

#[test]
fn layered_rollup_context_having_removes_final_cells() {
    let context = runtime_context(executor(), policy(false, false));
    let rows = fetch(
        &context,
        source(SelectQuery::new("Kind").group_by("label")).having(Expr::gt("schools", 5)),
    )
    .unwrap();
    assert!(rows.is_empty());
}

#[test]
fn ordinary_statistics_having_count_alias_filters_scalar_result() {
    let context = runtime_context(executor(), policy(false, false));
    let query = SelectQuery::new("School")
        .count("schools")
        .having(Expr::gt("schools", 4))
        .comment("Filter the selected school count")
        .purpose("Prove result aliases work without a roll-up");
    let rows = fetch(&context, query.clone()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(5)));
    assert!(
        fetch(&context, query.having(Expr::gt("schools", 5)))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn ordinary_statistics_having_alias_composes_with_source_filter_and_aggregate_expression() {
    let context = runtime_context(executor(), policy(false, false));
    let query = SelectQuery::new("School")
        .group_by("kind_id")
        .aggregate(Aggregate::sum("capacity", "total"))
        .filter(Expr::gte("capacity", 100))
        .having(Expr::and([
            Expr::gt("total", 3000),
            Expr::binary(Expr::count_all(), BinaryOp::Gt, Expr::value(2)),
        ]))
        .comment("Filter completed kind totals using aliases and explicit aggregates")
        .purpose("Keep HAVING after population filtering and grouping");
    let rows = fetch(&context, query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("kind_id"), Some(&Value::I64(1001)));
    assert_eq!(rows[0].get("total"), Some(&Value::I64(3200)));
}

#[test]
fn ordinary_statistics_having_named_dimension_uses_legacy_column_binding() {
    let context = runtime_context(executor(), policy(false, false));
    let query = SelectQuery::new("School")
        .group_by("capacity")
        .project_expr("bucket", Expr::column("capacity"))
        .count("schools")
        .having(Expr::gte("bucket", 800))
        .comment("Select the named capacity dimension")
        .purpose("Resolve dimension aliases through logical metadata");
    let rows = fetch(&context, query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("bucket"), Some(&Value::I64(800)));
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(4)));
}

#[test]
fn layered_rollup_named_source_dimension_supports_having_and_preserves_owner_grain() {
    let context = runtime_context(executor(), policy(false, false));
    let query = source(SelectQuery::new("Kind").group_by("label").count("kinds"))
        .group_by("capacity")
        .project_expr("bucket", Expr::column("capacity"))
        .having(Expr::gte("bucket", 800))
        .order_desc("bucket");
    let rows = fetch(&context, query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("bucket"), Some(&Value::I64(800)));
    assert_eq!(rows[0].get("capacity"), Some(&Value::I64(800)));
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(4)));
    assert_eq!(rows[0].get("total"), Some(&Value::I64(3200)));
    let Some(Value::Object(kind)) = rows[0].get("kind") else {
        panic!("missing kind");
    };
    assert_eq!(kind.get("kinds"), Some(&Value::I64(1)));
}

#[test]
fn layered_rollup_named_parent_dimension_supports_cell_paging() {
    let context = runtime_context(executor(), policy(false, false));
    let parent = SelectQuery::new("Kind")
        .group_by("ordinal")
        .project_expr("rank", Expr::column("ordinal"))
        .order_desc("rank")
        .limit(1);
    let rows = fetch(&context, source(parent)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(1)));
    let Some(Value::Object(kind)) = rows[0].get("kind") else {
        panic!("missing kind");
    };
    assert_eq!(kind.get("rank"), Some(&Value::I64(2)));
}

#[test]
fn layered_rollup_conflicting_alias_fails_before_sql() {
    let transport = executor();
    transport
        .connection()
        .lock()
        .unwrap()
        .execute("DROP TABLE legacy_school", [])
        .unwrap();
    let context = runtime_context(transport, policy(false, false));
    let query = source(SelectQuery::new("Kind").group_by("label"))
        .group_by("capacity")
        .project_expr("kind.label", Expr::column("capacity"));
    let error = fetch(&context, query).unwrap_err();
    assert!(error.contains("statistical result path"), "{error}");
    assert!(!error.contains("no such table"), "{error}");
}

#[test]
fn layered_rollup_context_parent_bindings_do_not_reuse_previous_values() {
    let context = runtime_context(executor(), policy(false, false));
    for (ordinal, count) in [(1, 4), (2, 1), (1, 4)] {
        let rows = fetch(
            &context,
            source(
                SelectQuery::new("Kind")
                    .group_by("label")
                    .and_filter(Expr::eq("ordinal", ordinal)),
            ),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("schools"), Some(&Value::I64(count)));
    }
}

#[test]
fn layered_rollup_context_denied_parent_stops_before_sql() {
    let transport = executor();
    transport
        .connection()
        .lock()
        .unwrap()
        .execute("DROP TABLE legacy_school", [])
        .unwrap();
    let policy = policy(false, true);
    let seen = policy.seen.clone();
    let context = runtime_context(transport, policy);
    let parent = SelectQuery::new("Kind")
        .group_by("platform_id")
        .object_group_by(
            "platform",
            "platform_id",
            SelectQuery::new("Platform").group_by("name"),
        );
    let error = fetch(&context, source(parent)).unwrap_err();
    assert!(error.contains("STATISTICS_PLATFORM_DENIED"), "{error}");
    assert!(
        !error.contains("no such table"),
        "SQL must not run before all population policies"
    );
    assert_eq!(seen.lock().unwrap().len(), 3);
}

#[test]
fn layered_rollup_context_multihop_preserves_parent_measure_grain() {
    let context = runtime_context(executor(), policy(false, false));
    let parent = SelectQuery::new("Kind")
        .aggregate(Aggregate::sum("ordinal", "ordinalTotal"))
        .group_by("platform_id")
        .object_group_by(
            "platform",
            "platform_id",
            SelectQuery::new("Platform").group_by("name"),
        );
    let rows = fetch(&context, source(parent)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(5)));
    let Some(Value::Object(kind)) = rows[0].get("kind") else {
        panic!("missing kind result");
    };
    assert_eq!(kind.get("ordinalTotal"), Some(&Value::I64(3)));
    let Some(Value::Object(platform)) = kind.get("platform") else {
        panic!("missing platform dimension");
    };
    assert_eq!(platform.get("name"), Some(&Value::from("Campus")));
}

fn direct_executor() -> StatisticalExecutor {
    let metadata = descriptors().into_iter().fold(
        teaql_runtime::InMemoryMetadataStore::new(),
        |metadata, descriptor| metadata.with_entity(descriptor),
    );
    teaql_sql::SqlDataServiceExecutor::new(SqliteDialect, executor(), metadata)
}

fn query_request(query: SelectQuery, capture: bool) -> teaql_data_service::QueryRequest {
    teaql_data_service::QueryRequest {
        intent: teaql_core::QueryIntent::new(
            query.comment.clone().unwrap(),
            query.purpose.clone().unwrap(),
        )
        .unwrap(),
        query,
        trace_chain: Vec::new(),
        capture_execution_metadata: capture,
        capture_debug_query: false,
    }
}

#[test]
fn layered_rollup_native_stream_restores_paths_for_every_chunk() {
    use futures_util::StreamExt;
    let executor = direct_executor();
    let query = source(SelectQuery::new("Kind").group_by("ordinal")).order_desc("schools");
    let mut stream = teaql_data_service::StreamQueryExecutor::query_stream(
        &executor,
        query_request(query, false),
        1,
    );
    let rows = futures_executor::block_on(async {
        let mut rows = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            assert_eq!(chunk.rows.len(), 1);
            rows.extend(chunk.rows);
        }
        rows
    });
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(4)));
    assert_eq!(rows[1].get("schools"), Some(&Value::I64(1)));
    assert!(rows.iter().all(|row| {
        row.get("kind")
            .is_some_and(|value| matches!(value, Value::Object(_)))
    }));
}

#[test]
fn layered_rollup_observed_stream_reports_final_logical_rows() {
    use futures_util::StreamExt;
    let executor = direct_executor();
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = events.clone();
    let query = source(SelectQuery::new("Kind").group_by("label")).limit(1);
    let mut stream = teaql_data_service::StreamQueryExecutor::query_stream_observed(
        &executor,
        query_request(query, true),
        1,
        Arc::new(move |metadata| observed.lock().unwrap().push(metadata)),
    );
    futures_executor::block_on(async {
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.rows[0].get("schools"), Some(&Value::I64(5)));
        assert!(stream.next().await.is_none());
    });
    drop(stream);
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].result_count, Some(1));
    assert!(
        events[0]
            .parameterized_query
            .as_ref()
            .unwrap()
            .contains("__teaql_grain0")
    );
}

#[test]
fn layered_rollup_decoder_preserves_dots_in_a_measure_alias() {
    let executor = direct_executor();
    let query = SelectQuery::new("School")
        .aggregate(Aggregate::count("school.count"))
        .group_by("kind_id")
        .object_group_by(
            "kind",
            "kind_id",
            SelectQuery::new("Kind").group_by("label"),
        )
        .comment("Preserve caller supplied alias spelling")
        .purpose("Verify paths do not split alias text")
        .limit(1);
    let result = futures_executor::block_on(teaql_data_service::QueryExecutor::query(
        &executor,
        query_request(query, false),
    ))
    .unwrap();
    assert_eq!(result.rows[0].get("school.count"), Some(&Value::I64(5)));
    assert!(!result.rows[0].contains_key("school"));
}

#[test]
fn layered_rollup_parent_limit_selects_cells_not_first_parent_id() {
    let context = runtime_context(executor(), policy(false, false));
    let rows = fetch(
        &context,
        source(SelectQuery::new("Kind").group_by("label").limit(1)),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(5)));
}

#[test]
fn layered_rollup_parent_page_is_computed_after_source_filters() {
    let context = runtime_context(executor(), policy(false, false));
    let parent = SelectQuery::new("Kind")
        .group_by("ordinal")
        .order_desc("ordinal")
        .limit(1);
    let rows = fetch(&context, source(parent.clone())).unwrap();
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(1)));
    let rows = fetch(
        &context,
        source(parent).and_filter(Expr::gte("capacity", 800)),
    )
    .unwrap();
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(4)));
    let Some(Value::Object(kind)) = rows[0].get("kind") else {
        panic!("missing dimension");
    };
    assert_eq!(kind.get("ordinal"), Some(&Value::I64(1)));
}

#[test]
fn layered_rollup_parent_having_uses_deduplicated_owner_measures() {
    let context = runtime_context(executor(), policy(false, false));
    let parent = SelectQuery::new("Kind")
        .group_by("label")
        .aggregate(Aggregate::count("kinds"))
        .having(Expr::gt("kinds", 1));
    let rows = fetch(&context, source(parent.clone())).unwrap();
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(5)));
    assert!(
        fetch(
            &context,
            source(parent).and_filter(Expr::gte("capacity", 800))
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn layered_rollup_parent_measure_order_does_not_multiply_by_source_children() {
    let context = runtime_context(executor(), policy(false, false));
    let parent = SelectQuery::new("Kind")
        .group_by("ordinal")
        .aggregate(Aggregate::sum("ordinal", "ordinalTotal"))
        .order_desc("ordinalTotal")
        .limit(1);
    let rows = fetch(&context, source(parent)).unwrap();
    // A child-weighted sum incorrectly ranks kind 1001 (4 * 1) over 1002 (1 * 2).
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(1)));
}
