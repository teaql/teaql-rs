//! Real PostgreSQL execution for the small #326 roll-up fixture. The local
//! verifier supplies an isolated, owned database and requires these tests.
use deadpool_postgres::{Pool, Runtime};
use serde_json::json;
use teaql_core::{
    Aggregate, CompactRow, DataType, EntityDescriptor, Expr, PropertyDescriptor as P,
    RelationDescriptor as R, SelectQuery, Value,
};
use teaql_provider_postgres::{PgMutationExecutor, PostgresDialect};
use teaql_runtime::{InMemoryMetadataStore, PurposedSelectQuery, UserContext};
use teaql_sql::SqlDataServiceExecutor;

type Executor = SqlDataServiceExecutor<PostgresDialect, PgMutationExecutor, InMemoryMetadataStore>;

fn descriptors() -> Vec<EntityDescriptor> {
    vec![
        EntityDescriptor::new("School")
            .table_name("legacy_school")
            .property(P::new("id", DataType::I64).id())
            .property(P::new("capacity", DataType::I64).column_name("Legacy Capacity"))
            .property(P::new("kind_id", DataType::I64).column_name("Legacy Kind Key"))
            .relation(R::new("kind", "Kind").local_key("kind_id"))
            .audit_mask_fields(vec!["capacity".into()]),
        EntityDescriptor::new("Kind")
            .table_name("legacy_kind")
            .property(P::new("id", DataType::I64).id())
            .property(P::new("label", DataType::Text).column_name("Legacy Kind Label"))
            .property(P::new("ordinal", DataType::I64))
            .property(P::new("platform_id", DataType::I64))
            .relation(R::new("platform", "Platform").local_key("platform_id"))
            .audit_mask_fields(vec![]),
        EntityDescriptor::new("Platform")
            .table_name("legacy_platform")
            .property(P::new("id", DataType::I64).id())
            .property(P::new("name", DataType::Text))
            .audit_mask_fields(vec![]),
    ]
}

async fn fixture() -> Option<(UserContext, Pool)> {
    let Ok(url) = std::env::var("TEAQL_TEST_POSTGRES_URL") else {
        assert!(
            !std::env::var("TEAQL_REQUIRE_LIVE_DB").is_ok_and(|v| v == "true"),
            "Live PostgreSQL acceptance requires TEAQL_TEST_POSTGRES_URL"
        );
        return None;
    };
    let mut config = deadpool_postgres::Config::new();
    config.url = Some(url);
    let pool = config
        .create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
        .unwrap();
    {
        let client = pool.get().await.unwrap();
        let ownership = client.query_one(
            "SELECT current_database(), shobj_description(oid,'pg_database') FROM pg_database WHERE datname=current_database()",
            &[],
        ).await.unwrap();
        assert_eq!(ownership.get::<_, String>(0), "teaql_rollup_326");
        assert_eq!(
            ownership.get::<_, Option<String>>(1).as_deref(),
            Some("TeaQL #326 owned layered statistics fixture")
        );
        client.batch_execute(
            "CREATE TABLE IF NOT EXISTS legacy_platform(id BIGINT PRIMARY KEY, name TEXT);
             CREATE TABLE IF NOT EXISTS legacy_kind(id BIGINT PRIMARY KEY, \"Legacy Kind Label\" TEXT, ordinal BIGINT, platform_id BIGINT);
             CREATE TABLE IF NOT EXISTS legacy_school(id BIGINT PRIMARY KEY, \"Legacy Capacity\" BIGINT, \"Legacy Kind Key\" BIGINT);
             INSERT INTO legacy_platform VALUES(1,'Campus') ON CONFLICT DO NOTHING;
             INSERT INTO legacy_kind VALUES(1001,'shared',1,1),(1002,'shared',2,1) ON CONFLICT DO NOTHING;
             INSERT INTO legacy_school VALUES(1,800,1001),(2,800,1001),(3,800,1001),(4,800,1001),(5,100,1002),(6,NULL,1001),(7,800,1002) ON CONFLICT DO NOTHING;"
        ).await.unwrap();
    }
    let metadata = descriptors()
        .into_iter()
        .fold(InMemoryMetadataStore::new(), |m, d| m.with_entity(d));
    let executor = SqlDataServiceExecutor::new(
        PostgresDialect,
        PgMutationExecutor::new(pool.clone()),
        metadata.clone(),
    );
    let mut context = UserContext::new().with_metadata(metadata);
    context.register_executor(executor);
    Some((context, pool))
}

fn measures(query: SelectQuery, field: &str) -> SelectQuery {
    query
        .count("n")
        .aggregate(Aggregate::count_field(field, "nonnull"))
        .sum(field, "total")
        .min(field, "minimum")
        .max(field, "maximum")
        .avg(field, "mean")
        .stddev(field, "stddev")
        .stddev_pop(field, "stddevPop")
        .var_samp(field, "variance")
        .var_pop(field, "variancePop")
        .bit_and(field, "bitAnd")
        .bit_or(field, "bitOr")
        .bit_xor(field, "bitXor")
}

fn source(parent: SelectQuery) -> SelectQuery {
    measures(SelectQuery::new("School"), "capacity")
        .filter(Expr::lte("id", 5_i64))
        .group_by("kind_id")
        .object_group_by("kind", "kind_id", parent)
        .comment("Compute school observations and parent measures at their own identity grain")
        .purpose("Verify mapped, layered statistics through the PostgreSQL provider")
}

#[tokio::test]
async fn live_computed_grouping_merges_keys_and_preserves_owner_grain() {
    let Some((_context, pool)) = fixture().await else {
        return;
    };
    // A separate, stable fixture leaves the existing retained baseline intact.
    // Both rounds reuse these same rows; no cleanup or reseeding between runs.
    {
        let client = pool.get().await.unwrap();
        client.batch_execute(
            "CREATE TABLE IF NOT EXISTS computed_platform(id BIGINT PRIMARY KEY,name TEXT);
             CREATE TABLE IF NOT EXISTS computed_kind(id BIGINT PRIMARY KEY,\"Legacy Kind Label\" TEXT,ordinal BIGINT,platform_id BIGINT);
             CREATE TABLE IF NOT EXISTS computed_school(id BIGINT PRIMARY KEY,\"Legacy Capacity\" BIGINT,\"Legacy Kind Key\" BIGINT);
             INSERT INTO computed_platform VALUES(1,'Robert'),(2,'Rupert') ON CONFLICT DO NOTHING;
             INSERT INTO computed_kind VALUES(1001,'Robert',1,1),(1002,'Rupert',2,2) ON CONFLICT DO NOTHING;
             INSERT INTO computed_school VALUES(1,800,1001),(2,800,1001),(3,800,1001),(4,800,1001),(5,100,1002) ON CONFLICT DO NOTHING;"
        ).await.unwrap();
    }
    let metadata =
        descriptors()
            .into_iter()
            .fold(InMemoryMetadataStore::new(), |metadata, mut descriptor| {
                descriptor.table_name = descriptor.table_name.replacen("legacy_", "computed_", 1);
                metadata.with_entity(descriptor)
            });
    let executor = SqlDataServiceExecutor::new(
        PostgresDialect,
        PgMutationExecutor::new(pool.clone()),
        metadata.clone(),
    );
    let mut context = UserContext::new().with_metadata(metadata);
    context.register_executor(executor);
    let parent = SelectQuery::new("Kind")
        .group_by_expr("phonetic", Expr::soundex(Expr::column("label")))
        .count("kinds")
        .sum("ordinal", "ordinalTotal")
        .having(Expr::eq("phonetic", "R163"))
        .order_asc("phonetic")
        .limit(1)
        .comment("Merge school types by their phonetic key before paging");
    let rows = fetch(&context, source(parent)).await;
    assert_eq!(rows.len(), 1);
    let kind = object(&rows[0], "kind");
    assert_eq!(kind.get("phonetic"), Some(&Value::from("R163")));
    assert_eq!(kind.get("kinds").and_then(Value::try_i64), Some(2));
    assert_eq!(kind.get("ordinalTotal").and_then(Value::try_i64), Some(3));
    assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(5));
    assert_eq!(rows[0].get("total").and_then(Value::try_i64), Some(3300));
    let client = pool.get().await.unwrap();
    let oracle = client.query_one("SELECT soundex(\"Legacy Kind Label\"),COUNT(*),SUM(ordinal)::bigint FROM computed_kind GROUP BY soundex(\"Legacy Kind Label\")", &[]).await.unwrap();
    assert_eq!(oracle.get::<_, String>(0), "R163");
    assert_eq!(
        kind.get("kinds").and_then(Value::try_i64),
        Some(oracle.get::<_, i64>(1))
    );
    assert_eq!(
        kind.get("ordinalTotal").and_then(Value::try_i64),
        Some(oracle.get::<_, i64>(2))
    );
    drop(client);
    observe("computed-parent-group", &rows);
    let platform = SelectQuery::new("Platform")
        .group_by_expr("phonetic", Expr::soundex(Expr::column("name")))
        .count("platforms")
        .comment("Merge distinct platform identities under one computed key");
    let parent = SelectQuery::new("Kind")
        .count("kinds")
        .sum("ordinal", "ordinalTotal")
        .group_by("platform_id")
        .object_group_by("platform", "platform_id", platform)
        .comment("Retain school type grain through two roll-up levels");
    let rows = fetch(&context, source(parent)).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("total").and_then(Value::try_i64), Some(3300));
    let kind = object(&rows[0], "kind");
    assert_eq!(kind.get("kinds").and_then(Value::try_i64), Some(2));
    assert_eq!(kind.get("ordinalTotal").and_then(Value::try_i64), Some(3));
    let platform = object(kind, "platform");
    assert_eq!(platform.get("phonetic"), Some(&Value::from("R163")));
    assert_eq!(platform.get("platforms").and_then(Value::try_i64), Some(2));
    observe("computed-multihop-group", &rows);
    let query = SelectQuery::new("Kind")
        .group_by_expr("phonetic", Expr::soundex(Expr::column("label")))
        .count("kinds")
        .having(Expr::eq("phonetic", "R163"))
        .order_desc("kinds")
        .limit(1)
        .comment("Use the same computed key in ordinary statistics");
    let rows = fetch(&context, query).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("phonetic"), Some(&Value::from("R163")));
    assert_eq!(rows[0].get("kinds").and_then(Value::try_i64), Some(2));
    observe("computed-ordinary-group", &rows);
}

async fn fetch(context: &UserContext, query: SelectQuery) -> Vec<CompactRow> {
    let request = PurposedSelectQuery::new(query, "Verify live PostgreSQL roll-up statistics");
    context
        .entity_data_service::<Executor>("School")
        .unwrap()
        .fetch_all(&request)
        .await
        .unwrap()
}

trait Fields {
    fn get(&self, name: &str) -> Option<&Value>;
}
impl Fields for CompactRow {
    fn get(&self, name: &str) -> Option<&Value> {
        CompactRow::get(self, name)
    }
}
impl Fields for std::collections::BTreeMap<String, Value> {
    fn get(&self, name: &str) -> Option<&Value> {
        std::collections::BTreeMap::get(self, name)
    }
}

fn object<'a>(row: &'a dyn Fields, key: &str) -> &'a std::collections::BTreeMap<String, Value> {
    let Some(Value::Object(object)) = row.get(key) else {
        panic!("Missing object {key}")
    };
    object
}

fn assert_numeric(row: &dyn Fields, key: &str, expected: f64) {
    let actual = row
        .get(key)
        .and_then(Value::try_f64)
        .unwrap_or_else(|| panic!("Missing numeric {key}"));
    assert!(
        (actual - expected).abs() <= 1e-10 * expected.abs().max(1.0),
        "{key}: expected {expected}, actual {actual}"
    );
}

fn observe(label: &str, rows: &[CompactRow]) {
    let rows = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|(key, value)| (key.clone(), value.to_json_value()))
                .collect::<std::collections::BTreeMap<_, _>>()
        })
        .collect::<Vec<_>>();
    println!(
        "\nROLLUP_POSTGRES_CASE={}",
        json!({"case":label,"rows":rows})
    );
}

#[tokio::test]
async fn live_rollup_extended_operators_preserve_owner_grain() {
    let Some((context, pool)) = fixture().await else {
        return;
    };
    let parent = measures(SelectQuery::new("Kind"), "ordinal")
        .group_by("platform_id")
        .object_group_by(
            "platform",
            "platform_id",
            SelectQuery::new("Platform")
                .group_by("name")
                .comment("Group platform labels"),
        )
        .comment("Aggregate each school type once, not once per school");
    let rows = fetch(&context, source(parent)).await;
    assert_eq!(rows.len(), 1);
    let root = &rows[0];
    let kind = object(root, "kind");
    for (key, value) in [
        ("n", 5),
        ("nonnull", 5),
        ("total", 3300),
        ("minimum", 100),
        ("maximum", 800),
        ("bitAnd", 32),
        ("bitOr", 868),
        ("bitXor", 100),
    ] {
        assert_eq!(
            root.get(key).and_then(Value::try_i64),
            Some(value),
            "source {key}"
        );
    }
    for (key, value) in [
        ("mean", 660.0),
        ("variance", 98000.0),
        ("variancePop", 78400.0),
        ("stddev", 98000_f64.sqrt()),
        ("stddevPop", 280.0),
    ] {
        assert_numeric(root, key, value);
    }
    for (key, value) in [
        ("n", 2),
        ("nonnull", 2),
        ("total", 3),
        ("minimum", 1),
        ("maximum", 2),
        ("bitAnd", 0),
        ("bitOr", 3),
        ("bitXor", 3),
    ] {
        assert_eq!(
            kind.get(key).and_then(Value::try_i64),
            Some(value),
            "parent {key}"
        );
    }
    for (key, value) in [
        ("mean", 1.5),
        ("variance", 0.5),
        ("variancePop", 0.25),
        ("stddev", 0.5_f64.sqrt()),
        ("stddevPop", 0.5),
    ] {
        assert_numeric(kind, key, value);
    }
    assert_eq!(
        object(kind, "platform").get("name"),
        Some(&Value::from("Campus"))
    );
    // Independent straightforward SQL, not the runtime's generated CTEs.
    let client = pool.get().await.unwrap();
    for (map, sql) in [
        (
            root as &dyn Fields,
            "SELECT jsonb_build_object('n',COUNT(*),'nonnull',COUNT(\"Legacy Capacity\"),'total',SUM(\"Legacy Capacity\"),'minimum',MIN(\"Legacy Capacity\"),'maximum',MAX(\"Legacy Capacity\"),'mean',AVG(\"Legacy Capacity\"),'variance',VAR_SAMP(\"Legacy Capacity\"),'variancePop',VAR_POP(\"Legacy Capacity\"),'stddev',STDDEV(\"Legacy Capacity\"),'stddevPop',STDDEV_POP(\"Legacy Capacity\"),'bitAnd',BIT_AND(\"Legacy Capacity\"),'bitOr',BIT_OR(\"Legacy Capacity\"),'bitXor',BIT_XOR(\"Legacy Capacity\")) FROM legacy_school WHERE id<=5",
        ),
        (
            kind,
            "SELECT jsonb_build_object('n',COUNT(*),'nonnull',COUNT(ordinal),'total',SUM(ordinal),'minimum',MIN(ordinal),'maximum',MAX(ordinal),'mean',AVG(ordinal),'variance',VAR_SAMP(ordinal),'variancePop',VAR_POP(ordinal),'stddev',STDDEV(ordinal),'stddevPop',STDDEV_POP(ordinal),'bitAnd',BIT_AND(ordinal),'bitOr',BIT_OR(ordinal),'bitXor',BIT_XOR(ordinal)) FROM legacy_kind WHERE id IN(SELECT \"Legacy Kind Key\" FROM legacy_school WHERE id<=5)",
        ),
    ] {
        let oracle: serde_json::Value = client.query_one(sql, &[]).await.unwrap().get(0);
        for (key, expected) in oracle.as_object().unwrap() {
            assert_numeric(map, key, expected.as_f64().unwrap());
        }
    }
    observe("extended-owner-grain", &rows);
}

#[tokio::test]
async fn live_rollup_having_and_parent_cell_page() {
    let Some((context, _pool)) = fixture().await else {
        return;
    };
    let parent = SelectQuery::new("Kind")
        .group_by("ordinal")
        .count("kinds")
        .order_desc("ordinal")
        .limit(1)
        .comment("Choose the secondary type's completed cell");
    let rows = fetch(&context, source(parent).having(Expr::gt("total", 50_i64))).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(1));
    assert_eq!(rows[0].get("total").and_then(Value::try_i64), Some(100));
    assert_eq!(
        object(&rows[0], "kind")
            .get("ordinal")
            .and_then(Value::try_i64),
        Some(2)
    );
    observe("parent-cell-page-and-having", &rows);
    let query = measures(SelectQuery::new("School"), "capacity")
        .filter(Expr::lte("id", 5_i64))
        .having(Expr::gt("total", 3000_i64))
        .comment("Filter an ordinary aggregate result alias")
        .purpose("Compare ordinary and layered numeric HAVING");
    let rows = fetch(&context, query).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(5));
    observe("ordinary-having", &rows);
}

#[tokio::test]
async fn live_rollup_empty_and_null_measures() {
    let Some((context, _pool)) = fixture().await else {
        return;
    };
    let parent = measures(SelectQuery::new("Kind"), "ordinal");
    let rows = fetch(&context, source(parent).and_filter(Expr::lt("id", 0_i64))).await;
    assert_eq!(rows.len(), 1);
    for owner in [&rows[0] as &dyn Fields, object(&rows[0], "kind")] {
        for name in ["n", "nonnull"] {
            assert_eq!(owner.get(name).and_then(Value::try_i64), Some(0));
        }
        for name in [
            "total",
            "minimum",
            "maximum",
            "mean",
            "variance",
            "variancePop",
            "stddev",
            "stddevPop",
            "bitAnd",
            "bitOr",
            "bitXor",
        ] {
            assert_eq!(owner.get(name), Some(&Value::Null), "empty {name}");
        }
    }
    observe("empty-scalar", &rows);
    let parent = measures(SelectQuery::new("Kind"), "ordinal").group_by("platform_id");
    let mut query = source(parent);
    query.filter = Some(Expr::eq("id", 6_i64));
    let rows = fetch(&context, query).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(1));
    assert_eq!(rows[0].get("nonnull").and_then(Value::try_i64), Some(0));
    for name in [
        "total",
        "minimum",
        "maximum",
        "mean",
        "variance",
        "variancePop",
        "stddev",
        "stddevPop",
        "bitAnd",
        "bitOr",
        "bitXor",
    ] {
        assert_eq!(
            rows[0].get(name),
            Some(&Value::Null),
            "all-null source {name}"
        );
    }
    let kind = object(&rows[0], "kind");
    assert_eq!(kind.get("n").and_then(Value::try_i64), Some(1));
    assert_eq!(kind.get("variance"), Some(&Value::Null));
    assert_numeric(kind, "variancePop", 0.0);
    observe("all-null-source-and-singleton-parent", &rows);
}

#[tokio::test]
async fn live_distinct_count_uses_final_population_and_ignores_nulls() {
    let Some((context, pool)) = fixture().await else {
        return;
    };
    let parent = SelectQuery::new("Kind")
        .group_by("label")
        .count("kinds")
        .count_distinct("label", "labels")
        .having(Expr::eq("labels", 1_i64));
    let query = SelectQuery::new("School")
        .count("schools")
        .count_distinct("capacity", "capacities")
        .object_group_by("kind", "kind_id", parent)
        .having(Expr::eq("capacities", 2_i64))
        .comment("Count distinct observations after merging intermediate kinds")
        .purpose("Reject the sum-of-distinct-counts approximation");
    let rows = fetch(&context, query).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools").and_then(Value::try_i64), Some(7));
    assert_eq!(rows[0].get("capacities").and_then(Value::try_i64), Some(2));
    assert_eq!(
        object(&rows[0], "kind")
            .get("labels")
            .and_then(Value::try_i64),
        Some(1)
    );
    assert_eq!(
        object(&rows[0], "kind")
            .get("kinds")
            .and_then(Value::try_i64),
        Some(2)
    );
    let client = pool.get().await.unwrap();
    let oracle: i64 = client
        .query_one(
            "SELECT COUNT(DISTINCT \"Legacy Capacity\") FROM legacy_school",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let wrong_sum: i64 = client.query_one("SELECT SUM(n)::bigint FROM(SELECT COUNT(DISTINCT \"Legacy Capacity\") n FROM legacy_school GROUP BY \"Legacy Kind Key\")t", &[]).await.unwrap().get(0);
    assert_eq!(oracle, 2);
    assert_eq!(wrong_sum, 3);
    observe("distinct-overlap", &rows);
    for (id, expected_label, label) in [(-1_i64, 0, "distinct-empty"), (6_i64, 1, "distinct-null")]
    {
        let query = SelectQuery::new("School")
            .filter(Expr::eq("id", id))
            .count_distinct("capacity", "capacities")
            .object_group_by(
                "kind",
                "kind_id",
                SelectQuery::new("Kind").count_distinct("label", "labels"),
            )
            .comment("Preserve empty and null distinct count semantics")
            .purpose("Verify source and parent empty scalar results");
        let rows = fetch(&context, query).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("capacities").and_then(Value::try_i64), Some(0));
        assert_eq!(
            object(&rows[0], "kind")
                .get("labels")
                .and_then(Value::try_i64),
            Some(expected_label)
        );
        observe(label, &rows);
    }
    let ordinary = SelectQuery::new("School")
        .count_distinct("capacity", "capacities")
        .having(Expr::eq("capacities", 2_i64))
        .comment("Filter a distinct count alias without roll-up")
        .purpose("Verify ordinary and layered operator parity");
    let rows = fetch(&context, ordinary).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("capacities").and_then(Value::try_i64), Some(2));
    observe("ordinary-distinct-having", &rows);
}
