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
async fn live_multilayer_membership_retains_nested_cells_and_ordinary_query_policy() {
    let Some((context, _pool)) = fixture().await else {
        return;
    };
    let school = descriptors()
        .into_iter()
        .find(|entity| entity.name == "School")
        .unwrap();
    let independent = source(SelectQuery::new("Kind").group_by("id")).having(Expr::gte("n", 4));
    for negative in [false, true] {
        let membership = if negative {
            Expr::not_in_subquery("kind_id", school.clone(), independent.clone(), "kind.id")
        } else {
            Expr::in_subquery("kind_id", school.clone(), independent.clone(), "kind.id")
        };
        let rows = fetch(
            &context,
            source(SelectQuery::new("Kind").group_by("label")).and_filter(membership),
        )
        .await;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("n").and_then(Value::try_i64),
            Some(if negative { 1 } else { 4 })
        );
        observe(
            if negative {
                "multilayer-membership-negative"
            } else {
                "multilayer-membership-positive"
            },
            &rows,
        );
    }
    let parent = SelectQuery::new("Kind")
        .group_by("id")
        .filter(Expr::gte("ordinal", 1));
    let independent = source(parent)
        .and_filter(Expr::gte("capacity", 100))
        .having(Expr::gte("n", 1))
        .order_desc("kind.id")
        .limit(1);
    let query = source(SelectQuery::new("Kind").group_by("label"))
        .and_filter(Expr::lte("capacity", 1000))
        .and_filter(Expr::in_subquery(
            "kind_id",
            school.clone(),
            independent,
            "kind.id",
        ));
    let rows = fetch(&context, query).await;
    assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(1));
    observe("multilayer-membership-page-and-parameters", &rows);
    #[derive(Clone)]
    struct NestedPolicy(std::sync::Arc<std::sync::Mutex<Vec<String>>>);
    impl teaql_runtime::RequestPolicy for NestedPolicy {
        fn enforce_select(
            &self,
            _context: &UserContext,
            query: &mut SelectQuery,
        ) -> Result<(), teaql_runtime::RuntimeError> {
            if query.entity == "Kind" {
                self.0
                    .lock()
                    .unwrap()
                    .push(query.request_scope().unwrap().comment().to_owned());
                query.filter = Some(
                    query
                        .filter
                        .take()
                        .map(|filter| filter.and_expr(Expr::eq("ordinal", 1)))
                        .unwrap_or_else(|| Expr::eq("ordinal", 1)),
                );
            }
            Ok(())
        }
    }
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let context = context.with_request_policy(NestedPolicy(seen.clone()));
    let independent = source(
        SelectQuery::new("Kind")
            .group_by("id")
            .comment("Authorize nested independent types"),
    )
    .having(Expr::lte("n", 1));
    let query = SelectQuery::new("School")
        .count("schools")
        .filter(Expr::lte("id", 5))
        .and_filter(Expr::in_subquery("kind_id", school, independent, "kind.id"))
        .comment("An ordinary scalar request must authorize nested roll-up filters");
    let rows = fetch(&context, query).await;
    assert_eq!(rows[0].get("schools").and_then(Value::try_i64), Some(0));
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .any(|comment| comment == "Authorize nested independent types")
    );
    observe("multilayer-membership-ordinary-policy", &rows);
}

#[tokio::test]
async fn live_independent_grouped_membership_keeps_cells_and_context_policy() {
    let Some((context, _pool)) = fixture().await else {
        return;
    };
    let kind = descriptors()
        .into_iter()
        .find(|entity| entity.name == "Kind")
        .unwrap();
    for (threshold, expected, label) in [
        (2, 1, "independent-grouped-membership"),
        (3, 0, "independent-grouped-empty"),
    ] {
        let independent = SelectQuery::new("Kind")
            .group_by("id")
            .sum("ordinal", "ordinalTotal")
            .having(Expr::eq("ordinalTotal", threshold))
            .comment("Filter independent completed type cells");
        let query = source(SelectQuery::new("Kind").group_by("label")).and_filter(
            Expr::in_subquery("kind_id", kind.clone(), independent.clone(), "id"),
        );
        let rows = fetch(&context, query).await;
        assert_eq!(rows.len(), expected);
        if expected == 1 {
            assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(1));
        }
        observe(label, &rows);
        let query = source(SelectQuery::new("Kind").group_by("label")).and_filter(
            Expr::not_in_subquery("kind_id", kind.clone(), independent, "id"),
        );
        let rows = fetch(&context, query).await;
        assert_eq!(
            rows[0].get("n").and_then(Value::try_i64),
            Some(if threshold == 2 { 4 } else { 5 })
        );
    }
    let independent = SelectQuery::new("Kind")
        .group_by_expr("kindKey", Expr::column("id"))
        .sum("ordinal", "ordinalTotal")
        .order_desc("ordinalTotal")
        .limit(1)
        .comment("Page independent aliased cells before selecting one membership column");
    let query = source(SelectQuery::new("Kind").group_by("label")).and_filter(
        Expr::not_in_subquery("kind_id", kind.clone(), independent, "kindKey"),
    );
    let rows = fetch(&context, query).await;
    assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(4));
    observe("independent-grouped-alias-page", &rows);
    let independent = SelectQuery::new("Kind")
        .filter(Expr::gt("ordinal", 2))
        .sum("ordinal", "total")
        .comment("Remove null scalar membership after aggregation");
    let query = source(SelectQuery::new("Kind").group_by("label")).and_filter(
        Expr::not_in_subquery("kind_id", kind.clone(), independent, "total"),
    );
    let rows = fetch(&context, query).await;
    assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(5));
    observe("independent-grouped-null", &rows);

    #[derive(Clone)]
    struct PredicatePolicy(std::sync::Arc<std::sync::Mutex<Vec<String>>>);
    impl teaql_runtime::RequestPolicy for PredicatePolicy {
        fn enforce_select(
            &self,
            _context: &UserContext,
            query: &mut SelectQuery,
        ) -> Result<(), teaql_runtime::RuntimeError> {
            if query.entity == "Kind" {
                self.0
                    .lock()
                    .unwrap()
                    .push(query.request_scope().unwrap().comment().to_owned());
                query.filter = Some(
                    query
                        .filter
                        .take()
                        .map(|filter| filter.and_expr(Expr::eq("ordinal", 1)))
                        .unwrap_or_else(|| Expr::eq("ordinal", 1)),
                );
            }
            Ok(())
        }
    }
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let context = context.with_request_policy(PredicatePolicy(seen.clone()));
    let independent = SelectQuery::new("Kind")
        .group_by("id")
        .sum("ordinal", "ordinalTotal")
        .having(Expr::eq("ordinalTotal", 2))
        .comment("Authorize independent PostgreSQL statistics");
    let query = source(SelectQuery::new("Kind").group_by("label")).and_filter(Expr::in_subquery(
        "kind_id",
        kind,
        independent,
        "id",
    ));
    let rows = fetch(&context, query).await;
    assert!(rows.is_empty());
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .any(|comment| comment == "Authorize independent PostgreSQL statistics")
    );
    observe("independent-grouped-policy", &rows);
}

#[tokio::test]
async fn live_identity_grouping_keeps_values_aliases_and_owner_grain() {
    let Some((context, pool)) = fixture().await else {
        return;
    };
    let ordinary = SelectQuery::new("School")
        .filter(Expr::lte("id", 5_i64))
        .group_by_expr("bucket", Expr::identity(Expr::column("capacity")))
        .count("schools")
        .having(Expr::gte("bucket", 100_i64))
        .order_desc("bucket")
        .offset(1)
        .limit(1)
        .comment("Group legacy capacity using the Java SELF equivalent")
        .purpose("Verify identity grouping without value conversion");
    let rows = fetch(&context, ordinary).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("bucket"), Some(&Value::I64(100)));
    assert_eq!(rows[0].get("schools").and_then(Value::try_i64), Some(1));
    assert!(!rows[0].contains_key("capacity"));
    observe("identity-ordinary-group", &rows);
    let parent = SelectQuery::new("Kind")
        .group_by_expr("rank", Expr::identity(Expr::column("ordinal")))
        .count("kinds")
        .sum("ordinal", "ordinalTotal")
        .having(Expr::gte("rank", 1_i64))
        .order_desc("ordinal")
        .limit(1)
        .comment("Page completed identity groups");
    let rows = fetch(&context, source(parent)).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(1));
    assert_eq!(object(&rows[0], "kind").get("rank"), Some(&Value::I64(2)));
    assert!(!object(&rows[0], "kind").contains_key("ordinal"));
    observe("identity-parent-group", &rows);
    let platform = SelectQuery::new("Platform")
        .group_by_expr("caption", Expr::identity(Expr::column("name")))
        .count("platforms")
        .order_asc("name")
        .comment("Retain the platform name without transformation");
    let parent = SelectQuery::new("Kind")
        .count("kinds")
        .sum("ordinal", "ordinalTotal")
        .group_by("platform_id")
        .object_group_by("platform", "platform_id", platform);
    let rows = fetch(&context, source(parent)).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(5));
    let kind = object(&rows[0], "kind");
    assert_eq!(kind.get("kinds").and_then(Value::try_i64), Some(2));
    assert_eq!(kind.get("ordinalTotal").and_then(Value::try_i64), Some(3));
    let Some(Value::Object(platform)) = kind.get("platform") else {
        panic!("missing platform")
    };
    assert_eq!(platform.get("caption"), Some(&Value::from("Campus")));
    assert_eq!(platform.get("platforms").and_then(Value::try_i64), Some(1));
    observe("identity-multihop-group", &rows);
    let oracle=pool.get().await.unwrap().query_one(
        "SELECT \"Legacy Capacity\",COUNT(*) FROM legacy_school WHERE id<=5 GROUP BY \"Legacy Capacity\" HAVING \"Legacy Capacity\">=100 ORDER BY \"Legacy Capacity\" DESC OFFSET 1 LIMIT 1",&[]).await.unwrap();
    assert_eq!(oracle.get::<_, i64>(0), 100);
    assert_eq!(oracle.get::<_, i64>(1), 1);
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

#[tokio::test]
async fn live_nullable_and_orphan_membership_is_distinct_from_a_null_dimension() {
    let Some((_context, pool)) = fixture().await else {
        return;
    };
    {
        let client = pool.get().await.unwrap();
        client.batch_execute(
            "CREATE TABLE IF NOT EXISTS membership_platform(id BIGINT PRIMARY KEY,name TEXT);
             CREATE TABLE IF NOT EXISTS membership_kind(id BIGINT PRIMARY KEY,\"Legacy Kind Label\" TEXT,ordinal BIGINT,platform_id BIGINT);
             CREATE TABLE IF NOT EXISTS membership_school(id BIGINT PRIMARY KEY,\"Legacy Capacity\" BIGINT,\"Legacy Kind Key\" BIGINT);
             INSERT INTO membership_platform VALUES(1,NULL) ON CONFLICT DO NOTHING;
             INSERT INTO membership_kind VALUES(1001,NULL,1,1),(1002,'shared',2,1),(1003,'no root',3,NULL),(1004,'orphan root',4,9999) ON CONFLICT DO NOTHING;
             INSERT INTO membership_school VALUES(1,800,1001),(2,800,1001),(3,800,1001),(4,800,1001),(5,100,1002),(6,50,NULL),(7,700,9999),(8,30,1003),(9,40,1004) ON CONFLICT DO NOTHING;"
        ).await.unwrap();
    }
    let metadata =
        descriptors()
            .into_iter()
            .fold(InMemoryMetadataStore::new(), |metadata, mut descriptor| {
                descriptor.table_name = descriptor.table_name.replacen("legacy_", "membership_", 1);
                metadata.with_entity(descriptor)
            });
    let mut context = UserContext::new().with_metadata(metadata.clone());
    context.register_executor(SqlDataServiceExecutor::new(
        PostgresDialect,
        PgMutationExecutor::new(pool.clone()),
        metadata,
    ));
    let raw = SelectQuery::new("School")
        .group_by("kind_id")
        .count("n")
        .sum("capacity", "total")
        .comment("Compare raw foreign key groups with matched-parent roll-up membership");
    let rows = fetch(&context, raw).await;
    assert_eq!(rows.len(), 6);
    assert_eq!(
        rows.iter()
            .map(|row| row.get("n").and_then(Value::try_i64).unwrap())
            .sum::<i64>(),
        9
    );
    assert!(
        rows.iter()
            .any(|row| row.get("kind_id") == Some(&Value::Null))
    );
    assert!(
        rows.iter()
            .any(|row| row.get("kind_id").and_then(Value::try_i64) == Some(9999))
    );
    observe("nullable-raw-foreign-keys", &rows);
    let source = |parent: SelectQuery| {
        SelectQuery::new("School")
            .count("n")
            .sum("capacity", "total")
            .group_by("kind_id")
            .object_group_by("kind", "kind_id", parent)
            .comment("Require a matching parent at every declared roll-up edge")
    };
    let rows = fetch(
        &context,
        source(SelectQuery::new("Kind").group_by("label").count("kinds")),
    )
    .await;
    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows.iter()
            .map(|row| row.get("n").and_then(Value::try_i64).unwrap())
            .sum::<i64>(),
        7
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.get("total").and_then(Value::try_i64).unwrap())
            .sum::<i64>(),
        3370
    );
    let null_cell = rows
        .iter()
        .find(|row| object(*row, "kind").get("label") == Some(&Value::Null))
        .unwrap();
    assert_eq!(null_cell.get("n").and_then(Value::try_i64), Some(4));
    assert_eq!(null_cell.get("total").and_then(Value::try_i64), Some(3200));
    assert_eq!(
        object(null_cell, "kind")
            .get("kinds")
            .and_then(Value::try_i64),
        Some(1)
    );
    observe("nullable-parent-membership", &rows);
    let platform = SelectQuery::new("Platform")
        .group_by("name")
        .count("platforms");
    let parent = SelectQuery::new("Kind")
        .count("kinds")
        .sum("ordinal", "ordinalTotal")
        .group_by("platform_id")
        .object_group_by("platform", "platform_id", platform);
    let rows = fetch(&context, source(parent)).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(5));
    assert_eq!(rows[0].get("total").and_then(Value::try_i64), Some(3300));
    let kind = object(&rows[0], "kind");
    assert_eq!(kind.get("kinds").and_then(Value::try_i64), Some(2));
    assert_eq!(kind.get("ordinalTotal").and_then(Value::try_i64), Some(3));
    let platform = object(kind, "platform");
    assert_eq!(platform.get("name"), Some(&Value::Null));
    assert_eq!(platform.get("platforms").and_then(Value::try_i64), Some(1));
    let client = pool.get().await.unwrap();
    let oracle=client.query_one("SELECT COUNT(*),SUM(s.\"Legacy Capacity\")::bigint FROM membership_school s JOIN membership_kind k ON k.id=s.\"Legacy Kind Key\" JOIN membership_platform p ON p.id=k.platform_id",&[]).await.unwrap();
    assert_eq!(
        rows[0].get("n").and_then(Value::try_i64),
        Some(oracle.get::<_, i64>(0))
    );
    assert_eq!(
        rows[0].get("total").and_then(Value::try_i64),
        Some(oracle.get::<_, i64>(1))
    );
    drop(client);
    observe("nullable-grandparent-membership", &rows);
    let grouped = source(SelectQuery::new("Kind").group_by("label"))
        .and_filter(Expr::in_list("id", [6_i64, 7].map(Value::from)));
    assert!(fetch(&context, grouped).await.is_empty());
    let scalar = source(
        SelectQuery::new("Kind")
            .count("kinds")
            .sum("ordinal", "ordinalTotal"),
    )
    .and_filter(Expr::in_list("id", [6_i64, 7].map(Value::from)));
    let rows = fetch(&context, scalar).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("n").and_then(Value::try_i64), Some(0));
    assert_eq!(rows[0].get("total"), Some(&Value::Null));
    assert_eq!(
        object(&rows[0], "kind")
            .get("kinds")
            .and_then(Value::try_i64),
        Some(0)
    );
    assert_eq!(
        object(&rows[0], "kind").get("ordinalTotal"),
        Some(&Value::Null)
    );
    observe("nullable-empty-scalar", &rows);
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
