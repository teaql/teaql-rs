use super::*;

#[tokio::test]
async fn live_gbk_binary_grouping_keeps_bytes_nulls_and_parent_grain() {
    use teaql_sql::{SqlDialect, SqlTransport};
    let Some((_, pool)) = fixture().await else {
        return;
    };
    let client = pool.get().await.unwrap();
    // Exercise the empty binary literal without changing PostgreSQL's existing
    // rule: adding a required column never fabricates values for legacy rows.
    let empty_sql = format!(
        "SELECT {} AS encoded",
        PostgresDialect.fallback_default_value_sql(DataType::Binary)
    );
    assert_eq!(
        client
            .query_one(&empty_sql, &[])
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
        Vec::<u8>::new()
    );
    client.batch_execute(
        "CREATE TABLE IF NOT EXISTS gbk_binary_kind_326(id BIGINT PRIMARY KEY,\"Legacy Label\" TEXT);
         CREATE TABLE IF NOT EXISTS gbk_binary_school_326(id BIGINT PRIMARY KEY,kind_id BIGINT,capacity BIGINT);
         INSERT INTO gbk_binary_kind_326 VALUES(1001,'中'),(1002,'阿'),(1003,''),(1004,NULL) ON CONFLICT DO NOTHING;
         INSERT INTO gbk_binary_school_326 VALUES(1,1001,800),(2,1001,800),(3,1002,100) ON CONFLICT DO NOTHING;"
    ).await.unwrap();
    let oracle = client.query("SELECT convert_to(\"Legacy Label\",'GBK') FROM gbk_binary_kind_326 ORDER BY convert_to(\"Legacy Label\",'GBK')", &[])
        .await.unwrap().iter().map(|row| row.get::<_, Option<Vec<u8>>>(0)).collect::<Vec<_>>();
    assert_eq!(
        oracle,
        [
            Some(vec![]),
            Some(vec![0xb0, 0xa2]),
            Some(vec![0xd6, 0xd0]),
            None
        ]
    );
    drop(client);
    let transport = PgMutationExecutor::new(pool.clone());
    for value in [
        Value::Null,
        Value::TypedNull(DataType::Binary),
        Value::Binary(vec![]),
        Value::Binary(vec![0, 0xff, 0x27]),
        Value::Binary(vec![0xb0, 0xa2]),
    ] {
        let query = teaql_sql::CompiledQuery {
            sql: "SELECT $1::bytea AS encoded".into(),
            params: vec![value.clone()],
            comment: None,
            log_context: Default::default(),
        };
        let rows = transport.fetch_all_compact_sql(&query).await.unwrap();
        let expected = if matches!(value, Value::TypedNull(_)) {
            Value::Null
        } else {
            value
        };
        assert_eq!(rows[0].get("encoded"), Some(&expected));
    }
    let metadata = InMemoryMetadataStore::new()
        .with_entity(
            EntityDescriptor::new("BinaryKind")
                .table_name("gbk_binary_kind_326")
                .property(P::new("id", DataType::I64).id())
                .property(P::new("label", DataType::Text).column_name("Legacy Label")),
        )
        .with_entity(
            EntityDescriptor::new("BinarySchool")
                .table_name("gbk_binary_school_326")
                .property(P::new("id", DataType::I64).id())
                .property(P::new("kind_id", DataType::I64))
                .property(P::new("capacity", DataType::I64))
                .relation(R::new("kind", "BinaryKind").local_key("kind_id")),
        );
    let executor = SqlDataServiceExecutor::new(PostgresDialect, transport, metadata.clone());
    let mut context = UserContext::new().with_metadata(metadata);
    context.register_executor(executor);
    let ordinary = SelectQuery::new("BinaryKind")
        .group_by_expr("encoded", Expr::gbk(Expr::column("label")))
        .count("types")
        .order_asc("encoded")
        .limit(10)
        .comment("Read exact binary GBK grouping keys");
    let rows = fetch(&context, ordinary).await;
    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows.iter()
            .map(|row| row.get("encoded").unwrap().try_binary().map(<[u8]>::to_vec))
            .collect::<Vec<_>>(),
        oracle
    );
    observe("gbk-binary-ordinary", &rows);
    let parent = SelectQuery::new("BinaryKind")
        .group_by_expr("encoded", Expr::gbk(Expr::column("label")))
        .count("types")
        .having(Expr::eq("encoded", Value::Binary(vec![0xb0, 0xa2])))
        .order_asc("encoded")
        .limit(1);
    let query = SelectQuery::new("BinarySchool")
        .count("schools")
        .sum("capacity", "total")
        .object_group_by("kind", "kind_id", parent)
        .limit(10)
        .comment("Keep source totals while filtering a binary parent grouping key");
    let rows = fetch(&context, query).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools"), Some(&Value::I64(1)));
    assert_eq!(rows[0].get("total").and_then(Value::try_i64), Some(100));
    let Some(Value::Object(parent)) = rows[0].get("kind") else {
        panic!("missing parent");
    };
    assert_eq!(
        parent.get("encoded"),
        Some(&Value::Binary(vec![0xb0, 0xa2]))
    );
    observe("gbk-binary-parent", &rows);
}

#[tokio::test]
async fn live_gbk_orders_selected_chinese_cells_not_utf8_source_rows() {
    let Some((_, pool)) = fixture().await else {
        return;
    };
    let client = pool.get().await.unwrap();
    client.batch_execute(
        "CREATE TABLE IF NOT EXISTS gbk_kind_326(id BIGINT PRIMARY KEY,\"Legacy GBK Label\" TEXT);
         CREATE TABLE IF NOT EXISTS gbk_school_326(id BIGINT PRIMARY KEY,kind_id BIGINT,capacity BIGINT);
         INSERT INTO gbk_kind_326 VALUES(1001,'中'),(1002,'阿'),(1003,'文'),(1004,NULL) ON CONFLICT DO NOTHING;
         INSERT INTO gbk_school_326 VALUES(1,1001,800),(2,1001,800),(3,1002,100),(4,1003,300) ON CONFLICT DO NOTHING;"
    ).await.unwrap();
    let utf8 = client.query("SELECT \"Legacy GBK Label\" FROM gbk_kind_326 WHERE \"Legacy GBK Label\" IS NOT NULL ORDER BY convert_to(\"Legacy GBK Label\",'UTF8')", &[]).await.unwrap()
        .iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>();
    assert_eq!(utf8, ["中", "文", "阿"]);
    let gbk = client.query("SELECT \"Legacy GBK Label\",encode(convert_to(\"Legacy GBK Label\",'GBK'),'hex') FROM gbk_kind_326 WHERE \"Legacy GBK Label\" IS NOT NULL ORDER BY convert_to(\"Legacy GBK Label\",'GBK')", &[]).await.unwrap();
    assert_eq!(
        gbk.iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect::<Vec<_>>(),
        [
            ("阿".into(), "b0a2".into()),
            ("文".into(), "cec4".into()),
            ("中".into(), "d6d0".into())
        ]
    );
    drop(client);
    let metadata = InMemoryMetadataStore::new()
        .with_entity(
            EntityDescriptor::new("GbkKind")
                .table_name("gbk_kind_326")
                .property(P::new("id", DataType::I64).id())
                .property(P::new("label", DataType::Text).column_name("Legacy GBK Label")),
        )
        .with_entity(
            EntityDescriptor::new("GbkSchool")
                .table_name("gbk_school_326")
                .property(P::new("id", DataType::I64).id())
                .property(P::new("capacity", DataType::I64))
                .property(P::new("kind_id", DataType::I64))
                .relation(R::new("kind", "GbkKind").local_key("kind_id")),
        );
    let service = SqlDataServiceExecutor::new(
        PostgresDialect,
        PgMutationExecutor::new(pool.clone()),
        metadata.clone(),
    );
    let mut context = UserContext::new().with_metadata(metadata);
    context.register_executor(service);
    let ordinary = SelectQuery::new("GbkKind")
        .group_by_expr("caption", Expr::identity(Expr::column("label")))
        .count("types")
        .filter(Expr::IsNotNull(Box::new(Expr::column("label"))))
        .order_gbk_asc("caption")
        .limit(2)
        .comment("Order completed Chinese cells by their GBK bytes");
    let rows = fetch(&context, ordinary.clone()).await;
    assert_eq!(
        rows.iter().map(|r| r.get("caption")).collect::<Vec<_>>(),
        [Some(&Value::from("阿")), Some(&Value::from("文"))]
    );
    observe("gbk-ordinary", &rows);
    let parent = SelectQuery::new("GbkKind")
        .group_by("label")
        .count("types")
        .order_gbk_asc("label")
        .limit(1);
    let source = |parent| {
        SelectQuery::new("GbkSchool")
            .count("schools")
            .sum("capacity", "total")
            .group_by("kind_id")
            .object_group_by("kind", "kind_id", parent)
            .limit(20)
            .comment("Retain source and parent grain while ordering a GBK text result")
    };
    let rows = fetch(&context, source(parent)).await;
    assert_first_kind(&rows);
    observe("gbk-parent", &rows);
    let query = source(SelectQuery::new("GbkKind").group_by("label").count("types"))
        .order_expr_asc(Expr::gbk(Expr::column("kind.label")))
        .limit(1);
    let original = query.clone();
    let rows = fetch(&context, query.clone()).await;
    assert_first_kind(&rows);
    assert_eq!(query, original);
    observe("gbk-nested", &rows);
    let null = SelectQuery::new("GbkKind")
        .group_by("label")
        .count("types")
        .filter(Expr::IsNull(Box::new(Expr::column("label"))))
        .order_gbk_asc("label")
        .limit(1)
        .comment("Preserve a genuine null GBK sort operand");
    assert_eq!(
        fetch(&context, null).await[0].get("label"),
        Some(&Value::Null)
    );
}

fn assert_first_kind(rows: &[CompactRow]) {
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("schools").and_then(Value::try_i64), Some(1));
    assert_eq!(rows[0].get("total").and_then(Value::try_i64), Some(100));
    let Some(Value::Object(kind)) = rows[0].get("kind") else {
        panic!("missing parent");
    };
    assert_eq!(kind.get("label"), Some(&Value::from("阿")));
    assert_eq!(kind.get("types").and_then(Value::try_i64), Some(1));
}
