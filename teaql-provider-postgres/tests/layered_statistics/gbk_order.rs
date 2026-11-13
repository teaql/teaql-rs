use super::*;

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
