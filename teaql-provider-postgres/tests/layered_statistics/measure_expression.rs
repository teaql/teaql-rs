use super::*;
use teaql_core::ExprFunction as F;

#[tokio::test]
async fn live_expression_measure_order_matches_exact_selected_ast() {
    let Some((context, _pool)) = fixture().await else {
        return;
    };
    let total = Expr::sum_expr(Expr::identity(Expr::column("capacity")));
    let rank = Expr::sum_expr(Expr::identity(Expr::column("ordinal")));
    let parent = SelectQuery::new("Kind")
        .group_by("ordinal")
        .project_expr("rankTotal", rank.clone());
    let source = |parent| {
        SelectQuery::new("School")
            .filter(Expr::lte("id", 5_i64))
            .count("schools")
            .project_expr("computed", total.clone())
            .object_group_by("kind", "kind_id", parent)
            .limit(1)
            .comment("Order the exact selected statistical expression")
    };
    for query in [
        source(parent.clone()).order_expr_desc(total.clone()),
        source(parent.clone()).order_desc("computed"),
    ] {
        let original = query.clone();
        let rows = fetch(&context, query.clone()).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("computed").and_then(Value::try_i64), Some(3200));
        assert_eq!(rows[0].get("schools"), Some(&Value::I64(4)));
        assert_eq!(query, original);
    }
    for parent in [
        parent.clone().order_expr_desc(rank).limit(1),
        parent.order_desc("rankTotal").limit(1),
    ] {
        let rows = fetch(&context, source(parent)).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("schools"), Some(&Value::I64(1)));
        assert_eq!(
            object(&rows[0], "kind")
                .get("rankTotal")
                .and_then(Value::try_i64),
            Some(2)
        );
    }
    observe(
        "expression-measure-order",
        &fetch(
            &context,
            source(SelectQuery::new("Kind").group_by("ordinal")).order_expr_desc(total.clone()),
        )
        .await,
    );
}

#[tokio::test]
async fn live_layered_expression_measures_keep_owner_grain_and_scalar_types() {
    let Some((context, pool)) = fixture().await else {
        return;
    };
    let oracle=pool.get().await.unwrap().query_one("SELECT count(id),sum(ordinal),avg(ordinal),sum(7::bigint)::bigint,bit_xor(id) FROM legacy_kind WHERE id IN(1001,1002)",&[]).await.unwrap();
    assert_eq!(oracle.get::<_, i64>(0), 2);
    assert_eq!(oracle.get::<_, i64>(3), 14);
    assert_eq!(oracle.get::<_, i64>(4), 3);
    let source = |parent| {
        SelectQuery::new("School")
            .filter(Expr::lte("id", 5_i64))
            .count("schools")
            .object_group_by("kind", "kind_id", parent)
            .limit(20)
            .comment("Compute expressions at original observation grain")
    };
    let parent = SelectQuery::new("Kind")
        .group_by("platform_id")
        .project_expr(
            "perType",
            Expr::sum_expr(Expr::identity(Expr::value(7_i64))),
        )
        .project_expr(
            "parentXor",
            Expr::bit_xor_expr(Expr::identity(Expr::column("id"))),
        )
        .having(Expr::eq("perType", 14_i64))
        .order_desc("perType")
        .limit(1);
    let query = source(parent)
        .project_expr("perSchool", Expr::sum_expr(Expr::value(3_i64)))
        .having(Expr::eq("perSchool", 15_i64))
        .order_desc("kind.parentXor");
    let original = query.clone();
    let rows = fetch(&context, query.clone()).await;
    assert_eq!(query, original);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("perSchool").and_then(Value::try_i64), Some(15));
    let kind = object(&rows[0], "kind");
    assert_eq!(kind.get("perType").and_then(Value::try_i64), Some(14));
    assert_eq!(kind.get("parentXor"), Some(&Value::I64(3)));
    observe("expression-measure-owner-grain", &rows);
    for (function, expected) in [
        (F::Count, 2.0),
        (F::CountDistinct, 2.0),
        (F::Sum, 3.0),
        (F::Min, 1.0),
        (F::Max, 2.0),
        (F::Avg, 1.5),
        (F::Stddev, 0.5_f64.sqrt()),
        (F::StddevPop, 0.5),
        (F::VarSamp, 0.5),
        (F::VarPop, 0.25),
        (F::BitAnd, 0.0),
        (F::BitOr, 3.0),
        (F::BitXor, 3.0),
    ] {
        let query = source(
            SelectQuery::new("Kind")
                .group_by("platform_id")
                .project_expr(
                    "computed",
                    Expr::function(function, [Expr::identity(Expr::column("ordinal"))]),
                ),
        );
        let rows = fetch(&context, query).await;
        let kind = object(&rows[0], "kind");
        assert!(
            (kind["computed"].try_f64().unwrap() - expected).abs() < 1e-10,
            "{function:?}: {kind:?}"
        );
        let query = source(SelectQuery::new("Kind").project_expr(
            "computed",
            Expr::function(
                function,
                [Expr::identity(Expr::value(Value::TypedNull(DataType::I64)))],
            ),
        ));
        let rows = fetch(&context, query).await;
        let kind = object(&rows[0], "kind");
        assert_eq!(
            kind.get("computed"),
            Some(&if matches!(function, F::Count | F::CountDistinct) {
                Value::I64(0)
            } else {
                Value::Null
            })
        );
    }
    let parent = SelectQuery::new("Kind")
        .project_expr(
            "labels",
            Expr::count_distinct(Expr::soundex(Expr::column("label"))),
        )
        .project_expr(
            "decimal",
            Expr::sum_expr(Expr::identity(Expr::value(teaql_core::Decimal::new(1, 9)))),
        )
        .project_expr("caption", Expr::min_expr(Expr::value("owner")));
    let rows = fetch(&context, source(parent)).await;
    let kind = object(&rows[0], "kind");
    assert_eq!(kind.get("labels"), Some(&Value::I64(1)));
    assert_eq!(
        kind.get("decimal"),
        Some(&Value::Decimal(teaql_core::Decimal::new(2, 9)))
    );
    assert_eq!(kind.get("caption"), Some(&Value::from("owner")));
    observe("expression-measure-transforms", &rows);
}
