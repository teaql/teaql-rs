use super::*;

#[tokio::test]
async fn live_bitwise_expressions_keep_wrapped_fields_and_signed_parameters() {
    let Some((context, pool)) = fixture().await else {
        return;
    };
    let oracle = pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT bit_and(id),bit_or(id),bit_xor(id) FROM legacy_kind WHERE id IN(1001,1002)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        (
            oracle.get::<_, i64>(0),
            oracle.get::<_, i64>(1),
            oracle.get::<_, i64>(2)
        ),
        (1000, 1003, 3)
    );
    let mut field_rows = Vec::new();
    let mut parameter_rows = Vec::new();
    for (function, expected) in [
        (teaql_core::ExprFunction::BitAnd, 1000),
        (teaql_core::ExprFunction::BitOr, 1003),
        (teaql_core::ExprFunction::BitXor, 3),
    ] {
        let mut field = Expr::column("id");
        for _ in 0..16 {
            field = Expr::identity(field);
        }
        let query = SelectQuery::new("Kind")
            .project_expr("bits", Expr::function(function, [field]))
            .filter(Expr::in_list("id", [Value::I64(1001), Value::I64(1002)]))
            .limit(20)
            .comment("Keep integer field types through nested identity wrappers");
        let original = query.clone();
        let rows = fetch(&context, query.clone()).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("bits"), Some(&Value::I64(expected)));
        assert_eq!(query, original);
        if function == teaql_core::ExprFunction::BitXor {
            field_rows = rows;
        }
        for value in [
            Value::I64(i64::MIN),
            Value::I64(i64::MAX),
            Value::U64(9_007_199_254_740_993),
            Value::Null,
            Value::TypedNull(DataType::I64),
        ] {
            let representative =
                value == Value::I64(i64::MAX) && function == teaql_core::ExprFunction::BitXor;
            let expected = match value.try_i64() {
                Some(_) if function == teaql_core::ExprFunction::BitXor => Value::I64(0),
                Some(value) => Value::I64(value),
                None => Value::Null,
            };
            let query = SelectQuery::new("Kind")
                .project_expr(
                    "bits",
                    Expr::function(
                        function,
                        [Expr::identity(Expr::identity(Expr::value(value)))],
                    ),
                )
                .filter(Expr::in_list("id", [Value::I64(1001), Value::I64(1002)]))
                .limit(20)
                .comment("Retain signed integer parameter bounds and null results");
            let rows = fetch(&context, query).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("bits"), Some(&expected));
            if representative {
                parameter_rows = rows;
            }
        }
    }
    observe("bitwise-expression-fields", &field_rows);
    observe("bitwise-expression-parameters", &parameter_rows);
}
