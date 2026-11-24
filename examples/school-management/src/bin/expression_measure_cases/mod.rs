//! Current field Assist: typed aggregate ASTs through generated Q builders.
use school_management_service_core::{Q, SchoolRequest, TeaqlRuntime};
use teaql_core::{CompactRow, Decimal, Expr, ExprFunction as F, NamedExpr, Value};

fn source() -> SchoolRequest {
    Q::schools_minimal().count_as("schools")
}
async fn rows(
    request: SchoolRequest,
    context: &impl TeaqlRuntime,
) -> Result<Vec<CompactRow>, Box<dyn std::error::Error>> {
    Ok(request
        .limit(20)
        .comment("Compute aggregate expressions at each declared owner grain")
        .purpose("Verify generated aggregate ASTs without application SQL")
        .execute_for_rows(context)
        .await?
        .into_iter()
        .collect())
}
fn object(row: &CompactRow) -> &std::collections::BTreeMap<String, Value> {
    let Some(Value::Object(value)) = row.get("school_type") else {
        panic!("missing parent: {row:?}")
    };
    value
}
fn json(row: &CompactRow) -> serde_json::Value {
    serde_json::Value::Object(
        row.iter()
            .map(|(k, v)| (k.clone(), v.to_json_value()))
            .collect(),
    )
}
pub async fn verify(context: &impl TeaqlRuntime) -> Result<(), Box<dyn std::error::Error>> {
    let mut evidence = Vec::new();
    let result = rows(
        source()
            .add_aggregate(NamedExpr::new(
                "perSchool",
                Expr::sum_expr(Expr::value(3_i64)),
            ))
            .add_aggregate(NamedExpr::new(
                "perSchool",
                Expr::sum_expr(Expr::value(9_i64)),
            )),
        context,
    )
    .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].get("perSchool").and_then(Value::try_i64),
        Some(15)
    );
    evidence.push(serde_json::json!({"case":"expression-ordinary","row":json(&result[0])}));

    let mut query = source().add_aggregate(NamedExpr::new(
        "perSchool",
        Expr::sum_expr(Expr::value(3_i64)),
    ));
    query
        .roll_up_to_school_type()
        .group_by_platform()
        .add_aggregate(NamedExpr::new(
            "perType",
            Expr::sum_expr(Expr::identity(Expr::value(7_i64))),
        ))
        .having(Expr::eq("perType", 14_i64))
        .add_order_by_descending("perType")
        .limit(1);
    let result = rows(
        query
            .having(Expr::eq("perSchool", 15_i64))
            .add_order_by_descending("school_type.perType"),
        context,
    )
    .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].get("perSchool").and_then(Value::try_i64),
        Some(15)
    );
    assert_eq!(
        object(&result[0]).get("perType").and_then(Value::try_i64),
        Some(14)
    );
    evidence.push(serde_json::json!({"case":"expression-owner-grain","row":json(&result[0])}));

    for (function, expected) in [
        (F::Count, 2.0),
        (F::CountDistinct, 2.0),
        (F::Sum, 2003.0),
        (F::Min, 1001.0),
        (F::Max, 1002.0),
        (F::Avg, 1001.5),
        (F::Stddev, 0.5_f64.sqrt()),
        (F::StddevPop, 0.5),
        (F::VarSamp, 0.5),
        (F::VarPop, 0.25),
        (F::BitAnd, 1000.0),
        (F::BitOr, 1003.0),
        (F::BitXor, 3.0),
    ] {
        let mut query = source();
        query
            .roll_up_to_school_type()
            .group_by_platform()
            .add_aggregate(NamedExpr::new(
                "computed",
                Expr::function(function, [Expr::identity(Expr::column("id"))]),
            ));
        let result = rows(query, context).await?;
        assert_eq!(result.len(), 1);
        let value = &object(&result[0])["computed"];
        assert!(
            (value.try_f64().unwrap() - expected).abs() < 1e-10,
            "{function:?}: {value:?}"
        );
        if matches!(
            function,
            F::Count | F::CountDistinct | F::BitAnd | F::BitOr | F::BitXor
        ) {
            assert!(matches!(value, Value::I64(_) | Value::U64(_)));
        }
    }
    evidence.push(serde_json::json!({"case":"expression-operators","checked":13}));

    let mut query = source();
    query
        .roll_up_to_school_type()
        .add_aggregate(NamedExpr::new(
            "names",
            Expr::count_distinct(Expr::soundex(Expr::column("name"))),
        ))
        .add_aggregate(NamedExpr::new(
            "decimal",
            Expr::sum_expr(Expr::identity(Expr::value(Decimal::new(1, 9)))),
        ));
    let result = rows(query, context).await?;
    assert_eq!(result.len(), 1);
    assert_eq!(
        object(&result[0]).get("names").and_then(Value::try_i64),
        Some(2)
    );
    assert_eq!(
        object(&result[0]).get("decimal"),
        Some(&Value::Decimal(Decimal::new(2, 9)))
    );
    evidence.push(serde_json::json!({"case":"expression-transforms","row":json(&result[0])}));

    for function in [
        F::Count,
        F::CountDistinct,
        F::Sum,
        F::Avg,
        F::Min,
        F::Max,
        F::Stddev,
        F::StddevPop,
        F::VarSamp,
        F::VarPop,
        F::BitAnd,
        F::BitOr,
        F::BitXor,
    ] {
        let mut query = source().with_student_capacity_less_than(0_i64);
        query.roll_up_to_school_type().add_aggregate(NamedExpr::new(
            "computed",
            Expr::function(function, [Expr::value(Value::Null)]),
        ));
        let result = rows(query, context).await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("schools").and_then(Value::try_i64), Some(0));
        assert_eq!(
            object(&result[0]).get("computed"),
            Some(&if matches!(function, F::Count | F::CountDistinct) {
                Value::I64(0)
            } else {
                Value::Null
            })
        );
    }
    evidence.push(serde_json::json!({"case":"expression-empty-null","checked":13}));

    let mut query = source();
    query
        .roll_up_to_school_type()
        .add_aggregate(NamedExpr::new("owners", Expr::count_all()));
    let result = rows(query, context).await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("schools").and_then(Value::try_i64), Some(5));
    assert_eq!(
        object(&result[0]).get("owners").and_then(Value::try_i64),
        Some(2)
    );
    evidence.push(serde_json::json!({"case":"expression-parent-only","row":json(&result[0])}));
    println!(
        "GENERATED_EXPRESSION_MEASURES={}",
        serde_json::json!({"passed":true,"generatedQ":true,"caseCount":evidence.len(),"observations":evidence})
    );
    Ok(())
}
