//! Application-owned Q/E/Mutation acceptance; no SQL or generated-source edits.
mod gbk_rollup_cases;
use school_management_service_core::{
    E, Q, SchoolRequest, SchoolTypeRequest, ServiceRuntimeConfig, TeaqlRuntime,
    request_support::AuditedSave as _, service_runtime,
};
use teaql_core::{CompactRow, Decimal, Entity as _, Expr, Value};

type Error = Box<dyn std::error::Error>;

async fn set_rank(context: &impl TeaqlRuntime, id: u64, value: Decimal) -> Result<Decimal, Error> {
    let Some(mut kind) = Q::school_types()
        .with_id_is(id)
        .limit(1)
        .comment("Load every scalar field before changing a constant presentation value")
        .purpose("Prepare an audited numeric statistical regression fixture")
        .execute_for_one(context)
        .await?
    else {
        return Err("missing seeded SchoolType".into());
    };
    let before = E::school_type(&kind)
        .get_display_order()
        .eval()
        .ok_or("missing display order")?;
    let version = E::school_type(&kind)
        .get_version()
        .eval()
        .ok_or("missing version")?;
    let code = E::school_type(&kind)
        .get_code()
        .eval()
        .ok_or("missing constant code")?;
    kind.update_display_order(value);
    kind.audit_as("Change only a mutable constant presentation value for numeric acceptance")
        .save(context)
        .await?;
    let saved = Q::school_types()
        .with_id_is(id)
        .limit(1)
        .comment("Read back the audited constant presentation change")
        .purpose("Verify persisted value and optimistic version through generated E")
        .execute_for_one(context)
        .await?
        .ok_or("missing saved SchoolType")?;
    assert_eq!(
        E::school_type(&saved).get_display_order().eval(),
        Some(value)
    );
    assert_eq!(E::school_type(&saved).get_id().eval(), Some(id));
    assert_eq!(E::school_type(&saved).get_code().eval(), Some(code));
    assert_eq!(
        E::school_type(&saved).get_version().eval(),
        Some(version + i64::from(before != value))
    );
    Ok(before)
}

async fn kind_rows(
    request: SchoolTypeRequest,
    context: &impl TeaqlRuntime,
) -> Result<Vec<CompactRow>, Error> {
    Ok(request
        .limit(20)
        .comment("Read completed exact SchoolType totals")
        .purpose("Verify generated alias sorting and exact HAVING")
        .execute_for_rows(context)
        .await?
        .into_iter()
        .collect())
}

async fn school_rows(
    request: SchoolRequest,
    context: &impl TeaqlRuntime,
) -> Result<Vec<CompactRow>, Error> {
    Ok(request
        .limit(20)
        .comment("Read source totals at the selected parent grain")
        .purpose("Verify generated numeric parent and nested result semantics")
        .execute_for_rows(context)
        .await?
        .into_iter()
        .collect())
}

fn source() -> SchoolRequest {
    Q::schools_minimal()
        .count_as("schools")
        .sum_student_capacity_as("capacity")
}

fn assert_school(row: &CompactRow) {
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(1));
    assert_eq!(row.get("capacity").and_then(Value::try_i64), Some(100));
    let Some(Value::Object(kind)) = row.get("school_type") else {
        panic!("missing type")
    };
    assert_eq!(kind.get("id").and_then(Value::try_i64), Some(1002));
    assert_eq!(
        kind.get("rankTotal"),
        Some(&Value::Decimal(Decimal::from(10)))
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Error> {
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_GROUPED_MAPPING_DATABASE")?,
    })
    .await?;
    context.ensure_schema().await?;
    let first = set_rank(&context, 1001, Decimal::from(2)).await?;
    let second = set_rank(&context, 1002, Decimal::from(10)).await?;
    assert_eq!((first, second), (Decimal::ONE, Decimal::from(2)));
    let mut cases = Vec::new();

    let result = Q::school_types_minimal()
        .group_by_id()
        .sum_display_order_as("rankTotal")
        .add_order_by_descending("rankTotal")
        .limit(1)
        .comment("Page the largest exact ordinary total")
        .purpose("Distinguish numeric order from lexical order through generated Q")
        .execute_for_rows(&context)
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("id").and_then(Value::try_i64), Some(1002));
    assert_eq!(
        result[0].get("rankTotal"),
        Some(&Value::Decimal(Decimal::from(10)))
    );
    let ascending = kind_rows(
        Q::school_types_minimal()
            .group_by_id()
            .sum_display_order_as("rankTotal")
            .add_order_by_ascending("rankTotal"),
        &context,
    )
    .await?;
    assert_eq!(ascending[0].get("id").and_then(Value::try_i64), Some(1001));
    cases.push("numeric-ordinary-order");

    for (op, threshold, expected) in [
        (teaql_core::BinaryOp::Eq, 2, 1001),
        (teaql_core::BinaryOp::Ne, 2, 1002),
        (teaql_core::BinaryOp::Gt, 2, 1002),
        (teaql_core::BinaryOp::Gte, 10, 1002),
        (teaql_core::BinaryOp::Lt, 10, 1001),
        (teaql_core::BinaryOp::Lte, 2, 1001),
    ] {
        let result = kind_rows(
            Q::school_types_minimal()
                .group_by_id()
                .sum_display_order_as("rankTotal")
                .having(Expr::binary(
                    Expr::column("rankTotal"),
                    op,
                    Expr::value(threshold),
                )),
            &context,
        )
        .await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("id").and_then(Value::try_i64), Some(expected));
    }
    for condition in [
        Expr::between("rankTotal", 2, 2),
        Expr::in_list("rankTotal", [Value::I64(2)]),
    ] {
        let result = kind_rows(
            Q::school_types_minimal()
                .group_by_id()
                .sum_display_order_as("rankTotal")
                .having(condition),
            &context,
        )
        .await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("id").and_then(Value::try_i64), Some(1001));
    }
    cases.push("numeric-ordinary-having");

    let mut request = source();
    request
        .roll_up_to_school_type()
        .group_by_id()
        .sum_display_order_as("rankTotal")
        .add_order_by_descending("rankTotal")
        .limit(1);
    let result = school_rows(request, &context).await?;
    assert_eq!(result.len(), 1);
    assert_school(&result[0]);
    cases.push("numeric-parent-page");

    let mut request = source();
    request
        .roll_up_to_school_type()
        .group_by_id()
        .sum_display_order_as("rankTotal");
    let request = request
        .add_order_by_descending("school_type.rankTotal")
        .limit(1);
    // Keep the caller's page: do not replace it with the helper's list ceiling.
    let result = request
        .comment("Page the final nested numeric result")
        .purpose("Verify generated final-cell ordering")
        .execute_for_rows(&context)
        .await?;
    assert_eq!(result.len(), 1);
    assert_school(&result[0]);
    cases.push("numeric-nested-page");

    let mut request = source();
    request
        .roll_up_to_school_type()
        .group_by_id()
        .sum_display_order_as("rankTotal")
        .having(Expr::gt("rankTotal", 2));
    let result = school_rows(request, &context).await?;
    assert_eq!(result.len(), 1);
    assert_school(&result[0]);
    cases.push("numeric-parent-having");

    let mut request = source();
    request
        .roll_up_to_school_type()
        .group_by_id()
        .sum_display_order_as("rankTotal");
    let result = school_rows(
        request.having(Expr::gt("school_type.rankTotal", 2)),
        &context,
    )
    .await?;
    assert_eq!(result.len(), 1);
    assert_school(&result[0]);
    cases.push("numeric-nested-having");

    let mut invalid = source();
    invalid
        .roll_up_to_school_type()
        .group_by_id()
        .sum_display_order_as("rankTotal");
    let error = school_rows(invalid.add_order_by_descending("not_selected"), &context)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not a selected statistical result"),
        "{error}"
    );
    cases.push("numeric-unselected-order-rejected");

    for key in ["name", "code"] {
        let error = kind_rows(
            Q::school_types_minimal()
                .group_by_id()
                .sum_display_order_as("rankTotal")
                .add_order_by_descending(key),
            &context,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("not a selected statistical result"),
            "{error}"
        );
    }
    let error = kind_rows(
        Q::school_types_minimal()
            .group_by_display_order_as("a")
            .group_by_display_order_as("b")
            .count_as("types")
            .order_by_display_order_desc(),
        &context,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("ambiguous statistical order"),
        "{error}"
    );
    cases.push("numeric-ordinary-order-rejected");

    set_rank(&context, 1001, first).await?;
    set_rank(&context, 1002, second).await?;
    gbk_rollup_cases::verify(&context, false).await?;
    println!(
        "GENERATED_NUMERIC_ROLLUP={}",
        serde_json::json!({
            "passed":true,"caseCount":cases.len(),"cases":cases,"generatedQAndE":true,
            "auditedMutation":true,"restoredRanks":["1","2"],"numericRanks":["2","10"]
        })
    );
    Ok(())
}
