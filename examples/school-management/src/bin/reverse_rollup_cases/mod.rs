//! Reverse-relation membership keeps the child's complete statistical request.
//! APIs come from current school_type.school_list and school.platform Assist.
use school_management_service_core::{Q, SchoolRequest, SchoolTypeRequest, TeaqlRuntime};
use teaql_core::{Expr, Value};

fn children(visible: bool) -> SchoolRequest {
    let mut request = Q::schools_minimal()
        .group_by_school_type()
        .count_as("schools")
        .sum_student_capacity_as("capacity")
        .limit(20)
        .comment("Select completed school cells for reverse membership");
    request
        .roll_up_to_platform()
        .group_by_name()
        .having(Expr::eq(
            "name",
            if visible {
                "Campus Learning Platform"
            } else {
                "NO MATCHING PLATFORM CELL"
            },
        ));
    request
}

async fn count(
    request: SchoolTypeRequest,
    context: &impl TeaqlRuntime,
) -> Result<i64, Box<dyn std::error::Error>> {
    let result = request
        .count_as("matchedTypes")
        .limit(20)
        .comment("Count school types using reverse statistical membership")
        .purpose("Preserve child roll-up eligibility in generated predicates")
        .execute_for_rows(context)
        .await?;
    assert_eq!(result.len(), 1);
    Ok(result[0]
        .get("matchedTypes")
        .and_then(Value::try_i64)
        .ok_or("missing reverse membership count")?)
}

pub async fn verify(
    context: &impl TeaqlRuntime,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let mut evidence = Vec::new();
    let included = count(
        Q::school_types_minimal().with_school_list_matching(children(false)),
        context,
    )
    .await?;
    assert_eq!(
        included, 0,
        "a rejected parent cell must exclude all child membership"
    );
    evidence.push(serde_json::json!({"case":"reverse-layered-empty-membership","types":included}));

    let excluded = count(
        Q::school_types_minimal().without_school_list_matching(children(false)),
        context,
    )
    .await?;
    assert_eq!(
        excluded, 2,
        "NOT IN must retain both types when no completed child cell qualifies"
    );
    evidence.push(serde_json::json!({"case":"reverse-layered-empty-exclusion","types":excluded}));

    let qualifying = children(true).having(Expr::gte("capacity", 3000_i64));
    let primary = count(
        Q::school_types_minimal()
            .with_id_is(1001_u64)
            .with_school_list_matching(qualifying.clone()),
        context,
    )
    .await?;
    let secondary = count(
        Q::school_types_minimal()
            .with_id_is(1002_u64)
            .without_school_list_matching(qualifying),
        context,
    )
    .await?;
    assert_eq!((primary, secondary), (1, 1));
    evidence.push(serde_json::json!({"case":"reverse-layered-having","primary":primary,"secondary":secondary}));

    let paged = children(true).order_by_school_type_desc().limit(1);
    let secondary = count(
        Q::school_types_minimal()
            .with_id_is(1002_u64)
            .with_school_list_matching(paged.clone()),
        context,
    )
    .await?;
    let primary = count(
        Q::school_types_minimal()
            .with_id_is(1001_u64)
            .without_school_list_matching(paged),
        context,
    )
    .await?;
    assert_eq!((primary, secondary), (1, 1));
    evidence.push(serde_json::json!({"case":"reverse-layered-cell-page","primary":primary,"secondary":secondary}));
    let mut types = Q::school_types_minimal()
        .group_by_id()
        .count_as("parents")
        .limit(20)
        .comment("Select type identities from a complete statistical request");
    types
        .roll_up_to_platform()
        .group_by_name()
        .having(Expr::eq("name", "NO MATCHING PLATFORM CELL"));
    let matched = count(Q::school_types_minimal().matching_any_of(types), context).await?;
    assert_eq!(
        matched, 0,
        "same-entity membership must not discard the parent result filter"
    );
    evidence.push(serde_json::json!({"case":"same-entity-layered-membership","types":matched}));
    Ok(evidence)
}
