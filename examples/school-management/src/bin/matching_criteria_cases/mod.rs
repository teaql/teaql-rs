use school_management_service_core::{Q, SchoolTypeRequest, TeaqlRuntime};
use teaql_core::{Expr, Value};
type Error = Box<dyn std::error::Error>;

async fn count(request: SchoolTypeRequest, context: &impl TeaqlRuntime) -> Result<i64, Error> {
    let rows = request.count_as("types").limit(20)
        .comment("Count types using generated criteria alternatives")
        .purpose("Compare Java and Rust matching semantics without entity-ID substitution")
        .execute_for_rows(context).await?;
    Ok(rows.first().and_then(|row| row.get("types")).and_then(Value::try_i64).ok_or("missing type count")?)
}

pub async fn verify(context: &impl TeaqlRuntime) -> Result<(), Error> {
    let mut evidence = Vec::new();
    let source = Q::school_types_minimal().with_name_is("Primary").with_code_is("SECONDARY");
    let matched = count(Q::school_types_minimal().matching_any_of(source.clone()), context).await?;
    assert_eq!(matched, 2, "the supplied criteria are alternatives, not an ID subquery with AND criteria");
    evidence.push(serde_json::json!({"case":"matching-basic-or","types":matched}));
    let matched = count(Q::school_types_minimal().with_id_is(1001_u64).matching_any_of(source), context).await?;
    assert_eq!(matched, 1);
    evidence.push(serde_json::json!({"case":"matching-retains-receiver","types":matched}));
    let matched = count(Q::school_types_minimal().matching_any_of(Q::school_types_minimal().with_id_is(1002_u64)), context).await?;
    assert_eq!(matched, 1);
    evidence.push(serde_json::json!({"case":"matching-single","types":matched}));
    let matched = count(Q::school_types_minimal().with_id_is(1001_u64).matching_any_of(Q::school_types_minimal()), context).await?;
    assert_eq!(matched, 1);
    evidence.push(serde_json::json!({"case":"matching-empty-no-op","types":matched}));
    let matched = count(Q::school_types_minimal().matching_any_of(Q::school_types_minimal().with_id_is(1001_u64)
        .group_by_id().count_as("unused").having(Expr::gt("unused", 99)).limit(0)), context).await?;
    assert_eq!(matched, 1, "matching alternatives copy criteria, not a statistical page");
    evidence.push(serde_json::json!({"case":"matching-ignores-result-configuration","types":matched}));
    let matched = count(Q::school_types_minimal().matching_any_of(Q::school_types_minimal()
        .deleted_rows_only().with_name_is("Primary")), context).await?;
    assert_eq!(matched, 1);
    evidence.push(serde_json::json!({"case":"matching-ignores-source-version","types":matched}));
    let matched = count(Q::school_types_minimal().with_deleted_rows().matching_any_of(Q::school_types_minimal()
        .with_name_is("Primary").with_code_is("SECONDARY")), context).await?;
    assert_eq!(matched, 2);
    evidence.push(serde_json::json!({"case":"matching-receiver-without-guard","types":matched}));
    let matched = count(Q::school_types_minimal().matching_ids_of(Q::school_types_minimal()
        .group_by_id().count_as("types").having(Expr::gt("types",99)).limit(1)), context).await?;
    assert_eq!(matched, 0);
    evidence.push(serde_json::json!({"case":"matching-ids-statistical","types":matched}));
    let mut source = Q::school_types_minimal().group_by_id().count_as("types").limit(20);
    source.roll_up_to_platform().group_by_name().having(Expr::eq("name","NO MATCHING PLATFORM CELL"));
    let matched = count(Q::school_types_minimal().matching_ids_of(source),context).await?;
    assert_eq!(matched,0);
    evidence.push(serde_json::json!({"case":"matching-ids-layered","types":matched}));
    let source = Q::school_types_minimal().with_platform_matching(Q::platforms_minimal()
        .with_name_is("NO MATCHING PLATFORM CELL").comment("Select an empty independent platform branch"))
        .with_code_is("SECONDARY");
    let matched = count(Q::school_types_minimal().matching_any_of(source),context).await?;
    assert_eq!(matched,1);
    evidence.push(serde_json::json!({"case":"matching-nested-request-branch","types":matched}));
    println!("GENERATED_MATCHING_CRITERIA={}", serde_json::json!({"passed":true,"generatedQ":true,
        "caseCount":evidence.len(),"observations":evidence}));
    Ok(())
}
