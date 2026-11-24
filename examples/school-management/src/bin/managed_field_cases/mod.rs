//! Execute the exact ID/version operations advertised by current field Assist.
use school_management_service_core::{Q, SchoolTypeRequest, TeaqlRuntime};
use teaql_core::{Value, request::FieldOperator};
type Error = Box<dyn std::error::Error>;

async fn count(request: SchoolTypeRequest, context: &impl TeaqlRuntime) -> Result<i64, Error> {
    let rows = request.count_as("types").limit(20)
        .comment("Count constants using documented managed-field APIs")
        .purpose("Verify the generated ID and version field Assist against actual execution")
        .execute_for_rows(context).await?;
    Ok(rows.first().and_then(|row| row.get("types")).and_then(Value::try_i64).ok_or("missing count")?)
}

pub async fn verify(context: &impl TeaqlRuntime) -> Result<(), Error> {
    let mut observations = Vec::new();
    let n = count(Q::school_types_minimal()
        .with_id(FieldOperator::GreaterThan, [1000_u64])
        .with_id(FieldOperator::LessThan, [1002_u64])
        .with_id(FieldOperator::Between, [1001_u64, 1002_u64]), context).await?;
    assert_eq!(n, 1);
    observations.push(serde_json::json!({"case":"id-operator-comparisons","count":n}));
    let n = count(Q::school_types_minimal().with_id_in([1001_u64, 1002_u64])
        .with_id_not_in([1001_u64]).with_id_is_not(1001_u64), context).await?;
    assert_eq!(n, 1);
    observations.push(serde_json::json!({"case":"id-set-filters","count":n}));
    let rows = Q::school_types_minimal().select_name().order_by_id_asc().limit(20)
        .comment("Load minimal constants with mandatory identity and version")
        .purpose("Verify that managed fields require no select helper")
        .execute_for_list(context).await?;
    assert_eq!(rows.iter().map(|row| row.id()).collect::<Vec<_>>(), [1001, 1002]);
    assert!(rows.iter().all(|row| row.version() > 0));
    observations.push(serde_json::json!({"case":"automatic-identity-projection","count":rows.len()}));
    let rows = Q::school_types_minimal().group_by_id_as("typeKey").group_by_version()
        .count_as("types").order_by_id_asc().limit(20)
        .comment("Group constants by managed identity and version")
        .purpose("Verify managed-field statistics through generated Q")
        .execute_for_rows(context).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.iter().map(|row| row.get("typeKey").and_then(Value::try_u64)).collect::<Vec<_>>(),
        [Some(1001), Some(1002)]);
    assert!(rows.iter().all(|row| row.get("types").and_then(Value::try_i64) == Some(1)
        && row.get("version").and_then(Value::try_i64).is_some_and(|version| version > 0)));
    observations.push(serde_json::json!({"case":"managed-field-grouping","count":rows.len()}));
    assert_eq!(count(Q::school_types_minimal().with_deleted_rows(), context).await?, 2);
    let n = count(Q::school_types_minimal().deleted_rows_only(), context).await?;
    assert_eq!(n, 0);
    observations.push(serde_json::json!({"case":"version-row-visibility","count":n}));
    println!("GENERATED_MANAGED_FIELDS={}", serde_json::json!({"passed":true,"generatedQ":true,
        "caseCount":observations.len(),"observations":observations}));
    Ok(())
}
