//! Use current field Assist and generated Q APIs; never patch generated source.
mod expression_measure_cases;
mod reverse_rollup_cases;
mod rollup_trace_cases;
mod matching_criteria_cases;
use school_management_service_core::{E, request_support::AuditedSave as _};
use school_management_service_core::{
    Q, SchoolRequest, ServiceRuntimeConfig, TeaqlRuntime, service_runtime,
};
use teaql_core::{CompactRow, Entity as _, Expr, ExprFunction, Value};

fn source() -> SchoolRequest {
    Q::schools_minimal()
        .with_student_capacity_greater_than(0_i64)
        .count_as("schools")
        .sum_student_capacity_as("capacity")
        .avg_student_capacity_as("mean")
}

async fn rows(
    request: SchoolRequest,
    context: &impl TeaqlRuntime,
    comment: &str,
) -> Result<Vec<CompactRow>, Box<dyn std::error::Error>> {
    Ok(request
        .limit(20)
        .comment(comment)
        .purpose("Verify generated layered roll-up over legacy metadata")
        .execute_for_rows(context)
        .await?
        .into_iter()
        .collect())
}

fn object<'a>(row: &'a CompactRow, key: &str) -> &'a std::collections::BTreeMap<String, Value> {
    let Some(Value::Object(value)) = row.get(key) else {
        panic!("missing {key}: {row:?}");
    };
    value
}

fn record(label: &str, row: &CompactRow) -> serde_json::Value {
    serde_json::json!({"case":label,"row":row.iter().map(|(key,value)| (key.clone(),value.to_json_value()))
        .collect::<std::collections::BTreeMap<_,_>>()})
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_GROUPED_MAPPING_DATABASE")?,
    })
    .await?;
    context.ensure_schema().await?;
    let existing = Q::schools()
        .with_name_is("Mapping API ordering secondary")
        .limit(1)
        .comment("Find the native secondary statistics observation")
        .purpose("Reuse the same five-school population without cleanup")
        .execute_for_first(&context)
        .await?;
    if existing.is_none() {
        let mut school = Q::schools()
            .comment("Prepare the secondary school observation")
            .purpose("Complete the four-primary one-secondary statistics population")
            .new_entity(&context);
        school.update_platform_id(1_u64);
        school.update_school_type_to_secondary();
        school.update_name("Mapping API ordering secondary");
        school.update_address("Mapping ordering address");
        school.update_established_date(Value::Date("1995-09-01".parse()?));
        school.update_student_capacity(100_i64);
        school.update_active(true);
        school.update_create_time(context.fix_time());
        school.update_update_time(context.fix_time());
        let saved = school
            .audit_as("Seed the native generated statistics fixture")
            .save(&context)
            .await?;
        assert_eq!(E::school(&saved).get_student_capacity().eval(), Some(100));
        assert_eq!(E::school(&saved).get_school_type_id().eval(), Some(1002));
    } else {
        let school = existing.as_ref().unwrap();
        assert_eq!(E::school(school).get_student_capacity().eval(), Some(100));
        assert_eq!(E::school(school).get_school_type_id().eval(), Some(1002));
        assert_eq!(school.version(), 1);
    }
    rollup_trace_cases::verify(&mut context).await?;
    matching_criteria_cases::verify(&context).await?;
    let mut evidence = Vec::new();

    let mut request = source();
    request
        .roll_up_to_school_type()
        .group_by_platform()
        .count_as("types")
        .sum_display_order_as("displayOrderTotal");
    let result = rows(
        request,
        &context,
        "Merge school types at the shared platform dimension",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(5));
    assert_eq!(row.get("capacity").and_then(Value::try_i64), Some(3300));
    assert_eq!(row.get("mean").and_then(Value::try_f64), Some(660.0));
    let kind = object(row, "school_type");
    assert_eq!(kind.get("platform_id").and_then(Value::try_i64), Some(1));
    assert_eq!(kind.get("types").and_then(Value::try_i64), Some(2));
    assert_eq!(
        kind.get("displayOrderTotal").and_then(Value::try_i64),
        Some(3)
    );
    assert!(!row.contains_key("school_type_id"));
    evidence.push(record("parent-owner-grain", row));

    let mut request = source();
    let mut kind = request
        .roll_up_to_school_type()
        .count_as("types")
        .sum_display_order_as("displayOrderTotal");
    kind.roll_up_to_platform().group_by_name();
    let result = rows(
        request,
        &context,
        "Configure two typed parent levels and execute the source statistics",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(5));
    let kind = object(row, "school_type");
    let Some(Value::Object(platform)) = kind.get("platform") else {
        panic!("missing platform grouping");
    };
    assert_eq!(
        platform.get("name"),
        Some(&Value::from("Campus Learning Platform"))
    );
    assert_eq!(kind.get("types").and_then(Value::try_i64), Some(2));
    evidence.push(record("two-hop-native-navigation", row));

    let mut request = source();
    let mut kind = request
        .roll_up_to_school_type()
        .group_by_platform()
        .count_as("types");
    let frozen = request.clone();
    kind = kind.with_code_is("PRIMARY");
    let result = rows(
        request,
        &context,
        "Observe a late filter on a grouped parent",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(4));
    assert_eq!(
        object(row, "school_type")
            .get("types")
            .and_then(Value::try_i64),
        Some(1)
    );
    assert_eq!(
        rows(frozen, &context, "Keep a grouped-parent clone independent").await?[0]
            .get("schools")
            .and_then(Value::try_i64),
        Some(5)
    );
    assert_eq!(kind.query().entity, "SchoolType");
    evidence.push(record("late-grouped-filter-and-clone", row));

    let mut request = source();
    request
        .roll_up_to_school_type()
        .group_by_display_order()
        .order_by_display_order_desc()
        .limit(1);
    let result = rows(request, &context, "Apply the parent page after grouping").await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(1));
    assert_eq!(row.get("capacity").and_then(Value::try_i64), Some(100));
    evidence.push(record("parent-cell-page", row));

    let mut request = Q::schools_minimal();
    request.roll_up_to_school_type().group_by_platform();
    let result = rows(request, &context, "Return only final grouped dimensions").await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.len(), 1);
    assert_eq!(
        object(row, "school_type")
            .get("platform_id")
            .and_then(Value::try_i64),
        Some(1)
    );
    evidence.push(record("group-only", row));

    let mut request = source();
    let mut kind = request.roll_up_to_school_type();
    kind.roll_up_to_platform()
        .group_by_name()
        .with_name_is("a platform outside this population");
    assert!(
        rows(
            request,
            &context,
            "Preserve a late grandparent eligibility filter"
        )
        .await?
        .is_empty()
    );
    evidence.push(serde_json::json!({"case":"grandparent-filter","rows":0}));

    let mut request = source().group_by_active().group_by_student_capacity();
    request
        .roll_up_to_school_type()
        .group_by_platform()
        .count_as("types");
    let result = rows(
        request,
        &context,
        "Retain every source dimension while rolling up the parent",
    )
    .await?;
    assert_eq!(result.len(), 2);
    assert_eq!(
        result
            .iter()
            .map(|row| row.get("schools").and_then(Value::try_i64).unwrap())
            .sum::<i64>(),
        5
    );
    for row in &result {
        assert_eq!(row.get("active").and_then(Value::try_bool), Some(true));
        assert_eq!(
            object(row, "school_type")
                .get("types")
                .and_then(Value::try_i64),
            Some(1)
        );
        assert!(matches!(
            row.get("student_capacity").and_then(Value::try_i64),
            Some(100 | 800)
        ));
    }
    evidence.push(serde_json::json!({"case":"multiple-dimensions","rows":result.iter().map(|row| record("cell", row)["row"].clone()).collect::<Vec<_>>()}));

    let mut request = source();
    request
        .roll_up_to_school_type()
        .group_by_platform()
        .count_as("types");
    request
        .roll_up_to_platform()
        .group_by_name()
        .count_as("platforms");
    let result = rows(
        request,
        &context,
        "Combine independent parent paths without multiplying observations",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(5));
    assert_eq!(row.get("capacity").and_then(Value::try_i64), Some(3300));
    assert_eq!(
        object(row, "school_type")
            .get("types")
            .and_then(Value::try_i64),
        Some(2)
    );
    assert_eq!(
        object(row, "platform")
            .get("platforms")
            .and_then(Value::try_i64),
        Some(1)
    );
    evidence.push(record("branching-parent-dimensions", row));

    let mut request = source().having(Expr::gt("schools", 4_i64));
    let mut kind = request
        .roll_up_to_school_type()
        .group_by_platform()
        .count_as("types")
        .having(Expr::eq("types", 2_i64));
    let result = rows(
        request.clone(),
        &context,
        "Apply source and parent statistical result conditions",
    )
    .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("schools").and_then(Value::try_i64), Some(5));
    kind = kind.having(Expr::lt("types", 2_i64));
    assert!(
        rows(
            request,
            &context,
            "Keep repeated parent HAVING conditions conjunctive"
        )
        .await?
        .is_empty()
    );
    assert_eq!(kind.query().entity, "SchoolType");
    evidence.push(record("parent-and-root-having", &result[0]));

    let mut request = source().group_by_student_capacity_as("bucket");
    request
        .roll_up_to_school_type()
        .group_by_platform()
        .count_as("types");
    let result = rows(
        request.having(Expr::gte("bucket", 800_i64)),
        &context,
        "Carry a named grouping dimension through layered statistics",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("bucket").and_then(Value::try_i64), Some(800));
    assert!(!row.contains_key("student_capacity"));
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(4));
    assert_eq!(
        object(row, "school_type")
            .get("types")
            .and_then(Value::try_i64),
        Some(1)
    );
    evidence.push(record("named-group-dimension", row));

    let request = source().having(Expr::gt("schools", 4_i64));
    let result = rows(
        request.clone(),
        &context,
        "Filter the scalar school count without a roll-up",
    )
    .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("schools").and_then(Value::try_i64), Some(5));
    assert!(
        rows(
            request.having(Expr::gt("schools", 5_i64)),
            &context,
            "Reject the scalar result using conjunctive HAVING",
        )
        .await?
        .is_empty()
    );
    evidence.push(record("ordinary-scalar-having", &result[0]));

    let result = rows(
        source()
            .group_by_student_capacity()
            .having(Expr::gt("capacity", 3000_i64)),
        &context,
        "Filter school capacity totals after grouping legacy columns",
    )
    .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].get("student_capacity").and_then(Value::try_i64),
        Some(800)
    );
    assert_eq!(
        result[0].get("capacity").and_then(Value::try_i64),
        Some(3200)
    );
    evidence.push(record("ordinary-grouped-having", &result[0]));

    let result = rows(
        source()
            .group_by_student_capacity_as("bucket")
            .having(Expr::gte("bucket", 800_i64)),
        &context,
        "Filter a named grouping dimension without a parent layer",
    )
    .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("bucket").and_then(Value::try_i64), Some(800));
    assert_eq!(result[0].get("schools").and_then(Value::try_i64), Some(4));
    assert!(!result[0].contains_key("student_capacity"));
    evidence.push(record("ordinary-named-dimension-having", &result[0]));

    let mut request = Q::schools_minimal()
        .count_as("schools")
        .standard_deviation_student_capacity_as("sampleSD")
        .square_root_of_population_standard_deviation_student_capacity_as("populationSD")
        .sample_variance_student_capacity_as("sampleVariance")
        .sample_population_variance_student_capacity_as("populationVariance");
    request
        .roll_up_to_school_type()
        .group_by_platform()
        .count_as("types");
    let result = rows(
        request,
        &context,
        "Compute extended statistics through typed field methods",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(5));
    assert_eq!(
        object(row, "school_type")
            .get("types")
            .and_then(Value::try_i64),
        Some(2)
    );
    for (alias, expected) in [
        ("sampleSD", 98000_f64.sqrt()),
        ("populationSD", 280.0),
        ("sampleVariance", 98000.0),
        ("populationVariance", 78400.0),
    ] {
        let actual = row
            .get(alias)
            .and_then(Value::try_f64)
            .expect("numeric extended statistic");
        assert!(
            (actual - expected).abs() <= 1e-10 * expected.abs().max(1.0),
            "{alias}: {actual}"
        );
    }
    evidence.push(record("typed-extended-statistics", row));

    let mut request = Q::schools_minimal()
        .count_as("schools")
        .count_distinct_active_as("activeValues")
        .count_distinct_student_capacity_as("capacities")
        .count_distinct_school_type();
    request
        .roll_up_to_school_type()
        .group_by_platform()
        .count_as("types")
        .count_distinct_platform_as("platformReferences");
    let result = rows(
        request.clone().having(Expr::eq("activeValues", 1)),
        &context,
        "Count distinct values at the final source and parent grain",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    for (alias, expected) in [
        ("schools", 5),
        ("activeValues", 1),
        ("capacities", 2),
        ("school_type_distinct_count", 2),
    ] {
        assert_eq!(row.get(alias).and_then(Value::try_i64), Some(expected));
    }
    assert_eq!(
        object(row, "school_type")
            .get("types")
            .and_then(Value::try_i64),
        Some(2)
    );
    assert_eq!(
        object(row, "school_type")
            .get("platformReferences")
            .and_then(Value::try_i64),
        Some(1)
    );
    assert!(
        rows(
            request.having(Expr::gt("activeValues", 1)),
            &context,
            "Reject a summed intermediate distinct count"
        )
        .await?
        .is_empty()
    );
    evidence.push(record("typed-distinct-owner-grain", row));

    let result = rows(
        Q::schools_minimal()
            .count_distinct_name_as("names")
            .count_distinct_school_type_as("types")
            .count_distinct_student_capacity()
            .having(Expr::eq("names", 5)),
        &context,
        "Count distinct ordinary field values",
    )
    .await?;
    assert_eq!(result.len(), 1);
    for (alias, expected) in [
        ("names", 5),
        ("types", 2),
        ("student_capacity_distinct_count", 2),
    ] {
        assert_eq!(
            result[0].get(alias).and_then(Value::try_i64),
            Some(expected)
        );
    }
    evidence.push(record("typed-distinct-ordinary", &result[0]));

    let mut request = source();
    let mut kind = request.roll_up_to_school_type().count_as("types");
    kind.roll_up_to_platform()
        .group_by_name_with_function("phonetic", ExprFunction::Soundex)
        .count_as("platforms")
        .having(Expr::eq("phonetic", "C512"));
    let result = rows(
        request,
        &context,
        "Group the typed grandparent by a transformed name",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(5));
    assert_eq!(row.get("capacity").and_then(Value::try_i64), Some(3300));
    let kind = object(row, "school_type");
    assert_eq!(kind.get("types").and_then(Value::try_i64), Some(2));
    let Some(Value::Object(platform)) = kind.get("platform") else {
        panic!("missing platform");
    };
    assert_eq!(platform.get("phonetic"), Some(&Value::from("C512")));
    assert_eq!(platform.get("platforms").and_then(Value::try_i64), Some(1));
    assert!(!platform.contains_key("name"));
    evidence.push(record("typed-computed-grandparent", row));

    let result = rows(
        source()
            .group_by_name_with_function("phonetic", ExprFunction::Soundex)
            .having(Expr::eq("phonetic", "G613")),
        &context,
        "Merge four distinct mapping fixture names by their phonetic grouping key",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("phonetic"), Some(&Value::from("G613")));
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(4));
    assert_eq!(row.get("capacity").and_then(Value::try_i64), Some(3200));
    assert!(!row.contains_key("name"));
    evidence.push(record("typed-computed-ordinary", row));

    let result = rows(
        source()
            .group_by_student_capacity_with_function("bucket", ExprFunction::Identity)
            .having(Expr::eq("bucket", 800_i64)),
        &context,
        "Keep the typed mapped capacity key unchanged",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("bucket"), Some(&Value::I64(800)));
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(4));
    assert!(!row.contains_key("student_capacity"));
    evidence.push(record("typed-identity-ordinary", row));

    let mut request = source();
    request
        .roll_up_to_school_type()
        .group_by_display_order_with_function("rank", ExprFunction::Identity)
        .count_as("types")
        .sum_display_order_as("displayOrderTotal")
        .having(Expr::gte("rank", teaql_core::Decimal::from(1_i64)))
        .order_by_display_order_desc()
        .limit(1);
    let result = rows(
        request,
        &context,
        "Page the typed Decimal parent identity groups",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(1));
    assert_eq!(
        object(row, "school_type").get("rank"),
        Some(&Value::Decimal(teaql_core::Decimal::from(2_i64)))
    );
    assert_eq!(
        object(row, "school_type")
            .get("types")
            .and_then(Value::try_i64),
        Some(1)
    );
    assert!(!object(row, "school_type").contains_key("display_order"));
    evidence.push(record("typed-identity-parent", row));

    let mut request = source();
    let mut kind = request.roll_up_to_school_type().count_as("types");
    kind.roll_up_to_platform()
        .group_by_name_with_function("caption", ExprFunction::Identity)
        .count_as("platforms");
    let result = rows(request, &context, "Keep the typed grandparent identity key").await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(5));
    assert_eq!(
        object(row, "school_type")
            .get("types")
            .and_then(Value::try_i64),
        Some(2)
    );
    let Some(Value::Object(platform)) = object(row, "school_type").get("platform") else {
        panic!("missing platform")
    };
    assert_eq!(
        platform.get("caption"),
        Some(&Value::from("Campus Learning Platform"))
    );
    assert_eq!(platform.get("platforms").and_then(Value::try_i64), Some(1));
    assert!(!platform.contains_key("name"));
    evidence.push(record("typed-identity-grandparent", row));

    let result = rows(
        source()
            .group_by_school_type_as("kindKey")
            .order_by_school_type_asc(),
        &context,
        "Replace the ordinary FK result key with its alias",
    )
    .await?;
    assert_eq!(result.len(), 2);
    for (row, (id, n, total)) in result.iter().zip([(1001, 4, 3200), (1002, 1, 100)]) {
        assert_eq!(row.get("kindKey").and_then(Value::try_i64), Some(id));
        assert_eq!(row.get("schools").and_then(Value::try_i64), Some(n));
        assert_eq!(row.get("capacity").and_then(Value::try_i64), Some(total));
        assert!(!row.contains_key("school_type_id"));
    }
    evidence.push(serde_json::json!({"case":"alias-ordinary-reference","rows":result.iter().map(|r|record("cell",r)["row"].clone()).collect::<Vec<_>>()}));
    let mut request = source();
    request
        .roll_up_to_school_type()
        .group_by_display_order_as("rank")
        .count_as("types")
        .having(Expr::gte("rank", teaql_core::Decimal::from(1)))
        .order_by_display_order_desc()
        .limit(1);
    let result = rows(
        request,
        &context,
        "Apply the parent alias without duplicating its raw key",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(1));
    assert_eq!(
        object(row, "school_type").get("rank"),
        Some(&Value::Decimal(teaql_core::Decimal::from(2)))
    );
    assert!(!object(row, "school_type").contains_key("display_order"));
    evidence.push(record("alias-parent-cell-page", row));
    let mut request = source();
    let mut kind = request.roll_up_to_school_type().count_as("types");
    kind.roll_up_to_platform()
        .group_by_name_as("caption")
        .count_as("platforms");
    let result = rows(
        request,
        &context,
        "Replace the nested grandparent dimension key",
    )
    .await?;
    assert_eq!(result.len(), 1);
    let row = &result[0];
    assert_eq!(row.get("schools").and_then(Value::try_i64), Some(5));
    assert_eq!(
        object(row, "school_type")
            .get("types")
            .and_then(Value::try_i64),
        Some(2)
    );
    let Some(Value::Object(platform)) = object(row, "school_type").get("platform") else {
        panic!("missing platform")
    };
    assert_eq!(
        platform.get("caption"),
        Some(&Value::from("Campus Learning Platform"))
    );
    assert!(!platform.contains_key("name"));
    evidence.push(record("alias-grandparent", row));
    let mut request = source()
        .group_by_school_type_as("kindKey")
        .order_by_school_type_asc();
    request
        .roll_up_to_school_type()
        .group_by_platform()
        .count_as("types");
    let result = rows(
        request,
        &context,
        "Keep an explicitly requested FK alias beside a parent grouping",
    )
    .await?;
    assert_eq!(result.len(), 2);
    for (row, (id, n)) in result.iter().zip([(1001, 4), (1002, 1)]) {
        assert_eq!(row.get("kindKey").and_then(Value::try_i64), Some(id));
        assert_eq!(row.get("schools").and_then(Value::try_i64), Some(n));
        assert_eq!(
            object(row, "school_type")
                .get("types")
                .and_then(Value::try_i64),
            Some(1)
        );
        assert!(!row.contains_key("school_type_id"));
    }
    evidence.push(serde_json::json!({"case":"alias-reference-with-rollup","rows":result.iter().map(|r|record("cell",r)["row"].clone()).collect::<Vec<_>>()}));
    let alias = "capacity \"Key\"\\path";
    let result = rows(
        source()
            .group_by_student_capacity_as(alias)
            .having(Expr::eq(alias, 800_i64))
            .order_by_student_capacity_desc(),
        &context,
        "Preserve escaped grouping aliases without using storage names",
    )
    .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get(alias), Some(&Value::I64(800)));
    assert!(!result[0].contains_key("student_capacity"));
    evidence.push(record("alias-escaped-ordinary", &result[0]));

    let types = Q::school_types_minimal()
        .group_by_id()
        .sum_display_order_as("rankTotal")
        .having(Expr::gte("rankTotal", 2_i64))
        .comment("Select independently grouped school types by their completed rank");
    let mut request = source().with_school_type_matching(types);
    request
        .roll_up_to_school_type()
        .group_by_platform()
        .count_as("types");
    let result = rows(
        request,
        &context,
        "Use a generated statistical relation predicate",
    )
    .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("schools").and_then(Value::try_i64), Some(1));
    assert_eq!(
        result[0].get("capacity").and_then(Value::try_i64),
        Some(100)
    );
    evidence.push(record(
        "typed-independent-statistical-membership",
        &result[0],
    ));

    let mut types = Q::school_types_minimal()
        .group_by_id()
        .sum_display_order_as("rankTotal")
        .having(Expr::gte("rankTotal", 1_i64))
        .order_by_id_desc()
        .limit(1)
        .comment("Page independently grouped types after navigating to Platform");
    types.roll_up_to_platform().group_by_name();
    let mut request = source().with_school_type_matching(types.clone());
    request.roll_up_to_school_type().group_by_platform();
    let result = rows(
        request,
        &context,
        "Embed a generated roll-up request in a typed relation filter",
    )
    .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("schools").and_then(Value::try_i64), Some(1));
    assert_eq!(
        result[0].get("capacity").and_then(Value::try_i64),
        Some(100)
    );
    let negative = rows(
        source().without_school_type_matching(types),
        &context,
        "Negate independent layered membership on an ordinary statistical request",
    )
    .await?;
    assert_eq!(negative[0].get("schools").and_then(Value::try_i64), Some(4));
    assert_eq!(
        negative[0].get("capacity").and_then(Value::try_i64),
        Some(3200)
    );
    evidence.push(record("typed-independent-layered-membership", &result[0]));

    let mut original = source();
    original
        .roll_up_to_school_type()
        .group_by_platform()
        .with_id_is(1001_u64);
    let mut rebound = original.clone();
    rebound
        .roll_up_to_school_type()
        .group_by_platform()
        .with_id_is(1002_u64);
    let result = rows(
        rebound,
        &context,
        "Renavigate an independent grouped-parent copy",
    )
    .await?;
    assert_eq!(result[0].get("schools").and_then(Value::try_i64), Some(1));
    assert_eq!(
        rows(
            original,
            &context,
            "Keep the original grouped parent unchanged"
        )
        .await?[0]
            .get("schools")
            .and_then(Value::try_i64),
        Some(4)
    );
    evidence.push(record("renavigate-grouped-copy", &result[0]));

    let mut original = source();
    original.roll_up_to_school_type().with_id_is(1001_u64);
    let mut rebound = original.clone();
    rebound
        .roll_up_to_school_type()
        .group_by_platform()
        .with_id_is(1002_u64);
    let result = rows(
        rebound,
        &context,
        "Replace an entity-shaped navigation with a grouped parent",
    )
    .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("schools").and_then(Value::try_i64), Some(1));
    assert_eq!(
        rows(
            original,
            &context,
            "Do not rebind the original entity parent"
        )
        .await?[0]
            .get("schools")
            .and_then(Value::try_i64),
        Some(4)
    );
    evidence.push(record("renavigate-entity-copy", &result[0]));

    let mut original =
        source().with_school_type_matching(Q::school_types_minimal().with_id_is(1001_u64));
    original.roll_up_to_school_type().group_by_platform();
    let mut rebound = original.clone();
    rebound
        .roll_up_to_school_type()
        .group_by_platform()
        .with_id_is(1002_u64);
    let result = rows(
        rebound,
        &context,
        "Retain an independent business predicate while renavigating",
    )
    .await?;
    assert!(result.is_empty());
    assert_eq!(
        rows(
            original,
            &context,
            "Retain the original independent predicate"
        )
        .await?[0]
            .get("schools")
            .and_then(Value::try_i64),
        Some(4)
    );
    evidence.push(serde_json::json!({"case":"renavigate-keeps-independent-filter","rows":0}));

    let mut collision = source().group_by_student_capacity_as("school_type.platform_id");
    collision.roll_up_to_school_type().group_by_platform();
    let error = rows(
        collision,
        &context,
        "Reject a literal alias that shadows a nested result path",
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("statistical result path"),
        "{error}"
    );

    let error = rows(
        source().group_by_name_with_function("invalid", ExprFunction::Count),
        &context,
        "Reject aggregate functions as scalar grouping transforms",
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("scalar field transform"),
        "{error}"
    );

    evidence.extend(reverse_rollup_cases::verify(&context).await?);
    expression_measure_cases::verify(&context).await?;
    println!(
        "LAYERED_ROLLUP={}",
        serde_json::json!({"passed":true,"runtimeAbi":"7.1","javaShaped":true,
        "sourceEntity":"School","caseCount":evidence.len(),"observations":evidence})
    );
    Ok(())
}
