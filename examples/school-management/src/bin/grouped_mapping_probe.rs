//! Generated Q regression: logical FK keys and grouped relation details.
use school_management_service_core::{
    Q, ServiceRuntimeConfig, request_support::AuditedSave as _, service_runtime,
};
use teaql_core::{Entity as _, Value};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::var("TEAQL_GROUPED_MAPPING_DATABASE")?;
    let round = std::env::var("TEAQL_GROUPED_MAPPING_ROUND")?.parse::<u32>()?;
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: database,
    })
    .await?;
    context.ensure_schema().await?;
    let names = [
        format!("Grouped mapping {round} A"),
        format!("Grouped mapping {round} B"),
    ];
    for name in &names {
        let mut school = Q::schools()
            .comment("Prepare grouped mapping example")
            .purpose("Verify generated Q mapping against SQLite")
            .new_entity(&context);
        school.update_platform_id(1_u64);
        school.update_school_type_to_primary();
        school.update_name(name.clone());
        school.update_address("Mapping example address");
        school.update_established_date(Value::Date("1995-09-01".parse()?));
        school.update_student_capacity(800_i64);
        school.update_active(true);
        let now = context.fix_time();
        school.update_create_time(now);
        school.update_update_time(now);
        school
            .audit_as("Create isolated grouped mapping rows")
            .save(&context)
            .await?;
    }
    let mut observed = Vec::new();
    macro_rules! query {
        ($label:literal, $request:expr) => {{
            let rows = $request.with_name_in(names.clone()).limit(10).comment($label)
                .purpose("Verify logical grouped results without changing storage columns")
                .execute_for_rows(&context).await?;
            assert_eq!(rows.len(), 1, $label);
            let row = rows.into_iter().next().unwrap();
            observed.push(serde_json::json!({"case": $label, "row": row.iter()
                .map(|(name,value)| (name.clone(),value.to_json_value()))
                .collect::<std::collections::BTreeMap<_,_>>()}));
            row
        }};
    }
    let key = query!(
        "foreign-key-group",
        Q::schools().group_by_school_type().count_as("n")
    );
    assert_eq!(
        key.get("school_type_id").and_then(Value::try_u64),
        Some(1001)
    );
    assert_eq!(key.get("n").and_then(Value::try_u64), Some(2));
    assert!(!key.contains_key("school_type"));
    let alias = query!(
        "foreign-key-alias",
        Q::schools()
            .group_by_school_type_as("kindKey")
            .count_as("n")
    );
    assert_eq!(
        alias.get("school_type_id").and_then(Value::try_u64),
        Some(1001)
    );
    assert_eq!(alias.get("kindKey").and_then(Value::try_u64), Some(1001));
    for row in [
        query!(
            "grouped-details",
            Q::schools()
                .group_by_school_type_with_details()
                .count_as("n")
        ),
        query!(
            "roll-up-details",
            Q::schools().roll_up_to_school_type().count_as("n")
        ),
    ] {
        assert_eq!(
            row.get("school_type_id").and_then(Value::try_u64),
            Some(1001)
        );
        let Some(Value::Object(kind)) = row.get("school_type") else {
            panic!("expected SchoolType object: {row:?}");
        };
        assert_eq!(kind.get("id").and_then(Value::try_u64), Some(1001));
        assert_eq!(kind.get("code"), Some(&Value::from("PRIMARY")));
        assert_eq!(row.get("n").and_then(Value::try_u64), Some(2));
    }
    let dimensions = query!(
        "group-without-aggregate",
        Q::schools().group_by_school_type()
    );
    assert_eq!(dimensions.len(), 1);
    assert_eq!(
        dimensions.get("school_type_id").and_then(Value::try_u64),
        Some(1001)
    );
    println!(
        "RUST_GROUPED_MAPPING={}",
        serde_json::json!({"passed":true,"round":round,
        "originalGeneratedLibrary":true,"observations":observed})
    );
    Ok(())
}
