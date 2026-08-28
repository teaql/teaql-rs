use school_management_service_core::{
    request_support::AuditedSave as _, service_runtime, teaql_core::Entity as _,
    ServiceRuntimeConfig, Q,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::temp_dir().join(format!(
        "teaql-school-rust-{}.sqlite",
        std::process::id()
    ));
    if database.exists() {
        std::fs::remove_file(&database)?;
    }
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: database.to_string_lossy().into_owned(),
    })
    .await?;
    context.ensure_schema().await?;
    context.ensure_schema().await?;

    let platforms = Q::platforms()
        .comment("verify seeded Platform root")
        .purpose("local runtime verification")
        .execute_for_list(&context)
        .await?;
    let constants = Q::school_types()
        .order_by_id_asc()
        .comment("verify seeded SchoolType constants")
        .purpose("local runtime verification")
        .execute_for_list(&context)
        .await?;
    assert_eq!(platforms.len(), 1);
    assert_eq!(platforms[0].id(), 1);
    assert_eq!(constants.len(), 2);
    assert_eq!((constants[0].id(), constants[0].version()), (1001, 1));
    assert_eq!((constants[1].id(), constants[1].version()), (1002, 1));

    let mut school = Q::schools()
        .comment("Create the deterministic School query fixture")
        .purpose("Initialize the shared School Query conformance cases")
        .new_entity(&context);
    school.update_platform_id(1_u64);
    school.update_school_type_to_primary();
    school.update_name("Riverside Primary School");
    school.update_address("12 River Road, Springfield");
    school.update_established_date(school_management_service_core::teaql_core::Value::Date(
        "1995-09-01".parse()?,
    ));
    school.update_student_capacity(800_i64);
    school.update_active(true);
    let now = school_management_service_core::teaql_core::time::Timestamp::now();
    school.update_create_time(now);
    school.update_update_time(now);
    let _school = school
        .audit_as("Create the School Query conformance fixture")
        .save(&context)
        .await?;

    macro_rules! assert_query {
        ($label:expr, $request:expr, $expected:expr) => {{
            let rows = $request
                .comment(format!("Query parity: {}", $label))
                .purpose("Execute the shared School Query conformance case")
                .execute_for_list(&context)
                .await?;
            assert_eq!(rows.len(), $expected, "{}", $label);
        }};
    }

    assert_query!("string equality", Q::schools().with_name_is("Riverside Primary School"), 1);
    assert_query!("string inequality", Q::schools().with_name_is_not("Another School"), 1);
    assert_query!("string membership", Q::schools().with_name_in(["Riverside Primary School", "Another School"]), 1);
    assert_query!("negative membership", Q::schools().with_name_not_in(["Another School"]), 1);
    assert_query!("contains", Q::schools().with_name_containing("Primary"), 1);
    assert_query!("negative contains", Q::schools().with_name_not_containing("Secondary"), 1);
    assert_query!("starts with", Q::schools().with_name_starting_with("Riverside"), 1);
    assert_query!("negative starts with", Q::schools().with_name_not_starting_with("Lakeside"), 1);
    assert_query!("ends with", Q::schools().with_name_ending_with("School"), 1);
    assert_query!("negative ends with", Q::schools().with_name_not_ending_with("Academy"), 1);
    assert_query!("number range", Q::schools().with_student_capacity_between(700_i64, 900_i64), 1);
    assert_query!("strict comparison", Q::schools().with_student_capacity_greater_than(799_i64).with_student_capacity_less_than(801_i64), 1);
    assert_query!(
        "date range",
        Q::schools().with_established_date_between(
            school_management_service_core::teaql_core::Value::Date("1995-01-01".parse()?),
            school_management_service_core::teaql_core::Value::Date("1995-12-31".parse()?),
        ),
        1
    );
    assert_query!("known", Q::schools().with_address_is_known(), 1);
    assert_query!("unknown", Q::schools().with_address_is_unknown(), 0);
    assert_query!("boolean", Q::schools().which_are_active(), 1);
    assert_query!("constant relation", Q::schools().with_school_type_is_primary(), 1);

    let projected = Q::schools()
        .select_name()
        .order_by_id_desc()
        .comment("Query parity: projection and ordering")
        .purpose("Execute the shared School Query conformance case")
        .execute_for_list(&context)
        .await?;
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].name(), "Riverside Primary School");
    println!("PASS Rust School bootstrap and portable Query parity");
    std::fs::remove_file(database)?;
    Ok(())
}
