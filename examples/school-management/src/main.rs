use school_management_service_core::{
    request_support::AuditedSave as _, service_runtime, teaql_core::Entity as _,
    ServiceRuntimeConfig, Q,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database =
        std::env::temp_dir().join(format!("teaql-school-rust-{}.sqlite", std::process::id()));
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

    assert_query!(
        "string equality",
        Q::schools().with_name_is("Riverside Primary School"),
        1
    );
    assert_query!(
        "string inequality",
        Q::schools().with_name_is_not("Another School"),
        1
    );
    assert_query!(
        "string membership",
        Q::schools().with_name_in(["Riverside Primary School", "Another School"]),
        1
    );
    assert_query!(
        "negative membership",
        Q::schools().with_name_not_in(["Another School"]),
        1
    );
    assert_query!("contains", Q::schools().with_name_containing("Primary"), 1);
    assert_query!(
        "negative contains",
        Q::schools().with_name_not_containing("Secondary"),
        1
    );
    assert_query!(
        "starts with",
        Q::schools().with_name_starting_with("Riverside"),
        1
    );
    assert_query!(
        "negative starts with",
        Q::schools().with_name_not_starting_with("Lakeside"),
        1
    );
    assert_query!("ends with", Q::schools().with_name_ending_with("School"), 1);
    assert_query!(
        "negative ends with",
        Q::schools().with_name_not_ending_with("Academy"),
        1
    );
    assert_query!(
        "number range",
        Q::schools().with_student_capacity_between(700_i64, 900_i64),
        1
    );
    assert_query!(
        "strict comparison",
        Q::schools()
            .with_student_capacity_greater_than(799_i64)
            .with_student_capacity_less_than(801_i64),
        1
    );
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
    assert_query!("boolean true", Q::schools().which_are_active(), 1);
    assert_query!("boolean false", Q::schools().which_are_not_active(), 0);
    assert_query!(
        "constant relation",
        Q::schools().with_school_type_is_primary(),
        1
    );

    let related = Q::schools()
        .with_name_is("Riverside Primary School")
        .select_platform_with(Q::platforms_minimal().select_name().select_base_url())
        .select_school_type_with(Q::school_types_minimal().select_name().select_code())
        .comment("Query parity: typed forward relations")
        .purpose("Execute the shared School Query conformance case")
        .execute_for_one(&context)
        .await?
        .expect("School must exist");
    assert_eq!(
        related.platform().expect("Platform must be loaded").name(),
        "Campus Learning Platform"
    );
    assert_eq!(
        related
            .school_type()
            .expect("SchoolType must be loaded")
            .code(),
        "PRIMARY"
    );

    let projected = Q::schools()
        .select_name()
        .order_by_id_desc()
        .comment("Query parity: projection and ordering")
        .purpose("Execute the shared School Query conformance case")
        .execute_for_list(&context)
        .await?;
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].name(), "Riverside Primary School");

    let include_all = Q::schools()
        .with_name_containing("Primary")
        .facet_by_school_type_as_with_options(
            "schoolTypeFacet",
            Q::school_types_minimal()
                .select_code()
                .count_schools_as("schoolCount"),
            true,
        )
        .comment("Facet SchoolType values including zero-count constants")
        .purpose("Verify Rust native SQLite Facet semantics")
        .execute_for_list(&context)
        .await?;
    let all_values = include_all
        .facet("schoolTypeFacet")
        .expect("SchoolType facet must be attached to SmartList");
    assert_eq!(all_values.len(), 2);
    assert_eq!(
        all_values[0].get("code").and_then(|v| v.try_text()),
        Some("PRIMARY")
    );
    assert_eq!(
        all_values[0].get("schoolCount").and_then(|v| v.try_u64()),
        Some(1)
    );
    assert_eq!(
        all_values[1].get("code").and_then(|v| v.try_text()),
        Some("SECONDARY")
    );
    assert_eq!(
        all_values[1].get("schoolCount").and_then(|v| v.try_u64()),
        Some(0)
    );

    let matched_only = Q::schools()
        .with_name_containing("Primary")
        .facet_by_school_type_as_with_options(
            "schoolTypeFacet",
            Q::school_types_minimal()
                .select_code()
                .count_schools_as("schoolCount"),
            false,
        )
        .comment("Facet only SchoolType values matched by the outer School filter")
        .purpose("Verify Rust native SQLite matched-only Facet semantics")
        .execute_for_list(&context)
        .await?;
    let matched_values = matched_only
        .facet("schoolTypeFacet")
        .expect("Matched SchoolType facet must be attached to SmartList");
    assert_eq!(matched_values.len(), 1);
    assert_eq!(
        matched_values[0].get("code").and_then(|v| v.try_text()),
        Some("PRIMARY")
    );
    assert_eq!(
        matched_values[0]
            .get("schoolCount")
            .and_then(|v| v.try_u64()),
        Some(1)
    );

    for (index, name) in ["North School", "East School", "South School", "West School"]
        .into_iter()
        .enumerate()
    {
        let mut additional = Q::schools()
            .comment("Create an ID-set pagination fixture")
            .purpose("Verify generated Rust ID-set pagination on SQLite")
            .new_entity(&context);
        additional.update_platform_id(1_u64);
        additional.update_school_type_to_primary();
        additional.update_name(name);
        additional.update_address(format!("{} Pagination Road", index + 1));
        additional.update_established_date(
            school_management_service_core::teaql_core::Value::Date("2000-01-01".parse()?),
        );
        additional.update_student_capacity(100_i64 + index as i64);
        additional.update_active(true);
        let timestamp = school_management_service_core::teaql_core::time::Timestamp::now();
        additional.update_create_time(timestamp);
        additional.update_update_time(timestamp);
        additional
            .audit_as("Create an ID-set pagination fixture")
            .save(&context)
            .await?;
    }

    let jumped_page = Q::schools()
        .order_by_id_desc()
        .optimize_pagination_with_id_set_config("school-example", 60, 100)
        .comment("Jump directly to the second retained ID-set page")
        .purpose("Verify generated Rust ID-set pagination on SQLite")
        .execute_for_page(&context, 2, 2)
        .await?;
    assert_eq!(jumped_page.total_count, Some(5));
    assert_eq!(
        jumped_page
            .iter()
            .map(|school| school.id())
            .collect::<Vec<_>>(),
        vec![3, 2]
    );
    assert_eq!(context.id_set_plan().as_deref(), Some("ID_SET_BUILD"));

    let first_page = Q::schools()
        .order_by_id_desc()
        .optimize_pagination_with_id_set_config("school-example", 60, 100)
        .comment("Read the first page from the retained ID set")
        .purpose("Verify generated Rust ID-set pagination on SQLite")
        .execute_for_page(&context, 0, 2)
        .await?;
    assert_eq!(first_page.total_count, Some(5));
    assert_eq!(
        first_page
            .iter()
            .map(|school| school.id())
            .collect::<Vec<_>>(),
        vec![5, 4]
    );
    assert_eq!(context.id_set_plan().as_deref(), Some("ID_SET_HIT"));

    // Two independently loaded objects must not share pending mutation intent
    // merely because the same UserContext is used for both requests.
    let mut committed = Q::schools()
        .with_name_is("Riverside Primary School")
        .select_self_fields()
        .select_platform_with(Q::platforms_minimal().select_name())
        .select_school_type_with(Q::school_types_minimal().select_code())
        .comment("Load the complete School and forward relations for ledger isolation")
        .purpose("Verify audited Save touches only this independently loaded graph")
        .execute_for_one(&context)
        .await?
        .expect("Riverside School must exist before the isolation gate");
    let mut abandoned = Q::schools()
        .with_name_is("North School")
        .select_self_fields()
        .comment("Load a second complete School in the same UserContext")
        .purpose("Verify its unsaved ledger cannot leak into the first Save")
        .execute_for_one(&context)
        .await?
        .expect("North School must exist before the isolation gate");
    let committed_id = committed.id();
    let committed_version = committed.version();
    let abandoned_id = abandoned.id();
    let abandoned_version = abandoned.version();
    committed.update_name("Riverside Primary Academy");
    abandoned.update_name("North School Unsaved");

    let saved = committed
        .audit_as("Rename only Riverside School, not the separate pending North School")
        .save(&context)
        .await?;
    assert_eq!(saved.id(), committed_id);
    assert_eq!(saved.version(), committed_version + 1);
    assert_eq!(saved.name(), "Riverside Primary Academy");
    assert_eq!(
        saved
            .platform()
            .expect("loaded Platform must survive Save")
            .name(),
        "Campus Learning Platform"
    );
    assert_eq!(
        saved
            .school_type()
            .expect("loaded SchoolType must survive Save")
            .code(),
        "PRIMARY"
    );
    let untouched = Q::schools()
        .with_id_is(abandoned_id)
        .select_self_fields()
        .comment("Reload the independent School that was modified but not saved")
        .purpose("Prove no pending mutation leaked through UserContext")
        .execute_for_one(&context)
        .await?
        .expect("North School must remain in SQLite");
    assert_eq!(untouched.name(), "North School");
    assert_eq!(untouched.version(), abandoned_version);

    // A sparse new-entity ledger must reject a missing required field before
    // the insert reaches SQLite. Run this after the fixed-ID query fixtures:
    // allocating a rejected entity may leave an intentional gap in ID space.
    let mut incomplete = Q::schools()
        .comment("Allocate an intentionally incomplete School")
        .purpose("Verify Checker rejects sparse CREATE payloads")
        .new_entity(&context);
    incomplete.update_name("Incomplete School");
    let error = match incomplete
        .audit_as("Verify required address rejection")
        .save(&context)
        .await
    {
        Ok(_) => panic!("incomplete School must not be persisted"),
        Err(error) => error,
    };
    let diagnosis = error.to_string().to_lowercase();
    assert!(diagnosis.contains("required"), "{diagnosis}");
    assert!(diagnosis.contains("address"), "{diagnosis}");
    assert!(!diagnosis.contains("transport error"), "{diagnosis}");
    let rejected = Q::schools()
        .with_name_is("Incomplete School")
        .comment("Confirm Checker rejection did not insert a School")
        .purpose("Verify sparse CREATE was rejected before SQLite write")
        .execute_for_list(&context)
        .await?;
    assert_eq!(rejected.len(), 0);

    println!("PASS Rust School bootstrap, ID-set pagination, portable Query, native SQLite Facet, independent ledger isolation, and sparse ledger Checker parity");
    std::fs::remove_file(database)?;
    Ok(())
}
