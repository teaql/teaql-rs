use school_management_service_core::{
    request_support::AuditedSave as _, service_runtime_from_env, teaql_core::Entity as _, Q,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The generated helper must install its graph saver; this application does
    // not call register_executor or add a type-erased saver manually.
    let mut context = service_runtime_from_env().await?;
    context.ensure_schema().await?;

    let mut school = Q::schools()
        .comment("Create a School through the generated environment runtime helper")
        .purpose("Verify generated executor registration supports audited Save")
        .new_entity(&context);
    school.update_platform_id(1_u64);
    school.update_school_type_to_primary();
    school.update_name("Env Helper School");
    school.update_address("1 Runtime Road");
    school.update_established_date(school_management_service_core::teaql_core::Value::Date(
        "2001-01-01".parse()?,
    ));
    school.update_student_capacity(100_i64);
    school.update_active(true);
    let now = school_management_service_core::teaql_core::time::Timestamp::now();
    school.update_create_time(now);
    school.update_update_time(now);

    let saved = school
        .audit_as("Verify service_runtime_from_env installs the graph saver")
        .save(&context)
        .await?;
    assert!(saved.id() > 0);
    let mut reread = Q::schools()
        .with_id_is(saved.id())
        .select_self_fields()
        .comment("Reload School created through service_runtime_from_env")
        .purpose("Prove audited Save reached SQLite without manual registration")
        .execute_for_one(&context)
        .await?
        .expect("environment helper must persist the audited School");
    assert_eq!(reread.name(), "Env Helper School");
    assert_eq!(reread.version(), saved.version());
    // The physical FK column is `school_type`, the same spelling as the
    // relation. Scalar hydration must not fabricate a partial SchoolType.
    assert_eq!(reread.school_type_id(), 1001);
    assert!(reread.school_type().is_none());
    reread.update_name("Env Helper School Renamed");
    let updated = reread
        .audit_as("Verify scalar FK hydration does not create a ghost relation")
        .save(&context)
        .await?;
    assert_eq!(updated.version(), saved.version() + 1);
    let mut verified = Q::schools()
        .with_id_is(updated.id())
        .select_self_fields()
        .comment("Reload School after colliding-column audited update")
        .purpose("Verify physical school_type column does not create a ghost relation")
        .execute_for_one(&context)
        .await?
        .expect("updated School must remain queryable");
    assert_eq!(verified.name(), "Env Helper School Renamed");
    assert_eq!(verified.school_type_id(), 1001);
    assert_eq!(verified.version(), updated.version());

    let select_logs_before = context
        .sql_logs()
        .into_iter()
        .filter(|entry| entry.operation.is_select())
        .count();
    context.disable_select_sql_log();
    let ignored_query = Q::schools()
        .with_id_is(updated.id())
        .select_self_fields()
        .comment("what: query School while Query SQL logging is disabled")
        .purpose("why: verify the Query log switch does not disable execution")
        .execute_for_one(&context)
        .await?;
    assert!(ignored_query.is_some());
    assert_eq!(
        context
            .sql_logs()
            .into_iter()
            .filter(|entry| entry.operation.is_select())
            .count(),
        select_logs_before
    );
    context.enable_select_sql_log();

    let mutation_logs_before = context
        .sql_logs()
        .into_iter()
        .filter(|entry| entry.operation.is_mutation())
        .count();
    context.disable_mutation_sql_log();
    verified.update_address("2 Runtime Road");
    let final_saved = verified
        .audit_as("Verify audited update executes with Mutation SQL logging disabled")
        .save(&context)
        .await?;
    assert_eq!(final_saved.version(), updated.version() + 1);
    assert_eq!(
        context
            .sql_logs()
            .into_iter()
            .filter(|entry| entry.operation.is_mutation())
            .count(),
        mutation_logs_before
    );
    context.enable_mutation_sql_log();
    let final_reread = Q::schools()
        .with_id_is(final_saved.id())
        .select_self_fields()
        .comment("what: reload School after Mutation SQL logging was disabled")
        .purpose("why: prove the muted audited update still reached SQLite")
        .execute_for_one(&context)
        .await?
        .expect("muted audited update must still persist");
    assert_eq!(final_reread.address(), "2 Runtime Road");
    println!("SQL_LOG_SWITCH_SQLITE_PASS query=off mutation=off");
    println!(
        "ENV_RUNTIME_SAVE_PASS school_id={} version={}",
        saved.id(),
        final_saved.version()
    );
    println!("COLLIDING_FK_SAVE_PASS school_type_id=1001");
    Ok(())
}
