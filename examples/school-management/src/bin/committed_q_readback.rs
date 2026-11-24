use school_management_service_core::{
    request_support::AuditedSave as _, service_runtime, teaql_core::Entity as _,
    DataServiceMutationExecutor, School, ServiceRuntimeConfig, Q,
};
use teaql_provider_sqlite::committed_events::CommittedEventTarget;
use teaql_runtime::CommittedEventSource;

const SOURCE_ID: &str = "school-generated-example-source";
const MODEL_HASH: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

fn fill_school(school: &mut School) -> Result<(), Box<dyn std::error::Error>> {
    school.update_platform_id(1_u64);
    school.update_school_type_to_primary();
    school.update_name("Committed Q Readback School");
    school.update_address("1 Source Road");
    school.update_established_date(school_management_service_core::teaql_core::Value::Date(
        "2001-01-01".parse()?,
    ));
    school.update_student_capacity(100_i64);
    school.update_active(true);
    let now = school_management_service_core::teaql_core::time::Timestamp::now();
    school.update_create_time(now);
    school.update_update_time(now);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source_database = std::env::var("TEAQL_COMMITTED_SCHOOL_SOURCE_DB")?;
    let target_database = std::env::var("TEAQL_COMMITTED_SCHOOL_TARGET_DB")?;
    let initial_source = service_runtime(ServiceRuntimeConfig {
        database_url: source_database,
    })
    .await?;
    initial_source.ensure_schema().await?;
    let source = initial_source
        .with_committed_event_source(CommittedEventSource::new(SOURCE_ID, MODEL_HASH)?);
    source.ensure_schema().await?;
    let source_transport = source
        .get_resource::<DataServiceMutationExecutor>()
        .expect("generated source must register SQLite transport")
        .clone();

    let mut school = Q::schools()
        .comment("allocate a School for committed Q readback")
        .purpose("verify generated persistence and replication")
        .new_entity(&source);
    fill_school(&mut school)?;
    let created = school
        .audit_as("create School for committed Q readback")
        .save(&source)
        .await?;
    let outbox = source_transport.committed_outbox_after(SOURCE_ID, 0, 10)?;
    assert_eq!(outbox.len(), 1, "only the School save belongs in Outbox");

    let mut target = service_runtime(ServiceRuntimeConfig {
        database_url: target_database,
    })
    .await?;
    target.insert_resource(CommittedEventTarget::new(SOURCE_ID, MODEL_HASH)?);
    target.ensure_schema().await?;
    let target_transport = target
        .get_resource::<DataServiceMutationExecutor>()
        .expect("generated target must register SQLite transport")
        .clone();
    let published = source_transport
        .publish_committed_batch(
            SOURCE_ID,
            "school-generated-target",
            MODEL_HASH,
            10,
            |record| {
                let transport = &target_transport;
                let context = &target;
                async move {
                    transport
                        .apply_model_checked_committed_set(context, &record)
                        .await
                }
            },
        )
        .await?;
    assert_eq!(published.acknowledged, 1);
    assert_eq!(published.status.backlog, 0);

    let mut replicated = Q::schools()
        .with_id_is(created.id())
        .comment("load replicated School through generated Q")
        .purpose("prove model-checked target is queryable by application")
        .execute_for_one(&target)
        .await?
        .expect("replicated School must be visible through generated Q");
    assert_eq!(replicated.name(), "Committed Q Readback School");
    assert_eq!(replicated.version(), created.version());
    replicated.update_name("must not modify the analytical target");
    let denied = replicated
        .audit_as("attempt ordinary target write")
        .save(&target)
        .await
        .err()
        .expect("ordinary generated save must be rejected on target");
    assert!(denied.to_string().contains("READ_ONLY_TARGET"), "{denied}");
    let unchanged = Q::schools()
        .with_id_is(created.id())
        .comment("reload target after denied ordinary write")
        .purpose("verify generated mutation did not change replicated facts")
        .execute_for_one(&target)
        .await?
        .expect("replicated School remains visible");
    assert_eq!(unchanged.name(), "Committed Q Readback School");
    assert_eq!(unchanged.version(), created.version());
    assert_eq!(
        source_transport
            .committed_outbox_after(SOURCE_ID, 0, 10)?
            .len(),
        1
    );
    println!("CMR15_GENERATED_READ_ONLY_PASS language=rust backend=sqlite generated_audited_save=1 fresh_query=1 source_isolated=1");
    println!(
        "COMMITTED_GENERATED_Q_PASS id={} version={} outbox=1 target=1",
        created.id(),
        created.version()
    );
    Ok(())
}
