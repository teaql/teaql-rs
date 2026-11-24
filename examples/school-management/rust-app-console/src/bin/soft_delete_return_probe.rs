use school_management_service_core::{
    request_support::AuditedSave as _, service_runtime, teaql_core::Entity as _,
    DataServiceExecutor, School, ServiceRuntimeConfig, Q,
};

fn fill_school(school: &mut School, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    school.update_platform_id(1_u64);
    school.update_school_type_to_primary();
    school.update_name(name);
    school.update_address("1 Test Road");
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
    let database_url = std::env::var("TEAQL_SCHOOL_DELETE_PROBE_DATABASE")?;
    let context = service_runtime(ServiceRuntimeConfig { database_url }).await?;
    context.ensure_schema().await?;

    // Test-only database behavior: make an explicit-transaction deletion
    // differ from the simple -(prior_version+1) formula. Save must return
    // the row produced by the database, not a guessed in-memory version.
    let fixture_connection = rusqlite::Connection::open(std::env::var(
        "TEAQL_SCHOOL_DELETE_PROBE_DATABASE",
    )?)?;
    fixture_connection.execute_batch(
        "CREATE TRIGGER IF NOT EXISTS school_transaction_delete_version_probe \
         AFTER UPDATE OF version ON school_data \
         WHEN NEW.name = 'Transaction Delete Probe School' AND NEW.version < 0 \
         BEGIN UPDATE school_data SET version = -9 WHERE id = NEW.id; END;",
    )?;
    drop(fixture_connection);

    let mut school = Q::schools()
        .comment("what: allocate School for a soft-delete return probe")
        .purpose("why: verify authoritative tombstone result")
        .new_entity(&context);
    fill_school(&mut school, "Soft Delete Return Probe School")?;
    let created = school
        .audit_as("create the School soft-delete return probe")
        .save(&context)
        .await?;
    assert_eq!(created.version(), 1);

    let mut loaded = Q::schools()
        .with_id_is(created.id())
        .comment("what: load the complete School before soft delete")
        .purpose("why: carry its original optimistic version")
        .execute_for_one(&context)
        .await?
        .expect("created School must be queryable");
    loaded.mark_for_deletion();
    let deleted = loaded
        .audit_as("soft delete the School probe")
        .save(&context)
        .await?;
    assert_eq!(deleted.id(), created.id());
    assert_eq!(deleted.version(), -2);

    let mut transaction_school = Q::schools()
        .comment("what: allocate School for transaction tombstone probe")
        .purpose("why: verify explicit transaction authoritative readback")
        .new_entity(&context);
    fill_school(&mut transaction_school, "Transaction Delete Probe School")?;
    let transaction_created = transaction_school
        .audit_as("create School for explicit transaction deletion")
        .save(&context)
        .await?;
    let mut transaction_loaded = Q::schools()
        .with_id_is(transaction_created.id())
        .comment("what: load complete School for explicit transaction deletion")
        .purpose("why: carry original optimistic version")
        .execute_for_one(&context)
        .await?
        .expect("transaction School must be queryable");
    transaction_loaded.mark_for_deletion();
    let transaction_deleted = context
        .execute_in_transaction::<DataServiceExecutor, _, _>(|scope| {
            Box::pin(async move {
                scope
                    .save_audited(transaction_loaded.audit_as("soft delete in transaction"))
                    .await
            })
        })
        .await?;
    assert_eq!(transaction_deleted.id(), transaction_created.id());
    assert_eq!(transaction_deleted.version(), -9);

    let mut cancelled_school = Q::schools()
        .comment("what: allocate a School that will be cancelled")
        .purpose("why: reject a fictitious persisted Save return")
        .new_entity(&context);
    fill_school(&mut cancelled_school, "Cancelled New School")?;
    let cancelled_id = cancelled_school.id();
    cancelled_school.mark_for_deletion();
    let cancellation_error = cancelled_school
        .audit_as("cancel new School before save")
        .save(&context)
        .await
        .expect_err("a cancelled new root has no persisted School to return");
    assert!(cancellation_error.to_string().contains("cancelled new root"));
    let cancelled_row = Q::schools()
        .with_id_is(cancelled_id)
        .comment("what: check cancelled School row is absent")
        .purpose("why: prove cancellation did not write SQL")
        .execute_for_one(&context)
        .await?;
    assert!(cancelled_row.is_none());

    let mut transaction_cancelled_school = Q::schools()
        .comment("what: allocate a transaction-scoped School that will be cancelled")
        .purpose("why: reject a fictitious transaction Save return")
        .new_entity(&context);
    fill_school(&mut transaction_cancelled_school, "Transaction Cancelled New School")?;
    let transaction_cancelled_id = transaction_cancelled_school.id();
    transaction_cancelled_school.mark_for_deletion();
    let transaction_cancellation_error = context
        .execute_in_transaction::<DataServiceExecutor, _, _>(|scope| {
            Box::pin(async move {
                scope
                    .save_audited(
                        transaction_cancelled_school.audit_as("cancel new School in transaction"),
                    )
                    .await
            })
        })
        .await
        .expect_err("transaction Save cannot return a cancelled new root");
    assert!(transaction_cancellation_error
        .to_string()
        .contains("cancelled new root"));
    let transaction_cancelled_row = Q::schools()
        .with_id_is(transaction_cancelled_id)
        .comment("what: check transaction-cancelled School row is absent")
        .purpose("why: prove cancelled root rolled back without SQL")
        .execute_for_one(&context)
        .await?;
    assert!(transaction_cancelled_row.is_none());
    println!(
        "SCHOOL_SOFT_DELETE_RETURN_PASS id={} version={} transaction_id={} transaction_version={} cancelled_id={} transaction_cancelled_id={}",
        deleted.id(),
        deleted.version(),
        transaction_deleted.id(),
        transaction_deleted.version(),
        cancelled_id,
        transaction_cancelled_id
    );
    Ok(())
}
