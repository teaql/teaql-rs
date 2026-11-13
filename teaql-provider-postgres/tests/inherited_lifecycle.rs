//! Live, ownership-checked inheritance tests. Each case creates one private
//! schema, catches test panics, and drops only that schema before propagating
//! the original outcome. No application/public-schema tables are changed.
use deadpool_postgres::{Pool, Runtime};
use futures_util::FutureExt;
use std::{future::Future, panic::AssertUnwindSafe};
use teaql_core::{
    DataType, DeleteCommand, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor as P,
    RecoverCommand, SelectQuery, UpdateCommand, Value,
};
use teaql_data_service::{
    GuardedMutationExecutor, GuardedMutationRequest, MutationCommand, MutationExecutor,
    MutationRequest, QueryExecutor, QueryRequest, Transaction, TransactionExecutor,
};
use teaql_provider_postgres::{PgMutationExecutor, PostgresDialect, PostgresProviderExt};
use teaql_runtime::{InMemoryMetadataStore, UserContext};
use teaql_sql::SqlDataServiceExecutor;

type Executor = SqlDataServiceExecutor<PostgresDialect, PgMutationExecutor, InMemoryMetadataStore>;

fn metadata() -> InMemoryMetadataStore {
    let root = EntityDescriptor::new("School")
        .property(P::new("id", DataType::I64).id().not_null())
        .property(P::new("version", DataType::I64).version().not_null())
        .property(
            P::new("name", DataType::Text)
                .column_name("Legacy Name")
                .not_null(),
        )
        .audit_mask_fields(vec![]);
    let middle = EntityDescriptor::new("Academy")
        .inherits(root.clone())
        .property(P::new("campus", DataType::Text).not_null())
        .audit_mask_fields(vec!["campus".into()]);
    let leaf = EntityDescriptor::new("TrainingAcademy")
        .inherits(middle.clone())
        .property(P::new("specialty", DataType::Text).not_null())
        .audit_mask_fields(vec!["name".into(), "specialty".into()]);
    let unrelated = EntityDescriptor::new("Unrelated")
        .property(P::new("id", DataType::I64).id().not_null())
        .property(P::new("version", DataType::I64).version().not_null())
        .property(P::new("name", DataType::Text))
        .audit_mask_fields(vec![]);
    [root, middle, leaf, unrelated]
        .into_iter()
        .fold(InMemoryMetadataStore::new(), |store, entity| {
            store.with_entity(entity)
        })
}

async fn case<F, Fut>(name: &str, body: F)
where
    F: FnOnce(UserContext, Executor, Pool) -> Fut,
    Fut: Future<Output = ()>,
{
    assert_eq!(
        std::env::var("TEAQL_REQUIRE_LIVE_DB").as_deref(),
        Ok("true")
    );
    let url = std::env::var("TEAQL_TEST_POSTGRES_URL").expect("owned PostgreSQL URL required");
    let mut config = deadpool_postgres::Config::new();
    config.url = Some(url);
    let admin = config
        .create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
        .unwrap();
    let client = admin.get().await.unwrap();
    let owner = client.query_one("SELECT current_database(),shobj_description(oid,'pg_database') FROM pg_database WHERE datname=current_database()", &[]).await.unwrap();
    assert_eq!(owner.get::<_, String>(0), "teaql_rollup_326");
    assert_eq!(
        owner.get::<_, Option<String>>(1).as_deref(),
        Some("TeaQL #326 owned layered statistics fixture")
    );
    assert!(name.chars().all(|c| c.is_ascii_lowercase()));
    let schema = format!("teaql_inherited_327_{}_{}", name, std::process::id());
    config.options = Some(format!("-c search_path={schema}"));
    let pool = config
        .create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
        .unwrap();
    // No IF NOT EXISTS: an unexpected namespace is not ours to reuse or drop.
    client
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let result = AssertUnwindSafe(async {
        let store = metadata();
        let transport = PgMutationExecutor::new(pool.clone());
        let executor =
            SqlDataServiceExecutor::new(PostgresDialect, transport.clone(), store.clone());
        let mut context = UserContext::new().with_metadata(store);
        context.use_postgres_provider(transport);
        context.register_executor(executor.clone());
        body(context, executor, pool.clone()).await;
    })
    .catch_unwind()
    .await;
    pool.close();
    let cleanup = client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    match result {
        Err(panic) => {
            if cleanup.is_err() {
                eprintln!("Task-owned PostgreSQL schema retained after cleanup error: {schema}");
            }
            std::panic::resume_unwind(panic);
        }
        Ok(()) => cleanup.expect("remove only this test's private schema"),
    }
    println!("INHERITED_POSTGRES_CASE={name}; rounds=2; schemaRemoved=true");
}

fn mutation(command: MutationCommand) -> MutationRequest {
    MutationRequest::new(command, "Verify a scoped inherited mutation").unwrap()
}
fn insert(id: i64, specialty: &str) -> MutationRequest {
    mutation(MutationCommand::Insert(
        InsertCommand::new("TrainingAcademy")
            .value("id", id)
            .value("version", 1_i64)
            .value("name", "Original")
            .value("campus", "NORTH")
            .value("specialty", specialty),
    ))
}
fn query(entity: &str, id: i64) -> QueryRequest {
    QueryRequest::from_query(
        SelectQuery::new(entity)
            .filter(Expr::eq("id", id))
            .limit(1)
            .comment("Read one inherited fixture object")
            .purpose("Verify transactional persistence through typed metadata"),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires the ownership-checked PostgreSQL fixture"]
async fn guards_versions_and_same_id_unrelated_records() {
    case("guards", |context, executor, pool| async move {
        context.ensure_schema().await.unwrap();
        context.ensure_schema().await.unwrap();
        for id in [1_i64, 2] {
            let tx = executor.begin().await.unwrap();
            tx.mutate(mutation(MutationCommand::Insert(
                InsertCommand::new("Unrelated")
                    .value("id", id)
                    .value("version", 99_i64)
                    .value("name", "Independent"),
            )))
            .await
            .unwrap();
            let saved = tx.mutate(insert(id, "robotics")).await.unwrap();
            assert_eq!(saved.affected_rows, 1);
            assert_eq!(
                saved.persisted_snapshot.unwrap().get("version"),
                Some(&Value::I64(1))
            );
            let first = &saved.metadata.statements[0];
            let index = first
                .params
                .iter()
                .position(|v| v == &Value::from("Original"))
                .unwrap();
            assert_eq!(
                first.sql_log.parameter_policies[index],
                teaql_data_service::SqlParameterLogPolicy::Masked
            );
            tx.commit().await.unwrap();
            assert_eq!(
                executor
                    .query(query("School", id))
                    .await
                    .unwrap()
                    .rows
                    .len(),
                1
            );
            assert_eq!(
                executor.query(query("Unrelated", id)).await.unwrap().rows[0].get("version"),
                Some(&Value::I64(99))
            );
            let update = || {
                mutation(MutationCommand::Update(
                    UpdateCommand::new("TrainingAcademy", id)
                        .expected_version(1)
                        .value("name", "Approved")
                        .value("specialty", "engineering"),
                ))
            };
            assert!(executor
                .mutate_guarded(GuardedMutationRequest::new(
                    update(),
                    Expr::eq("campus", "NORTH")
                ))
                .await
                .is_err());
            let tx = executor.begin().await.unwrap();
            assert_eq!(
                tx.mutate_guarded(GuardedMutationRequest::new(
                    update(),
                    Expr::eq("specialty", "forbidden")
                ))
                .await
                .unwrap()
                .affected_rows,
                0
            );
            tx.commit().await.unwrap();
            let tx = executor.begin().await.unwrap();
            assert_eq!(
                tx.mutate_guarded(GuardedMutationRequest::new(
                    update(),
                    Expr::eq("campus", "NORTH")
                ))
                .await
                .unwrap()
                .affected_rows,
                1
            );
            tx.commit().await.unwrap();
            let tx = executor.begin().await.unwrap();
            assert!(tx
                .mutate_guarded(GuardedMutationRequest::new(
                    mutation(MutationCommand::Update(
                        UpdateCommand::new("TrainingAcademy", id)
                            .expected_version(2)
                            .value("campus", "SOUTH")
                    )),
                    Expr::eq("campus", "NORTH")
                ))
                .await
                .is_err());
            tx.rollback().await.unwrap();
            let tx = executor.begin().await.unwrap();
            tx.mutate_guarded(GuardedMutationRequest::new(
                mutation(MutationCommand::Delete(
                    DeleteCommand::new("TrainingAcademy", id).expected_version(2),
                )),
                Expr::eq("campus", "NORTH"),
            ))
            .await
            .unwrap();
            tx.commit().await.unwrap();
            let tx = executor.begin().await.unwrap();
            tx.mutate_guarded(GuardedMutationRequest::new(
                mutation(MutationCommand::Recover(RecoverCommand::new(
                    "TrainingAcademy",
                    id,
                    -3,
                ))),
                Expr::eq("campus", "NORTH"),
            ))
            .await
            .unwrap();
            tx.commit().await.unwrap();
            let tx = executor.begin().await.unwrap();
            let denied = tx
                .mutate_guarded(GuardedMutationRequest::new(
                    mutation(MutationCommand::Delete(
                        DeleteCommand::new("TrainingAcademy", id)
                            .expected_version(4)
                            .hard_delete(),
                    )),
                    Expr::eq("campus", "SOUTH"),
                ))
                .await
                .unwrap();
            assert_eq!(denied.affected_rows, 0);
            tx.commit().await.unwrap();
            let tx = executor.begin().await.unwrap();
            tx.mutate_guarded(GuardedMutationRequest::new(
                mutation(MutationCommand::Delete(
                    DeleteCommand::new("TrainingAcademy", id)
                        .expected_version(4)
                        .hard_delete(),
                )),
                Expr::eq("campus", "NORTH"),
            ))
            .await
            .unwrap();
            tx.commit().await.unwrap();
            for entity in ["School", "Academy", "TrainingAcademy"] {
                assert!(executor
                    .query(query(entity, id))
                    .await
                    .unwrap()
                    .rows
                    .is_empty());
            }
            assert_eq!(
                executor.query(query("Unrelated", id)).await.unwrap().rows[0].get("version"),
                Some(&Value::I64(99))
            );
        }
        let client = pool.get().await.unwrap();
        assert_eq!(
            client
                .query_one("SELECT count(*) FROM school_data", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            0
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires the ownership-checked PostgreSQL fixture"]
async fn driver_failures_and_final_root_conflicts_roll_back_all_tables() {
    case("rollback", |context, executor, pool| async move {
        context.ensure_schema().await.unwrap(); context.ensure_schema().await.unwrap();
        {
            let client = pool.get().await.unwrap();
            client.batch_execute("ALTER TABLE training_academy_data ADD CHECK(specialty <> 'reject');
                CREATE FUNCTION change_version() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE school_data SET version=version+1 WHERE id=old.id; RETURN old; END $$;
                CREATE TRIGGER change_version AFTER DELETE ON training_academy_data FOR EACH ROW EXECUTE FUNCTION change_version();").await.unwrap();
        }
        for id in [1_i64, 2] {
            let tx = executor.begin().await.unwrap();
            assert!(tx.mutate(insert(id, "reject")).await.is_err());
            tx.rollback().await.unwrap();
            assert!(executor.query(query("School", id)).await.unwrap().rows.is_empty());
            let tx = executor.begin().await.unwrap(); tx.mutate(insert(id, "robotics")).await.unwrap(); tx.commit().await.unwrap();
            let tx = executor.begin().await.unwrap();
            assert!(tx.mutate_guarded(GuardedMutationRequest::new(mutation(MutationCommand::Update(
                UpdateCommand::new("TrainingAcademy", id).expected_version(1).value("name", "Rejected").value("specialty", "reject")
            )), Expr::eq("campus", "NORTH"))).await.is_err());
            tx.rollback().await.unwrap();
            let tx = executor.begin().await.unwrap();
            assert!(tx.mutate_guarded(GuardedMutationRequest::new(mutation(MutationCommand::Delete(
                DeleteCommand::new("TrainingAcademy", id).expected_version(1).hard_delete()
            )), Expr::eq("campus", "NORTH"))).await.is_err());
            tx.rollback().await.unwrap();
            let row = executor.query(query("TrainingAcademy", id)).await.unwrap();
            assert_eq!(row.rows[0].get("version"), Some(&Value::I64(1)));
            assert_eq!(row.rows[0].get("name"), Some(&Value::from("Original")));
            assert_eq!(row.rows[0].get("specialty"), Some(&Value::from("robotics")));
        }
    }).await;
}

#[tokio::test]
#[ignore = "requires the ownership-checked PostgreSQL fixture"]
async fn old_flattened_schema_is_rejected_without_rewriting_rows() {
    case("migration", |context, _, pool| async move {
        let client = pool.get().await.unwrap();
        client.batch_execute("CREATE TABLE academy_data(id BIGINT PRIMARY KEY,version BIGINT NOT NULL,name TEXT,campus TEXT);
            INSERT INTO academy_data VALUES(7,1,'Legacy','NORTH');").await.unwrap();
        for _ in 0..2 {
            let error = context.ensure_schema().await.unwrap_err().to_string();
            assert!(error.contains("migrate flattened inherited storage explicitly"), "{error}");
            assert_eq!(client.query_one("SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema()", &[]).await.unwrap().get::<_, i64>(0), 1);
            assert_eq!(client.query_one("SELECT name FROM academy_data WHERE id=7 AND version=1", &[]).await.unwrap().get::<_, String>(0), "Legacy");
        }
    }).await;
}
