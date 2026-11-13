use super::*;
use teaql_core::{
    DeleteCommand, InsertCommand, PropertyDescriptor as P, RecoverCommand, SelectQuery,
    UpdateCommand,
};
use teaql_data_service::{
    MutationCommand, MutationExecutor, MutationRequest, QueryExecutor, QueryRequest,
    SchemaProvider as QuerySchema, Transaction, TransactionExecutor,
};

#[derive(Clone)]
struct Schema(Vec<EntityDescriptor>);
impl QuerySchema for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        self.0
            .iter()
            .find(|entity| entity.name == name)
            .cloned()
            .map(Arc::new)
    }
}
type Executor = teaql_sql::SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;

fn fixture() -> (Executor, Arc<Mutex<Connection>>) {
    let school = EntityDescriptor::new("School")
        .property(P::new("id", DataType::I64).id().not_null())
        .property(P::new("version", DataType::I64).version().not_null())
        .property(
            P::new("name", DataType::Text)
                .column_name("Legacy Name")
                .not_null(),
        );
    let academy = EntityDescriptor::new("Academy")
        .inherits(school.clone())
        .property(P::new("campus", DataType::Text).not_null());
    let training = EntityDescriptor::new("TrainingAcademy")
        .inherits(academy.clone())
        .property(P::new("specialty", DataType::Text).not_null());
    let transport = SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
    transport
        .connection()
        .lock()
        .unwrap()
        .pragma_update(None, "foreign_keys", true)
        .unwrap();
    for _ in 0..2 {
        transport
            .ensure_schema(&SqliteDialect, &[&training, &school, &academy])
            .unwrap();
    }
    let connection = transport.connection();
    (
        teaql_sql::SqlDataServiceExecutor::new(
            SqliteDialect,
            transport,
            Schema(vec![school, academy, training]),
        ),
        connection,
    )
}

fn mutation(command: impl Into<MutationCommand>) -> MutationRequest {
    MutationRequest::new(command.into(), "Verify the natural inherited lifecycle").unwrap()
}
fn insert(id: i64) -> MutationRequest {
    mutation(MutationCommand::Insert(
        InsertCommand::new("TrainingAcademy")
            .value("id", id)
            .value("version", 1_i64)
            .value("name", "Original school")
            .value("campus", "NORTH")
            .value("specialty", "robotics"),
    ))
}
fn query(entity: &str, id: i64) -> QueryRequest {
    QueryRequest::from_query(
        SelectQuery::new(entity)
            .filter(Expr::eq("id", id))
            .limit(1)
            .comment("Read one inherited object")
            .purpose("Verify declaring-table consistency"),
    )
    .unwrap()
}

#[test]
fn inherited_save_and_update_use_declaring_tables_and_one_root_version() {
    futures_executor::block_on(async {
        let (executor, connection) = fixture();
        for id in [1_i64, 2] {
            let tx = executor.begin().await.unwrap();
            let result = tx.mutate(insert(id)).await.unwrap();
            assert_eq!(result.affected_rows, 1, "one logical mutation, not three");
            assert_eq!(
                result.persisted_snapshot.as_ref().unwrap().get("version"),
                Some(&Value::I64(1))
            );
            tx.commit().await.unwrap();
            let parent = executor.query(query("School", id)).await.unwrap();
            assert_eq!(parent.rows.len(), 1, "no manual parent seed");
            assert_eq!(
                parent.rows[0].get("name"),
                Some(&Value::from("Original school"))
            );

            let tx = executor.begin().await.unwrap();
            tx.mutate(mutation(MutationCommand::Update(
                UpdateCommand::new("TrainingAcademy", id)
                    .expected_version(1)
                    .value("name", "Renamed school")
                    .value("campus", "SOUTH")
                    .value("specialty", "engineering"),
            )))
            .await
            .unwrap();
            tx.commit().await.unwrap();
            let leaf = executor.query(query("TrainingAcademy", id)).await.unwrap();
            assert_eq!(leaf.rows[0].get("version"), Some(&Value::I64(2)));
            assert_eq!(
                leaf.rows[0].get("name"),
                Some(&Value::from("Renamed school"))
            );
            assert_eq!(leaf.rows[0].get("campus"), Some(&Value::from("SOUTH")));

            // Editing through a parent updates the same inherited value/version.
            let tx = executor.begin().await.unwrap();
            tx.mutate(mutation(MutationCommand::Update(
                UpdateCommand::new("School", id)
                    .expected_version(2)
                    .value("name", "Parent edit"),
            )))
            .await
            .unwrap();
            tx.commit().await.unwrap();
            let leaf = executor.query(query("TrainingAcademy", id)).await.unwrap();
            assert_eq!(leaf.rows[0].get("version"), Some(&Value::I64(3)));
            assert_eq!(leaf.rows[0].get("name"), Some(&Value::from("Parent edit")));

            let tx = executor.begin().await.unwrap();
            assert!(
                tx.mutate(mutation(MutationCommand::Update(
                    UpdateCommand::new("TrainingAcademy", id)
                        .expected_version(2)
                        .value("campus", "STALE")
                )))
                .await
                .is_err()
            );
            tx.rollback().await.unwrap();
            let leaf = executor.query(query("TrainingAcademy", id)).await.unwrap();
            assert_eq!(leaf.rows[0].get("campus"), Some(&Value::from("SOUTH")));
            assert_eq!(leaf.rows[0].get("version"), Some(&Value::I64(3)));
        }
        let db = connection.lock().unwrap();
        for (table, expected) in [
            ("school_data", vec!["id", "version", "Legacy Name"]),
            ("academy_data", vec!["id", "campus"]),
            ("training_academy_data", vec!["id", "specialty"]),
        ] {
            let mut statement = db.prepare(&format!("PRAGMA table_info({table})")).unwrap();
            let fields = statement
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(fields, expected, "inherited values must not be duplicated");
        }
    });
}

#[test]
fn inherited_failure_rolls_back_ancestors_and_autocommit_is_rejected_before_writes() {
    futures_executor::block_on(async {
        let (executor, connection) = fixture();
        assert!(
            executor
                .mutate(insert(1))
                .await
                .unwrap_err()
                .to_string()
                .contains("explicit transaction")
        );
        connection.lock().unwrap().execute_batch(
            "CREATE TRIGGER reject_training BEFORE INSERT ON training_academy_data BEGIN SELECT RAISE(ABORT, 'fixture rejection'); END;"
        ).unwrap();
        for id in [1_i64, 2] {
            let tx = executor.begin().await.unwrap();
            assert!(tx.mutate(insert(id)).await.is_err());
            tx.rollback().await.unwrap();
            for entity in ["School", "Academy", "TrainingAcademy"] {
                assert!(
                    executor
                        .query(query(entity, id))
                        .await
                        .unwrap()
                        .rows
                        .is_empty()
                );
            }
        }
    });
}

#[test]
fn inherited_delete_and_recovery_change_only_the_shared_version() {
    futures_executor::block_on(async {
        let (executor, _) = fixture();
        for id in [1_i64, 2] {
            let tx = executor.begin().await.unwrap();
            tx.mutate(insert(id)).await.unwrap();
            tx.commit().await.unwrap();
            let tx = executor.begin().await.unwrap();
            let delete = DeleteCommand::new("TrainingAcademy", id).expected_version(1);
            let result = tx
                .mutate(mutation(MutationCommand::Delete(delete)))
                .await
                .unwrap();
            assert_eq!(
                result.persisted_snapshot.unwrap().get("version"),
                Some(&Value::I64(-2))
            );
            tx.commit().await.unwrap();
            let tx = executor.begin().await.unwrap();
            tx.mutate(mutation(MutationCommand::Recover(
                RecoverCommand::new("TrainingAcademy", id, -2),
            )))
            .await
            .unwrap();
            tx.commit().await.unwrap();
            let leaf = executor.query(query("TrainingAcademy", id)).await.unwrap();
            assert_eq!(leaf.rows[0].get("version"), Some(&Value::I64(3)));
            assert_eq!(
                leaf.rows[0].get("specialty"),
                Some(&Value::from("robotics"))
            );
        }
    });
}
