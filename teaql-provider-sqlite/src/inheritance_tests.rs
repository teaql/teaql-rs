use super::*;
use teaql_core::{
    DeleteCommand, InsertCommand, PropertyDescriptor as P, RecoverCommand, SelectQuery,
    UpdateCommand,
};
use teaql_data_service::{
    GuardedMutationExecutor, GuardedMutationRequest, MutationCommand, MutationExecutor,
    MutationRequest, QueryExecutor, QueryRequest, SchemaProvider as QuerySchema, Transaction,
    TransactionExecutor,
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

#[test]
fn inherited_guards_are_atomic_at_all_depths_and_cannot_escape_on_readback() {
    futures_executor::block_on(async {
        let (executor, connection) = fixture();
        for id in [1_i64, 2] {
            let tx = executor.begin().await.unwrap();
            tx.mutate(insert(id)).await.unwrap();
            tx.commit().await.unwrap();
        }
        for id in [1_i64, 2] {
            let update = || {
                mutation(MutationCommand::Update(
                    UpdateCommand::new("TrainingAcademy", id)
                        .expected_version(1)
                        .value("name", "Approved school")
                        .value("specialty", "mechanics"),
                ))
            };
            assert!(
                executor
                    .mutate_guarded(GuardedMutationRequest::new(
                        update(),
                        Expr::eq("campus", "NORTH")
                    ))
                    .await
                    .is_err()
            );
            let tx = executor.begin().await.unwrap();
            let denied = tx
                .mutate_guarded(GuardedMutationRequest::new(
                    update(),
                    Expr::eq("specialty", "forbidden"),
                ))
                .await
                .unwrap();
            assert_eq!(denied.affected_rows, 0);
            tx.commit().await.unwrap();
            let tx = executor.begin().await.unwrap();
            let allowed = tx
                .mutate_guarded(GuardedMutationRequest::new(
                    update(),
                    Expr::and([
                        Expr::eq("name", "Original school"),
                        Expr::eq("campus", "NORTH"),
                        Expr::eq("specialty", "robotics"),
                    ]),
                ))
                .await;
            // Changing a field in the guard makes authoritative readback fail.
            assert!(allowed.is_err());
            tx.rollback().await.unwrap();
            let tx = executor.begin().await.unwrap();
            let allowed = tx
                .mutate_guarded(GuardedMutationRequest::new(
                    update(),
                    Expr::eq("campus", "NORTH"),
                ))
                .await
                .unwrap();
            assert_eq!(allowed.affected_rows, 1);
            assert_eq!(
                allowed.persisted_snapshot.unwrap().get("version"),
                Some(&Value::I64(2))
            );
            tx.commit().await.unwrap();
            let tx = executor.begin().await.unwrap();
            assert!(
                tx.mutate_guarded(GuardedMutationRequest::new(
                    mutation(MutationCommand::Update(
                        UpdateCommand::new("TrainingAcademy", id)
                            .expected_version(2)
                            .value("campus", "SOUTH")
                    )),
                    Expr::eq("campus", "NORTH")
                ))
                .await
                .is_err()
            );
            tx.rollback().await.unwrap();
            let leaf = executor.query(query("TrainingAcademy", id)).await.unwrap();
            assert_eq!(leaf.rows[0].get("version"), Some(&Value::I64(2)));
            assert_eq!(leaf.rows[0].get("campus"), Some(&Value::from("NORTH")));

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
        assert_eq!(
            connection
                .lock()
                .unwrap()
                .query_row("SELECT count(*) FROM school_data", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    });
}

#[test]
fn inherited_hard_delete_rolls_back_when_the_final_root_version_conflicts() {
    futures_executor::block_on(async {
        let (executor, connection) = fixture();
        connection.lock().unwrap().execute_batch(
            "CREATE TRIGGER change_version AFTER DELETE ON training_academy_data BEGIN UPDATE school_data SET version=version+1 WHERE id=old.id; END;"
        ).unwrap();
        for id in [1_i64, 2] {
            let tx = executor.begin().await.unwrap();
            tx.mutate(insert(id)).await.unwrap();
            tx.commit().await.unwrap();
            let tx = executor.begin().await.unwrap();
            assert!(
                tx.mutate_guarded(GuardedMutationRequest::new(
                    mutation(MutationCommand::Delete(
                        DeleteCommand::new("TrainingAcademy", id)
                            .expected_version(1)
                            .hard_delete()
                    )),
                    Expr::eq("campus", "NORTH")
                ))
                .await
                .is_err()
            );
            tx.rollback().await.unwrap();
            let leaf = executor.query(query("TrainingAcademy", id)).await.unwrap();
            assert_eq!(leaf.rows.len(), 1);
            assert_eq!(leaf.rows[0].get("version"), Some(&Value::I64(1)));
        }
    });
}

#[test]
fn legacy_flattened_inheritance_requires_explicit_migration_without_ddl_or_data_changes() {
    let root = EntityDescriptor::new("School")
        .property(P::new("id", DataType::I64).id())
        .property(P::new("version", DataType::I64).version())
        .property(P::new("name", DataType::Text));
    let child = EntityDescriptor::new("Academy")
        .inherits(root)
        .property(P::new("campus", DataType::Text));
    let transport = SqliteMutationExecutor::from_connection(Connection::open_in_memory().unwrap());
    transport.connection().lock().unwrap().execute_batch(
        "CREATE TABLE academy_data(id INTEGER PRIMARY KEY,version INTEGER NOT NULL,name TEXT,campus TEXT);
         INSERT INTO academy_data VALUES(7,1,'Legacy academy','NORTH');"
    ).unwrap();
    for _ in 0..2 {
        let error = transport
            .ensure_schema(&SqliteDialect, &[&child])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("migrate flattened inherited storage explicitly"),
            "{error}"
        );
        assert!(
            error.contains("academy_data") && error.contains("inherited column"),
            "{error}"
        );
        let connection = transport.connection();
        let db = connection.lock().unwrap();
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            db.query_row(
                "SELECT name FROM academy_data WHERE id=7 AND version=1",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "Legacy academy"
        );
    }
}

#[test]
fn inherited_privacy_includes_future_table_values_before_the_first_statement() {
    futures_executor::block_on(async {
        let (executor, _) = fixture();
        let tx = executor.begin().await.unwrap();
        let mut request = insert(1);
        if let MutationCommand::Insert(command) = &mut request.command {
            command
                .values
                .insert("specialty".into(), Value::from("LATER-TABLE-SECRET"));
        }
        // Successful diagnostics are returned in the result; the observer is
        // reserved for exceptional exits, not a second success notification.
        let result = tx.mutate(request).await.unwrap();
        tx.commit().await.unwrap();
        let first = &result.metadata.statements[0];
        assert!(
            first
                .parameterized_query
                .as_ref()
                .unwrap()
                .contains("school_data")
        );
        let mut secrets = Vec::new();
        first
            .sql_log
            .intent_redactions
            .extend_secrets(false, &mut secrets);
        assert!(
            secrets.contains(&"LATER-TABLE-SECRET".to_owned()),
            "later child values must already be scrubbed from the root reason"
        );
    });
}
type Executor = teaql_sql::SqlDataServiceExecutor<SqliteDialect, SqliteMutationExecutor, Schema>;

fn fixture() -> (Executor, Arc<Mutex<Connection>>) {
    fixture_with_log_policies(false)
}

fn fixture_with_log_policies(explicit: bool) -> (Executor, Arc<Mutex<Connection>>) {
    let mut school = EntityDescriptor::new("School")
        .inheritance_child("Academy", "academy_data", "id")
        .property(P::new("id", DataType::I64).id().not_null())
        .property(P::new("version", DataType::I64).version().not_null())
        .property(
            P::new("name", DataType::Text)
                .column_name("Legacy Name")
                .not_null(),
        );
    if explicit {
        school = school.audit_mask_fields(vec![]);
    }
    let mut academy = EntityDescriptor::new("Academy")
        .inheritance_child("TrainingAcademy", "training_academy_data", "id")
        .inherits(school.clone())
        .property(P::new("campus", DataType::Text).not_null());
    if explicit {
        academy = academy.audit_mask_fields(vec!["campus".into()]);
    }
    let mut training = EntityDescriptor::new("TrainingAcademy")
        .inherits(academy.clone())
        .property(P::new("specialty", DataType::Text).not_null());
    if explicit {
        training = training.audit_mask_fields(vec!["name".into(), "specialty".into()]);
    }
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

#[test]
fn inherited_physical_statements_keep_the_logical_subtypes_stricter_mask_policy() {
    futures_executor::block_on(async {
        let (executor, _) = fixture_with_log_policies(true);
        let tx = executor.begin().await.unwrap();
        let mut request = insert(1);
        if let MutationCommand::Insert(command) = &mut request.command {
            command
                .values
                .insert("name".into(), Value::from("CHILD-PRIVATE-NAME"));
            command
                .values
                .insert("campus".into(), Value::from("MIDDLE-PRIVATE-CAMPUS"));
        }
        let result = tx.mutate(request).await.unwrap();
        tx.commit().await.unwrap();
        let first = &result.metadata.statements[0];
        let index = first
            .params
            .iter()
            .position(|value| value == &Value::from("CHILD-PRIVATE-NAME"))
            .unwrap();
        assert_eq!(
            first.sql_log.parameter_policies[index],
            teaql_data_service::SqlParameterLogPolicy::Masked
        );
        let mut secrets = Vec::new();
        first
            .sql_log
            .intent_redactions
            .extend_secrets(false, &mut secrets);
        assert!(secrets.contains(&"MIDDLE-PRIVATE-CAMPUS".to_owned()));
    });
}

#[test]
fn typed_parent_and_middle_views_keep_next_subtype_identity_without_schema_changes() {
    futures_executor::block_on(async {
        let (executor, _) = fixture();
        let tx = executor.begin().await.unwrap();
        tx.mutate(mutation(MutationCommand::Insert(
            InsertCommand::new("School")
                .value("id", 1_i64)
                .value("version", 1_i64)
                .value("name", "Base"),
        )))
        .await
        .unwrap();
        tx.mutate(insert(2)).await.unwrap();
        tx.commit().await.unwrap();
        let base = executor.query(query("School", 1)).await.unwrap();
        assert_eq!(base.rows[0].get("_type_"), Some(&Value::from("School")));
        let parent = executor.query(query("School", 2)).await.unwrap();
        assert_eq!(parent.rows[0].get("_type_"), Some(&Value::from("Academy")));
        let middle = executor.query(query("Academy", 2)).await.unwrap();
        assert_eq!(
            middle.rows[0].get("_type_"),
            Some(&Value::from("TrainingAcademy"))
        );
        let leaf = executor.query(query("TrainingAcademy", 2)).await.unwrap();
        assert!(leaf.rows[0].get("_type_").is_none());
    });
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
            tx.mutate(mutation(MutationCommand::Recover(RecoverCommand::new(
                "TrainingAcademy",
                id,
                -2,
            ))))
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

#[test]
fn inherited_recovery_can_correct_declaring_fields_with_one_root_version() {
    futures_executor::block_on(async {
        let (executor, _) = fixture();
        let tx = executor.begin().await.unwrap();
        tx.mutate(insert(41)).await.unwrap();
        tx.mutate(mutation(MutationCommand::Delete(
            DeleteCommand::new("TrainingAcademy", 41_i64).expected_version(1),
        )))
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let command = RecoverCommand::new("TrainingAcademy", 41_i64, -2)
            .value("name", "Recovered school")
            .value("campus", "WEST")
            .value("specialty", "mechanics");
        let tx = executor.begin().await.unwrap();
        let denied = tx
            .mutate_guarded(GuardedMutationRequest::new(
                mutation(MutationCommand::Recover(command.clone())),
                Expr::eq("campus", "forbidden"),
            ))
            .await
            .unwrap();
        assert_eq!(denied.affected_rows, 0);
        tx.commit().await.unwrap();
        let tx = executor.begin().await.unwrap();
        let recovered = tx
            .mutate(mutation(MutationCommand::Recover(command.clone())))
            .await
            .unwrap();
        assert_eq!(recovered.affected_rows, 1);
        let row = recovered.persisted_snapshot.unwrap();
        for (field, value) in [
            ("name", Value::from("Recovered school")),
            ("campus", Value::from("WEST")),
            ("specialty", Value::from("mechanics")),
            ("version", Value::I64(3)),
        ] {
            assert_eq!(row.get(field), Some(&value));
        }
        tx.commit().await.unwrap();
        let tx = executor.begin().await.unwrap();
        assert!(
            tx.mutate(mutation(MutationCommand::Recover(command)))
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
        assert_eq!(
            executor.query(query("School", 41)).await.unwrap().rows[0].get("version"),
            Some(&Value::I64(3))
        );
    });
}
