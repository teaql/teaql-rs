use std::sync::Arc;

use teaql_core::{
    DataType, DeleteCommand, EntityDescriptor, Expr, InsertCommand, PropertyDescriptor,
    RecoverCommand, UpdateCommand,
};
use teaql_data_service::{
    GuardedMutationExecutor, GuardedMutationRequest, MutationRequest, SchemaProvider, Transaction,
    TransactionExecutor,
};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor};
use teaql_sql::{SqlDataServiceExecutor, SqlDialect};

#[derive(Clone)]
struct Schema(Arc<EntityDescriptor>);

impl SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        (self.0.name == name).then(|| self.0.clone())
    }
}

fn order_entity() -> EntityDescriptor {
    EntityDescriptor::new("Order")
        .table_name("orders")
        .property(PropertyDescriptor::new("id", DataType::I64).id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new("tenant_id", DataType::I64).not_null())
        .property(PropertyDescriptor::new("name", DataType::Text).not_null())
}

#[test]
fn tenant_guard_is_atomic_for_update_delete_and_recover() {
    futures_executor::block_on(async {
        let entity = Arc::new(order_entity());
        let transport = SqliteMutationExecutor::from_connection(
            rusqlite::Connection::open_in_memory().expect("in-memory sqlite"),
        );
        transport
            .connection()
            .lock()
            .expect("sqlite connection")
            .execute_batch(
                "CREATE TABLE orders (\
                    id INTEGER PRIMARY KEY NOT NULL, \
                    version INTEGER NOT NULL, \
                    tenant_id INTEGER NOT NULL, \
                    name TEXT NOT NULL\
                )",
            )
            .expect("create table");
        for (id, tenant, name) in [
            (1_i64, 1_i64, "tenant-one"),
            (2, 2, "tenant-two"),
            (3, 2, "tenant-two-hard-delete"),
        ] {
            let insert = InsertCommand::new("Order")
                .value("id", id)
                .value("version", 1_i64)
                .value("tenant_id", tenant)
                .value("name", name);
            transport
                .execute(
                    &SqliteDialect
                        .compile_insert(&entity, &insert)
                        .expect("insert SQL"),
                )
                .expect("seed row");
        }
        let executor =
            SqlDataServiceExecutor::new(SqliteDialect, transport.clone(), Schema(entity.clone()));
        let tenant_one = || Expr::eq("tenant_id", 1_i64);
        let tenant_two = || Expr::eq("tenant_id", 2_i64);

        let cross_tenant_update = executor
            .mutate_guarded(GuardedMutationRequest::new(
                MutationRequest::Update(
                    UpdateCommand::new("Order", 2_i64)
                        .expected_version(1)
                        .value("name", "stolen"),
                ),
                tenant_one(),
            ))
            .await
            .expect("guarded update");
        assert_eq!(cross_tenant_update.affected_rows, 0);

        let own_update = executor
            .mutate_guarded(GuardedMutationRequest::new(
                MutationRequest::Update(
                    UpdateCommand::new("Order", 1_i64)
                        .expected_version(1)
                        .value("name", "updated"),
                ),
                tenant_one(),
            ))
            .await
            .expect("same-tenant update");
        assert_eq!(own_update.affected_rows, 1);

        let cross_tenant_delete = executor
            .mutate_guarded(GuardedMutationRequest::new(
                MutationRequest::Delete(DeleteCommand::new("Order", 2_i64).expected_version(1)),
                tenant_one(),
            ))
            .await
            .expect("guarded delete");
        assert_eq!(cross_tenant_delete.affected_rows, 0);

        let own_delete = executor
            .mutate_guarded(GuardedMutationRequest::new(
                MutationRequest::Delete(DeleteCommand::new("Order", 2_i64).expected_version(1)),
                tenant_two(),
            ))
            .await
            .expect("same-tenant delete");
        assert_eq!(own_delete.affected_rows, 1);

        let cross_tenant_recover = executor
            .mutate_guarded(GuardedMutationRequest::new(
                MutationRequest::Recover(RecoverCommand::new("Order", 2_i64, -2)),
                tenant_one(),
            ))
            .await
            .expect("guarded recover");
        assert_eq!(cross_tenant_recover.affected_rows, 0);

        let own_recover = executor
            .mutate_guarded(GuardedMutationRequest::new(
                MutationRequest::Recover(RecoverCommand::new("Order", 2_i64, -2)),
                tenant_two(),
            ))
            .await
            .expect("same-tenant recover");
        assert_eq!(own_recover.affected_rows, 1);

        let cross_tenant_hard_delete = executor
            .mutate_guarded(GuardedMutationRequest::new(
                MutationRequest::Delete(
                    DeleteCommand::new("Order", 3_i64)
                        .expected_version(1)
                        .hard_delete(),
                ),
                tenant_one(),
            ))
            .await
            .expect("guarded hard delete");
        assert_eq!(cross_tenant_hard_delete.affected_rows, 0);

        let own_hard_delete = executor
            .mutate_guarded(GuardedMutationRequest::new(
                MutationRequest::Delete(
                    DeleteCommand::new("Order", 3_i64)
                        .expected_version(1)
                        .hard_delete(),
                ),
                tenant_two(),
            ))
            .await
            .expect("same-tenant hard delete");
        assert_eq!(own_hard_delete.affected_rows, 1);

        let transaction = executor.begin().await.expect("begin transaction");
        let transactional_cross_tenant_update = transaction
            .mutate_guarded(GuardedMutationRequest::new(
                MutationRequest::Update(
                    UpdateCommand::new("Order", 2_i64)
                        .expected_version(3)
                        .value("name", "transactional theft"),
                ),
                tenant_one(),
            ))
            .await
            .expect("transactional guarded update");
        assert_eq!(transactional_cross_tenant_update.affected_rows, 0);
        transaction.rollback().await.expect("rollback transaction");

        let connection = transport.connection();
        let connection = connection.lock().expect("sqlite connection");
        let tenant_two_row: (i64, i64, String) = connection
            .query_row(
                "SELECT tenant_id, version, name FROM orders WHERE id = 2",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("tenant two row");
        assert_eq!(tenant_two_row, (2, 3, "tenant-two".to_owned()));
        let tenant_one_name: String = connection
            .query_row("SELECT name FROM orders WHERE id = 1", [], |row| row.get(0))
            .expect("tenant one row");
        assert_eq!(tenant_one_name, "updated");
        let hard_deleted_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM orders WHERE id = 3", [], |row| {
                row.get(0)
            })
            .expect("hard-delete count");
        assert_eq!(hard_deleted_count, 0);
    });
}
