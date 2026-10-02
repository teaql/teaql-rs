//! SQLite transport-lifetime regression tests, not generated graph proof (#240).
use std::{
    future::Future,
    task::{Context, Poll},
};

use futures_util::StreamExt;
use teaql_core::{
    DataType, EntityDescriptor, InsertCommand, PropertyDescriptor, SelectQuery, Value,
};
use teaql_provider_sqlite::{SqliteDialect, SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::{InMemoryMetadataStore, UserContext};
use teaql_sql::{
    CompiledQuery, SqlDialect, SqlTransaction, SqlTransactionTransport, SqlTransport,
    StreamingSqlTransport,
};

async fn fixture() -> (SqliteMutationExecutor, EntityDescriptor) {
    let descriptor = EntityDescriptor::new("LeaseProbe")
        .table_name("lease_probe")
        .property(PropertyDescriptor::new("id", DataType::I64).id())
        .property(PropertyDescriptor::new("version", DataType::I64).version())
        .property(PropertyDescriptor::new("name", DataType::Text))
        .audit_mask_fields(vec!["name".into()]);
    let executor =
        SqliteMutationExecutor::from_connection(rusqlite::Connection::open_in_memory().unwrap());
    let mut context = UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(descriptor.clone()));
    context.use_sqlite_provider(executor.clone());
    context.ensure_schema().await.unwrap();
    (executor, descriptor)
}

fn insert(descriptor: &EntityDescriptor, id: i64) -> CompiledQuery {
    SqliteDialect
        .compile_insert(
            descriptor,
            &InsertCommand::new("LeaseProbe")
                .value("id", id)
                .value("version", 1_i64)
                .value("name", "lease fixture"),
        )
        .unwrap()
}

fn select(descriptor: &EntityDescriptor) -> CompiledQuery {
    SqliteDialect
        .compile_select(
            descriptor,
            &SelectQuery::new("LeaseProbe")
                .limit(10)
                .comment("inspect leased transport")
                .purpose("verify transaction isolation"),
        )
        .unwrap()
}

#[test]
fn ordinary_root_io_waits_until_the_transaction_releases_its_lease() {
    futures_executor::block_on(async {
        let (executor, descriptor) = fixture().await;
        let transaction = executor.begin_sql().await.unwrap();
        let compiled = insert(&descriptor, 1);
        let mut root_write = Box::pin(executor.execute_sql(&compiled));
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        assert!(root_write.as_mut().poll(&mut cx).is_pending());
        transaction.rollback_sql().await.unwrap();
        assert_eq!(root_write.await.unwrap(), 1);
        let rows = executor
            .fetch_all_compact_sql(&select(&descriptor))
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "root write must occur after, not inside, rolled-back transaction"
        );
    });
}

#[test]
fn cancelled_waiter_and_dropped_transaction_rollback_and_leave_connection_reusable() {
    futures_executor::block_on(async {
        let (executor, descriptor) = fixture().await;
        let transaction = executor.begin_sql().await.unwrap();
        transaction
            .execute_sql(&insert(&descriptor, 1))
            .await
            .unwrap();
        let mut waiting = Box::pin(executor.begin_sql());
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        drop(waiting);
        drop(transaction);
        assert!(
            executor
                .fetch_all_compact_sql(&select(&descriptor))
                .await
                .unwrap()
                .is_empty()
        );
        let next = executor.begin_sql().await.unwrap();
        next.execute_sql(&insert(&descriptor, 1)).await.unwrap();
        next.commit_sql().await.unwrap();
        assert_eq!(
            executor
                .fetch_all_compact_sql(&select(&descriptor))
                .await
                .unwrap()
                .len(),
            1
        );
    });
}

#[test]
fn transaction_stream_and_repeated_probe_work_without_reacquiring_the_lease() {
    futures_executor::block_on(async {
        let (executor, descriptor) = fixture().await;
        let transaction = executor.begin_sql().await.unwrap();
        for id in 1..=3 {
            transaction
                .execute_sql(&insert(&descriptor, id))
                .await
                .unwrap();
        }
        let repeated = SqliteDialect
            .compile_select(
                &descriptor,
                &SelectQuery::new("LeaseProbe")
                    .filter(teaql_core::Expr::eq("id", 1_i64))
                    .limit(1)
                    .comment("probe leased rows")
                    .purpose("verify prepared statement reuse"),
            )
            .unwrap();
        let rows = transaction
            .fetch_repeated_compact_sql(&repeated, 0, &[Value::I64(1), Value::I64(3)])
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id").and_then(Value::try_i64), Some(1));
        assert_eq!(rows[1].get("id").and_then(Value::try_i64), Some(3));
        let mut stream = transaction.stream_sql(select(&descriptor), 2);
        let mut count = 0;
        while let Some(chunk) = stream.next().await {
            count += chunk.unwrap().rows.len();
        }
        assert_eq!(count, 3);
        drop(stream);
        transaction.commit_sql().await.unwrap();
        let mut root_stream = executor.stream_sql(select(&descriptor), 1);
        assert_eq!(root_stream.next().await.unwrap().unwrap().rows.len(), 1);
        let mut waiting = Box::pin(executor.begin_sql());
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        assert!(matches!(waiting.as_mut().poll(&mut cx), Poll::Pending));
        drop(root_stream);
        waiting.await.unwrap().rollback_sql().await.unwrap();
    });
}
