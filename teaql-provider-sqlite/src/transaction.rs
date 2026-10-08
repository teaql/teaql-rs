//! Operation-owned connection lease, not a Context/global transaction flag (#240).
use super::{CompactRow, CompiledQuery, MutationExecutorError, SqliteMutationExecutor, Value};
use teaql_sql::{SqlTransaction, SqlTransactionTransport, SqlTransport, StreamingSqlTransport};

/// One exclusive transaction on the provider's shared connection. It cannot be
/// cloned; drop rolls back before releasing the asynchronous connection lease.
/// Generated Context Q/E/save APIs retain their existing spelling.
pub struct SqliteTransaction {
    pub(super) executor: SqliteMutationExecutor,
    _lease: futures_util::lock::OwnedMutexGuard<()>,
    pub(super) active: bool,
}

impl SqlTransactionTransport for SqliteMutationExecutor {
    type Tx<'a>
        = SqliteTransaction
    where
        Self: 'a;

    async fn begin_sql(&self) -> Result<Self::Tx<'_>, Self::Error> {
        let lease = self.transaction_lease.clone().lock_owned().await;
        self.begin_transaction()?;
        Ok(SqliteTransaction {
            executor: self.clone(),
            _lease: lease,
            active: true,
        })
    }
}

impl SqlTransport for SqliteTransaction {
    type Error = MutationExecutorError;

    fn dynamic_field_store(
        &self,
    ) -> Option<&dyn teaql_data_service::dynamic_fields::DynamicFieldStore> {
        Some(self)
    }

    async fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, Self::Error> {
        self.executor.fetch_all_compact(query)
    }

    async fn fetch_repeated_compact_sql(
        &self,
        template: &CompiledQuery,
        param_index: usize,
        values: &[Value],
    ) -> Result<Vec<CompactRow>, Self::Error> {
        self.executor
            .fetch_repeated_compact_unleased(template, param_index, values)
    }

    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        self.executor.execute(query)
    }
}

impl StreamingSqlTransport for SqliteTransaction {
    fn stream_sql(
        &self,
        query: CompiledQuery,
        chunk_size: usize,
    ) -> teaql_data_service::QueryStream<'_, Self::Error> {
        self.executor.stream_sql_unleased(query, chunk_size, false)
    }
}

impl SqlTransaction for SqliteTransaction {
    type Error = MutationExecutorError;

    async fn commit_sql(mut self) -> Result<(), Self::Error> {
        self.executor.commit_transaction()?;
        self.active = false;
        Ok(())
    }

    async fn rollback_sql(mut self) -> Result<(), Self::Error> {
        self.executor.rollback_transaction()?;
        self.active = false;
        Ok(())
    }
}

impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        if self.active {
            let _ = self.executor.rollback_transaction();
        }
    }
}
