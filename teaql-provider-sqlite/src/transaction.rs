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

    async fn append_committed_set_sql(
        &self,
        append: &teaql_data_service::CommittedSetAppend,
    ) -> Result<bool, Self::Error> {
        let mut payload: serde_json::Value = serde_json::from_str(&append.payload_json)
            .map_err(|_| MutationExecutorError::Bind("invalid committed-event JSON".into()))?;
        let fields = payload.as_object().ok_or_else(|| {
            MutationExecutorError::Bind("committed-event payload must be an object".into())
        })?;
        if fields.contains_key("source_sequence") {
            return Err(MutationExecutorError::Bind(
                "committed-event sequence must be allocated by the transaction".into(),
            ));
        }
        SqliteTransaction::append_committed_event(
            self,
            &append.source_runtime_id,
            &append.change_set_id,
            &append.model_hash,
            |sequence| {
                payload
                    .as_object_mut()
                    .expect("validated JSON object")
                    .insert(
                        "source_sequence".into(),
                        serde_json::Value::String(sequence.to_string()),
                    );
                serde_json::to_string(&payload).map_err(Into::into)
            },
        )
        .map_err(|_| MutationExecutorError::Bind("committed-event append failed".into()))?;
        Ok(true)
    }

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
