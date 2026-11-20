//! Test-only fault below the normal SQL compiler. Writes and pre-write reads
//! delegate unchanged to SQLite; only the first post-write read is rejected.
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use teaql_provider_sqlite::MutationExecutorError;
use teaql_sql::{CompiledQuery, SqlTransaction, SqlTransactionTransport, SqlTransport};
use trace_chain_service_core::teaql_core::CompactRow;

pub struct ReadbackFault<T> {
    inner: T,
    wrote: Arc<AtomicBool>,
    remaining_writes: Option<Arc<AtomicUsize>>,
}

impl<T> ReadbackFault<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            wrote: Arc::new(AtomicBool::new(false)),
            remaining_writes: None,
        }
    }

    /// Inject once after the requested successful write; retry uses real reads.
    pub fn after_writes(inner: T, count: usize) -> Self {
        assert!(count > 0);
        Self {
            inner,
            wrote: Arc::new(AtomicBool::new(false)),
            remaining_writes: Some(Arc::new(AtomicUsize::new(count))),
        }
    }
}

impl<T: SqlTransport<Error = MutationExecutorError>> SqlTransport for ReadbackFault<T> {
    type Error = MutationExecutorError;

    async fn fetch_all_compact_sql(
        &self,
        query: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, Self::Error> {
        if self.wrote.swap(false, Ordering::SeqCst) {
            return Err(MutationExecutorError::Bind(
                "INJECTED_READBACK_FAILURE".to_owned(),
            ));
        }
        self.inner.fetch_all_compact_sql(query).await
    }

    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        let affected = self.inner.execute_sql(query).await?;
        let fail_next_read = self.remaining_writes.as_ref().is_none_or(|remaining| {
            remaining.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                == Ok(1)
        });
        self.wrote.store(fail_next_read, Ordering::SeqCst);
        Ok(affected)
    }
}

impl<T: SqlTransactionTransport<Error = MutationExecutorError>> SqlTransactionTransport
    for ReadbackFault<T>
{
    type Tx<'a>
        = ReadbackFault<T::Tx<'a>>
    where
        Self: 'a;

    async fn begin_sql(&self) -> Result<Self::Tx<'_>, Self::Error> {
        Ok(ReadbackFault {
            inner: self.inner.begin_sql().await?,
            wrote: self.wrote.clone(),
            remaining_writes: self.remaining_writes.clone(),
        })
    }
}

impl<T: SqlTransaction<Error = MutationExecutorError> + Send> SqlTransaction for ReadbackFault<T> {
    type Error = MutationExecutorError;
    async fn commit_sql(self) -> Result<(), Self::Error> {
        self.inner.commit_sql().await
    }
    async fn rollback_sql(self) -> Result<(), Self::Error> {
        self.inner.rollback_sql().await
    }
}
