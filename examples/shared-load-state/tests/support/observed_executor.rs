//! Test-only forwarding observer: measure provider entry, not merely final errors.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, ExecutionObserver, MutationExecutor,
    MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult, SqlIntentRedactions,
    Transaction, TransactionExecutor,
};

#[derive(Clone)]
pub struct ObservedExecutor<E> {
    inner: E,
    calls: Arc<[AtomicUsize; 5]>,
}
impl<E> ObservedExecutor<E> {
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            calls: Arc::new(std::array::from_fn(|_| AtomicUsize::new(0))),
        }
    }
    /// query, mutation, begin, commit, rollback
    pub fn counts(&self) -> [usize; 5] {
        std::array::from_fn(|i| self.calls[i].load(Ordering::SeqCst))
    }
}
impl<E: DataServiceExecutor> DataServiceExecutor for ObservedExecutor<E> {
    type Error = E::Error;
    fn capabilities(&self) -> DataServiceCapabilities {
        self.inner.capabilities()
    }
}
impl<E: QueryExecutor + Sync> QueryExecutor for ObservedExecutor<E> {
    fn dynamic_field_store(
        &self,
    ) -> Option<&dyn teaql_data_service::dynamic_fields::DynamicFieldStore> {
        self.inner.dynamic_field_store()
    }
    fn query_log_intent(&self, query: &teaql_core::SelectQuery) -> SqlIntentRedactions {
        self.inner.query_log_intent(query)
    }
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
        self.calls[0].fetch_add(1, Ordering::SeqCst);
        self.inner.query(request).await
    }
    async fn query_observed<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<QueryResult, Self::Error> {
        self.calls[0].fetch_add(1, Ordering::SeqCst);
        self.inner.query_observed(request, observer).await
    }
}
impl<E: MutationExecutor + Sync> MutationExecutor for ObservedExecutor<E> {
    async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
        self.calls[1].fetch_add(1, Ordering::SeqCst);
        self.inner.mutate(request).await
    }
    async fn mutate_observed<'a>(
        &'a self,
        request: MutationRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<MutationResult, Self::Error> {
        self.calls[1].fetch_add(1, Ordering::SeqCst);
        self.inner.mutate_observed(request, observer).await
    }
}
impl<E: TransactionExecutor + Sync + 'static> TransactionExecutor for ObservedExecutor<E>
where
    for<'tx> E::Tx<'tx>: Send + Sync,
{
    type Tx<'a>
        = ObservedExecutor<E::Tx<'a>>
    where
        Self: 'a;
    async fn begin(&self) -> Result<Self::Tx<'_>, Self::Error> {
        self.calls[2].fetch_add(1, Ordering::SeqCst);
        Ok(ObservedExecutor {
            inner: self.inner.begin().await?,
            calls: self.calls.clone(),
        })
    }
}
impl<E: Transaction + Send> Transaction for ObservedExecutor<E> {
    type Error = E::Error;
    async fn commit(self) -> Result<(), Self::Error> {
        self.calls[3].fetch_add(1, Ordering::SeqCst);
        self.inner.commit().await
    }
    async fn rollback(self) -> Result<(), Self::Error> {
        self.calls[4].fetch_add(1, Ordering::SeqCst);
        self.inner.rollback().await
    }
}
