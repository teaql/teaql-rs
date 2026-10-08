//! Test-only forwarding observer: measure provider entry, not merely final errors.
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
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
    native_omission: Arc<[AtomicBool; 3]>,
}
impl<E> ObservedExecutor<E> {
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            calls: Arc::new(std::array::from_fn(|_| AtomicUsize::new(0))),
            native_omission: Arc::new(std::array::from_fn(|_| AtomicBool::new(false))),
        }
    }
    /// query, mutation, begin, commit, rollback
    pub fn counts(&self) -> [usize; 5] {
        std::array::from_fn(|i| self.calls[i].load(Ordering::SeqCst))
    }
    pub fn omit_native_date_after_write(&self) {
        self.native_omission[1].store(false, Ordering::SeqCst);
        self.native_omission[2].store(false, Ordering::SeqCst);
        self.native_omission[0].store(true, Ordering::SeqCst);
    }
    pub fn native_omission_observed(&self) -> bool {
        self.native_omission[2].load(Ordering::SeqCst)
    }
    pub fn clear_native_omission(&self) {
        self.native_omission[0].store(false, Ordering::SeqCst);
    }
    fn after_write(&self) {
        if self.native_omission[0].load(Ordering::SeqCst) {
            self.native_omission[1].store(true, Ordering::SeqCst);
        }
    }
    fn readback_view(&self, mut result: QueryResult) -> QueryResult {
        if self.native_omission[1].load(Ordering::SeqCst)
            && result
                .rows
                .iter()
                .any(|row| row.contains_key("established_date"))
            && self.native_omission[0].swap(false, Ordering::SeqCst)
        {
            self.native_omission[2].store(true, Ordering::SeqCst);
            for row in &mut result.rows {
                row.remove("established_date");
            }
        }
        result
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
        self.inner
            .query(request)
            .await
            .map(|result| self.readback_view(result))
    }
    async fn query_observed<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<QueryResult, Self::Error> {
        self.calls[0].fetch_add(1, Ordering::SeqCst);
        self.inner
            .query_observed(request, observer)
            .await
            .map(|result| self.readback_view(result))
    }
}
impl<E: MutationExecutor + Sync> MutationExecutor for ObservedExecutor<E> {
    async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
        self.calls[1].fetch_add(1, Ordering::SeqCst);
        let result = self.inner.mutate(request).await?;
        self.after_write();
        Ok(result)
    }
    async fn mutate_observed<'a>(
        &'a self,
        request: MutationRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<MutationResult, Self::Error> {
        self.calls[1].fetch_add(1, Ordering::SeqCst);
        let result = self.inner.mutate_observed(request, observer).await?;
        self.after_write();
        Ok(result)
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
            native_omission: self.native_omission.clone(),
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
