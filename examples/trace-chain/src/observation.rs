//! Runtime SPI observation only. Delegates commands, transactions and metadata
//! unchanged; no test-generated trace nodes are supplied to the executor.
use std::sync::{Arc, Mutex};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, ExecutionMetadata, ExecutionObserver,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
    Transaction, TransactionExecutor,
};

type BeforeBegin = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone, Default)]
pub struct Observation {
    commands: Arc<Mutex<Vec<MutationRequest>>>,
    metadata: Arc<Mutex<Vec<ExecutionMetadata>>>,
    begin_barrier: Arc<Mutex<Option<Arc<tokio::sync::Barrier>>>>,
    before_begin: Arc<Mutex<Option<BeforeBegin>>>,
}

impl Observation {
    /// Observe runtime-owned planning state once, without changing the request.
    pub fn observe_next_begin(&self, observe: impl Fn() + Send + Sync + 'static) {
        *self.before_begin.lock().unwrap() = Some(Arc::new(observe));
    }

    pub fn probe_concurrent_begins(&self, enabled: bool) {
        *self.begin_barrier.lock().unwrap() =
            enabled.then(|| Arc::new(tokio::sync::Barrier::new(2)));
    }
    pub fn clear(&self) {
        self.commands.lock().unwrap().clear();
        self.metadata.lock().unwrap().clear();
    }

    pub fn commands(&self) -> Vec<MutationRequest> {
        self.commands.lock().unwrap().clone()
    }

    pub fn metadata(&self) -> Vec<ExecutionMetadata> {
        self.metadata.lock().unwrap().clone()
    }

    pub fn query_metadata_observer(&self) -> Arc<dyn Fn(&ExecutionMetadata) + Send + Sync> {
        let observation = self.clone();
        Arc::new(move |metadata| observation.capture_metadata(metadata))
    }

    fn capture_metadata(&self, metadata: &ExecutionMetadata) {
        if metadata.statements.is_empty() {
            self.metadata.lock().unwrap().push(metadata.clone());
        } else {
            for statement in &metadata.statements {
                self.capture_metadata(statement);
            }
        }
    }

    fn capture_command(&self, request: &MutationRequest) {
        if let teaql_data_service::MutationCommand::Batch(children) = &request.command {
            for child in children {
                self.capture_command(child);
            }
        } else {
            self.commands.lock().unwrap().push(request.clone());
        }
    }

    fn failure_observer<'a>(
        &self,
        upstream: Option<ExecutionObserver<'a>>,
    ) -> ExecutionObserver<'a> {
        let observation = self.clone();
        Arc::new(move |metadata| {
            observation.capture_metadata(&metadata);
            if let Some(upstream) = &upstream {
                upstream(metadata);
            }
        })
    }
}

pub struct Observed<E> {
    inner: E,
    observation: Observation,
}

impl<E> Observed<E> {
    pub fn new(inner: E, observation: Observation) -> Self {
        Self { inner, observation }
    }
}

impl<E: DataServiceExecutor> DataServiceExecutor for Observed<E> {
    type Error = E::Error;

    fn capabilities(&self) -> DataServiceCapabilities {
        self.inner.capabilities()
    }
}

impl<E: QueryExecutor + Send + Sync> QueryExecutor for Observed<E> {
    fn query_log_intent(
        &self,
        query: &trace_chain_service_core::teaql_core::SelectQuery,
    ) -> teaql_data_service::SqlIntentRedactions {
        self.inner.query_log_intent(query)
    }

    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
        self.query_observed(request, None).await
    }

    async fn query_observed<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<QueryResult, Self::Error> {
        let observer = self.observation.failure_observer(observer);
        let result = self.inner.query_observed(request, Some(observer)).await?;
        self.observation.capture_metadata(&result.metadata);
        Ok(result)
    }
}

impl<E: MutationExecutor + Send + Sync> MutationExecutor for Observed<E> {
    async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
        self.mutate_observed(request, None).await
    }

    async fn mutate_observed<'a>(
        &'a self,
        request: MutationRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<MutationResult, Self::Error> {
        self.observation.capture_command(&request);
        let observer = self.observation.failure_observer(observer);
        let result = self.inner.mutate_observed(request, Some(observer)).await?;
        self.observation.capture_metadata(&result.metadata);
        Ok(result)
    }
}

impl<E: TransactionExecutor + Send + Sync> TransactionExecutor for Observed<E>
where
    for<'tx> E::Tx<'tx>: Send + Sync,
{
    type Tx<'a>
        = Observed<E::Tx<'a>>
    where
        Self: 'a;

    async fn begin(&self) -> Result<Self::Tx<'_>, Self::Error> {
        let observe = { self.observation.before_begin.lock().unwrap().take() };
        if let Some(observe) = observe {
            observe();
        }
        let barrier = self.observation.begin_barrier.lock().unwrap().clone();
        if let Some(barrier) = &barrier {
            // Both generated saves reach begin before either enters SQLite.
            // No identity, reason, command or metadata is fabricated here.
            barrier.wait().await;
        }
        let transaction = self.inner.begin().await?;
        if barrier.is_some() {
            tokio::task::yield_now().await;
        }
        Ok(Observed::new(transaction, self.observation.clone()))
    }
}

impl<E: Transaction + Send> Transaction for Observed<E> {
    type Error = E::Error;

    async fn commit(self) -> Result<(), Self::Error> {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        self.inner.rollback().await
    }
}
