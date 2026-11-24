use std::any::{Any, TypeId};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use teaql_core::SelectQuery;
use teaql_core::dynamic_fields::DynamicFieldError;
use teaql_data_service::dynamic_fields::{DynamicFieldStore, DynamicFieldStreamPlan};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, ExecutionObserver, MutationExecutor,
    MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult, QueryStream,
    SqlIntentRedactions, StreamQueryExecutor, Transaction, TransactionExecutor,
};

use super::transaction::TransactionSaveState;
use crate::{ContextError, RuntimeError, UserContext};

/// Provider capability for a scope whose physical lease does not borrow its registry.
pub trait ContextTransactionExecutor: TransactionExecutor {
    type OwnedTransaction: QueryExecutor<Error = Self::Error>
        + MutationExecutor
        + StreamQueryExecutor
        + Transaction<Error = Self::Error>
        + Send
        + Sync
        + 'static;

    fn begin_context_transaction(
        &self,
    ) -> impl Future<Output = Result<Self::OwnedTransaction, Self::Error>> + Send;
}

type Operation<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

trait Session<E>: Send + Sync {
    fn capabilities(&self) -> DataServiceCapabilities;
    fn store(&self) -> Option<&dyn DynamicFieldStore>;
    fn log_intent(&self, query: &SelectQuery) -> SqlIntentRedactions;
    fn query<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Operation<'a, Result<QueryResult, E>>;
    fn mutate<'a>(
        &'a self,
        request: MutationRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Operation<'a, Result<MutationResult, E>>;
    fn stream<'a>(
        &'a self,
        request: QueryRequest,
        size: usize,
        observer: Option<ExecutionObserver<'a>>,
    ) -> QueryStream<'a, E>;
    fn dynamic_stream<'a>(
        &'a self,
        request: QueryRequest,
        size: usize,
        plan: DynamicFieldStreamPlan,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<QueryStream<'a, E>, DynamicFieldError>;
    fn finish(self: Box<Self>, commit: bool) -> Operation<'static, Result<(), RuntimeError>>;
}

impl<T> Session<<T as DataServiceExecutor>::Error> for T
where
    T: QueryExecutor
        + MutationExecutor
        + StreamQueryExecutor
        + Transaction<Error = <T as DataServiceExecutor>::Error>
        + Send
        + Sync
        + 'static,
{
    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceExecutor::capabilities(self)
    }
    fn store(&self) -> Option<&dyn DynamicFieldStore> {
        self.dynamic_field_store()
    }
    fn log_intent(&self, query: &SelectQuery) -> SqlIntentRedactions {
        self.query_log_intent(query)
    }
    fn query<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Operation<'a, Result<QueryResult, <T as DataServiceExecutor>::Error>> {
        Box::pin(self.query_observed(request, observer))
    }
    fn mutate<'a>(
        &'a self,
        request: MutationRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Operation<'a, Result<MutationResult, <T as DataServiceExecutor>::Error>> {
        Box::pin(self.mutate_observed(request, observer))
    }
    fn stream<'a>(
        &'a self,
        request: QueryRequest,
        size: usize,
        observer: Option<ExecutionObserver<'a>>,
    ) -> QueryStream<'a, <T as DataServiceExecutor>::Error> {
        match observer {
            Some(observer) => self.query_stream_observed(request, size, observer),
            None => self.query_stream(request, size),
        }
    }
    fn dynamic_stream<'a>(
        &'a self,
        request: QueryRequest,
        size: usize,
        plan: DynamicFieldStreamPlan,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<QueryStream<'a, <T as DataServiceExecutor>::Error>, DynamicFieldError> {
        self.query_stream_with_dynamic_fields(request, size, plan, observer)
    }
    fn finish(self: Box<Self>, commit: bool) -> Operation<'static, Result<(), RuntimeError>> {
        Box::pin(async move {
            let result = if commit {
                (*self).commit().await
            } else {
                (*self).rollback().await
            };
            result.map_err(|_| RuntimeError::TransactionOutcomeUnknown)
        })
    }
}

struct TypedSession<E>(Box<dyn Session<E>>);

trait NativeSession: Send + Sync {
    fn typed(&self) -> &(dyn Any + Send + Sync);
    fn capabilities(&self) -> DataServiceCapabilities;
    fn store(&self) -> Option<&dyn DynamicFieldStore>;
    fn log_intent(&self, query: &SelectQuery) -> SqlIntentRedactions;
    fn query<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Operation<'a, Result<QueryResult, RuntimeError>>;
    fn mutate<'a>(
        &'a self,
        request: MutationRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Operation<'a, Result<MutationResult, RuntimeError>>;
    fn finish(self: Box<Self>, commit: bool) -> Operation<'static, Result<(), RuntimeError>>;
}

impl<E: std::error::Error + Send + Sync + 'static> NativeSession for TypedSession<E> {
    fn typed(&self) -> &(dyn Any + Send + Sync) {
        self
    }
    fn capabilities(&self) -> DataServiceCapabilities {
        self.0.capabilities()
    }
    fn store(&self) -> Option<&dyn DynamicFieldStore> {
        self.0.store()
    }
    fn log_intent(&self, query: &SelectQuery) -> SqlIntentRedactions {
        self.0.log_intent(query)
    }
    fn query<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Operation<'a, Result<QueryResult, RuntimeError>> {
        Box::pin(async move {
            self.0
                .query(request, observer)
                .await
                .map_err(|e| RuntimeError::Transaction(e.to_string()))
        })
    }
    fn mutate<'a>(
        &'a self,
        request: MutationRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Operation<'a, Result<MutationResult, RuntimeError>> {
        Box::pin(async move {
            self.0
                .mutate(request, observer)
                .await
                .map_err(|e| RuntimeError::Transaction(e.to_string()))
        })
    }
    fn finish(self: Box<Self>, commit: bool) -> Operation<'static, Result<(), RuntimeError>> {
        self.0.finish(commit)
    }
}

pub(crate) struct BoundTransaction {
    owner: TypeId,
    session: Box<dyn NativeSession>,
    pub(crate) saves: TransactionSaveState,
}

impl BoundTransaction {
    pub(super) fn owner(&self) -> TypeId {
        self.owner
    }
    pub(crate) fn executor<E: 'static>(&self) -> Result<ContextExecutor<'_, E>, ContextError> {
        if self.owner != TypeId::of::<E>() {
            return Err(ContextError::TransactionRouteMismatch);
        }
        Ok(ContextExecutor::Scoped(self.session.typed(), PhantomData))
    }

    pub(crate) fn save_executor(&self) -> impl QueryExecutor + MutationExecutor + Send + Sync + '_ {
        SaveExecutor(self.session.as_ref())
    }

    async fn complete<T>(self, result: Result<T, RuntimeError>) -> Result<T, RuntimeError> {
        let Self {
            session, mut saves, ..
        } = self;
        let result = saves.failure().map_or(result, Err);
        session.finish(result.is_ok()).await?;
        saves.settle(result.is_ok());
        result
    }
}

struct SaveExecutor<'a>(&'a dyn NativeSession);
impl DataServiceExecutor for SaveExecutor<'_> {
    type Error = RuntimeError;
    fn capabilities(&self) -> DataServiceCapabilities {
        self.0.capabilities()
    }
}
impl QueryExecutor for SaveExecutor<'_> {
    fn dynamic_field_store(&self) -> Option<&dyn DynamicFieldStore> {
        self.0.store()
    }
    fn query_log_intent(&self, query: &SelectQuery) -> SqlIntentRedactions {
        self.0.log_intent(query)
    }
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, RuntimeError> {
        self.0.query(request, None).await
    }
    async fn query_observed<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<QueryResult, RuntimeError> {
        self.0.query(request, observer).await
    }
}
impl MutationExecutor for SaveExecutor<'_> {
    async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, RuntimeError> {
        self.0.mutate(request, None).await
    }
    async fn mutate_observed<'a>(
        &'a self,
        request: MutationRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<MutationResult, RuntimeError> {
        self.0.mutate(request, observer).await
    }
}

trait TransactionFactory: Send + Sync {
    fn begin<'a>(
        &'a self,
        context: &'a UserContext,
    ) -> Operation<'a, Result<BoundTransaction, RuntimeError>>;
}

struct ProviderFactory<E>(PhantomData<fn() -> E>);
impl<E: ContextTransactionExecutor + Send + Sync + 'static> TransactionFactory
    for ProviderFactory<E>
{
    fn begin<'a>(
        &'a self,
        context: &'a UserContext,
    ) -> Operation<'a, Result<BoundTransaction, RuntimeError>> {
        Box::pin(async move {
            let provider = context
                .require_resource::<E>()
                .map_err(|error| RuntimeError::Transaction(error.to_string()))?;
            let session = provider
                .begin_context_transaction()
                .await
                .map_err(|error| RuntimeError::Transaction(error.to_string()))?;
            Ok(BoundTransaction {
                owner: TypeId::of::<E>(),
                session: Box::new(TypedSession(Box::new(session) as Box<dyn Session<E::Error>>)),
                saves: TransactionSaveState::default(),
            })
        })
    }
}

impl UserContext {
    /// Register transaction capability for the data service already installed in this Context.
    pub fn register_context_transactions<E: ContextTransactionExecutor + Send + Sync + 'static>(
        &mut self,
    ) {
        self.insert_resource::<Arc<dyn TransactionFactory>>(Arc::new(ProviderFactory::<E>(
            PhantomData,
        )));
    }

    /// Run work with a transaction-bound Context. Nesting on that Context joins its transaction.
    ///
    /// The Context and repositories that borrow it cannot outlive the callback.
    ///
    /// ```compile_fail
    /// use teaql_runtime::UserContext;
    ///
    /// async fn escape(context: &UserContext) {
    ///     let _escaped = context
    ///         .transaction(|scoped| Box::pin(async move { Ok(scoped) }))
    ///         .await;
    /// }
    /// ```
    pub async fn transaction<T>(
        &self,
        work: impl for<'scope> FnOnce(&'scope UserContext) -> Operation<'scope, Result<T, RuntimeError>>,
    ) -> Result<T, RuntimeError> {
        if let Some(transaction) = &self.bound_transaction {
            transaction.saves.require_writable()?;
            let result = work(self).await;
            if let Err(error) = &result {
                transaction.saves.fail(error);
            }
            return result;
        }
        let factory = self
            .get_resource::<Arc<dyn TransactionFactory>>()
            .ok_or_else(|| {
                RuntimeError::Transaction(
                    "configured data service does not support UserContext transactions".to_owned(),
                )
            })?;
        let transaction = factory.begin(self).await?;
        let mut view = self.transaction_view(transaction);
        let (result, audits) =
            crate::commit_audit::collect(&view, async { work(&view).await }).await;
        let transaction = view
            .bound_transaction
            .take()
            .expect("transaction view owns its session");
        let value = transaction.complete(result).await?;
        crate::commit_audit::deliver(self, audits)?;
        Ok(value)
    }
}

pub(crate) enum ContextExecutor<'a, E> {
    Original(&'a E),
    Scoped(&'a (dyn Any + Send + Sync), PhantomData<fn() -> E>),
}

impl<E> Copy for ContextExecutor<'_, E> {}
impl<E> Clone for ContextExecutor<'_, E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, E: DataServiceExecutor> ContextExecutor<'a, E> {
    fn session(session: &'a (dyn Any + Send + Sync)) -> &'a dyn Session<E::Error> {
        session
            .downcast_ref::<TypedSession<E::Error>>()
            .expect("transaction owner matches executor")
            .0
            .as_ref()
    }
}

impl<E: DataServiceExecutor> DataServiceExecutor for ContextExecutor<'_, E> {
    type Error = E::Error;
    fn capabilities(&self) -> DataServiceCapabilities {
        match self {
            Self::Original(executor) => executor.capabilities(),
            Self::Scoped(session, _) => Self::session(*session).capabilities(),
        }
    }
}

impl<E: QueryExecutor + Sync> QueryExecutor for ContextExecutor<'_, E> {
    fn dynamic_field_store(&self) -> Option<&dyn DynamicFieldStore> {
        match self {
            Self::Original(executor) => executor.dynamic_field_store(),
            Self::Scoped(session, _) => Self::session(*session).store(),
        }
    }
    fn query_log_intent(&self, query: &SelectQuery) -> SqlIntentRedactions {
        match self {
            Self::Original(executor) => executor.query_log_intent(query),
            Self::Scoped(session, _) => Self::session(*session).log_intent(query),
        }
    }
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, E::Error> {
        self.query_observed(request, None).await
    }
    async fn query_observed<'a>(
        &'a self,
        request: QueryRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<QueryResult, E::Error> {
        match self {
            Self::Original(executor) => executor.query_observed(request, observer).await,
            Self::Scoped(session, _) => Self::session(*session).query(request, observer).await,
        }
    }
}

impl<E: MutationExecutor + Sync> MutationExecutor for ContextExecutor<'_, E> {
    async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, E::Error> {
        self.mutate_observed(request, None).await
    }
    async fn mutate_observed<'a>(
        &'a self,
        request: MutationRequest,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<MutationResult, E::Error> {
        match self {
            Self::Original(executor) => executor.mutate_observed(request, observer).await,
            Self::Scoped(session, _) => Self::session(*session).mutate(request, observer).await,
        }
    }
}

impl<E: StreamQueryExecutor> StreamQueryExecutor for ContextExecutor<'_, E> {
    fn query_stream(&self, request: QueryRequest, size: usize) -> QueryStream<'_, E::Error> {
        match self {
            Self::Original(executor) => executor.query_stream(request, size),
            Self::Scoped(session, _) => Self::session(*session).stream(request, size, None),
        }
    }
    fn query_stream_observed<'a>(
        &'a self,
        request: QueryRequest,
        size: usize,
        observer: ExecutionObserver<'a>,
    ) -> QueryStream<'a, E::Error> {
        match self {
            Self::Original(executor) => executor.query_stream_observed(request, size, observer),
            Self::Scoped(session, _) => {
                Self::session(*session).stream(request, size, Some(observer))
            }
        }
    }
    fn query_stream_with_dynamic_fields<'a>(
        &'a self,
        request: QueryRequest,
        size: usize,
        plan: DynamicFieldStreamPlan,
        observer: Option<ExecutionObserver<'a>>,
    ) -> Result<QueryStream<'a, E::Error>, DynamicFieldError> {
        match self {
            Self::Original(executor) => {
                executor.query_stream_with_dynamic_fields(request, size, plan, observer)
            }
            Self::Scoped(session, _) => {
                Self::session(*session).dynamic_stream(request, size, plan, observer)
            }
        }
    }
}
