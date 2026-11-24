use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use teaql_data_service::{
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
    Transaction, TransactionExecutor,
};

use crate::{ContextError, EntityDataService, EntityRuntimeState, RuntimeError, UserContext};

#[derive(Default)]
pub(crate) struct TransactionSaveState {
    failure: Mutex<Option<RuntimeError>>,
    attempted: Mutex<Vec<EntityRuntimeState>>,
    readbacks: Mutex<Vec<EntityRuntimeState>>,
    staged: Mutex<Vec<crate::entity_save::StagedTransactionGraph>>,
    gate: futures_util::lock::Mutex<()>,
    settled: bool,
}

impl TransactionSaveState {
    fn remember(ledgers: &Mutex<Vec<EntityRuntimeState>>, ledger: EntityRuntimeState) {
        let mut ledgers = ledgers.lock().unwrap_or_else(|error| error.into_inner());
        if !ledgers.iter().any(|existing| existing == &ledger) {
            ledgers.push(ledger);
        }
    }

    pub(crate) fn failure(&self) -> Option<RuntimeError> {
        self.failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub(crate) fn fail(&self, error: &RuntimeError) {
        self.failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_or_insert_with(|| error.clone());
    }

    pub(super) fn require_writable(&self) -> Result<(), RuntimeError> {
        if self.failure().is_some() {
            return Err(RuntimeError::Transaction(
                "TRANSACTION_ROLLBACK_ONLY: a previous save failed".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn save<T, E>(
        &self,
        audited: teaql_core::Audited<T>,
        context: &UserContext,
        executor: &E,
    ) -> Result<T, RuntimeError>
    where
        T: crate::LedgerEntity + Send + 'static,
        E: QueryExecutor + MutationExecutor + Send + Sync,
    {
        let _guard = self.gate.lock().await;
        self.require_writable()?;
        let original = audited.entity().entity_runtime_state();
        if let Some(root) = &original {
            Self::remember(&self.attempted, root.clone());
        }
        match Box::pin(crate::entity_save::save_transaction_graph(
            audited, context, executor,
        ))
        .await
        {
            Ok((entity, graph)) => {
                if let Some(root) = entity.entity_runtime_state() {
                    if graph.is_some()
                        && !original
                            .as_ref()
                            .is_some_and(|input| input.shares_mutation_ledger(&root))
                    {
                        Self::remember(&self.readbacks, root.clone());
                    }
                    Self::remember(&self.attempted, root);
                }
                if let Some(graph) = graph {
                    self.staged
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .push(graph.stage());
                }
                Ok(entity)
            }
            Err(error) => {
                self.fail(&error);
                Err(error)
            }
        }
    }

    pub(crate) fn settle(&mut self, committed: bool) {
        self.settled = true;
        let staged = self
            .staged
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        while let Some(graph) = staged.pop() {
            if committed {
                graph.discard();
            } else {
                graph.restore();
            }
        }
        if !committed {
            for ledger in self
                .readbacks
                .get_mut()
                .unwrap_or_else(|error| error.into_inner())
            {
                ledger.mark_readback_rolled_back();
            }
        }
    }
}

impl Drop for TransactionSaveState {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        for ledger in self
            .attempted
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
        {
            ledger.fence_transaction_outcome();
        }
        for graph in self
            .staged
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .drain(..)
        {
            graph.discard();
        }
    }
}

/// A real provider transaction bound to a [`UserContext`].
///
/// All work must be performed through this object. This makes it impossible to
/// accidentally open a transaction and then execute against the context's
/// non-transactional executor, which was the defect in the former placeholder
/// API.
#[must_use = "transaction work must be completed through TransactionScope::execute"]
pub struct TransactionScope<'context, E>
where
    E: TransactionExecutor + 'context,
{
    context: &'context UserContext,
    transaction: Option<E::Tx<'context>>,
    saves: TransactionSaveState,
}

impl UserContext {
    /// Begin a typed transaction using the executor registered in this context.
    async fn start_transaction<E>(&self) -> Result<TransactionScope<'_, E>, RuntimeError>
    where
        E: TransactionExecutor + Send + Sync + 'static,
        for<'transaction> E::Tx<'transaction>: Send + Sync,
    {
        let executor = self
            .require_resource::<E>()
            .map_err(|error| RuntimeError::Transaction(error.to_string()))?;
        let transaction = TransactionExecutor::begin(executor)
            .await
            .map_err(|error| RuntimeError::Transaction(error.to_string()))?;
        Ok(TransactionScope {
            context: self,
            transaction: Some(transaction),
            saves: TransactionSaveState::default(),
        })
    }

    /// Execute work against one typed transaction, committing on success and
    /// rolling back on error.
    ///
    /// The callback receives a [`TransactionScope`]; using the surrounding
    /// `UserContext` inside the callback would intentionally execute outside the
    /// transaction.
    pub async fn execute_in_transaction<'context, E, T, F>(
        &'context self,
        operation: F,
    ) -> Result<T, RuntimeError>
    where
        E: TransactionExecutor + Send + Sync + 'static,
        for<'transaction> E::Tx<'transaction>: Send + Sync,
        F: for<'scope> FnOnce(
            &'scope TransactionScope<'context, E>,
        )
            -> Pin<Box<dyn Future<Output = Result<T, RuntimeError>> + 'scope>>,
    {
        self.start_transaction::<E>()
            .await?
            .execute(operation)
            .await
    }

    /// Execute transaction work whose callback must remain `Send` across await points.
    ///
    /// Async service SPIs commonly require a `Send` future. Callers in that boundary should
    /// choose this method explicitly. The ordinary [`Self::execute_in_transaction`] remains
    /// available for provider cursors and other deliberately thread-local transaction work.
    pub async fn execute_in_send_transaction<'context, E, T, F>(
        &'context self,
        operation: F,
    ) -> Result<T, RuntimeError>
    where
        E: TransactionExecutor + Send + Sync + 'static,
        for<'transaction> E::Tx<'transaction>: Send + Sync,
        F: for<'scope> FnOnce(
            &'scope TransactionScope<'context, E>,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, RuntimeError>> + Send + 'scope>,
        >,
    {
        self.start_transaction::<E>()
            .await?
            .execute_send(operation)
            .await
    }
}

impl<'context, E> TransactionScope<'context, E>
where
    E: TransactionExecutor + 'context,
{
    fn transaction(&self) -> &E::Tx<'context> {
        self.transaction
            .as_ref()
            .expect("transaction scope cannot be used after completion")
    }

    /// The runtime context that owns metadata, policy, telemetry and audit
    /// configuration for this transaction.
    pub fn context(&self) -> &'context UserContext {
        self.context
    }

    /// Build an entity data service that is guaranteed to use this transaction.
    pub fn entity_data_service(
        &self,
        entity: impl Into<String>,
    ) -> Result<EntityDataService<'_, E::Tx<'context>>, ContextError>
    where
        E::Tx<'context>: QueryExecutor + MutationExecutor + Send + Sync,
    {
        let entity = entity.into();
        if !self.context.has_entity_data_service(&entity) {
            return Err(ContextError::MissingEntityDataService(entity));
        }
        Ok(EntityDataService::for_executor(
            self.context,
            entity,
            self.transaction(),
        ))
    }

    /// Execute a provider-neutral query on the transaction-owned connection.
    pub async fn query(&self, mut request: QueryRequest) -> Result<QueryResult, RuntimeError>
    where
        E::Tx<'context>: QueryExecutor,
    {
        if self.context.records_sql_diagnostics() {
            request.capture_execution_metadata = true;
        }
        let observer = (self.context.sql_log_options().select
            || self.context.records_sql_diagnostics())
        .then(|| {
            std::sync::Arc::new(|metadata| self.context.record_metadata_log(&metadata))
                as teaql_data_service::ExecutionObserver<'_>
        });
        let result = self
            .context
            .observe_sql_statements(QueryExecutor::query_observed(
                self.transaction(),
                request,
                observer,
            ))
            .await
            .map_err(|error| RuntimeError::Transaction(error.to_string()))?;
        self.context.record_metadata_log(&result.metadata);
        Ok(result)
    }

    /// Execute a provider-neutral mutation on the transaction-owned connection.
    pub async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, RuntimeError>
    where
        E::Tx<'context>: MutationExecutor,
    {
        self.saves.require_writable()?;
        let observer = (self.context.sql_log_options().mutation
            || self.context.records_sql_diagnostics())
        .then(|| {
            std::sync::Arc::new(|metadata| self.context.record_metadata_log(&metadata))
                as teaql_data_service::ExecutionObserver<'_>
        });
        let result = self
            .context
            .observe_sql_statements(MutationExecutor::mutate_observed(
                self.transaction(),
                request,
                observer,
            ))
            .await
            .map_err(|error| {
                let error = RuntimeError::Transaction(error.to_string());
                self.saves.fail(&error);
                error
            })?;
        self.context.record_metadata_log(&result.metadata);
        Ok(result)
    }

    /// Save one audited generated entity through this transaction.
    ///
    /// Its mutation ledger remains pending until the enclosing scope commits.
    /// A confirmed rollback preserves retryable intent; an unknown commit or
    /// rollback result marks every attempted graph as requiring a reload.
    pub async fn save_audited<T>(&self, audited: teaql_core::Audited<T>) -> Result<T, RuntimeError>
    where
        T: crate::LedgerEntity + Send + 'static,
        E::Tx<'context>: QueryExecutor + MutationExecutor + Send + Sync,
    {
        self.saves
            .save(audited, self.context, self.transaction())
            .await
    }

    /// Run a callback and complete this transaction deterministically.
    pub async fn execute<T, F>(self, operation: F) -> Result<T, RuntimeError>
    where
        E::Tx<'context>: Send + Sync,
        F: for<'scope> FnOnce(
            &'scope TransactionScope<'context, E>,
        )
            -> Pin<Box<dyn Future<Output = Result<T, RuntimeError>> + 'scope>>,
    {
        let context = self.context;
        let (result, audits) =
            crate::commit_audit::collect(context, async { operation(&self).await }).await;
        let value = self.complete(result).await?;
        crate::commit_audit::deliver(context, audits)?;
        Ok(value)
    }

    /// Run a `Send` callback and complete this transaction deterministically.
    pub async fn execute_send<T, F>(self, operation: F) -> Result<T, RuntimeError>
    where
        E::Tx<'context>: Send + Sync,
        F: for<'scope> FnOnce(
            &'scope TransactionScope<'context, E>,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, RuntimeError>> + Send + 'scope>,
        >,
    {
        let context = self.context;
        let (result, audits) =
            crate::commit_audit::collect(context, async { operation(&self).await }).await;
        let value = self.complete(result).await?;
        crate::commit_audit::deliver(context, audits)?;
        Ok(value)
    }

    async fn complete<T>(mut self, result: Result<T, RuntimeError>) -> Result<T, RuntimeError>
    where
        E::Tx<'context>: Send + Sync,
    {
        let result = self.saves.failure().map_or(result, Err);
        match result {
            Ok(value) => {
                let transaction = self
                    .transaction
                    .take()
                    .expect("transaction scope cannot be completed twice");
                Transaction::commit(transaction)
                    .await
                    .map_err(|_| RuntimeError::TransactionOutcomeUnknown)?;
                self.saves.settle(true);
                Ok(value)
            }
            Err(error) => {
                let transaction = self
                    .transaction
                    .take()
                    .expect("transaction scope cannot be completed twice");
                Transaction::rollback(transaction)
                    .await
                    .map_err(|_| RuntimeError::TransactionOutcomeUnknown)?;
                self.saves.settle(false);
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod transaction_outcome_tests {
    use super::*;
    use teaql_data_service::{DataServiceCapabilities, DataServiceExecutor, MutationResult};

    #[derive(Clone, Copy)]
    enum FailureMode {
        Commit,
        Rollback,
        CommitSuccess,
        RollbackSuccess,
        PendingCommit,
        PendingRollback,
    }

    struct FailingExecutor;
    struct FailingTransaction(FailureMode);

    impl DataServiceExecutor for FailingExecutor {
        type Error = std::io::Error;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl DataServiceExecutor for FailingTransaction {
        type Error = std::io::Error;

        fn capabilities(&self) -> DataServiceCapabilities {
            DataServiceCapabilities::default()
        }
    }

    impl QueryExecutor for FailingTransaction {
        async fn query(&self, _request: QueryRequest) -> Result<QueryResult, Self::Error> {
            unreachable!("transaction-outcome test never issues a query")
        }
    }

    impl MutationExecutor for FailingTransaction {
        async fn mutate(&self, _request: MutationRequest) -> Result<MutationResult, Self::Error> {
            unreachable!("transaction-outcome test never issues a mutation")
        }
    }

    impl TransactionExecutor for FailingExecutor {
        type Tx<'a> = FailingTransaction;

        async fn begin(&self) -> Result<Self::Tx<'_>, Self::Error> {
            unreachable!("transaction-outcome test constructs the scope directly")
        }
    }

    impl Transaction for FailingTransaction {
        type Error = std::io::Error;

        async fn commit(self) -> Result<(), Self::Error> {
            match self.0 {
                FailureMode::Commit => Err(std::io::Error::other("COMMIT-SECRET-CANARY")),
                FailureMode::CommitSuccess => Ok(()),
                FailureMode::PendingCommit => std::future::pending().await,
                FailureMode::Rollback
                | FailureMode::RollbackSuccess
                | FailureMode::PendingRollback => {
                    unreachable!("wrong test branch")
                }
            }
        }

        async fn rollback(self) -> Result<(), Self::Error> {
            match self.0 {
                FailureMode::Rollback => Err(std::io::Error::other("ROLLBACK-SECRET-CANARY")),
                FailureMode::RollbackSuccess => Ok(()),
                FailureMode::PendingRollback => std::future::pending().await,
                FailureMode::Commit | FailureMode::CommitSuccess | FailureMode::PendingCommit => {
                    unreachable!("wrong test branch")
                }
            }
        }
    }

    fn attempted(ledgers: impl IntoIterator<Item = EntityRuntimeState>) -> TransactionSaveState {
        let saves = TransactionSaveState::default();
        for ledger in ledgers {
            TransactionSaveState::remember(&saves.attempted, ledger);
        }
        saves
    }

    #[tokio::test]
    async fn unknown_outer_transaction_poison_all_attempted_ledgers_without_leaking_errors() {
        let context = UserContext::default();
        for mode in [FailureMode::Commit, FailureMode::Rollback] {
            let first = crate::EntityRuntimeState::default();
            let second = crate::EntityRuntimeState::default();
            let scope = TransactionScope::<FailingExecutor> {
                context: &context,
                transaction: Some(FailingTransaction(mode)),
                saves: attempted([first.clone(), second.clone()]),
            };
            let result = match mode {
                FailureMode::Commit => scope.complete(Ok(())).await,
                FailureMode::Rollback => {
                    scope
                        .complete::<()>(Err(RuntimeError::Graph("OP-SECRET-CANARY".into())))
                        .await
                }
                FailureMode::CommitSuccess
                | FailureMode::RollbackSuccess
                | FailureMode::PendingCommit
                | FailureMode::PendingRollback => unreachable!(),
            };
            let error = result.unwrap_err();
            assert!(matches!(error, RuntimeError::TransactionOutcomeUnknown));
            assert_eq!(
                error.to_string(),
                "TRANSACTION_OUTCOME_UNKNOWN: commit or rollback could not confirm the transaction outcome; reload the graph before any retry"
            );
            for ledger in [&first, &second] {
                assert!(matches!(
                    ledger.require_known_transaction_outcome(),
                    Err(RuntimeError::TransactionOutcomeUnknownReloadRequired)
                ));
            }
        }
    }

    #[tokio::test]
    async fn confirmed_completion_preserves_known_graph_outcomes() {
        let context = UserContext::default();
        for mode in [FailureMode::CommitSuccess, FailureMode::RollbackSuccess] {
            let ledger = crate::EntityRuntimeState::default();
            let scope = TransactionScope::<FailingExecutor> {
                context: &context,
                transaction: Some(FailingTransaction(mode)),
                saves: attempted([ledger.clone()]),
            };
            let result = match mode {
                FailureMode::CommitSuccess => scope.complete(Ok(())).await,
                FailureMode::RollbackSuccess => {
                    scope
                        .complete::<()>(Err(RuntimeError::Graph("original failure".into())))
                        .await
                }
                FailureMode::Commit
                | FailureMode::Rollback
                | FailureMode::PendingCommit
                | FailureMode::PendingRollback => unreachable!(),
            };
            match mode {
                FailureMode::CommitSuccess => assert!(result.is_ok()),
                FailureMode::RollbackSuccess => {
                    assert!(matches!(result, Err(RuntimeError::Graph(_))))
                }
                _ => unreachable!(),
            }
            assert!(ledger.require_known_transaction_outcome().is_ok());
        }
    }

    #[tokio::test]
    async fn cancelling_in_flight_completion_requires_reloading_every_attempted_graph() {
        let context = UserContext::default();
        for mode in [FailureMode::PendingCommit, FailureMode::PendingRollback] {
            let first = crate::EntityRuntimeState::default();
            let second = crate::EntityRuntimeState::default();
            let scope = TransactionScope::<FailingExecutor> {
                context: &context,
                transaction: Some(FailingTransaction(mode)),
                saves: attempted([first.clone(), second.clone()]),
            };
            let result = match mode {
                FailureMode::PendingCommit => {
                    tokio::time::timeout(
                        std::time::Duration::from_millis(10),
                        scope.complete(Ok(())),
                    )
                    .await
                }
                FailureMode::PendingRollback => {
                    tokio::time::timeout(
                        std::time::Duration::from_millis(10),
                        scope.complete::<()>(Err(RuntimeError::Graph("test failure".into()))),
                    )
                    .await
                }
                FailureMode::Commit
                | FailureMode::Rollback
                | FailureMode::CommitSuccess
                | FailureMode::RollbackSuccess => unreachable!(),
            };
            assert!(
                result.is_err(),
                "fixture must cancel an in-flight provider call"
            );
            for ledger in [&first, &second] {
                assert!(matches!(
                    ledger.require_known_transaction_outcome(),
                    Err(RuntimeError::TransactionOutcomeUnknownReloadRequired)
                ));
            }
        }
    }
}
