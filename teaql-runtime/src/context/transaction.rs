use std::future::Future;
use std::pin::Pin;

use teaql_data_service::{
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
    Transaction, TransactionExecutor,
};

use crate::{ContextError, EntityDataService, RuntimeError, UserContext};

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
        })
    }

    /// Execute work against one typed transaction, committing on success and
    /// rolling back on error.
    ///
    /// The callback receives a [`TransactionScope`]; using the surrounding
    /// `UserContext` inside the callback would intentionally execute outside the
    /// transaction.
    pub async fn execute_in_transaction<E, T, F>(&self, operation: F) -> Result<T, RuntimeError>
    where
        E: TransactionExecutor + Send + Sync + 'static,
        for<'transaction> E::Tx<'transaction>: Send + Sync,
        F: for<'scope> FnOnce(
            &'scope TransactionScope<'_, E>,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, RuntimeError>> + Send + 'scope>,
        >,
    {
        self.start_transaction::<E>()
            .await?
            .execute(operation)
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
    pub async fn query(&self, request: QueryRequest) -> Result<QueryResult, RuntimeError>
    where
        E::Tx<'context>: QueryExecutor<Error = E::Error>,
    {
        let result = QueryExecutor::query(self.transaction(), request)
            .await
            .map_err(|error| RuntimeError::Transaction(error.to_string()))?;
        self.context.record_metadata_log(&result.metadata);
        Ok(result)
    }

    /// Execute a provider-neutral mutation on the transaction-owned connection.
    pub async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, RuntimeError>
    where
        E::Tx<'context>: MutationExecutor<Error = E::Error>,
    {
        let result = MutationExecutor::mutate(self.transaction(), request)
            .await
            .map_err(|error| RuntimeError::Transaction(error.to_string()))?;
        self.context.record_metadata_log(&result.metadata);
        Ok(result)
    }

    /// Run a callback and complete this transaction deterministically.
    pub async fn execute<T, F>(mut self, operation: F) -> Result<T, RuntimeError>
    where
        E::Tx<'context>: Send + Sync,
        F: for<'scope> FnOnce(
            &'scope TransactionScope<'context, E>,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, RuntimeError>> + Send + 'scope>,
        >,
    {
        match operation(&self).await {
            Ok(value) => {
                let transaction = self
                    .transaction
                    .take()
                    .expect("transaction scope cannot be completed twice");
                Transaction::commit(transaction)
                    .await
                    .map_err(|error| RuntimeError::Transaction(error.to_string()))?;
                Ok(value)
            }
            Err(error) => {
                let transaction = self
                    .transaction
                    .take()
                    .expect("transaction scope cannot be completed twice");
                Transaction::rollback(transaction)
                    .await
                    .map_err(|rollback| {
                        RuntimeError::Transaction(format!(
                            "operation failed ({error}); rollback also failed ({rollback})"
                        ))
                    })?;
                Err(error)
            }
        }
    }
}
