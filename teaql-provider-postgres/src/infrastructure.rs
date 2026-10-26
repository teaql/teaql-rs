use super::{MutationExecutorError, PgMutationExecutor, PgTransactionExecutor};
use teaql_data_service::infrastructure_sql::{
    InfrastructureSql, InfrastructureSqlError, InfrastructureSqlFuture, InfrastructureStatement,
    InfrastructureTransaction, PhysicalTransactionIdentity, PhysicalTransactionOutcome,
    SqlResourceIdentity,
};
use teaql_sql::{CompiledQuery, SqlTransaction, SqlTransactionTransport, SqlTransport};

impl PgTransactionExecutor {
    pub(super) fn client(&self) -> &deadpool_postgres::Object {
        self.client.as_ref().expect("owned physical connection")
    }
}
impl Drop for PgTransactionExecutor {
    fn drop(&mut self) {
        // Closing a connection rolls back on the server. It must never return an
        // uncompleted/aborted transaction to the pool, including cancellation of BEGIN/COMMIT.
        drop(self.completion.take());
        if let Some(client) = self.client.take() {
            if matches!(
                self.identity.outcome(),
                PhysicalTransactionOutcome::Committed | PhysicalTransactionOutcome::RolledBack
            ) {
                drop(client);
            } else {
                drop(deadpool_postgres::Object::take(client));
            }
        }
    }
}
fn compiled(statement: &InfrastructureStatement) -> CompiledQuery {
    CompiledQuery {
        sql: statement.sql().to_owned(),
        params: statement.parameters().to_vec(),
        comment: None,
        log_context: Default::default(),
    }
}
fn safe_error(error: MutationExecutorError) -> InfrastructureSqlError {
    match error {
        MutationExecutorError::Driver(error)
            if error.code().is_some_and(|code| {
                code.code() == "23505" || code.code() == "40001" || code.code() == "40P01"
            }) =>
        {
            InfrastructureSqlError::Conflict
        }
        _ => InfrastructureSqlError::StatementFailed,
    }
}
impl InfrastructureSql for PgMutationExecutor {
    fn database_kind(&self) -> &'static str {
        "postgresql"
    }
    fn resource_identity(&self) -> SqlResourceIdentity {
        self.resource_identity.clone()
    }
    fn fetch<'a>(
        &'a self,
        statement: &'a InfrastructureStatement,
    ) -> InfrastructureSqlFuture<'a, Vec<teaql_core::CompactRow>> {
        Box::pin(async move {
            self.fetch_all_compact_sql(&compiled(statement))
                .await
                .map_err(safe_error)
        })
    }
    fn execute<'a>(
        &'a self,
        statement: &'a InfrastructureStatement,
    ) -> InfrastructureSqlFuture<'a, u64> {
        Box::pin(async move {
            self.execute_sql(&compiled(statement))
                .await
                .map_err(safe_error)
        })
    }
    fn begin(&self) -> InfrastructureSqlFuture<'_, Box<dyn InfrastructureTransaction>> {
        Box::pin(async move {
            self.begin_sql()
                .await
                .map(|transaction| Box::new(transaction) as Box<dyn InfrastructureTransaction>)
                .map_err(safe_error)
        })
    }
}
impl InfrastructureSql for PgTransactionExecutor {
    fn database_kind(&self) -> &'static str {
        "postgresql"
    }
    fn resource_identity(&self) -> SqlResourceIdentity {
        self.identity.resource().clone()
    }
    fn transaction_identity(&self) -> Option<PhysicalTransactionIdentity> {
        Some(self.identity.clone())
    }
    fn mark_rollback_only(&self) -> Result<(), InfrastructureSqlError> {
        self.identity.require_active()?;
        self.rollback_only
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn fetch<'a>(
        &'a self,
        statement: &'a InfrastructureStatement,
    ) -> InfrastructureSqlFuture<'a, Vec<teaql_core::CompactRow>> {
        Box::pin(async move {
            self.identity.require_active()?;
            self.fetch_all_compact_sql(&compiled(statement))
                .await
                .map_err(safe_error)
        })
    }
    fn execute<'a>(
        &'a self,
        statement: &'a InfrastructureStatement,
    ) -> InfrastructureSqlFuture<'a, u64> {
        Box::pin(async move {
            self.identity.require_active()?;
            self.execute_sql(&compiled(statement))
                .await
                .map_err(safe_error)
        })
    }
}
impl InfrastructureTransaction for PgTransactionExecutor {
    fn commit(self: Box<Self>) -> InfrastructureSqlFuture<'static, ()> {
        Box::pin(async move {
            let identity = self.identity.clone();
            self.commit_sql().await.map_err(|error| {
                if identity.outcome() == PhysicalTransactionOutcome::Unknown {
                    InfrastructureSqlError::TransactionUnknown
                } else if matches!(
                    error,
                    MutationExecutorError::TransactionCompletionAfterCommit
                ) {
                    InfrastructureSqlError::TransactionAfterCommit
                } else if matches!(
                    error,
                    MutationExecutorError::TransactionCompletionRejected
                        | MutationExecutorError::TransactionCompletionAfterRollback
                ) {
                    InfrastructureSqlError::CompletionFailed
                } else {
                    InfrastructureSqlError::StatementFailed
                }
            })
        })
    }
    fn rollback(self: Box<Self>) -> InfrastructureSqlFuture<'static, ()> {
        Box::pin(async move {
            let identity = self.identity.clone();
            self.rollback_sql().await.map_err(|error| {
                if identity.outcome() == PhysicalTransactionOutcome::Unknown {
                    InfrastructureSqlError::TransactionUnknown
                } else if matches!(
                    error,
                    MutationExecutorError::TransactionCompletionAfterRollback
                ) {
                    InfrastructureSqlError::CompletionFailed
                } else {
                    InfrastructureSqlError::StatementFailed
                }
            })
        })
    }
}
