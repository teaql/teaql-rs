//! Framework infrastructure access on the native executor's actual SQL resource.
//! This is not a wire/business query API. Statements and transaction owners come
//! from trusted adapters/modules, never request-selected SQL or client markers.
use std::{fmt, future::Future, pin::Pin};
use teaql_core::{CompactRow, Value};

mod completion;
pub use completion::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InfrastructureSqlError {
    Unsupported,
    StatementFailed,
    Conflict,
    InvalidStatement,
    TransactionRequired,
    TransactionUnknown,
    CompletionFailed,
    TransactionAfterCommit,
}
impl fmt::Display for InfrastructureSqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unsupported => "INFRASTRUCTURE_SQL_UNSUPPORTED",
            Self::StatementFailed => "INFRASTRUCTURE_SQL_FAILED",
            Self::Conflict => "INFRASTRUCTURE_SQL_CONFLICT",
            Self::InvalidStatement => "INFRASTRUCTURE_SQL_INVALID",
            Self::TransactionRequired => "INFRASTRUCTURE_TRANSACTION_REQUIRED",
            Self::TransactionUnknown => "INFRASTRUCTURE_TRANSACTION_UNKNOWN",
            Self::CompletionFailed => "INFRASTRUCTURE_COMPLETION_FAILED",
            Self::TransactionAfterCommit => "INFRASTRUCTURE_TRANSACTION_AFTER_COMMIT_FAILED",
        })
    }
}
impl std::error::Error for InfrastructureSqlError {}
/// No Debug/Serialize: SQL arguments can contain protected infrastructure facts.
pub struct InfrastructureStatement {
    sql: String,
    parameters: Vec<Value>,
}
impl InfrastructureStatement {
    pub fn new(
        sql: impl Into<String>,
        parameters: Vec<Value>,
    ) -> Result<Self, InfrastructureSqlError> {
        let sql = sql.into();
        if sql.trim().is_empty()
            || sql.len() > 131_072
            || sql.contains('\0')
            || parameters.len() > 10_000
        {
            return Err(InfrastructureSqlError::InvalidStatement);
        }
        Ok(Self { sql, parameters })
    }
    pub fn sql(&self) -> &str {
        &self.sql
    }
    pub fn parameters(&self) -> &[Value] {
        &self.parameters
    }
}
pub type InfrastructureSqlFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, InfrastructureSqlError>> + Send + 'a>>;
pub trait InfrastructureSql: Send + Sync {
    fn database_kind(&self) -> &'static str;
    fn resource_identity(&self) -> SqlResourceIdentity;
    fn transaction_identity(&self) -> Option<PhysicalTransactionIdentity> {
        None
    }
    fn mark_rollback_only(&self) -> Result<(), InfrastructureSqlError> {
        Err(InfrastructureSqlError::Unsupported)
    }
    fn fetch<'a>(
        &'a self,
        statement: &'a InfrastructureStatement,
    ) -> InfrastructureSqlFuture<'a, Vec<CompactRow>>;
    fn execute<'a>(
        &'a self,
        statement: &'a InfrastructureStatement,
    ) -> InfrastructureSqlFuture<'a, u64>;
    /// A transaction handle does not override this: nested infrastructure owners fail closed.
    fn begin(&self) -> InfrastructureSqlFuture<'_, Box<dyn InfrastructureTransaction>> {
        Box::pin(async { Err(InfrastructureSqlError::Unsupported) })
    }
}
pub trait InfrastructureTransaction: InfrastructureSql + 'static {
    fn commit(self: Box<Self>) -> InfrastructureSqlFuture<'static, ()>;
    fn rollback(self: Box<Self>) -> InfrastructureSqlFuture<'static, ()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clones_share_only_their_resource_and_each_physical_owner_has_distinct_provenance() {
        let resource = SqlResourceIdentity::new();
        let clone = resource.clone();
        assert_eq!(resource, clone);
        assert_ne!(resource, SqlResourceIdentity::new());
        let first = resource.begin();
        let second = resource.begin();
        let token = first.identity();
        assert_ne!(token, second.identity());
        assert_eq!(token.resource(), &resource);
        first.committed().unwrap();
        assert_eq!(token.outcome(), PhysicalTransactionOutcome::Committed);
        assert!(token.require_active().is_err());
        let token = second.identity();
        second.rolled_back().unwrap();
        assert_eq!(token.outcome(), PhysicalTransactionOutcome::RolledBack);
    }
    #[test]
    fn dropping_an_uncompleted_owner_is_unknown_without_reusing_its_token() {
        let resource = SqlResourceIdentity::new();
        let owner = resource.begin();
        let token = owner.identity();
        drop(owner);
        assert_eq!(token.outcome(), PhysicalTransactionOutcome::Unknown);
        let next = resource.begin();
        assert_ne!(token, next.identity());
        assert!(next.identity().require_active().is_ok());
    }
}
