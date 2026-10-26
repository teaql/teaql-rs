//! Framework infrastructure access on the native executor's actual SQL resource.
//! This is not a wire/business query API. Statements and transaction owners come
//! from trusted adapters/modules, never request-selected SQL or client markers.
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};
use teaql_core::{CompactRow, Value};

#[derive(Clone)]
pub struct SqlResourceIdentity(Arc<()>);
impl SqlResourceIdentity {
    pub fn new() -> Self {
        Self(Arc::new(()))
    }
    pub fn begin(&self) -> PhysicalTransactionOwner {
        PhysicalTransactionOwner {
            identity: PhysicalTransactionIdentity {
                resource: self.clone(),
                state: Arc::new(AtomicU8::new(0)),
            },
        }
    }
}
impl Default for SqlResourceIdentity {
    fn default() -> Self {
        Self::new()
    }
}
impl PartialEq for SqlResourceIdentity {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for SqlResourceIdentity {}
impl fmt::Debug for SqlResourceIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SqlResourceIdentity([opaque])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhysicalTransactionOutcome {
    Active,
    Committed,
    RolledBack,
    Unknown,
}
#[derive(Clone)]
pub struct PhysicalTransactionIdentity {
    resource: SqlResourceIdentity,
    state: Arc<AtomicU8>,
}
impl PhysicalTransactionIdentity {
    pub fn resource(&self) -> &SqlResourceIdentity {
        &self.resource
    }
    pub fn outcome(&self) -> PhysicalTransactionOutcome {
        match self.state.load(Ordering::SeqCst) {
            0 => PhysicalTransactionOutcome::Active,
            1 => PhysicalTransactionOutcome::Committed,
            2 => PhysicalTransactionOutcome::RolledBack,
            _ => PhysicalTransactionOutcome::Unknown,
        }
    }
    pub fn require_active(&self) -> Result<(), InfrastructureSqlError> {
        if self.outcome() == PhysicalTransactionOutcome::Active {
            Ok(())
        } else {
            Err(InfrastructureSqlError::TransactionRequired)
        }
    }
}
impl PartialEq for PhysicalTransactionIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.resource == other.resource && Arc::ptr_eq(&self.state, &other.state)
    }
}
impl Eq for PhysicalTransactionIdentity {}
impl fmt::Debug for PhysicalTransactionIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PhysicalTransactionIdentity([opaque])")
    }
}

/// Only the adapter keeps this controller. A borrowed identity cannot change its outcome.
pub struct PhysicalTransactionOwner {
    identity: PhysicalTransactionIdentity,
}
impl PhysicalTransactionOwner {
    pub fn identity(&self) -> PhysicalTransactionIdentity {
        self.identity.clone()
    }
    pub fn committed(self) {
        self.identity.state.store(1, Ordering::SeqCst);
    }
    pub fn rolled_back(self) {
        self.identity.state.store(2, Ordering::SeqCst);
    }
    pub fn unknown(self) {
        self.identity.state.store(3, Ordering::SeqCst);
    }
}
impl Drop for PhysicalTransactionOwner {
    fn drop(&mut self) {
        let _ = self
            .identity
            .state
            .compare_exchange(0, 3, Ordering::SeqCst, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InfrastructureSqlError {
    Unsupported,
    StatementFailed,
    Conflict,
    InvalidStatement,
    TransactionRequired,
    TransactionUnknown,
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
        first.committed();
        assert_eq!(token.outcome(), PhysicalTransactionOutcome::Committed);
        assert!(token.require_active().is_err());
        let token = second.identity();
        second.rolled_back();
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
