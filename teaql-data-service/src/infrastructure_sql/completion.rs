//! Framework completion on one real SQL owner, never client-selected lifecycle code.
use super::InfrastructureSqlError;
use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

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
                state: Arc::new(CompletionState {
                    outcome: AtomicU8::new(0),
                    settled: AtomicBool::new(false),
                    failed: AtomicBool::new(false),
                    callbacks: Mutex::new(Callbacks {
                        phase: Phase::Active,
                        veto: false,
                        participants: Vec::new(),
                    }),
                }),
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

/// Framework-only owned state/journal integration. No SQL/request/context lifetime
/// is captured implicitly. Validation/settlement must be bounded and nonblocking.
/// All settlements are attempted before the first committed notification. Each
/// method is isolated from panics/errors; callers must not use a notification as
/// the database's authority for whether COMMIT succeeded.
#[doc(hidden)]
pub trait PhysicalTransactionParticipant: Send + Sync + 'static {
    fn before_commit(&self) -> Result<(), InfrastructureSqlError> {
        Ok(())
    }
    fn after_completion(
        &self,
        outcome: PhysicalTransactionOutcome,
    ) -> Result<(), InfrastructureSqlError>;
    fn after_commit(&self) -> Result<(), InfrastructureSqlError> {
        Ok(())
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Active,
    Preparing,
    Prepared,
    Completing,
    Complete,
}
struct Callbacks {
    phase: Phase,
    veto: bool,
    participants: Vec<Arc<dyn PhysicalTransactionParticipant>>,
}
struct CompletionState {
    outcome: AtomicU8,
    settled: AtomicBool,
    failed: AtomicBool,
    callbacks: Mutex<Callbacks>,
}
#[derive(Clone)]
pub struct PhysicalTransactionIdentity {
    resource: SqlResourceIdentity,
    state: Arc<CompletionState>,
}
impl PhysicalTransactionIdentity {
    pub fn resource(&self) -> &SqlResourceIdentity {
        &self.resource
    }
    pub fn outcome(&self) -> PhysicalTransactionOutcome {
        match self.state.outcome.load(Ordering::SeqCst) {
            0 => PhysicalTransactionOutcome::Active,
            1 => PhysicalTransactionOutcome::Committed,
            2 => PhysicalTransactionOutcome::RolledBack,
            _ => PhysicalTransactionOutcome::Unknown,
        }
    }
    /// True after every settlement attempt, including error/UNKNOWN paths. It is
    /// separate from outcome so another thread cannot present half-settled state.
    pub fn is_settled(&self) -> bool {
        self.state.settled.load(Ordering::SeqCst)
    }
    pub fn has_completion_error(&self) -> bool {
        self.state.failed.load(Ordering::SeqCst)
    }
    pub fn require_active(&self) -> Result<(), InfrastructureSqlError> {
        if self.outcome() == PhysicalTransactionOutcome::Active {
            Ok(())
        } else {
            Err(InfrastructureSqlError::TransactionRequired)
        }
    }
    /// Opaque provenance is already bound to the native owner. Registration cannot
    /// complete it. Re-enlisting the exact same Arc is idempotent; registration
    /// after commit preparation starts fails, including callback re-entry.
    #[doc(hidden)]
    pub fn enlist(
        &self,
        participant: Arc<dyn PhysicalTransactionParticipant>,
    ) -> Result<(), InfrastructureSqlError> {
        let mut callbacks = self
            .state
            .callbacks
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.outcome() != PhysicalTransactionOutcome::Active || callbacks.phase != Phase::Active
        {
            return Err(InfrastructureSqlError::TransactionRequired);
        }
        if !callbacks
            .participants
            .iter()
            .any(|old| Arc::ptr_eq(old, &participant))
        {
            callbacks.participants.push(participant);
        }
        Ok(())
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

/// Adapter-only controller. A borrowed identity cannot change the physical result.
/// The adapter calls prepare_commit before sending COMMIT, then acknowledges only
/// the actual driver/server result. An I/O failure/drop remains UNKNOWN.
pub struct PhysicalTransactionOwner {
    identity: PhysicalTransactionIdentity,
}
impl PhysicalTransactionOwner {
    pub fn identity(&self) -> PhysicalTransactionIdentity {
        self.identity.clone()
    }
    pub fn prepare_commit(&mut self) -> Result<(), InfrastructureSqlError> {
        let participants = {
            let mut callbacks = self
                .identity
                .state
                .callbacks
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if callbacks.phase != Phase::Active
                || self.identity.outcome() != PhysicalTransactionOutcome::Active
            {
                return Err(InfrastructureSqlError::TransactionRequired);
            }
            callbacks.phase = Phase::Preparing;
            callbacks.participants.clone()
        };
        let mut failed = false;
        for participant in participants {
            failed |= invoke(|| participant.before_commit()).is_err();
        }
        let mut callbacks = self
            .identity
            .state
            .callbacks
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        callbacks.phase = Phase::Prepared;
        callbacks.veto = failed;
        if failed {
            Err(InfrastructureSqlError::CompletionFailed)
        } else {
            Ok(())
        }
    }
    pub fn committed(self) -> Result<(), InfrastructureSqlError> {
        self.complete(PhysicalTransactionOutcome::Committed)
    }
    pub fn rolled_back(self) -> Result<(), InfrastructureSqlError> {
        self.complete(PhysicalTransactionOutcome::RolledBack)
    }
    pub fn unknown(self) -> Result<(), InfrastructureSqlError> {
        self.complete(PhysicalTransactionOutcome::Unknown)
    }
    fn complete(&self, outcome: PhysicalTransactionOutcome) -> Result<(), InfrastructureSqlError> {
        let (participants, mut failed) = {
            let mut callbacks = self
                .identity
                .state
                .callbacks
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if self.identity.outcome() != PhysicalTransactionOutcome::Active {
                return Err(InfrastructureSqlError::TransactionRequired);
            }
            // Even a buggy adapter reporting a real COMMIT after a veto must not
            // relabel the database truth as rollback. Report completion failure.
            let invalid_commit = outcome == PhysicalTransactionOutcome::Committed
                && !callbacks.participants.is_empty()
                && (callbacks.phase != Phase::Prepared || callbacks.veto);
            callbacks.phase = Phase::Completing;
            self.identity.state.outcome.store(
                match outcome {
                    PhysicalTransactionOutcome::Committed => 1,
                    PhysicalTransactionOutcome::RolledBack => 2,
                    _ => 3,
                },
                Ordering::SeqCst,
            );
            (std::mem::take(&mut callbacks.participants), invalid_commit)
        };
        for participant in &participants {
            failed |= invoke(|| participant.after_completion(outcome)).is_err();
        }
        self.identity.state.failed.store(failed, Ordering::SeqCst);
        self.identity
            .state
            .callbacks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .phase = Phase::Complete;
        self.identity.state.settled.store(true, Ordering::SeqCst);
        if outcome == PhysicalTransactionOutcome::Committed {
            for participant in &participants {
                if invoke(|| participant.after_commit()).is_err() {
                    failed = true;
                    self.identity.state.failed.store(true, Ordering::SeqCst);
                }
            }
        }
        if failed {
            Err(InfrastructureSqlError::CompletionFailed)
        } else {
            Ok(())
        }
    }
}
impl Drop for PhysicalTransactionOwner {
    fn drop(&mut self) {
        if self.identity.outcome() == PhysicalTransactionOutcome::Active {
            let _ = self.complete(PhysicalTransactionOutcome::Unknown);
        }
    }
}
fn invoke(
    work: impl FnOnce() -> Result<(), InfrastructureSqlError>,
) -> Result<(), InfrastructureSqlError> {
    match catch_unwind(AssertUnwindSafe(work)) {
        Ok(Ok(())) => Ok(()),
        _ => Err(InfrastructureSqlError::CompletionFailed),
    }
}

#[cfg(test)]
mod tests;
