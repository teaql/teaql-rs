use super::*;
type Trace = Arc<Mutex<Vec<(usize, &'static str, PhysicalTransactionOutcome, bool)>>>;

struct Noop;
impl PhysicalTransactionParticipant for Noop {
    fn after_completion(
        &self,
        _: PhysicalTransactionOutcome,
    ) -> Result<(), InfrastructureSqlError> {
        Ok(())
    }
}
struct Probe {
    index: usize,
    identity: PhysicalTransactionIdentity,
    trace: Trace,
    // 0 succeeds, 1 returns an error, 2 panics. Each stage runs independently.
    failure: [u8; 3],
}
impl Probe {
    fn step(&self, stage: usize, name: &'static str) -> Result<(), InfrastructureSqlError> {
        self.trace.lock().unwrap().push((
            self.index,
            name,
            self.identity.outcome(),
            self.identity.is_settled(),
        ));
        // Re-entry must fail without holding the completion mutex or deadlocking.
        assert_eq!(
            self.identity.enlist(Arc::new(Noop)),
            Err(InfrastructureSqlError::TransactionRequired)
        );
        match self.failure[stage] {
            1 => Err(InfrastructureSqlError::StatementFailed),
            2 => panic!("deliberate completion callback fault"),
            _ => Ok(()),
        }
    }
}
impl PhysicalTransactionParticipant for Probe {
    fn before_commit(&self) -> Result<(), InfrastructureSqlError> {
        self.step(0, "prepare")
    }
    fn after_completion(
        &self,
        outcome: PhysicalTransactionOutcome,
    ) -> Result<(), InfrastructureSqlError> {
        assert_eq!(outcome, self.identity.outcome());
        self.step(1, "settle")
    }
    fn after_commit(&self) -> Result<(), InfrastructureSqlError> {
        self.step(2, "notify")
    }
}
fn enlist(identity: &PhysicalTransactionIdentity, trace: &Trace, index: usize, failure: [u8; 3]) {
    let probe: Arc<dyn PhysicalTransactionParticipant> = Arc::new(Probe {
        index,
        identity: identity.clone(),
        trace: trace.clone(),
        failure,
    });
    identity.enlist(probe.clone()).unwrap();
    identity.enlist(probe).unwrap();
}
fn sequence(trace: &Trace) -> Vec<(usize, &'static str)> {
    trace
        .lock()
        .unwrap()
        .iter()
        .map(|&(index, stage, _, _)| (index, stage))
        .collect()
}

#[test]
fn all_state_settlements_precede_every_notification_and_exact_arc_registration_is_idempotent() {
    let mut owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    let trace = Trace::default();
    enlist(&identity, &trace, 1, [0; 3]);
    enlist(&identity, &trace, 2, [0; 3]);
    assert!(!identity.is_settled());
    assert!(identity.require_active().is_ok());
    owner.prepare_commit().unwrap();
    assert_eq!(
        identity.enlist(Arc::new(Noop)),
        Err(InfrastructureSqlError::TransactionRequired)
    );
    owner.committed().unwrap();
    assert_eq!(
        sequence(&trace),
        [
            (1, "prepare"),
            (2, "prepare"),
            (1, "settle"),
            (2, "settle"),
            (1, "notify"),
            (2, "notify")
        ]
    );
    for &(_, stage, outcome, settled) in trace.lock().unwrap().iter() {
        assert_eq!(
            outcome,
            if stage == "prepare" {
                PhysicalTransactionOutcome::Active
            } else {
                PhysicalTransactionOutcome::Committed
            }
        );
        assert_eq!(settled, stage == "notify");
    }
    assert!(identity.is_settled());
    assert!(!identity.has_completion_error());
    assert!(identity.require_active().is_err());
    assert_eq!(
        identity.enlist(Arc::new(Noop)),
        Err(InfrastructureSqlError::TransactionRequired)
    );
}

#[test]
fn veto_and_panic_attempt_remaining_validators_then_roll_back_without_commit_notifications() {
    for failure in [1, 2] {
        let mut owner = SqlResourceIdentity::new().begin();
        let identity = owner.identity();
        let trace = Trace::default();
        enlist(&identity, &trace, 1, [failure, 0, 0]);
        enlist(&identity, &trace, 2, [0; 3]);
        assert_eq!(
            owner.prepare_commit(),
            Err(InfrastructureSqlError::CompletionFailed)
        );
        assert_eq!(
            owner.prepare_commit(),
            Err(InfrastructureSqlError::TransactionRequired)
        );
        owner.rolled_back().unwrap();
        assert_eq!(
            sequence(&trace),
            [(1, "prepare"), (2, "prepare"), (1, "settle"), (2, "settle")]
        );
        assert_eq!(identity.outcome(), PhysicalTransactionOutcome::RolledBack);
        assert!(identity.is_settled());
    }
}

#[test]
fn errors_and_panics_drain_both_completion_stages_without_changing_acknowledged_commit() {
    for stage in [1, 2] {
        for failure in [1, 2] {
            let mut owner = SqlResourceIdentity::new().begin();
            let identity = owner.identity();
            let trace = Trace::default();
            let mut faults = [0; 3];
            faults[stage] = failure;
            enlist(&identity, &trace, 1, faults);
            enlist(&identity, &trace, 2, [0; 3]);
            owner.prepare_commit().unwrap();
            assert_eq!(
                owner.committed(),
                Err(InfrastructureSqlError::CompletionFailed)
            );
            assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Committed);
            assert!(identity.is_settled());
            assert!(identity.has_completion_error());
            assert_eq!(
                sequence(&trace),
                [
                    (1, "prepare"),
                    (2, "prepare"),
                    (1, "settle"),
                    (2, "settle"),
                    (1, "notify"),
                    (2, "notify")
                ]
            );
        }
    }
}

#[test]
fn rollback_explicit_unknown_and_owner_drop_settle_once_even_when_one_observer_panics() {
    for kind in 0..3 {
        let owner = SqlResourceIdentity::new().begin();
        let identity = owner.identity();
        let trace = Trace::default();
        enlist(&identity, &trace, 1, [0, 2, 0]);
        enlist(&identity, &trace, 2, [0; 3]);
        let expected = if kind == 0 {
            PhysicalTransactionOutcome::RolledBack
        } else {
            PhysicalTransactionOutcome::Unknown
        };
        match kind {
            0 => assert_eq!(
                owner.rolled_back(),
                Err(InfrastructureSqlError::CompletionFailed)
            ),
            1 => assert_eq!(
                owner.unknown(),
                Err(InfrastructureSqlError::CompletionFailed)
            ),
            _ => drop(owner),
        }
        assert_eq!(identity.outcome(), expected);
        assert!(identity.is_settled());
        assert!(identity.has_completion_error());
        assert_eq!(sequence(&trace), [(1, "settle"), (2, "settle")]);
    }
}

#[test]
fn missing_preparation_or_ignored_veto_reports_error_and_preserves_the_real_commit_result() {
    for veto in [false, true] {
        let mut owner = SqlResourceIdentity::new().begin();
        let identity = owner.identity();
        let trace = Trace::default();
        enlist(&identity, &trace, 1, [u8::from(veto), 0, 0]);
        if veto {
            assert!(owner.prepare_commit().is_err());
        }
        assert_eq!(
            owner.committed(),
            Err(InfrastructureSqlError::CompletionFailed)
        );
        assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Committed);
        assert!(identity.is_settled());
        assert!(identity.has_completion_error());
        assert_eq!(sequence(&trace).last(), Some(&(1, "notify")));
    }
}

#[test]
fn another_thread_sees_terminal_outcome_before_settlement_but_cannot_present_it_as_settled() {
    use std::sync::Barrier;
    struct Blocked {
        entered: Arc<Barrier>,
        released: Arc<Barrier>,
    }
    impl PhysicalTransactionParticipant for Blocked {
        fn after_completion(
            &self,
            _: PhysicalTransactionOutcome,
        ) -> Result<(), InfrastructureSqlError> {
            self.entered.wait();
            self.released.wait();
            Ok(())
        }
    }
    let mut owner = SqlResourceIdentity::new().begin();
    let identity = owner.identity();
    let entered = Arc::new(Barrier::new(2));
    let released = Arc::new(Barrier::new(2));
    identity
        .enlist(Arc::new(Blocked {
            entered: entered.clone(),
            released: released.clone(),
        }))
        .unwrap();
    owner.prepare_commit().unwrap();
    let thread = std::thread::spawn(move || owner.committed());
    entered.wait();
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Committed);
    assert!(!identity.is_settled());
    let denied = identity.enlist(Arc::new(Noop));
    released.wait();
    thread.join().unwrap().unwrap();
    assert_eq!(denied, Err(InfrastructureSqlError::TransactionRequired));
    assert!(identity.is_settled());
}

#[test]
fn completion_drains_identity_participant_cycles_on_every_terminal_path() {
    for kind in 0..3 {
        let mut owner = SqlResourceIdentity::new().begin();
        let identity = owner.identity();
        let participant = Arc::new(Probe {
            index: 1,
            identity: identity.clone(),
            trace: Trace::default(),
            failure: [0; 3],
        });
        let weak = Arc::downgrade(&participant);
        identity.enlist(participant.clone()).unwrap();
        drop(participant);
        assert!(weak.upgrade().is_some());
        match kind {
            0 => {
                owner.prepare_commit().unwrap();
                owner.committed().unwrap();
            }
            1 => owner.rolled_back().unwrap(),
            _ => drop(owner),
        }
        assert!(weak.upgrade().is_none());
        assert!(identity.is_settled());
    }
}
