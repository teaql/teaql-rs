//! Real owner completion and independent SQL oracles. No synthetic commit acknowledgement.
use super::{Fixture, execute, scalar};
use std::sync::{Arc, Mutex};
use teaql_data_service::infrastructure_sql::*;
use teaql_data_service::{DataServiceExecutor, Transaction, TransactionExecutor};

type Trace = Arc<Mutex<Vec<(u8, &'static str, PhysicalTransactionOutcome, bool)>>>;
struct Probe {
    index: u8,
    identity: PhysicalTransactionIdentity,
    trace: Trace,
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
        match self.failure[stage] {
            1 => Err(InfrastructureSqlError::CompletionFailed),
            2 => panic!("deliberate real SQL completion callback fault"),
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
fn enlist(identity: &PhysicalTransactionIdentity, trace: &Trace, index: u8, failure: [u8; 3]) {
    let probe: Arc<dyn PhysicalTransactionParticipant> = Arc::new(Probe {
        index,
        identity: identity.clone(),
        trace: trace.clone(),
        failure,
    });
    identity.enlist(probe.clone()).unwrap();
    identity.enlist(probe).unwrap();
}
fn assert_trace(
    trace: &Trace,
    expected: &[(u8, &'static str)],
    outcome: PhysicalTransactionOutcome,
) {
    let trace = trace.lock().unwrap();
    assert_eq!(
        trace
            .iter()
            .map(|&(index, stage, _, _)| (index, stage))
            .collect::<Vec<_>>(),
        expected
    );
    for &(_, stage, actual, settled) in trace.iter() {
        assert_eq!(
            actual,
            if stage == "prepare" {
                PhysicalTransactionOutcome::Active
            } else {
                outcome
            }
        );
        assert_eq!(settled, stage == "notify");
    }
}
const COMMIT_TRACE: &[(u8, &str)] = &[
    (1, "prepare"),
    (2, "prepare"),
    (1, "settle"),
    (2, "settle"),
    (1, "notify"),
    (2, "notify"),
];
const VETO_TRACE: &[(u8, &str)] = &[(1, "prepare"), (2, "prepare"), (1, "settle"), (2, "settle")];
const ABORT_TRACE: &[(u8, &str)] = &[(1, "settle"), (2, "settle")];
async fn insert(sql: &dyn InfrastructureSql) {
    execute(
        sql,
        "INSERT INTO r5_assets(id,version) VALUES (42,1)",
        vec![],
    )
    .await;
}

#[tokio::test]
async fn native_wrapper_settles_all_participants_before_notifications_on_the_real_commit() {
    let f = Fixture::with_port(true, 55432, 1).await;
    let business = f.business();
    let tx = TransactionExecutor::begin(&business).await.unwrap();
    let sql = tx.infrastructure_sql().unwrap();
    let identity = sql.transaction_identity().unwrap();
    let pid = scalar(sql, "SELECT pg_backend_pid()::bigint AS value").await;
    let trace = Trace::default();
    enlist(&identity, &trace, 1, [0; 3]);
    enlist(&identity, &trace, 2, [0; 3]);
    insert(sql).await;
    assert_eq!(f.count("r5_assets").await, 0);
    assert!(trace.lock().unwrap().is_empty());
    Transaction::commit(tx).await.unwrap();
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Committed);
    assert!(identity.is_settled());
    assert!(!identity.has_completion_error());
    assert_trace(&trace, COMMIT_TRACE, PhysicalTransactionOutcome::Committed);
    assert_eq!(f.count("r5_assets").await, 1);
    assert_eq!(
        scalar(f.root.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await,
        pid
    );
    let next = f.root.begin().await.unwrap();
    assert_ne!(identity, next.transaction_identity().unwrap());
    next.rollback().await.unwrap();
    println!("R5_NATIVE_PHYSICAL_COMPLETION_ORDER_AND_POOL_REUSE_PASS");
    f.finish().await;
}

#[tokio::test]
async fn before_commit_veto_and_panic_really_roll_back_and_preserve_all_remaining_settlements() {
    for failure in [1, 2] {
        let f = Fixture::with_port(true, 55432, 1).await;
        let tx = f.root.begin().await.unwrap();
        let identity = tx.transaction_identity().unwrap();
        let pid = scalar(tx.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await;
        let trace = Trace::default();
        enlist(&identity, &trace, 1, [failure, 0, 0]);
        enlist(&identity, &trace, 2, [0; 3]);
        insert(tx.as_ref()).await;
        assert_eq!(
            tx.commit().await.unwrap_err(),
            InfrastructureSqlError::CompletionFailed
        );
        assert_eq!(identity.outcome(), PhysicalTransactionOutcome::RolledBack);
        assert!(identity.is_settled());
        assert_trace(&trace, VETO_TRACE, PhysicalTransactionOutcome::RolledBack);
        assert_eq!(f.count("r5_assets").await, 0);
        assert_eq!(
            scalar(f.root.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await,
            pid
        );
        f.finish().await;
    }
}

#[tokio::test]
async fn settlement_or_notification_failure_reports_after_commit_and_cannot_undo_database_rows() {
    for stage in [1, 2] {
        for failure in [1, 2] {
            let f = Fixture::with_port(true, 55432, 1).await;
            let tx = f.root.begin().await.unwrap();
            let identity = tx.transaction_identity().unwrap();
            let pid = scalar(tx.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await;
            let trace = Trace::default();
            let mut faults = [0; 3];
            faults[stage] = failure;
            enlist(&identity, &trace, 1, faults);
            enlist(&identity, &trace, 2, [0; 3]);
            insert(tx.as_ref()).await;
            let error = tx.commit().await.unwrap_err();
            assert_eq!(error, InfrastructureSqlError::TransactionAfterCommit);
            let blob_error = teaql_blob_reference::BlobError::from(error);
            assert_eq!(
                blob_error.completion,
                Some(PhysicalTransactionOutcome::Committed)
            );
            assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Committed);
            assert!(identity.is_settled());
            assert!(identity.has_completion_error());
            assert_trace(&trace, COMMIT_TRACE, PhysicalTransactionOutcome::Committed);
            assert_eq!(f.count("r5_assets").await, 1);
            assert_eq!(
                scalar(f.root.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await,
                pid
            );
            f.finish().await;
        }
    }
    println!("R5_REAL_AFTER_COMMIT_FAILURE_DRAINS_AND_RETAINS_COMMIT_PASS");
}

#[tokio::test]
async fn explicit_rollback_and_rollback_only_skip_commit_notifications_even_if_settlement_panics() {
    for rollback_only in [false, true] {
        let f = Fixture::with_port(true, 55432, 1).await;
        let tx = f.root.begin().await.unwrap();
        let identity = tx.transaction_identity().unwrap();
        let trace = Trace::default();
        enlist(&identity, &trace, 1, [0, 2, 0]);
        enlist(&identity, &trace, 2, [0; 3]);
        insert(tx.as_ref()).await;
        let error = if rollback_only {
            tx.mark_rollback_only().unwrap();
            tx.commit().await.unwrap_err()
        } else {
            tx.rollback().await.unwrap_err()
        };
        assert_eq!(error, InfrastructureSqlError::CompletionFailed);
        assert_eq!(identity.outcome(), PhysicalTransactionOutcome::RolledBack);
        assert!(identity.is_settled());
        assert!(identity.has_completion_error());
        assert_trace(&trace, ABORT_TRACE, PhysicalTransactionOutcome::RolledBack);
        assert_eq!(f.count("r5_assets").await, 0);
        f.finish().await;
    }
}

#[tokio::test]
async fn dropped_native_owner_settles_unknown_once_discards_backend_and_rolls_back_rows() {
    let f = Fixture::with_port(true, 55432, 1).await;
    let tx = f.root.begin().await.unwrap();
    let identity = tx.transaction_identity().unwrap();
    let trace = Trace::default();
    enlist(&identity, &trace, 1, [0, 2, 0]);
    enlist(&identity, &trace, 2, [0; 3]);
    let pid = scalar(tx.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await;
    insert(tx.as_ref()).await;
    drop(tx);
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Unknown);
    assert!(identity.is_settled());
    assert!(identity.has_completion_error());
    assert_trace(&trace, ABORT_TRACE, PhysicalTransactionOutcome::Unknown);
    assert_ne!(
        scalar(f.root.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await,
        pid
    );
    f.wait_backend_gone(pid).await;
    assert_eq!(f.count("r5_assets").await, 0);
    f.finish().await;
}

#[tokio::test]
async fn real_commit_ack_loss_settles_unknown_without_notifications_while_sql_proves_commit() {
    let mut proxy = super::support::proxy::ProtocolFault::new().await;
    let f = Fixture::with_port(true, proxy.port, 1).await;
    let tx = f.root.begin().await.unwrap();
    let identity = tx.transaction_identity().unwrap();
    let trace = Trace::default();
    enlist(&identity, &trace, 1, [0; 3]);
    enlist(&identity, &trace, 2, [0; 3]);
    insert(tx.as_ref()).await;
    proxy.drop_commit_ack();
    let error = tx.commit().await.unwrap_err();
    assert_eq!(error, InfrastructureSqlError::TransactionUnknown);
    assert_eq!(
        teaql_blob_reference::BlobError::from(error).completion,
        Some(PhysicalTransactionOutcome::Unknown)
    );
    proxy.intercepted().await;
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Unknown);
    assert!(identity.is_settled());
    assert!(!identity.has_completion_error());
    assert_trace(&trace, VETO_TRACE, PhysicalTransactionOutcome::Unknown);
    assert_eq!(f.count("r5_assets").await, 1);
    println!("R5_REAL_COMMIT_ACK_LOSS_COMPLETION_UNKNOWN_SQL_COMMITTED_PASS");
    f.finish().await;
    drop(proxy);
}

#[tokio::test]
async fn cancelled_waiting_query_settles_unknown_and_discards_the_physical_owner() {
    let f = Fixture::with_port(true, 55432, 2).await;
    let blocker = f.root.begin().await.unwrap();
    insert(blocker.as_ref()).await;
    let tx = f.root.begin().await.unwrap();
    let identity = tx.transaction_identity().unwrap();
    let trace = Trace::default();
    enlist(&identity, &trace, 1, [0; 3]);
    enlist(&identity, &trace, 2, [0; 3]);
    execute(
        tx.as_ref(),
        "SET LOCAL client_connection_check_interval='100ms'",
        vec![],
    )
    .await;
    let pid = scalar(tx.as_ref(), "SELECT pg_backend_pid()::bigint AS value").await;
    execute(
        tx.as_ref(),
        "INSERT INTO r5_assets(id,version) VALUES (43,1)",
        vec![],
    )
    .await;
    let pending = tokio::spawn(async move {
        insert(tx.as_ref()).await;
        tx.commit().await
    });
    f.wait_for_lock().await;
    pending.abort();
    assert!(matches!(pending.await, Err(error) if error.is_cancelled()));
    assert_eq!(identity.outcome(), PhysicalTransactionOutcome::Unknown);
    assert!(identity.is_settled());
    assert_trace(&trace, ABORT_TRACE, PhysicalTransactionOutcome::Unknown);
    f.wait_backend_gone(pid).await;
    assert_eq!(f.count("r5_assets").await, 0);
    blocker.rollback().await.unwrap();
    assert_eq!(f.count("r5_assets").await, 0);
    println!("R5_REAL_CANCELLED_QUERY_PHYSICAL_COMPLETION_AND_BACKEND_CLOSE_PASS");
    f.finish().await;
}
