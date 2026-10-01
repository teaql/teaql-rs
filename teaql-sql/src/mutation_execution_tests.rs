use super::*;
use std::{
    future::Future,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Context,
};
use teaql_core::{
    DataType, InsertCommand, PropertyDescriptor, TraceKind, TraceNode, UpdateCommand,
};
use teaql_data_service::{ExecutionObserver, SqlExecutionOutcome, TransactionExecutor};

#[derive(Clone, Copy)]
struct Dialect;
impl SqlDialect for Dialect {
    fn kind(&self) -> crate::DatabaseKind {
        crate::DatabaseKind::Sqlite
    }
    fn quote_ident(&self, name: &str) -> String {
        format!("\"{name}\"")
    }
    fn placeholder(&self, _: usize) -> String {
        "?".into()
    }
}
#[derive(Clone)]
struct Schema;
impl teaql_data_service::SchemaProvider for Schema {
    fn get_entity(&self, name: &str) -> Option<Arc<EntityDescriptor>> {
        (name == "Customer").then(|| {
            Arc::new(
                EntityDescriptor::new("Customer")
                    .property(PropertyDescriptor::new("id", DataType::U64).id())
                    .property(PropertyDescriptor::new("version", DataType::I64).version())
                    .property(PropertyDescriptor::new("name", DataType::Text))
                    .property(PropertyDescriptor::new("status", DataType::Text))
                    .audit_mask_fields(vec!["name".into()]),
            )
        })
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Success,
    FailWrite(usize),
    PendingWrite(usize),
    FailRead,
    EmptyRead,
    PendingRead,
}
#[derive(Clone)]
struct Transport {
    mode: Mode,
    writes: Arc<AtomicUsize>,
    released: Arc<AtomicBool>,
}
struct Lease(Arc<AtomicBool>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
impl SqlTransport for Transport {
    type Error = std::io::Error;
    async fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error> {
        self.released.store(false, Ordering::SeqCst);
        let _lease = Lease(self.released.clone());
        let index = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        if query
            .log_context
            .parameter_policies
            .contains(&teaql_data_service::SqlParameterLogPolicy::Masked)
        {
            assert!(
                query.params.contains(&Value::from("Riverside")),
                "driver value changed"
            );
        }
        match self.mode {
            Mode::FailWrite(at) if index == at => {
                Err(std::io::Error::other("original write error"))
            }
            Mode::PendingWrite(at) if index == at => std::future::pending().await,
            _ => Ok(1),
        }
    }
    async fn fetch_all_compact_sql(
        &self,
        _: &CompiledQuery,
    ) -> Result<Vec<CompactRow>, Self::Error> {
        self.released.store(false, Ordering::SeqCst);
        let _lease = Lease(self.released.clone());
        match self.mode {
            Mode::FailRead => Err(std::io::Error::other("original readback error")),
            Mode::PendingRead => std::future::pending().await,
            Mode::EmptyRead => Ok(vec![]),
            _ => Ok(vec![CompactRow::new(
                Arc::from(["id".into(), "name".into()]),
                vec![Value::U64(1), Value::from("Riverside")],
            )]),
        }
    }
}
impl SqlTransaction for Transport {
    type Error = std::io::Error;
    async fn commit_sql(self) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn rollback_sql(self) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl SqlTransactionTransport for Transport {
    type Tx<'a> = Self;
    async fn begin_sql(&self) -> Result<Self::Tx<'_>, Self::Error> {
        Ok(self.clone())
    }
}
type Entries = Arc<Mutex<Vec<ExecutionMetadata>>>;
type Executor = SqlDataServiceExecutor<Dialect, Transport, Schema>;
fn fixture(mode: Mode, panic_sink: bool) -> (Executor, Entries, ExecutionObserver<'static>) {
    let entries = Arc::new(Mutex::new(Vec::new()));
    let released = Arc::new(AtomicBool::new(true));
    let observer_entries = entries.clone();
    let observer_released = released.clone();
    let observer = Arc::new(move |metadata| {
        assert!(observer_released.load(Ordering::SeqCst));
        observer_entries.lock().unwrap().push(metadata);
        assert!(!panic_sink, "controlled sink failure");
    });
    (
        Executor::new(
            Dialect,
            Transport {
                mode,
                writes: Arc::new(AtomicUsize::new(0)),
                released,
            },
            Schema,
        ),
        entries,
        observer,
    )
}
fn insert(id: u64) -> MutationRequest {
    let mut command = InsertCommand::new("Customer")
        .value("id", id)
        .value("version", 1_i64)
        .value("name", "Riverside");
    command.trace_chain.push(TraceNode::typed(
        TraceKind::AuditReason,
        "Customer",
        Some(id),
        "audited test",
    ));
    teaql_data_service::MutationCommand::Insert(command)
        .request("audited provider conformance test")
        .unwrap()
}
fn batch() -> MutationRequest {
    teaql_data_service::MutationCommand::Batch(vec![insert(1), insert(2), insert(3)])
        .request("audited provider conformance test")
        .unwrap()
}

#[tokio::test]
async fn mutation_target_id_is_sql_intent_provenance_without_changing_plain_binding() {
    let requests = [
        insert(1001),
        teaql_data_service::MutationCommand::Update(
            UpdateCommand::new("Customer", 1001_u64)
                .expected_version(1)
                .value("name", "Riverside"),
        )
        .request("audited provider conformance test")
        .unwrap(),
        teaql_data_service::MutationCommand::Delete(
            teaql_core::DeleteCommand::new("Customer", 1001_u64).expected_version(1),
        )
        .request("audited provider conformance test")
        .unwrap(),
        teaql_data_service::MutationCommand::Recover(teaql_core::RecoverCommand::new(
            "Customer", 1001_u64, -3,
        ))
        .request("audited provider conformance test")
        .unwrap(),
    ];
    for failure in [false, true] {
        for mut request in requests.clone() {
            let trace = match &mut request.command {
                teaql_data_service::MutationCommand::Insert(cmd) => &mut cmd.trace_chain,
                teaql_data_service::MutationCommand::Update(cmd) => &mut cmd.trace_chain,
                teaql_data_service::MutationCommand::Delete(cmd) => &mut cmd.trace_chain,
                teaql_data_service::MutationCommand::Recover(cmd) => &mut cmd.trace_chain,
                teaql_data_service::MutationCommand::Batch(_) => unreachable!(),
            };
            trace.push(TraceNode::typed(
                TraceKind::AuditReason,
                "Customer",
                Some(1001),
                "what: mutate customer 1001",
            ));
            let (executor, entries, observer) = fixture(
                if failure {
                    Mode::FailWrite(1)
                } else {
                    Mode::Success
                },
                false,
            );
            let result = executor.mutate_observed(request, Some(observer)).await;
            let write = if failure {
                assert!(result.is_err());
                entries.lock().unwrap()[0].clone()
            } else {
                assert!(entries.lock().unwrap().is_empty());
                result.unwrap().metadata
            };
            assert!(write.params.contains(&Value::U64(1001)));
            assert!(
                write
                    .trace_chain
                    .iter()
                    .any(|node| node.comment.contains("1001"))
            );
            let mut hidden = Vec::new();
            write
                .sql_log
                .intent_redactions
                .extend_secrets(false, &mut hidden);
            assert!(hidden.contains(&"1001".to_owned()));
        }
    }
}

#[tokio::test]
async fn cached_query_intent_compilation_uses_current_values_without_transport_io() {
    let (executor, entries, _) = fixture(Mode::FailRead, false);
    for name in ["Riverside", "Lakeside"] {
        let query = SelectQuery::new("Customer")
            .filter(Expr::and([
                Expr::eq("name", name),
                Expr::eq("status", "ACTIVE"),
            ]))
            .limit(10)
            .comment("what: compile cached intent");
        let source = executor.query_log_intent(&query);
        let mut safe = Vec::new();
        source.extend_secrets(false, &mut safe);
        assert_eq!(safe, [name]);
        let mut debug = Vec::new();
        source.extend_secrets(true, &mut debug);
        assert!(debug.is_empty());
    }
    assert_eq!(executor.select_plan_cache.read().unwrap().len(), 1);
    assert!(executor.transport.released.load(Ordering::SeqCst));
    assert_eq!(executor.transport.writes.load(Ordering::SeqCst), 0);
    assert!(entries.lock().unwrap().is_empty());
}

#[tokio::test]
async fn transaction_cached_query_intent_compilation_is_io_free_and_conservative_on_unknown() {
    let (executor, _, _) = fixture(Mode::FailRead, false);
    let tx = executor.begin().await.unwrap();
    let query = SelectQuery::new("Customer")
        .filter(Expr::eq("name", "Riverside"))
        .limit(10)
        .comment("what: transaction cached intent");
    let mut safe = Vec::new();
    tx.query_log_intent(&query).extend_secrets(false, &mut safe);
    assert_eq!(safe, ["Riverside"]);
    let unknown = SelectQuery::new("Unknown")
        .filter(Expr::eq("field", "UNKNOWN-SECRET"))
        .limit(10)
        .comment("what: unavailable metadata");
    let mut debug = Vec::new();
    tx.query_log_intent(&unknown)
        .extend_secrets(true, &mut debug);
    assert_eq!(debug, ["UNKNOWN-SECRET"]);
    assert!(executor.transport.released.load(Ordering::SeqCst));
    assert_eq!(executor.transport.writes.load(Ordering::SeqCst), 0);
    teaql_data_service::Transaction::rollback(tx).await.unwrap();
}
fn assert_write(metadata: &ExecutionMetadata, outcome: SqlExecutionOutcome, affected: Option<u64>) {
    assert_eq!(metadata.operation, DataServiceOperation::Insert);
    assert_eq!(metadata.sql_log.execution_outcome, Some(outcome));
    assert_eq!(metadata.affected_rows, affected);
    assert!(metadata.debug_query.is_none());
    assert!(
        metadata
            .sql_log
            .parameter_policies
            .contains(&teaql_data_service::SqlParameterLogPolicy::Masked)
    );
    assert!(
        metadata
            .trace_chain
            .iter()
            .any(|n| n.kind == TraceKind::AuditReason && n.comment == "audited test")
    );
}

#[tokio::test]
async fn direct_write_failure_preserves_driver_error() {
    let (executor, entries, observer) = fixture(Mode::FailWrite(1), false);
    let error = executor
        .mutate_observed(insert(1), Some(observer))
        .await
        .unwrap_err();
    assert!(
        matches!(error, SqlExecutorError::Transport(ref e) if e.to_string() == "original write error")
    );
    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 1);
    assert_write(&entries[0], SqlExecutionOutcome::Failure, None);
}
#[tokio::test]
async fn partial_batch_retains_execution_order_despite_sink_panics() {
    let (executor, entries, observer) = fixture(Mode::FailWrite(2), true);
    let error = executor
        .mutate_observed(batch(), Some(observer))
        .await
        .unwrap_err();
    assert!(
        matches!(error, SqlExecutorError::Transport(ref e) if e.to_string() == "original write error")
    );
    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 2);
    assert_write(&entries[0], SqlExecutionOutcome::Success, Some(1));
    assert_write(&entries[1], SqlExecutionOutcome::Failure, None);
    assert_eq!(executor.transport.writes.load(Ordering::SeqCst), 2);
}
#[test]
fn partial_batch_cancellation_retains_success_and_unknown_inflight_count() {
    let (executor, entries, observer) = fixture(Mode::PendingWrite(2), false);
    let mut future = Box::pin(executor.mutate_observed(batch(), Some(observer)));
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    drop(future);
    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 2);
    assert_write(&entries[0], SqlExecutionOutcome::Success, Some(1));
    assert_write(&entries[1], SqlExecutionOutcome::Cancelled, None);
}
#[tokio::test]
async fn write_success_readback_failure_is_two_distinct_outcomes() {
    let (executor, entries, observer) = fixture(Mode::FailRead, false);
    let tx = executor.begin().await.unwrap();
    let error = tx
        .mutate_observed(insert(1), Some(observer))
        .await
        .unwrap_err();
    assert!(
        matches!(error, SqlExecutorError::Transport(ref e) if e.to_string() == "original readback error")
    );
    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 2);
    assert_write(&entries[0], SqlExecutionOutcome::Success, Some(1));
    assert_eq!(entries[1].operation, DataServiceOperation::Query);
    assert_eq!(
        entries[1].sql_log.execution_outcome,
        Some(SqlExecutionOutcome::Failure)
    );
    assert_eq!(entries[1].result_count, None);
}
#[tokio::test]
async fn empty_readback_is_sql_success_but_mutation_contract_failure() {
    let (executor, entries, observer) = fixture(Mode::EmptyRead, false);
    let tx = executor.begin().await.unwrap();
    assert!(matches!(
        tx.mutate_observed(insert(1), Some(observer))
            .await
            .unwrap_err(),
        SqlExecutorError::PersistedRecord(_)
    ));
    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 2);
    assert_write(&entries[0], SqlExecutionOutcome::Success, Some(1));
    assert_eq!(
        entries[1].sql_log.execution_outcome,
        Some(SqlExecutionOutcome::Success)
    );
    assert_eq!(entries[1].result_count, Some(0));
}
#[tokio::test]
async fn cancellation_during_readback_does_not_relabel_successful_write() {
    let (executor, entries, observer) = fixture(Mode::PendingRead, false);
    let tx = executor.begin().await.unwrap();
    let mut future = Box::pin(tx.mutate_observed(insert(1), Some(observer)));
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    drop(future);
    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 2);
    assert_write(&entries[0], SqlExecutionOutcome::Success, Some(1));
    assert_eq!(entries[1].operation, DataServiceOperation::Query);
    assert_eq!(
        entries[1].sql_log.execution_outcome,
        Some(SqlExecutionOutcome::Cancelled)
    );
}
#[tokio::test]
async fn successful_batch_does_not_notify_fallback_observer() {
    let (executor, entries, observer) = fixture(Mode::Success, false);
    let tx = executor.begin().await.unwrap();
    let result = tx.mutate_observed(batch(), Some(observer)).await.unwrap();
    assert_eq!(result.affected_rows, 3);
    assert_eq!(result.metadata.statements.len(), 3);
    assert!(entries.lock().unwrap().is_empty());
}
#[tokio::test]
async fn later_compile_failure_does_not_invent_an_executed_statement() {
    let (executor, entries, observer) = fixture(Mode::Success, false);
    let missing = teaql_data_service::MutationCommand::Insert(InsertCommand::new("Missing"))
        .request("audited provider conformance test")
        .unwrap();
    assert!(matches!(
        executor
            .mutate_observed(
                teaql_data_service::MutationCommand::Batch(vec![insert(1), missing])
                    .request("audited provider conformance test")
                    .unwrap(),
                Some(observer)
            )
            .await
            .unwrap_err(),
        SqlExecutorError::Compile(_)
    ));
    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 1);
    assert_write(&entries[0], SqlExecutionOutcome::Success, Some(1));
}
#[tokio::test]
async fn guarded_readback_failure_retains_guard_on_both_statements() {
    let (executor, entries, observer) = fixture(Mode::FailRead, false);
    let request = GuardedMutationRequest::new(
        teaql_data_service::MutationCommand::Update(
            UpdateCommand::new("Customer", 1_u64)
                .expected_version(1)
                .value("name", "Riverside"),
        )
        .request("audited provider conformance test")
        .unwrap(),
        Expr::eq("status", "ACTIVE"),
    );
    let error = executor
        .mutate_guarded_observed(request, Some(observer))
        .await
        .unwrap_err();
    assert!(
        matches!(error, SqlExecutorError::Transport(ref e) if e.to_string() == "original readback error")
    );
    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].operation, DataServiceOperation::Update);
    assert_eq!(
        entries[0].sql_log.execution_outcome,
        Some(SqlExecutionOutcome::Success)
    );
    assert_eq!(
        entries[1].sql_log.execution_outcome,
        Some(SqlExecutionOutcome::Failure)
    );
    let mut inherited = Vec::new();
    entries[1]
        .sql_log
        .intent_redactions
        .extend_secrets(false, &mut inherited);
    assert!(inherited.contains(&"Riverside".into()));
    for metadata in entries.iter() {
        assert!(
            metadata
                .parameterized_query
                .as_ref()
                .unwrap()
                .contains("status")
        );
        assert!(metadata.params.contains(&Value::from("ACTIVE")));
    }
}

#[tokio::test]
async fn guarded_transaction_write_failure_has_one_outcome() {
    let (executor, entries, observer) = fixture(Mode::FailWrite(1), false);
    let tx = executor.begin().await.unwrap();
    let request = GuardedMutationRequest::new(
        teaql_data_service::MutationCommand::Update(
            UpdateCommand::new("Customer", 1_u64)
                .expected_version(1)
                .value("name", "Riverside"),
        )
        .request("audited provider conformance test")
        .unwrap(),
        Expr::eq("status", "ACTIVE"),
    );
    assert!(
        tx.mutate_guarded_observed(request, Some(observer))
            .await
            .is_err()
    );
    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].operation, DataServiceOperation::Update);
    assert_eq!(
        entries[0].sql_log.execution_outcome,
        Some(SqlExecutionOutcome::Failure)
    );
    assert_eq!(entries[0].affected_rows, None);
}

#[tokio::test]
async fn nested_batch_failure_does_not_duplicate_prior_statement() {
    let (executor, entries, observer) = fixture(Mode::FailWrite(2), false);
    let request = teaql_data_service::MutationCommand::Batch(vec![
        insert(1),
        teaql_data_service::MutationCommand::Batch(vec![insert(2), insert(3)])
            .request("audited provider conformance test")
            .unwrap(),
    ])
    .request("audited provider conformance test")
    .unwrap();
    assert!(
        executor
            .mutate_observed(request, Some(observer))
            .await
            .is_err()
    );
    let entries = entries.lock().unwrap();
    assert_eq!(entries.len(), 2);
    assert_write(&entries[0], SqlExecutionOutcome::Success, Some(1));
    assert_write(&entries[1], SqlExecutionOutcome::Failure, None);
}

#[tokio::test]
async fn all_mutation_kinds_keep_failure_kind_and_unknown_affected_count() {
    for (request, operation) in [
        (insert(1), DataServiceOperation::Insert),
        (
            teaql_data_service::MutationCommand::Update(
                UpdateCommand::new("Customer", 1_u64)
                    .expected_version(1)
                    .value("name", "Riverside"),
            )
            .request("audited provider conformance test")
            .unwrap(),
            DataServiceOperation::Update,
        ),
        (
            teaql_data_service::MutationCommand::Delete(
                teaql_core::DeleteCommand::new("Customer", 1_u64).expected_version(1),
            )
            .request("audited provider conformance test")
            .unwrap(),
            DataServiceOperation::Delete,
        ),
        (
            teaql_data_service::MutationCommand::Recover(teaql_core::RecoverCommand::new(
                "Customer", 1_u64, -1,
            ))
            .request("audited provider conformance test")
            .unwrap(),
            DataServiceOperation::Recover,
        ),
    ] {
        let (executor, entries, observer) = fixture(Mode::FailWrite(1), false);
        assert!(matches!(
            executor
                .mutate_observed(request, Some(observer))
                .await
                .unwrap_err(),
            SqlExecutorError::Transport(_)
        ));
        let entries = entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operation, operation);
        assert_eq!(
            entries[0].sql_log.execution_outcome,
            Some(SqlExecutionOutcome::Failure)
        );
        assert_eq!(entries[0].affected_rows, None);
    }
}
