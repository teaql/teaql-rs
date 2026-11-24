use super::*;
use crate::{RuntimeAttributeValue, RuntimeTelemetry, RuntimeTelemetryScope};
use futures_util::FutureExt;
use std::{
    cell::{Cell, RefCell},
    sync::{Arc, Mutex},
};

thread_local! { static ACTIVE: RefCell<Option<String>> = const { RefCell::new(None) }; }
fn active() -> Option<String> {
    ACTIVE.with(|slot| slot.borrow().clone())
}
struct Activation(Option<String>);
impl Drop for Activation {
    fn drop(&mut self) {
        ACTIVE.with(|slot| slot.replace(self.0.take()));
    }
}
#[derive(Debug, Clone, PartialEq)]
enum Event {
    Start(String, Option<String>),
    End(String, Option<String>),
}
#[derive(Default)]
struct Recorder(Mutex<Vec<Event>>);
struct Telemetry(Arc<Recorder>);
struct Scope {
    name: String,
    events: Arc<Recorder>,
}
impl RuntimeTelemetry for Telemetry {
    fn records_sql_diagnostics(&self) -> bool {
        true
    }
    fn start(&self, operation: RuntimeOperation) -> Box<dyn RuntimeTelemetryScope> {
        self.0
            .0
            .lock()
            .unwrap()
            .push(Event::Start(operation.name.clone(), active()));
        Box::new(Scope {
            name: operation.name,
            events: self.0.clone(),
        })
    }
}
impl RuntimeTelemetryScope for Scope {
    fn with_context(&self, callback: &mut dyn FnMut()) {
        let _activation = Activation(ACTIVE.with(|slot| slot.replace(Some(self.name.clone()))));
        callback();
    }
    fn success(&mut self, _: BTreeMap<String, RuntimeAttributeValue>) {
        self.events
            .0
            .lock()
            .unwrap()
            .push(Event::End(self.name.clone(), None));
    }
    fn failure(&mut self, error: &str) {
        self.events
            .0
            .lock()
            .unwrap()
            .push(Event::End(self.name.clone(), Some(error.into())));
    }
}
fn context() -> (UserContext, Arc<Recorder>) {
    let events = Arc::new(Recorder::default());
    (
        UserContext::new().with_runtime_telemetry(Arc::new(Telemetry(events.clone()))),
        events,
    )
}
fn ends(events: &Arc<Recorder>, name: &str) -> Vec<Event> {
    events
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|event| matches!(event, Event::End(value, _) if value == name))
        .cloned()
        .collect()
}
fn assert_idle() {
    assert!(active().is_none());
    assert!(
        teaql_data_service::statement_telemetry::start_sql_statement(
            "test",
            teaql_data_service::DataServiceOperation::Query
        )
        .is_none()
    );
}

#[tokio::test]
async fn root_is_lazy_and_carries_statement_factory_without_repeating_completion() {
    let (context, events) = context();
    let future = context.observe_mutation_request(|| "Probe".into(), async {
        assert_eq!(active().as_deref(), Some("Probe.saveGraph"));
        let statement = teaql_data_service::statement_telemetry::start_sql_statement(
            "test",
            teaql_data_service::DataServiceOperation::Query,
        )
        .unwrap();
        statement.finish(teaql_data_service::SqlExecutionOutcome::Success);
        Ok::<_, ()>(42)
    });
    assert!(events.0.lock().unwrap().is_empty());
    assert_eq!(future.await, Ok(42));
    assert_idle();
    assert_eq!(
        ends(&events, "Probe.saveGraph"),
        [Event::End("Probe.saveGraph".into(), None)]
    );
    assert!(events.0.lock().unwrap().contains(&Event::Start(
        "test.statement".into(),
        Some("Probe.saveGraph".into())
    )));
}

#[tokio::test]
async fn failures_keep_original_result_and_export_only_fixed_error_type() {
    let (context, events) = context();
    let result = context
        .observe_mutation_request(|| "Failure".into(), async {
            Err::<(), _>("PRIVATE_DATABASE_MESSAGE")
        })
        .await;
    assert_eq!(result, Err("PRIVATE_DATABASE_MESSAGE"));
    assert_eq!(
        ends(&events, "Failure.saveGraph"),
        [Event::End(
            "Failure.saveGraph".into(),
            Some("mutation_request_error".into())
        )]
    );
    assert!(!format!("{:?}", events.0.lock().unwrap()).contains("PRIVATE_DATABASE_MESSAGE"));
    assert_idle();
}

#[tokio::test]
async fn concurrent_requests_have_distinct_roots_and_restore_context_after_pending() {
    let (context, events) = context();
    let a = context.observe_mutation_request(|| "A".into(), async {
        assert_eq!(active().as_deref(), Some("A.saveGraph"));
        tokio::task::yield_now().await;
        assert_eq!(active().as_deref(), Some("A.saveGraph"));
        Ok::<_, ()>(1)
    });
    let b = context.observe_mutation_request(|| "B".into(), async {
        assert_eq!(active().as_deref(), Some("B.saveGraph"));
        tokio::task::yield_now().await;
        assert_eq!(active().as_deref(), Some("B.saveGraph"));
        Ok::<_, ()>(2)
    });
    assert_eq!(tokio::join!(a, b), (Ok(1), Ok(2)));
    assert_idle();
    let events = events.0.lock().unwrap();
    assert!(events.contains(&Event::Start("A.saveGraph".into(), None)));
    assert!(events.contains(&Event::Start("B.saveGraph".into(), None)));
}

#[tokio::test]
async fn nested_requests_are_not_merged_into_one_mutation_owner() {
    let (context, events) = context();
    context
        .observe_mutation_request(|| "Outer".into(), async {
            context
                .observe_mutation_request(|| "Inner".into(), async { Ok::<_, ()>(()) })
                .await?;
            assert_eq!(active().as_deref(), Some("Outer.saveGraph"));
            Ok::<_, ()>(())
        })
        .await
        .unwrap();
    assert_idle();
    assert!(events.0.lock().unwrap().contains(&Event::Start(
        "Inner.saveGraph".into(),
        Some("Outer.saveGraph".into())
    )));
    assert_eq!(ends(&events, "Outer.saveGraph").len(), 1);
    assert_eq!(ends(&events, "Inner.saveGraph").len(), 1);
}

#[test]
fn dropping_pending_request_records_cancellation_but_unpolled_drop_does_not_invent_execution() {
    let (context, events) = context();
    let untouched =
        context.observe_mutation_request(|| "Untouched".into(), async { Ok::<_, ()>(()) });
    drop(untouched);
    assert!(events.0.lock().unwrap().is_empty());
    let mut pending = Box::pin(context.observe_mutation_request(
        || "Pending".into(),
        std::future::pending::<Result<(), ()>>(),
    ));
    assert!(pending.as_mut().now_or_never().is_none());
    assert_idle();
    drop(pending);
    assert_eq!(
        ends(&events, "Pending.saveGraph"),
        [Event::End(
            "Pending.saveGraph".into(),
            Some("mutation_request_cancelled".into())
        )]
    );
}

#[test]
fn source_panic_preserves_payload_and_finishes_request_without_poisoned_scope() {
    let (context, events) = context();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            context
                .observe_mutation_request(|| "Panic".into(), async {
                    panic!("controlled mutation panic");
                    #[allow(unreachable_code)]
                    Ok::<_, ()>(())
                })
                .now_or_never()
        }))
        .is_err()
    );
    assert_idle();
    assert_eq!(
        ends(&events, "Panic.saveGraph"),
        [Event::End(
            "Panic.saveGraph".into(),
            Some("mutation_request_error".into())
        )]
    );
}

#[tokio::test]
async fn disabled_path_never_evaluates_metadata_or_allocates_a_scope() {
    let context = UserContext::new().with_runtime_telemetry(Arc::new(crate::NoopRuntimeTelemetry));
    let calls = Cell::new(0);
    let value = context
        .observe_mutation_request(
            || {
                calls.set(calls.get() + 1);
                panic!("metadata should not be read")
            },
            async { Ok::<_, ()>(7) },
        )
        .await;
    assert_eq!(value, Ok(7));
    assert_eq!(calls.get(), 0);
}
