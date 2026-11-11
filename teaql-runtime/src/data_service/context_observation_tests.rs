use super::*;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

struct Executor;
impl teaql_data_service::DataServiceExecutor for Executor {
    type Error = std::io::Error;
    fn capabilities(&self) -> teaql_data_service::DataServiceCapabilities {
        Default::default()
    }
}
impl teaql_data_service::QueryExecutor for Executor {
    async fn query(
        &self,
        _: teaql_data_service::QueryRequest,
    ) -> Result<teaql_data_service::QueryResult, Self::Error> {
        unreachable!()
    }
}
impl teaql_data_service::MutationExecutor for Executor {
    async fn mutate(
        &self,
        _: teaql_data_service::MutationRequest,
    ) -> Result<teaql_data_service::MutationResult, Self::Error> {
        unreachable!()
    }
}

#[derive(Default)]
struct Events {
    names: AtomicUsize,
    factories: AtomicUsize,
    polls: AtomicUsize,
    contexts: AtomicUsize,
    operations: Mutex<Vec<String>>,
    finishes: Mutex<Vec<&'static str>>,
}
struct Telemetry(Arc<Events>, bool);
struct Scope(Arc<Events>, bool);
impl crate::RuntimeTelemetry for Telemetry {
    fn start(&self, operation: crate::RuntimeOperation) -> Box<dyn crate::RuntimeTelemetryScope> {
        self.0.operations.lock().unwrap().push(operation.family);
        Box::new(Scope(self.0.clone(), self.1))
    }
}
impl crate::RuntimeTelemetryScope for Scope {
    fn with_context(&self, callback: &mut dyn FnMut()) {
        self.0.contexts.fetch_add(1, Ordering::Relaxed);
        if self.1 {
            panic!("telemetry adapter failed before polling work");
        }
        callback();
    }
    fn success(&mut self, _: BTreeMap<String, crate::RuntimeAttributeValue>) {
        self.0.finishes.lock().unwrap().push("success");
    }
    fn failure(&mut self, _: &str) {
        self.0.finishes.lock().unwrap().push("failure");
    }
}

// A deliberately large, yielding future catches inline telemetry storage
// multiplying the size of the ordinary path, independent of SQL details.
struct Work {
    payload: [u8; 16 * 1024],
    events: Arc<Events>,
    pending: std::cell::Cell<bool>,
    _pin: std::marker::PhantomPinned,
    fail: bool,
    panic: bool,
}
impl Future for Work {
    type Output = Result<u64, DataServiceError<std::io::Error>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.events.polls.fetch_add(1, Ordering::Relaxed);
        if self.pending.replace(false) {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        assert!(!self.panic, "business work panic");
        if self.fail {
            Poll::Ready(Err(DataServiceError::Runtime(crate::RuntimeError::Graph(
                "business work failed".into(),
            ))))
        } else {
            Poll::Ready(Ok(u64::from(self.payload[0])))
        }
    }
}

async fn exercise(enabled: bool, fail: bool, broken_adapter: bool) {
    let events = Arc::new(Events::default());
    let mut context = UserContext::new();
    if enabled {
        context =
            context.with_runtime_telemetry(Arc::new(Telemetry(events.clone(), broken_adapter)));
    }
    let executor = Executor;
    let service = ContextDataService {
        metadata: UserContextMetadata { context: &context },
        executor: &executor,
        mutation_intent: None,
        mutation_privacy: None,
    };
    let work = || {
        events.factories.fetch_add(1, Ordering::Relaxed);
        Work {
            payload: [7; 16 * 1024],
            events: events.clone(),
            pending: std::cell::Cell::new(true),
            _pin: std::marker::PhantomPinned,
            fail,
            panic: false,
        }
    };
    let future = service.observe(
        "mutation",
        || {
            events.names.fetch_add(1, Ordering::Relaxed);
            "Probe.insert".to_owned()
        },
        "Probe",
        work,
    );
    assert!(std::mem::size_of_val(&future) <= std::mem::size_of::<Work>() + 256);
    assert_eq!(events.names.load(Ordering::Relaxed), 0);
    assert_eq!(events.factories.load(Ordering::Relaxed), 0);
    assert_eq!(events.polls.load(Ordering::Relaxed), 0);
    assert!(events.operations.lock().unwrap().is_empty());
    let result = future.await;
    assert_eq!(result.is_err(), fail);
    if !fail {
        assert_eq!(result.unwrap(), 7);
    }
    assert_eq!(events.factories.load(Ordering::Relaxed), 1);
    assert_eq!(events.polls.load(Ordering::Relaxed), 2);
    assert_eq!(events.names.load(Ordering::Relaxed), usize::from(enabled));
    if enabled {
        assert_eq!(*events.operations.lock().unwrap(), ["mutation", "provider"]);
        let expected = if fail { "failure" } else { "success" };
        assert_eq!(*events.finishes.lock().unwrap(), [expected, expected]);
        assert!(events.contexts.load(Ordering::Relaxed) >= 4);
    } else {
        assert!(events.operations.lock().unwrap().is_empty());
        assert!(events.finishes.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn ordinary_observation_has_bounded_future_and_preserves_results() {
    exercise(false, false, false).await;
    exercise(false, true, false).await;
}

#[tokio::test]
async fn instrumented_observation_is_lazy_and_handles_pending_success_and_failure() {
    exercise(true, false, false).await;
    exercise(true, true, false).await;
}

#[tokio::test]
async fn instrumented_box_does_not_contain_the_large_business_future() {
    let events = Arc::new(Events::default());
    let context =
        UserContext::new().with_runtime_telemetry(Arc::new(Telemetry(events.clone(), false)));
    let executor = Executor;
    let service = ContextDataService {
        metadata: UserContextMetadata { context: &context },
        executor: &executor,
        mutation_intent: None,
        mutation_privacy: None,
    };
    let mut work = std::pin::pin!(Work {
        payload: [7; 16 * 1024],
        events: events.clone(),
        pending: std::cell::Cell::new(true),
        _pin: std::marker::PhantomPinned,
        fail: false,
        panic: false,
    });
    let future = service.observe_instrumented(
        "mutation",
        || "Probe.insert".to_owned(),
        "Probe",
        work.as_mut(),
    );
    assert!(std::mem::size_of_val(future.as_ref().get_ref()) <= 1024);
    assert!(events.operations.lock().unwrap().is_empty());
    assert_eq!(future.await.unwrap(), 7);
    assert_eq!(events.polls.load(Ordering::Relaxed), 2);
    assert_eq!(*events.finishes.lock().unwrap(), ["success", "success"]);
}

#[tokio::test]
async fn instrumented_observation_remains_fail_open() {
    exercise(true, false, true).await;
}

#[tokio::test]
async fn unpolled_instrumented_work_is_dropped_without_starting_observation() {
    let events = Arc::new(Events::default());
    let context =
        UserContext::new().with_runtime_telemetry(Arc::new(Telemetry(events.clone(), false)));
    let executor = Executor;
    let service = ContextDataService {
        metadata: UserContextMetadata { context: &context },
        executor: &executor,
        mutation_intent: None,
        mutation_privacy: None,
    };
    let work = || {
        events.factories.fetch_add(1, Ordering::Relaxed);
        Work {
            payload: [7; 16 * 1024],
            events: events.clone(),
            pending: std::cell::Cell::new(true),
            _pin: std::marker::PhantomPinned,
            fail: false,
            panic: false,
        }
    };
    let future = service.observe("mutation", || panic!("must stay lazy"), "Probe", work);
    drop(future);
    assert_eq!(
        Arc::strong_count(&events),
        2,
        "unpolled factory must not retain extra state"
    );
    assert_eq!(events.factories.load(Ordering::Relaxed), 0);
    assert_eq!(events.polls.load(Ordering::Relaxed), 0);
    assert!(events.operations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn pending_instrumented_work_can_be_cancelled_without_finishing_observation() {
    let events = Arc::new(Events::default());
    let context =
        UserContext::new().with_runtime_telemetry(Arc::new(Telemetry(events.clone(), false)));
    let executor = Executor;
    let service = ContextDataService {
        metadata: UserContextMetadata { context: &context },
        executor: &executor,
        mutation_intent: None,
        mutation_privacy: None,
    };
    let work = || {
        events.factories.fetch_add(1, Ordering::Relaxed);
        Work {
            payload: [7; 16 * 1024],
            events: events.clone(),
            pending: std::cell::Cell::new(true),
            _pin: std::marker::PhantomPinned,
            fail: false,
            panic: false,
        }
    };
    let mut future =
        Box::pin(service.observe("mutation", || "Probe.insert".to_owned(), "Probe", work));
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert_eq!(events.polls.load(Ordering::Relaxed), 1);
    drop(future);
    assert_eq!(
        Arc::strong_count(&events),
        2,
        "cancelled work and scopes must be released"
    );
    assert_eq!(*events.operations.lock().unwrap(), ["mutation", "provider"]);
    assert!(events.finishes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn instrumented_observation_propagates_business_panic() {
    use futures_util::FutureExt;
    let events = Arc::new(Events::default());
    let context =
        UserContext::new().with_runtime_telemetry(Arc::new(Telemetry(events.clone(), false)));
    let executor = Executor;
    let service = ContextDataService {
        metadata: UserContextMetadata { context: &context },
        executor: &executor,
        mutation_intent: None,
        mutation_privacy: None,
    };
    let work = || {
        events.factories.fetch_add(1, Ordering::Relaxed);
        Work {
            payload: [7; 16 * 1024],
            events: events.clone(),
            pending: std::cell::Cell::new(true),
            _pin: std::marker::PhantomPinned,
            fail: false,
            panic: true,
        }
    };
    let result = std::panic::AssertUnwindSafe(service.observe(
        "mutation",
        || "Probe.insert".to_owned(),
        "Probe",
        work,
    ))
    .catch_unwind()
    .await;
    assert!(result.is_err());
    assert_eq!(events.factories.load(Ordering::Relaxed), 1);
    assert_eq!(events.polls.load(Ordering::Relaxed), 2);
    assert!(events.finishes.lock().unwrap().is_empty());
}
