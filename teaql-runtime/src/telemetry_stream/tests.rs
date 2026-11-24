use super::*;
use crate::{RuntimeAttributeValue, RuntimeTelemetry, RuntimeTelemetryScope};
use futures_util::{FutureExt, StreamExt};
use std::{cell::RefCell, collections::VecDeque, sync::Mutex};

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
    Poll(Option<String>),
    Close(Option<String>),
    Finish(String, Option<String>, i64),
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
impl Scope {
    fn finish(&self, error: Option<&str>, attributes: BTreeMap<String, RuntimeAttributeValue>) {
        let count = match attributes.get("teaql.result.cardinality") {
            Some(RuntimeAttributeValue::Integer(value)) => *value,
            _ => -1,
        };
        self.events.0.lock().unwrap().push(Event::Finish(
            self.name.clone(),
            error.map(str::to_owned),
            count,
        ));
    }
}
impl RuntimeTelemetryScope for Scope {
    fn with_context(&self, callback: &mut dyn FnMut()) {
        let _activation = Activation(ACTIVE.with(|slot| slot.replace(Some(self.name.clone()))));
        callback();
    }
    fn success(&mut self, attributes: BTreeMap<String, RuntimeAttributeValue>) {
        self.finish(None, attributes);
    }
    fn failure(&mut self, error: &str) {
        self.finish(Some(error), BTreeMap::new());
    }
    fn failure_with_attributes(
        &mut self,
        error: &str,
        attributes: BTreeMap<String, RuntimeAttributeValue>,
    ) {
        self.finish(Some(error), attributes);
    }
}

fn context() -> (UserContext, Arc<Recorder>) {
    let events = Arc::new(Recorder::default());
    (
        UserContext::new().with_runtime_telemetry(Arc::new(Telemetry(events.clone()))),
        events,
    )
}
enum Step {
    Row(usize),
    Pending,
    Error(&'static str),
    Panic,
}
struct Probe<'a> {
    context: &'a UserContext,
    other: Option<&'a UserContext>,
    events: Arc<Recorder>,
    steps: VecDeque<Step>,
}
impl Stream for Probe<'_> {
    type Item = Result<usize, &'static str>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.events.0.lock().unwrap().push(Event::Poll(active()));
        // Both request ownership and the SDK-neutral physical factory must be
        // available during every poll, and absent between polls.
        let child = this
            .context
            .observe_query_request("SameContextChild", async { Ok::<_, ()>(()) });
        let mut child = std::pin::pin!(child);
        assert_eq!(child.as_mut().poll(cx), Poll::Ready(Ok(())));
        if let Some(other) = this.other {
            let child = other.observe_query_request("OtherContextChild", async { Ok::<_, ()>(()) });
            let mut child = std::pin::pin!(child);
            assert_eq!(child.as_mut().poll(cx), Poll::Ready(Ok(())));
        }
        let statement = teaql_data_service::statement_telemetry::start_sql_statement(
            "test",
            teaql_data_service::DataServiceOperation::Query,
        )
        .expect("carried statement factory");
        statement.finish(teaql_data_service::SqlExecutionOutcome::Success);
        match this.steps.pop_front() {
            Some(Step::Row(value)) => Poll::Ready(Some(Ok(value))),
            Some(Step::Pending) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(Step::Error(error)) => Poll::Ready(Some(Err(error))),
            Some(Step::Panic) => panic!("controlled source panic"),
            None => Poll::Ready(None),
        }
    }
}
impl Drop for Probe<'_> {
    fn drop(&mut self) {
        self.events.0.lock().unwrap().push(Event::Close(active()));
    }
}
fn source<'a>(
    context: &'a UserContext,
    events: &Arc<Recorder>,
    steps: Vec<Step>,
) -> EntityStream<'a, usize, &'static str> {
    Box::pin(Probe {
        context,
        other: None,
        events: events.clone(),
        steps: steps.into(),
    })
}
fn assert_idle() {
    assert!(active().is_none());
    assert!(
        teaql_data_service::statement_telemetry::start_sql_statement(
            "test",
            teaql_data_service::DataServiceOperation::Query,
        )
        .is_none()
    );
}
fn finishes(events: &Arc<Recorder>, name: &str) -> Vec<Event> {
    events
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|event| matches!(event, Event::Finish(value, _, _) if value == name))
        .cloned()
        .collect()
}

#[test]
fn unpolled_drop_does_not_invent_request_or_sql() {
    let (context, events) = context();
    let stream =
        context.observe_generated_stream("Never", source(&context, &events, vec![Step::Row(1)]));
    assert!(events.0.lock().unwrap().is_empty());
    drop(stream);
    assert_eq!(*events.0.lock().unwrap(), [Event::Close(None)]);
    assert_idle();
}

#[tokio::test]
async fn exhaustion_counts_entities_and_closes_source_before_root_once() {
    let (context, events) = context();
    let mut stream = context.observe_generated_stream(
        "Probe",
        source(&context, &events, vec![Step::Row(1), Step::Row(2)]),
    );
    assert_eq!(stream.next().await, Some(Ok(1)));
    assert_idle();
    assert_eq!(stream.next().await, Some(Ok(2)));
    assert_idle();
    assert!(finishes(&events, "Probe.stream").is_empty());
    assert_eq!(stream.next().await, None);
    assert_eq!(stream.next().await, None);
    drop(stream);
    assert_eq!(
        finishes(&events, "Probe.stream"),
        [Event::Finish("Probe.stream".into(), None, 2)]
    );
    let events = events.0.lock().unwrap();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::Start(name, _) if name == "SameContextChild.request"))
    );
    let close = events
        .iter()
        .position(|e| *e == Event::Close(Some("Probe.stream".into())))
        .unwrap();
    let finish = events
        .iter()
        .position(|e| matches!(e, Event::Finish(name, _, _) if name == "Probe.stream"))
        .unwrap();
    assert!(close < finish);
    assert_idle();
}

#[tokio::test]
async fn early_drop_counts_only_delivered_entities_not_remaining_buffer() {
    let (context, events) = context();
    let mut stream = context.observe_generated_stream(
        "Partial",
        source(
            &context,
            &events,
            vec![Step::Row(1), Step::Row(2), Step::Row(3)],
        ),
    );
    assert_eq!(stream.next().await, Some(Ok(1)));
    drop(stream);
    assert_eq!(
        finishes(&events, "Partial.stream"),
        [Event::Finish(
            "Partial.stream".into(),
            Some("stream_cancelled".into()),
            1
        )]
    );
    assert_idle();
}

#[tokio::test]
async fn pending_drop_and_empty_exhaustion_have_distinct_zero_row_outcomes() {
    let (context, events) = context();
    let mut pending =
        context.observe_generated_stream("Pending", source(&context, &events, vec![Step::Pending]));
    assert!(pending.next().now_or_never().is_none());
    assert_idle();
    drop(pending);
    let mut empty = context.observe_generated_stream("Empty", source(&context, &events, vec![]));
    assert_eq!(empty.next().await, None);
    drop(empty);
    assert_eq!(
        finishes(&events, "Pending.stream"),
        [Event::Finish(
            "Pending.stream".into(),
            Some("stream_cancelled".into()),
            0
        )]
    );
    assert_eq!(
        finishes(&events, "Empty.stream"),
        [Event::Finish("Empty.stream".into(), None, 0)]
    );
    assert_idle();
}

#[tokio::test]
async fn original_error_is_returned_once_and_never_exported_as_error_type() {
    let (context, events) = context();
    let mut stream = context.observe_generated_stream(
        "Error",
        source(
            &context,
            &events,
            vec![Step::Row(1), Step::Error("PRIVATE_PROVIDER_MESSAGE")],
        ),
    );
    assert_eq!(stream.next().await, Some(Ok(1)));
    assert_eq!(stream.next().await, Some(Err("PRIVATE_PROVIDER_MESSAGE")));
    assert_eq!(stream.next().await, None);
    drop(stream);
    assert_eq!(
        finishes(&events, "Error.stream"),
        [Event::Finish(
            "Error.stream".into(),
            Some("stream_query_error".into()),
            1
        )]
    );
    assert!(!format!("{:?}", events.0.lock().unwrap()).contains("PRIVATE_PROVIDER_MESSAGE"));
    assert_idle();
}

#[tokio::test]
async fn overlapping_pending_streams_restore_their_own_context_on_resume_and_close() {
    let (context, events) = context();
    let mut a = context.observe_generated_stream(
        "A",
        source(&context, &events, vec![Step::Pending, Step::Row(1)]),
    );
    let mut b =
        context.observe_generated_stream("B", source(&context, &events, vec![Step::Row(2)]));
    assert!(a.next().now_or_never().is_none());
    assert_idle();
    assert_eq!(b.next().await, Some(Ok(2)));
    assert_idle();
    context
        .observe_query_request("Independent", async { Ok::<_, ()>(()) })
        .await
        .unwrap();
    assert_idle();
    drop(b);
    assert_eq!(a.next().await, Some(Ok(1)));
    assert_eq!(a.next().await, None);
    drop(a);
    let events = events.0.lock().unwrap();
    assert!(events.contains(&Event::Start("Independent.request".into(), None)));
    assert!(events.contains(&Event::Close(Some("B.stream".into()))));
    assert!(events.contains(&Event::Close(Some("A.stream".into()))));
    assert_idle();
}

#[tokio::test]
async fn request_suppression_does_not_cross_user_context_boundaries() {
    let (context, events) = context();
    let other = UserContext::new().with_runtime_telemetry(Arc::new(Telemetry(events.clone())));
    let source = Box::pin(Probe {
        context: &context,
        other: Some(&other),
        events: events.clone(),
        steps: vec![Step::Row(1)].into(),
    });
    let mut stream = context.observe_generated_stream("Owner", source);
    assert_eq!(stream.next().await, Some(Ok(1)));
    drop(stream);
    assert!(events.0.lock().unwrap().contains(&Event::Start(
        "OtherContextChild.request".into(),
        Some("Owner.stream".into())
    )));
    assert_idle();
}

#[test]
fn source_panic_is_preserved_and_activation_is_restored() {
    let (context, events) = context();
    let mut stream =
        context.observe_generated_stream("Panic", source(&context, &events, vec![Step::Panic]));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| stream
            .next()
            .now_or_never()))
        .is_err()
    );
    assert_idle();
    drop(stream);
    assert_eq!(
        finishes(&events, "Panic.stream"),
        [Event::Finish(
            "Panic.stream".into(),
            Some("stream_query_error".into()),
            0
        )]
    );
}

#[test]
fn noop_returns_original_box_without_wrapping_or_allocation() {
    let context = UserContext::new();
    let source: EntityStream<'_, usize, &'static str> =
        Box::pin(futures_util::stream::iter([Ok(1)]));
    let address = std::ptr::from_ref(source.as_ref().get_ref()).cast::<()>();
    let observed = context.observe_generated_stream("Noop", source);
    assert_eq!(
        address,
        std::ptr::from_ref(observed.as_ref().get_ref()).cast::<()>()
    );
}
