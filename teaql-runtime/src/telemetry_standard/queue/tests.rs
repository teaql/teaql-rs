use super::*;
use opentelemetry::{
    logs::{AnyValue, LogRecord, Logger, LoggerProvider},
    trace::{Span as _, Tracer, TracerProvider},
};
use opentelemetry_sdk::{
    error::OTelSdkError,
    logs::{LogBatch, LogExporter, SdkLoggerProvider},
    trace::{SdkTracerProvider, SpanExporter},
};
use std::{
    sync::{
        Barrier, Condvar,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};

#[test]
fn concurrent_producers_share_one_exact_quota() {
    let health = Arc::new(ExportHealth::default());
    let quota = Quota::new(8, Signal::Traces, health.clone());
    let entered = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(32));
    let workers = (0..32)
        .map(|_| {
            let quota = quota.clone();
            let entered = entered.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                quota.admit(|| {
                    entered.fetch_add(1, Ordering::SeqCst);
                });
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(entered.load(Ordering::SeqCst), 8);
    assert_eq!(quota.outstanding(), 8);
    assert_eq!(health.loss(Signal::Traces, LossReason::QueueOverflow), 24);
}
#[test]
fn reservation_release_handles_failed_and_panicking_exports() {
    let health = Arc::new(ExportHealth::default());
    let quota = Quota::new(4, Signal::Logs, health);
    for _ in 0..4 {
        quota.admit(|| {});
    }
    let result: Result<(), ()> = {
        let _permit = quota.batch(4);
        Err(())
    };
    assert!(result.is_err());
    assert_eq!(quota.outstanding(), 0);
    for _ in 0..4 {
        quota.admit(|| {});
    }
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _permit = quota.batch(4);
            panic!("controlled exporter panic");
        }))
        .is_err()
    );
    assert_eq!(quota.outstanding(), 0);
    quota.admit(|| {});
    assert_eq!(quota.outstanding(), 1);
}
#[test]
fn unexpected_sdk_handoff_panic_releases_reservation_without_poisoning_quota() {
    let health = Arc::new(ExportHealth::default());
    let quota = Quota::new(1, Signal::Logs, health.clone());
    quota.admit(|| panic!("controlled handoff panic"));
    assert_eq!(quota.outstanding(), 0);
    assert_eq!(health.loss(Signal::Logs, LossReason::HandoffFailure), 1);
    quota.admit(|| {});
    assert_eq!(quota.outstanding(), 1);
}
#[test]
fn shutdown_serializes_with_sdk_handoff_and_rejects_later_records() {
    let health = Arc::new(ExportHealth::default());
    let quota = Quota::new(4, Signal::Traces, health.clone());
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let worker_quota = quota.clone();
    let producer = thread::spawn(move || {
        worker_quota.admit(|| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (closed_tx, closed_rx) = mpsc::channel();
    let close_quota = quota.clone();
    let closer = thread::spawn(move || {
        started_tx.send(()).unwrap();
        close_quota.close();
        closed_tx.send(()).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(closed_rx.recv_timeout(Duration::from_millis(20)).is_err());
    release_tx.send(()).unwrap();
    producer.join().unwrap();
    closer.join().unwrap();
    closed_rx.recv().unwrap();
    quota.admit(|| panic!("closed queue reached SDK"));
    assert_eq!(health.loss(Signal::Traces, LossReason::Shutdown), 1);
    assert_eq!(quota.outstanding(), 1);
    drop(quota.batch(1));
    assert_eq!(quota.outstanding(), 0);
}

#[derive(Debug)]
struct Blocker {
    entered: mpsc::Sender<()>,
    released: Mutex<bool>,
    release: Condvar,
    first: AtomicBool,
    exported: AtomicUsize,
    quota: Arc<Quota>,
    fail: bool,
}
impl Blocker {
    fn new(quota: Arc<Quota>, fail: bool) -> (Arc<Self>, mpsc::Receiver<()>) {
        let (entered, receiver) = mpsc::channel();
        (
            Arc::new(Self {
                entered,
                released: Mutex::new(false),
                release: Condvar::new(),
                first: AtomicBool::new(true),
                exported: AtomicUsize::new(0),
                quota,
                fail,
            }),
            receiver,
        )
    }
    fn exported(&self, entries: usize) -> OTelSdkResult {
        let _permit = self.quota.batch(entries);
        if self.first.swap(false, Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            let released = self.released.lock().unwrap();
            let (released, timeout) = self
                .release
                .wait_timeout_while(released, Duration::from_secs(4), |ready| !*ready)
                .unwrap();
            assert!(
                *released && !timeout.timed_out(),
                "fixture must release first export"
            );
        }
        self.exported.fetch_add(entries, Ordering::SeqCst);
        if self.fail {
            Err(OTelSdkError::InternalFailure(
                "controlled export failure".into(),
            ))
        } else {
            Ok(())
        }
    }
    fn unblock(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}
#[derive(Debug)]
struct FakeExporter(Arc<Blocker>);
impl SpanExporter for FakeExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.exported(batch.len())
    }
}
impl LogExporter for FakeExporter {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        self.0.exported(batch.iter().count())
    }
}

#[test]
fn real_sdk_span_queue_counts_exporting_entries_and_recovers() {
    let health = Arc::new(ExportHealth::default());
    let quota = Quota::new(8, Signal::Traces, health.clone());
    let (exporter, entered) = Blocker::new(quota.clone(), false);
    let batch = BatchSpanProcessor::builder(FakeExporter(exporter.clone()))
        .with_batch_config(
            opentelemetry_sdk::trace::BatchConfigBuilder::default()
                .with_max_queue_size(8)
                .with_max_export_batch_size(1)
                .with_scheduled_delay(Duration::from_secs(60))
                .build(),
        )
        .build();
    let provider = SdkTracerProvider::builder()
        .with_span_processor(SpanQueue::new(batch, quota.clone()))
        .build();
    let tracer = provider.tracer("quota-test");
    tracer.start("first").end();
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    for _ in 0..9 {
        tracer.start("queued").end();
    }
    assert_eq!(quota.outstanding(), 8);
    assert_eq!(health.loss(Signal::Traces, LossReason::QueueOverflow), 2);
    assert_eq!(exporter.exported.load(Ordering::SeqCst), 0);
    exporter.unblock();
    provider.force_flush().unwrap();
    assert_eq!(quota.outstanding(), 0);
    assert_eq!(exporter.exported.load(Ordering::SeqCst), 8);
    for _ in 0..4 {
        tracer.start("recovered").end();
    }
    provider.force_flush().unwrap();
    assert_eq!(exporter.exported.load(Ordering::SeqCst), 12);
    assert_eq!(quota.outstanding(), 0);
    provider.shutdown().unwrap();
}
#[test]
fn real_sdk_log_queue_releases_failed_export_capacity_and_recovers() {
    let health = Arc::new(ExportHealth::default());
    let quota = Quota::new(8, Signal::Logs, health.clone());
    let (exporter, entered) = Blocker::new(quota.clone(), true);
    let batch = BatchLogProcessor::builder(FakeExporter(exporter.clone()))
        .with_batch_config(
            opentelemetry_sdk::logs::BatchConfigBuilder::default()
                .with_max_queue_size(8)
                .with_max_export_batch_size(1)
                .with_scheduled_delay(Duration::from_secs(60))
                .build(),
        )
        .build();
    let provider = SdkLoggerProvider::builder()
        .with_log_processor(LogQueue::new(batch, quota.clone()))
        .build();
    let logger = provider.logger("quota-test");
    let emit = || {
        let mut record = logger.create_log_record();
        record.set_body(AnyValue::String("safe record".into()));
        logger.emit(record);
    };
    emit();
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    for _ in 0..9 {
        emit();
    }
    assert_eq!(quota.outstanding(), 8);
    assert_eq!(health.loss(Signal::Logs, LossReason::QueueOverflow), 2);
    exporter.unblock();
    let _ = provider.force_flush();
    assert_eq!(quota.outstanding(), 0);
    assert_eq!(exporter.exported.load(Ordering::SeqCst), 8);
    for _ in 0..4 {
        emit();
    }
    let _ = provider.force_flush();
    assert_eq!(exporter.exported.load(Ordering::SeqCst), 12);
    assert_eq!(quota.outstanding(), 0);
    provider.shutdown().unwrap();
}
