use opentelemetry::{
    KeyValue,
    metrics::{Meter, ObservableCounter},
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Copy, Debug)]
pub(super) enum Signal {
    Traces = 0,
    Logs = 1,
    Metrics = 2,
}
impl Signal {
    pub(super) const ALL: [Self; 3] = [Self::Traces, Self::Logs, Self::Metrics];
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Traces => "traces",
            Self::Logs => "logs",
            Self::Metrics => "metrics",
        }
    }
    fn unit(self) -> &'static str {
        match self {
            Self::Traces => "spans",
            Self::Logs => "log_records",
            Self::Metrics => "metric_data_points",
        }
    }
}
#[derive(Clone, Copy)]
pub(super) enum ResponseKind {
    PartialSuccess = 0,
    Warning = 1,
    Invalid = 2,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum LossReason {
    ExportFailure = 0,
    QueueOverflow = 1,
    Shutdown = 2,
    HandoffFailure = 3,
}
impl LossReason {
    const ALL: [Self; 4] = [
        Self::ExportFailure,
        Self::QueueOverflow,
        Self::Shutdown,
        Self::HandoffFailure,
    ];
    fn name(self) -> &'static str {
        match self {
            Self::ExportFailure => "export_failure",
            Self::QueueOverflow => "queue_overflow",
            Self::Shutdown => "shutdown",
            Self::HandoffFailure => "handoff_failure",
        }
    }
}
impl ResponseKind {
    const ALL: [Self; 3] = [Self::PartialSuccess, Self::Warning, Self::Invalid];
    fn name(self) -> &'static str {
        match self {
            Self::PartialSuccess => "partial_success",
            Self::Warning => "warning",
            Self::Invalid => "invalid_response",
        }
    }
}

#[derive(Default)]
pub(super) struct ExportHealth {
    lost: [[AtomicU64; 4]; 3],
    rejected: [AtomicU64; 3],
    responses: [[AtomicU64; 3]; 3],
}
impl ExportHealth {
    pub(super) fn lost(&self, signal: Signal, entries: usize) {
        self.dropped(signal, entries, LossReason::ExportFailure);
    }
    pub(super) fn dropped(&self, signal: Signal, entries: usize, reason: LossReason) {
        if entries == 0 {
            return;
        }
        if self.lost[signal as usize][reason as usize].fetch_add(entries as u64, Ordering::Relaxed)
            == 0
        {
            // Exact cumulative counters, but one fixed notice per signal/reason:
            // queue pressure must not generate another unbounded log stream.
            crate::telemetry_startup::notice(match reason {
                LossReason::ExportFailure => {
                    "TeaQL telemetry: export incomplete; records lost; business outcome unchanged."
                }
                LossReason::QueueOverflow => {
                    "TeaQL telemetry: queue overflow; records lost; business outcome unchanged."
                }
                LossReason::Shutdown => {
                    "TeaQL telemetry: post-shutdown handoff rejected; records lost; business outcome unchanged."
                }
                LossReason::HandoffFailure => {
                    "TeaQL telemetry: SDK handoff failed; records lost; business outcome unchanged."
                }
            });
        }
    }
    pub(super) fn response(&self, signal: Signal, rejected: u64, kind: ResponseKind) {
        self.rejected[signal as usize].fetch_add(rejected, Ordering::Relaxed);
        self.responses[signal as usize][kind as usize].fetch_add(1, Ordering::Relaxed);
        crate::telemetry_startup::notice(match kind {
            ResponseKind::PartialSuccess => {
                "TeaQL telemetry: receiver partially accepted export; rejected entries counted; remote detail masked."
            }
            ResponseKind::Warning => {
                "TeaQL telemetry: receiver export warning; remote detail masked."
            }
            ResponseKind::Invalid => {
                "TeaQL telemetry: invalid export acknowledgement; remote detail masked."
            }
        });
    }
    pub(super) fn instruments(self: &Arc<Self>, meter: &Meter) -> Vec<ObservableCounter<u64>> {
        let lost = self.clone();
        let rejected = self.clone();
        let responses = self.clone();
        vec![
            meter.u64_observable_counter("teaql.telemetry.export.lost")
                .with_description("Lost span/log entries by export, overflow, shutdown or handoff reason; metric export failures count MetricData objects, not sampling points")
                .with_callback(move |observer| for signal in Signal::ALL {
                    for reason in LossReason::ALL {
                        // Metrics are cumulative snapshots, not a record queue.
                        if matches!(signal, Signal::Metrics) && !matches!(reason, LossReason::ExportFailure) { continue; }
                        observer.observe(lost.lost[signal as usize][reason as usize].load(Ordering::Relaxed), &[
                            KeyValue::new("teaql.telemetry.signal", signal.name()),
                            KeyValue::new("teaql.telemetry.reason", reason.name()),
                        ]);
                    }
                }).build(),
            meter.u64_observable_counter("teaql.telemetry.export.rejected")
                .with_description("Receiver rejected spans, log records, or metric data points; units identified by signal")
                .with_callback(move |observer| for signal in Signal::ALL {
                    observer.observe(rejected.rejected[signal as usize].load(Ordering::Relaxed), &[
                        KeyValue::new("teaql.telemetry.signal", signal.name()),
                        KeyValue::new("teaql.telemetry.unit", signal.unit()),
                    ]);
                }).build(),
            meter.u64_observable_counter("teaql.telemetry.export.response")
                .with_description("Partial success, warning, or invalid acknowledgement; no raw remote text")
                .with_callback(move |observer| for signal in Signal::ALL {
                    for kind in ResponseKind::ALL {
                        observer.observe(responses.responses[signal as usize][kind as usize].load(Ordering::Relaxed), &[
                            KeyValue::new("teaql.telemetry.signal", signal.name()),
                            KeyValue::new("teaql.telemetry.reason", kind.name()),
                        ]);
                    }
                }).build(),
        ]
    }
    #[cfg(test)]
    pub(super) fn counts(&self, signal: Signal) -> (u64, u64, [u64; 3]) {
        (
            self.lost[signal as usize]
                .iter()
                .map(|v| v.load(Ordering::Relaxed))
                .sum(),
            self.rejected[signal as usize].load(Ordering::Relaxed),
            std::array::from_fn(|index| {
                self.responses[signal as usize][index].load(Ordering::Relaxed)
            }),
        )
    }
    #[cfg(test)]
    pub(super) fn loss(&self, signal: Signal, reason: LossReason) -> u64 {
        self.lost[signal as usize][reason as usize].load(Ordering::Relaxed)
    }
}
