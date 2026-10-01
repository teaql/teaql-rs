//! Transaction-local pending business facts, not an operator-log buffer.
//! They are delivered only after commit and projected before every public sink.
//! No queue is stored on UserContext, and nested transactions own separate queues.
use std::future::Future;
use std::sync::Mutex;

use crate::{RawAuditEvent, RuntimeError, UserContext};

struct PendingAuditBatch {
    context_address: usize,
    events: Mutex<Vec<RawAuditEvent>>,
}

tokio::task_local! {
    static PENDING_AUDITS: PendingAuditBatch;
}

pub(crate) fn try_enqueue(
    context: &UserContext,
    event: RawAuditEvent,
) -> Result<(), RawAuditEvent> {
    let mut event = Some(event);
    let buffered = PENDING_AUDITS
        .try_with(|batch| {
            if batch.context_address != std::ptr::from_ref(context) as usize {
                return false;
            }
            batch
                .events
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(event.take().expect("one pending audit fact"));
            true
        })
        .unwrap_or(false);
    if buffered {
        Ok(())
    } else {
        Err(event.expect("unbuffered audit fact"))
    }
}

pub(crate) async fn collect<F: Future>(
    context: &UserContext,
    work: F,
) -> (F::Output, Vec<RawAuditEvent>) {
    PENDING_AUDITS
        .scope(
            PendingAuditBatch {
                context_address: std::ptr::from_ref(context) as usize,
                events: Mutex::new(Vec::new()),
            },
            async {
                let result = work.await;
                let events = PENDING_AUDITS.with(|batch| {
                    std::mem::take(
                        &mut *batch
                            .events
                            .lock()
                            .unwrap_or_else(|error| error.into_inner()),
                    )
                });
                (result, events)
            },
        )
        .await
}

pub(crate) fn deliver(
    context: &UserContext,
    events: Vec<RawAuditEvent>,
) -> Result<(), RuntimeError> {
    let mut first_error = None;
    for event in events {
        if let Err(error) = context.deliver_audit_event(event) {
            // A sink failure must not hide the other committed entity facts.
            first_error.get_or_insert(error);
        }
    }
    match first_error {
        Some(source) => Err(RuntimeError::AuditAfterCommit {
            source: Box::new(source),
        }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scopes_are_isolated_for_concurrent_tasks_reusing_one_context() {
        let context = UserContext::default();
        let run = |entity: &'static str| {
            let context = &context;
            async move {
                collect(context, async {
                    context
                        .send_event(RawAuditEvent::created(entity, Default::default()))
                        .unwrap();
                    tokio::task::yield_now().await;
                })
                .await
                .1
            }
        };
        let (left, right) = tokio::join!(run("Order"), run("Payment"));
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 1);
        assert_eq!(left[0].entity, "Order");
        assert_eq!(right[0].entity, "Payment");
    }

    #[tokio::test]
    async fn nested_transaction_scope_does_not_leak_to_outer_queue() {
        let context = UserContext::default();
        let (inner, outer) = collect(&context, async {
            context
                .send_event(RawAuditEvent::created("Order", Default::default()))
                .unwrap();
            let (_, events) = collect(&context, async {
                context
                    .send_event(RawAuditEvent::created("Payment", Default::default()))
                    .unwrap();
            })
            .await;
            events
        })
        .await;
        assert_eq!(outer.len(), 1);
        assert_eq!(inner.len(), 1);
        assert_eq!(outer[0].entity, "Order");
        assert_eq!(inner[0].entity, "Payment");
    }
}
