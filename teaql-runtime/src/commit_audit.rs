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

pub(crate) fn has_scope(context: &UserContext) -> bool {
    PENDING_AUDITS
        .try_with(|batch| batch.context_address == std::ptr::from_ref(context) as usize)
        .unwrap_or(false)
}

/// Add extension intent to the same native entity fact, never an independent
/// pre-commit event. Required facts must exist before commit can succeed.
pub(crate) fn enrich_dynamic(
    context: &UserContext,
    owner: &str,
    id: u64,
    changes: impl IntoIterator<Item = crate::EntityPropertyChange>,
) -> Result<(), RuntimeError> {
    let changes = changes.into_iter().collect::<Vec<_>>();
    let result = PENDING_AUDITS
        .try_with(|batch| {
            if batch.context_address != std::ptr::from_ref(context) as usize {
                return false;
            }
            let mut events = batch
                .events
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let Some(event) = events.iter_mut().rev().find(|event| {
                event.entity == owner
                    && event.values.get("id").and_then(teaql_core::Value::try_u64) == Some(id)
            }) else {
                return false;
            };
            for change in changes {
                if let Some(value) = &change.new_value {
                    event.values.insert(change.field.clone(), value.clone());
                    if let Some(values) = &mut event.new_values {
                        values.insert(change.field.clone(), value.clone());
                    }
                }
                event.updated_fields.push(change.field.clone());
                event.changes.push(change);
            }
            true
        })
        .unwrap_or(false);
    if !result {
        return Err(RuntimeError::DynamicField(
            teaql_core::dynamic_fields::DynamicFieldError {
                code: "DYNAMIC_FIELD_AUDIT_SCOPE_REQUIRED",
                field: owner.into(),
            },
        ));
    }
    Ok(())
}

/// Return an unbuffered fact to the caller for immediate delivery. Not having
/// this Context's transaction scope is not an audit error, and must not require
/// boxing every large fact on the ordinary unscoped delivery path.
pub(crate) fn try_enqueue(context: &UserContext, event: RawAuditEvent) -> Option<RawAuditEvent> {
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
        None
    } else {
        Some(event.expect("unbuffered audit fact"))
    }
}

pub(crate) fn collect<'a, F: Future + 'a>(
    context: &'a UserContext,
    work: F,
) -> impl Future<Output = (F::Output, Vec<RawAuditEvent>)> + 'a {
    let work = Box::pin(work);
    async move {
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

    #[test]
    fn audit_scope_does_not_inline_work_future_payload() {
        let context = UserContext::default();
        let padding = std::hint::black_box([0_u8; 64 * 1024]);
        let large = async move {
            std::future::pending::<()>().await;
            std::hint::black_box(padding);
        };
        let work_size = std::mem::size_of_val(&large);
        let small = collect(&context, async {});
        let large = collect(&context, large);
        assert_eq!(std::mem::size_of_val(&small), std::mem::size_of_val(&large));
        assert!(std::mem::size_of_val(&large) < work_size);
    }

    #[test]
    fn unscoped_enqueue_returns_the_original_fact_for_immediate_delivery() {
        let context = UserContext::default();
        let event = RawAuditEvent::created("Order", Default::default());
        assert_eq!(try_enqueue(&context, event.clone()), Some(event));
    }

    #[tokio::test]
    async fn other_context_enqueue_returns_the_fact_without_joining_this_queue() {
        let owner = UserContext::default();
        let other = UserContext::default();
        let event = RawAuditEvent::created("Payment", Default::default());
        let (returned, queued) = collect(&owner, async {
            assert!(
                try_enqueue(&owner, RawAuditEvent::created("Order", Default::default())).is_none()
            );
            try_enqueue(&other, event.clone())
        })
        .await;
        assert_eq!(returned, Some(event));
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].entity, "Order");
    }

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
