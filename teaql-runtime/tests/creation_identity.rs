use std::sync::{Arc, Mutex};
use teaql_runtime::{AtomicCounterIdGenerator, InternalIdGenerator, RuntimeError, UserContext};

struct Allocator {
    values: Mutex<Vec<Result<u64, RuntimeError>>>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl InternalIdGenerator for Allocator {
    fn generate_id(&self, entity: &str) -> Result<u64, RuntimeError> {
        self.calls.lock().unwrap().push(entity.to_owned());
        self.values.lock().unwrap().remove(0)
    }
}

#[test]
fn context_returns_positive_ids_and_propagates_failure_without_fallback() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let context = UserContext::new().with_internal_id_generator(Allocator {
        values: Mutex::new(vec![
            Ok(701),
            Err(RuntimeError::IdGeneration("controlled failure".into())),
            Ok(0),
            Ok(704),
        ]),
        calls: calls.clone(),
    });
    assert_eq!(context.next_id("purchase_order").unwrap(), 701);
    match context.next_id("purchase_order") {
        Err(RuntimeError::IdGeneration(message)) => assert_eq!(message, "controlled failure"),
        other => panic!("allocator error must be preserved: {other:?}"),
    }
    match context.next_id("purchase_order") {
        Err(RuntimeError::IdGeneration(message)) => {
            assert!(message.contains("zero"));
            assert!(message.contains("purchase_order"));
        }
        other => panic!("zero is never an allocated ID: {other:?}"),
    }
    assert_eq!(context.next_id("purchase_order").unwrap(), 704);
    assert_eq!(*calls.lock().unwrap(), vec!["purchase_order"; 4]);
}

#[test]
fn default_context_assigns_nonzero_distinct_ids() {
    let context = UserContext::new();
    let first = context.next_id("purchase_order").unwrap();
    let second = context.next_id("purchase_order").unwrap();
    assert!(first > 0 && second > first);
}

#[test]
fn exhausted_memory_allocator_never_wraps_or_panics() {
    let allocator = AtomicCounterIdGenerator::new(u64::MAX - 1);
    assert_eq!(allocator.generate_id("purchase_order").unwrap(), u64::MAX);
    for _ in 0..2 {
        assert!(matches!(
            allocator.generate_id("purchase_order"),
            Err(RuntimeError::IdGeneration(_))
        ));
    }
}
