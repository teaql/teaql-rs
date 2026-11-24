use checker_fix_advanced_core::{Q, service_runtime_from_pool};
use std::sync::{Arc, Mutex};
use teaql_runtime::{InternalIdGenerator, LedgerEntity, RuntimeError};

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

async fn context(values: Vec<Result<u64, RuntimeError>>) -> (checker_fix_advanced_core::ServiceRuntime, Arc<Mutex<Vec<String>>>) {
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap()));
    let mut context = service_runtime_from_pool(pool).await.unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    context.set_internal_id_generator(Allocator { values: Mutex::new(values), calls: calls.clone() });
    (context, calls)
}

#[tokio::test]
async fn positive_identity_is_available_before_any_save() {
    let (context, calls) = context(vec![Ok(701)]).await;
    let entity = Q::purchase_orders().comment("create an order").purpose("reserve identity before returning")
        .new_entity(&context).unwrap();
    assert_eq!(entity.id(), 701);
    assert_eq!(*calls.lock().unwrap(), vec!["purchase_order"]);
}

#[tokio::test]
async fn allocator_error_is_returned_not_swallowed() {
    let (context, calls) = context(vec![Err(RuntimeError::IdGeneration("controlled failure".into()))]).await;
    let error = Q::purchase_orders().comment("exercise allocation failure").purpose("do not return an invalid entity")
        .new_entity(&context).unwrap_err();
    assert!(matches!(error, RuntimeError::IdGeneration(ref m) if m == "controlled failure"));
    assert_eq!(calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn reserved_zero_id_is_an_error() {
    let (context, calls) = context(vec![Ok(0)]).await;
    let error = Q::purchase_orders().comment("exercise a zero allocation").purpose("reject invalid identity")
        .new_entity(&context).unwrap_err();
    assert!(matches!(error, RuntimeError::IdGeneration(ref m) if m.contains("zero")));
    assert_eq!(calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn failed_allocation_does_not_poison_the_next_creation() {
    let (context, calls) = context(vec![Err(RuntimeError::IdGeneration("controlled failure".into())), Ok(702)]).await;
    let request = Q::purchase_orders().comment("recover after failed allocation").purpose("preserve the selected allocator");
    assert!(request.new_entity(&context).is_err());
    assert_eq!(request.new_entity(&context).unwrap().id(), 702);
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn independent_creations_in_one_context_keep_independent_ledgers() {
    let (context, calls) = context(vec![Ok(701), Ok(702)]).await;
    let request = Q::purchase_orders().comment("create two independent orders").purpose("isolate mutation ownership");
    let mut left = request.new_entity(&context).unwrap();
    let right = request.new_entity(&context).unwrap();
    left.update_contact_phone("left-only");
    let left_state = left.entity_runtime_state().unwrap();
    let right_state = right.entity_runtime_state().unwrap();
    assert_ne!(left_state, right_state);
    assert_eq!(right.id(), 702);
    let key = teaql_runtime::EntityKey::new("PurchaseOrder", 701_u64);
    assert_eq!(left_state.get(&key, "contact_phone"), Some(teaql_core::Value::from("left-only")));
    assert!(right_state.get(&key, "contact_phone").is_none());
    assert_eq!(calls.lock().unwrap().len(), 2);
}
