//! Observe the real runtime typed-checker reconstruction adapter without writing data.
use school_management_service_core::School;
use std::sync::{Arc, Mutex};
use teaql_core::{Entity, LoadedSnapshot, Value};
use teaql_runtime::{
    CheckObjectStatus, CheckResults, Checker, ObjectLocation, TypedChecker, TypedEntityChecker,
    UserContext,
};

struct Observe {
    snapshots: Arc<Mutex<Vec<Arc<LoadedSnapshot>>>>,
}
impl TypedChecker<School> for Observe {
    fn check_and_fix_typed(
        &self,
        _context: &UserContext,
        entity: &mut School,
        _status: CheckObjectStatus,
        _location: &ObjectLocation,
        _results: &mut CheckResults,
    ) {
        assert!(
            entity.dirty_fields().is_none(),
            "readonly reconstruction must not create mutation intent"
        );
        self.snapshots
            .lock()
            .unwrap()
            .push(entity.loaded_state_snapshot().unwrap());
    }
}
pub fn verify(context: &UserContext, full: &School, sparse: &School) {
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let checker = TypedEntityChecker::<School, _>::new(Observe {
        snapshots: snapshots.clone(),
    });
    for (index, source) in [full, sparse].into_iter().enumerate() {
        let mut values: teaql_runtime::EntityValues = source.clone().into_values().into();
        let original = values.clone();
        let expected = source.loaded_state_snapshot().unwrap();
        let fields = School::__TEAQL_FIXED_FIELD_MAPPINGS
            .iter()
            .filter(|(_, member, _)| expected.is_loaded(member))
            .map(|(_, member, _)| Value::Text((*member).to_owned()))
            .collect();
        values.insert("_loaded_fields".into(), Value::List(fields));
        let mut results = CheckResults::new();
        checker.check_and_fix(context, &mut values, &ObjectLocation::root(), &mut results);
        assert_eq!(
            values, original,
            "checker defaults must not widen or change readonly input"
        );
        let captured = snapshots.lock().unwrap();
        assert_eq!(captured.len(), index + 1);
        assert!(
            Arc::ptr_eq(&expected, &captured[index]),
            "checker geometry must rejoin the original immutable snapshot"
        );
        assert_eq!(captured[index].is_loaded("address"), index == 0);
        if index == 0 {
            assert!(
                results.is_empty(),
                "complete native values should remain valid: {results:?}"
            );
        } else {
            assert!(
                !results.is_empty(),
                "missing required fields must not become silently loaded defaults"
            );
        }
    }
    println!(
        "PASS generated Rust typed Checker reconstruction shares snapshot without widening values"
    );
}
