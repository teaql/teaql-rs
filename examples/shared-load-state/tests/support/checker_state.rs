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
    if std::env::var_os("TEAQL_CHECKER_ALLOCATION_PROBE").is_some() {
        allocation_probe(context, full, sparse);
    }
}

struct Noop {
    expect_dirty: bool,
}
impl TypedChecker<School> for Noop {
    fn check_and_fix_typed(
        &self,
        _context: &UserContext,
        entity: &mut School,
        _status: CheckObjectStatus,
        _location: &ObjectLocation,
        _results: &mut CheckResults,
    ) {
        if self.expect_dirty {
            assert!(entity.dirty_fields().unwrap().contains("name"));
        } else {
            assert!(entity.dirty_fields().is_none());
        }
    }
}

fn allocation_probe(context: &UserContext, full: &School, sparse: &School) {
    let location = ObjectLocation::root();
    for (shape, source) in [("full", full), ("sparse", sparse)] {
        for dirty in [false, true] {
            let checker = TypedEntityChecker::<School, _>::new(Noop {
                expect_dirty: dirty,
            });
            let mut template: teaql_runtime::EntityValues = source.clone().into_values().into();
            let expected = source.loaded_state_snapshot().unwrap();
            let fields = School::__TEAQL_FIXED_FIELD_MAPPINGS
                .iter()
                .filter(|(_, member, _)| expected.is_loaded(member))
                .map(|(_, member, _)| Value::Text((*member).to_owned()))
                .collect();
            template.insert("_loaded_fields".into(), Value::List(fields));
            if dirty {
                template.insert(
                    "_dirty_fields".into(),
                    Value::List(vec![Value::Text("name".into())]),
                );
            }
            // Warm layout and snapshot caches before measuring synchronous adapter work.
            checker.check_and_fix(
                context,
                &mut template.clone(),
                &location,
                &mut CheckResults::new(),
            );
            for rows in [1, 100, 1000] {
                // Input map copies and result-vector allocation are outside the counter.
                let mut inputs = vec![template.clone(); rows];
                let mut results: Vec<_> = (0..rows).map(|_| CheckResults::new()).collect();
                let (_, calls, bytes) = crate::allocation_counter::measured(|| {
                    for (values, result) in inputs.iter_mut().zip(&mut results) {
                        checker.check_and_fix(context, values, &location, result);
                    }
                });
                let original: teaql_runtime::EntityValues = source.clone().into_values().into();
                for (values, result) in inputs.iter().zip(&results) {
                    assert_eq!(values, &original);
                    assert_eq!(result.is_empty(), shape == "full");
                }
                println!("CHECKER_ALLOC shape={shape} dirty={dirty} rows={rows} calls={calls} requested_bytes={bytes}");
            }
        }
    }
}
