//! Published-runtime regression guard; no generated implementation is inspected.
use school_management_service_core::School;
use std::sync::Arc;
use teaql_core::Entity;
use teaql_runtime::EntityRuntimeState;

pub fn verify(entity: &School) {
    let (control, calls, bytes) = crate::allocation_counter::measured(|| {
        std::hint::black_box(Box::new(std::hint::black_box(42_u64)))
    });
    assert_eq!((calls, bytes), (1, std::mem::size_of::<u64>() as u64));
    drop(control);
    let state = entity
        .__teaql_runtime_state_any()
        .unwrap()
        .downcast_ref::<EntityRuntimeState>()
        .unwrap();
    let snapshot = entity.loaded_state_snapshot().unwrap();
    let baseline = state.original_snapshot().unwrap();
    // First clone promotes the immutable original row. Only subsequent clones
    // are required to be allocation-free, and no output container is measured.
    let held = state.clone();
    for clones in [1, 100, 10_000] {
        let (_, calls, bytes) = crate::allocation_counter::measured(|| {
            for _ in 0..clones {
                std::hint::black_box(state.clone());
            }
        });
        assert_eq!(
            (calls, bytes),
            (0, 0),
            "repeat state clones must not copy the original row"
        );
        assert_eq!(state.original_snapshot().unwrap(), baseline);
        assert_eq!(held.original_snapshot().unwrap(), baseline);
        assert!(held.current_change_set().is_empty());
    }
    assert!(Arc::ptr_eq(
        &snapshot,
        &entity.loaded_state_snapshot().unwrap()
    ));
    assert!(entity.dirty_fields().is_none());
    println!("PASS generated Rust original baseline repeat clones allocate zero and preserve loaded state");
}
