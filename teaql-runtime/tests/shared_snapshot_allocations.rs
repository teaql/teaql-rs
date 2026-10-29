use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use teaql_core::{CompactRow, Value};
use teaql_runtime::EntityRuntimeState;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
struct CountAlloc;
fn record() {
    if COUNTING.try_with(Cell::get).unwrap_or(false) {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
    }
}
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record();
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountAlloc = CountAlloc;

#[test]
fn warmed_original_snapshot_reads_and_clones_allocate_nothing() {
    let mut root = EntityRuntimeState::default();
    root.set_original_compact_row(
        "Probe",
        CompactRow::from_map(BTreeMap::from([
            ("id".into(), Value::U64(1)),
            ("name".into(), Value::from("payload")),
        ])),
    );
    let initial = root.original_snapshot().unwrap();
    let retained = root.clone();
    // Warm the new carrier's platform mutex too (macOS initializes it lazily).
    let retained_initial = retained.original_snapshot().unwrap();
    assert!(std::ptr::eq(&*initial, &*retained_initial));
    ALLOCATIONS.with(|n| n.set(0));
    COUNTING.with(|n| n.set(true));
    for _ in 0..1000 {
        let snapshot = std::hint::black_box(retained.original_snapshot().unwrap());
        std::hint::black_box(snapshot.clone());
        std::hint::black_box(snapshot.get("name"));
    }
    COUNTING.with(|n| n.set(false));
    assert_eq!(ALLOCATIONS.with(Cell::get), 0);
    let mut changed = initial.clone();
    changed.insert("name".into(), Value::from("new"));
    assert_eq!(
        root.original_snapshot().unwrap().get("name"),
        Some(&Value::from("payload"))
    );
}
