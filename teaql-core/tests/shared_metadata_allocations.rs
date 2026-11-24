use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use teaql_core::TeaqlEntity as _;
use teaql_macros::TeaqlEntity;

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

#[allow(dead_code)]
#[derive(TeaqlEntity)]
#[teaql(
    entity = "SharedMetadataAllocationProbe",
    table = "shared_metadata_probe"
)]
struct Probe {
    #[teaql(id)]
    id: u64,
    #[teaql(max_length = 100)]
    name: String,
}

#[test]
fn warm_descriptor_reads_and_clones_allocate_nothing() {
    let initial = Probe::entity_descriptor();
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
    for _ in 0..1000 {
        let descriptor = std::hint::black_box(Probe::entity_descriptor());
        let cloned = std::hint::black_box(descriptor.clone());
        std::hint::black_box(cloned.property_by_name("name"));
    }
    COUNTING.with(|counting| counting.set(false));
    assert_eq!(
        ALLOCATIONS.with(Cell::get),
        0,
        "metadata read or clone allocated"
    );
    assert_eq!(initial.properties.len(), 2);
}
