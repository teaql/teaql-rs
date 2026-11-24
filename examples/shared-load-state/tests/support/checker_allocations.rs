//! Thread-local requested allocation counts; no clocks, database or input preparation.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAllocator;
thread_local! {
    static COUNTS: Cell<(bool, u64, u64)> = const { Cell::new((false, 0, 0)) };
}
fn count(size: usize) {
    let _ = COUNTS.try_with(|counter| {
        let (enabled, calls, bytes) = counter.get();
        if enabled {
            counter.set((true, calls + 1, bytes + size as u64));
        }
    });
}
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count(size);
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

pub(crate) fn measured<T>(action: impl FnOnce() -> T) -> (T, u64, u64) {
    COUNTS.with(|c| c.set((true, 0, 0)));
    let output = std::hint::black_box(action());
    let (_, calls, bytes) = COUNTS.with(|c| {
        let value = c.get();
        c.set((false, value.1, value.2));
        value
    });
    (output, calls, bytes)
}
