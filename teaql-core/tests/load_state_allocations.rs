//! Measures availability bookkeeping only, excluding row payloads, database I/O and fixture setup.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use teaql_core::dynamic_fields::{DynamicFieldDefinitions, DynamicFieldValues};
use teaql_core::{CompactRow, CompactRowLayout, DataType, FieldLayout, LoadedSnapshot, Value};

struct CountingAllocator;
thread_local! {
    static COUNTS: Cell<(bool, u64, u64)> = const { Cell::new((false, 0, 0)) };
}
fn count(size: usize) {
    let _ = COUNTS.try_with(|counter| {
        let (enabled, calls, bytes) = counter.get();
        if enabled {
            counter.set((enabled, calls + 1, bytes + size as u64));
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

#[test]
fn same_shape_compact_enhancement_does_not_copy_column_names_or_allocate() {
    for width in [3, 64, 141] {
        let (_, source) = fixture(width);
        for count in [1, 100, 10_000] {
            let rows: Vec<_> = (0..count)
                .map(|_| (source.clone(), source.clone()))
                .collect();
            let mut prepared = rows.into_iter();
            let (calls, bytes, _) = measure(count, || {
                let (mut left, right) = prepared.next().unwrap();
                let layout = left.shared_layout();
                left.extend(right);
                assert!(Arc::ptr_eq(&layout, &left.shared_layout()));
                black_box(left);
            });
            println!(
                "COMPACT_SAME_SHAPE_MERGE,width={width},rows={count},calls={calls},bytes={bytes}"
            );
            assert_eq!((calls, bytes), (0, 0));
        }
    }
}

fn measure(iterations: usize, mut action: impl FnMut()) -> (u64, u64, u128) {
    COUNTS.with(|counter| counter.set((false, 0, 0)));
    let start = std::time::Instant::now();
    COUNTS.with(|counter| counter.set((true, 0, 0)));
    for _ in 0..iterations {
        action();
    }
    let counts = COUNTS.with(|counter| {
        let counts = counter.get();
        counter.set((false, counts.1, counts.2));
        counts
    });
    (counts.1, counts.2, start.elapsed().as_nanos())
}

fn fixture(width: usize) -> (Arc<FieldLayout>, CompactRow) {
    let names: Vec<_> = (0..width)
        .map(|index| match index {
            0 => "id".to_owned(),
            1 => "version".to_owned(),
            _ => format!("field_{index}"),
        })
        .collect();
    let indexes: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(i, name)| (name.as_str(), i))
        .collect();
    let mappings: Vec<_> = names
        .iter()
        .map(|name| (name.as_str(), name.as_str(), name.as_str()))
        .collect();
    let members: Vec<_> = names.iter().map(String::as_str).collect();
    let layout =
        FieldLayout::from_generated("Probe", "v1", &indexes, &mappings, &[], &members).unwrap();
    let columns = CompactRowLayout::new(names.into());
    (
        layout,
        CompactRow::with_layout(columns, vec![Value::Null; width]),
    )
}

// Retained reconstruction of the old no-op branch, never a production path.
fn reference_dynamic_noop(
    state: &Arc<LoadedSnapshot>,
    values: &DynamicFieldValues,
) -> Arc<LoadedSnapshot> {
    let mut names: std::collections::HashSet<String> = state
        .selected_names()
        .iter()
        .flat_map(|names| names.iter())
        .filter(|name| !name.starts_with('#'))
        .cloned()
        .collect();
    names.extend(
        values
            .selected_codes()
            .iter()
            .map(|code| format!("#{code}")),
    );
    assert_eq!(
        state.selected_names(),
        Some(&names),
        "reference accepts only unchanged geometry"
    );
    state.clone()
}

#[test]
fn flat_edge_shape_allocations_do_not_grow_with_row_count() {
    println!("case,width,iterations,allocation_calls,requested_bytes,elapsed_ns");
    let mut baseline = None;
    for count in [1, 100, 10_000] {
        let (_, row) = fixture(4);
        let mut rows = vec![row; count];
        let mut cache = teaql_core::RelationShapeCache::default();
        let (calls, bytes, elapsed) = measure(1, || {
            for row in &mut rows {
                row.mark_relation_loaded("child_list", &mut cache);
            }
        });
        assert!(calls <= 10, "shape metadata allocated per row: {calls}");
        if let Some(expected) = baseline {
            assert_eq!((calls, bytes), expected);
        }
        baseline = Some((calls, bytes));
        let first = rows[0].shared_layout();
        assert!(
            rows.iter()
                .all(|row| Arc::ptr_eq(&first, &row.shared_layout()))
        );
        assert!(
            rows.iter()
                .all(|row| row.len() == 4 && row.is_loaded_relation("child_list"))
        );
        println!("flat_edge_shape,4,{count},{calls},{bytes},{elapsed}");
    }
}

#[test]
fn shared_availability_allocation_probe() {
    println!("case,width,iterations,allocation_calls,requested_bytes,elapsed_ns");
    for width in [4, 64, 130] {
        let (layout, row) = fixture(width);
        let base = match row.indexed_load_state(layout.clone()) {
            teaql_core::eval::LoadState::Indexed(state) => state,
            _ => panic!("indexed fixture"),
        };
        let definitions = DynamicFieldDefinitions::new(
            "Probe",
            "extension-v1",
            [("note".into(), DataType::Text)],
        )
        .unwrap();
        let values = DynamicFieldValues::from_values(
            definitions,
            HashMap::from([("note".into(), Value::Null)]),
        )
        .unwrap();
        let selected = LoadedSnapshot::with_dynamic_fields(&base, &values).unwrap();
        for iterations in [1, 100, 10_000] {
            let (calls, bytes, elapsed) = measure(iterations, || {
                black_box(row.indexed_load_state(black_box(layout.clone())));
            });
            println!("cached_projection,{width},{iterations},{calls},{bytes},{elapsed}");
            assert_eq!(calls, 0, "cached shape must not allocate per row");
            let (calls, bytes, elapsed) = measure(iterations, || {
                black_box(LoadedSnapshot::with_loaded(black_box(&base), "id", true).unwrap());
            });
            println!("loaded_fixed_noop,{width},{iterations},{calls},{bytes},{elapsed}");
            assert_eq!(calls, 0, "value-only updates must not allocate state");
            let (calls, bytes, elapsed) = measure(iterations, || {
                black_box(
                    LoadedSnapshot::with_dynamic_fields(black_box(&selected), black_box(&values))
                        .unwrap(),
                );
            });
            println!("loaded_dynamic_noop,{width},{iterations},{calls},{bytes},{elapsed}");
            assert_eq!(
                calls, 0,
                "unchanged dynamic availability must not allocate temporary sets"
            );
            let (calls, bytes, elapsed) = measure(iterations, || {
                black_box(reference_dynamic_noop(
                    black_box(&selected),
                    black_box(&values),
                ));
            });
            println!("reference_dynamic_noop,{width},{iterations},{calls},{bytes},{elapsed}");
        }
    }
}
