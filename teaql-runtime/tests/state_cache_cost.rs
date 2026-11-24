//! Isolated metadata hits, not SQL/query latency or retained heap.
#[path = "support/allocation_counter.rs"]
mod allocation_counter;
use allocation_counter::measured;
use std::{hint::black_box, sync::Arc};
use teaql_core::{FieldLayout, LoadedSnapshot};

#[test]
fn shared_state_cache_hit_cost_with_retained_shapes() {
    for hints in [1, 64, 512] {
        let layout = FieldLayout::from_generated(
            "StateCacheCost",
            "v1",
            &[("id", 0), ("version", 1), ("name", 2)],
            &[
                ("id", "id", "id"),
                ("version", "version", "version"),
                ("name", "name", "name"),
            ],
            &[],
            &["id", "version", "name"],
        )
        .unwrap();
        let target = LoadedSnapshot::projection(layout.clone(), ["id", "name"]).into_shared();
        let mut held = Vec::with_capacity(hints - 1);
        for index in 1..hints {
            let field = format!("#retained_{index}");
            held.push(
                LoadedSnapshot::projection(layout.clone(), ["id", field.as_str()]).into_shared(),
            );
        }
        for _ in 0..100 {
            black_box((*target).clone().into_shared());
        }
        for sample in 0..7 {
            let (last, calls, bytes, elapsed) = measured(|| {
                let mut last = target.clone();
                for _ in 0..10_000 {
                    last = black_box((*target).clone()).into_shared();
                }
                last
            });
            assert!(Arc::ptr_eq(&target, &last));
            assert!(last.is_loaded("id") && last.is_loaded("name"));
            assert!(!last.is_loaded("version"));
            assert_eq!((calls, bytes), (0, 0));
            println!("STATE_CACHE_HIT,{hints},10000,{sample},{calls},{bytes},{elapsed}");
        }
        assert_eq!(held.len(), hints - 1);
    }
    println!("PASS shared-state cache hits preserve geometry with zero allocation");
}
