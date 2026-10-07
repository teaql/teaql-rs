//! Generated wide types; all field discovery uses public generated metadata.
#[path = "../../../teaql-runtime/tests/support/allocation_counter.rs"]
mod allocation_counter;
use allocation_counter::measured;
use school_management_service_core::{School, E};
use std::sync::Arc;
use teaql_core::{CompactRow, CompactRowLayout, DataType, Entity, TeaqlEntity, Value};

#[test]
fn generated_wide_hydration_allocations() {
    let descriptor = School::entity_descriptor();
    let layout = School::field_layout().unwrap().unwrap();
    assert!(
        layout.field_count() > 130,
        "requires --generate --wide fixture"
    );
    println!("case,selected_fields,rows,allocation_calls,requested_bytes,elapsed_ns");
    for width in [3, 64, layout.field_count()] {
        let mut indexes: Vec<_> = School::__TEAQL_FIXED_FIELD_INDEXES.iter().collect();
        indexes.sort_by_key(|(_, index)| *index);
        let mut names = vec!["id", "version", "name"];
        for (canonical, _) in indexes {
            if names.len() >= width {
                break;
            }
            let member = School::__TEAQL_FIXED_FIELD_MAPPINGS
                .iter()
                .find(|(name, _, _)| name == canonical)
                .unwrap()
                .1;
            if !names.contains(&member) {
                names.push(member);
            }
        }
        assert_eq!(names.len(), width);
        let columns = CompactRowLayout::new(
            names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>()
                .into(),
        );
        let cells: Vec<_> = names
            .iter()
            .map(|name| {
                let property = descriptor
                    .properties
                    .iter()
                    .find(|property| property.name == *name)
                    .unwrap();
                if property.name.starts_with("probe_") {
                    return Value::Null;
                }
                match property.data_type {
                    DataType::Text | DataType::LargeText => "sample name".into(),
                    DataType::U64 => Value::U64(1),
                    DataType::I64 => Value::I64(if property.name == "version" { 1 } else { 0 }),
                    DataType::Bool => Value::Bool(false),
                    DataType::Date => Value::Date("2000-01-01".parse().unwrap()),
                    DataType::Timestamp => {
                        Value::Timestamp(teaql_core::time::Timestamp(1_700_000_000_000))
                    }
                    DataType::Decimal => Value::Decimal("0".parse().unwrap()),
                    DataType::F64 => Value::F64(0.0),
                    DataType::Json => panic!("the controlled fixture has no JSON field"),
                }
            })
            .collect();
        let root = teaql_runtime::EntityRuntimeState::default();
        let warmed = School::from_compact_row_with_context(
            CompactRow::with_layout(columns.clone(), cells.clone()),
            &root,
        )
        .unwrap();
        let state = warmed.loaded_state_snapshot().unwrap();
        let overflow = state.overflow().cloned();
        for count in [1, 100, 10_000] {
            let inputs: Vec<_> = (1..=count)
                .map(|id| {
                    let mut values = cells.clone();
                    values[0] = Value::U64(id as u64);
                    CompactRow::with_layout(columns.clone(), values)
                })
                .collect();
            let (entities, calls, bytes, elapsed) = measured(|| {
                inputs
                    .into_iter()
                    .map(|row| School::from_compact_row_with_context(row, &root).unwrap())
                    .collect::<Vec<_>>()
            });
            println!("generated_wide_hydration,{width},{count},{calls},{bytes},{elapsed}");
            for entity in &entities {
                assert!(Arc::ptr_eq(
                    &state,
                    &entity.loaded_state_snapshot().unwrap()
                ));
                assert_eq!(
                    entity.loaded_state_snapshot().unwrap().overflow(),
                    overflow.as_ref()
                );
                assert_eq!(
                    E::school(entity).get_name().eval().as_deref(),
                    Some("sample name")
                );
            }
            // Two text payloads at most, plus the private lazy ledger cell and
            // output vector. Width must not introduce per-row overflow sets.
            assert!(
                calls <= count as u64 * 3 + 1,
                "width-dependent state allocation: {calls}"
            );
            if count > 1 {
                let probe = School::__TEAQL_FIXED_FIELD_INDEXES
                    .iter()
                    .find(|(_, index)| *index == 64)
                    .unwrap()
                    .0;
                let changed =
                    teaql_core::LoadedSnapshot::with_loaded(&state, probe, !state.is_loaded(probe))
                        .unwrap();
                assert!(!Arc::ptr_eq(&state, &changed));
                assert!(Arc::ptr_eq(
                    &state,
                    &entities[1].loaded_state_snapshot().unwrap()
                ));
            }
        }
    }
    println!("PASS generated Rust wide hydration shares one overflow snapshot per shape");
}
