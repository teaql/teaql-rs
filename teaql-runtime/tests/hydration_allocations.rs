//! Full typed row hydration, excluding prepared input rows, database I/O and logging.
#[path = "support/allocation_counter.rs"]
mod allocation_counter;
use allocation_counter::measured;
use std::sync::Arc;
use teaql_core::{CompactRow, CompactRowLayout, Entity, TeaqlEntity as _, Value};
use teaql_macros::{TeaqlEntity, teaql_entity};

#[teaql_entity]
#[derive(Clone, Debug, TeaqlEntity)]
#[teaql(entity = "HydrationProbe", indexed_layout)]
struct Probe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    name: String,
    note: Option<String>,
    #[teaql(skip)]
    __load_state: teaql_core::eval::LoadState,
}
impl Probe {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "hydration-probe-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] =
        &[("id", 0), ("version", 1), ("name", 2), ("note", 3)];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("name", "name", "name"),
        ("note", "note", "note"),
    ];
}

#[derive(Debug)]
struct Plain {
    id: u64,
    version: i64,
    name: String,
    note: Option<String>,
}
fn plain(row: CompactRow) -> Plain {
    Plain {
        id: match row.get("id") {
            Some(Value::U64(value)) => *value,
            other => panic!("bad ID {other:?}"),
        },
        version: match row.get("version") {
            Some(Value::I64(value)) => *value,
            other => panic!("bad version {other:?}"),
        },
        name: match row.get("name") {
            Some(Value::Text(value)) => value.clone(),
            other => panic!("bad name {other:?}"),
        },
        note: match row.get("note") {
            Some(Value::Text(value)) => Some(value.clone()),
            None | Some(Value::Null) => None,
            other => panic!("bad note {other:?}"),
        },
    }
}
fn rows(layout: &Arc<CompactRowLayout>, count: usize, full: bool) -> Vec<CompactRow> {
    (1..=count)
        .map(|id| {
            let mut values = vec![Value::U64(id as u64), Value::I64(1), "sample name".into()];
            if full {
                values.push(Value::Null);
            }
            CompactRow::with_layout(layout.clone(), values)
        })
        .collect()
}

#[test]
fn typed_hydration_allocation_probe() {
    println!("case,projection,rows,allocation_calls,requested_bytes,elapsed_ns");
    for full in [false, true] {
        let names: Vec<String> = if full {
            vec!["id", "version", "name", "note"]
        } else {
            vec!["id", "version", "name"]
        }
        .into_iter()
        .map(str::to_owned)
        .collect();
        let layout = CompactRowLayout::new(names.into());
        // Keep a shape alive and warm runtime/type metadata before counting rows.
        let warmed = Probe::from_compact_row(rows(&layout, 1, full).pop().unwrap()).unwrap();
        let snapshot = warmed.loaded_state_snapshot().unwrap();
        let field_layout = Probe::field_layout().unwrap().unwrap();
        assert!(Arc::ptr_eq(snapshot.layout(), &field_layout));
        assert!(Arc::ptr_eq(
            &field_layout.shared_entity_name(),
            &field_layout.shared_entity_name()
        ));
        let projection = if full { "full" } else { "sparse" };
        let root = teaql_runtime::EntityRuntimeState::default();
        for size in [1, 100, 10_000] {
            let inputs = rows(&layout, size, full);
            let (entities, calls, bytes, elapsed) = measured(|| {
                inputs
                    .into_iter()
                    .map(|row| Probe::from_compact_row(row).unwrap())
                    .collect::<Vec<_>>()
            });
            println!("typed_hydration,{projection},{size},{calls},{bytes},{elapsed}");
            assert!(
                calls <= size as u64 * 3 + 1,
                "unexpected per-row state allocation"
            );
            let inputs = rows(&layout, size, full);
            let graph_root = teaql_runtime::EntityRuntimeState::default();
            let mut registry = teaql_runtime::InMemoryEntityGraphDecoderRegistry::default();
            registry.register::<Probe>();
            let (builder, graph_calls, graph_bytes, graph_elapsed) = measured(|| {
                let mut builder = teaql_runtime::EntityGraphBuilder::default();
                registry
                    .decode_compact_batch("HydrationProbe", inputs, &graph_root, &mut builder)
                    .unwrap();
                builder
            });
            println!(
                "identity_batch,{projection},{size},{graph_calls},{graph_bytes},{graph_elapsed}"
            );
            graph_root.freeze_graph(builder).unwrap();
            assert!(Arc::ptr_eq(
                &snapshot,
                &graph_root
                    .resolve_entity::<Probe>(1)
                    .unwrap()
                    .loaded_state_snapshot()
                    .unwrap()
            ));
            assert!(Arc::ptr_eq(
                &snapshot,
                &graph_root
                    .resolve_entity::<Probe>(size as u64)
                    .unwrap()
                    .loaded_state_snapshot()
                    .unwrap()
            ));
            // Boxes and the type/ID table are real graph payload costs; a
            // temporary Vec<Probe> or per-row state collections are not needed.
            assert!(
                graph_calls <= size as u64 * 3 + 32,
                "unexpected graph/state allocations: {graph_calls}"
            );
            for (index, entity) in entities.iter().enumerate() {
                assert_eq!(entity.id, index as u64 + 1);
                assert_eq!(entity.name, "sample name");
                assert_eq!(entity.version, 1);
                assert_eq!(entity.note, None);
                assert_eq!(entity.is_field_loaded("note"), full);
                assert!(Arc::ptr_eq(
                    &snapshot,
                    &entity.loaded_state_snapshot().unwrap()
                ));
                assert!(entity.dirty_fields().unwrap_or_default().is_empty());
            }
            let inputs = rows(&layout, size, full);
            let (mut entities, calls, bytes, elapsed) = measured(|| {
                inputs
                    .into_iter()
                    .map(|row| Probe::from_compact_row_with_context(row, &root).unwrap())
                    .collect::<Vec<_>>()
            });
            println!("context_hydration,{projection},{size},{calls},{bytes},{elapsed}");
            assert!(
                calls <= size as u64 * 2 + 1,
                "unexpected per-row state allocation"
            );
            for entity in &entities {
                assert!(Arc::ptr_eq(
                    &snapshot,
                    &entity.loaded_state_snapshot().unwrap()
                ));
                assert_eq!(entity.is_field_loaded("note"), full);
            }
            if entities.len() > 1 {
                entities[0].set_comment("first row only");
                assert_eq!(entities[0].get_comment().as_deref(), Some("first row only"));
                assert!(entities[1].get_comment().is_none());
            }
            let inputs = rows(&layout, size, full);
            let (entities, calls, bytes, elapsed) =
                measured(|| inputs.into_iter().map(plain).collect::<Vec<_>>());
            println!("plain_hydration,{projection},{size},{calls},{bytes},{elapsed}");
            for (index, entity) in entities.iter().enumerate() {
                assert_eq!(entity.id, index as u64 + 1);
                assert_eq!(entity.version, 1);
                assert_eq!(entity.name, "sample name");
                assert_eq!(entity.note, None);
            }
        }
    }
}
