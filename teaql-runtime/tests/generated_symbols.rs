//! Slot identity, sparse ownership, and real generated-ledger allocation regressions.
#[path = "support/allocation_counter.rs"]
mod allocation_counter;
use allocation_counter::measured;
use std::{collections::BTreeMap, hint::black_box};
use teaql_core::{
    Entity, EntityTypeId, FieldSlot, ModelSymbols, MutationValues, TeaqlEntity as _, Value,
};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_runtime::{EntityRuntimeState, LedgerCompositionError, LedgerEntity};

static MODEL: ModelSymbols = ModelSymbols {
    entities: &["Inventory", "Other"],
};
#[teaql_entity]
#[derive(Clone, Debug, Default, PartialEq, TeaqlEntity)]
#[teaql(entity = "Inventory", symbol_model = "MODEL", symbol_entity = 0)]
#[teaql(reverse_relation(name = "stock_records", target = "Other", many))]
struct Inventory {
    #[teaql(id)]
    id: u64,
    quantity: i64,
    nullable: Option<String>,
    #[teaql(version)]
    version: i64,
}
mod another_model {
    use super::*;
    #[teaql_entity]
    #[derive(Clone, Debug, Default, PartialEq, TeaqlEntity)]
    #[teaql(entity = "Inventory")]
    pub struct Inventory {
        #[teaql(id)]
        pub id: u64,
        pub quantity: i64,
    }
}

#[test]
fn identities_are_four_bytes_scoped_and_bounds_checked() {
    assert_eq!(std::mem::size_of::<FieldSlot>(), 4);
    assert_eq!(std::mem::size_of::<EntityTypeId>(), 4);
    assert_eq!(std::mem::size_of::<teaql_core::RelationSlot>(), 4);
    let symbols = Inventory::symbols().unwrap();
    assert!(std::ptr::eq(symbols.model, &MODEL));
    assert_eq!(symbols.entity, EntityTypeId(0));
    assert_eq!(symbols.name(), Some("Inventory"));
    assert_eq!(symbols.field(FieldSlot(0)), Some("id"));
    assert_eq!(symbols.field(FieldSlot(u32::MAX)), None);
    assert_eq!(symbols.slot("unknown"), None);
    assert_eq!(symbols.slot("#quantity"), None);
    assert!(symbols.validate().is_ok());
    assert_eq!(
        symbols.relation_slot("stock_records"),
        Some(Inventory::__TEAQL_RELATION_stock_records)
    );
    assert_eq!(
        symbols.relation(Inventory::__TEAQL_RELATION_stock_records),
        Some("stock_records")
    );
    assert_eq!(symbols.relation(teaql_core::RelationSlot(u32::MAX)), None);
    for (index, name) in symbols.fields.iter().enumerate() {
        assert_eq!(symbols.slot(name), Some(FieldSlot(index as u32)));
    }
    let mut values = MutationValues::indexed(symbols);
    values.insert_slot(symbols, Inventory::__TEAQL_SLOT_quantity, Value::I64(1));
    let other = another_model::Inventory::symbols().unwrap();
    assert_ne!(symbols, other);
    assert_eq!(
        values.get_slot(other, another_model::Inventory::__TEAQL_SLOT_quantity),
        None
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        values.insert_slot(
            other,
            another_model::Inventory::__TEAQL_SLOT_quantity,
            Value::I64(2),
        );
    }));
    assert!(result.is_err());
    assert_eq!(values.get("quantity"), Some(&Value::I64(1)));
}

#[test]
fn warmed_generated_ledger_overwrites_and_reads_allocate_nothing() {
    let entity = Inventory {
        id: 7,
        ..Default::default()
    };
    let state = entity.entity_runtime_state().unwrap();
    let symbols = Inventory::symbols().unwrap();
    state.set_slot(
        entity.entity_key(),
        symbols,
        Inventory::__TEAQL_SLOT_quantity,
        Value::I64(3),
    );
    let (_, calls, bytes, _) = measured(|| {
        for _ in 0..1000 {
            state.set_slot(
                entity.entity_key(),
                symbols,
                Inventory::__TEAQL_SLOT_quantity,
                Value::I64(3),
            );
            assert_eq!(
                black_box(state.get_slot(
                    &entity.entity_key(),
                    symbols,
                    Inventory::__TEAQL_SLOT_quantity
                )),
                Some(Value::I64(3))
            );
            black_box(symbols.field(Inventory::__TEAQL_SLOT_quantity));
        }
    });
    assert_eq!((calls, bytes), (0, 0));
    let changes = state.current_change_set();
    let values = changes.changes().get(&entity.entity_key()).unwrap();
    assert_eq!(values.len(), 1); // explicit same-value assignment is still intent
    assert!(std::ptr::eq(values.symbols().unwrap(), symbols));
}

#[test]
fn sparse_null_unknown_fields_and_legacy_view_keep_independent_values() {
    let symbols = Inventory::symbols().unwrap();
    let mut values = MutationValues::indexed(symbols);
    values.insert_slot(symbols, Inventory::__TEAQL_SLOT_nullable, Value::Null);
    assert_eq!(values.get("nullable"), Some(&Value::Null));
    assert_eq!(values.get("quantity"), None); // missing is not explicit NULL
    values.insert_named("#custom", Value::from("business text"));
    values.insert_named("_selection", Value::Bool(false));
    let old = values.clone();
    let legacy: &BTreeMap<String, Value> = &values;
    assert_eq!(legacy.len(), 3);
    values.insert_slot(
        symbols,
        Inventory::__TEAQL_SLOT_nullable,
        Value::from("next"),
    );
    assert_eq!(old.get("nullable"), Some(&Value::Null));
    assert_eq!(values.remove("#custom"), Some(Value::from("business text")));
    assert_eq!(old.get("#custom"), Some(&Value::from("business text")));
    let names = values.named_entries().map(|(n, _)| n).collect::<Vec<_>>();
    assert_eq!(names, vec!["_selection", "nullable"]);
    let mut owned: BTreeMap<String, Value> = values.into();
    owned.insert("nullable".into(), Value::Null);
    assert_eq!(old.get("nullable"), Some(&Value::Null));
}

#[test]
fn different_models_cannot_be_silently_combined() {
    let left = Inventory {
        id: 7,
        ..Default::default()
    };
    let mut right = another_model::Inventory::default();
    right.id = 7;
    left.entity_runtime_state().unwrap().set_slot(
        left.entity_key(),
        Inventory::symbols().unwrap(),
        Inventory::__TEAQL_SLOT_quantity,
        Value::I64(11),
    );
    right.entity_runtime_state().unwrap().set_slot(
        right.entity_key(),
        another_model::Inventory::symbols().unwrap(),
        another_model::Inventory::__TEAQL_SLOT_quantity,
        Value::I64(22),
    );
    let before = left.entity_runtime_state().unwrap().current_change_set();
    assert_eq!(
        left.include_pending_mutations_from(&right),
        Err(LedgerCompositionError::ConflictingSymbolLayout {
            entity: "Inventory".into()
        })
    );
    assert_eq!(
        left.entity_runtime_state().unwrap().current_change_set(),
        before
    );
}

#[test]
fn mismatched_key_or_invalid_slot_is_rejected_before_changing_the_ledger() {
    let key = teaql_runtime::EntityKey::for_type::<Inventory>(7_u64);
    let other = another_model::Inventory::symbols().unwrap();
    let mut changes = teaql_runtime::EntityChangeSet::default();
    for (symbols, slot) in [
        (other, another_model::Inventory::__TEAQL_SLOT_quantity),
        (Inventory::symbols().unwrap(), FieldSlot(u32::MAX)),
    ] {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            changes.set_slot(key.clone(), symbols, slot, Value::I64(1));
        }));
        assert!(result.is_err());
        assert!(changes.is_empty());
    }
}

#[test]
fn named_keys_keep_bound_layout_ownership_during_composition() {
    let left_entity = Inventory {
        id: 7,
        ..Default::default()
    };
    let mut right_entity = another_model::Inventory::default();
    right_entity.id = 7;
    let left = left_entity.entity_runtime_state().unwrap();
    let right = right_entity.entity_runtime_state().unwrap();
    let legacy_key = teaql_runtime::EntityKey::new("Inventory", 7_u64);
    left.set(legacy_key.clone(), "quantity", Value::I64(1));
    right.set(legacy_key, "quantity", Value::I64(2));
    left.set_slot(
        teaql_runtime::EntityKey::for_type::<Inventory>(7_u64),
        Inventory::symbols().unwrap(),
        Inventory::__TEAQL_SLOT_quantity,
        Value::I64(11),
    );
    right.set_slot(
        teaql_runtime::EntityKey::for_type::<another_model::Inventory>(7_u64),
        another_model::Inventory::symbols().unwrap(),
        another_model::Inventory::__TEAQL_SLOT_quantity,
        Value::I64(22),
    );
    let before = left.current_change_set();
    assert_eq!(
        left_entity.include_pending_mutations_from(&right_entity),
        Err(LedgerCompositionError::ConflictingSymbolLayout {
            entity: "Inventory".into()
        })
    );
    assert_eq!(left.current_change_set(), before);
}

#[test]
fn fix_frame_and_rollback_keep_caller_intent() {
    let entity = Inventory {
        id: 7,
        ..Default::default()
    };
    let state = entity.entity_runtime_state().unwrap();
    let symbols = Inventory::symbols().unwrap();
    state.set_slot(
        entity.entity_key(),
        symbols,
        Inventory::__TEAQL_SLOT_quantity,
        Value::I64(10),
    );
    assert!(entity.begin_checker_fix());
    state.set_slot(
        entity.entity_key(),
        symbols,
        Inventory::__TEAQL_SLOT_quantity,
        Value::I64(20),
    );
    let fixed = entity.take_checker_fix_changes();
    assert_eq!(fixed.get("quantity"), Some(&Value::I64(20)));
    assert_eq!(
        state.get_slot(
            &entity.entity_key(),
            symbols,
            Inventory::__TEAQL_SLOT_quantity
        ),
        Some(Value::I64(10))
    );
    let untouched = EntityRuntimeState::default();
    assert!(untouched.current_change_set().is_empty());
}

#[test]
fn registered_models_use_scoped_entity_ids_without_allocating() {
    let context = teaql_runtime::UserContext::new().with_metadata(
        teaql_runtime::InMemoryMetadataStore::new().with_entity(Inventory::entity_descriptor()),
    );
    let symbols = Inventory::symbols().unwrap();
    let (_, calls, bytes, _) = measured(|| {
        for _ in 0..1000 {
            black_box(context.require_entity_symbols(symbols).unwrap());
        }
    });
    assert_eq!((calls, bytes), (0, 0));
    assert!(
        context
            .require_entity_symbols(another_model::Inventory::symbols().unwrap())
            .is_err()
    );
    let other = teaql_runtime::UserContext::new().with_metadata(
        teaql_runtime::InMemoryMetadataStore::new()
            .with_entity(another_model::Inventory::entity_descriptor()),
    );
    assert!(
        other
            .require_entity_symbols(another_model::Inventory::symbols().unwrap())
            .is_ok()
    );
    assert!(other.require_entity_symbols(symbols).is_err());
}

#[test]
fn sparse_storage_and_clone_do_not_scale_with_dictionary_size() {
    static SYMBOLS: teaql_core::EntitySymbols = teaql_core::EntitySymbols {
        model: &MODEL,
        entity: EntityTypeId(0),
        fields: &[
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
            "r", "s", "t", "u", "v", "w", "x", "y", "z",
        ],
        relations: &[],
    };
    let (values, calls, bytes, _) = measured(|| {
        let mut values = MutationValues::indexed(&SYMBOLS);
        values.insert_slot(&SYMBOLS, FieldSlot(25), Value::Null);
        values
    });
    assert_eq!(calls, 2); // one sparse carrier, one small Vec; no dictionary/name copies
    assert!(bytes < (SYMBOLS.fields.len() * std::mem::size_of::<Value>()) as u64);
    let _: &BTreeMap<String, Value> = &values; // warm the optional legacy view
    let (copy, calls, _, _) = measured(|| values.clone());
    assert_eq!(calls, 2); // copying an old legacy view must not duplicate it again
    assert_eq!(copy.get_slot(&SYMBOLS, FieldSlot(25)), Some(&Value::Null));
}

#[teaql_entity]
#[derive(Clone, Debug, Default, PartialEq, TeaqlEntity)]
#[teaql(entity = "ReferenceProbe", symbol_fields = "type")]
struct ReferenceProbe {
    #[teaql(id)]
    id: u64,
    #[teaql(column = "type")]
    type_field_id: Option<u64>,
}

#[test]
fn canonical_reference_slots_stay_distinct_from_native_storage_slots() {
    let entity = ReferenceProbe {
        id: 7,
        type_field_id: Some(3),
        ..Default::default()
    };
    let symbols = ReferenceProbe::symbols().unwrap();
    assert_ne!(
        ReferenceProbe::__TEAQL_SLOT_type,
        ReferenceProbe::__TEAQL_SLOT_type_field_id
    );
    let state = entity.entity_runtime_state().unwrap();
    state.set_slot(
        entity.entity_key(),
        symbols,
        ReferenceProbe::__TEAQL_SLOT_type,
        Value::U64(9),
    );
    let changes = state.current_change_set();
    let fields = &changes.changes()[&entity.entity_key()];
    assert_eq!(fields.get("type"), Some(&Value::U64(9)));
    assert!(!fields.contains_key("type_field_id"));
    let storage = entity.into_values();
    assert_eq!(storage.get("type_field_id"), Some(&Value::U64(3)));
    assert!(!storage.contains_key("type"));
}
