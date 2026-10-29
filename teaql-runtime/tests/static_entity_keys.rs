//! Generated entity names are literals; cloning their keys must not copy names.
#[path = "support/allocation_counter.rs"]
mod allocation_counter;

use allocation_counter::measured;
use std::{borrow::Cow, collections::BTreeMap, hint::black_box};
use teaql_core::{Entity, Value};
use teaql_macros::{TeaqlEntity, teaql_entity};
use teaql_runtime::EntityKey;

#[teaql_entity]
#[derive(Clone, Debug, Default, PartialEq, TeaqlEntity)]
#[teaql(entity = "StaticKeyProbe")]
struct Probe {
    #[teaql(id)]
    id: u64,
    name: String,
}

#[test]
fn generated_keys_and_clones_allocate_nothing() {
    let probe = Probe {
        id: 7,
        ..Default::default()
    };
    assert!(matches!(
        probe.entity_key().entity,
        Cow::Borrowed("StaticKeyProbe")
    ));
    let (_, calls, bytes, _) = measured(|| {
        for _ in 0..1000 {
            let key = black_box(probe.entity_key());
            black_box(key.clone());
        }
    });
    assert_eq!((calls, bytes), (0, 0));
}

#[test]
fn static_and_dynamic_keys_share_identity_without_sharing_a_lifetime() {
    let dynamic = {
        let temporary = String::from("StaticKeyProbe");
        EntityKey::new(temporary.as_str(), 7_i64)
    };
    let borrowed = EntityKey::new_static("StaticKeyProbe", 7_u64);
    assert!(matches!(dynamic.entity, Cow::Owned(_)));
    assert_eq!(borrowed, dynamic);
    assert_eq!(borrowed.cmp(&dynamic), std::cmp::Ordering::Equal);
    let mut keys = BTreeMap::from([(dynamic, "old")]);
    assert_eq!(keys.insert(borrowed.clone(), "new"), Some("old"));
    assert_eq!(keys.len(), 1);
    assert_eq!(keys.get(&borrowed), Some(&"new"));
}

#[test]
fn generated_state_and_fix_keys_stay_borrowed_and_match_dynamic_lookup() {
    let mut probe = Probe {
        id: 7,
        ..Default::default()
    };
    probe.mark_as_new();
    assert!(probe.is_new());
    probe.set_comment("required audit comment");
    assert_eq!(
        probe.get_comment().as_deref(),
        Some("required audit comment")
    );
    let fields = ["name".to_owned()].into_iter().collect();
    let values = BTreeMap::from([("name".to_owned(), Value::from("context fix"))]);
    assert!(probe.begin_checker_fix());
    probe.set_checker_dirty_fields_borrowed(&fields, &values);
    assert_eq!(probe.dirty_fields(), Some(fields));
    let changes = probe.__teaql_runtime_state().current_change_set();
    assert!(!changes.changes().is_empty());
    assert!(
        changes
            .changes()
            .keys()
            .all(|key| matches!(key.entity, Cow::Borrowed("StaticKeyProbe")))
    );
    assert_eq!(
        &**changes
            .changes()
            .get(&EntityKey::new("StaticKeyProbe", 7_u64))
            .unwrap(),
        &values
    );
    assert_eq!(&*probe.take_checker_fix_changes(), &values);
    probe.mark_for_deletion();
    assert!(probe.is_marked_as_delete());
}
