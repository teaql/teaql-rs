//! Public runtime API on an unmodified generated entity, never source discovery.
use school_management_service_core::School;
use std::sync::Arc;
use teaql_core::{Entity, TeaqlEntity, Value};
use teaql_runtime::UserContext;

pub fn verify(context: &UserContext, full: &School) -> Result<(), Box<dyn std::error::Error>> {
    let mut json = full.clone().into_json();
    let object = json.as_object_mut().unwrap();
    object.insert("_name".into(), "derived name".into());
    object.insert("_zero".into(), 0.into());
    object.insert("_null".into(), serde_json::Value::Null);
    object.insert("_false".into(), false.into());
    object.insert("_empty".into(), "".into());
    let entity: School = context.decode_json_entity(&json)?;
    let snapshot = entity.loaded_state_snapshot().unwrap();
    let layout = School::field_layout()?.unwrap();
    assert_eq!(
        layout.field_count(),
        School::__TEAQL_FIXED_FIELD_INDEXES.len()
    );
    let derived_name = entity.dynamic_property("_name").unwrap();
    for reads in [1, 100, 10_000] {
        let (_, calls, bytes) = crate::allocation_counter::measured(|| {
            for _ in 0..reads {
                assert!(std::ptr::eq(
                    derived_name,
                    entity.dynamic_property("_name").unwrap()
                ));
                assert!(
                    matches!(entity.dynamic_property("_zero"), Some(Value::Json(value)) if value.as_i64() == Some(0))
                );
                assert!(
                    matches!(entity.dynamic_property("_false"), Some(Value::Json(value)) if value.as_bool() == Some(false))
                );
                assert!(
                    matches!(entity.dynamic_property("_empty"), Some(Value::Json(value)) if value.as_str() == Some(""))
                );
                assert!(entity.dynamic_property("_null").is_none());
                assert!(entity.has_dynamic_property("_null"));
                assert!(entity.dynamic_property("_absent").is_none());
                assert!(!entity.has_dynamic_property("_absent"));
                assert!(entity.dynamic_property("name").is_none());
                assert!(!entity.has_dynamic_property("#name"));
            }
        });
        assert_eq!((calls, bytes), (0, 0));
    }
    assert!(Arc::ptr_eq(
        &snapshot,
        &entity.loaded_state_snapshot().unwrap()
    ));
    for key in ["_name", "_zero", "_null", "_absent", "#name"] {
        assert!(layout.index(key).is_none());
        assert!(!snapshot.is_loaded(key));
    }
    assert!(entity.dirty_fields().is_none());
    assert!(!entity.has_pending_dynamic_mutations());
    println!("PASS generated Rust dynamic property borrowed null/missing/zero reads with zero allocation and provider entry");
    Ok(())
}
