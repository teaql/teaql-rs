//! Same-base-name namespace and availability-divergence acceptance.
use school_management_service_core::{AuditedSave, School, Q};
use std::sync::Arc;
use teaql_core::dynamic_fields::{DynamicFieldSelection, DynamicFieldState};
use teaql_core::{DataType, Entity, Value};
use teaql_runtime::UserContext;

pub async fn verify(
    context: &UserContext,
    id: u64,
    name: &str,
    sibling: &str,
) -> Result<School, Box<dyn std::error::Error>> {
    let rows = Q::schools()
        .with_name_in([name, sibling])
        .select_self_fields()
        .select_dynamic_fields_with(DynamicFieldSelection::fields([(
            "note".into(),
            DataType::Text,
        )])?)
        .order_by_id_asc()
        .limit(2)
        .comment("what: load exact same-shape namespace controls")
        .purpose("why: separate fixed derived and persistent members with one base name")
        .execute_for_list(context)
        .await?;
    assert_eq!(rows.len(), 2);
    let shared = rows[0].loaded_state_snapshot().unwrap();
    assert!(Arc::ptr_eq(
        &shared,
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    assert_eq!(rows[0].id(), id);
    let fields = rows[0].dynamic_field_values().unwrap().clone();
    assert_eq!(fields.field("name")?.state(), DynamicFieldState::NotLoaded);
    let mut json = rows[0].clone().into_json();
    let object = json.as_object_mut().unwrap();
    object.retain(|key, _| !key.starts_with('#'));
    object.insert("_name".into(), "readonly same name".into());
    let mut first: School = context.decode_json_entity(&json)?;
    first.install_loaded_dynamic_fields(fields, shared.clone())?;
    assert_eq!(
        first.dynamic_field_values().unwrap().field("note")?.state(),
        DynamicFieldState::Value
    );
    let source_note = rows[0]
        .dynamic_field_values()
        .unwrap()
        .field("note")?
        .value()
        .cloned();
    let sibling_note = rows[1]
        .dynamic_field_values()
        .unwrap()
        .field("note")?
        .value()
        .cloned();
    first.update_dynamic_field("note", "value-only private control".into())?;
    assert!(Arc::ptr_eq(
        &shared,
        &first.loaded_state_snapshot().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &shared,
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    assert_eq!(
        first.dynamic_field_values().unwrap().field("note")?.value(),
        Some(&Value::Text("value-only private control".into()))
    );
    assert_eq!(
        rows[0]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .value(),
        source_note.as_ref()
    );
    assert_eq!(
        rows[1]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .value(),
        sibling_note.as_ref()
    );
    assert!(rows[0].dirty_fields().is_none());
    assert!(rows[1].dirty_fields().is_none());
    assert!(!rows[1].has_pending_dynamic_mutations());
    println!("PASS generated Rust value-only dynamic mutation retains shared snapshot and sibling payload");
    first.update_dynamic_field("name", "persistent same name".into())?;
    assert!(!Arc::ptr_eq(
        &shared,
        &first.loaded_state_snapshot().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &shared,
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    assert!(!shared.is_loaded("#name"));
    assert_eq!(
        rows[1]
            .dynamic_field_values()
            .unwrap()
            .field("name")?
            .state(),
        DynamicFieldState::NotLoaded
    );
    assert!(rows[1].dirty_fields().is_none());
    assert_eq!(first.name(), name);
    let json = first.clone().into_json();
    assert_eq!(json["name"], name);
    assert_eq!(json["_name"], "readonly same name");
    assert_eq!(json["#name"], "persistent same name");
    first
        .audit_as("persist only the namespaced extension")
        .save(context)
        .await?;
    let mut read = Q::schools()
        .with_id_is(id)
        .select_self_fields()
        .select_dynamic_fields_with(DynamicFieldSelection::fields([(
            "name".into(),
            DataType::Text,
        )])?)
        .limit(1)
        .comment("what: read back the namespace controls")
        .purpose("why: prove readonly properties never become persistent fields")
        .execute_for_one(context)
        .await?
        .unwrap();
    assert_eq!(read.name(), name);
    assert!(read.dynamic_property("_name").is_none());
    assert_eq!(
        read.dynamic_field_values().unwrap().field("name")?.value(),
        Some(&Value::Text("persistent same name".into()))
    );
    let before = read.loaded_state_snapshot().unwrap();
    read.delete_dynamic_field("name")?;
    assert!(!Arc::ptr_eq(
        &before,
        &read.loaded_state_snapshot().unwrap()
    ));
    assert!(before.is_loaded("#name"));
    assert_eq!(
        read.dynamic_field_values().unwrap().field("name")?.state(),
        DynamicFieldState::NotLoaded
    );
    let saved = read
        .audit_as("delete only the namespaced extension control")
        .save(context)
        .await?;
    println!("PASS generated Rust LF19 fixed derived and persistent same-name namespace isolation");
    println!("PASS generated Rust LF23 dynamic availability detaches only one view");
    Ok(saved)
}
