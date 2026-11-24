//! Combined real SQLite readback-failure, mixed-list lifetime and retry control.
use school_management_service_core::{AuditedSave, School, E, Q};
use std::sync::Arc;
use teaql_core::dynamic_fields::{DynamicFieldSelection, DynamicFieldState};
use teaql_core::{DataType, Entity, SmartList, Value};
use teaql_runtime::UserContext;

pub async fn verify(
    context: &UserContext,
    mut first: School,
    first_name: &str,
    second_name: &str,
) -> Result<School, Box<dyn std::error::Error>> {
    first.update_dynamic_field("note", "matrix baseline".into())?;
    first.update_dynamic_field("unused", "keep unselected".into())?;
    first
        .audit_as("seed the combined-state value control")
        .save(context)
        .await?;
    let mut second = Q::schools()
        .with_name_in([second_name])
        .select_dynamic_fields_with(DynamicFieldSelection::All)
        .limit(1)
        .comment("what: load the companion School for a null extension")
        .purpose("why: preserve Value versus Null within one query shape")
        .execute_for_one(context)
        .await?
        .unwrap();
    second.update_dynamic_field("note", Value::Null)?;
    second
        .audit_as("seed the combined-state null control")
        .save(context)
        .await?;

    let selected = DynamicFieldSelection::fields([("note".into(), DataType::Text)])?;
    let rows = Q::schools()
        .with_name_in([first_name, second_name])
        .select_dynamic_fields_with(selected.clone())
        .order_by_id_asc()
        .limit(2)
        .comment("what: load Value and Null dynamic rows together")
        .purpose("why: combine shared metadata with independent payloads")
        .execute_for_list(context)
        .await?;
    assert_eq!(rows.len(), 2);
    let shared = rows[0].loaded_state_snapshot().unwrap();
    assert!(Arc::ptr_eq(
        &shared,
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    if std::env::var("TEAQL_LOAD_STATE_WIDE").as_deref() == Ok("true") {
        assert!(shared.overflow().is_some());
    }
    assert_eq!(
        rows[0]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .state(),
        DynamicFieldState::Value
    );
    assert_eq!(
        rows[1]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .state(),
        DynamicFieldState::Null
    );
    for row in &rows {
        assert_eq!(
            row.dynamic_field_values().unwrap().field("unused")?.state(),
            DynamicFieldState::NotLoaded
        );
    }
    let sparse = Q::schools_minimal()
        .with_name_in([first_name])
        .select_name()
        .select_dynamic_fields_with(selected)
        .limit(1)
        .comment("what: append a sparse view of the same School")
        .purpose("why: a mixed list must not union native projections")
        .execute_for_one(context)
        .await?
        .unwrap();
    let sparse_state = sparse.loaded_state_snapshot().unwrap();
    let mut held = SmartList::new(vec![rows[0].clone(), rows[1].clone()]);
    held.push(sparse);
    drop(rows);
    assert!(!held[2].is_field_loaded("address"));
    assert!(Arc::ptr_eq(
        &sparse_state,
        &held[2].loaded_state_snapshot().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &shared,
        &held[0].loaded_state_snapshot().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &shared,
        &held[1].loaded_state_snapshot().unwrap()
    ));
    let first_version = held[0].version();
    let second_version = held[1].version();
    let mut pending = held[0].clone();
    // Trusted fixture hydration attaches a readonly result without treating
    // serialized persistent fields as authorized untrusted JSON input.
    let fields = pending.dynamic_field_values().unwrap().clone();
    let mut json = pending.into_json();
    let object = json.as_object_mut().unwrap();
    object.retain(|key, _| !key.starts_with('#'));
    object.insert("_matrix_total".into(), 17.into());
    pending = context.decode_json_entity(&json)?;
    pending.install_loaded_dynamic_fields(fields, shared.clone())?;
    assert!(pending.has_dynamic_property("_matrix_total"));
    let next_name = format!("{first_name} retry");
    pending.update_name(next_name.as_str());
    pending.update_dynamic_field("note", "matrix retry".into())?;
    let dirty = pending.dirty_fields().unwrap();
    assert!(!dirty.contains("_matrix_total"));
    assert!(!dirty.contains("unused"));
    assert!(Arc::ptr_eq(
        &shared,
        &pending.loaded_state_snapshot().unwrap()
    ));

    // Direct SQL is only deterministic fault injection in this run's isolated database.
    let control = rusqlite::Connection::open(std::env::var("TEAQL_LOAD_STATE_DATABASE")?)?;
    control.execute_batch("CREATE TRIGGER corrupt_matrix_readback_insert AFTER INSERT ON teaql_dynamic_field_storage_v1
        WHEN NEW.code='note' AND NEW.is_null=0 AND NEW.payload <> 'not-json'
        BEGIN UPDATE teaql_dynamic_field_storage_v1 SET payload='not-json'
        WHERE namespace=NEW.namespace AND owner_type=NEW.owner_type AND owner_id=NEW.owner_id AND code=NEW.code; END;
        CREATE TRIGGER corrupt_matrix_readback_update AFTER UPDATE ON teaql_dynamic_field_storage_v1
        WHEN NEW.code='note' AND NEW.is_null=0 AND NEW.payload <> 'not-json'
        BEGIN UPDATE teaql_dynamic_field_storage_v1 SET payload='not-json'
        WHERE namespace=NEW.namespace AND owner_type=NEW.owner_type AND owner_id=NEW.owner_id AND code=NEW.code; END")?;
    let failure = pending
        .clone()
        .audit_as("rollback a malformed dynamic readback")
        .save(context)
        .await;
    control.execute_batch(
        "DROP TRIGGER corrupt_matrix_readback_insert; DROP TRIGGER corrupt_matrix_readback_update",
    )?;
    let error = match failure {
        Err(error) => error,
        Ok(_) => panic!("malformed authoritative dynamic readback must fail"),
    };
    assert!(error.to_string().contains("DYNAMIC_FIELD_INVALID_STORAGE"));
    assert_eq!(pending.version(), first_version);
    assert!(pending.has_dynamic_property("_matrix_total"));
    assert!(pending.has_pending_dynamic_mutations());
    assert!(pending.dirty_fields().unwrap().contains("name"));
    assert!(Arc::ptr_eq(
        &shared,
        &pending.loaded_state_snapshot().unwrap()
    ));
    assert_eq!(held[1].version(), second_version);
    assert!(held[1].dirty_fields().is_none());
    assert_eq!(
        held[1]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .state(),
        DynamicFieldState::Null
    );
    assert_eq!(
        held[2]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .value(),
        Some(&Value::Text("matrix baseline".into()))
    );
    assert!(!held[2].is_field_loaded("address"));
    let stored = Q::schools()
        .with_name_in([first_name, second_name])
        .select_dynamic_fields_with(DynamicFieldSelection::All)
        .order_by_id_asc()
        .limit(2)
        .comment("what: inspect native and extension values after rollback")
        .purpose("why: failed authoritative readback must roll back both stores")
        .execute_for_list(context)
        .await?;
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].version(), first_version);
    assert_eq!(
        E::school(&stored[0]).get_name().eval().as_deref(),
        Some(first_name)
    );
    assert_eq!(
        stored[0]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .value(),
        Some(&Value::Text("matrix baseline".into()))
    );
    assert_eq!(stored[1].version(), second_version);
    assert_eq!(
        stored[0]
            .dynamic_field_values()
            .unwrap()
            .field("unused")?
            .value(),
        Some(&Value::Text("keep unselected".into()))
    );
    assert!(!stored[0].has_dynamic_property("_matrix_total"));
    assert_eq!(
        stored[1]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .state(),
        DynamicFieldState::Null
    );
    let saved = pending
        .audit_as("retry the original combined native and extension intent")
        .save(context)
        .await?;
    assert_eq!(saved.version(), first_version + 1);
    assert_eq!(
        E::school(&saved).get_name().eval().as_deref(),
        Some(next_name.as_str())
    );
    assert_eq!(
        saved.dynamic_field_values().unwrap().field("note")?.value(),
        Some(&Value::Text("matrix retry".into()))
    );
    assert_eq!(
        saved
            .dynamic_field_values()
            .unwrap()
            .field("unused")?
            .state(),
        DynamicFieldState::NotLoaded
    );
    assert!(Arc::ptr_eq(
        &shared,
        &saved.loaded_state_snapshot().unwrap()
    ));
    assert!(!saved.has_pending_dynamic_mutations());
    assert!(saved.dirty_fields().is_none());
    let inspected = Q::schools()
        .with_name_in([next_name.as_str()])
        .select_dynamic_fields_with(DynamicFieldSelection::All)
        .limit(1)
        .comment("what: inspect the unselected extension after retry")
        .purpose("why: NotLoaded must not become a clear or delete intent")
        .execute_for_one(context)
        .await?
        .unwrap();
    assert_eq!(
        inspected
            .dynamic_field_values()
            .unwrap()
            .field("unused")?
            .value(),
        Some(&Value::Text("keep unselected".into()))
    );
    assert!(!inspected.has_dynamic_property("_matrix_total"));
    assert_eq!(held[1].version(), second_version);
    assert_eq!(
        held[1]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .state(),
        DynamicFieldState::Null
    );
    assert!(Arc::ptr_eq(
        &shared,
        &held[1].loaded_state_snapshot().unwrap()
    ));
    println!("PASS generated Rust mixed dynamic Value/Null/NotLoaded list lifetime readback rollback and retry");
    println!("PASS generated Rust stored unselected extension survives rollback retry and readonly property is not persisted");
    Ok(saved)
}
