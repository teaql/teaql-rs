//! Generated Q/E and audited mutations on a real cursor with durable extensions.
use futures_util::StreamExt;
use school_management_service_core::{
    service_runtime, AuditedSave, School, ServiceRuntimeConfig, ServiceRuntimeExecutor, E, Q,
};
use std::sync::Arc;
use teaql_core::{
    dynamic_fields::{DynamicFieldDefinitions, DynamicFieldSelection, DynamicFieldState},
    time::Timestamp,
    DataType, Entity, TeaqlEntity, Value,
};
use teaql_runtime::dynamic_fields::DatabaseDynamicFieldsProvider;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let database = format!(
        "{}.dynamic-stream",
        std::env::var("TEAQL_LOAD_STATE_DATABASE")?
    );
    let round = std::env::var("TEAQL_LOAD_STATE_ROUND")?;
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: database,
    })
    .await?;
    let definitions = DynamicFieldDefinitions::new(
        <School as TeaqlEntity>::ENTITY_NAME,
        "stream-example-v1",
        [
            ("note".into(), DataType::Text),
            ("unused".into(), DataType::Text),
        ],
    )?;
    context.set_dynamic_fields_provider(Arc::new(DatabaseDynamicFieldsProvider::<
        ServiceRuntimeExecutor,
    >::new("generated-stream", [definitions])?));
    context.ensure_schema().await?;
    let names = [
        format!("{round} stream value"),
        format!("{round} stream null"),
        format!("{round} stream missing"),
    ];
    for (index, name) in names.iter().enumerate() {
        let mut school = Q::schools()
            .comment("what: allocate an isolated stream School")
            .purpose("why: verify streaming without changing other example fixtures")
            .new_entity(&context);
        school.update_platform_id(1);
        school.update_school_type_to_primary();
        school.update_name(name.as_str());
        school.update_address("Stream address");
        school.update_established_date(Value::Date("1995-09-01".parse()?));
        school.update_student_capacity(0);
        school.update_active(false);
        school.update_create_time(Timestamp(1_700_000_000_000));
        school.update_update_time(Timestamp(1_700_000_000_000));
        let created = school
            .audit_as("create an isolated generated streaming fixture")
            .save(&context)
            .await?;
        if index < 2 {
            let mut row = Q::schools()
                .with_id_is(created.id())
                .select_self_fields()
                .select_dynamic_fields_with(DynamicFieldSelection::All)
                .limit(1)
                .comment("what: load the complete School for extension setup")
                .purpose("why: seed durable value and null through generated audited mutation")
                .execute_for_one(&context)
                .await?
                .unwrap();
            row.update_dynamic_field(
                "note",
                if index == 0 {
                    "stream-private".into()
                } else {
                    Value::Null
                },
            )?;
            row.update_dynamic_field("unused", "unselected-private".into())?;
            row.audit_as("seed durable stream extensions")
                .save(&context)
                .await?;
        }
    }
    let selection = DynamicFieldSelection::fields([("note".into(), DataType::Text)])?;
    for size in [1, 2, 3, 73] {
        let mut stream = Q::schools()
            .with_name_in(names.iter().map(String::as_str))
            .select_self_fields()
            .select_dynamic_fields_with(selection.clone())
            .order_by_id_asc()
            .limit(3)
            .stream(size)
            .comment("what: stream generated Schools with selected durable extensions")
            .purpose("why: preserve Value Null NotLoaded and bounded batch state")
            .execute_for_stream(&context)
            .await?;
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await {
            rows.push(row?);
        }
        drop(stream);
        assert_eq!(rows.len(), 3);
        assert!(Arc::ptr_eq(
            &rows[0].loaded_state_snapshot().unwrap(),
            &rows[1].loaded_state_snapshot().unwrap()
        ));
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(
                E::school(row).get_name().eval().as_deref(),
                Some(names[index].as_str())
            );
            assert_eq!(E::school(row).get_student_capacity().eval(), Some(0));
            assert_eq!(E::school(row).get_active().eval(), Some(false));
            let state = row.loaded_state_snapshot().unwrap();
            for (name, slot) in School::__TEAQL_FIXED_FIELD_INDEXES {
                // A reference's fixed bit stores its FK, not materialized target detail.
                if state.layout().is_relation(name) {
                    assert!(
                        !row.is_field_loaded(name),
                        "target detail was not requested"
                    );
                } else {
                    assert!(
                        row.is_field_loaded(name),
                        "complete stream omitted fixed slot {slot}: {name}"
                    );
                }
                if *slot < 64 {
                    assert_ne!(
                        state.bits() & (1_u64 << slot),
                        0,
                        "native/FK slot {slot} missing"
                    );
                } else {
                    assert!(state.overflow().unwrap().contains(slot));
                }
            }
            if std::env::var("TEAQL_LOAD_STATE_WIDE").as_deref() == Ok("true") {
                let json = row.clone().into_json();
                for slot in [63, 64, 65, 129] {
                    let (name, _) = School::__TEAQL_FIXED_FIELD_INDEXES
                        .iter()
                        .find(|(_, index)| *index == slot)
                        .unwrap();
                    assert!(
                        json.get(*name).unwrap().is_null(),
                        "loaded-null slot {slot} changed during enhancement"
                    );
                }
            }
            let fields = row.dynamic_field_values().unwrap();
            assert_eq!(
                fields.field("note")?.state(),
                match index {
                    0 => DynamicFieldState::Value,
                    1 => DynamicFieldState::Null,
                    _ => DynamicFieldState::NotLoaded,
                }
            );
            assert_eq!(
                fields.field("unused")?.state(),
                DynamicFieldState::NotLoaded
            );
            assert!(row.dirty_fields().is_none());
            assert!(!row.has_pending_dynamic_mutations());
        }
    }
    let mut sparse_stream = Q::schools_minimal()
        .with_name_in(names.iter().map(String::as_str))
        .select_name()
        .select_dynamic_fields_with(selection.clone())
        .order_by_id_asc()
        .limit(3)
        .stream(1)
        .comment("what: stream a sparse native projection with loaded extensions")
        .purpose("why: enhancement must not authorize an incomplete object save")
        .execute_for_stream(&context)
        .await?;
    let mut sparse = Vec::new();
    while let Some(row) = sparse_stream.next().await {
        sparse.push(row?);
    }
    drop(sparse_stream);
    assert_eq!(sparse.len(), 3);
    for row in &sparse {
        for (name, _) in School::__TEAQL_FIXED_FIELD_INDEXES {
            assert_eq!(
                row.is_field_loaded(name),
                ["id", "version", "name"].contains(name)
            );
        }
        assert_eq!(
            row.dynamic_field_values().unwrap().field("unused")?.state(),
            DynamicFieldState::NotLoaded
        );
    }
    let shared = sparse[0].loaded_state_snapshot().unwrap();
    assert!(Arc::ptr_eq(
        &shared,
        &sparse[1].loaded_state_snapshot().unwrap()
    ));
    let observed = crate::observed_executor::ObservedExecutor::new(
        context
            .require_resource::<ServiceRuntimeExecutor>()?
            .clone(),
    );
    context.register_executor(observed.clone());
    let before = observed.counts();
    let mut pending = sparse[0].clone();
    pending.update_dynamic_field("note", "must-not-persist".into())?;
    let error = pending
        .clone()
        .audit_as("reject extension mutation on an incomplete streamed object")
        .save(&context)
        .await
        .expect_err("loaded extensions cannot authorize a sparse object save");
    assert!(
        matches!(error, teaql_runtime::RuntimeError::Check(_)),
        "wrong rejection: {error}"
    );
    assert_eq!(observed.counts(), before);
    assert!(Arc::ptr_eq(
        &shared,
        &pending.loaded_state_snapshot().unwrap()
    ));
    assert!(!pending.is_field_loaded("address"));
    assert!(pending.has_pending_dynamic_mutations());
    assert_eq!(
        sparse[1]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .state(),
        DynamicFieldState::Null
    );
    assert!(!sparse[1].has_pending_dynamic_mutations());
    let mut stream = Q::schools()
        .with_name_in(names.iter().map(String::as_str))
        .select_self_fields()
        .select_dynamic_fields_with(selection.clone())
        .order_by_id_asc()
        .limit(3)
        .stream(1)
        .comment("what: take the first streamed School then close the cursor")
        .purpose("why: allow audited mutation of a held complete streamed object")
        .execute_for_stream(&context)
        .await?;
    let mut first = stream.next().await.unwrap()?;
    drop(stream);
    first.update_dynamic_field("note", "after-stream-close".into())?;
    first
        .audit_as("update a held streamed entity after early cursor close")
        .save(&context)
        .await?;
    assert!(
        observed.counts()[1] > before[1],
        "positive save must prove the observer is live"
    );
    let reloaded = Q::schools()
        .with_name_is(names[0].as_str())
        .select_self_fields()
        .select_dynamic_fields_with(selection)
        .limit(1)
        .comment("what: reload the modified streamed School")
        .purpose("why: verify released cursor and authoritative extension readback")
        .execute_for_one(&context)
        .await?
        .unwrap();
    assert_eq!(
        reloaded
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .value(),
        Some(&"after-stream-close".into())
    );
    println!("PASS generated Rust durable dynamic stream Value/Null/NotLoaded sharing and early-drop audited save {round}");
    println!("PASS generated Rust dynamic stream full/overflow/sparse state and Checker-before-provider {round}");
    Ok(())
}
