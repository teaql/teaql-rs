use school_management_service_core::{
    AuditedSave, E, Q, ServiceRuntimeConfig, ServiceRuntimeExecutor, service_runtime,
};
use std::sync::Arc;
use teaql_core::dynamic_fields::{
    DynamicFieldDefinitions, DynamicFieldSelection, DynamicFieldState,
};
use teaql_core::{Entity, TeaqlEntity, Value, time::Timestamp};
use teaql_runtime::dynamic_fields::DatabaseDynamicFieldsProvider;

#[path = "support/checker_allocations.rs"]
mod allocation_counter;
#[path = "support/checker_state.rs"]
mod checker_state;
#[path = "support/dynamic_property.rs"]
mod dynamic_property;
#[path = "support/dynamic_rollback.rs"]
mod dynamic_rollback;
#[path = "support/dynamic_stream.rs"]
mod dynamic_stream;
#[path = "support/empty_native.rs"]
mod empty_native;
#[path = "support/field_order.rs"]
mod field_order;
#[path = "support/independent_mutation.rs"]
mod independent_mutation;
#[path = "support/materialization.rs"]
mod materialization;
#[path = "support/namespace_cow.rs"]
mod namespace_cow;
#[path = "support/native_rollback.rs"]
mod native_rollback;
#[path = "support/nested_graph.rs"]
mod nested_graph;
#[path = "support/observed_executor.rs"]
mod observed_executor;
#[path = "support/original_clone.rs"]
mod original_clone;
#[path = "support/page_stream.rs"]
mod page_stream;
#[path = "support/partial_graph_checker.rs"]
mod partial_graph_checker;
#[path = "support/property_metadata.rs"]
mod property_metadata;

#[tokio::test]
async fn fresh_generated_school_uses_shared_indexed_state() -> Result<(), Box<dyn std::error::Error>>
{
    let flow = Box::pin(generated_school_flow());
    println!(
        "GENERATED_SCHOOL_FLOW_BYTES={}",
        std::mem::size_of_val(flow.as_ref().get_ref())
    );
    flow.await
}

#[tokio::test]
async fn fresh_generated_materialization_uses_its_own_model_target()
-> Result<(), Box<dyn std::error::Error>> {
    Box::pin(materialization::run()).await
}

#[tokio::test]
async fn fresh_generated_dynamic_stream_uses_native_cursor_and_audited_save()
-> Result<(), Box<dyn std::error::Error>> {
    Box::pin(dynamic_stream::run()).await
}

async fn generated_school_flow() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::var("TEAQL_LOAD_STATE_DATABASE")?;
    let round = std::env::var("TEAQL_LOAD_STATE_ROUND")?;
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: database,
    })
    .await?;
    let definitions = DynamicFieldDefinitions::new(
        <school_management_service_core::School as TeaqlEntity>::ENTITY_NAME,
        "example-v1",
        [
            ("note".into(), teaql_core::DataType::Text),
            ("unused".into(), teaql_core::DataType::Text),
            ("name".into(), teaql_core::DataType::Text),
        ],
    )?;
    context.set_dynamic_fields_provider(Arc::new(DatabaseDynamicFieldsProvider::<
        ServiceRuntimeExecutor,
    >::new("example", [definitions.clone()])?));
    context.ensure_schema().await?;
    context.ensure_schema().await?;
    let constants = Q::school_types()
        .select_self_fields()
        .order_by_id_asc()
        .limit(10)
        .comment("what: inspect model-owned SchoolType bootstrap")
        .purpose("why: prove repeated schema setup keeps fixed constants")
        .execute_for_list(&context)
        .await?;
    assert_eq!(constants.len(), 2);
    assert_eq!(constants[0].id(), 1001);
    assert_eq!(constants[1].id(), 1002);
    let first = format!("{round} First School");
    let second = format!("{round} Second School");
    for name in [&first, &second] {
        let mut school = Q::schools()
            .comment("what: allocate a load-state probe School")
            .purpose("why: exercise the generated mutation and Checker contracts")
            .new_entity(&context);
        school.update_platform_id(1_u64);
        school.update_school_type_to_primary();
        school.update_name(name.as_str());
        school.update_address("Private row address");
        school.update_established_date(Value::Date("1995-09-01".parse()?));
        school.update_student_capacity(0_i64);
        school.update_active(false);
        school.update_create_time(Timestamp(1_700_000_000_000));
        school.update_update_time(Timestamp(1_700_000_000_000));
        school
            .audit_as("create controlled load-state probe")
            .save(&context)
            .await?;
    }
    let full = Q::schools()
        .with_name_in([first.as_str(), second.as_str()])
        .select_self_fields()
        .order_by_id_asc()
        .limit(2)
        .comment("what: load two compatible full projections")
        .purpose("why: compare actual immutable state references")
        .execute_for_list(&context)
        .await?;
    assert_eq!(full.len(), 2);
    original_clone::verify(&full[0]);
    let snapshot = full[0]
        .loaded_state_snapshot()
        .expect("generated indexed state");
    assert!(Arc::ptr_eq(
        &snapshot,
        &full[1].loaded_state_snapshot().unwrap()
    ));
    let layout = school_management_service_core::School::field_layout()?.unwrap();
    assert!(Arc::ptr_eq(snapshot.layout(), &layout));
    for (name, index) in school_management_service_core::School::__TEAQL_FIXED_FIELD_INDEXES {
        assert_eq!(layout.index(name), Some(*index));
    }
    let wide = std::env::var("TEAQL_LOAD_STATE_WIDE").as_deref() == Ok("true");
    if wide {
        assert!(layout.field_count() > 130);
        let values = full[0].clone().into_values();
        for index in [0, 31, 32, 63, 64, 65, 129] {
            let (name, _) = school_management_service_core::School::__TEAQL_FIXED_FIELD_INDEXES
                .iter()
                .find(|(_, slot)| *slot == index)
                .unwrap();
            assert!(snapshot.is_loaded(name), "missing generated slot {index}");
            if index < 64 {
                assert_ne!(snapshot.bits() & (1_u64 << index), 0);
            } else {
                assert!(snapshot.overflow().unwrap().contains(&index));
            }
            if index >= 31 {
                assert!(
                    name.starts_with("probe_"),
                    "expected a nullable boundary probe"
                );
                assert!(
                    matches!(values.get(*name), Some(Value::Null | Value::TypedNull(_))),
                    "slot {index} must retain loaded NULL"
                );
            }
        }
        let (name, _) = school_management_service_core::School::__TEAQL_FIXED_FIELD_INDEXES
            .iter()
            .find(|(_, slot)| *slot == 64)
            .unwrap();
        let detached = teaql_core::LoadedSnapshot::with_loaded(&snapshot, name, false)?;
        assert!(!Arc::ptr_eq(&snapshot, &detached));
        assert!(!detached.is_loaded(name));
        assert!(full[1].is_field_loaded(name));
        println!(
            "WIDE generated Rust fields={} bit63/overflow64/65/129 loaded NULL and COW",
            layout.field_count()
        );
    }
    assert_eq!(E::school(&full[0]).get_student_capacity().eval(), Some(0));
    assert_eq!(E::school(&full[0]).get_active().eval(), Some(false));
    assert_eq!(E::school(&full[0]).get_school_type_id().eval(), Some(1001));
    field_order::verify(&context, full[0].id()).await?;
    page_stream::verify(&context, &first, &second).await?;
    Box::pin(property_metadata::verify(&context, &first, &second)).await?;

    let sparse = Q::schools_minimal()
        .with_name_in([first.as_str(), second.as_str()])
        .select_name()
        .order_by_id_asc()
        .limit(2)
        .comment("what: load the same rows with a minimal projection")
        .purpose("why: keep actual projection boundaries independent")
        .execute_for_list(&context)
        .await?;
    let sparse_snapshot = sparse[0].loaded_state_snapshot().unwrap();
    assert!(Arc::ptr_eq(
        &sparse_snapshot,
        &sparse[1].loaded_state_snapshot().unwrap()
    ));
    assert!(!Arc::ptr_eq(&snapshot, &sparse_snapshot));
    assert!(sparse[0].is_field_loaded("id"));
    assert!(sparse[0].is_field_loaded("version"));
    assert!(!sparse[0].is_field_loaded("address"));
    checker_state::verify(&context, &full[0], &sparse[0]);
    let sparse_json = sparse[0].clone().into_json();
    assert_eq!(sparse_json["name"], first);
    assert!(sparse_json.get("address").is_none());
    assert!(sparse_json.get("established_date").is_none());
    let full_json = full[0].clone().into_json();
    assert_eq!(full_json["established_date"], "1995-09-01");
    assert_eq!(full_json["student_capacity"], 0);
    assert_eq!(full_json["active"], false);
    assert!(
        full_json.get("platform").is_none(),
        "FK identity is not materialized target detail"
    );
    assert!(full_json.get("school_type").is_none());
    let restored =
        context.decode_json_entity::<school_management_service_core::School>(&sparse_json)?;
    assert_eq!(
        E::school(&restored).get_name().eval().as_deref(),
        Some(first.as_str())
    );
    assert!(!restored.is_field_loaded("address"));
    assert!(restored.dirty_fields().is_none());
    assert!(!restored.has_pending_dynamic_mutations());
    empty_native::verify(&context, &sparse_json)?;
    assert_eq!(restored.into_json(), sparse_json);
    let restored =
        context.decode_json_entity::<school_management_service_core::School>(&full_json)?;
    assert_eq!(E::school(&restored).get_student_capacity().eval(), Some(0));
    assert_eq!(E::school(&restored).get_active().eval(), Some(false));
    assert_eq!(E::school(&restored).get_school_type_id().eval(), Some(1001));
    assert!(restored.dirty_fields().is_none());
    assert_eq!(restored.into_json(), full_json);
    let decoded = context.decode_json_entities::<school_management_service_core::School>(
        &teaql_core::serde_json::json!([
            sparse[0].clone().into_json(),
            sparse[1].clone().into_json()
        ]),
    )?;
    assert!(Arc::ptr_eq(
        &decoded[0].loaded_state_snapshot().unwrap(),
        &decoded[1].loaded_state_snapshot().unwrap()
    ));
    assert_eq!(
        E::school(&decoded[1]).get_name().eval().as_deref(),
        Some(second.as_str())
    );
    println!("PASS generated Rust typed native JSON roundtrip and snapshot sharing {round}");
    assert!(Arc::ptr_eq(
        &snapshot,
        &full[0].loaded_state_snapshot().unwrap()
    ));
    if wide {
        let values = sparse[0].clone().into_values();
        for index in [31, 32, 63, 64, 65, 129] {
            let (name, _) = school_management_service_core::School::__TEAQL_FIXED_FIELD_INDEXES
                .iter()
                .find(|(_, slot)| *slot == index)
                .unwrap();
            assert!(!sparse[0].is_field_loaded(name));
            assert!(
                !values.contains_key(*name),
                "NotLoaded slot {index} must not serialize as NULL"
            );
        }
    }
    assert_eq!(
        E::school(&sparse[0]).get_name().eval().as_deref(),
        Some(first.as_str())
    );
    let mut divergent = sparse[0].clone();
    divergent.update_address("New private address");
    assert!(divergent.is_field_loaded("address"));
    assert!(!sparse[1].is_field_loaded("address"));
    assert!(!Arc::ptr_eq(
        &sparse_snapshot,
        &divergent.loaded_state_snapshot().unwrap()
    ));
    let original_executor = context
        .require_resource::<ServiceRuntimeExecutor>()?
        .clone();
    let observed = observed_executor::ObservedExecutor::new(original_executor.clone());
    context.register_executor(observed.clone());
    dynamic_property::verify(&context, &full[0])?;
    assert_eq!(
        observed.counts(),
        [0; 5],
        "derived-property reads must not enter any provider"
    );
    let rejection = divergent
        .audit_as("reject sparse whole-object mutation")
        .save(&context)
        .await
        .expect_err("sparse whole-object mutation must be rejected");
    assert!(
        matches!(rejection, teaql_runtime::RuntimeError::Check(_)),
        "wrong rejection: {rejection}"
    );
    assert_eq!(
        observed.counts(),
        [0; 5],
        "Checker must reject before query, mutation or transaction provider entry"
    );

    let mut changed = full[0].clone();
    let renamed = format!("{round} Renamed School");
    changed.update_name(renamed.as_str());
    assert!(Arc::ptr_eq(
        &snapshot,
        &changed.loaded_state_snapshot().unwrap()
    ));
    assert_eq!(full[1].name(), second);
    let changed_id = changed.id();
    let saved = changed
        .audit_as("rename one row without altering its sibling")
        .save(&context)
        .await?;
    assert_eq!(saved.version(), full[0].version() + 1);
    let accepted_counts = observed.counts();
    assert!(
        accepted_counts[0] > 0
            && accepted_counts[1] > 0
            && accepted_counts[2] > 0
            && accepted_counts[3] > 0,
        "the observer must see an accepted readback, mutation, transaction and commit: {accepted_counts:?}"
    );
    println!(
        "PASS generated Rust sparse Checker rejects before provider entry; positive save counts={accepted_counts:?}"
    );
    independent_mutation::verify(&context, changed_id, &renamed, &second).await?;
    partial_graph_checker::verify(&context, &renamed, &observed).await?;
    context.register_executor(original_executor);
    let related = Q::schools()
        .with_id_is(changed_id)
        .select_platform_with(Q::platforms_minimal().select_name())
        .select_school_type_with(Q::school_types_minimal().select_code())
        .limit(1)
        .comment("what: reload the renamed School and its forward relations")
        .purpose("why: verify generated multiword FK and typed E traversal")
        .execute_for_one(&context)
        .await?
        .unwrap();
    assert_eq!(
        E::school(&related)
            .get_platform()
            .get_name()
            .eval()
            .as_deref(),
        Some("Campus Learning Platform")
    );
    assert_eq!(
        E::school(&related)
            .get_school_type()
            .get_code()
            .eval()
            .as_deref(),
        Some("PRIMARY")
    );
    let related_json = related.clone().into_json();
    assert_eq!(related_json["platform"]["name"], "Campus Learning Platform");
    assert_eq!(related_json["school_type"]["code"], "PRIMARY");
    nested_graph::verify(&context, changed_id, &renamed, &second).await?;
    let mut extended = Q::schools()
        .with_id_is(changed_id)
        .select_dynamic_fields_with(DynamicFieldSelection::All)
        .limit(1)
        .comment("what: select runtime-defined persistent extension metadata")
        .purpose("why: qualify the freshly generated dynamic-field carrier")
        .execute_for_one(&context)
        .await?
        .unwrap();
    assert_eq!(
        extended
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .state(),
        DynamicFieldState::NotLoaded
    );
    extended.update_dynamic_field("note", "Private extension".into())?;
    let pending_json = extended.clone().into_json();
    assert_eq!(pending_json["#note"], "Private extension");
    for internal in [
        "_comment",
        "_is_new",
        "_is_deleted",
        "_dirty_fields",
        "_original_values",
    ] {
        assert!(pending_json.get(internal).is_none());
    }
    let mut extended = extended
        .audit_as("save native and extension metadata together")
        .save(&context)
        .await?;
    assert_eq!(
        extended
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .value(),
        Some(&Value::Text("Private extension".into()))
    );
    let saved_json = extended.clone().into_json();
    assert_eq!(saved_json["#note"], "Private extension");
    assert_eq!(saved_json["name"], renamed);
    assert!(saved_json.get("_original_values").is_none());
    println!("PASS generated Rust namespace serialization and NotLoaded boundary");
    nested_graph::verify_dynamic(&context, changed_id, &renamed, &second).await?;
    let held_version = extended.version();
    extended.update_dynamic_field("note", "must not cross storage profile".into())?;
    context.set_dynamic_fields_provider(Arc::new(DatabaseDynamicFieldsProvider::<
        ServiceRuntimeExecutor,
    >::new(
        "other-profile", [definitions.clone()]
    )?));
    let before_provenance_rejection = observed.counts();
    let rejected = extended
        .clone()
        .audit_as("reject a held view in another profile")
        .save(&context)
        .await
        .unwrap_err();
    assert!(
        rejected
            .to_string()
            .contains("DYNAMIC_FIELD_STORAGE_PROVENANCE_MISMATCH")
    );
    assert_eq!(
        before_provenance_rejection,
        observed.counts(),
        "held storage provenance must reject before executor entry"
    );
    assert_eq!(extended.version(), held_version);
    assert!(extended.has_pending_dynamic_mutations());
    context.set_dynamic_fields_provider(Arc::new(DatabaseDynamicFieldsProvider::<
        ServiceRuntimeExecutor,
    >::new("example", [definitions])?));
    let stored = Q::schools()
        .with_id_is(changed_id)
        .select_dynamic_fields_with(DynamicFieldSelection::All)
        .limit(1)
        .comment("what: inspect original extension after rejected save")
        .purpose("why: prove a changed profile cannot redirect held data")
        .execute_for_one(&context)
        .await?
        .unwrap();
    assert_eq!(stored.version(), held_version);
    assert_eq!(
        stored
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .value(),
        Some(&Value::Text("Private extension".into()))
    );
    extended.update_dynamic_field("note", Value::Null)?;
    let before_valid_extension_save = observed.counts();
    let mut extended = extended
        .audit_as("persist explicit extension null")
        .save(&context)
        .await?;
    assert_ne!(
        before_valid_extension_save,
        observed.counts(),
        "positive control: a valid extension save must enter the executor"
    );
    assert_eq!(
        extended
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .state(),
        DynamicFieldState::Null
    );
    extended.delete_dynamic_field("note")?;
    let extended = extended
        .audit_as("remove the stored extension without confusing null")
        .save(&context)
        .await?;
    assert_eq!(
        extended
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .state(),
        DynamicFieldState::NotLoaded
    );
    println!("PASS generated Rust dynamic storage provenance and retry");
    println!(
        "PASS generated Rust LF20 held provenance rejects before provider with valid-save positive control"
    );
    let extended = Box::pin(dynamic_rollback::verify(
        &context, extended, &renamed, &second,
    ))
    .await?;
    let deletion_name = extended.name().to_owned();
    let extended = Box::pin(namespace_cow::verify(
        &context,
        extended.id(),
        &deletion_name,
        &second,
    ))
    .await?;
    let mut deleted = extended;
    deleted.mark_for_deletion();
    deleted
        .audit_as("soft delete only the selected probe")
        .save(&context)
        .await?;
    let remaining = Q::schools()
        .with_name_in([deletion_name.as_str(), second.as_str()])
        .order_by_id_asc()
        .limit(2)
        .comment("what: read the surviving probe after soft deletion")
        .purpose("why: prove isolated graph mutation and normal deleted-row exclusion")
        .execute_for_list(&context)
        .await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].name(), second);
    // The combined control explicitly seeded NULL on this companion once.
    // Its later sibling mutation/rollback/retry must not add another version.
    assert_eq!(remaining[0].version(), full[1].version() + 1);
    println!(
        "PASS generated indexed Q/E/Checker/create/update/delete and snapshot sharing {round}"
    );
    Ok(())
}
