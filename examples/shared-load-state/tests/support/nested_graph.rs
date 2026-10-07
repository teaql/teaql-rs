//! Application-owned graph acceptance. API names come from retained current Assist.
use school_management_service_core::{AuditedSave, TeaqlRuntime, E, Q};
use std::sync::Arc;
use teaql_core::Entity;
use teaql_runtime::LoadedRelation;

pub async fn verify_dynamic(
    context: &impl TeaqlRuntime,
    school_id: u64,
    renamed: &str,
    second: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use teaql_core::dynamic_fields::{DynamicFieldSelection, DynamicFieldState};
    use teaql_core::{time::Timestamp, Value};
    let null_name = format!("{renamed} Dynamic NULL Probe");
    let mut probe = Q::schools()
        .comment("what: allocate a separate nullable extension probe")
        .purpose("why: keep the ordinary sibling mutation-isolation test unchanged")
        .new_entity(context);
    probe.update_platform_id(1);
    probe.update_school_type_to_primary();
    probe.update_name(null_name.as_str());
    probe.update_address("Controlled extension probe");
    probe.update_established_date(Value::Date("1995-09-01".parse()?));
    probe.update_student_capacity(0);
    probe.update_active(false);
    probe.update_create_time(Timestamp(1_700_000_000_000));
    probe.update_update_time(Timestamp(1_700_000_000_000));
    let created = probe
        .audit_as("create isolated nested extension probe")
        .save(context)
        .await?;
    let mut probe = Q::schools()
        .with_id_is(created.id())
        .select_dynamic_fields_with(DynamicFieldSelection::All)
        .limit(1)
        .comment("what: load trusted definitions for the NULL probe")
        .purpose("why: bind extension mutation to the configured storage")
        .execute_for_one(context)
        .await?
        .unwrap();
    probe.update_dynamic_field("note", Value::Null)?;
    probe
        .audit_as("seed explicit NULL through the generated mutation API")
        .save(context)
        .await?;

    let platform = Q::platforms_minimal()
        .with_id_is(1)
        .select_school_list_with(
            Q::schools_minimal()
                .with_name_in([renamed, second, null_name.as_str()])
                .select_name()
                .select_dynamic_fields_with(DynamicFieldSelection::All)
                .order_by_id_asc()
                .limit(3),
        )
        .limit(1)
        .comment("what: load Value, absent and NULL extensions on bounded children")
        .purpose("why: verify graph hydration retains row-owned dynamic field carriers")
        .execute_for_one(context)
        .await?
        .unwrap();
    let handle = platform.school_list();
    let rows = handle.value().unwrap();
    assert_eq!(rows.len(), 3);
    let values = rows[0]
        .dynamic_field_values()
        .expect("selected child extension carrier");
    assert_eq!(
        values.field("note")?.value(),
        Some(&Value::Text("Private extension".into()))
    );
    assert_eq!(
        rows[1]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .state(),
        DynamicFieldState::NotLoaded
    );
    assert_eq!(
        rows[2]
            .dynamic_field_values()
            .unwrap()
            .field("note")?
            .state(),
        DynamicFieldState::Null
    );
    assert!(Arc::ptr_eq(
        &rows[0].loaded_state_snapshot().unwrap(),
        &rows[2].loaded_state_snapshot().unwrap()
    ));
    assert!(!Arc::ptr_eq(
        &rows[0].loaded_state_snapshot().unwrap(),
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    let json = platform.clone().into_json();
    assert_eq!(json["school_list"][0]["#note"], "Private extension");
    assert!(json["school_list"][1].get("#note").is_none());
    assert!(json["school_list"][2].get("#note").unwrap().is_null());

    let nested = Q::schools_minimal()
        .with_id_is(school_id)
        .select_platform_with(
            Q::platforms_minimal().select_school_list_with(
                Q::schools_minimal()
                    .with_name_in([renamed, null_name.as_str()])
                    .select_name()
                    .select_dynamic_fields_with(DynamicFieldSelection::All)
                    .order_by_id_asc()
                    .limit(2),
            ),
        )
        .limit(1)
        .comment("what: retain dynamic child values through a forward ancestor")
        .purpose("why: verify nested flat identity graph presentation")
        .execute_for_one(context)
        .await?
        .unwrap();
    let json = nested.into_json();
    assert_eq!(
        json["platform"]["school_list"][0]["#note"],
        "Private extension"
    );
    assert!(json["platform"]["school_list"][1]
        .get("#note")
        .unwrap()
        .is_null());
    println!("PASS generated Rust nested dynamic Value/NULL/NotLoaded and shared snapshots");
    Ok(())
}

pub async fn verify(
    context: &impl TeaqlRuntime,
    school_id: u64,
    renamed: &str,
    second: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let platform = Q::platforms_minimal()
        .with_id_is(1)
        .select_school_list_with(
            Q::schools_minimal()
                .with_name_in([renamed, second])
                .select_name()
                .order_by_id_asc()
                .limit(2),
        )
        .limit(1)
        .comment("what: load two selected Schools under the seeded Platform")
        .purpose("why: verify generated reverse graph state and JSON")
        .execute_for_one(context)
        .await?
        .unwrap();
    let handle = platform.school_list();
    assert_eq!(handle.state(), LoadedRelation::Loaded);
    let children = handle.value().unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(
        E::school(&children[0]).get_name().eval().as_deref(),
        Some(renamed)
    );
    assert_eq!(
        E::school(&children[1]).get_name().eval().as_deref(),
        Some(second)
    );
    let state = children[0].loaded_state_snapshot().unwrap();
    assert!(Arc::ptr_eq(
        &state,
        &children[1].loaded_state_snapshot().unwrap()
    ));
    let json = platform.clone().into_json();
    assert_eq!(json["school_list"][0]["name"], renamed);
    assert_eq!(json["school_list"][1]["name"], second);
    assert!(json["school_list"][0].get("address").is_none());
    assert!(json["school_list"][0].get("_original_values").is_none());
    assert!(Arc::ptr_eq(
        &state,
        &children[0].loaded_state_snapshot().unwrap()
    ));

    let nested = Q::schools_minimal()
        .with_id_is(school_id)
        .select_platform_with(
            Q::platforms_minimal().select_school_list_with(
                Q::schools_minimal()
                    .with_name_in([renamed, second])
                    .select_name()
                    .order_by_id_asc()
                    .limit(2),
            ),
        )
        .limit(1)
        .comment("what: load School to Platform to its bounded School list")
        .purpose("why: verify availability survives a forward ancestor and reverse descendants")
        .execute_for_one(context)
        .await?
        .unwrap();
    let parent = E::school(&nested).get_platform().eval().unwrap();
    assert_eq!(parent.school_list().state(), LoadedRelation::Loaded);
    assert_eq!(parent.school_list().value().unwrap().len(), 2);
    let nested_json = nested.into_json();
    assert_eq!(nested_json["platform"]["school_list"][0]["name"], renamed);
    assert_eq!(nested_json["platform"]["school_list"][1]["name"], second);

    let empty = Q::school_types_minimal()
        .with_id_is(1002)
        .select_school_list_with(
            Q::schools_minimal()
                .select_name()
                .order_by_id_asc()
                .limit(2),
        )
        .limit(1)
        .comment("what: explicitly load the unused Secondary constant's Schools")
        .purpose("why: distinguish loaded empty from omitted reverse detail")
        .execute_for_one(context)
        .await?
        .unwrap();
    assert_eq!(empty.school_list().state(), LoadedRelation::Empty);
    assert!(empty.school_list().value().unwrap().is_empty());
    assert!(empty.into_json()["school_list"]
        .as_array()
        .unwrap()
        .is_empty());

    let unselected = Q::school_types_minimal()
        .with_id_is(1002)
        .limit(1)
        .comment("what: load the same constant without selecting its Schools")
        .purpose("why: NotLoaded must not serialize as an empty list")
        .execute_for_one(context)
        .await?
        .unwrap();
    assert_eq!(unselected.school_list().state(), LoadedRelation::NotLoaded);
    assert!(unselected.school_list().value().is_none());
    assert!(unselected.into_json().get("school_list").is_none());

    let filtered = Q::schools_minimal()
        .with_id_is(school_id)
        .select_school_type_with(Q::school_types_minimal().with_id_is(1002))
        .limit(1)
        .comment("what: retain Primary FK while excluding it from selected target detail")
        .purpose("why: verify current filtered-forward NotLoaded semantics")
        .execute_for_one(context)
        .await?
        .unwrap();
    assert_eq!(E::school(&filtered).get_school_type_id().eval(), Some(1001));
    assert!(!filtered.is_field_loaded("school_type"));
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        E::school(&filtered).get_school_type().get_code().eval()
    }))
    .expect_err("NotLoaded expression access must fail fast, not return NULL");
    let diagnostic = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(diagnostic.contains("school_type") && diagnostic.contains("missing_preload"));
    assert!(filtered.into_json().get("school_type").is_none());
    println!("PASS generated Rust nested/reverse graph Q/E/JSON and Empty/NotLoaded isolation");
    Ok(())
}
