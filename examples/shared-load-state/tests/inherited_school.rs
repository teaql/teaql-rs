#![cfg(feature = "inheritance")]

use school_management_service_core::{
    service_runtime, Academy, AuditedSave, School, ServiceRuntimeConfig, E, Q,
};
use std::sync::Arc;
use teaql_core::{time::Timestamp, Entity, TeaqlEntity, Value};

#[test]
fn inherited_fixed_layout_preserves_parent_positions_and_type_identity() {
    let parent = School::field_layout().unwrap().unwrap();
    let child = Academy::field_layout().unwrap().unwrap();
    for (name, index) in School::__TEAQL_FIXED_FIELD_INDEXES {
        assert_eq!(child.index(name), Some(*index), "inherited field {name}");
    }
    assert!(child.index("campus_code").unwrap() >= School::__TEAQL_FIXED_FIELD_INDEXES.len());
    assert_eq!(
        child.field_count(),
        Academy::entity_descriptor().properties.len()
    );
    assert_ne!(child.revision(), parent.revision());
    assert!(!std::sync::Arc::ptr_eq(&parent, &child));
    assert!(Academy::entity_descriptor()
        .audit_mask_fields
        .iter()
        .any(|field| field == "address"));
}

#[tokio::test]
async fn inherited_query_expression_mutation_keeps_shared_state_private(
) -> Result<(), Box<dyn std::error::Error>> {
    let round = std::env::var("TEAQL_LOAD_STATE_ROUND")?;
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: std::env::var("TEAQL_LOAD_STATE_DATABASE")?,
    })
    .await?;
    context.ensure_schema().await?;
    let codes = [format!("{round}-campus-a"), format!("{round}-campus-b")];
    for code in &codes {
        let mut row = Q::academies()
            .comment("what: create a controlled inherited entity")
            .purpose("why: verify complete generated inherited fields")
            .new_entity(&context);
        row.update_platform_id(1_u64);
        row.update_school_type_to_primary();
        row.update_name(code.as_str());
        row.update_address("Private inherited address");
        row.update_established_date(Value::Date("1995-09-01".parse()?));
        row.update_student_capacity(0_i64);
        row.update_active(false);
        row.update_create_time(Timestamp(1_700_000_000_000));
        row.update_update_time(Timestamp(1_700_000_000_000));
        row.update_campus_code(code.as_str());
        row.audit_as("create inherited state fixture")
            .save(&context)
            .await?;
    }
    let full = Q::academies()
        .with_campus_code_in(codes.iter().map(String::as_str))
        .select_self_fields()
        .order_by_id_asc()
        .limit(2)
        .comment("what: load two full inherited rows")
        .purpose("why: inspect actual shared snapshots")
        .execute_for_list(&context)
        .await?;
    assert_eq!(full.len(), 2);
    let snapshot = full[0].loaded_state_snapshot().unwrap();
    assert!(Arc::ptr_eq(
        &snapshot,
        &full[1].loaded_state_snapshot().unwrap()
    ));
    assert_eq!(
        E::academy(&full[0]).get_name().eval().as_deref(),
        Some(codes[0].as_str())
    );
    assert_eq!(
        E::academy(&full[0]).get_campus_code().eval().as_deref(),
        Some(codes[0].as_str())
    );
    assert_eq!(E::academy(&full[0]).get_student_capacity().eval(), Some(0));
    assert_eq!(E::academy(&full[0]).get_active().eval(), Some(false));
    if std::env::var("TEAQL_LOAD_STATE_WIDE").as_deref() == Ok("true") {
        for slot in [63, 64, 65, 129] {
            let (name, _) = School::__TEAQL_FIXED_FIELD_INDEXES
                .iter()
                .find(|(_, index)| *index == slot)
                .unwrap();
            assert!(snapshot.is_loaded(name));
            assert_eq!(full[0].clone().into_values().get(*name), Some(&Value::Null));
        }
    }
    let id = full[0].id();
    let sparse = Q::academies_minimal()
        .with_id_is(id)
        .select_campus_code()
        .limit(1)
        .comment("what: load an incomplete inherited projection")
        .purpose("why: retain full-object Checker enforcement")
        .execute_for_one(&context)
        .await?
        .unwrap();
    assert!(!sparse.is_loaded("name"));
    assert!(sparse
        .audit_as("reject sparse inherited save")
        .save(&context)
        .await
        .is_err());
    let mut changed = full[0].clone();
    let sibling_version = full[1].version();
    let renamed = format!("{round} renamed inherited entity");
    changed.update_name(renamed.as_str());
    assert!(Arc::ptr_eq(
        &snapshot,
        &changed.loaded_state_snapshot().unwrap()
    ));
    changed
        .audit_as("change only one inherited object")
        .save(&context)
        .await?;
    assert_eq!(full[1].version(), sibling_version);
    assert_eq!(
        E::academy(&full[1]).get_name().eval().as_deref(),
        Some(codes[1].as_str())
    );
    let mut stored = Q::academies()
        .with_id_is(id)
        .select_self_fields()
        .limit(1)
        .comment("what: read the inherited update")
        .purpose("why: verify persisted values and optimistic version")
        .execute_for_one(&context)
        .await?
        .unwrap();
    assert_eq!(stored.version(), 2);
    assert_eq!(
        E::academy(&stored).get_name().eval().as_deref(),
        Some(renamed.as_str())
    );
    stored.mark_for_deletion();
    stored
        .audit_as("delete only the changed inherited object")
        .save(&context)
        .await?;
    let remaining = Q::academies()
        .with_campus_code_in(codes.iter().map(String::as_str))
        .limit(2)
        .comment("what: inspect the independent inherited sibling")
        .purpose("why: verify graph isolation after deletion")
        .execute_for_list(&context)
        .await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].version(), sibling_version);
    println!("PASS generated Rust inherited indexes Q/E/save and snapshot isolation {round}");
    Ok(())
}
