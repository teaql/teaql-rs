//! A readonly calculated property is persisted only by an explicit modeled target.
use school_management_service_core::{
    service_runtime, AuditedSave, School, SchoolCapacitySummary, ServiceRuntimeConfig, E, Q,
};
use std::sync::Arc;
use teaql_core::{Entity, TeaqlEntity};
use teaql_runtime::UserContext;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let database = format!(
        "{}.materialization",
        std::env::var("TEAQL_LOAD_STATE_DATABASE")?
    );
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: database.clone(),
    })
    .await?;
    context.ensure_schema().await?;
    let round = std::env::var("TEAQL_LOAD_STATE_ROUND")?;
    let name = format!("{round} materialization source");
    let sibling = format!("{round} materialization sibling");
    let mut id = 0;
    for name in [&name, &sibling] {
        let mut source = Q::schools()
            .comment("what: allocate the isolated capacity contributor")
            .purpose("why: keep reporting acceptance independent of the relation fixture")
            .new_entity(&context);
        source.update_platform_id(1);
        source.update_school_type_to_primary();
        source.update_name(name.as_str());
        source.update_address("Materialization source address");
        source.update_established_date(teaql_core::Value::Date("1995-09-01".parse()?));
        source.update_student_capacity(0);
        source.update_active(false);
        source.update_create_time(teaql_core::time::Timestamp(1_700_000_000_000));
        source.update_update_time(teaql_core::time::Timestamp(1_700_000_000_000));
        let saved = source
            .audit_as("create an isolated materialization contributor")
            .save(&context)
            .await?;
        if id == 0 {
            id = saved.id();
        }
    }
    Box::pin(crate::native_rollback::verify(
        &mut context, &database, &name, &sibling,
    ))
    .await?;
    Box::pin(verify(&context, id, &name, &sibling, &round)).await?;
    Ok(())
}

pub async fn verify(
    context: &UserContext,
    id: u64,
    name: &str,
    sibling: &str,
    round: &str,
) -> Result<School, Box<dyn std::error::Error>> {
    let mut source = Q::schools()
        .with_id_is(id)
        .select_self_fields()
        .limit(1)
        .comment("what: load the capacity source before its audited change")
        .purpose("why: retain complete Checker input")
        .execute_for_one(context)
        .await?
        .unwrap();
    source.update_student_capacity(37_i64);
    source
        .audit_as("set the nonzero materialization control")
        .save(context)
        .await?;
    let rows = Q::schools()
        .with_name_in([name, sibling])
        .select_self_fields()
        .order_by_id_asc()
        .limit(2)
        .comment("what: read the two capacity contributors")
        .purpose("why: calculate the bounded reporting snapshot with E")
        .execute_for_list(context)
        .await?;
    assert_eq!(rows.len(), 2);
    let total: i64 = rows
        .iter()
        .map(|row| E::school(row).get_student_capacity().eval().unwrap())
        .sum();
    assert_eq!(total, 37);
    let shared = rows[0].loaded_state_snapshot().unwrap();
    assert!(Arc::ptr_eq(
        &shared,
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    let mut json = rows[0].clone().into_json();
    json.as_object_mut()
        .unwrap()
        .insert("_total_capacity".into(), total.into());
    let mut view: School = context.decode_json_entity(&json)?;
    assert!(view.has_dynamic_property("_total_capacity"));
    assert!(!view.has_dynamic_property("_missing_total"));
    assert!(view.dynamic_property("_missing_total").is_none());
    assert!(view.dirty_fields().is_none());
    assert!(School::field_layout()?
        .unwrap()
        .index("_total_capacity")
        .is_none());
    view.update_active(true);
    assert!(!view.dirty_fields().unwrap().contains("_total_capacity"));
    view.audit_as("save a native change without persisting its readonly total")
        .save(context)
        .await?;

    let mut target = Q::school_capacity_summaries()
        .comment("what: allocate a modeled capacity summary")
        .purpose("why: explicitly persist the reporting result")
        .new_entity(context);
    target.update_platform_id(1_u64);
    target.update_name(format!("{round} capacity snapshot"));
    target.update_total_capacity(total);
    target.update_school_count(i64::try_from(rows.len())?);
    let target = target
        .audit_as("materialize the calculated total in its own entity")
        .save(context)
        .await?;
    let read = Q::school_capacity_summaries()
        .with_id_is(target.id())
        .select_self_fields()
        .limit(1)
        .comment("what: reload the persisted summary")
        .purpose("why: verify modeled values through E")
        .execute_for_one(context)
        .await?
        .unwrap();
    assert_eq!(
        E::school_capacity_summary(&read)
            .get_total_capacity()
            .eval(),
        Some(37)
    );
    assert_eq!(
        E::school_capacity_summary(&read).get_school_count().eval(),
        Some(2)
    );
    assert!(SchoolCapacitySummary::field_layout()?
        .unwrap()
        .index("total_capacity")
        .is_some());
    assert!(Arc::ptr_eq(
        &shared,
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    assert!(rows[1].dirty_fields().is_none());
    let source = Q::schools()
        .with_id_is(id)
        .select_self_fields()
        .limit(1)
        .comment("what: reload the source after materialization")
        .purpose("why: prove the readonly property never became stored source data")
        .execute_for_one(context)
        .await?
        .unwrap();
    assert!(source.dynamic_property("_total_capacity").is_none());
    assert_eq!(E::school(&source).get_student_capacity().eval(), Some(37));
    assert_eq!(E::school(&source).get_active().eval(), Some(true));
    println!(
        "PASS generated Rust LF20 readonly total persists only through modeled materialization"
    );
    Ok(source)
}
