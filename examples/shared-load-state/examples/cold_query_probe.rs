//! One native process and one first generated query. No warmup or schema writes in query mode.
#[path = "../../../teaql-runtime/tests/support/allocation_counter.rs"]
mod allocation_counter;
use allocation_counter::measured;
use school_management_service_core::{service_runtime, AuditedSave, ServiceRuntimeConfig, E, Q};
use std::sync::Arc;
use teaql_core::{time::Timestamp, Entity, Value};

type Error = Box<dyn std::error::Error>;
fn main() -> Result<(), Error> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() != 2 || !matches!(arguments[0].as_str(), "prepare" | "query") {
        return Err("Usage: cold_query_probe prepare|query <database>".into());
    }
    let mode = std::env::var("TEAQL_COLD_LOG_MODE").unwrap_or_else(|_| "off".into());
    assert!(matches!(mode.as_str(), "off" | "default"));
    let database = &arguments[1];
    let prepare = arguments[0] == "prepare";
    assert_eq!(std::path::Path::new(database).exists(), !prepare);
    let (context, init_calls, init_bytes, init_ns) = measured(|| {
        futures_executor::block_on(service_runtime(ServiceRuntimeConfig {
            database_url: database.clone(),
        }))
    });
    let mut context = context?;
    if mode == "off" {
        context.disable_sql_log();
    }
    if prepare {
        futures_executor::block_on(context.ensure_schema())?;
        futures_executor::block_on(Box::pin(async {
            for name in ["cold-school-1", "cold-school-2"] {
                let mut school = Q::schools()
                    .comment("what: create a cold-query fixture School")
                    .purpose("why: prepare outside the cold measurement process")
                    .new_entity(&context);
                school.update_platform_id(1);
                school.update_school_type_to_primary();
                school.update_name(name);
                school.update_address("Cold fixture address");
                school.update_established_date(Value::Date("1995-09-01".parse()?));
                school.update_student_capacity(17);
                school.update_active(false);
                school.update_create_time(Timestamp(1_700_000_000_000));
                school.update_update_time(Timestamp(1_700_000_000_000));
                school
                    .audit_as("prepare the generated cold-query fixture")
                    .save(&context)
                    .await?;
            }
            Ok::<_, Error>(())
        }))?;
        println!("PASS prepared generated Rust cold-query fixture");
        return Ok(());
    }
    let (result, query_calls, query_bytes, query_ns) = measured(|| {
        futures_executor::block_on(
            Q::schools_minimal()
                .select_name()
                .order_by_id_asc()
                .limit(2)
                .comment("what: load the first bounded generated cold-query result")
                .purpose("why: separate process-cold startup from warmed query costs")
                .execute_for_list(&context),
        )
    });
    let rows = result?;
    assert_eq!(rows.len(), 2);
    let first = rows[0]
        .loaded_state_snapshot()
        .ok_or("missing indexed snapshot")?;
    assert!(Arc::ptr_eq(
        &first,
        &rows[1].loaded_state_snapshot().unwrap()
    ));
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.id(), index as u64 + 1);
        assert_eq!(row.version(), 1);
        assert_eq!(
            E::school(row).get_name().eval().as_deref(),
            Some(format!("cold-school-{}", index + 1).as_str())
        );
        assert!(!row.is_field_loaded("address"));
        assert!(row.dirty_fields().is_none());
    }
    println!("COLD_QUERY,{mode},{},{init_ns},{query_ns},{init_calls},{init_bytes},{query_calls},{query_bytes}", std::process::id());
    println!("PASS generated Rust process-cold Q/E and shared snapshot");
    Ok(())
}
