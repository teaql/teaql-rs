//! Generated Q/E read shapes; fixture expansion SQL is outside measurements.
#[path = "../../../teaql-runtime/tests/support/allocation_counter.rs"]
mod allocation_counter;
use allocation_counter::measured;
use school_management_service_core::{
    service_runtime, AuditedSave, Platform, School, ServiceRuntimeConfig, ServiceRuntimeExecutor,
    E, Q,
};
use std::sync::Arc;
use teaql_core::{
    dynamic_fields::{DynamicFieldDefinitions, DynamicFieldSelection, DynamicFieldState},
    time::Timestamp,
    DataType, Entity, SmartList, TeaqlEntity, Value,
};
use teaql_runtime::{dynamic_fields::DatabaseDynamicFieldsProvider, UserContext};

type Error = Box<dyn std::error::Error>;
enum Output {
    Schools(SmartList<School>),
    Parent(Platform),
}
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn expand_fixture(database: &str) -> Result<(), Error> {
    let mut connection = rusqlite::Connection::open(database)?;
    let columns = connection
        .prepare("PRAGMA table_info(school_data)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    assert!(columns.len() > 130, "requires the existing wide fixture");
    assert!(columns.iter().any(|c| c == "id") && columns.iter().any(|c| c == "name"));
    let target = columns
        .iter()
        .map(|c| quoted(c))
        .collect::<Vec<_>>()
        .join(",");
    let select = columns
        .iter()
        .map(|c| match c.as_str() {
            "id" => "?1".to_owned(),
            "name" => "?2".to_owned(),
            _ => quoted(c),
        })
        .collect::<Vec<_>>()
        .join(",");
    let transaction = connection.transaction()?;
    {
        let mut insert = transaction.prepare(&format!(
            "INSERT INTO school_data({target}) SELECT {select} FROM school_data WHERE id=1"
        ))?;
        for id in 2_i64..=10_000 {
            assert_eq!(
                insert.execute(rusqlite::params![id, format!("bench-school-{id}")])?,
                1
            );
        }
    }
    transaction.commit()?;
    Ok(())
}
fn query(context: &UserContext, shape: &str, count: usize) -> Result<Output, Error> {
    futures_executor::block_on(async {
        if shape == "reverse" {
            let parent = Q::platforms_minimal()
                .with_id_is(1)
                .select_school_list_with(
                    Q::schools_minimal()
                        .select_name()
                        .order_by_id_asc()
                        .limit(count as u64),
                )
                .limit(1)
                .comment("what: load bounded generated reverse graph")
                .purpose("why: measure relation hydration and shared state")
                .execute_for_one(context)
                .await?
                .ok_or("missing benchmark Platform")?;
            return Ok(Output::Parent(parent));
        }
        let mut request = if matches!(shape, "wide" | "dynamic") {
            Q::schools().select_self_fields()
        } else {
            Q::schools_minimal().select_name()
        };
        if shape == "dynamic" {
            request = request.select_dynamic_fields_with(DynamicFieldSelection::fields([(
                "note".into(),
                DataType::Text,
            )])?);
        }
        if shape == "forward" {
            request = request
                .select_platform_with(Q::platforms_minimal().select_name())
                .select_school_type_with(Q::school_types_minimal().select_code());
        }
        Ok(Output::Schools(
            request
                .order_by_id_asc()
                .limit(count as u64)
                .comment("what: load bounded generated benchmark Schools")
                .purpose("why: measure indexed state and row construction")
                .execute_for_list(context)
                .await?,
        ))
    })
}
fn validate_rows(rows: &SmartList<School>, shape: &str, count: usize) -> Result<(), Error> {
    assert_eq!(rows.len(), count);
    let first = rows[0].loaded_state_snapshot().unwrap();
    let missing = if shape == "dynamic" && count > 2 {
        rows[2].loaded_state_snapshot()
    } else {
        None
    };
    for (index, row) in rows.iter().enumerate() {
        let id = index as u64 + 1;
        assert_eq!(row.id(), id);
        assert_eq!(row.version(), if id <= 2 { 2 } else { 1 });
        assert_eq!(
            E::school(row).get_name().eval().as_deref(),
            Some(format!("bench-school-{id}").as_str())
        );
        assert!(row.dirty_fields().is_none());
        let state = row.loaded_state_snapshot().unwrap();
        let expected = if shape == "dynamic" && index >= 2 {
            missing.as_ref().unwrap()
        } else {
            &first
        };
        assert!(
            Arc::ptr_eq(&state, expected),
            "same actual projection must share state"
        );
        if matches!(shape, "wide" | "dynamic") {
            assert_eq!(E::school(row).get_student_capacity().eval(), Some(17));
            assert_eq!(E::school(row).get_active().eval(), Some(false));
            assert_eq!(
                E::school(row).get_established_date().eval(),
                Some("1995-09-01".parse()?)
            );
            for (name, slot) in School::__TEAQL_FIXED_FIELD_INDEXES {
                if !state.layout().is_relation(name) {
                    assert!(row.is_field_loaded(name), "missing {name}");
                }
                if *slot < 64 {
                    assert_ne!(state.bits() & (1_u64 << slot), 0);
                } else {
                    assert!(state.overflow().unwrap().contains(slot));
                }
            }
        } else {
            assert!(!row.is_field_loaded("address"));
            assert!(!row.is_field_loaded("student_capacity"));
        }
        if shape == "forward" {
            assert_eq!(
                E::school(row).get_platform().get_name().eval().as_deref(),
                Some("Campus Learning Platform")
            );
            assert_eq!(
                E::school(row)
                    .get_school_type()
                    .get_code()
                    .eval()
                    .as_deref(),
                Some("PRIMARY")
            );
        }
        if shape == "dynamic" {
            let values = row
                .dynamic_field_values()
                .ok_or("missing selected dynamic carrier")?;
            let field = values.field("note")?;
            assert_eq!(
                field.state(),
                match id {
                    1 => DynamicFieldState::Value,
                    2 => DynamicFieldState::Null,
                    _ => DynamicFieldState::NotLoaded,
                }
            );
            if id == 1 {
                assert_eq!(field.value(), Some(&Value::Text("benchmark note".into())));
            }
            assert!(state.layout().index("#note").is_none());
        }
    }
    if let Some(missing) = missing {
        assert!(!Arc::ptr_eq(&first, &missing));
    }
    Ok(())
}
fn validate(output: &Output, shape: &str, count: usize) -> Result<(), Error> {
    match output {
        Output::Schools(rows) => validate_rows(rows, shape, count),
        Output::Parent(parent) => {
            assert_eq!(parent.id(), 1);
            assert_eq!(parent.version(), 1);
            let handle = parent.school_list();
            validate_rows(
                handle.value().ok_or("reverse list NotLoaded")?,
                shape,
                count,
            )
        }
    }
}
fn report(shape: &str, count: usize, samples: &[(u64, u64, u128)]) {
    for (index, (calls, bytes, nanos)) in samples.iter().enumerate() {
        println!("GENERATED_SAMPLE,{shape},{count},{index},{nanos},{calls},{bytes}");
    }
    let mut times = samples.iter().map(|x| x.2).collect::<Vec<_>>();
    times.sort_unstable();
    let mut calls = samples.iter().map(|x| x.0).collect::<Vec<_>>();
    calls.sort_unstable();
    let mut bytes = samples.iter().map(|x| x.1).collect::<Vec<_>>();
    bytes.sort_unstable();
    let n = samples.len();
    let total = times.iter().sum::<u128>();
    println!(
        "GENERATED_SUMMARY,{shape},{count},{n},{},{},{:.2},{},{}",
        times[n / 2],
        times[(n * 95).div_ceil(100) - 1],
        n as f64 * 1e9 / total as f64,
        calls[n / 2],
        bytes[n / 2]
    );
}
#[test]
#[ignore = "opt-in generated wide benchmark; run with --release --ignored --nocapture"]
fn generated_wide_dynamic_and_relations() -> Result<(), Error> {
    let samples = std::env::var("TEAQL_GENERATED_SAMPLES")
        .unwrap_or_else(|_| "31".into())
        .parse::<usize>()?;
    assert!([3, 31].contains(&samples));
    let database = std::env::var("TEAQL_GENERATED_DATABASE")?;
    assert!(
        !std::path::Path::new(&database).exists(),
        "use a fresh benchmark database"
    );
    let mut context = futures_executor::block_on(service_runtime(ServiceRuntimeConfig {
        database_url: database.clone(),
    }))?;
    let definitions = DynamicFieldDefinitions::new(
        <School as TeaqlEntity>::ENTITY_NAME,
        "generated-benchmark-v1",
        [("note".into(), DataType::Text)],
    )?;
    context.set_dynamic_fields_provider(Arc::new(DatabaseDynamicFieldsProvider::<
        ServiceRuntimeExecutor,
    >::new("generated-benchmark", [definitions])?));
    futures_executor::block_on(context.ensure_schema())?;
    futures_executor::block_on(Box::pin(async {
        let mut school = Q::schools()
            .comment("what: allocate generated benchmark prototype")
            .purpose("why: seed through generated audited mutation")
            .new_entity(&context);
        school.update_platform_id(1);
        school.update_school_type_to_primary();
        school.update_name("bench-school-1");
        school.update_address("Benchmark address");
        school.update_established_date(Value::Date("1995-09-01".parse()?));
        school.update_student_capacity(17);
        school.update_active(false);
        school.update_create_time(Timestamp(1_700_000_000_000));
        school.update_update_time(Timestamp(1_700_000_000_000));
        let saved = school
            .audit_as("create the generated benchmark prototype")
            .save(&context)
            .await?;
        assert_eq!(saved.id(), 1);
        Ok::<_, Error>(())
    }))?;
    expand_fixture(&database)?;
    futures_executor::block_on(Box::pin(async {
        for id in [1, 2] {
            let mut row = Q::schools()
                .with_id_is(id)
                .select_self_fields()
                .select_dynamic_fields_with(DynamicFieldSelection::All)
                .limit(1)
                .comment("what: load generated benchmark extension owner")
                .purpose("why: seed durable Value and NULL through audited save")
                .execute_for_one(&context)
                .await?
                .unwrap();
            row.update_dynamic_field(
                "note",
                if id == 1 {
                    "benchmark note".into()
                } else {
                    Value::Null
                },
            )?;
            row.audit_as("seed benchmark durable extension")
                .save(&context)
                .await?;
        }
        Ok::<_, Error>(())
    }))?;
    let log_mode = std::env::var("TEAQL_GENERATED_LOG_MODE").unwrap_or_else(|_| "off".into());
    assert!(matches!(log_mode.as_str(), "off" | "default"));
    if log_mode == "off" {
        context.disable_sql_log();
    }
    let layout = School::field_layout()?.unwrap();
    assert!(layout.field_count() > 130);
    println!("GENERATED_LOG_MODE,{log_mode}");
    println!("GENERATED_FIXED_FIELDS,{}", layout.field_count());
    for shape in ["sparse", "wide", "dynamic", "forward", "reverse"] {
        for count in [1, 100, 10000] {
            for _ in 0..5 {
                let output = query(&context, shape, count)?;
                validate(&output, shape, count)?;
            }
            let mut measurements = Vec::with_capacity(samples);
            for _ in 0..samples {
                let (output, calls, bytes, nanos) = measured(|| query(&context, shape, count));
                let output = output?;
                validate(&output, shape, count)?;
                measurements.push((calls, bytes, nanos));
                std::hint::black_box(&output);
            }
            report(shape, count, &measurements);
        }
    }
    println!("PASS generated Rust wide dynamic forward reverse Q/E sharing benchmark");
    Ok(())
}
