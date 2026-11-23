//! Application-owned probe over three evaluated generated libraries.
use scalar_type_conformance_service_core::{E, Q, service_runtime_from_pool};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);
fn observe_statement(_: rusqlite::trace::TraceEvent<'_>) {
    STATEMENTS.fetch_add(1, Ordering::Relaxed);
}

#[tokio::main]
async fn main() {
    let profile = std::env::var("TEAQL_EXPRESSION_PROFILE").unwrap();
    let round: u32 = std::env::var("TEAQL_EXPRESSION_ROUND")
        .unwrap()
        .parse()
        .unwrap();
    let pointer = match profile.as_str() {
        "camelCase" => "/scalarTypeProbeList",
        "snake_case" => "/scalar_type_probe_list",
        "PascalCase" => "/ScalarTypeProbeList",
        _ => panic!("unsupported model profile"),
    };
    let attempted = match profile.as_str() {
        "camelCase" => "/scalarTypeProbeList/0/requiredString",
        "snake_case" => "/scalar_type_probe_list/0/required_string",
        "PascalCase" => "/ScalarTypeProbeList/0/RequiredString",
        _ => unreachable!(),
    };
    let mut connection =
        rusqlite::Connection::open(std::env::var("TEAQL_EXPRESSION_DB").unwrap()).unwrap();
    connection
        .trace_v2(
            rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
            Some(observe_statement),
        )
        .unwrap();
    let pool = Arc::new(Mutex::new(connection));
    let context = service_runtime_from_pool(pool).await.unwrap();
    context.ensure_schema().await.unwrap();
    STATEMENTS.store(0, Ordering::Relaxed);
    let row = Q::type_platforms_minimal()
        .with_id_is(1)
        .select_name()
        .limit(1)
        .comment("Load the root without its reverse list")
        .purpose("Verify the actual generated E API uses the selected model JSON profile")
        .execute_for_one(&context)
        .await
        .unwrap()
        .expect("ensure_schema must seed the root");
    assert_eq!(row.id(), 1);
    let calibrated = STATEMENTS.load(Ordering::Relaxed);
    assert!(
        calibrated > 0,
        "the provider observer must measure the real generated Q query"
    );
    let name_before = E::type_platform(&row).get_name().try_eval().unwrap();
    let error = E::type_platform(&row)
        .get_scalar_type_probe_list()
        .first()
        .get_required_string()
        .try_eval()
        .unwrap_err();
    let wire = error.to_wire();
    let canonical = serde_json::json!([{"kind":"property","name":"scalar_type_probe_list"}]);
    assert_eq!(serde_json::to_value(&wire.location).unwrap(), canonical);
    assert_eq!(
        serde_json::to_value(&wire.suggested_fix.location).unwrap(),
        canonical
    );
    assert_eq!(wire.instance_path, pointer);
    assert_eq!(wire.attempted_instance_path, attempted);
    assert_eq!(wire.break_point, "scalar_type_probe_list");
    assert_eq!(wire.break_point_index, 0);
    assert_eq!(
        serde_json::to_value(&wire.attempted_location).unwrap(),
        serde_json::json!([
        {"kind":"property","name":"scalar_type_probe_list"},{"kind":"index","index":0},
        {"kind":"property","name":"required_string"}])
    );
    assert_eq!(
        wire.original_expression.as_deref(),
        Some(
            "E::type_platform(<root>).get_scalar_type_probe_list().first().get_required_string().try_eval()"
        )
    );
    assert_eq!(
        wire.expression_with_broken_point.as_deref(),
        Some(
            "E::type_platform(<root>).get_scalar_type_probe_list()<broken>.first().get_required_string().try_eval()"
        )
    );
    let human = error.to_string();
    assert!(human.starts_with("TeaQLNotLoadedError:"));
    assert!(human.contains("break_point=scalar_type_probe_list"));
    assert!(human.contains(wire.expression_with_broken_point.as_ref().unwrap()));
    assert!(!human.contains("\"instancePath\""));
    assert_eq!(STATEMENTS.load(Ordering::Relaxed), calibrated);
    assert_eq!(
        E::type_platform(&row).get_name().try_eval().unwrap(),
        name_before
    );
    println!(
        "GENERATED_E_MODEL_PROFILE={}",
        serde_json::json!({
            "language":"rust","profile":profile,"round":round,"passed":true,"wire":wire,"human":human,
            "providerEntries":0,"queryCalibration":calibrated,"selectedNameUnchanged":true
        })
    );
}
