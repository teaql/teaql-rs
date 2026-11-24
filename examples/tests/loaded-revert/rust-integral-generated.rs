//! Application-owned original scalar-model regression; current Q/create/E Assist is retained.
use scalar_type_conformance_service_core::{
    AuditedSave, E, Q, ScalarTypeProbe, service_runtime_from_pool,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, Value, time::Timestamp};
use teaql_runtime::{
    CheckResults, Checker, EntityValues, InMemoryCheckerRegistry, ObjectLocation, UserContext,
};

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);
fn count_statement(_: &str) {
    STATEMENTS.fetch_add(1, Ordering::SeqCst);
}
pub(crate) fn prepared(context: &UserContext, name: &str) -> ScalarTypeProbe {
    let mut entity = Q::scalar_type_probes()
        .comment("prepare exact integral regression")
        .purpose("validate generated audited save before provider entry")
        .new_entity(context);
    entity.update_type_platform_id(1);
    entity.update_required_string(name.to_owned());
    entity.update_optional_string(String::new());
    entity.update_large_text("Unicode 雪".to_owned());
    entity.update_required_boolean(true);
    entity.update_optional_boolean(false);
    entity.update_required_integer(i32::MAX as i64);
    entity.update_optional_integer(i32::MIN as i64);
    entity.update_required_long(i64::MAX);
    entity.update_optional_long(i64::MIN);
    entity.update_required_decimal(
        "123456789012.3456789"
            .parse::<rust_decimal::Decimal>()
            .unwrap(),
    );
    entity.update_optional_decimal("3.50".parse::<rust_decimal::Decimal>().unwrap());
    entity.update_required_float(1.5_f32);
    entity.update_optional_float(-3.5_f32);
    entity.update_required_double(2.25_f64);
    entity.update_optional_double(-0.125_f64);
    entity.update_required_date(chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap());
    entity.update_optional_date(chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap());
    entity.update_required_time("00:00:00".to_owned());
    entity.update_optional_time("23:59:59.123456".to_owned());
    entity.update_required_datetime(Timestamp(1_709_210_096_789));
    entity.update_optional_datetime(Some(Timestamp(1_735_689_600_000)));
    entity
}
pub(crate) async fn load(context: &UserContext, id: u64) -> ScalarTypeProbe {
    Q::scalar_type_probes()
        .with_id_is(id)
        .limit(1)
        .comment("verify persisted integral row")
        .purpose("compare generated E and authoritative SQLite storage")
        .execute_for_one(context)
        .await
        .unwrap()
        .unwrap()
}
struct FractionalFix {
    field: &'static str,
    value: Value,
    calls: Arc<AtomicUsize>,
}
impl Checker for FractionalFix {
    fn entity(&self) -> &str {
        "ScalarTypeProbe"
    }
    fn check_and_fix(
        &self,
        context: &UserContext,
        input: &mut EntityValues,
        _: &ObjectLocation,
        _: &mut CheckResults,
    ) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        // Case labels retain the native carrier name used by historical
        // evidence; the public Checker/Fix map itself uses KSML vocabulary.
        let canonical = if self.field == "type_platform_id" { "type_platform" } else { self.field };
        input.insert(canonical.to_owned(), self.value.clone());
        let now = context.fix_time();
        input
            .entry("create_time".into())
            .or_insert(Value::Timestamp(now));
        input.insert("update_time".into(), Value::Timestamp(now));
    }
}

#[tokio::test]
async fn generated_integral_exactness_after_fix_preserves_database_and_version() {
    let round = std::env::var("TEAQL_PROBE_ROUND").unwrap();
    let pool = Arc::new(Mutex::new(
        rusqlite::Connection::open(std::env::var("TEAQL_INTEGRAL_DB").unwrap()).unwrap(),
    ));
    let mut context = service_runtime_from_pool(pool.clone()).await.unwrap();
    context.ensure_schema().await.unwrap();
    pool.lock().unwrap().trace(Some(count_statement));
    let before = STATEMENTS.load(Ordering::SeqCst);
    let name = format!("Exact integral round {round}");
    let control_calls = Arc::new(AtomicUsize::new(0));
    context.set_checker_registry(InMemoryCheckerRegistry::new().with_checker(FractionalFix {
        field: "required_long",
        value: Value::I64(i64::MAX),
        calls: control_calls.clone(),
    }));
    let persisted = prepared(&context, &name)
        .audit_as("create exact integral control")
        .save(&context)
        .await
        .unwrap();
    assert_eq!(control_calls.load(Ordering::SeqCst), 1);
    println!(
        "RUST_INTEGRAL_FIX_CONTROL={}",
        serde_json::json!({"saved":true,"fixCalls":1,"providerStatements":STATEMENTS.load(Ordering::SeqCst)-before})
    );
    context = service_runtime_from_pool(pool.clone()).await.unwrap();
    let mut control = load(&context, persisted.id()).await;
    assert_eq!(
        E::scalar_type_probe(&control).get_required_long().eval(),
        Some(i64::MAX)
    );
    assert_eq!(
        E::scalar_type_probe(&control).get_optional_long().eval(),
        Some(Some(i64::MIN))
    );
    control.update_required_integer(i32::MIN as i64);
    control.update_optional_integer(i32::MAX as i64);
    control
        .audit_as("update exact integral control")
        .save(&context)
        .await
        .unwrap();
    let observed = STATEMENTS.load(Ordering::SeqCst) - before;
    assert!(
        observed > 0,
        "SQLite observer must see actual generated create/read/update"
    );
    let mut failures = Vec::new();
    let mut count = 0;
    for status in ["create", "update"] {
        for field in [
            "required_integer",
            "optional_integer",
            "required_long",
            "optional_long",
            "type_platform_id",
        ] {
            for (case, text) in ["1.9", "-0.1", "9223372036854775807.1"]
                .into_iter()
                .enumerate()
            {
                let calls = Arc::new(AtomicUsize::new(0));
                context.set_checker_registry(InMemoryCheckerRegistry::new().with_checker(
                    FractionalFix {
                        field,
                        value: Value::Decimal(text.parse().unwrap()),
                        calls: calls.clone(),
                    },
                ));
                let mut entity = if status == "create" {
                    prepared(&context, "Rejected fractional integral")
                } else {
                    load(&context, persisted.id()).await
                };
                if status == "update" {
                    entity.update_required_string("Rejected integral rename".to_owned());
                }
                let before = STATEMENTS.load(Ordering::SeqCst);
                let result = entity
                    .audit_as("reject fractional post-Fix integral")
                    .save(&context)
                    .await;
                let statements = STATEMENTS.load(Ordering::SeqCst) - before;
                let fix_calls = calls.load(Ordering::SeqCst);
                let rejected = result.is_err();
                println!(
                    "RUST_INTEGRAL_CASE={}",
                    serde_json::json!({"status":status,"field":field,"case":case,
                    "rejected":rejected,"providerStatements":statements,"fixCalls":fix_calls,"observerCalibration":observed})
                );
                if !rejected || statements != 0 || fix_calls != 1 {
                    failures.push(format!("{status}/{field}/{case}: rejected={rejected} statements={statements} fixes={fix_calls}"));
                }
                count += 1;
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    let stored = load(&context, persisted.id()).await;
    assert_eq!(E::scalar_type_probe(&stored).get_version().eval(), Some(2));
    assert_eq!(
        E::scalar_type_probe(&stored).get_required_string().eval(),
        Some(name)
    );
    assert_eq!(
        E::scalar_type_probe(&stored).get_required_integer().eval(),
        Some(i32::MIN as i64)
    );
    assert_eq!(
        E::scalar_type_probe(&stored).get_optional_integer().eval(),
        Some(Some(i32::MAX as i64))
    );
    assert_eq!(
        E::scalar_type_probe(&stored).get_required_long().eval(),
        Some(i64::MAX)
    );
    assert_eq!(
        E::scalar_type_probe(&stored).get_optional_long().eval(),
        Some(Some(i64::MIN))
    );
    assert_eq!(
        E::scalar_type_probe(&stored)
            .get_required_decimal()
            .eval()
            .unwrap()
            .to_string(),
        "123456789012.3456789"
    );
    println!(
        "RUST_INTEGRAL_GENERATED={}",
        serde_json::json!({"passed":true,"cases":count,"version":2,"round":round,
        "generatedQ":true,"generatedE":true,"auditedSave":true,"customFixOnce":true,"sameDatabase":true})
    );
}
