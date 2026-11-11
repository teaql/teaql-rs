//! Application-owned consumer; generated fixture source is never edited.
#[path = "prepared.rs"]
mod exact;
use scalar_type_conformance_service_core::{AuditedSave, E, Q, service_runtime_from_pool};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, Value};
use teaql_runtime::{
    CheckResults, CheckRule, Checker, EntityValues, InMemoryCheckerRegistry, LedgerEntity,
    ObjectLocation, RuntimeError, UserContext,
};

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);
fn observe(_: &str) {
    STATEMENTS.fetch_add(1, Ordering::SeqCst);
}

struct BadBooleanFix {
    field: &'static str,
    value: Value,
    calls: Arc<AtomicUsize>,
}
impl Checker for BadBooleanFix {
    fn entity(&self) -> &str {
        "ScalarTypeProbe"
    }
    fn check_and_fix(
        &self,
        context: &UserContext,
        values: &mut EntityValues,
        _: &ObjectLocation,
        _: &mut CheckResults,
    ) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        values.insert(self.field.to_owned(), self.value.clone());
        let now = context.fix_time();
        values
            .entry("create_time".into())
            .or_insert(Value::Timestamp(now));
        values.insert("update_time".into(), Value::Timestamp(now));
    }
}

#[tokio::main]
async fn main() {
    let round = std::env::var("TEAQL_BOOLEAN_ROUND").unwrap();
    let database = std::env::var("TEAQL_BOOLEAN_DB").unwrap();
    let pool = Arc::new(Mutex::new(rusqlite::Connection::open(database).unwrap()));
    let context = service_runtime_from_pool(pool.clone()).await.unwrap();
    context.ensure_schema().await.unwrap();
    pool.lock().unwrap().trace_v2(rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT, Some(|event: rusqlite::trace::TraceEvent<'_>| { if let rusqlite::trace::TraceEvent::Stmt(statement, sql) = event { if sql.starts_with("--") { observe(sql); } else { observe(&statement.expanded_sql().expect("expanded SQLite trace SQL")); } } })).unwrap();
    let mut entity = exact::prepared(&context, &format!("Boolean round {round}"));
    entity.update_required_boolean(true);
    entity.update_optional_boolean(false);
    let mut saved = entity
        .audit_as("create a typed Boolean control")
        .save(&context)
        .await
        .unwrap();
    let id = saved.id();
    assert_eq!(E::scalar_type_probe(&saved).get_version().eval(), Some(1));
    assert_eq!(
        E::scalar_type_probe(&saved).get_required_boolean().eval(),
        Some(true)
    );
    let false_rows = Q::scalar_type_probes_minimal()
        .with_id_is(id)
        .which_are_not_optional_boolean()
        .select_optional_boolean()
        .limit(1)
        .comment("load the false Boolean control")
        .purpose("verify generated false predicate")
        .execute_for_list(&context)
        .await
        .unwrap();
    assert_eq!(false_rows.len(), 1);
    assert_eq!(
        E::scalar_type_probe(&false_rows[0])
            .get_optional_boolean()
            .try_eval()
            .unwrap(),
        Some(Some(false))
    );

    saved.update_required_boolean(false);
    saved.update_optional_boolean(true);
    let mut saved = saved
        .audit_as("update typed Boolean controls")
        .save(&context)
        .await
        .unwrap();
    assert_eq!(E::scalar_type_probe(&saved).get_version().eval(), Some(2));
    assert_eq!(
        E::scalar_type_probe(&saved).get_required_boolean().eval(),
        Some(false)
    );
    let true_rows = Q::scalar_type_probes_minimal()
        .with_id_is(id)
        .which_are_optional_boolean()
        .select_optional_boolean()
        .limit(1)
        .comment("load the true Boolean control")
        .purpose("verify generated true predicate")
        .execute_for_list(&context)
        .await
        .unwrap();
    assert_eq!(true_rows.len(), 1);
    assert_eq!(
        E::scalar_type_probe(&true_rows[0])
            .get_optional_boolean()
            .try_eval()
            .unwrap(),
        Some(Some(true))
    );

    saved.update_optional_boolean(Value::Null);
    let saved = saved
        .audit_as("clear the nullable Boolean control")
        .save(&context)
        .await
        .unwrap();
    assert_eq!(E::scalar_type_probe(&saved).get_version().eval(), Some(3));
    let unknown = Q::scalar_type_probes_minimal()
        .with_id_is(id)
        .with_optional_boolean_is_unknown()
        .select_optional_boolean()
        .limit(1)
        .comment("load the unknown Boolean control")
        .purpose("distinguish null from false")
        .execute_for_list(&context)
        .await
        .unwrap();
    assert_eq!(unknown.len(), 1);
    assert!(unknown[0].is_field_loaded("optional_boolean"));
    assert_eq!(
        E::scalar_type_probe(&unknown[0])
            .get_optional_boolean()
            .try_eval()
            .unwrap(),
        None
    );
    let sparse = Q::scalar_type_probes_minimal()
        .with_id_is(id)
        .select_required_string()
        .limit(1)
        .comment("load a projection without the Boolean field")
        .purpose("preserve NotLoaded separately from null")
        .execute_for_one(&context)
        .await
        .unwrap()
        .unwrap();
    assert!(!sparse.is_field_loaded("optional_boolean"));
    assert!(
        E::scalar_type_probe(&sparse)
            .get_optional_boolean()
            .try_eval()
            .is_err()
    );

    let mut refusals = 0;
    for update in [false, true] {
        for field in ["required_boolean", "optional_boolean"] {
            for wrong in [
                Value::I64(1),
                Value::Text("private-boolean-fix-canary".into()),
            ] {
                let mut attempt = service_runtime_from_pool(pool.clone()).await.unwrap();
                let mut entity = if update {
                    exact::load(&attempt, id).await
                } else {
                    exact::prepared(&attempt, "rejected Boolean creation")
                };
                if update {
                    entity.update_required_string("must not persist".to_owned());
                }
                let ledger = LedgerEntity::entity_runtime_state(&entity).unwrap();
                let pending = ledger.current_change_set();
                let calls = Arc::new(AtomicUsize::new(0));
                attempt.set_checker_registry(InMemoryCheckerRegistry::new().with_checker(
                    BadBooleanFix {
                        field,
                        value: wrong,
                        calls: calls.clone(),
                    },
                ));
                let before = STATEMENTS.load(Ordering::SeqCst);
                let error = entity
                    .audit_as("reject an invalid post-Fix Boolean")
                    .save(&attempt)
                    .await
                    .expect_err("invalid Boolean must not reach provider");
                assert_eq!(STATEMENTS.load(Ordering::SeqCst), before);
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(ledger.current_change_set(), pending);
                let RuntimeError::Check(results) = error else {
                    panic!("expected checker rejection")
                };
                assert!(results.iter().any(|r| r.rule == CheckRule::InvalidType
                    && r.location.model_path().ends_with(field)));
                refusals += 1;
            }
        }
    }
    assert_eq!(refusals, 8);
    let reloaded = exact::load(&context, id).await;
    assert_eq!(
        E::scalar_type_probe(&reloaded).get_version().eval(),
        Some(3)
    );
    assert_eq!(
        E::scalar_type_probe(&reloaded).get_required_string().eval(),
        Some(format!("Boolean round {round}"))
    );
    let physical: (i64, Option<i64>, i64) = pool.lock().unwrap().query_row(
        "SELECT required_boolean,optional_boolean,version FROM scalar_type_probe_data WHERE id=?1",
        [id as i64], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(physical, (0, None, 3));

    // Explicit legacy-storage negative, never a business mutation. Restore the
    // database before asserting the result, including the ordinary error path.
    pool.lock()
        .unwrap()
        .execute_batch("SAVEPOINT boolean_storage_negative")
        .unwrap();
    pool.lock()
        .unwrap()
        .execute(
            "UPDATE scalar_type_probe_data SET required_boolean=2 WHERE id=?1",
            [id as i64],
        )
        .unwrap();
    let result = Q::scalar_type_probes_minimal()
        .with_id_is(id)
        .select_required_boolean()
        .limit(1)
        .comment("inspect the corrupt Boolean storage oracle")
        .purpose("reject fabricated true values")
        .execute_for_list(&context)
        .await;
    pool.lock()
        .unwrap()
        .execute_batch("ROLLBACK TO boolean_storage_negative; RELEASE boolean_storage_negative")
        .unwrap();
    let error = result
        .expect_err("legacy integer 2 must not hydrate as true")
        .to_string();
    assert!(error.contains("required_boolean"));
    assert!(error.contains("boolean"));
    println!(
        "RUST_BOOLEAN_GENERATED={}",
        serde_json::json!({
            "passed":true,"round":round,"id":id,"version":3,"postFixRefusals":refusals,
            "typedQAndE":true,"trueFalseUnknown":true,"notLoadedIsNotNull":true,
            "rejectedFixProviderEntries":0,"rejectedFixLedgerUnchanged":true,
            "legacyIntegerRejected":true,"physicalBoolean":physical.0,"physicalNull":physical.1,
            "completeExampleInventoryAccepted":false,"installedArtifactAccepted":false
        })
    );
}
