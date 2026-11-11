//! App-owned generated Q/E/save consumer. The only low-level read projects
//! readonly literal properties through a bounded Expr AST to exercise runtime
//! schema hydration; it executes no raw SQL or caller-supplied JavaScript.
#[path = "rust-integral-generated.rs"]
mod exact;
use scalar_type_conformance_service_core::{
    AuditedSave, E, Q, ScalarTypeProbe, ServiceRuntimeExecutor, service_runtime_from_pool,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{
    DataType, Entity, Expr, SelectQuery, Value, dynamic_properties::DynamicPropertyDefinitions,
};
use teaql_runtime::{
    EntityKey, LedgerEntity, MutationDecision, MutationPlan, MutationPolicy,
    MutationPolicyIdentity, PurposedSelectQuery, RuntimeError, SafeAuditEvent, SafeAuditEventSink,
    UserContext,
};
static STATEMENTS: AtomicUsize = AtomicUsize::new(0);
fn observe(_: &str) {
    STATEMENTS.fetch_add(1, Ordering::SeqCst);
}
#[derive(Clone, Default)]
struct Evidence {
    plans: Arc<Mutex<Vec<MutationPlan>>>,
    audits: Arc<Mutex<Vec<SafeAuditEvent>>>,
}
impl SafeAuditEventSink for Evidence {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.audits.lock().unwrap().push(event.clone());
        Ok(())
    }
}
struct Policy(Evidence);
impl MutationPolicy for Policy {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("restoration-views", "1", "outer-transaction")
    }
    fn review(&self, _: &UserContext, plan: &MutationPlan) -> MutationDecision {
        self.0.plans.lock().unwrap().push(plan.clone());
        MutationDecision::allow()
    }
}
async fn readonly(context: &UserContext, id: u64) -> ScalarTypeProbe {
    let definitions = DynamicPropertyDefinitions::new([
        ("_count".into(), DataType::I64),
        ("_null".into(), DataType::Text),
        ("_missing".into(), DataType::I64),
    ])
    .unwrap();
    let query = SelectQuery::new("ScalarTypeProbe")
        .projects(["id", "version", "required_string"])
        .project_expr("_count", Expr::value(0_i64))
        .project_expr("_null", Expr::value(Value::TypedNull(DataType::Text)))
        .filter(Expr::eq("id", id))
        .with_dynamic_property_definitions(definitions)
        .limit(1)
        .comment("load a sparse generated view with readonly result metadata");
    context
        .entity_data_service::<ServiceRuntimeExecutor>("ScalarTypeProbe")
        .unwrap()
        .fetch_enhanced_entities::<ScalarTypeProbe>(&PurposedSelectQuery::new(
            query,
            "verify typed dynamic properties across a no-op save",
        ))
        .await
        .unwrap()
        .data
        .remove(0)
}
fn assert_view(value: &ScalarTypeProbe, name: &str) {
    assert_eq!(
        E::scalar_type_probe(value)
            .get_required_string()
            .eval()
            .as_deref(),
        Some(name)
    );
    assert_eq!(E::scalar_type_probe(value).get_version().eval(), Some(1));
    assert!(
        E::scalar_type_probe(value)
            .get_required_long()
            .try_eval()
            .is_err()
    );
    assert_eq!(value.dynamic_property_type("_count"), Some(DataType::I64));
    assert_eq!(value.dynamic_property_type("_missing"), Some(DataType::I64));
    assert_eq!(value.dynamic_property("_count"), Some(&Value::I64(0)));
    assert!(value.has_dynamic_property("_null"));
    assert!(value.dynamic_property("_null").is_none());
    assert!(!value.has_dynamic_property("_missing"));
    assert!(value.dynamic_property("_missing").is_none());
    assert!(value.dirty_fields().is_none());
}
#[tokio::test]
async fn generated_restoration_views_and_outer_transactions() {
    let round = std::env::var("TEAQL_VIEWS_ROUND").unwrap();
    let pool = Arc::new(Mutex::new(
        rusqlite::Connection::open(std::env::var("TEAQL_VIEWS_DATABASE").unwrap()).unwrap(),
    ));
    let mut context = service_runtime_from_pool(pool.clone()).await.unwrap();
    context.ensure_schema().await.unwrap();
    pool.lock()
        .unwrap()
        .trace_v2(
            rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
            Some(|event: rusqlite::trace::TraceEvent<'_>| {
                if let rusqlite::trace::TraceEvent::Stmt(statement, sql) = event {
                    if sql.starts_with("--") {
                        observe(sql);
                    } else {
                        observe(&statement.expanded_sql().expect("expanded SQLite trace SQL"));
                    }
                }
            }),
        )
        .unwrap();
    let first_name = format!("views first {round}");
    let second_name = format!("views second {round}");
    let mut first = exact::prepared(&context, &first_name);
    first.update_optional_string(Value::Null);
    let first = first
        .audit_as("seed the unchanged generated view")
        .save(&context)
        .await
        .unwrap();
    let first_id = first.id();
    let second = exact::prepared(&context, &second_name)
        .audit_as("seed the independently changed generated view")
        .save(&context)
        .await
        .unwrap();
    let second_id = second.id();
    let evidence = Evidence::default();
    context.set_custom_event_sink(evidence.clone());
    let policy: Arc<dyn MutationPolicy> = Arc::new(Policy(evidence.clone()));
    context.set_mutation_policy_registry(move |_: &str| Some(policy.clone()));
    let mut view = readonly(&context, first_id).await;
    view.update_required_string("temporary");
    view.update_required_string(first_name.clone());
    let statements = STATEMENTS.load(Ordering::SeqCst);
    let view = view
        .audit_as("retain readonly metadata without saving a sparse row")
        .save(&context)
        .await
        .unwrap();
    assert_eq!(STATEMENTS.load(Ordering::SeqCst), statements);
    assert_view(&view, &first_name);
    assert!(evidence.plans.lock().unwrap().is_empty());
    assert!(evidence.audits.lock().unwrap().is_empty());
    let mut changed = exact::load(&context, second_id).await;
    let changed_name = format!("views retry {round}");
    changed.update_required_string(changed_name.clone());
    let retry = changed.clone();
    let pending = changed.entity_runtime_state().unwrap();
    let expected = first_name.clone();
    let captured = evidence.clone();
    let rollback: Result<(), RuntimeError> = context
        .execute_in_transaction::<ServiceRuntimeExecutor, _, _>(|transaction| {
            Box::pin(async move {
                let statements = STATEMENTS.load(Ordering::SeqCst);
                let saved = transaction
                    .save_audited(view.audit_as("no-op stays inside the caller rollback scope"))
                    .await?;
                assert_eq!(STATEMENTS.load(Ordering::SeqCst), statements);
                assert_view(&saved, &expected);
                let saved = transaction
                    .save_audited(changed.audit_as("roll back only the independent real edit"))
                    .await?;
                assert_eq!(E::scalar_type_probe(&saved).get_version().eval(), Some(2));
                assert!(captured.audits.lock().unwrap().is_empty());
                Err(RuntimeError::Graph(
                    "intentional generated restoration rollback".into(),
                ))
            })
        })
        .await;
    assert!(
        rollback
            .unwrap_err()
            .to_string()
            .contains("intentional generated restoration rollback")
    );
    assert!(evidence.audits.lock().unwrap().is_empty());
    assert_eq!(
        pending.get(
            &EntityKey::new_static("ScalarTypeProbe", second_id),
            "required_string"
        ),
        Some(Value::from(changed_name.clone()))
    );
    let physical: (String, i64) = pool
        .lock()
        .unwrap()
        .query_row(
            "SELECT required_string,version FROM scalar_type_probe_data WHERE id=?1",
            [second_id as i64],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(physical, (second_name, 1));
    let retried = retry
        .audit_as("retry after outer rollback with the original optimistic version")
        .save(&context)
        .await
        .unwrap();
    assert_eq!(E::scalar_type_probe(&retried).get_version().eval(), Some(2));
    assert_eq!(evidence.audits.lock().unwrap().len(), 1);
    let view = readonly(&context, first_id).await;
    let mut second = exact::load(&context, second_id).await;
    let committed = format!("views committed {round}");
    second.update_optional_string(committed.clone());
    let expected = first_name.clone();
    let captured = evidence.clone();
    let view = context
        .execute_in_transaction::<ServiceRuntimeExecutor, _, _>(|transaction| {
            Box::pin(async move {
                let statements = STATEMENTS.load(Ordering::SeqCst);
                let view = transaction
                    .save_audited(view.audit_as("commit a no-op together with one actual edit"))
                    .await?;
                assert_eq!(STATEMENTS.load(Ordering::SeqCst), statements);
                assert_view(&view, &expected);
                let saved = transaction
                    .save_audited(second.audit_as("commit only the second generated entity"))
                    .await?;
                assert_eq!(E::scalar_type_probe(&saved).get_version().eval(), Some(3));
                assert_eq!(captured.audits.lock().unwrap().len(), 1);
                Ok(view)
            })
        })
        .await
        .unwrap();
    assert_view(&view, &first_name);
    assert_eq!(evidence.audits.lock().unwrap().len(), 2);
    let plans = evidence.plans.lock().unwrap();
    assert_eq!(plans.len(), 3);
    assert!(plans.iter().all(|plan| plan.operations.len() == 1
        && plan.operations[0].entity == EntityKey::new_static("ScalarTypeProbe", second_id)));
    let physical:(i64,String,Option<String>)=pool.lock().unwrap().query_row("SELECT version,required_string,optional_string FROM scalar_type_probe_data WHERE id=?1",[first_id as i64],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap();
    assert_eq!(physical, (1, first_name, None));
    let physical:(i64,String,Option<String>)=pool.lock().unwrap().query_row("SELECT version,required_string,optional_string FROM scalar_type_probe_data WHERE id=?1",[second_id as i64],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap();
    assert_eq!(physical, (3, changed_name, Some(committed)));
    let root_version: i64 = pool
        .lock()
        .unwrap()
        .query_row(
            "SELECT version FROM type_platform_data WHERE id=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(root_version, 1);
    // Final generated Q and E are independent of the raw SQLite oracle above.
    let rows = Q::scalar_type_probes()
        .with_id_is(first_id)
        .limit(1)
        .comment("verify the unchanged object after both outer transactions")
        .purpose("confirm no hidden version bump through the generated Q API")
        .execute_for_one(&context)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(E::scalar_type_probe(&rows).get_version().eval(), Some(1));
    println!(
        "RUST_RESTORATION_VIEWS={}",
        serde_json::json!({"passed":true,"round":round,"firstId":first_id,"secondId":second_id,"firstVersion":1,"secondVersion":3,"policyReviews":3,"mutationAudits":2,
  "readOnlyTypes":true,"readOnlyNullAndMissing":true,"unchangedSparseNoWrite":true,"outerRollbackOwned":true,"rollbackIntentRetained":true,"retrySavedOnce":true,"outerCommitOwned":true,"readonlyStateAfterTransaction":true,"actualQAndE":true,"independentSqliteOracle":true,
  "completeExampleInventoryAccepted":false,"internalArtifactAccepted":false})
    );
}
