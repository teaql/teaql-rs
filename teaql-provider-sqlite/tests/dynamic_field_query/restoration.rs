use super::*;
use std::sync::Mutex;
use teaql_runtime::{
    MutationDecision, MutationPlan, MutationPolicy, MutationPolicyIdentity, RuntimeError,
};

struct Plans(Arc<Mutex<Vec<MutationPlan>>>);
impl MutationPolicy for Plans {
    fn identity(&self) -> MutationPolicyIdentity {
        MutationPolicyIdentity::new("restoration-sqlite", "1", "native-plus-dynamic")
    }
    fn review(&self, _: &UserContext, plan: &MutationPlan) -> MutationDecision {
        self.0.lock().unwrap().push(plan.clone());
        MutationDecision::allow()
    }
}
fn policy(context: &mut UserContext) -> Arc<Mutex<Vec<MutationPlan>>> {
    let plans = Arc::new(Mutex::new(Vec::new()));
    let handler: Arc<dyn MutationPolicy> = Arc::new(Plans(plans.clone()));
    context.set_mutation_policy_registry(move |_: &str| Some(handler.clone()));
    plans
}
fn physical(transport: &SqliteMutationExecutor) -> Vec<(i64, i64, String)> {
    let connection = transport.connection();
    let connection = connection.lock().unwrap();
    let mut query = connection
        .prepare("SELECT id,version,name FROM dynamic_school ORDER BY id")
        .unwrap();
    query
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}
#[test]
fn restoration_preserves_real_dynamic_edit_and_excludes_restored_native_intent() {
    futures_executor::block_on(async {
        let (mut context, transport, _, audits) = atomic_fixture(true).await;
        let plans = policy(&mut context);
        let mut row = atomic_rows(&context).await.remove(0);
        audits.lock().unwrap().clear();
        row.update_native_name("temporary");
        row.update_native_name("school-1");
        row.update_dynamic_field("note", "remaining edit".into())
            .unwrap();
        let saved = row
            .audit_as("persist only the surviving dynamic edit")
            .save(&context)
            .await
            .unwrap();
        assert_eq!((saved.name.as_str(), saved.version), ("school-1", 2));
        assert_eq!(
            saved
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&Value::from("remaining edit"))
        );
        let plans = plans.lock().unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].operations.len(), 1);
        assert!(!plans[0].operations[0].changed_values.contains_key("name"));
        assert!(plans[0].operations[0].changed_values.contains_key("#note"));
        assert_eq!(audits.lock().unwrap().len(), 1);
        assert_eq!(
            physical(&transport),
            vec![
                (1, 2, "school-1".into()),
                (2, 1, "school-2".into()),
                (3, 1, "school-3".into())
            ]
        );
        assert_eq!(
            atomic_rows(&context).await[0]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&Value::from("remaining edit"))
        );
    });
}
#[test]
fn restoration_noop_keeps_dynamic_null_missing_definitions_and_provenance() {
    futures_executor::block_on(async {
        let (mut context, transport, _, audits) = atomic_fixture(true).await;
        let mut row = atomic_rows(&context).await.remove(0);
        row.update_dynamic_field("note", Value::Null).unwrap();
        row.audit_as("seed a durable loaded null")
            .save(&context)
            .await
            .unwrap();
        let mut row = atomic_rows(&context).await.remove(0);
        let before = row.loaded_state_snapshot().unwrap();
        let definitions = row.dynamic_field_values().unwrap().definitions().clone();
        row.update_native_name("temporary");
        row.update_native_name("school-1");
        audits.lock().unwrap().clear();
        let plans = policy(&mut context);
        context.enable_all_sql_log();
        context.clear_sql_logs();
        let saved = row
            .audit_as("retain dynamic states without a native write")
            .save(&context)
            .await
            .unwrap();
        assert!(context.sql_logs().is_empty());
        assert!(plans.lock().unwrap().is_empty());
        assert!(audits.lock().unwrap().is_empty());
        assert_eq!(saved.version, 2);
        let values = saved.dynamic_field_values().unwrap();
        assert_eq!(values.definitions(), &definitions);
        assert_eq!(
            values.field("note").unwrap().state(),
            DynamicFieldState::Null
        );
        assert_eq!(
            values.field("untouched").unwrap().state(),
            DynamicFieldState::NotLoaded
        );
        assert_eq!(
            saved.loaded_state_snapshot().unwrap().selected_names(),
            before.selected_names()
        );
        assert_eq!(physical(&transport)[0], (1, 2, "school-1".into()));
    });
}
#[test]
fn restoration_noop_does_not_commit_outer_transaction_or_consume_retry_intent() {
    futures_executor::block_on(async {
        let (mut context, transport, _, audits) = atomic_fixture(true).await;
        context.enable_all_sql_log();
        let plans = policy(&mut context);
        let mut rows = atomic_rows(&context).await;
        let mut noop = rows.remove(0);
        let mut changed = rows.remove(0);
        noop.update_native_name("temporary");
        noop.update_native_name("school-1");
        changed.update_native_name("outer transaction edit");
        let retry = changed.clone();
        let pending = changed.entity_runtime_state().unwrap();
        let audits_in_work = audits.clone();
        audits.lock().unwrap().clear();
        let result: Result<(), RuntimeError> = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move {
                    let logs = transaction.context().sql_logs().len();
                    let saved = transaction
                        .save_audited(noop.audit_as("no-op within the outer rollback"))
                        .await?;
                    assert_eq!(saved.version, 1);
                    assert_eq!(transaction.context().sql_logs().len(), logs);
                    let changed = transaction
                        .save_audited(changed.audit_as("real edit owned by outer transaction"))
                        .await?;
                    assert_eq!(changed.version, 2);
                    assert!(audits_in_work.lock().unwrap().is_empty());
                    Err(RuntimeError::Graph(
                        "intentional restoration rollback".into(),
                    ))
                })
            })
            .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("intentional restoration rollback")
        );
        assert!(audits.lock().unwrap().is_empty());
        assert_eq!(
            physical(&transport),
            vec![
                (1, 1, "school-1".into()),
                (2, 1, "school-2".into()),
                (3, 1, "school-3".into())
            ]
        );
        assert_eq!(
            pending.get(&EntityKey::new("DynamicSchool", 2_u64), "name"),
            Some(Value::from("outer transaction edit"))
        );
        retry
            .audit_as("retry the unchanged outer optimistic baseline")
            .save(&context)
            .await
            .unwrap();
        assert_eq!(
            physical(&transport)[1],
            (2, 2, "outer transaction edit".into())
        );
        assert_eq!(audits.lock().unwrap().len(), 1);
        let plans = plans.lock().unwrap();
        assert_eq!(plans.len(), 2);
        assert!(plans.iter().all(|plan| plan.operations.len() == 1
            && plan.operations[0].entity == EntityKey::new("DynamicSchool", 2_u64)));
    });
}
#[test]
fn restoration_noop_commit_preserves_a_writable_dynamic_view() {
    futures_executor::block_on(async {
        let (context, transport, _, audits) = atomic_fixture(true).await;
        let mut rows = atomic_rows(&context).await;
        let mut noop = rows.remove(0);
        let mut changed = rows.remove(0);
        noop.update_native_name("temporary");
        noop.update_native_name("school-1");
        changed.update_native_name("committed independently");
        audits.lock().unwrap().clear();
        let audits_in_work = audits.clone();
        let mut saved = context
            .execute_in_transaction::<Executor, _, _>(|transaction| {
                Box::pin(async move {
                    let saved = transaction
                        .save_audited(noop.audit_as("keep the first view unchanged"))
                        .await?;
                    transaction
                        .save_audited(changed.audit_as("commit only the second view"))
                        .await?;
                    assert!(audits_in_work.lock().unwrap().is_empty());
                    Ok(saved)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            physical(&transport),
            vec![
                (1, 1, "school-1".into()),
                (2, 2, "committed independently".into()),
                (3, 1, "school-3".into())
            ]
        );
        assert_eq!(audits.lock().unwrap().len(), 1);
        saved
            .update_dynamic_field("note", "after no-op commit".into())
            .unwrap();
        saved
            .audit_as("reuse preserved dynamic-field provenance")
            .save(&context)
            .await
            .unwrap();
        assert_eq!(physical(&transport)[0], (1, 2, "school-1".into()));
        assert_eq!(audits.lock().unwrap().len(), 2);
        assert_eq!(
            atomic_rows(&context).await[0]
                .dynamic_field_values()
                .unwrap()
                .field("note")
                .unwrap()
                .value(),
            Some(&Value::from("after no-op commit"))
        );
    });
}
