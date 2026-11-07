//! Application-owned generated Q/E/save regression with real SQLite triggers.
use checker_fix_advanced_core::{AuditedSave, E, Q, ServiceRuntimeExecutor};
use chrono::{TimeZone, Utc};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, Value};
use teaql_runtime::{
    EntityKey, FixedBusinessClock, JsonFieldNamingProfile, LedgerEntity, MutationDecision,
    MutationPlan, MutationPolicy, MutationPolicyIdentity, RuntimeError, SafeAuditEvent,
    SafeAuditEventSink, UserContext,
};

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);
fn observe(_: &str) {
    STATEMENTS.fetch_add(1, Ordering::SeqCst);
}
#[derive(Clone, Default)]
struct Evidence {
    reviews: Arc<Mutex<Vec<MutationPlan>>>,
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
        MutationPolicyIdentity::new("tombstone-probe", "1", "authoritative-SQLite-state")
    }
    fn review(&self, _: &UserContext, plan: &MutationPlan) -> MutationDecision {
        self.0.reviews.lock().unwrap().push(plan.clone());
        MutationDecision::allow()
    }
}

pub async fn verify(
    context: &mut UserContext,
    pool: Arc<Mutex<rusqlite::Connection>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let clock = Utc.with_ymd_and_hms(2032, 4, 5, 9, 0, 0).single().unwrap();
    context.set_business_clock(FixedBusinessClock::new(clock));
    pool.lock().unwrap().trace(Some(observe));
    pool.lock().unwrap().execute_batch(
        "CREATE TRIGGER IF NOT EXISTS core_ml13_authoritative_tombstone
         AFTER UPDATE OF version ON purchase_order_data
         WHEN NEW.version < 0 AND OLD.version > 0
          AND NEW.contact_phone IN ('5550941','5550943','5550945')
         BEGIN
          UPDATE purchase_order_data SET version=-900-NEW.id,
            internal_note='database tombstone', update_time=1964854800000
          WHERE id=NEW.id;
         END;",
    )?;
    let evidence = Evidence::default();
    context.set_custom_event_sink(evidence.clone());
    let policy: Arc<dyn MutationPolicy> = Arc::new(Policy(evidence.clone()));
    context.set_mutation_policy_registry(move |_: &str| Some(policy.clone()));
    let mut cases = Vec::new();
    for (path_index, path) in ["ordinary", "explicit-commit", "explicit-rollback-retry"]
        .into_iter()
        .enumerate()
    {
        for trigger in [false, true] {
            let mut order = Q::purchase_orders()
                .comment("Prepare one authoritative tombstone fixture")
                .purpose("Verify generated deletion using the ordinary audited Save boundary")
                .new_entity(context).expect("reserve example identity");
            order.update_contact_phone(format!("555094{}", path_index * 2 + usize::from(trigger)));
            order.update_internal_note(Value::Null);
            let mut order = order
                .audit_as("Create a synthetic order for the deletion lifecycle")
                .save(context)
                .await?;
            let id = order.id();
            let key = EntityKey::new_static("PurchaseOrder", id);
            let version = if trigger {
                -900 - i64::try_from(id)?
            } else {
                -2
            };
            order.mark_for_deletion();
            let pending = order.entity_runtime_state().unwrap();
            let reviews_before = evidence.reviews.lock().unwrap().len();
            let audits_before = evidence.audits.lock().unwrap().len();
            let statements_before = STATEMENTS.load(Ordering::SeqCst);
            let mut rollback = None;
            let saved = if path == "ordinary" {
                order
                    .audit_as("Delete and return the actual persisted tombstone")
                    .save(context)
                    .await?
            } else if path == "explicit-commit" {
                let captured = evidence.clone();
                context
                    .execute_in_transaction::<ServiceRuntimeExecutor, _, _>(|transaction| {
                        Box::pin(async move {
                            let saved = transaction
                                .save_audited(
                                    order.audit_as("Delete inside an explicit commit scope"),
                                )
                                .await?;
                            assert_eq!(
                                E::purchase_order(&saved).get_version().eval(),
                                Some(version)
                            );
                            assert_eq!(captured.audits.lock().unwrap().len(), audits_before);
                            Ok(saved)
                        })
                    })
                    .await?
            } else {
                let retry = order.clone();
                let captured = evidence.clone();
                let result: Result<(), RuntimeError> = context
                    .execute_in_transaction::<ServiceRuntimeExecutor, _, _>(|transaction| {
                        Box::pin(async move {
                            let saved =
                                transaction
                                    .save_audited(order.audit_as(
                                        "Roll back a deletion after authoritative readback",
                                    ))
                                    .await?;
                            assert_eq!(
                                E::purchase_order(&saved).get_version().eval(),
                                Some(version)
                            );
                            assert_eq!(captured.audits.lock().unwrap().len(), audits_before);
                            Err(RuntimeError::Graph("intentional tombstone rollback".into()))
                        })
                    })
                    .await;
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("intentional tombstone rollback")
                );
                let restored: i64 = pool.lock().unwrap().query_row(
                    "SELECT version FROM purchase_order_data WHERE id=?1",
                    [i64::try_from(id)?],
                    |row| row.get(0),
                )?;
                assert_eq!(restored, 1);
                assert_eq!(pending.get_original_version(&key), Some(1));
                assert!(pending.deleted_keys().contains(&key));
                assert_eq!(evidence.audits.lock().unwrap().len(), audits_before);
                rollback = Some(
                    serde_json::json!({"storedVersion":restored,"originalVersion":pending.get_original_version(&key).unwrap(),
                    "pendingDeletion":pending.deleted_keys().contains(&key),"mutationAudits":evidence.audits.lock().unwrap().len()-audits_before}),
                );
                retry
                    .audit_as("Retry the same deletion intent after outer rollback")
                    .save(context)
                    .await?
            };
            let first_statements = STATEMENTS.load(Ordering::SeqCst) - statements_before;
            assert!(
                first_statements > 0,
                "the SQLite statement observer must be live"
            );
            assert_eq!(
                E::purchase_order(&saved).get_version().eval(),
                Some(version)
            );
            let original = saved
                .entity_runtime_state()
                .unwrap()
                .get_original_version(&key);
            assert_eq!(original, Some(version));
            let note = serde_json::to_value(
                E::purchase_order(&saved)
                    .get_internal_note()
                    .try_eval()
                    .unwrap(),
            )?;
            assert_eq!(
                note,
                if trigger {
                    serde_json::json!("database tombstone")
                } else {
                    serde_json::Value::Null
                }
            );
            let updated = E::purchase_order(&saved)
                .get_update_time()
                .try_eval()
                .unwrap()
                .unwrap()
                .as_millis();
            assert_eq!(
                updated,
                if trigger {
                    1964854800000
                } else {
                    clock.timestamp_millis()
                }
            );
            assert_eq!(evidence.audits.lock().unwrap().len() - audits_before, 1);
            let repeat_reviews_before = evidence.reviews.lock().unwrap().len();
            let repeat_audits_before = evidence.audits.lock().unwrap().len();
            let repeat_statements_before = STATEMENTS.load(Ordering::SeqCst);
            let mut repeat = saved.clone();
            repeat.mark_for_deletion();
            let error = repeat
                .audit_as("Reject repeated deletion without policy or database activity")
                .save(context)
                .await
                .unwrap_err();
            let RuntimeError::Check(checks) = error else {
                panic!("expected structured lifecycle rejection: {error}");
            };
            assert_eq!(checks.len(), 1);
            let wire = checks[0].to_wire(JsonFieldNamingProfile::CamelCase);
            assert_eq!(wire.rule_id, "invalid_deletion_state");
            assert_eq!(wire.instance_path, "/version");
            let repeat_statements = STATEMENTS.load(Ordering::SeqCst) - repeat_statements_before;
            let repeat_reviews = evidence.reviews.lock().unwrap().len() - repeat_reviews_before;
            let repeat_audits = evidence.audits.lock().unwrap().len() - repeat_audits_before;
            assert_eq!(
                (repeat_statements, repeat_reviews, repeat_audits),
                (0, 0, 0)
            );
            let visible = Q::purchase_orders()
                .with_id_is(id)
                .limit(1)
                .comment("Confirm the deleted root is absent from normal queries")
                .purpose("Verify a repeated deletion did not revive the row")
                .execute_for_list(context)
                .await?;
            assert_eq!(visible.len(), 0);
            let physical: (i64, Option<String>, i64) = pool.lock().unwrap().query_row(
                "SELECT version,internal_note,CAST(update_time AS INTEGER) FROM purchase_order_data WHERE id=?1",
                [i64::try_from(id)?], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            assert_eq!(physical.0, version);
            assert_eq!(serde_json::to_value(&physical.1)?, note);
            assert_eq!(physical.2, updated);
            cases.push(serde_json::json!({"path":path,"trigger":trigger,"id":id,"version":version,"originalVersion":original.unwrap(),
                "storedVersion":physical.0,"note":note,"updateTimeMillis":updated,"visibleRows":visible.len(),"firstSaveStatements":first_statements,
                "committedMutationAudits":evidence.audits.lock().unwrap().len()-audits_before,"repeatErrorRule":wire.rule_id,
                "repeatStatements":repeat_statements,"repeatPolicyReviews":repeat_reviews,"repeatMutationAudits":repeat_audits,
                "preCommitMutationAudits":if path=="ordinary" {None} else {Some(0)},
                "policyReviews":evidence.reviews.lock().unwrap().len()-reviews_before,"rollback":rollback}));
        }
    }
    println!(
        "RUST_AUTHORITATIVE_TOMBSTONES={}",
        serde_json::json!({"language":"rust","passed":true,"cases":cases})
    );
    Ok(())
}
