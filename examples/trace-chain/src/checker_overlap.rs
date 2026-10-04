//! Actual generated Checker acceptance and rejection overlap on two OS threads.
//! The wrapper delegates unchanged values/results and never manufactures lineage.
use super::{
    AuditCapture, ExpectedItem, Observation, Outcome, assert_audit_graph, assert_execution_lineage,
};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use teaql_runtime::{
    CheckResults, CheckRule, Checker, CheckerRegistry, EntityKey, EntityValues,
    InMemoryCheckerRegistry, ObjectLocation, RawAuditEventKind, RuntimeError, UserContext,
};
use trace_chain_service_core::teaql_core::Entity as _;
use trace_chain_service_core::{AuditedSave as _, E, LedgerEntity as _, Q, checker_registry};

#[derive(Debug)]
struct CheckObservation {
    id: u64,
    thread: std::thread::ThreadId,
    context: usize,
    results: CheckResults,
}

#[derive(Default)]
struct Overlap {
    active: AtomicUsize,
    peak: AtomicUsize,
    arrived: Mutex<Vec<CheckObservation>>,
    ready: Condvar,
}

struct Registry {
    inner: InMemoryCheckerRegistry,
    overlap: Arc<Overlap>,
    context_address: usize,
}

impl CheckerRegistry for Registry {
    fn checker(&self, entity: &str) -> Option<Arc<dyn Checker>> {
        self.inner.checker(entity).map(|inner| {
            Arc::new(Delegate {
                inner,
                overlap: self.overlap.clone(),
                context_address: self.context_address,
            }) as Arc<dyn Checker>
        })
    }
}

struct Delegate {
    inner: Arc<dyn Checker>,
    overlap: Arc<Overlap>,
    context_address: usize,
}

impl Checker for Delegate {
    fn entity(&self) -> &str {
        self.inner.entity()
    }

    fn check_and_fix(
        &self,
        context: &UserContext,
        values: &mut EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        assert_eq!(
            context as *const UserContext as usize, self.context_address,
            "checker must receive the original shared Context, not a clone"
        );
        if self.entity() != "OrderItem" {
            self.inner.check_and_fix(context, values, location, results);
            return;
        }
        let active = self.overlap.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.overlap.peak.fetch_max(active, Ordering::SeqCst);
        // The genuine generated required-field rule populates the caller's
        // invocation-local results before we hold this synchronous callback open.
        self.inner.check_and_fix(context, values, location, results);
        let mut arrived = self.overlap.arrived.lock().unwrap();
        arrived.push(CheckObservation {
            id: values.get("id").and_then(|value| value.try_u64()).unwrap(),
            thread: std::thread::current().id(),
            context: context as *const UserContext as usize,
            results: results.clone(),
        });
        assert!(
            arrived.len() <= 2,
            "only the two generated child checks are expected"
        );
        self.overlap.ready.notify_all();
        let (arrived, wait) = self
            .overlap
            .ready
            .wait_timeout_while(arrived, Duration::from_secs(15), |entries| {
                entries.len() < 2
            })
            .unwrap();
        assert!(
            !wait.timed_out() && arrived.len() == 2,
            "bounded Checker overlap timed out"
        );
        drop(arrived);
        self.overlap.active.fetch_sub(1, Ordering::SeqCst);
    }
}

pub async fn checker_overlap(
    context: &mut UserContext,
    capture: &AuditCapture,
    observation: &Observation,
) -> Outcome<()> {
    let existing = Q::customer_orders()
        .order_by_id_asc()
        .limit(2)
        .comment("what: check bounded overlap fixture")
        .purpose("why: reuse retained rows without database cleanup")
        .execute_for_list(context)
        .await?;
    if existing.len() < 2 {
        let platform = Q::platforms()
            .limit(1)
            .comment("what: load fixture Platform")
            .purpose("why: seed only missing overlap roots")
            .execute_for_one(context)
            .await?
            .ok_or("Platform must be bootstrapped")?;
        for index in existing.len()..2 {
            let mut root = Q::customer_orders()
                .comment("what: prepare overlap root")
                .purpose("why: seed persistent Checker fixture")
                .new_entity(context);
            root.update_platform_id(platform.id());
            root.update_order_number(format!("CHECKER-OVERLAP-{index}"));
            root.update_description("Checker overlap initial state");
            root.audit_as("seed Checker overlap root")
                .save(context)
                .await?;
        }
    }
    for logging in [false, true] {
        if logging {
            context.enable_all_sql_log();
        } else {
            context.disable_sql_log();
        }
        let mut rows = Q::customer_orders()
            .order_by_id_asc()
            .limit(2)
            .select_platform_with(Q::platforms().limit(1))
            .comment("what: load complete roots sharing immutable Platform")
            .purpose("why: overlap actual generated validation on independent graphs")
            .execute_for_list(context)
            .await?;
        assert_eq!(rows.len(), 2);
        let mut bad = rows.data.pop().unwrap();
        let mut good = rows.data.pop().unwrap();
        let good_id = good.id();
        let bad_id = bad.id();
        let good_version = E::customer_order(&good).get_version().eval().unwrap();
        let bad_version = E::customer_order(&bad).get_version().eval().unwrap();
        let bad_description = E::customer_order(&bad).get_description().eval().unwrap();
        let good_platform = E::customer_order(&good).get_platform().eval().unwrap();
        let bad_platform = E::customer_order(&bad).get_platform().eval().unwrap();
        assert!(
            std::ptr::eq(good_platform, bad_platform),
            "one shared immutable snapshot"
        );
        let platform_id = good_platform.id();
        let platform_state = good_platform.entity_runtime_state().unwrap();
        let platform_snapshot = platform_state.original_snapshot().unwrap();
        let platform_key = EntityKey::new("Platform", platform_id);
        let platform_version = platform_state.get_original_version(&platform_key);
        let good_state = good.entity_runtime_state().unwrap();
        let bad_state = bad.entity_runtime_state().unwrap();
        assert_ne!(good_state, bad_state);
        assert_ne!(good_state, platform_state);
        assert_ne!(bad_state, platform_state);

        let description = format!("Accepted checker overlap revision {good_version}");
        good.update_description(description.as_str());
        bad.update_description("Rejected checker graph must never persist");
        let mut good_child = Q::order_items()
            .comment("what: compose valid child")
            .purpose("why: exercise generated required-name Checker")
            .new_entity(context);
        good_child.update_customer_order_id(good_id);
        good_child.update_name("Accepted checker overlap child");
        let good_child_id = good_child.id();
        let good_child = good_child.audit_as("valid overlap child").into_entity();
        good.include_pending_mutations_from(&good_child)?;
        let mut bad_child = Q::order_items()
            .comment("what: compose invalid child")
            .purpose("why: prove a real generated required-field rejection")
            .new_entity(context);
        bad_child.update_customer_order_id(bad_id);
        // Intentionally omit update_name. Do not replace the real Checker with
        // a fake failure or assume that blank text is the generated rule.
        let bad_child_id = bad_child.id();
        let bad_child = bad_child.audit_as("invalid overlap child").into_entity();
        bad.include_pending_mutations_from(&bad_child)?;
        assert!(
            !good_state
                .current_change_set()
                .changes()
                .contains_key(&EntityKey::new("OrderItem", bad_child_id))
        );
        assert!(
            !bad_state
                .current_change_set()
                .changes()
                .contains_key(&EntityKey::new("OrderItem", good_child_id))
        );

        let overlap = Arc::new(Overlap::default());
        let context_address = context as *const UserContext as usize;
        context.set_checker_registry(Registry {
            inner: checker_registry(),
            overlap: overlap.clone(),
            context_address,
        });
        for entity in [
            "Platform",
            "CustomerOrder",
            "OrderItem",
            "Payment",
            "PaymentAttempt",
            "Shipment",
        ] {
            assert!(
                context.has_checker(entity),
                "wrapper must preserve all generated Checkers"
            );
        }
        capture.clear();
        observation.clear();
        context.clear_sql_logs();
        let original: &UserContext = context;
        let handle = tokio::runtime::Handle::current();
        let (accepted, rejected) = std::thread::scope(|scope| {
            let good_handle = &handle;
            let good_thread = scope.spawn(move || {
                good_handle.block_on(good.audit_as("accept checker graph").save(original))
            });
            let bad_handle = &handle;
            let bad_thread = scope.spawn(move || {
                bad_handle.block_on(bad.audit_as("reject checker graph").save(original))
            });
            (
                good_thread.join().expect("accepted save thread"),
                bad_thread.join().expect("rejected save thread"),
            )
        });
        accepted?;
        let RuntimeError::Check(violations) =
            rejected.expect_err("missing required name must reject")
        else {
            panic!("expected typed generated Checker rejection");
        };
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].rule, CheckRule::Required);
        assert_eq!(
            violations[0].location.to_string(),
            "order_item_list[0].name"
        );
        let checks = overlap.arrived.lock().unwrap();
        assert_eq!(checks.len(), 2);
        assert_ne!(checks[0].thread, checks[1].thread);
        assert_eq!(overlap.peak.load(Ordering::SeqCst), 2);
        assert_eq!(overlap.active.load(Ordering::SeqCst), 0);
        assert!(checks.iter().all(|entry| entry.context == context_address));
        assert!(
            checks
                .iter()
                .find(|entry| entry.id == good_child_id)
                .unwrap()
                .results
                .is_empty()
        );
        let invalid = &checks
            .iter()
            .find(|entry| entry.id == bad_child_id)
            .unwrap()
            .results;
        assert_eq!(invalid.len(), 1);
        assert_eq!(invalid[0].rule, CheckRule::Required);
        assert_eq!(invalid[0].location, violations[0].location);
        println!(
            "CHECKER_OVERLAP threads={:?},{:?} original_context={context_address:x} peak=2 rejected_location={}",
            checks[0].thread, checks[1].thread, violations[0].location
        );
        drop(checks);
        let root = ("CustomerOrder", good_id, "accept checker graph");
        let expected = [
            ExpectedItem {
                entity: "CustomerOrder",
                id: good_id,
                kind: RawAuditEventKind::Updated,
                reasons: vec![root],
            },
            ExpectedItem {
                entity: "OrderItem",
                id: good_child_id,
                kind: RawAuditEventKind::Created,
                reasons: vec![root, ("OrderItem", good_child_id, "valid overlap child")],
            },
        ];
        assert_audit_graph(&capture.events(), &expected);
        assert_execution_lineage(observation, &expected);
        let logs = context.sql_logs();
        if logging {
            assert_eq!(
                logs.iter()
                    .filter(|entry| entry.operation.is_mutation())
                    .count(),
                2
            );
            assert!(
                logs.iter()
                    .all(|entry| !format!("{entry:?}").contains("reject checker graph"))
            );
        } else {
            assert!(logs.is_empty());
        }
        assert!(
            !bad_state.current_change_set().changes().is_empty(),
            "valid save cannot clear rejected ledger"
        );
        assert!(platform_state.current_change_set().changes().is_empty());
        assert!(platform_state.new_keys().is_empty());
        assert!(platform_state.deleted_keys().is_empty());
        assert_eq!(platform_state.get_comment(), None);
        assert_eq!(platform_state.original_snapshot(), Some(platform_snapshot));
        assert_eq!(
            platform_state.get_original_version(&platform_key),
            platform_version
        );
        assert!(platform_state.get_trace_chain(&platform_key).is_empty());
        for (id, version, expected_description) in [
            (good_id, good_version + 1, description.as_str()),
            (bad_id, bad_version, bad_description.as_str()),
        ] {
            let row = Q::customer_orders()
                .with_id_is(id)
                .limit(1)
                .comment("what: read overlap outcome")
                .purpose("why: verify committed and rejected database versions")
                .execute_for_one(context)
                .await?
                .unwrap();
            assert_eq!(E::customer_order(&row).get_version().eval(), Some(version));
            assert_eq!(
                E::customer_order(&row).get_description().eval().as_deref(),
                Some(expected_description)
            );
        }
        let committed = Q::order_items()
            .with_id_is(good_child_id)
            .limit(1)
            .comment("what: read accepted child")
            .purpose("why: verify real child persistence")
            .execute_for_one(context)
            .await?
            .unwrap();
        assert_eq!(
            E::order_item(&committed).get_name().eval().as_deref(),
            Some("Accepted checker overlap child")
        );
        assert_eq!(
            E::order_item(&committed).get_customer_order_id().eval(),
            Some(good_id)
        );
        assert!(
            Q::order_items()
                .with_id_is(bad_child_id)
                .limit(1)
                .comment("what: inspect rejected child")
                .purpose("why: prove rejected graph emitted no child write")
                .execute_for_one(context)
                .await?
                .is_none()
        );
        let platform = Q::platforms()
            .with_id_is(platform_id)
            .limit(1)
            .comment("what: reload immutable reference")
            .purpose("why: prove shared Platform had no write")
            .execute_for_one(context)
            .await?
            .unwrap();
        assert_eq!(
            platform
                .entity_runtime_state()
                .unwrap()
                .get_original_version(&platform_key),
            platform_version
        );
        println!(
            "TC-MUT-12 GENERATED CHECKER OVERLAP PASSED logging={logging} accepted={good_id} rejected={bad_id} platform={platform_id}; two threads, same Context, required child rejection, command/SQL/audit isolation"
        );
    }
    // Restore the same generated registry before the example's later fixtures.
    context.set_checker_registry(checker_registry());
    context.enable_all_sql_log();
    Ok(())
}
