use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use teaql_core::{Value, time::Timestamp};
use teaql_macros::{TeaqlEntity as DeriveTeaqlEntity, teaql_entity};
use teaql_runtime::{
    CheckObjectStatus, CheckResults, Checker, ContextFixError, ContextFixInput, ContextFixResolver,
    EntityValues, FixedBusinessClock, ObjectLocation, TypedChecker, TypedEntityChecker,
    UserContext, mark_entity_status,
};

#[teaql_entity]
#[derive(Clone, Debug, PartialEq, DeriveTeaqlEntity)]
#[teaql(entity = "ContextCreationProbe", table = "context_creation_probe")]
struct Probe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    opened_at: Option<Timestamp>,
    modified_at: Option<Timestamp>,
    source_ip: Option<String>,
    last_ip: Option<String>,
}

struct Resolver;
impl ContextFixResolver for Resolver {
    fn resolve(
        &self,
        _: &UserContext,
        input: &ContextFixInput<'_>,
    ) -> Result<Value, ContextFixError> {
        Ok(if input.function == "now" {
            Value::Timestamp(input.now)
        } else {
            Value::Text(format!("ip-{}", input.now.0))
        })
    }
}

struct Defaults {
    fixes: Arc<AtomicUsize>,
    checks: Arc<AtomicUsize>,
}
impl TypedChecker<Probe> for Defaults {
    fn check_and_fix_typed(
        &self,
        _: &UserContext,
        _: &mut Probe,
        _: CheckObjectStatus,
        _: &ObjectLocation,
        _: &mut CheckResults,
    ) {
        panic!("the split checker must not enter the legacy combined hook");
    }

    fn separate_fix_and_check(&self) -> bool {
        true
    }
    fn managed_context_fields(&self) -> &'static [&'static str] {
        &["opened_at", "modified_at", "source_ip", "last_ip"]
    }
    fn fix_typed(
        &self,
        context: &UserContext,
        entity: &mut Probe,
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        self.fixes.fetch_add(1, Ordering::SeqCst);
        if status.is_create() {
            entity.opened_at = context.context_fix_value(
                "ContextCreationProbe",
                "opened_at",
                "now",
                &[],
                status,
                location,
                results,
            );
            entity.source_ip = context.context_fix_value(
                "ContextCreationProbe",
                "source_ip",
                "ip",
                &[],
                status,
                location,
                results,
            );
        }
        entity.modified_at = context.context_fix_value(
            "ContextCreationProbe",
            "modified_at",
            "now",
            &[],
            status,
            location,
            results,
        );
        entity.last_ip = context.context_fix_value(
            "ContextCreationProbe",
            "last_ip",
            "ip",
            &[],
            status,
            location,
            results,
        );
    }
    fn check_typed(
        &self,
        _: &UserContext,
        entity: &Probe,
        _: CheckObjectStatus,
        _: &ObjectLocation,
        _: &mut CheckResults,
        _: bool,
    ) {
        self.checks.fetch_add(1, Ordering::SeqCst);
        assert!(
            entity.opened_at.is_some() && entity.modified_at.is_some(),
            "readonly Check must see fixed values"
        );
    }
}

#[test]
fn create_without_dirty_fields_fixes_then_checks_but_clean_update_only_checks() {
    let fixes = Arc::new(AtomicUsize::new(0));
    let checks = Arc::new(AtomicUsize::new(0));
    let checker = TypedEntityChecker::<Probe, _>::new(Defaults {
        fixes: fixes.clone(),
        checks: checks.clone(),
    });
    let context = |millis| {
        UserContext::new()
            .with_business_clock(FixedBusinessClock::new(
                chrono::DateTime::from_timestamp_millis(millis).unwrap(),
            ))
            .with_context_fix_resolver(Resolver)
    };
    let mut values = EntityValues::from(std::collections::BTreeMap::from([
        ("id".into(), Value::U64(7)),
        ("version".into(), Value::I64(0)),
        ("_dirty_fields".into(), Value::List(vec![])),
    ]));
    mark_entity_status(&mut values, CheckObjectStatus::Create);
    let mut results = vec![];
    checker.check_and_fix_for_mutation(
        &context(17_000),
        &mut values,
        &ObjectLocation::root(),
        &mut results,
        false,
    );
    assert!(results.is_empty(), "{results:?}");
    assert_eq!(
        values.get("opened_at"),
        Some(&Value::Timestamp(Timestamp(17_000)))
    );
    assert_eq!(
        values.get("modified_at"),
        Some(&Value::Timestamp(Timestamp(17_000)))
    );
    assert_eq!(values.get("source_ip"), Some(&Value::from("ip-17000")));
    assert_eq!(fixes.load(Ordering::SeqCst), 1);
    assert_eq!(checks.load(Ordering::SeqCst), 1);

    mark_entity_status(&mut values, CheckObjectStatus::Update);
    values.insert("_dirty_fields".into(), Value::List(vec![]));
    checker.check_and_fix_for_mutation(
        &context(21_000),
        &mut values,
        &ObjectLocation::root(),
        &mut results,
        false,
    );
    assert!(results.is_empty(), "{results:?}");
    assert_eq!(
        values.get("modified_at"),
        Some(&Value::Timestamp(Timestamp(17_000)))
    );
    assert_eq!(
        fixes.load(Ordering::SeqCst),
        1,
        "changed Context must not invoke automatic Fix for a clean update"
    );
    assert_eq!(
        checks.load(Ordering::SeqCst),
        2,
        "readonly Check still runs"
    );

    checker.check_and_fix_for_mutation(
        &context(21_000),
        &mut values,
        &ObjectLocation::root(),
        &mut results,
        true,
    );
    assert!(results.is_empty(), "{results:?}");
    assert_eq!(
        values.get("opened_at"),
        Some(&Value::Timestamp(Timestamp(17_000)))
    );
    assert_eq!(values.get("source_ip"), Some(&Value::from("ip-17000")));
    assert_eq!(
        values.get("modified_at"),
        Some(&Value::Timestamp(Timestamp(21_000)))
    );
    assert_eq!(values.get("last_ip"), Some(&Value::from("ip-21000")));
    assert_eq!(fixes.load(Ordering::SeqCst), 2);
    assert_eq!(checks.load(Ordering::SeqCst), 3);
}
