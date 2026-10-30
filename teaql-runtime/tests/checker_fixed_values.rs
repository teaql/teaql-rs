use std::sync::{Arc, Mutex};
use teaql_core::Value;
use teaql_macros::TeaqlEntity;
use teaql_runtime::{
    CheckObjectStatus, CheckResult, CheckResults, EntityValues, InMemoryCheckerRegistry,
    ObjectLocation, RuntimeError, TypedChecker, TypedEntityChecker, UserContext,
};

#[derive(Clone, Debug, TeaqlEntity)]
#[teaql(entity = "FixedValuesProbe", table = "fixed_values_probe")]
struct Row {
    #[teaql(id)]
    id: u64,
    display_name: Option<String>,
}
struct Logic(Arc<Mutex<Vec<EntityValues>>>);
fn payload<const N: usize>(fields: [(String, Value); N]) -> EntityValues {
    fields
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>()
        .into()
}
impl TypedChecker<Row> for Logic {
    fn check_and_fix_typed(
        &self,
        _: &UserContext,
        entity: &mut Row,
        _: CheckObjectStatus,
        _: &ObjectLocation,
        _: &mut CheckResults,
    ) {
        if entity.display_name.as_deref() == Some("fix") {
            entity.display_name = Some("fixed".to_owned());
        }
    }
    fn check_fixed_values(
        &self,
        _: &UserContext,
        values: &EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        self.0.lock().unwrap().push(values.clone());
        if values.get("display_name") == Some(&Value::from("invalid")) {
            results.push(CheckResult::min_str(
                location.clone().member("display_name"),
                8,
                "invalid",
            ));
        }
    }
}
fn context() -> (UserContext, Arc<Mutex<Vec<EntityValues>>>) {
    let observed = Arc::new(Mutex::new(Vec::new()));
    (
        UserContext::new().with_checker_registry(
            InMemoryCheckerRegistry::new()
                .with_checker(TypedEntityChecker::<Row, _>::new(Logic(observed.clone()))),
        ),
        observed,
    )
}
#[test]
fn model_constraints_see_the_actual_post_custom_fix_values_once() {
    let (context, observed) = context();
    let mut values = payload([
        ("id".to_owned(), Value::U64(1)),
        ("display_name".to_owned(), Value::from("fix")),
    ]);
    context
        .check_and_fix_values("FixedValuesProbe", &mut values)
        .unwrap();
    assert_eq!(values.get("display_name"), Some(&Value::from("fixed")));
    assert_eq!(observed.lock().unwrap().len(), 1);
    assert_eq!(
        observed.lock().unwrap()[0].get("display_name"),
        Some(&Value::from("fixed"))
    );
}
#[test]
fn sparse_absence_and_explicit_null_are_not_materialized_type_defaults() {
    let (context, observed) = context();
    for present in [false, true] {
        let mut values = payload([("id".to_owned(), Value::U64(1))]);
        if present {
            values.insert("display_name".to_owned(), Value::Null);
        }
        context
            .check_and_fix_values("FixedValuesProbe", &mut values)
            .unwrap();
        assert_eq!(values.contains_key("display_name"), present);
    }
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), 2);
    assert!(!observed[0].contains_key("display_name"));
    assert_eq!(observed[1].get("display_name"), Some(&Value::Null));
}
#[test]
fn a_post_fix_constraint_keeps_declaring_type_and_indexed_canonical_path() {
    let (context, observed) = context();
    let mut values = payload([
        ("id".to_owned(), Value::U64(1)),
        ("display_name".to_owned(), Value::from("invalid")),
    ]);
    let location = ObjectLocation::root().member("order_item_list").element(3);
    let Err(RuntimeError::Check(results)) =
        context.check_and_fix_values_at("FixedValuesProbe", &mut values, &location)
    else {
        panic!("modeled constraint must reject");
    };
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entity_type.as_deref(), Some("FixedValuesProbe"));
    assert_eq!(
        results[0].location.instance_path(),
        "/orderItemList/3/displayName"
    );
    assert_eq!(observed.lock().unwrap().len(), 1);
}
struct ExistingLogic;
impl TypedChecker<Row> for ExistingLogic {
    fn check_and_fix_typed(
        &self,
        _: &UserContext,
        _: &mut Row,
        _: CheckObjectStatus,
        _: &ObjectLocation,
        _: &mut CheckResults,
    ) {
    }
}
#[test]
fn an_existing_typed_checker_remains_source_compatible_without_the_new_hook() {
    let context = UserContext::new().with_checker_registry(
        InMemoryCheckerRegistry::new()
            .with_checker(TypedEntityChecker::<Row, _>::new(ExistingLogic)),
    );
    context
        .check_and_fix_values(
            "FixedValuesProbe",
            &mut payload([("id".to_owned(), Value::U64(1))]),
        )
        .unwrap();
}

struct DuplicateRule;
impl TypedChecker<Row> for DuplicateRule {
    fn check_and_fix_typed(
        &self,
        _: &UserContext,
        _: &mut Row,
        _: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        results.push(
            CheckResult::min_str(location.clone().member("display_name"), 8, "invalid")
                .with_message("customer rule"),
        );
    }
    fn check_fixed_values(
        &self,
        _: &UserContext,
        _: &EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        results.push(CheckResult::min_str(
            location.clone().member("display_name"),
            8,
            "invalid",
        ));
    }
}
#[test]
fn a_customer_rule_message_is_not_duplicated_by_the_modeled_check() {
    let context = UserContext::new().with_checker_registry(
        InMemoryCheckerRegistry::new()
            .with_checker(TypedEntityChecker::<Row, _>::new(DuplicateRule)),
    );
    let mut values = payload([
        ("id".to_owned(), Value::U64(1)),
        ("display_name".to_owned(), Value::from("invalid")),
    ]);
    let Err(RuntimeError::Check(results)) =
        context.check_and_fix_values("FixedValuesProbe", &mut values)
    else {
        panic!("customer rule must reject");
    };
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message.as_deref(), Some("customer rule"));
    assert_eq!(results[0].entity_type.as_deref(), Some("FixedValuesProbe"));
}
