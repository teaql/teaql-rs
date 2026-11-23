//! Public Checker boundaries must retain rule identity before wire/localized projection.
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor, Value};
use teaql_runtime::{
    CheckObjectStatus, CheckResult, CheckResults, CheckRule, Checker, EntityValues,
    InMemoryCheckerRegistry, InMemoryMetadataStore, JsonFieldNamingProfile, ObjectLocation,
    RuntimeError, UserContext, mark_entity_status,
};

struct Rules(&'static str);
impl Checker for Rules {
    fn entity(&self) -> &str {
        self.0
    }
    fn check_and_fix(
        &self,
        _: &UserContext,
        _: &mut EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        let field = location.clone().member("user_url");
        results.extend([
            CheckResult::required(field.clone()),
            CheckResult::min(field.clone(), 1_i64, 0_i64),
            CheckResult::max(field.clone(), 8_i64, 9_i64),
            CheckResult::min_str(field.clone(), 2, "a"),
            CheckResult::max_str(field.clone(), 2, "abc"),
            CheckResult::new(CheckRule::InvalidType, field.clone()).with_entity_type(" "),
            CheckResult::required(field)
                .with_entity_type("SpecializedChild")
                .with_source_instance_path("/original_url")
                .with_message("custom display"),
        ]);
    }
}
fn context() -> UserContext {
    UserContext::new().with_checker_registry(
        InMemoryCheckerRegistry::new()
            .with_checker(Rules("OrderItem"))
            .with_checker(Rules("PurchaseOrder")),
    )
}
fn errors(context: &UserContext, entity: &str, location: &ObjectLocation) -> CheckResults {
    match context.check_and_fix_values_at(entity, &mut EntityValues::default(), location) {
        Err(RuntimeError::Check(results)) => results,
        result => panic!("expected structured Checker failure, got {result:?}"),
    }
}

#[test]
fn every_rule_family_gets_the_declaring_entity_type() {
    let checks = errors(&context(), "OrderItem", &ObjectLocation::root());
    assert_eq!(checks.len(), 7);
    for result in &checks[..6] {
        assert_eq!(result.entity_type.as_deref(), Some("OrderItem"));
    }
    assert_eq!(checks[1].input_value, Some(Value::I64(0)));
    assert_eq!(checks[1].system_value, Some(Value::I64(1)));
}

#[test]
fn an_explicit_specialized_type_and_alias_are_never_overwritten() {
    let checks = errors(&context(), "OrderItem", &ObjectLocation::root());
    let result = &checks[6];
    assert_eq!(result.entity_type.as_deref(), Some("SpecializedChild"));
    assert_eq!(
        result.source_instance_path.as_deref(),
        Some("/original_url")
    );
    assert_eq!(result.message.as_deref(), Some("custom display"));
}

#[test]
fn indexed_canonical_paths_and_three_wire_profiles_keep_the_same_identity() {
    let checks = errors(
        &context(),
        "OrderItem",
        &ObjectLocation::root().member("order_item_list").element(2),
    );
    let result = &checks[0];
    assert_eq!(result.location.model_path(), "order_item_list[2].user_url");
    assert_eq!(result.location.native_path(), "order_item_list[2].user_url");
    for (profile, pointer) in [
        (
            JsonFieldNamingProfile::CamelCase,
            "/orderItemList/2/userUrl",
        ),
        (
            JsonFieldNamingProfile::SnakeCase,
            "/order_item_list/2/user_url",
        ),
        (
            JsonFieldNamingProfile::PascalCase,
            "/OrderItemList/2/UserUrl",
        ),
    ] {
        let wire = result.to_wire(profile);
        assert_eq!(wire.entity_type.as_deref(), Some("OrderItem"));
        assert_eq!(wire.instance_path, pointer);
        assert_eq!(wire.location, result.location.wire_segments());
        assert_eq!(wire.rule_id, "required");
    }
}

#[test]
fn localization_does_not_discard_or_replace_rule_identity() {
    let mut context = context();
    context.set_locale_code("zh-CN").unwrap();
    let chinese = errors(&context, "OrderItem", &ObjectLocation::root());
    context.set_locale_code("en-US").unwrap();
    let english = errors(&context, "OrderItem", &ObjectLocation::root());
    assert_ne!(chinese[0].message, english[0].message);
    assert_eq!(chinese[0].entity_type.as_deref(), Some("OrderItem"));
    assert_eq!(chinese[0].entity_type, english[0].entity_type);
    assert_eq!(chinese[0].location, english[0].location);
    assert_eq!(chinese[0].rule, english[0].rule);
}

#[test]
fn independent_checkers_in_the_same_context_do_not_leak_types() {
    let context = context();
    for entity in ["OrderItem", "PurchaseOrder", "OrderItem"] {
        let checks = errors(&context, entity, &ObjectLocation::root());
        assert_eq!(checks[0].entity_type.as_deref(), Some(entity));
    }
}

struct Parent;
impl Checker for Parent {
    fn entity(&self) -> &str {
        "Parent"
    }
    fn check_and_fix(
        &self,
        context: &UserContext,
        _: &mut EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        results.extend(errors(
            context,
            "OrderItem",
            &location.clone().member("order_item_list").element(1),
        ));
        results.push(CheckResult::required(
            location.clone().member("contact_phone"),
        ));
    }
}
#[test]
fn a_recursive_child_is_not_relabelled_as_the_parent() {
    let context = UserContext::new().with_checker_registry(
        InMemoryCheckerRegistry::new()
            .with_checker(Parent)
            .with_checker(Rules("OrderItem")),
    );
    let results = errors(&context, "Parent", &ObjectLocation::root());
    assert_eq!(results[0].entity_type.as_deref(), Some("OrderItem"));
    assert_eq!(
        results.last().unwrap().entity_type.as_deref(),
        Some("Parent")
    );
    assert_eq!(
        results[0].location.instance_path(),
        "/orderItemList/1/userUrl"
    );
}

#[test]
fn metadata_required_checks_without_a_registered_checker_are_typed() {
    let context = UserContext::new().with_metadata(
        InMemoryMetadataStore::new().with_entity(
            EntityDescriptor::new("PurchaseOrder")
                .property(PropertyDescriptor::new("contact_phone", DataType::Text).not_null()),
        ),
    );
    let mut values = EntityValues::default();
    mark_entity_status(&mut values, CheckObjectStatus::Create);
    let Err(RuntimeError::Check(results)) =
        context.check_and_fix_values("PurchaseOrder", &mut values)
    else {
        panic!("required field must fail");
    };
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entity_type.as_deref(), Some("PurchaseOrder"));
    assert_eq!(results[0].location.instance_path(), "/contactPhone");
}
