//! Checker diagnostics are an output boundary, not a second copy of secret input.
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor, RelationDescriptor, Value};
use teaql_runtime::{
    CheckResult, CheckResults, CheckRule, Checker, EntityValues, InMemoryCheckerRegistry,
    InMemoryMetadataStore, JsonFieldNamingProfile, ObjectLocation, RuntimeError, UserContext,
};

const SECRET: &str = "private-contact-314159";

struct Rules(CheckResults);
impl Checker for Rules {
    fn entity(&self) -> &str {
        "OrderItem"
    }

    fn check_and_fix(
        &self,
        _: &UserContext,
        _: &mut EntityValues,
        _: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        results.extend(self.0.clone());
    }
}

fn context(results: CheckResults) -> UserContext {
    UserContext::new()
        .with_metadata(
            InMemoryMetadataStore::new().with_entity(
                EntityDescriptor::new("OrderItem")
                    .property(PropertyDescriptor::new("contact_phone", DataType::Text))
                    .audit_mask_fields(vec!["contact_phone".to_owned()]),
            ),
        )
        .with_checker_registry(InMemoryCheckerRegistry::new().with_checker(Rules(results)))
}

fn errors(context: &UserContext) -> CheckResults {
    match context.check_and_fix_values("OrderItem", &mut EntityValues::default()) {
        Err(RuntimeError::Check(results)) => results,
        other => panic!("expected Checker errors, got {other:?}"),
    }
}

fn invalid(field: &str, value: impl Into<Value>) -> CheckResult {
    CheckResult::new(CheckRule::InvalidType, ObjectLocation::root().member(field))
        .with_input_value(value)
}

#[test]
fn model_mask_is_applied_before_wire_display_and_debug_escape() {
    let context = context(vec![invalid("contact_phone", SECRET)]);
    let result = &errors(&context)[0];
    assert_eq!(
        result.input_value,
        Some(Value::from("pr******************59"))
    );
    for rendered in [
        format!("{result:?}"),
        format!("{result}"),
        serde_json::to_string(&result.to_wire(JsonFieldNamingProfile::CamelCase)).unwrap(),
    ] {
        assert!(
            !rendered.contains(SECRET),
            "sensitive input escaped: {rendered}"
        );
    }
    assert_eq!(result.entity_type.as_deref(), Some("OrderItem"));
    assert_eq!(result.location.model_path(), "contact_phone");
    assert_eq!(result.rule, CheckRule::InvalidType);
}

#[test]
fn credentials_and_nested_credentials_do_not_need_a_model_mask() {
    let results = errors(&context(vec![
        invalid("password", SECRET),
        invalid(
            "profile",
            Value::Json(serde_json::json!({"access_token": SECRET})),
        ),
    ]));
    for result in results {
        assert_eq!(result.input_value, Some(Value::from("[REDACTED]")));
        assert!(!format!("{result:?}").contains(SECRET));
    }
}

#[test]
fn localized_credential_length_uses_original_unicode_length() {
    let secret = "密钥🔑abc";
    let mut context = context(vec![CheckResult::min_str(
        ObjectLocation::root().member("password"),
        12,
        secret,
    )]);
    for (locale, phrase) in [("en-US", "is 6"), ("zh-CN", "实际长度为 6")] {
        context.set_locale_code(locale).unwrap();
        let result = &errors(&context)[0];
        assert_eq!(result.input_value, Some(Value::from("[REDACTED]")));
        assert!(result.message.as_deref().unwrap().contains(phrase));
        assert!(!format!("{result:?}").contains(secret));
        assert_eq!(result.system_value, Some(Value::U64(12)));
    }
}

#[test]
fn nested_child_mask_keeps_canonical_alias_and_wire_paths() {
    let location = ObjectLocation::root()
        .member("order_item_list")
        .element(2)
        .member("contact_phone");
    let result =
        invalid("contact_phone", SECRET).with_source_instance_path("/items/2/legacy~0phone");
    let mut result = CheckResult {
        location: location.clone(),
        ..result
    };
    result.entity_type = Some("OrderItem".into());
    let result = &errors(&context(vec![result]))[0];
    assert_ne!(result.input_value, Some(Value::from(SECRET)));
    assert_eq!(result.location, location);
    assert_eq!(
        result.source_instance_path.as_deref(),
        Some("/items/2/legacy~0phone")
    );
    for (profile, path) in [
        (
            JsonFieldNamingProfile::CamelCase,
            "/orderItemList/2/contactPhone",
        ),
        (
            JsonFieldNamingProfile::SnakeCase,
            "/order_item_list/2/contact_phone",
        ),
        (
            JsonFieldNamingProfile::PascalCase,
            "/OrderItemList/2/ContactPhone",
        ),
    ] {
        assert_eq!(result.to_wire(profile).instance_path, path);
    }
}

#[test]
fn custom_messages_cannot_echo_known_sensitive_inputs_even_on_a_sibling_rule() {
    let results = errors(&context(vec![
        invalid("contact_phone", SECRET).with_message(format!("Contact {SECRET} was rejected")),
        invalid("name", "ordinary").with_message(format!("Please fix {SECRET} first")),
    ]));
    for result in &results {
        assert!(!result.message.as_deref().unwrap().contains(SECRET));
        assert!(result.message.as_deref().unwrap().contains("[REDACTED]"));
    }
    assert_eq!(results[1].input_value, Some(Value::from("ordinary")));
}

#[test]
fn ordinary_inputs_expected_bounds_and_safe_custom_messages_are_unchanged() {
    let context = context(vec![
        CheckResult::min(ObjectLocation::root().member("quantity"), 2_i64, 1_i64),
        invalid("name", "ordinary").with_message("customer-owned explanation"),
        invalid("contact_phone", Value::Null),
    ]);
    let results = errors(&context);
    assert_eq!(results[0].input_value, Some(Value::I64(1)));
    assert_eq!(results[0].system_value, Some(Value::I64(2)));
    assert_eq!(results[1].input_value, Some(Value::from("ordinary")));
    assert_eq!(
        results[1].message.as_deref(),
        Some("customer-owned explanation")
    );
    assert_eq!(results[2].input_value, Some(Value::Null));
}

#[test]
fn checker_owned_cached_results_and_repeated_translation_are_stable() {
    let original = invalid("contact_phone", SECRET).with_message(format!("Rejected {SECRET}"));
    let context = context(vec![original.clone()]);
    let mut first = errors(&context);
    assert_ne!(first[0].input_value, original.input_value);
    assert_eq!(original.input_value, Some(Value::from(SECRET)));
    context.translate_check_results(&mut first);
    assert_eq!(first, errors(&context));
}

#[test]
fn numeric_short_list_and_null_masks_follow_existing_audit_rules() {
    let typed_null = Value::TypedNull(DataType::Text);
    let results = errors(&context(vec![
        invalid("contact_phone", 12345_i64),
        invalid("contact_phone", "短🔑文"),
        invalid(
            "contact_phone",
            Value::List(vec![Value::from("short"), Value::Null]),
        ),
        invalid("contact_phone", typed_null.clone()),
    ]));
    assert_eq!(results[0].input_value, Some(Value::from("*****")));
    assert_eq!(results[1].input_value, Some(Value::from("***")));
    assert_eq!(
        results[2].input_value,
        Some(Value::List(vec![Value::from("*****"), Value::Null]))
    );
    assert_eq!(results[3].input_value, Some(typed_null));
}

#[test]
fn masked_objects_and_localized_length_messages_are_idempotent() {
    let context = context(vec![
        invalid(
            "contact_phone",
            Value::Json(serde_json::json!({"text": SECRET})),
        ),
        CheckResult::max_str(ObjectLocation::root().member("contact_phone"), 2, SECRET),
    ]);
    let mut first = errors(&context);
    assert_eq!(first[0].input_value, Some(Value::from("[REDACTED]")));
    assert!(first[1].message.as_deref().unwrap().contains("is 22"));
    assert!(!first[1].message.as_deref().unwrap().contains(SECRET));
    context.translate_check_results(&mut first);
    assert_eq!(first, errors(&context));
}

#[test]
fn canonical_forward_relation_uses_the_storage_field_mask_without_path_rewriting() {
    let context = context(vec![invalid("account", 1001_i64)]).with_metadata(
        InMemoryMetadataStore::new().with_entity(
            EntityDescriptor::new("OrderItem")
                .property(PropertyDescriptor::new("account_id", DataType::I64))
                .relation(RelationDescriptor::new("account", "Account").local_key("account_id"))
                .audit_mask_fields(vec!["account_id".into()]),
        ),
    );
    let result = &errors(&context)[0];
    assert_eq!(result.input_value, Some(Value::from("****")));
    assert_eq!(result.location.model_path(), "account");
}

#[test]
fn credential_ancestor_and_nested_custom_constraint_are_always_redacted() {
    let result = CheckResult::new(
        CheckRule::InvalidType,
        ObjectLocation::root()
            .member("credentials")
            .member("password")
            .member("value"),
    )
    .with_input_value(SECRET)
    .with_system_value(Value::Json(serde_json::json!({"api_key": SECRET})))
    .with_message(format!("Rejected {SECRET}"));
    let result = &errors(&context(vec![result]))[0];
    assert_eq!(result.input_value, Some(Value::from("[REDACTED]")));
    assert_eq!(result.system_value, Some(Value::from("[REDACTED]")));
    assert!(!format!("{result:?}").contains(SECRET));
}

#[test]
fn dynamic_field_diagnostic_is_safe_and_entity_input_remains_intact() {
    let context = context(vec![invalid("#extra", SECRET)]);
    let mut values = EntityValues::default();
    values.insert("contact_phone".into(), Value::from(SECRET));
    let original = values.clone();
    let Err(RuntimeError::Check(results)) = context.check_and_fix_values("OrderItem", &mut values)
    else {
        panic!("expected Checker rejection");
    };
    assert_eq!(values, original);
    assert_ne!(results[0].input_value, Some(Value::from(SECRET)));
    assert_eq!(results[0].location.model_path(), "#extra");
}

#[test]
fn message_only_root_errors_use_checked_values_without_guessing_a_field_path() {
    let raw = Value::from("line\n\"private-contact\"🔑");
    let context = context(vec![
        CheckResult::new(CheckRule::InvalidType, ObjectLocation::root()).with_message(format!(
            "failed to materialize OrderItem: invalid field contact_phone: {raw:?}"
        )),
    ]);
    let mut values = EntityValues::default();
    values.insert("contact_phone".into(), raw.clone());
    let Err(RuntimeError::Check(results)) = context.check_and_fix_values("OrderItem", &mut values)
    else {
        panic!("expected Checker rejection");
    };
    assert_eq!(results[0].location, ObjectLocation::root());
    assert_eq!(results[0].input_value, None);
    assert_eq!(
        results[0].message.as_deref(),
        Some("failed to materialize OrderItem: invalid field contact_phone: [REDACTED]")
    );
    assert_eq!(values.get("contact_phone"), Some(&raw));
}

#[test]
fn model_masked_numeric_rules_render_the_mask_and_preserve_the_expected_bound() {
    let context = context(vec![CheckResult::min(
        ObjectLocation::root().member("contact_phone"),
        999999_i64,
        12345_i64,
    )]);
    let result = &errors(&context)[0];
    assert_eq!(result.input_value, Some(Value::from("*****")));
    assert_eq!(result.system_value, Some(Value::I64(999999)));
    assert!(
        result
            .message
            .as_deref()
            .unwrap()
            .ends_with("input is *****")
    );
}

#[test]
fn debug_opt_in_is_exact_and_never_unmasks_credentials() {
    const ENV: &str = "TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS";
    const ACK: &str = "I_UNDERSTAND_SENSITIVE_DATA_MAY_BE_WRITTEN_TO_DISK";
    if let Ok(expected) = std::env::var("TEAQL_CHECKER_PRIVACY_CHILD") {
        let results = errors(&context(vec![
            invalid("contact_phone", SECRET),
            invalid("password", SECRET),
        ]));
        assert_eq!(
            results[0].input_value == Some(Value::from(SECRET)),
            expected == "debug"
        );
        assert_eq!(results[1].input_value, Some(Value::from("[REDACTED]")));
        return;
    }
    for (value, expected) in [(None, "safe"), (Some("true"), "safe"), (Some(ACK), "debug")] {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "debug_opt_in_is_exact_and_never_unmasks_credentials",
                "--nocapture",
            ])
            .env("TEAQL_CHECKER_PRIVACY_CHILD", expected)
            .env_remove(ENV);
        if let Some(value) = value {
            command.env(ENV, value);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}
