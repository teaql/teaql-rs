//! Exact integral admission supplements, but does not narrow Long to Integer.
use crate::{
    CheckObjectStatus, CheckResults, CheckRule, Checker, EntityValues, InMemoryCheckerRegistry,
    InMemoryMetadataStore, JsonFieldNamingProfile, ObjectLocation, RuntimeError, UserContext,
    mark_entity_status,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use teaql_core::{Entity, TeaqlEntity, Value};

#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "ExactIntegralProbe")]
struct Probe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    signed_value: i64,
    optional_signed: Option<i64>,
    unsigned_value: u64,
    optional_unsigned: Option<u64>,
    price: rust_decimal::Decimal,
}

fn decimal(value: &str) -> Value {
    Value::Decimal(value.parse().unwrap())
}
fn context() -> UserContext {
    UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(Probe::entity_descriptor()))
}
fn values() -> EntityValues {
    std::collections::BTreeMap::from([
        ("id".to_owned(), Value::U64(1)),
        ("version".to_owned(), Value::I64(1)),
        ("signed_value".to_owned(), Value::I64(42)),
        ("optional_signed".to_owned(), Value::Null),
        ("unsigned_value".to_owned(), Value::U64(42)),
        ("optional_unsigned".to_owned(), Value::Null),
        ("price".to_owned(), decimal("1.90")),
    ])
    .into()
}
fn assert_invalid(error: RuntimeError, field: &str) {
    let RuntimeError::Check(results) = error else {
        panic!("expected structured checker failure")
    };
    assert_eq!(results.len(), 1);
    let result = &results[0];
    assert_eq!(result.rule, CheckRule::InvalidType);
    assert_eq!(
        result.location.model_path(),
        format!("line_items[2].{field}")
    );
    let wire = result.to_wire(JsonFieldNamingProfile::CamelCase);
    assert!(wire.input_value.is_none());
    assert_eq!(wire.entity_type.as_deref(), Some("ExactIntegralProbe"));
    assert!(!wire.message.unwrap().contains("PRIVATE-NUMERIC-CANARY"));
}
fn location() -> ObjectLocation {
    ObjectLocation::root().member("line_items").element(2)
}

#[test]
fn integral_exactness_create_and_update_reject_fractional_and_unrepresentable_values() {
    for status in [CheckObjectStatus::Create, CheckObjectStatus::Update] {
        for field in [
            "signed_value",
            "optional_signed",
            "unsigned_value",
            "optional_unsigned",
        ] {
            for invalid in [
                decimal("0.1"),
                decimal("-0.1"),
                decimal("1.9"),
                decimal("-1.9"),
                decimal("18446744073709551616"),
                decimal("-9223372036854775809"),
                Value::F64(1.0),
                Value::Bool(true),
                Value::Text("PRIVATE-NUMERIC-CANARY".into()),
            ] {
                let mut input = values();
                input.insert(field.to_owned(), invalid);
                mark_entity_status(&mut input, status);
                let before = input.clone();
                assert_invalid(
                    context()
                        .check_and_fix_values_at("ExactIntegralProbe", &mut input, &location())
                        .unwrap_err(),
                    field,
                );
                assert_eq!(
                    input, before,
                    "admission cannot silently repair or truncate input"
                );
            }
        }
    }
}

#[test]
fn integral_exactness_width_and_signedness_are_checked_without_float_intermediates() {
    for (field, invalid) in [
        ("signed_value", Value::U64(u64::MAX)),
        ("signed_value", decimal("9223372036854775808")),
        ("unsigned_value", Value::I64(-1)),
        ("unsigned_value", decimal("-1.000")),
    ] {
        let mut input = values();
        input.insert(field.to_owned(), invalid);
        assert_invalid(
            context()
                .check_and_fix_values_at("ExactIntegralProbe", &mut input, &location())
                .unwrap_err(),
            field,
        );
    }
}

#[test]
fn integral_exactness_legal_extremes_scaled_integers_and_decimal_prices_survive() {
    for (signed, unsigned) in [
        (Value::I64(i64::MIN), Value::U64(u64::MAX)),
        (Value::I64(i64::MAX), Value::I64(i64::MAX)),
        (
            decimal("-9223372036854775808.0"),
            decimal("18446744073709551615.0"),
        ),
        (decimal("0.000"), decimal("-0.000")),
    ] {
        let mut input = values();
        input.insert("signed_value".into(), signed);
        input.insert("unsigned_value".into(), unsigned);
        let before = input.clone();
        context()
            .check_and_fix_values("ExactIntegralProbe", &mut input)
            .unwrap();
        assert_eq!(input, before);
    }
    let mut partial = EntityValues::new();
    partial.insert("optional_signed".into(), Value::Null);
    partial.insert("optional_unsigned".into(), Value::Null);
    mark_entity_status(&mut partial, CheckObjectStatus::Update);
    context()
        .check_and_fix_values("ExactIntegralProbe", &mut partial)
        .unwrap();
}

struct FixIntegral {
    value: Value,
    calls: Arc<AtomicUsize>,
}
impl Checker for FixIntegral {
    fn entity(&self) -> &str {
        "ExactIntegralProbe"
    }
    fn check_and_fix(
        &self,
        _: &UserContext,
        input: &mut EntityValues,
        _: &ObjectLocation,
        _: &mut CheckResults,
    ) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        input.insert("signed_value".into(), self.value.clone());
    }
}
#[test]
fn integral_exactness_fix_runs_once_before_admission_and_can_repair_invalid_input() {
    let calls = Arc::new(AtomicUsize::new(0));
    let ctx =
        context().with_checker_registry(InMemoryCheckerRegistry::new().with_checker(FixIntegral {
            value: decimal("42.000"),
            calls: calls.clone(),
        }));
    let mut input = values();
    input.insert("signed_value".into(), decimal("1.9"));
    ctx.check_and_fix_values("ExactIntegralProbe", &mut input)
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(input["signed_value"].try_i64(), Some(42));
}
#[test]
fn integral_exactness_fix_cannot_introduce_fractional_values_or_bypass_final_create_payload() {
    let calls = Arc::new(AtomicUsize::new(0));
    let ctx =
        context().with_checker_registry(InMemoryCheckerRegistry::new().with_checker(FixIntegral {
            value: decimal("1.9"),
            calls: calls.clone(),
        }));
    let mut input = values();
    assert_invalid(
        ctx.check_and_fix_values_at("ExactIntegralProbe", &mut input, &location())
            .unwrap_err(),
        "signed_value",
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_invalid(
        ctx.validate_required_create_payload("ExactIntegralProbe", &input, &location())
            .unwrap_err(),
        "signed_value",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "sparse final admission must not rerun Fix"
    );
}
#[test]
fn integral_exactness_macro_hydration_rejects_fractional_values_instead_of_truncating() {
    for field in [
        "id",
        "version",
        "signed_value",
        "optional_signed",
        "unsigned_value",
        "optional_unsigned",
    ] {
        for text in ["0.1", "-0.1", "1.9", "-1.9"] {
            let mut input = values();
            input.insert(field.into(), decimal(text));
            let record: std::collections::BTreeMap<String, Value> = input.into();
            let error = Probe::from_compact_row(record.into()).unwrap_err();
            assert!(
                error.message.contains(field),
                "field must be identified: {error}"
            );
            assert!(
                !error.message.contains(text),
                "rejected values must remain private"
            );
        }
    }
    let input = values();
    let record: std::collections::BTreeMap<String, Value> = input.into();
    let row = Probe::from_compact_row(record.into()).unwrap();
    assert_eq!(row.signed_value, 42);
    assert_eq!(row.unsigned_value, 42);
    assert_eq!(row.price.to_string(), "1.90");
}
