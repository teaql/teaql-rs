//! A modeled Integer is signed 32-bit even when its native carrier is i64.
use crate::{
    CheckObjectStatus, CheckResults, CheckRule, Checker, EntityValues, InMemoryCheckerRegistry,
    InMemoryMetadataStore, ObjectLocation, RuntimeError, UserContext, mark_entity_status,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use teaql_core::{Entity, TeaqlEntity, Value, eval::LoadState};

#[teaql_macros::teaql_entity]
#[derive(Clone, Debug, teaql_macros::TeaqlEntity)]
#[teaql(entity = "Integer32Probe", indexed_layout)]
struct Probe {
    #[teaql(id)]
    id: u64,
    #[teaql(version)]
    version: i64,
    #[teaql(integer32)]
    required_integer: i64,
    #[teaql(integer32)]
    optional_integer: Option<i64>,
    required_long: i64,
    optional_long: Option<i64>,
    #[teaql(skip)]
    __load_state: LoadState,
}
impl Probe {
    const __TEAQL_FIELD_LAYOUT_REVISION: &'static str = "integer32-v1";
    const __TEAQL_FIXED_FIELD_INDEXES: &'static [(&'static str, usize)] = &[
        ("id", 0),
        ("version", 1),
        ("required_integer", 2),
        ("optional_integer", 3),
        ("required_long", 4),
        ("optional_long", 5),
    ];
    const __TEAQL_FIXED_FIELD_MAPPINGS: &'static [(&'static str, &'static str, &'static str)] = &[
        ("id", "id", "id"),
        ("version", "version", "version"),
        ("required_integer", "required_integer", "required_integer"),
        ("optional_integer", "optional_integer", "optional_integer"),
        ("required_long", "required_long", "required_long"),
        ("optional_long", "optional_long", "optional_long"),
    ];
}
fn context() -> UserContext {
    UserContext::new()
        .with_metadata(InMemoryMetadataStore::new().with_entity(Probe::entity_descriptor()))
}
fn values() -> EntityValues {
    BTreeMap::from([
        ("id".into(), Value::U64(9_007_199_254_740_993)),
        ("version".into(), Value::I64(4_294_967_296)),
        ("required_integer".into(), Value::I64(i32::MAX as i64)),
        ("optional_integer".into(), Value::I64(i32::MIN as i64)),
        ("required_long".into(), Value::I64(i64::MAX)),
        ("optional_long".into(), Value::I64(i64::MIN)),
    ])
    .into()
}
fn assert_invalid(error: RuntimeError, field: &str) {
    let RuntimeError::Check(results) = error else {
        panic!("expected a structured validation result")
    };
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].rule, CheckRule::InvalidType);
    assert_eq!(
        results[0].location.model_path(),
        format!("line_items[2].{field}")
    );
    assert!(
        results[0]
            .to_wire(crate::JsonFieldNamingProfile::CamelCase)
            .input_value
            .is_none()
    );
}
#[test]
fn integer32_create_update_required_and_optional_enforce_logical_width() {
    for status in [CheckObjectStatus::Create, CheckObjectStatus::Update] {
        for field in ["required_integer", "optional_integer"] {
            for value in [i32::MAX as i64 + 1, i32::MIN as i64 - 1] {
                let mut input = values();
                input.insert(field.into(), Value::I64(value));
                mark_entity_status(&mut input, status);
                let before = input.clone();
                let location = ObjectLocation::root().member("line_items").element(2);
                assert_invalid(
                    context()
                        .check_and_fix_values_at("Integer32Probe", &mut input, &location)
                        .unwrap_err(),
                    field,
                );
                assert_eq!(input, before);
            }
        }
    }
}
#[test]
fn integer32_hydration_rejects_out_of_domain_values_instead_of_widening() {
    for field in ["required_integer", "optional_integer"] {
        for number in [i32::MAX as i64 + 1, i32::MIN as i64 - 1] {
            let mut input = values();
            input.insert(field.into(), Value::I64(number));
            let record: BTreeMap<String, Value> = input.into();
            let error = Probe::from_compact_row(record.into()).unwrap_err();
            assert!(error.message.contains(field));
            assert!(!error.message.contains(&number.to_string()));
        }
    }
}
#[test]
fn integer32_native_json_validates_width_before_hydration() {
    for field in ["required_integer", "optional_integer"] {
        for number in [i32::MAX as i64 + 1, i32::MIN as i64 - 1] {
            let mut input = serde_json::json!({"id":1,"version":7});
            input[field] = number.into();
            let error = context().decode_json_entity::<Probe>(&input).unwrap_err();
            assert!(error.message.contains(field));
            assert!(!error.message.contains(&number.to_string()));
        }
    }
}
#[test]
fn integer32_endpoints_null_and_omission_preserve_wide_long_id_and_version() {
    let mut input = values();
    context()
        .check_and_fix_values("Integer32Probe", &mut input)
        .unwrap();
    let map: BTreeMap<String, Value> = input.into();
    let row = Probe::from_compact_row(map.into()).unwrap();
    assert_eq!(row.required_integer, i32::MAX as i64);
    assert_eq!(row.optional_integer, Some(i32::MIN as i64));
    assert_eq!(row.id, 9_007_199_254_740_993);
    assert_eq!(row.version, 4_294_967_296);
    assert_eq!(row.required_long, i64::MAX);
    assert_eq!(row.optional_long, Some(i64::MIN));
    let row = context()
        .decode_json_entity::<Probe>(&serde_json::json!({"id":1,"optional_integer":null}))
        .unwrap();
    assert!(row.is_field_loaded("optional_integer"));
    assert!(row.optional_integer.is_none());
    assert!(!row.is_field_loaded("required_integer"));
}
struct Fix {
    number: i64,
    calls: Arc<AtomicUsize>,
}
impl Checker for Fix {
    fn entity(&self) -> &str {
        "Integer32Probe"
    }
    fn check_and_fix(
        &self,
        _: &UserContext,
        input: &mut EntityValues,
        _: &ObjectLocation,
        _: &mut CheckResults,
    ) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        input.insert("required_integer".into(), Value::I64(self.number));
    }
}
#[test]
fn integer32_fix_runs_once_before_width_admission_and_can_repair_input() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ctx =
        context().with_checker_registry(InMemoryCheckerRegistry::new().with_checker(Fix {
            number: 42,
            calls: calls.clone(),
        }));
    let mut input = values();
    input.insert("required_integer".into(), Value::I64(i64::MAX));
    ctx.check_and_fix_values("Integer32Probe", &mut input)
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    ctx.set_checker_registry(InMemoryCheckerRegistry::new().with_checker(Fix {
        number: i32::MAX as i64 + 1,
        calls: calls.clone(),
    }));
    let location = ObjectLocation::root().member("line_items").element(2);
    assert_invalid(
        ctx.check_and_fix_values_at("Integer32Probe", &mut input, &location)
            .unwrap_err(),
        "required_integer",
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_invalid(
        ctx.validate_required_create_payload("Integer32Probe", &input, &location)
            .unwrap_err(),
        "required_integer",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "final sparse admission must not rerun Fix"
    );
}
