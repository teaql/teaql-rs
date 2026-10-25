use super::UserContext;
use crate::{CheckResult, CheckResults, CheckRule, EntityValues, ObjectLocation};
use teaql_core::{DataType, Value};

impl UserContext {
    /// Business Float/Double scalars must have a portable numeric value. Do
    /// this after custom Fix, but before IDs, transactions or provider work.
    pub(super) fn collect_floating_property_results(
        &self,
        entity: &str,
        values: &EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        let Some(descriptor) = self
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.entity(entity))
        else {
            return;
        };
        for property in descriptor
            .properties
            .iter()
            .filter(|property| property.data_type == DataType::F64)
        {
            if let Some(Value::F64(value)) = values.get(&property.name)
                && !value.is_finite()
            {
                let field_location = location.clone().member(&property.name);
                if !results.iter().any(|result| {
                    result.rule == CheckRule::InvalidType && result.location == field_location
                }) {
                    // No input_value: JSON cannot distinguish a non-finite
                    // number from null. Do not create misleading wire evidence.
                    results.push(
                        CheckResult::new(CheckRule::InvalidType, field_location)
                            .with_entity_type(entity)
                            .with_message(format!(
                                "invalid field {}: expected a finite Float/Double number",
                                property.name
                            )),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CheckObjectStatus, CheckResult, CheckResults, CheckRule, Checker, EntityValues,
        InMemoryCheckerRegistry, InMemoryMetadataStore, JsonFieldNamingProfile, ObjectLocation,
        RuntimeError, UserContext, mark_entity_status,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor, Value};

    fn context() -> UserContext {
        UserContext::new().with_metadata(
            InMemoryMetadataStore::new().with_entity(
                EntityDescriptor::new("FloatProbe")
                    .property(PropertyDescriptor::new("required_float", DataType::F64).not_null())
                    .property(PropertyDescriptor::new("required_double", DataType::F64).not_null())
                    .property(PropertyDescriptor::new("optional_float", DataType::F64))
                    .property(PropertyDescriptor::new("optional_double", DataType::F64)),
            ),
        )
    }
    fn values() -> EntityValues {
        let mut values = EntityValues::default();
        for field in [
            "required_float",
            "required_double",
            "optional_float",
            "optional_double",
        ] {
            values.insert(field.to_owned(), Value::F64(1.5));
        }
        values
    }
    fn invalid_result(error: RuntimeError, field: &str) -> CheckResult {
        let RuntimeError::Check(results) = error else {
            panic!("expected structured checker error")
        };
        results
            .into_iter()
            .find(|result| {
                result.rule == CheckRule::InvalidType
                    && result.location.to_string().ends_with(field)
            })
            .expect("canonical field-specific invalid_type result")
    }

    #[test]
    fn finite_float_nonfinite_create_update_have_canonical_nested_locations_without_checker() {
        let context = context();
        for status in [CheckObjectStatus::Create, CheckObjectStatus::Update] {
            for field in [
                "required_float",
                "required_double",
                "optional_float",
                "optional_double",
            ] {
                for number in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
                    let mut values = values();
                    values.insert(field.to_owned(), Value::F64(number));
                    mark_entity_status(&mut values, status);
                    let location = ObjectLocation::root().member("line_items").element(2);
                    let error = context
                        .check_and_fix_values_at("FloatProbe", &mut values, &location)
                        .expect_err("non-finite typed scalar must not pass admission");
                    let result = invalid_result(error, field);
                    assert_eq!(
                        result.location.model_path(),
                        format!("line_items[2].{field}")
                    );
                    let wire = result.to_wire(JsonFieldNamingProfile::CamelCase);
                    let field_camel = field
                        .replace("_float", "Float")
                        .replace("_double", "Double");
                    assert_eq!(wire.instance_path, format!("/lineItems/2/{field_camel}"));
                    assert_eq!(wire.entity_type.as_deref(), Some("FloatProbe"));
                    assert!(wire.message.unwrap().contains("finite"));
                    assert!(
                        wire.input_value.is_none(),
                        "do not encode NaN/Infinity as an ordinary JSON null"
                    );
                    let Value::F64(unchanged) = values.get(field).unwrap() else {
                        panic!("scalar changed")
                    };
                    assert_eq!(unchanged.to_bits(), number.to_bits());
                }
            }
        }
    }
    #[test]
    fn finite_float_legal_values_and_nullable_null_keep_their_bits() {
        let context = context();
        for number in [0.0, -0.0, 1.5, -3.5, f64::MIN_POSITIVE, f64::MAX] {
            let mut values = values();
            values.insert("required_double".to_owned(), Value::F64(number));
            values.insert("optional_double".to_owned(), Value::Null);
            context
                .check_and_fix_values("FloatProbe", &mut values)
                .unwrap();
            let Value::F64(actual) = values.get("required_double").unwrap() else {
                panic!("scalar changed")
            };
            assert_eq!(actual.to_bits(), number.to_bits());
            assert_eq!(values.get("optional_double"), Some(&Value::Null));
        }
    }
    struct FixDouble {
        value: f64,
        calls: Arc<AtomicUsize>,
    }
    impl Checker for FixDouble {
        fn entity(&self) -> &str {
            "FloatProbe"
        }
        fn check_and_fix(
            &self,
            _context: &UserContext,
            values: &mut EntityValues,
            _location: &ObjectLocation,
            _results: &mut CheckResults,
        ) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            values.insert("required_double".to_owned(), Value::F64(self.value));
        }
    }
    #[test]
    fn finite_float_custom_fix_gets_one_chance_before_final_admission() {
        let calls = Arc::new(AtomicUsize::new(0));
        let context = context().with_checker_registry(InMemoryCheckerRegistry::new().with_checker(
            FixDouble {
                value: 2.5,
                calls: calls.clone(),
            },
        ));
        let mut values = values();
        values.insert("required_double".to_owned(), Value::F64(f64::INFINITY));
        context
            .check_and_fix_values("FloatProbe", &mut values)
            .unwrap();
        assert_eq!(values.get("required_double"), Some(&Value::F64(2.5)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn finite_float_custom_fix_cannot_introduce_a_nonfinite_value() {
        let calls = Arc::new(AtomicUsize::new(0));
        let context = context().with_checker_registry(InMemoryCheckerRegistry::new().with_checker(
            FixDouble {
                value: f64::INFINITY,
                calls: calls.clone(),
            },
        ));
        let error = context
            .check_and_fix_values("FloatProbe", &mut values())
            .expect_err("bad Fix must be rejected");
        invalid_result(error, "required_double");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn finite_float_sparse_final_create_admission_is_metadata_only() {
        let context = context();
        let mut values = values();
        values.insert("optional_double".to_owned(), Value::F64(f64::INFINITY));
        let error = context
            .validate_required_create_payload("FloatProbe", &values, &ObjectLocation::root())
            .expect_err("sparse final payload must preserve finite scalar rules");
        invalid_result(error, "optional_double");
    }
}
