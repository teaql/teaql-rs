use super::UserContext;
use crate::{CheckResult, CheckResults, CheckRule, ObjectLocation};
use teaql_core::{DataType, Value};

impl UserContext {
    /// A modeled Boolean uses a typed carrier, never host-language truthiness.
    /// Custom Fix runs first; admission checks its final value before I/O.
    pub(super) fn collect_boolean_property_results(
        &self,
        entity: &str,
        values: &std::collections::BTreeMap<String, Value>,
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
        for property in &descriptor.properties {
            if property.data_type != DataType::Bool {
                continue;
            }
            let canonical =
                crate::ledger_keys::canonical_field_from_storage(descriptor, &property.name)
                    .unwrap_or_else(|_| property.name.clone());
            let Some(value) = values
                .get(&canonical)
                .or_else(|| values.get(&property.name))
            else {
                continue;
            };
            // Preserve absence/null handling; this check must not synthesize
            // false for null, nor mark an unselected field as loaded.
            if matches!(value, Value::Bool(_) | Value::Null | Value::TypedNull(_)) {
                continue;
            }
            let field_location = location.clone().member(&canonical);
            if !results.iter().any(|result| {
                result.rule == CheckRule::InvalidType && result.location == field_location
            }) {
                results.push(
                    CheckResult::new(CheckRule::InvalidType, field_location)
                        .with_entity_type(entity)
                        .with_message(format!("invalid field {canonical}: expected Bool")),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        CheckRule, EntityValues, InMemoryMetadataStore, ObjectLocation, RuntimeError, UserContext,
    };
    use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor, Value};

    #[test]
    fn sparse_final_create_payload_validates_boolean_without_repeating_fix() {
        let context = UserContext::new().with_metadata(
            InMemoryMetadataStore::new().with_entity(
                EntityDescriptor::new("BooleanProbe")
                    .property(PropertyDescriptor::new("optional_flag", DataType::Bool)),
            ),
        );
        let mut values = EntityValues::default();
        values.insert("optional_flag".to_owned(), Value::I64(1));
        let error = context
            .validate_required_create_payload(
                "BooleanProbe",
                &values,
                &ObjectLocation::root().member("child"),
            )
            .expect_err("sparse final CREATE must not bypass typed Boolean admission");
        let RuntimeError::Check(results) = error else {
            panic!("expected structured Boolean diagnostic")
        };
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].rule, CheckRule::InvalidType);
        assert_eq!(results[0].location.model_path(), "child.optional_flag");
        assert_eq!(values.get("optional_flag"), Some(&Value::I64(1)));
        for value in [Value::Bool(true), Value::Bool(false), Value::Null] {
            values.insert("optional_flag".to_owned(), value);
            context
                .validate_required_create_payload("BooleanProbe", &values, &ObjectLocation::root())
                .unwrap();
        }
    }
}
