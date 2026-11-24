//! Project Checker output using the same field policy as audit output.
//! Original entity values and customer-owned rule definitions are not modified.
use teaql_core::Value;

use crate::{CheckResult, CheckResults, EntityValues, LocationSegment, UserContext};

const REDACTED: &str = "[REDACTED]";

pub(crate) fn translate(
    context: &UserContext,
    results: &mut CheckResults,
    source_values: Option<(&str, &EntityValues)>,
    warnings: &std::cell::RefCell<
        std::collections::BTreeSet<crate::checker_messages::CheckerMessageWarning>,
    >,
) {
    let allow = crate::log_privacy::plaintext_enabled();
    let mut secrets = Vec::new();
    // Materialization failures can have a root location and only a message,
    // without an inputValue. Protect those messages using the actual checked
    // values as well; never depend on parsing a free-form diagnostic path.
    if let Some((entity, values)) = source_values {
        for (field, value) in values {
            project(
                &mut value.clone(),
                crate::log_privacy::credential_name(field)
                    || crate::log_privacy::has_credentials(value),
                !allow && field_masked(context, Some(entity), field),
                &mut secrets,
            );
        }
    }
    let mut input_lengths = Vec::with_capacity(results.len());
    for result in results.iter_mut() {
        // Keep only the scalar length during projection, never a hidden raw input.
        input_lengths.push(crate::checker_messages::original_input_length(result));
        let credential = result.location.segments().iter().any(|segment| {
            matches!(segment, LocationSegment::Member(name) if crate::log_privacy::credential_name(name))
        });
        let masked = !allow && model_masked(context, result);
        if let Some(value) = result.input_value.as_mut() {
            project(
                value,
                credential || crate::log_privacy::has_credentials(value),
                masked,
                &mut secrets,
            );
        }
        if let Some(value) = result.system_value.as_mut() {
            // Expected numeric bounds are not the submitted secret. Structured
            // credentials in custom constraints, however, must never escape.
            project(
                value,
                crate::log_privacy::has_credentials(value)
                    || (credential
                        && matches!(
                            value,
                            Value::Text(_)
                                | Value::Binary(_)
                                | Value::List(_)
                                | Value::Object(_)
                                | Value::Json(_)
                        )),
                false,
                &mut secrets,
            );
        }
    }
    secrets.sort_by(|left, right| right.len().cmp(&left.len()).then(left.cmp(right)));
    secrets.dedup();
    for (result, input_len) in results.iter_mut().zip(input_lengths) {
        // Project custom literal nodes before interpolation. Scrubbing a whole
        // generated sentence would also destroy typed positions/bounds/lengths.
        crate::checker_messages::render(context, result, input_len, &secrets, warnings);
    }
}

fn model_masked(context: &UserContext, result: &CheckResult) -> bool {
    let field = result
        .location
        .segments()
        .iter()
        .rev()
        .find_map(|segment| match segment {
            LocationSegment::Member(name) => Some(name.as_str()),
            _ => None,
        });
    let Some(field) = field else { return false };
    field_masked(context, result.entity_type.as_deref(), field)
}

fn field_masked(context: &UserContext, entity: Option<&str>, field: &str) -> bool {
    if field.starts_with('#') {
        return true;
    }
    let Some(descriptor) = entity.and_then(|entity| context.entity(entity)) else {
        return false;
    };
    descriptor.audit_mask_fields.iter().any(|masked| {
        masked == field
            || crate::ledger_keys::canonical_field_from_storage(descriptor, masked)
                .is_ok_and(|canonical| canonical == field)
    })
}

fn project(value: &mut Value, credential: bool, masked: bool, secrets: &mut Vec<String>) {
    if (!credential && !masked) || matches!(value, Value::Null | Value::TypedNull(_)) {
        return;
    }
    let projected = if credential {
        Value::Text(REDACTED.into())
    } else if matches!(value, Value::Text(text) if text == REDACTED) {
        value.clone()
    } else {
        crate::log_privacy::mask_business_value(value)
    };
    // Re-translation must not turn an already-safe mask into a new secret,
    // rewrite a cached localized message, or mask an object sentinel twice.
    if projected != *value {
        crate::log_privacy::collect_strings(value, secrets);
        // A materialization diagnostic may use Rust Debug or JSON escaping,
        // neither of which necessarily contains the unescaped source string.
        secrets.push(format!("{value:?}"));
        secrets.push(value.to_json_value().to_string());
        if let Value::Text(text) = value {
            secrets.push(format!("{text:?}"));
        }
        *value = projected;
    }
}
