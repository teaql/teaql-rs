//! Synchronous typed values for automatic Context assignments. The split
//! Checker's per-entity eligibility gate decides whether to call this service.
use std::sync::Arc;

use aes_gcm::aead::{OsRng, rand_core::RngCore};
use serde::de::DeserializeOwned;
use teaql_core::{Value, time::Timestamp};

use crate::{CheckObjectStatus, CheckResult, CheckResults, CheckRule, ObjectLocation, UserContext};

#[derive(Debug, Clone)]
pub struct ContextFixInput<'a> {
    pub entity: &'a str,
    pub field: &'a str,
    pub function: &'a str,
    pub arguments: &'a [String],
    pub status: CheckObjectStatus,
    pub now: Timestamp,
}

/// Deliberately opaque: provider secrets/errors cannot become public causes.
#[derive(Debug, Clone, Copy)]
pub struct ContextFixError;

pub trait ContextFixResolver: Send + Sync {
    fn resolve(
        &self,
        context: &UserContext,
        input: &ContextFixInput<'_>,
    ) -> Result<Value, ContextFixError>;
}

struct Provider(Arc<dyn ContextFixResolver>);

impl UserContext {
    pub fn with_context_fix_resolver(
        mut self,
        resolver: impl ContextFixResolver + 'static,
    ) -> Self {
        self.insert_resource(Provider(Arc::new(resolver)));
        self
    }

    /// Use only inside an eligible automatic Fix hook. HTTP/async results must
    /// be prepared before Save; this interface cannot await external work.
    #[allow(
        clippy::too_many_arguments,
        reason = "Generated Fix calls carry independent model, lifecycle and diagnostic inputs; retain their typed contract"
    )]
    pub fn context_fix_value<T: DeserializeOwned>(
        &self,
        entity: &str,
        field: &str,
        function: &str,
        arguments: &[String],
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) -> Option<T> {
        let input = ContextFixInput {
            entity,
            field,
            function,
            arguments,
            status,
            now: self.fix_time(),
        };
        let value = if let Some(provider) = self.get_resource::<Provider>() {
            provider.0.resolve(self, &input)
        } else {
            match function {
                "now" => Ok(Value::I64(input.now.0)),
                "uniStr" => {
                    let mut random = [0_u8; 16];
                    // A CSPRNG failure is a rejected Context field, never a
                    // time/counter fallback or a panic with raw diagnostics.
                    OsRng
                        .try_fill_bytes(&mut random)
                        .map_err(|_| ContextFixError)
                        .map(|_| {
                            let mut text = arguments.first().cloned().unwrap_or_default();
                            use std::fmt::Write;
                            for byte in random {
                                let _ = write!(text, "{byte:02x}");
                            }
                            Value::Text(text)
                        })
                }
                _ => Err(ContextFixError),
            }
        };
        let typed = value
            .ok()
            .filter(finite)
            .and_then(|value| serde_json::from_value(value.to_json_value()).ok());
        if typed.is_none() {
            results.push(
                CheckResult::new(CheckRule::InvalidType, location.clone().member(field))
                    .with_entity_type(entity)
                    .with_message("Context Fix failed for this field"),
            );
        }
        typed
    }
}

fn finite(value: &Value) -> bool {
    match value {
        Value::F64(value) => value.is_finite(),
        Value::List(values) => values.iter().all(finite),
        Value::Object(record) => record.values().all(finite),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FixedBusinessClock;
    fn context() -> UserContext {
        UserContext::new().with_business_clock(FixedBusinessClock::new(
            chrono::DateTime::from_timestamp_millis(17_000).unwrap(),
        ))
    }
    #[test]
    fn defaults_use_business_clock_and_typed_unique_string() {
        let context = context();
        let mut results = vec![];
        let now: Option<Timestamp> = context.context_fix_value(
            "Probe",
            "born_at",
            "now",
            &[],
            CheckObjectStatus::Create,
            &ObjectLocation::root(),
            &mut results,
        );
        assert_eq!(now, Some(Timestamp(17_000)));
        let args = ["prefix-".into()];
        let first: String = context
            .context_fix_value(
                "Probe",
                "token",
                "uniStr",
                &args,
                CheckObjectStatus::Create,
                &ObjectLocation::root(),
                &mut results,
            )
            .unwrap();
        let second: String = context
            .context_fix_value(
                "Probe",
                "token",
                "uniStr",
                &args,
                CheckObjectStatus::Create,
                &ObjectLocation::root(),
                &mut results,
            )
            .unwrap();
        assert!(first.starts_with("prefix-"));
        assert_ne!(first, second);
        assert!(results.is_empty());
    }
    struct Resolver(u8);
    impl ContextFixResolver for Resolver {
        fn resolve(
            &self,
            _: &UserContext,
            _: &ContextFixInput<'_>,
        ) -> Result<Value, ContextFixError> {
            match self.0 {
                0 => Ok(Value::Text("prepared IP".into())),
                1 => Err(ContextFixError),
                2 => Ok(Value::I64(1)),
                _ => Ok(Value::F64(f64::NAN)),
            }
        }
    }
    #[test]
    fn custom_prepared_values_are_typed() {
        let context = context().with_context_fix_resolver(Resolver(0));
        let mut results = vec![];
        let ip: Option<String> = context.context_fix_value(
            "Probe",
            "source_ip",
            "ip",
            &[],
            CheckObjectStatus::Update,
            &ObjectLocation::root(),
            &mut results,
        );
        assert_eq!(ip.as_deref(), Some("prepared IP"));
        assert!(results.is_empty());
    }
    #[test]
    fn provider_type_and_nonfinite_failures_are_safe_and_indexed() {
        for mode in [1, 2, 3] {
            let context = context().with_context_fix_resolver(Resolver(mode));
            let mut results = vec![];
            let value: Option<String> = context.context_fix_value(
                "Probe",
                "source_ip",
                "ip",
                &[],
                CheckObjectStatus::Update,
                &ObjectLocation::root().member("entry_list").element(1),
                &mut results,
            );
            assert!(value.is_none());
            assert_eq!(results.len(), 1);
            let result = &results[0];
            assert_eq!(result.location.instance_path(), "/entryList/1/sourceIp");
            assert_eq!(result.entity_type.as_deref(), Some("Probe"));
            assert!(result.input_value.is_none() && result.system_value.is_none());
            assert_eq!(
                result.message.as_deref(),
                Some("Context Fix failed for this field")
            );
        }
    }
}
