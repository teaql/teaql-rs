use crate::{NoopRuntimeTelemetry, RuntimeTelemetry};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock};

pub const TELEMETRY_ENABLED: &str = "TEAQL_TELEMETRY_ENABLED";
pub const TELEMETRY_SERVICE_NAME: &str = "TEAQL_TELEMETRY_SERVICE_NAME";
pub const TELEMETRY_OTLP_ENDPOINT: &str = "TEAQL_TELEMETRY_OTLP_ENDPOINT";
const HINT: &str = "TEAQL_TELEMETRY_ENABLED=true TEAQL_TELEMETRY_SERVICE_NAME=<application-name> TEAQL_TELEMETRY_OTLP_ENDPOINT=<collector-url>";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelemetryStartupConfiguration {
    Disabled,
    Enabled {
        service_name: String,
        endpoint: String,
    },
    Invalid(&'static str),
}

impl TelemetryStartupConfiguration {
    pub fn read(environment: &BTreeMap<String, String>) -> Self {
        let flag = environment
            .get(TELEMETRY_ENABLED)
            .map(|s| s.trim())
            .unwrap_or("");
        if flag.is_empty() || flag.eq_ignore_ascii_case("false") {
            return Self::Disabled;
        }
        if !flag.eq_ignore_ascii_case("true") {
            return Self::Invalid("invalid-enable");
        }
        let service = environment
            .get(TELEMETRY_SERVICE_NAME)
            .map(|s| s.trim())
            .unwrap_or("");
        if service.is_empty()
            || service.len() > 120
            || !service
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
        {
            return Self::Invalid("invalid-service");
        }
        let raw = environment
            .get(TELEMETRY_OTLP_ENDPOINT)
            .map(String::as_str)
            .unwrap_or("");
        let Ok(endpoint) = url::Url::parse(raw) else {
            return Self::Invalid("invalid-endpoint");
        };
        let authority = raw
            .split_once("://")
            .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or(""))
            .unwrap_or("");
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || authority.contains('@')
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Self::Invalid("invalid-endpoint");
        }
        Self::Enabled {
            service_name: service.to_owned(),
            endpoint: endpoint.to_string().trim_end_matches('/').to_owned(),
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::Disabled => format!(
                "TeaQL telemetry: supported, disabled. Enable full masked diagnostics with {HINT}"
            ),
            Self::Invalid(code) => {
                format!("TeaQL telemetry: disabled; configuration={code}. {HINT}")
            }
            Self::Enabled {
                service_name,
                endpoint,
            } => format!(
                "TeaQL telemetry: starting standard module; full recording, masked SQL; {TELEMETRY_ENABLED}=true {TELEMETRY_SERVICE_NAME}={service_name} {TELEMETRY_OTLP_ENDPOINT}={endpoint}"
            ),
        }
    }
}

pub(crate) fn notice(message: &str) {
    // Do not panic if an operator's stderr pipe is closed.
    use std::io::Write;
    let _ = writeln!(std::io::stderr().lock(), "{message}");
}

fn announce_once(message: String) {
    static ANNOUNCED: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();
    let mut announced = ANNOUNCED
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if announced.insert(message.clone()) {
        notice(&message);
    }
}

pub(crate) fn standard_runtime_telemetry() -> Arc<dyn RuntimeTelemetry> {
    let environment = [
        TELEMETRY_ENABLED,
        TELEMETRY_SERVICE_NAME,
        TELEMETRY_OTLP_ENDPOINT,
    ]
    .into_iter()
    .filter_map(|key| std::env::var(key).ok().map(|value| (key.to_owned(), value)))
    .collect();
    initialize(TelemetryStartupConfiguration::read(&environment))
}

fn initialize(config: TelemetryStartupConfiguration) -> Arc<dyn RuntimeTelemetry> {
    announce_once(config.message());
    let TelemetryStartupConfiguration::Enabled {
        service_name,
        endpoint,
    } = config
    else {
        return Arc::new(NoopRuntimeTelemetry);
    };
    #[cfg(not(feature = "standard-telemetry"))]
    {
        let _ = (service_name, endpoint);
        announce_once(format!(
            "TeaQL telemetry: standard module unavailable; enable teaql-runtime feature standard-telemetry. {HINT}"
        ));
        Arc::new(NoopRuntimeTelemetry)
    }
    #[cfg(feature = "standard-telemetry")]
    {
        use std::sync::Weak;
        type PipelineCache =
            BTreeMap<(String, String), Weak<crate::telemetry_standard::StandardTelemetry>>;
        static PIPELINES: OnceLock<Mutex<PipelineCache>> = OnceLock::new();
        let mut pipelines = PIPELINES
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let key = (service_name.clone(), endpoint.clone());
        if let Some(existing) = pipelines
            .get(&key)
            .and_then(Weak::upgrade)
            .filter(|p| !p.is_closed())
        {
            return existing;
        }
        // reqwest blocking setup must not construct/drop its private executor inside Tokio.
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let spawned = std::thread::Builder::new()
            .name("teaql-telemetry-init".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(|| {
                    crate::telemetry_standard::build(&service_name, &endpoint)
                })
                .unwrap_or(Err("initialization-failed"));
                let _ = sender.send(result);
            });
        let pipeline = spawned
            .ok()
            .and_then(|_| {
                receiver
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .ok()
            })
            .and_then(Result::ok);
        if let Some(pipeline) = pipeline {
            pipelines.retain(|_, value| value.strong_count() > 0);
            pipelines.insert(key, Arc::downgrade(&pipeline));
            announce_once(
                "TeaQL telemetry: enabled; full recording, traces, metrics, logs and masked SQL."
                    .into(),
            );
            pipeline
        } else {
            notice(
                "TeaQL telemetry: standard module initialization failed; business continues without export.",
            );
            Arc::new(NoopRuntimeTelemetry)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn env(endpoint: &str) -> BTreeMap<String, String> {
        [
            (TELEMETRY_ENABLED, "true"),
            (TELEMETRY_SERVICE_NAME, "orders"),
            (TELEMETRY_OTLP_ENDPOINT, endpoint),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
    }
    #[test]
    fn disabled_reports_all_hints_without_echoing_unused_values() {
        let config = TelemetryStartupConfiguration::read(&BTreeMap::from([(
            TELEMETRY_SERVICE_NAME.into(),
            "PRIVATE-SERVICE".into(),
        )]));
        assert_eq!(config, TelemetryStartupConfiguration::Disabled);
        assert!(!config.message().contains("PRIVATE-SERVICE"));
        for key in [
            TELEMETRY_ENABLED,
            TELEMETRY_SERVICE_NAME,
            TELEMETRY_OTLP_ENDPOINT,
        ] {
            assert!(config.message().contains(key));
        }
        assert!(initialize(config).is_noop());
    }
    #[test]
    fn rejects_credential_and_token_urls_without_echoing_input() {
        for url in [
            "http://SECRET:password@localhost:4318",
            "http://localhost:4318?token=SECRET",
            "http://localhost:4318/#SECRET",
            "file:///SECRET",
            "http://@localhost:4318",
        ] {
            let config = TelemetryStartupConfiguration::read(&env(url));
            assert_eq!(
                config,
                TelemetryStartupConfiguration::Invalid("invalid-endpoint")
            );
            assert!(!config.message().contains("SECRET"));
        }
    }
    #[test]
    fn enables_full_recording_with_normalized_safe_settings() {
        let config = TelemetryStartupConfiguration::read(&env("http://localhost:4318/"));
        assert_eq!(
            config,
            TelemetryStartupConfiguration::Enabled {
                service_name: "orders".into(),
                endpoint: "http://localhost:4318".into()
            }
        );
        assert!(config.message().contains("full recording"));
    }
    #[test]
    fn invalid_switch_and_service_are_fixed_errors() {
        let mut input = env("http://localhost:4318");
        input.insert(TELEMETRY_ENABLED.into(), "SECRET".into());
        assert_eq!(
            TelemetryStartupConfiguration::read(&input),
            TelemetryStartupConfiguration::Invalid("invalid-enable")
        );
        input.insert(TELEMETRY_ENABLED.into(), "true".into());
        input.insert(TELEMETRY_SERVICE_NAME.into(), "SECRET / private".into());
        assert_eq!(
            TelemetryStartupConfiguration::read(&input),
            TelemetryStartupConfiguration::Invalid("invalid-service")
        );
    }
    #[cfg(not(feature = "standard-telemetry"))]
    #[test]
    fn enabling_without_optional_module_remains_fail_open() {
        assert!(
            initialize(TelemetryStartupConfiguration::read(&env(
                "http://localhost:4318"
            )))
            .is_noop()
        );
    }
    #[cfg(feature = "standard-telemetry")]
    #[test]
    fn contexts_share_owned_pipeline_until_last_reference_is_released() {
        let config = TelemetryStartupConfiguration::read(&env("http://127.0.0.1:4318"));
        let one = initialize(config.clone());
        let two = initialize(config);
        assert!(Arc::ptr_eq(&one, &two));
        assert!(!one.is_noop());
        drop(one);
        drop(two);
    }
    #[cfg(feature = "standard-telemetry")]
    #[test]
    fn initialization_never_reuses_a_closed_but_still_referenced_pipeline() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let config = TelemetryStartupConfiguration::read(&env(&endpoint));
        let first = initialize(config.clone());
        let shared = initialize(config.clone());
        assert!(Arc::ptr_eq(&first, &shared));
        first.shutdown();
        let fresh = initialize(config);
        assert!(!Arc::ptr_eq(&first, &fresh));
        assert!(!fresh.is_noop());
        fresh.shutdown();
    }
}
