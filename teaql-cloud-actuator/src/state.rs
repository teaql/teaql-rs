use serde::Serialize;
use std::sync::Arc;
use teaql_cloud_core::{HealthIndicator, MetricsCollector};

/// Shared state for all actuator endpoints.
#[derive(Clone)]
pub struct ActuatorState {
    pub indicators: Vec<Arc<dyn HealthIndicator>>,
    pub collectors: Vec<Arc<dyn MetricsCollector>>,
    pub info: ServiceInfo,
}

/// Service build and runtime information.
///
/// Displayed at `GET /actuator/info`.
#[derive(Debug, Clone, Serialize)]
pub struct ServiceInfo {
    pub name: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rust_version: Option<String>,
    pub profile: String,
}

impl Default for ServiceInfo {
    fn default() -> Self {
        Self {
            name: String::new(),
            version: String::new(),
            git_commit: None,
            build_time: None,
            rust_version: None,
            profile: if cfg!(debug_assertions) {
                "debug".to_string()
            } else {
                "release".to_string()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_info_default() {
        let info = ServiceInfo::default();
        assert_eq!(info.profile, "debug"); // tests always run in debug
        assert!(info.git_commit.is_none());
        assert!(info.build_time.is_none());
    }

    #[test]
    fn test_service_info_serialization() {
        let info = ServiceInfo {
            name: "order-service".to_string(),
            version: "1.0.0".to_string(),
            git_commit: Some("abc123".to_string()),
            build_time: None,
            rust_version: Some("1.87.0".to_string()),
            profile: "release".to_string(),
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["name"], "order-service");
        assert_eq!(json["git_commit"], "abc123");
        // build_time is None, should be skipped
        assert!(json.get("build_time").is_none());
    }
}
