use async_trait::async_trait;
use serde::Serialize;
use std::collections::HashMap;

/// Health status — compatible with Spring Boot Actuator output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum HealthStatus {
    #[serde(rename = "UP")]
    Up,
    #[serde(rename = "DOWN")]
    Down,
    #[serde(rename = "UNKNOWN")]
    Unknown,
    /// Service is starting up: liveness = true, readiness = false.
    #[serde(rename = "OUT_OF_SERVICE")]
    OutOfService,
}

impl HealthStatus {
    /// Aggregate multiple statuses:
    /// - Any `Down` → `Down`
    /// - Any `OutOfService` → `OutOfService`
    /// - All `Up` → `Up`
    /// - Otherwise → `Unknown`
    pub fn aggregate(statuses: &[HealthStatus]) -> HealthStatus {
        if statuses.is_empty() {
            return HealthStatus::Unknown;
        }
        if statuses.contains(&HealthStatus::Down) {
            HealthStatus::Down
        } else if statuses.contains(&HealthStatus::OutOfService) {
            HealthStatus::OutOfService
        } else if statuses.iter().all(|s| *s == HealthStatus::Up) {
            HealthStatus::Up
        } else {
            HealthStatus::Unknown
        }
    }
}

/// Health detail for a single component.
#[derive(Debug, Clone, Serialize)]
pub struct HealthDetail {
    pub status: HealthStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl HealthDetail {
    /// Component is healthy with no additional details.
    pub fn up() -> Self {
        Self {
            status: HealthStatus::Up,
            details: None,
        }
    }

    /// Component is down with an error reason.
    pub fn down(reason: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Down,
            details: Some(serde_json::json!({ "error": reason.into() })),
        }
    }

    /// Component is healthy with additional diagnostic details.
    pub fn up_with_details(details: serde_json::Value) -> Self {
        Self {
            status: HealthStatus::Up,
            details: Some(details),
        }
    }
}

/// Health indicator — each component to be checked implements this trait.
///
/// Built-in implementations (in actuator crate):
/// - `DiskSpaceHealthIndicator`
///
/// Backend-provided implementations:
/// - `NacosHealthIndicator` (in `teaql-cloud-nacos`)
#[async_trait]
pub trait HealthIndicator: Send + Sync {
    /// Component name (e.g. "db", "nacos", "diskSpace").
    fn name(&self) -> &str;

    /// Perform the health check.
    async fn check(&self) -> HealthDetail;

    /// Whether this indicator contributes to readiness determination.
    ///
    /// Set to `false` for components that should not affect traffic routing
    /// (e.g. optional caches).
    fn contributes_to_readiness(&self) -> bool {
        true
    }
}

/// Aggregated health response — compatible with Spring Boot Actuator format.
#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub components: HashMap<String, HealthDetail>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_aggregate_all_up() {
        let statuses = vec![HealthStatus::Up, HealthStatus::Up, HealthStatus::Up];
        assert_eq!(HealthStatus::aggregate(&statuses), HealthStatus::Up);
    }

    #[test]
    fn test_health_status_aggregate_any_down() {
        let statuses = vec![HealthStatus::Up, HealthStatus::Down, HealthStatus::Up];
        assert_eq!(HealthStatus::aggregate(&statuses), HealthStatus::Down);
    }

    #[test]
    fn test_health_status_aggregate_out_of_service() {
        let statuses = vec![HealthStatus::Up, HealthStatus::OutOfService];
        assert_eq!(
            HealthStatus::aggregate(&statuses),
            HealthStatus::OutOfService
        );
    }

    #[test]
    fn test_health_status_aggregate_down_beats_out_of_service() {
        let statuses = vec![HealthStatus::Down, HealthStatus::OutOfService];
        assert_eq!(HealthStatus::aggregate(&statuses), HealthStatus::Down);
    }

    #[test]
    fn test_health_status_aggregate_empty() {
        assert_eq!(HealthStatus::aggregate(&[]), HealthStatus::Unknown);
    }

    #[test]
    fn test_health_status_aggregate_with_unknown() {
        let statuses = vec![HealthStatus::Up, HealthStatus::Unknown];
        assert_eq!(HealthStatus::aggregate(&statuses), HealthStatus::Unknown);
    }

    #[test]
    fn test_health_detail_up() {
        let detail = HealthDetail::up();
        assert_eq!(detail.status, HealthStatus::Up);
        assert!(detail.details.is_none());
    }

    #[test]
    fn test_health_detail_down() {
        let detail = HealthDetail::down("connection refused");
        assert_eq!(detail.status, HealthStatus::Down);
        let details = detail.details.unwrap();
        assert_eq!(details["error"], "connection refused");
    }

    #[test]
    fn test_health_detail_up_with_details() {
        let detail = HealthDetail::up_with_details(serde_json::json!({
            "database": "PostgreSQL",
            "latency_ms": 2
        }));
        assert_eq!(detail.status, HealthStatus::Up);
        let details = detail.details.unwrap();
        assert_eq!(details["database"], "PostgreSQL");
        assert_eq!(details["latency_ms"], 2);
    }

    #[test]
    fn test_health_response_serialization() {
        let mut components = HashMap::new();
        components.insert("db".to_string(), HealthDetail::up());
        components.insert("nacos".to_string(), HealthDetail::down("unreachable"));

        let response = HealthResponse {
            status: HealthStatus::Down,
            components,
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["status"], "DOWN");
        assert_eq!(json["components"]["db"]["status"], "UP");
        assert_eq!(json["components"]["nacos"]["status"], "DOWN");
    }

    #[test]
    fn test_health_status_serde() {
        assert_eq!(serde_json::to_string(&HealthStatus::Up).unwrap(), "\"UP\"");
        assert_eq!(
            serde_json::to_string(&HealthStatus::Down).unwrap(),
            "\"DOWN\""
        );
        assert_eq!(
            serde_json::to_string(&HealthStatus::OutOfService).unwrap(),
            "\"OUT_OF_SERVICE\""
        );
    }

    struct AlwaysUpIndicator;

    #[async_trait]
    impl HealthIndicator for AlwaysUpIndicator {
        fn name(&self) -> &str {
            "always_up"
        }

        async fn check(&self) -> HealthDetail {
            HealthDetail::up()
        }
    }

    #[tokio::test]
    async fn test_health_indicator_trait() {
        let indicator = AlwaysUpIndicator;
        assert_eq!(indicator.name(), "always_up");
        assert!(indicator.contributes_to_readiness());
        let detail = indicator.check().await;
        assert_eq!(detail.status, HealthStatus::Up);
    }
}
