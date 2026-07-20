//! # teaql-cloud-actuator
//!
//! Health, info, and metrics endpoints for TeaQL cloud integration.
//!
//! Provides Spring Boot Actuator-compatible HTTP endpoints via Axum:
//!
//! - `GET /actuator/health` — aggregated health status
//! - `GET /actuator/health/liveness` — process alive (K8s liveness probe)
//! - `GET /actuator/health/readiness` — traffic ready (K8s readiness probe)
//! - `GET /actuator/info` — service build/runtime info
//! - `GET /actuator/metrics` — Prometheus exposition format
//!
//! ## Usage
//!
//! ```ignore
//! use teaql_cloud_actuator::{actuator_routes, ActuatorState, ServiceInfo};
//!
//! let state = ActuatorState {
//!     indicators: vec![Arc::new(my_health_indicator)],
//!     collectors: vec![Arc::new(my_metrics_collector)],
//!     info: ServiceInfo { name: "my-service".into(), version: "1.0".into(), ..Default::default() },
//! };
//!
//! let app = Router::new()
//!     .nest("/api", my_routes())
//!     .merge(actuator_routes(state));
//! ```

mod health_handler;
mod info_handler;
mod metrics_handler;
mod state;

pub use state::{ActuatorState, ServiceInfo};

use axum::Router;
use axum::routing::get;

/// Build the actuator router with all endpoints.
///
/// Endpoints:
/// - `GET /actuator/health` — aggregated health (200 if UP, 503 otherwise)
/// - `GET /actuator/health/liveness` — always 200 UP
/// - `GET /actuator/health/readiness` — readiness-contributing indicators only
/// - `GET /actuator/info` — service info JSON
/// - `GET /actuator/metrics` — Prometheus exposition format (text/plain)
pub fn actuator_routes(state: ActuatorState) -> Router {
    Router::new()
        .route("/actuator/health", get(health_handler::health_handler))
        .route(
            "/actuator/health/liveness",
            get(health_handler::liveness_handler),
        )
        .route(
            "/actuator/health/readiness",
            get(health_handler::readiness_handler),
        )
        .route("/actuator/info", get(info_handler::info_handler))
        .route("/actuator/metrics", get(metrics_handler::metrics_handler))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::sync::Arc;
    use teaql_cloud_core::{HealthDetail, HealthIndicator, Metric, MetricsCollector};
    use tower::ServiceExt;

    struct TestIndicator;

    #[async_trait::async_trait]
    impl HealthIndicator for TestIndicator {
        fn name(&self) -> &str {
            "test"
        }
        async fn check(&self) -> HealthDetail {
            HealthDetail::up()
        }
    }

    struct TestCollector;

    #[async_trait::async_trait]
    impl MetricsCollector for TestCollector {
        async fn collect(&self) -> Vec<Metric> {
            vec![Metric::gauge("test_gauge", "A test gauge", 1.0)]
        }
    }

    #[tokio::test]
    async fn test_all_routes_accessible() {
        let state = ActuatorState {
            indicators: vec![Arc::new(TestIndicator)],
            collectors: vec![Arc::new(TestCollector)],
            info: ServiceInfo {
                name: "integration-test".to_string(),
                version: "0.1.0".to_string(),
                ..ServiceInfo::default()
            },
        };

        let app = actuator_routes(state);

        let paths = [
            "/actuator/health",
            "/actuator/health/liveness",
            "/actuator/health/readiness",
            "/actuator/info",
            "/actuator/metrics",
        ];

        for path in &paths {
            let response = app
                .clone()
                .oneshot(Request::get(*path).body(Body::empty()).unwrap())
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK, "Expected 200 for {path}");
        }
    }
}
