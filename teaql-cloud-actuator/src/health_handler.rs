use std::collections::HashMap;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use teaql_cloud_core::{HealthDetail, HealthResponse, HealthStatus};

use crate::ActuatorState;

/// `GET /actuator/health`
///
/// Aggregated health check across all registered indicators.
/// Response format is compatible with Spring Boot Actuator.
///
/// Returns HTTP 200 if UP, HTTP 503 if DOWN or OUT_OF_SERVICE.
pub async fn health_handler(State(state): State<ActuatorState>) -> impl IntoResponse {
    let (status, components) = check_all(&state).await;

    let response = HealthResponse { status, components };

    let http_status = match status {
        HealthStatus::Up => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };

    (http_status, Json(response))
}

/// `GET /actuator/health/liveness`
///
/// Always returns UP if the process is running.
/// Used by Kubernetes liveness probe — determines whether to **restart** the container.
pub async fn liveness_handler() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "UP" }))
}

/// `GET /actuator/health/readiness`
///
/// Aggregates only indicators where `contributes_to_readiness() == true`.
/// Used by Kubernetes readiness probe — determines whether to **route traffic**.
///
/// During shutdown, returns OUT_OF_SERVICE so the gateway stops routing.
pub async fn readiness_handler(State(state): State<ActuatorState>) -> impl IntoResponse {
    let mut components = HashMap::new();
    let mut statuses = Vec::new();

    for indicator in &state.indicators {
        if indicator.contributes_to_readiness() {
            let detail = indicator.check().await;
            statuses.push(detail.status);
            components.insert(indicator.name().to_string(), detail);
        }
    }

    let status = HealthStatus::aggregate(&statuses);
    let response = HealthResponse { status, components };

    let http_status = match status {
        HealthStatus::Up => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };

    (http_status, Json(response))
}

/// Check all health indicators and return aggregated status + component details.
async fn check_all(state: &ActuatorState) -> (HealthStatus, HashMap<String, HealthDetail>) {
    let mut components = HashMap::new();
    let mut statuses = Vec::new();

    for indicator in &state.indicators {
        let detail = indicator.check().await;
        statuses.push(detail.status);
        components.insert(indicator.name().to_string(), detail);
    }

    let status = HealthStatus::aggregate(&statuses);
    (status, components)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ServiceInfo;
    use async_trait::async_trait;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use std::sync::Arc;
    use teaql_cloud_core::HealthIndicator;
    use tower::ServiceExt;

    struct UpIndicator;

    #[async_trait]
    impl HealthIndicator for UpIndicator {
        fn name(&self) -> &str {
            "db"
        }
        async fn check(&self) -> HealthDetail {
            HealthDetail::up_with_details(serde_json::json!({ "database": "PostgreSQL" }))
        }
    }

    struct DownIndicator;

    #[async_trait]
    impl HealthIndicator for DownIndicator {
        fn name(&self) -> &str {
            "cache"
        }
        async fn check(&self) -> HealthDetail {
            HealthDetail::down("connection refused")
        }
    }

    struct NonReadyIndicator;

    #[async_trait]
    impl HealthIndicator for NonReadyIndicator {
        fn name(&self) -> &str {
            "optional_cache"
        }
        async fn check(&self) -> HealthDetail {
            HealthDetail::down("not configured")
        }
        fn contributes_to_readiness(&self) -> bool {
            false
        }
    }

    fn test_state(indicators: Vec<Arc<dyn HealthIndicator>>) -> ActuatorState {
        ActuatorState {
            indicators,
            collectors: vec![],
            info: ServiceInfo::default(),
        }
    }

    fn test_app(state: ActuatorState) -> Router {
        Router::new()
            .route("/actuator/health", get(health_handler))
            .route("/actuator/health/liveness", get(liveness_handler))
            .route("/actuator/health/readiness", get(readiness_handler))
            .with_state(state)
    }

    async fn get_json(app: Router, path: &str) -> (StatusCode, serde_json::Value) {
        let response = app
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();

        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        (status, json)
    }

    #[tokio::test]
    async fn test_health_all_up() {
        let state = test_state(vec![Arc::new(UpIndicator)]);
        let app = test_app(state);
        let (status, json) = get_json(app, "/actuator/health").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "UP");
        assert_eq!(json["components"]["db"]["status"], "UP");
    }

    #[tokio::test]
    async fn test_health_with_down_component() {
        let state = test_state(vec![Arc::new(UpIndicator), Arc::new(DownIndicator)]);
        let app = test_app(state);
        let (status, json) = get_json(app, "/actuator/health").await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["status"], "DOWN");
        assert_eq!(json["components"]["db"]["status"], "UP");
        assert_eq!(json["components"]["cache"]["status"], "DOWN");
    }

    #[tokio::test]
    async fn test_liveness_always_up() {
        let state = test_state(vec![Arc::new(DownIndicator)]);
        let app = test_app(state);
        let (status, json) = get_json(app, "/actuator/health/liveness").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "UP");
    }

    #[tokio::test]
    async fn test_readiness_excludes_non_contributing() {
        let state = test_state(vec![Arc::new(UpIndicator), Arc::new(NonReadyIndicator)]);
        let app = test_app(state);
        let (status, json) = get_json(app, "/actuator/health/readiness").await;

        // NonReadyIndicator is DOWN but contributes_to_readiness=false, so readiness=UP
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "UP");
        // NonReadyIndicator should not appear in readiness components
        assert!(json["components"].get("optional_cache").is_none());
    }

    #[tokio::test]
    async fn test_readiness_down_when_contributing_down() {
        let state = test_state(vec![Arc::new(DownIndicator)]);
        let app = test_app(state);
        let (status, json) = get_json(app, "/actuator/health/readiness").await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["status"], "DOWN");
    }
}
