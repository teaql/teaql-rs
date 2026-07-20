//! Integration tests: full actuator endpoint flow.
//!
//! Verifies all 5 endpoints work together with real health indicators
//! and metrics collectors, end-to-end through Axum.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use teaql_cloud_actuator::{ActuatorState, ServiceInfo, actuator_routes};
use teaql_cloud_core::{HealthDetail, HealthIndicator, Metric, MetricsCollector};
use tower::ServiceExt;

// --- Test indicators ---

struct DatabaseIndicator {
    healthy: bool,
}

#[async_trait]
impl HealthIndicator for DatabaseIndicator {
    fn name(&self) -> &str {
        "db"
    }
    async fn check(&self) -> HealthDetail {
        if self.healthy {
            HealthDetail::up_with_details(serde_json::json!({
                "database": "PostgreSQL",
                "pool_size": 10
            }))
        } else {
            HealthDetail::down("Connection pool exhausted")
        }
    }
}

struct NacosIndicator;

#[async_trait]
impl HealthIndicator for NacosIndicator {
    fn name(&self) -> &str {
        "nacos"
    }
    async fn check(&self) -> HealthDetail {
        HealthDetail::up_with_details(serde_json::json!({
            "server": "127.0.0.1:8848",
            "namespace": "production"
        }))
    }
}

struct NonCriticalIndicator;

#[async_trait]
impl HealthIndicator for NonCriticalIndicator {
    fn name(&self) -> &str {
        "cache"
    }
    async fn check(&self) -> HealthDetail {
        HealthDetail::down("Redis not configured")
    }
    fn contributes_to_readiness(&self) -> bool {
        false // Optional component
    }
}

// --- Test collectors ---

struct AppMetrics;

#[async_trait]
impl MetricsCollector for AppMetrics {
    async fn collect(&self) -> Vec<Metric> {
        vec![
            Metric::counter("http_requests_total", "Total HTTP requests", 1234.0)
                .with_label("method", "GET")
                .with_label("path", "/api/orders"),
            Metric::gauge("db_pool_active", "Active DB connections", 5.0),
            Metric::gauge("db_pool_idle", "Idle DB connections", 5.0),
        ]
    }
}

struct NacosMetrics;

#[async_trait]
impl MetricsCollector for NacosMetrics {
    async fn collect(&self) -> Vec<Metric> {
        vec![
            Metric::gauge("teaql_nacos_connected", "Nacos connection status", 1.0)
                .with_label("server", "127.0.0.1:8848"),
        ]
    }
}

// --- Test helpers ---

fn build_state(db_healthy: bool, include_non_critical: bool) -> ActuatorState {
    let mut indicators: Vec<Arc<dyn HealthIndicator>> = vec![
        Arc::new(DatabaseIndicator {
            healthy: db_healthy,
        }),
        Arc::new(NacosIndicator),
    ];

    if include_non_critical {
        indicators.push(Arc::new(NonCriticalIndicator));
    }

    ActuatorState {
        indicators,
        collectors: vec![Arc::new(AppMetrics), Arc::new(NacosMetrics)],
        info: ServiceInfo {
            name: "order-service-rust".to_string(),
            version: "4.1.1".to_string(),
            git_commit: Some("abc123def".to_string()),
            build_time: Some("2026-07-20T12:00:00Z".to_string()),
            rust_version: Some("1.87.0".to_string()),
            profile: "release".to_string(),
        },
    }
}

async fn get(app: axum::Router, path: &str) -> (StatusCode, String) {
    let response = app
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();

    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

async fn get_json(app: axum::Router, path: &str) -> (StatusCode, serde_json::Value) {
    let (status, body) = get(app, path).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    (status, json)
}

// --- Tests ---

#[tokio::test]
async fn test_actuator_health_all_healthy() {
    let app = actuator_routes(build_state(true, false));
    let (status, json) = get_json(app, "/actuator/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "UP");
    assert_eq!(json["components"]["db"]["status"], "UP");
    assert_eq!(json["components"]["nacos"]["status"], "UP");
    assert_eq!(
        json["components"]["db"]["details"]["database"],
        "PostgreSQL"
    );
}

#[tokio::test]
async fn test_actuator_health_db_down() {
    let app = actuator_routes(build_state(false, false));
    let (status, json) = get_json(app, "/actuator/health").await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json["status"], "DOWN");
    assert_eq!(json["components"]["db"]["status"], "DOWN");
    // Nacos should still be UP
    assert_eq!(json["components"]["nacos"]["status"], "UP");
}

#[tokio::test]
async fn test_actuator_liveness_always_up() {
    let app = actuator_routes(build_state(false, false));
    let (status, json) = get_json(app, "/actuator/health/liveness").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "UP");
}

#[tokio::test]
async fn test_actuator_readiness_ignores_non_critical() {
    // DB and Nacos healthy, cache DOWN but contributes_to_readiness=false
    let app = actuator_routes(build_state(true, true));
    let (status, json) = get_json(app, "/actuator/health/readiness").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "UP");
    // Cache should NOT appear in readiness
    assert!(json["components"].get("cache").is_none());
}

#[tokio::test]
async fn test_actuator_readiness_when_db_down() {
    let app = actuator_routes(build_state(false, true));
    let (status, json) = get_json(app, "/actuator/health/readiness").await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json["status"], "DOWN");
}

#[tokio::test]
async fn test_actuator_info_complete() {
    let app = actuator_routes(build_state(true, false));
    let (status, json) = get_json(app, "/actuator/info").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["name"], "order-service-rust");
    assert_eq!(json["version"], "4.1.1");
    assert_eq!(json["git_commit"], "abc123def");
    assert_eq!(json["build_time"], "2026-07-20T12:00:00Z");
    assert_eq!(json["rust_version"], "1.87.0");
    assert_eq!(json["profile"], "release");
}

#[tokio::test]
async fn test_actuator_metrics_prometheus_format() {
    let app = actuator_routes(build_state(true, false));
    let (status, body) = get(app, "/actuator/metrics").await;

    assert_eq!(status, StatusCode::OK);

    // Verify Prometheus exposition format
    assert!(body.contains("# HELP http_requests_total Total HTTP requests"));
    assert!(body.contains("# TYPE http_requests_total counter"));
    assert!(body.contains(r#"http_requests_total{method="GET",path="/api/orders"} 1234"#));

    assert!(body.contains("# TYPE db_pool_active gauge"));
    assert!(body.contains("db_pool_active 5"));

    assert!(body.contains("# TYPE teaql_nacos_connected gauge"));
    assert!(body.contains(r#"teaql_nacos_connected{server="127.0.0.1:8848"} 1"#));
}

#[tokio::test]
async fn test_actuator_metrics_aggregates_multiple_collectors() {
    let app = actuator_routes(build_state(true, false));
    let (_, body) = get(app, "/actuator/metrics").await;

    // Should contain metrics from both AppMetrics and NacosMetrics
    assert!(body.contains("http_requests_total"));
    assert!(body.contains("db_pool_active"));
    assert!(body.contains("db_pool_idle"));
    assert!(body.contains("teaql_nacos_connected"));
}
