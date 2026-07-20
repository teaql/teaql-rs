use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use teaql_cloud_core::to_prometheus_exposition;

use crate::ActuatorState;

/// `GET /actuator/metrics`
///
/// Returns metrics in Prometheus exposition format (text/plain).
///
/// Content-Type: `text/plain; version=0.0.4; charset=utf-8`
pub async fn metrics_handler(State(state): State<ActuatorState>) -> impl IntoResponse {
    let mut all_metrics = Vec::new();

    for collector in &state.collectors {
        let metrics = collector.collect().await;
        all_metrics.extend(metrics);
    }

    let body = to_prometheus_exposition(&all_metrics);

    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ActuatorState, ServiceInfo};
    use async_trait::async_trait;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use std::sync::Arc;
    use teaql_cloud_core::{Metric, MetricsCollector};
    use tower::ServiceExt;

    struct TestCollector;

    #[async_trait]
    impl MetricsCollector for TestCollector {
        async fn collect(&self) -> Vec<Metric> {
            vec![
                Metric::counter("http_requests_total", "Total HTTP requests", 42.0)
                    .with_label("method", "GET"),
                Metric::gauge("db_pool_active", "Active DB connections", 5.0),
            ]
        }
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let state = ActuatorState {
            indicators: vec![],
            collectors: vec![Arc::new(TestCollector)],
            info: ServiceInfo::default(),
        };

        let app = Router::new()
            .route("/actuator/metrics", get(metrics_handler))
            .with_state(state);

        let response = app
            .oneshot(
                Request::get("/actuator/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let content_type = response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(content_type.contains("text/plain"));
        assert!(content_type.contains("version=0.0.4"));

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();

        assert!(text.contains("# HELP http_requests_total Total HTTP requests"));
        assert!(text.contains("# TYPE http_requests_total counter"));
        assert!(text.contains("http_requests_total{method=\"GET\"} 42"));
        assert!(text.contains("# TYPE db_pool_active gauge"));
        assert!(text.contains("db_pool_active 5"));
    }

    #[tokio::test]
    async fn test_metrics_endpoint_no_collectors() {
        let state = ActuatorState {
            indicators: vec![],
            collectors: vec![],
            info: ServiceInfo::default(),
        };

        let app = Router::new()
            .route("/actuator/metrics", get(metrics_handler))
            .with_state(state);

        let response = app
            .oneshot(
                Request::get("/actuator/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(body.is_empty());
    }
}
