//! Integration tests: CloudApp builder configuration.
//!
//! These tests verify the builder pattern works correctly.
//! Note: `CloudApp::start()` cannot be tested without a real Nacos server,
//! so we only test the builder state.

use teaql_cloud_starter::CloudApp;

#[test]
fn test_cloud_app_full_builder() {
    let app = CloudApp::new()
        .nacos("10.0.0.1:8848,10.0.0.2:8848")
        .namespace("production")
        .service_name("order-service-rust")
        .ip("192.168.1.100")
        .port(9090)
        .version("4.1.1")
        .group("CLOUD_GROUP")
        .auth("admin", "secure-password");

    // Verify builder correctly captures all fields
    // (fields are private, but we can at least verify no panic)
    drop(app);
}

#[test]
fn test_cloud_app_minimal_builder() {
    let app = CloudApp::new()
        .nacos("127.0.0.1:8848")
        .service_name("my-service");

    drop(app);
}

#[test]
fn test_cloud_app_with_custom_routes() {
    use axum::Router;
    use axum::routing::get;

    let routes = Router::new().route("/hello", get(|| async { "Hello, World!" }));

    let app = CloudApp::new()
        .nacos("127.0.0.1:8848")
        .service_name("hello-service")
        .routes(routes);

    drop(app);
}

#[test]
fn test_cloud_app_with_custom_indicators() {
    use async_trait::async_trait;
    use std::sync::Arc;
    use teaql_cloud_core::{HealthDetail, HealthIndicator};

    struct CustomIndicator;

    #[async_trait]
    impl HealthIndicator for CustomIndicator {
        fn name(&self) -> &str {
            "custom"
        }
        async fn check(&self) -> HealthDetail {
            HealthDetail::up()
        }
    }

    let app = CloudApp::new()
        .nacos("127.0.0.1:8848")
        .service_name("custom-service")
        .health_indicator(Arc::new(CustomIndicator));

    drop(app);
}

#[test]
fn test_cloud_app_default() {
    let app = CloudApp::default();
    drop(app);
}
