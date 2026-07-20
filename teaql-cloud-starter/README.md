# teaql-cloud-starter

One-line cloud bootstrap for TeaQL Rust services — the Spring Boot Starter equivalent.

## Overview

Packages all cloud capabilities into a single `CloudApp` builder:

- Nacos v2 gRPC connection
- Service registration & discovery
- Configuration center
- Spring Boot Actuator-compatible endpoints (health/info/metrics)
- Graceful shutdown (SIGINT/SIGTERM)

## Usage

```rust
use teaql_cloud_starter::CloudApp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    CloudApp::new()
        .nacos("127.0.0.1:8848")
        .namespace("production")
        .service_name("order-service-rust")
        .port(8080)
        .routes(my_business_routes())
        .start()
        .await?;
    // Graceful shutdown is handled automatically
    Ok(())
}
```

## What `start()` Does

1. **Connect** to Nacos via gRPC
2. **Register** service instance
3. **Assemble** Axum Router (your routes + actuator routes)
4. **Start** HTTP server
5. **Wait** for SIGINT/SIGTERM
6. **Shutdown**: readiness → OUT_OF_SERVICE → deregister → drain → exit

## Auto-injected Components

- `NacosCloud` as `HealthIndicator` (nacos connection health)
- `NacosCloud` as `MetricsCollector` (connection alive gauge)
