# teaql-cloud-actuator

Spring Boot Actuator-compatible health, info, and metrics endpoints for Axum.

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/actuator/health` | GET | Aggregated health status (200 UP / 503 DOWN) |
| `/actuator/health/liveness` | GET | Always UP — K8s liveness probe |
| `/actuator/health/readiness` | GET | Readiness-contributing indicators — K8s readiness probe |
| `/actuator/info` | GET | Service build/runtime info (JSON) |
| `/actuator/metrics` | GET | Prometheus exposition format (text/plain) |

## Usage

```rust
use teaql_cloud_actuator::{actuator_routes, ActuatorState, ServiceInfo};

let state = ActuatorState {
    indicators: vec![Arc::new(my_health_indicator)],
    collectors: vec![Arc::new(my_metrics_collector)],
    info: ServiceInfo {
        name: "my-service".into(),
        version: "1.0".into(),
        ..Default::default()
    },
};

let app = Router::new()
    .nest("/api", my_routes())
    .merge(actuator_routes(state));
```

## Spring Boot Compatibility

The `/actuator/health` JSON output is compatible with Spring Boot Actuator format:

```json
{
  "status": "UP",
  "components": {
    "db": { "status": "UP", "details": { "database": "PostgreSQL" } },
    "nacos": { "status": "UP" }
  }
}
```

## Kubernetes Integration

- **Liveness probe**: `GET /actuator/health/liveness` — always returns 200 if process is running
- **Readiness probe**: `GET /actuator/health/readiness` — returns 503 during shutdown (OUT_OF_SERVICE)
