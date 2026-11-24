# teaql-cloud-consul

Consul HTTP agent integration for TeaQL cloud services.

The crate implements TeaQL's backend-neutral `ServiceRegistry`,
`HealthIndicator`, and `MetricsCollector` contracts. Registration preserves the
caller's explicit instance ID and metadata, installs an HTTP health check for
`/actuator/health`, and supports Consul ACL tokens.

```rust
use teaql_cloud_consul::{ConsulCloud, ConsulConfig};
use teaql_cloud_core::{ServiceInstance, ServiceRegistry};

# async fn register() -> Result<(), Box<dyn std::error::Error>> {
let consul = ConsulCloud::connect(
    ConsulConfig::new("127.0.0.1:8500").with_token("consul-token"),
).await?;
let instance = ServiceInstance::new("order-service", "10.0.0.8", 8080)
    .with_instance_id("order-service-rust-1")
    .with_metadata("runtime", "rust");
consul.register(&instance).await?;
# Ok(())
# }
```

For an executable real-server check:

```bash
TEAQL_TEST_CONSUL_ADDR=127.0.0.1:8500 \
  cargo run -p teaql-cloud-consul --example consul_conformance
```

The conformance example registers a uniquely named instance, reads it back,
checks metadata, health and metrics, deregisters it, and confirms cleanup.
