# teaql-cloud-nacos

Nacos v2 gRPC implementation for TeaQL cloud integration.

## Overview

Wraps the [`nacos-sdk`](https://crates.io/crates/nacos-sdk) crate (pinned to `=0.8`) and implements all traits from `teaql-cloud-core`:

| Trait | Implementation |
|-------|---------------|
| `ServiceRegistry` | `register_instance` / `deregister_instance` |
| `ServiceDiscovery` | `select_instances` / `subscribe` |
| `ConfigSource` | `get_config` / `publish_config` / `add_listener` |
| `HealthIndicator` | gRPC connection probe |
| `MetricsCollector` | `teaql_nacos_connected` gauge |

## Nacos v2 gRPC

- Nacos 2.x uses gRPC on port `server_port + 1000` (e.g. HTTP 8848 → gRPC 9848)
- The persistent gRPC connection **IS** the heartbeat — no explicit beat needed
- `heartbeat_interval()` returns `None`

## Usage

```rust
use teaql_cloud_nacos::{NacosCloud, NacosConfig};

let config = NacosConfig::new("127.0.0.1:8848")
    .with_namespace("production")
    .with_app_name("order-service");

let cloud = NacosCloud::connect(config).await?;

// Register
cloud.register(&instance).await?;

// Discover
let instances = cloud.get_instances("user-service", &group).await?;

// Config
let config = cloud.get_config(&config_id).await?;
```

## Version Pinning

`nacos-sdk` is pinned to `=0.8` to avoid breaking changes in the 0.x series.
