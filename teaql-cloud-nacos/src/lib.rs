//! # teaql-cloud-nacos
//!
//! Nacos v2 gRPC implementation for TeaQL cloud integration.
//!
//! Wraps the `nacos-sdk` crate (pinned to `=0.8`) and implements all traits
//! from `teaql-cloud-core`:
//!
//! - `ServiceRegistry` — register/deregister via NamingService
//! - `ServiceDiscovery` — select_instances/subscribe via NamingService
//! - `ConfigSource` — get_config/publish_config/watch via ConfigService
//! - `HealthIndicator` — gRPC connection status
//! - `MetricsCollector` — connection alive gauge
//!
//! ## Nacos v2 gRPC
//!
//! Nacos 2.x uses gRPC by default on port `server_port + 1000` (e.g. 9848).
//! The persistent gRPC connection serves as the heartbeat — no explicit
//! heartbeat requests are needed.
//!
//! ## Usage
//!
//! ```ignore
//! use teaql_cloud_nacos::{NacosCloud, NacosConfig};
//!
//! let config = NacosConfig::new("127.0.0.1:8848")
//!     .with_namespace("production")
//!     .with_app_name("order-service");
//!
//! let cloud = NacosCloud::connect(config).await?;
//!
//! // Use cloud as ServiceRegistry, ServiceDiscovery, ConfigSource, etc.
//! cloud.register(&instance).await?;
//! ```

mod cloud;
mod config;

pub use cloud::NacosCloud;
pub use config::{NacosAuth, NacosConfig};
