//! # teaql-cloud-core
//!
//! Core trait definitions and shared models for TeaQL cloud integration.
//!
//! This crate provides the abstraction layer for embedding Rust services
//! into Java microservice architectures. It defines traits for:
//!
//! - **Service Registry** — register/deregister/heartbeat
//! - **Service Discovery** — instance lookup and change subscription
//! - **Config Source** — configuration retrieval and watch
//! - **Health Indicator** — health check with Spring Boot Actuator compatibility
//! - **Metrics Collector** — metrics with Prometheus exposition format
//! - **Service Lifecycle** — graceful startup and shutdown
//!
//! ## Independence
//!
//! This crate does **not** depend on `teaql-runtime` or `teaql-core`.
//! It can be used standalone in any Rust service, not just TeaQL-based ones.
//!
//! ## Backend Implementations
//!
//! The traits defined here are implemented by backend-specific crates:
//!
//! - `teaql-cloud-nacos` — Nacos v2 gRPC
//! - `teaql-cloud-consul` — Consul (future)
//! - `teaql-cloud-etcd` — etcd (future)
//! - `teaql-cloud-k8s` — Kubernetes (future)

mod config;
mod discovery;
mod error;
mod health;
mod lifecycle;
mod metrics;
mod model;
mod registry;

// Re-export all public types
pub use config::{ConfigSource, WatchHandle};
pub use discovery::{ServiceChangeEvent, ServiceDiscovery, SubscriptionHandle};
pub use error::CloudError;
pub use health::{HealthDetail, HealthIndicator, HealthResponse, HealthStatus};
pub use lifecycle::{ServiceLifecycle, wait_for_shutdown_signal};
pub use metrics::{Metric, MetricValue, MetricsCollector, to_prometheus_exposition};
pub use model::{ConfigId, ServiceGroup, ServiceInstance};
pub use registry::ServiceRegistry;
