# TeaQL Cloud Integration Design

> **Date**: 2026-07-20
> **Branch**: `feature/cloud-integration`
> **Status**: Design Phase

## 1. Background

将基于 teaql-rs 构建的 Rust 服务嵌入以 Java 为主的微服务架构中。Java 侧使用 **Nacos** 作为服务注册发现与配置中心。

### 1.1 Design Decisions

| 项目 | 决策 |
|------|------|
| 架构风格 | **核心 trait crate + 独立后端 crate** |
| `teaql-cloud-core` 是否依赖 `teaql-runtime` | **否**，两个体系完全独立，仅在用户应用层组合 |
| 服务发现优先后端 | **Nacos** |
| Nacos 协议版本 | **v2 gRPC**（基于 `nacos-sdk` crate v0.8，原生 gRPC 长连接） |
| Actuator 端点范围 | health（含 liveness/readiness）+ info + metrics |
| Metrics 格式 | **Prometheus exposition format** |
| Loggers | 暂不实现，后续再设计 |
| Graceful Shutdown | **实现**，集成 tokio signal |

---

## 2. Crate Overview

```
teaql-cloud-core          trait 定义 + 公共模型 (零后端依赖)
teaql-cloud-actuator      Axum 运维端点 (health / info / metrics)
teaql-cloud-nacos         Nacos v2 gRPC 实现 (基于 nacos-sdk crate, pinned =0.8)
teaql-cloud-starter       一行启动, 打包所有 cloud 能力 (类似 Spring Boot Starter)
```

### 2.1 Dependency Graph

```
teaql-cloud-core                          (serde, async-trait, tokio, thiserror)
    ↑                  ↑
teaql-cloud-actuator   teaql-cloud-nacos
    (axum)                (nacos-sdk)
                   ↑
              用户应用
         (+ teaql-runtime, teaql-web-integration-axum)
```

> **重要**: `teaql-cloud-core` 不依赖 `teaql-runtime`。
> 这意味着 cloud 模块可以脱离 teaql 独立使用（纯 Axum 项目也能用）。

### 2.2 Future Backend Crates

同一套 trait，未来可扩展：

| crate | 后端 | 协议 |
|-------|------|------|
| `teaql-cloud-consul` | Consul | HTTP Blocking Query |
| `teaql-cloud-etcd` | etcd | gRPC Watch / Lease |
| `teaql-cloud-k8s` | Kubernetes | Watch API (kube-rs) |

不同后端的差异通过 trait 统一：

| 维度 | Nacos v2 | etcd | Consul | Kubernetes |
|------|----------|------|--------|------------|
| 服务注册 | gRPC | KV + Lease | Catalog + Agent | Service/Endpoint |
| 配置变更 | gRPC Push | Watch stream | Blocking Query | Watch API |
| 心跳 | gRPC 长连接 (连接即心跳) | Lease KeepAlive | TTL Check | 无需 (kubelet) |
| 命名空间 | namespace + group | key prefix | datacenter | namespace |

---

## 3. `teaql-cloud-core` — Trait Definitions

### 3.1 Directory Structure

```
teaql-cloud-core/
├── Cargo.toml
└── src/
    ├── lib.rs              # 公共 re-export
    ├── error.rs            # CloudError 统一错误
    ├── model.rs            # ServiceInstance, ServiceGroup, ConfigId
    ├── registry.rs         # ServiceRegistry trait
    ├── discovery.rs        # ServiceDiscovery trait
    ├── config.rs           # ConfigSource trait
    ├── health.rs           # HealthIndicator trait + HealthStatus
    ├── metrics.rs          # MetricsCollector trait + Prometheus 格式化
    └── lifecycle.rs        # ServiceLifecycle, GracefulShutdown
```

### 3.2 `Cargo.toml`

```toml
[package]
name = "teaql-cloud-core"
version.workspace = true
edition.workspace = true

[dependencies]
serde = { workspace = true }
serde_json = "1"
thiserror = { workspace = true }
async-trait = { workspace = true }
tokio = { version = "1", features = ["sync", "signal"] }
```

### 3.3 `error.rs`

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CloudError {
    #[error("Discovery error: {0}")]
    Discovery(String),

    #[error("Registration error: {0}")]
    Registration(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Health check error: {0}")]
    Health(String),

    #[error("Network error: {source}")]
    Network {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Timeout after {duration:?}")]
    Timeout { duration: std::time::Duration },

    #[error("Shutdown in progress")]
    ShuttingDown,
}
```

### 3.4 `model.rs`

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 服务实例 — 所有后端的公共模型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInstance {
    pub service_name: String,
    pub instance_id: String,
    pub ip: String,
    pub port: u16,
    pub weight: f64,
    pub healthy: bool,
    pub metadata: HashMap<String, String>,
}

impl ServiceInstance {
    pub fn new(
        service_name: impl Into<String>,
        ip: impl Into<String>,
        port: u16,
    ) -> Self {
        let service_name = service_name.into();
        let ip = ip.into();
        let instance_id = format!("{}-{}-{}", service_name, ip, port);
        Self {
            service_name,
            instance_id,
            ip,
            port,
            weight: 1.0,
            healthy: true,
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}

impl Default for ServiceInstance {
    fn default() -> Self {
        Self {
            service_name: String::new(),
            instance_id: String::new(),
            ip: "127.0.0.1".to_string(),
            port: 8080,
            weight: 1.0,
            healthy: true,
            metadata: HashMap::new(),
        }
    }
}

/// 服务分组 — 不同后端映射不同概念
///
/// - Nacos: namespace + group
/// - Consul: datacenter
/// - etcd: key prefix
/// - K8s: namespace
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServiceGroup {
    pub namespace: Option<String>,
    pub group: Option<String>,
}

impl ServiceGroup {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }

    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }
}

/// 配置标识
#[derive(Debug, Clone)]
pub struct ConfigId {
    pub data_id: String,
    pub group: String,
    pub namespace: Option<String>,
}

impl ConfigId {
    pub fn new(
        data_id: impl Into<String>,
        group: impl Into<String>,
    ) -> Self {
        Self {
            data_id: data_id.into(),
            group: group.into(),
            namespace: None,
        }
    }

    pub fn with_namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }
}
```

### 3.5 `registry.rs`

```rust
use crate::{CloudError, ServiceInstance};
use std::future::Future;
use std::time::Duration;

/// 服务注册能力
///
/// 生命周期: register → heartbeat (循环) → deregister
///
/// 不同后端心跳机制:
/// - Nacos v2: gRPC 长连接本身就是心跳, heartbeat() 可以是 no-op
/// - etcd: Lease KeepAlive
/// - Consul: TTL Check + PUT
/// - K8s: 无需心跳 (kubelet 管理)
pub trait ServiceRegistry: Send + Sync {
    fn register(
        &self,
        instance: &ServiceInstance,
    ) -> impl Future<Output = Result<(), CloudError>> + Send;

    fn deregister(
        &self,
        instance: &ServiceInstance,
    ) -> impl Future<Output = Result<(), CloudError>> + Send;

    fn heartbeat(
        &self,
        instance: &ServiceInstance,
    ) -> impl Future<Output = Result<(), CloudError>> + Send;

    /// 推荐的心跳间隔
    /// Nacos v2 gRPC 长连接场景下可返回较大值或 None
    fn heartbeat_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(5))
    }
}
```

### 3.6 `discovery.rs`

```rust
use crate::{CloudError, ServiceGroup, ServiceInstance};
use std::future::Future;

/// 服务变更事件
#[derive(Debug, Clone)]
pub struct ServiceChangeEvent {
    pub service_name: String,
    pub instances: Vec<ServiceInstance>,
}

/// 服务发现能力
pub trait ServiceDiscovery: Send + Sync {
    /// 获取健康的服务实例列表
    fn get_instances(
        &self,
        service_name: &str,
        group: &ServiceGroup,
    ) -> impl Future<Output = Result<Vec<ServiceInstance>, CloudError>> + Send;

    /// 获取所有实例 (包括不健康的)
    fn get_all_instances(
        &self,
        service_name: &str,
        group: &ServiceGroup,
    ) -> impl Future<Output = Result<Vec<ServiceInstance>, CloudError>> + Send {
        self.get_instances(service_name, group)
    }

    /// 订阅服务变更
    ///
    /// Nacos v2: gRPC 服务端主动推送, 延迟极低
    /// etcd: gRPC Watch stream
    /// Consul: Blocking Query (long polling)
    fn subscribe(
        &self,
        service_name: &str,
        group: &ServiceGroup,
        callback: Box<dyn Fn(ServiceChangeEvent) + Send + Sync>,
    ) -> impl Future<Output = Result<SubscriptionHandle, CloudError>> + Send;
}

/// 取消订阅 handle
pub struct SubscriptionHandle {
    cancel: Box<dyn FnOnce() + Send>,
}

impl SubscriptionHandle {
    pub fn new(cancel: impl FnOnce() + Send + 'static) -> Self {
        Self { cancel: Box::new(cancel) }
    }

    pub fn unsubscribe(self) {
        (self.cancel)();
    }
}
```

### 3.7 `config.rs`

```rust
use crate::CloudError;
use std::future::Future;

use crate::model::ConfigId;

/// 配置源抽象
pub trait ConfigSource: Send + Sync {
    /// 获取配置 (返回原始字符串, 由调用方解析为 YAML/TOML/JSON)
    fn get_config(
        &self,
        config_id: &ConfigId,
    ) -> impl Future<Output = Result<String, CloudError>> + Send;

    /// 发布配置
    fn publish_config(
        &self,
        config_id: &ConfigId,
        content: &str,
    ) -> impl Future<Output = Result<(), CloudError>> + Send;

    /// 监听配置变更
    ///
    /// Nacos v2: gRPC Push (服务端主动推送)
    /// etcd: gRPC Watch stream
    /// Consul: Blocking Query
    fn watch(
        &self,
        config_id: &ConfigId,
        callback: Box<dyn Fn(String) + Send + Sync>,
    ) -> impl Future<Output = Result<WatchHandle, CloudError>> + Send;
}

/// 取消监听 handle
pub struct WatchHandle {
    cancel: Box<dyn FnOnce() + Send>,
}

impl WatchHandle {
    pub fn new(cancel: impl FnOnce() + Send + 'static) -> Self {
        Self { cancel: Box::new(cancel) }
    }

    pub fn unwatch(self) {
        (self.cancel)();
    }
}
```

### 3.8 `health.rs`

```rust
use serde::Serialize;
use std::collections::HashMap;
use std::future::Future;

/// 健康状态 — 兼容 Spring Boot Actuator 输出
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HealthStatus {
    Up,
    Down,
    Unknown,
    /// 启动中: liveness = true, readiness = false
    OutOfService,
}

impl HealthStatus {
    /// 聚合: 任一 Down → Down, 任一 OutOfService → OutOfService
    pub fn aggregate(statuses: &[HealthStatus]) -> HealthStatus {
        if statuses.iter().any(|s| *s == HealthStatus::Down) {
            HealthStatus::Down
        } else if statuses.iter().any(|s| *s == HealthStatus::OutOfService) {
            HealthStatus::OutOfService
        } else if statuses.iter().all(|s| *s == HealthStatus::Up) {
            HealthStatus::Up
        } else {
            HealthStatus::Unknown
        }
    }
}

/// 单个组件健康详情
#[derive(Debug, Clone, Serialize)]
pub struct HealthDetail {
    pub status: HealthStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl HealthDetail {
    pub fn up() -> Self {
        Self { status: HealthStatus::Up, details: None }
    }

    pub fn down(reason: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Down,
            details: Some(serde_json::json!({ "error": reason.into() })),
        }
    }

    pub fn up_with_details(details: serde_json::Value) -> Self {
        Self { status: HealthStatus::Up, details: Some(details) }
    }
}

/// 健康指示器 — 每个要检查的组件实现一个
pub trait HealthIndicator: Send + Sync {
    /// 组件名 (如 "db", "nacos", "diskSpace")
    fn name(&self) -> &str;

    /// 执行健康检查
    fn check(&self) -> impl Future<Output = HealthDetail> + Send;

    /// 是否参与 readiness 判定 (默认 true)
    fn contributes_to_readiness(&self) -> bool {
        true
    }
}

/// 聚合健康响应
#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub components: HashMap<String, HealthDetail>,
}
```

### 3.9 `metrics.rs`

```rust
use std::future::Future;

/// 指标类型
#[derive(Debug, Clone)]
pub enum MetricValue {
    Counter(f64),
    Gauge(f64),
    Histogram {
        sum: f64,
        count: u64,
        buckets: Vec<(f64, u64)>,
    },
}

/// 单个指标
#[derive(Debug, Clone)]
pub struct Metric {
    pub name: String,
    pub help: String,
    pub labels: Vec<(String, String)>,
    pub value: MetricValue,
}

/// 指标收集器
pub trait MetricsCollector: Send + Sync {
    fn collect(&self) -> impl Future<Output = Vec<Metric>> + Send;
}

/// 格式化为 Prometheus exposition format
pub fn to_prometheus_exposition(metrics: &[Metric]) -> String {
    let mut output = String::new();
    for metric in metrics {
        output.push_str(&format!("# HELP {} {}\n", metric.name, metric.help));
        let type_str = match &metric.value {
            MetricValue::Counter(_) => "counter",
            MetricValue::Gauge(_) => "gauge",
            MetricValue::Histogram { .. } => "histogram",
        };
        output.push_str(&format!("# TYPE {} {}\n", metric.name, type_str));

        let labels_str = if metric.labels.is_empty() {
            String::new()
        } else {
            let pairs: Vec<String> = metric.labels.iter()
                .map(|(k, v)| format!("{}=\"{}\"", k, v))
                .collect();
            format!("{{{}}}", pairs.join(","))
        };

        match &metric.value {
            MetricValue::Counter(v) | MetricValue::Gauge(v) => {
                output.push_str(&format!("{}{} {}\n", metric.name, labels_str, v));
            }
            MetricValue::Histogram { sum, count, buckets } => {
                for (le, c) in buckets {
                    output.push_str(&format!(
                        "{}_bucket{}{{le=\"{}\"}} {}\n",
                        metric.name, labels_str, le, c
                    ));
                }
                output.push_str(&format!("{}_sum{} {}\n", metric.name, labels_str, sum));
                output.push_str(&format!("{}_count{} {}\n", metric.name, labels_str, count));
            }
        }
    }
    output
}
```

### 3.10 `lifecycle.rs` — Graceful Shutdown

```rust
use crate::{CloudError, ServiceInstance, ServiceRegistry};
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// 服务生命周期 Guard
///
/// 管理: register → heartbeat loop → (shutdown signal) → deregister → 等待在途请求
///
/// 用法:
/// ```ignore
/// let guard = ServiceLifecycle::start(registry, instance).await?;
/// // ... 服务运行中 ...
/// // Ctrl+C 或 guard.shutdown() 触发:
/// //   1. 标记 readiness = false (不再接收新流量)
/// //   2. deregister (从注册中心摘除)
/// //   3. 等待 drain_duration (让在途请求完成)
/// //   4. 退出
/// ```
pub struct ServiceLifecycle {
    shutdown_tx: watch::Sender<bool>,
    heartbeat_handle: JoinHandle<()>,
    instance: ServiceInstance,
    registry: Arc<dyn ServiceRegistry>,
}

impl ServiceLifecycle {
    /// 启动服务生命周期: 注册 + 开始心跳
    pub async fn start(
        registry: Arc<dyn ServiceRegistry>,
        instance: ServiceInstance,
    ) -> Result<Self, CloudError> {
        // 1. Register
        registry.register(&instance).await?;

        // 2. Start heartbeat loop (if backend needs it)
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let heartbeat_handle = {
            let registry = registry.clone();
            let instance = instance.clone();
            let mut shutdown_rx = shutdown_rx.clone();

            tokio::spawn(async move {
                let interval = registry.heartbeat_interval();
                if let Some(interval) = interval {
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(interval) => {
                                let _ = registry.heartbeat(&instance).await;
                            }
                            _ = shutdown_rx.changed() => {
                                break;
                            }
                        }
                    }
                }
            })
        };

        Ok(Self {
            shutdown_tx,
            heartbeat_handle,
            instance,
            registry,
        })
    }

    /// 返回一个 shutdown receiver, 用于 Axum graceful shutdown
    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// 主动触发 shutdown
    pub async fn shutdown(self) -> Result<(), CloudError> {
        // 1. Signal heartbeat to stop
        let _ = self.shutdown_tx.send(true);

        // 2. Wait for heartbeat task to finish
        let _ = self.heartbeat_handle.await;

        // 3. Deregister from service registry
        self.registry.deregister(&self.instance).await?;

        Ok(())
    }
}

/// 安装系统信号处理, 在 SIGINT/SIGTERM 时自动 shutdown
///
/// 返回一个 Future, 在信号到达时完成
pub async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
```

---

## 4. `teaql-cloud-actuator` — Axum Endpoints

### 4.1 Directory Structure

```
teaql-cloud-actuator/
├── Cargo.toml
└── src/
    ├── lib.rs              # actuator_routes() 入口
    ├── state.rs            # ActuatorState
    ├── health_handler.rs   # /actuator/health, /liveness, /readiness
    ├── info_handler.rs     # /actuator/info
    └── metrics_handler.rs  # /actuator/metrics (Prometheus)
```

### 4.2 `Cargo.toml`

```toml
[package]
name = "teaql-cloud-actuator"
version.workspace = true
edition.workspace = true

[dependencies]
teaql-cloud-core = { workspace = true }
axum = { version = "0.7", features = ["macros"] }
serde = { workspace = true }
serde_json = "1"
tokio = { workspace = true }
```

### 4.3 Core Design

```rust
use axum::{Router, routing::get};
use std::sync::Arc;
use teaql_cloud_core::{HealthIndicator, MetricsCollector};

/// 共享状态
#[derive(Clone)]
pub struct ActuatorState {
    pub indicators: Vec<Arc<dyn HealthIndicator>>,
    pub collectors: Vec<Arc<dyn MetricsCollector>>,
    pub info: ServiceInfo,
}

/// 服务信息
#[derive(Debug, Clone, serde::Serialize)]
pub struct ServiceInfo {
    pub name: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rust_version: Option<String>,
    pub profile: String,
}

/// 构建 actuator 路由
///
/// 端点:
/// - GET /actuator/health           → 聚合状态 (JSON)
/// - GET /actuator/health/liveness  → 进程存活 (JSON)
/// - GET /actuator/health/readiness → 可接流量 (JSON)
/// - GET /actuator/info             → 服务信息 (JSON)
/// - GET /actuator/metrics          → Prometheus exposition (text/plain)
pub fn actuator_routes(state: ActuatorState) -> Router {
    Router::new()
        .route("/actuator/health", get(health_handler))
        .route("/actuator/health/liveness", get(liveness_handler))
        .route("/actuator/health/readiness", get(readiness_handler))
        .route("/actuator/info", get(info_handler))
        .route("/actuator/metrics", get(metrics_handler))
        .with_state(state)
}
```

### 4.4 Endpoint Response Formats

#### `GET /actuator/health`

```json
{
  "status": "UP",
  "components": {
    "db": {
      "status": "UP",
      "details": { "database": "PostgreSQL", "latency_ms": 2 }
    },
    "nacos": {
      "status": "UP",
      "details": { "server": "127.0.0.1:9848", "protocol": "gRPC" }
    },
    "diskSpace": {
      "status": "UP",
      "details": { "total_bytes": 500107862016, "free_bytes": 387842883584 }
    }
  }
}
```

#### `GET /actuator/health/liveness`

```json
{ "status": "UP" }
```

Always `UP` if the process is running. Kubernetes uses this to decide whether to **restart** the container.

#### `GET /actuator/health/readiness`

```json
{ "status": "UP" }
```

Aggregates only indicators where `contributes_to_readiness() == true`. Kubernetes uses this to decide whether to **route traffic** to the container.

During shutdown: readiness returns `OUT_OF_SERVICE` → gateway stops routing → drain period → deregister → exit.

#### `GET /actuator/info`

```json
{
  "name": "order-service-rust",
  "version": "4.1.1",
  "git_commit": "a1b2c3d",
  "build_time": "2026-07-20T12:00:00Z",
  "rust_version": "1.87.0",
  "profile": "release"
}
```

#### `GET /actuator/metrics`

Content-Type: `text/plain; version=0.0.4; charset=utf-8`

```
# HELP teaql_http_requests_total Total HTTP requests
# TYPE teaql_http_requests_total counter
teaql_http_requests_total{method="GET",path="/api/orders"} 1234

# HELP teaql_db_pool_active Active database connections
# TYPE teaql_db_pool_active gauge
teaql_db_pool_active 5

# HELP teaql_db_query_duration_seconds Database query duration
# TYPE teaql_db_query_duration_seconds histogram
teaql_db_query_duration_seconds_bucket{le="0.01"} 100
teaql_db_query_duration_seconds_bucket{le="0.1"} 150
teaql_db_query_duration_seconds_bucket{le="1.0"} 155
teaql_db_query_duration_seconds_sum 12.5
teaql_db_query_duration_seconds_count 155
```

---

## 5. `teaql-cloud-nacos` — Nacos v2 gRPC Implementation

### 5.1 Strategy: Wrap `nacos-sdk` crate

已有官方 Rust SDK: [`nacos-sdk`](https://crates.io/crates/nacos-sdk) v0.8，原生支持 Nacos v2 gRPC 协议。

**我们不重写 gRPC 协议，而是封装 `nacos-sdk` 来实现 `teaql-cloud-core` 的 trait。**

Nacos v2 gRPC 相比 v1 HTTP 的关键优势:
- **长连接**: 连接本身就是心跳，无需主动 beat
- **服务端推送**: 服务/配置变更即时推送，无需 long polling
- **性能**: gRPC binary protocol，远优于 HTTP JSON
- **未来兼容**: Nacos 3.x 将移除 HTTP API，gRPC 成为唯一协议

### 5.2 Directory Structure

```
teaql-cloud-nacos/
├── Cargo.toml
└── src/
    ├── lib.rs              # NacosCloud 统一入口
    ├── config.rs           # NacosConfig 连接配置
    ├── registry.rs         # impl ServiceRegistry
    ├── discovery.rs        # impl ServiceDiscovery
    ├── config_source.rs    # impl ConfigSource
    ├── health.rs           # impl HealthIndicator
    └── metrics.rs          # impl MetricsCollector
```

### 5.3 `Cargo.toml`

```toml
[package]
name = "teaql-cloud-nacos"
version.workspace = true
edition.workspace = true

[dependencies]
teaql-cloud-core = { workspace = true }
nacos-sdk = "=0.8"
tokio = { workspace = true }
serde = { workspace = true }
serde_json = "1"
```

### 5.4 `NacosCloud` — 统一入口

```rust
use nacos_sdk::api::config::{ConfigService, ConfigServiceBuilder};
use nacos_sdk::api::naming::{NamingService, NamingServiceBuilder};
use nacos_sdk::api::props::ClientProps;
use teaql_cloud_core::CloudError;

/// Nacos 连接配置
#[derive(Debug, Clone)]
pub struct NacosConfig {
    /// Nacos server address (e.g. "127.0.0.1:8848")
    /// SDK 会自动推算 gRPC 端口 (offset +1000 → 9848)
    pub server_addr: String,
    pub namespace: Option<String>,
    pub app_name: Option<String>,
    pub auth: Option<NacosAuth>,
}

#[derive(Debug, Clone)]
pub struct NacosAuth {
    pub username: String,
    pub password: String,
}

/// Nacos 统一客户端
///
/// 内部持有 nacos-sdk 的 NamingService + ConfigService
/// 对外暴露 teaql-cloud-core 的 trait 实现
pub struct NacosCloud {
    naming_service: Arc<dyn NamingService>,
    config_service: Arc<dyn ConfigService>,
    config: NacosConfig,
}

impl NacosCloud {
    pub async fn new(config: NacosConfig) -> Result<Self, CloudError> {
        let mut props = ClientProps::new()
            .server_addr(&config.server_addr);

        if let Some(ns) = &config.namespace {
            props = props.namespace(ns);
        }
        if let Some(app) = &config.app_name {
            props = props.app_name(app);
        }

        let mut naming_builder = NamingServiceBuilder::new(props.clone());
        let mut config_builder = ConfigServiceBuilder::new(props);

        if let Some(auth) = &config.auth {
            naming_builder = naming_builder
                .enable_auth_plugin_http()
                .auth_username(auth.username.clone())
                .auth_password(auth.password.clone());
            config_builder = config_builder
                .enable_auth_plugin_http()
                .auth_username(auth.username.clone())
                .auth_password(auth.password.clone());
        }

        let naming_service = naming_builder.build()
            .map_err(|e| CloudError::Registration(e.to_string()))?;
        let config_service = config_builder.build()
            .await
            .map_err(|e| CloudError::Config(e.to_string()))?;

        Ok(Self {
            naming_service: Arc::new(naming_service),
            config_service: Arc::new(config_service),
            config,
        })
    }

    /// 返回 ServiceRegistry 实现
    pub fn registry(&self) -> NacosServiceRegistry { ... }

    /// 返回 ServiceDiscovery 实现
    pub fn discovery(&self) -> NacosServiceDiscovery { ... }

    /// 返回 ConfigSource 实现
    pub fn config_source(&self) -> NacosConfigSource { ... }

    /// 返回 HealthIndicator 实现
    pub fn health_indicator(&self) -> NacosHealthIndicator { ... }

    /// 返回 MetricsCollector 实现
    pub fn metrics_collector(&self) -> NacosMetricsCollector { ... }
}
```

### 5.5 Nacos v2 gRPC API Mapping

| teaql-cloud-core trait | nacos-sdk method | Nacos v2 protocol |
|------------------------|------------------|-------------------|
| `ServiceRegistry::register()` | `naming_service.register_instance()` | gRPC `InstanceRequest` |
| `ServiceRegistry::deregister()` | `naming_service.deregister_instance()` | gRPC `InstanceRequest` |
| `ServiceRegistry::heartbeat()` | **no-op** (gRPC 长连接即心跳) | 连接自动维持 |
| `ServiceRegistry::heartbeat_interval()` | 返回 `None` | — |
| `ServiceDiscovery::get_instances()` | `naming_service.get_all_instances()` | gRPC `ServiceQueryRequest` |
| `ServiceDiscovery::subscribe()` | `naming_service.subscribe()` | gRPC 服务端推送 |
| `ConfigSource::get_config()` | `config_service.get_config()` | gRPC `ConfigQueryRequest` |
| `ConfigSource::publish_config()` | `config_service.publish_config()` | gRPC `ConfigPublishRequest` |
| `ConfigSource::watch()` | `config_service.add_listener()` | gRPC 服务端推送 |

> **Note**: Nacos v2 gRPC 长连接本身充当心跳，连接断开即被 server 标记为不健康。
> 因此 `heartbeat_interval()` 返回 `None`，`heartbeat()` 为空操作。

---

## 6. Graceful Shutdown Flow

```
正常运行                  Ctrl+C / SIGTERM
    │                          │
    │                          ▼
    │                  ┌───────────────┐
    │                  │ 1. readiness  │
    │                  │    → OUT_OF   │
    │                  │    SERVICE    │
    │                  └───────┬───────┘
    │                          │  ← 网关/LB 停止路由新请求
    │                          ▼
    │                  ┌───────────────┐
    │                  │ 2. deregister │
    │                  │  from Nacos   │
    │                  └───────┬───────┘
    │                          │
    │                          ▼
    │                  ┌───────────────┐
    │                  │ 3. drain      │
    │                  │  等待在途请求  │
    │                  │  (可配置超时)  │
    │                  └───────┬───────┘
    │                          │
    │                          ▼
    │                  ┌───────────────┐
    │                  │ 4. 关闭 gRPC  │
    │                  │  连接, 退出   │
    │                  └───────────────┘
```

### Application-level Integration

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ... setup nacos, module, routes ...

    let lifecycle = ServiceLifecycle::start(
        nacos.registry(),
        instance,
    ).await?;

    let shutdown_rx = lifecycle.shutdown_receiver();

    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    // Axum serve with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            wait_for_shutdown_signal().await;
            // 给 gateway 时间感知 readiness 变化
            tokio::time::sleep(Duration::from_secs(3)).await;
        })
        .await?;

    // Axum has drained — now clean up
    lifecycle.shutdown().await?;

    Ok(())
}
```

---

## 7. Workspace Changes

```toml
# Cargo.toml (workspace root) — 新增 members
[workspace]
members = [
    # existing...
    "teaql-cloud-core",
    "teaql-cloud-actuator",
    "teaql-cloud-nacos",
]

[workspace.dependencies]
# existing...
teaql-cloud-core = { path = "teaql-cloud-core", version = "0.1.0" }
teaql-cloud-actuator = { path = "teaql-cloud-actuator", version = "0.1.0" }
teaql-cloud-nacos = { path = "teaql-cloud-nacos", version = "0.1.0" }
teaql-cloud-starter = { path = "teaql-cloud-starter", version = "0.1.0" }
```

---

## 8. Implementation Priority

| Phase | Task | Scope |
|-------|------|-------|
| **P0** | `teaql-cloud-core` | 所有 trait + model + error + lifecycle |
| **P1** | `teaql-cloud-actuator` | health (含 liveness/readiness) + info + metrics (pull + push gateway) |
| **P2** | `teaql-cloud-nacos` | registry + discovery + config (wrap nacos-sdk `=0.8`) |
| **P3** | `teaql-cloud-starter` | 一行启动 convenience API |
| **P4** | Integration test | 完整启动流程 + graceful shutdown 测试 |

---

## 9. Resolved Questions

- [x] **`nacos-sdk` 版本策略** → **Pin 到 `=0.8`**，完全锁死版本，避免 0.x 阶段的 breaking change
- [x] **Prometheus push gateway** → **两种都要**。pull 模式是默认（`/actuator/metrics`），同时支持 push gateway 模式用于短生命周期任务或防火墙受限场景
- [x] **`teaql-cloud-starter`** → **需要**。开发者体验很重要，提供一个 crate 一行启动所有 cloud 能力

---

## 10. `teaql-cloud-starter` — One-line Bootstrap

### 10.1 定位

类似 Spring Boot Starter 的概念，把 Nacos 连接 + 服务注册 + Actuator 路由 + Graceful Shutdown 全部打包。

### 10.2 Directory Structure

```
teaql-cloud-starter/
├── Cargo.toml
└── src/
    └── lib.rs
```

### 10.3 `Cargo.toml`

```toml
[package]
name = "teaql-cloud-starter"
version.workspace = true
edition.workspace = true

[dependencies]
teaql-cloud-core = { workspace = true }
teaql-cloud-actuator = { workspace = true }
teaql-cloud-nacos = { workspace = true }
tokio = { workspace = true }
axum = { version = "0.7" }
```

### 10.4 Usage

```rust
use teaql_cloud_starter::CloudApp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    CloudApp::new()
        .nacos("127.0.0.1:8848")
        .namespace("production")
        .service_name("order-service-rust")
        .port(8080)
        .health_indicator(DatabaseHealthIndicator::new(&db_pool))
        .health_indicator(DiskSpaceHealthIndicator::default())
        .metrics_collector(HttpMetricsCollector::new())
        .routes(business_routes())
        .start()
        .await?;
    // Graceful shutdown is handled automatically
    Ok(())
}
```

`CloudApp::start()` 内部执行:
1. 连接 Nacos (gRPC)
2. 拉取配置
3. 注册服务实例
4. 组装 Axum Router (业务路由 + actuator 路由)
5. 绑定端口，启动 HTTP 服务
6. 监听 SIGINT/SIGTERM → graceful shutdown → deregister
