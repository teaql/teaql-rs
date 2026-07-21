//! # teaql-cloud-starter
//!
//! One-line cloud bootstrap for TeaQL Rust services.
//!
//! Similar to Spring Boot Starter, this crate packages all cloud capabilities
//! into a single `CloudApp` builder:
//!
//! - Nacos v2 gRPC connection
//! - Service registration & discovery
//! - Configuration center integration
//! - Spring Boot Actuator-compatible health/info/metrics endpoints
//! - Graceful shutdown (SIGINT/SIGTERM → readiness=OUT_OF_SERVICE → deregister → drain → exit)
//!
//! ## Usage
//!
//! ```ignore
//! use teaql_cloud_starter::CloudApp;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     CloudApp::new()
//!         .nacos("127.0.0.1:8848")
//!         .namespace("production")
//!         .service_name("order-service-rust")
//!         .port(8080)
//!         .routes(my_business_routes())
//!         .start()
//!         .await?;
//!     Ok(())
//! }
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;

use teaql_cloud_actuator::{ActuatorState, ServiceInfo, actuator_routes};
use teaql_cloud_core::{
    CloudError, HealthIndicator, MetricsCollector, ServiceInstance, ServiceLifecycle,
    ServiceRegistry, wait_for_shutdown_signal,
};
use teaql_cloud_nacos::{NacosCloud, NacosConfig};
use teaql_cloud_consul::{ConsulCloud, ConsulConfig};

// Re-export key types for convenience
pub use teaql_cloud_actuator::{self as actuator};
pub use teaql_cloud_core::{self as core};
pub use teaql_cloud_nacos::{self as nacos};
pub use teaql_cloud_consul::{self as consul};

pub enum RegistryType {
    Nacos,
    Consul,
}

/// Cloud application builder — one-line bootstrap for all cloud capabilities.
///
/// `CloudApp::start()` internally executes:
/// 1. Connect to Nacos (gRPC)
/// 2. Register service instance
/// 3. Assemble Axum Router (business routes + actuator routes)
/// 4. Bind port, start HTTP server
/// 5. Listen for SIGINT/SIGTERM → graceful shutdown → deregister
pub struct CloudApp {
    registry_type: RegistryType,
    registry_addr: String,
    namespace: String,
    service_name: String,
    ip: String,
    port: u16,
    routes: Option<Router>,
    indicators: Vec<Arc<dyn HealthIndicator>>,
    collectors: Vec<Arc<dyn MetricsCollector>>,
    info: ServiceInfo,
    nacos_auth: Option<(String, String)>,
    nacos_group: String,
}

impl CloudApp {
    /// Create a new CloudApp builder with sensible defaults.
    pub fn new() -> Self {
        Self {
            registry_type: RegistryType::Consul,
            registry_addr: "127.0.0.1:8500".to_string(),
            namespace: String::new(),
            service_name: "teaql-service".to_string(),
            ip: "0.0.0.0".to_string(),
            port: 8080,
            routes: None,
            indicators: Vec::new(),
            collectors: Vec::new(),
            info: ServiceInfo::default(),
            nacos_auth: None,
            nacos_group: "DEFAULT_GROUP".to_string(),
        }
    }

    /// Set the Nacos server address (e.g. "127.0.0.1:8848").
    pub fn nacos(mut self, addr: impl Into<String>) -> Self {
        self.registry_type = RegistryType::Nacos;
        self.registry_addr = addr.into();
        self
    }

    /// Set the Consul server address (e.g. "127.0.0.1:8500").
    pub fn consul(mut self, addr: impl Into<String>) -> Self {
        self.registry_type = RegistryType::Consul;
        self.registry_addr = addr.into();
        self
    }

    /// Set the Nacos namespace.
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = ns.into();
        self
    }

    /// Set the service name for registration.
    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        self.info.name = name.clone();
        self.service_name = name;
        self
    }

    /// Set the bind IP address (default: "0.0.0.0").
    pub fn ip(mut self, ip: impl Into<String>) -> Self {
        self.ip = ip.into();
        self
    }

    /// Set the HTTP port (default: 8080).
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the business routes.
    pub fn routes(mut self, routes: Router) -> Self {
        self.routes = Some(routes);
        self
    }

    /// Add a health indicator.
    pub fn health_indicator(mut self, indicator: Arc<dyn HealthIndicator>) -> Self {
        self.indicators.push(indicator);
        self
    }

    /// Add a metrics collector.
    pub fn metrics_collector(mut self, collector: Arc<dyn MetricsCollector>) -> Self {
        self.collectors.push(collector);
        self
    }

    /// Set the service version.
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.info.version = version.into();
        self
    }

    /// Set the Nacos authentication credentials.
    pub fn auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.nacos_auth = Some((username.into(), password.into()));
        self
    }

    /// Set the Nacos group.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.nacos_group = group.into();
        self
    }

    /// Set the service info (build metadata).
    pub fn service_info(mut self, info: ServiceInfo) -> Self {
        self.info = info;
        self
    }

    /// Start the cloud application.
    ///
    /// This method:
    /// 1. Connects to Nacos via gRPC
    /// 2. Registers the service instance
    /// 3. Merges business routes with actuator routes
    /// 4. Starts the HTTP server
    /// 5. Waits for shutdown signal, then gracefully shuts down
    ///
    /// This method blocks until the application shuts down.
    pub async fn start(self) -> Result<(), CloudError> {
        // 1. Build Config and Connect based on registry type
        let (registry, health_indicator, metrics_collector): (
            Arc<dyn ServiceRegistry>,
            Arc<dyn HealthIndicator>,
            Arc<dyn MetricsCollector>,
        ) = match self.registry_type {
            RegistryType::Nacos => {
                let mut nacos_config = NacosConfig::new(&self.registry_addr)
                    .with_namespace(&self.namespace)
                    .with_app_name(&self.service_name)
                    .with_group(&self.nacos_group);

                if let Some((username, password)) = &self.nacos_auth {
                    nacos_config = nacos_config.with_auth(username, password);
                }

                let cloud = Arc::new(NacosCloud::connect(nacos_config).await?);
                (cloud.clone(), cloud.clone(), cloud.clone())
            }
            RegistryType::Consul => {
                let mut consul_config = ConsulConfig::new(&self.registry_addr);
                
                if let Some((token, _)) = &self.nacos_auth {
                    consul_config = consul_config.with_token(token);
                }
                
                let cloud = Arc::new(ConsulCloud::connect(consul_config).await?);
                (cloud.clone(), cloud.clone(), cloud.clone())
            }
        };

        // 2. Build service instance
        let instance = ServiceInstance::new(&self.service_name, &self.ip, self.port);

        // 3. Start lifecycle (register + heartbeat loop)
        let lifecycle =
            ServiceLifecycle::start(registry, instance).await?;

        // 4. Build actuator state — include Cloud as indicator + collector
        let mut indicators = self.indicators;
        indicators.push(health_indicator);

        let mut collectors = self.collectors;
        collectors.push(metrics_collector);

        let actuator_state = ActuatorState {
            indicators,
            collectors,
            info: self.info,
        };

        // 5. Assemble router: business routes + actuator routes
        let app = if let Some(routes) = self.routes {
            routes.merge(actuator_routes(actuator_state))
        } else {
            actuator_routes(actuator_state)
        };

        // 6. Start HTTP server with graceful shutdown
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let listener =
            tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| CloudError::Network {
                    source: Box::new(e),
                })?;

        axum::serve(listener, app)
            .with_graceful_shutdown(wait_for_shutdown_signal())
            .await
            .map_err(|e| CloudError::Network {
                source: Box::new(e),
            })?;

        // 7. Graceful shutdown: deregister from Nacos
        lifecycle.shutdown().await?;

        Ok(())
    }
}

impl Default for CloudApp {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloud_app_builder() {
        let app = CloudApp::new()
            .nacos("10.0.0.1:8848")
            .namespace("production")
            .service_name("order-service")
            .ip("192.168.1.10")
            .port(9090)
            .version("1.0.0")
            .group("MY_GROUP")
            .auth("admin", "secret");

        assert_eq!(app.registry_addr, "10.0.0.1:8848");
        assert_eq!(app.namespace, "production");
        assert_eq!(app.service_name, "order-service");
        assert_eq!(app.ip, "192.168.1.10");
        assert_eq!(app.port, 9090);
        assert_eq!(app.info.name, "order-service");
        assert_eq!(app.info.version, "1.0.0");
        assert_eq!(app.nacos_group, "MY_GROUP");
        assert!(app.nacos_auth.is_some());
    }

    #[test]
    fn test_cloud_app_defaults() {
        let app = CloudApp::new();
        assert_eq!(app.registry_addr, "127.0.0.1:8500");
        assert_eq!(app.namespace, "");
        assert_eq!(app.port, 8080);
        assert_eq!(app.ip, "0.0.0.0");
        assert!(app.indicators.is_empty());
        assert!(app.collectors.is_empty());
        assert!(app.routes.is_none());
    }

    #[test]
    fn test_cloud_app_default_trait() {
        let app = CloudApp::default();
        assert_eq!(app.port, 8080);
    }

    #[test]
    fn test_cloud_app_with_routes() {
        let routes = Router::new();
        let app = CloudApp::new().routes(routes);
        assert!(app.routes.is_some());
    }
}
