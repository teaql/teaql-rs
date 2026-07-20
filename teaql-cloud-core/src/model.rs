use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A service instance — the common model across all backends.
///
/// Represents a single running instance of a service, with its network
/// address, health status, and optional metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInstance {
    /// Service name (e.g. "order-service")
    pub service_name: String,
    /// Unique instance ID (e.g. "order-service-192.168.1.10-8080")
    pub instance_id: String,
    /// IP address
    pub ip: String,
    /// Port number
    pub port: u16,
    /// Weight for load balancing (default 1.0)
    pub weight: f64,
    /// Whether this instance is healthy
    pub healthy: bool,
    /// Custom metadata (e.g. version, region, protocol)
    pub metadata: HashMap<String, String>,
}

impl ServiceInstance {
    /// Create a new service instance with auto-generated instance ID.
    pub fn new(service_name: impl Into<String>, ip: impl Into<String>, port: u16) -> Self {
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

    /// Add a metadata key-value pair.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set the load balancing weight.
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight;
        self
    }

    /// Set the instance ID explicitly.
    pub fn with_instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = id.into();
        self
    }

    /// Returns the `host:port` address string.
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

/// Service grouping configuration — maps to different concepts per backend.
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

/// Configuration identifier — locates a specific config entry.
#[derive(Debug, Clone)]
pub struct ConfigId {
    /// Data ID (e.g. "order-service.yaml")
    pub data_id: String,
    /// Group name (e.g. "DEFAULT_GROUP")
    pub group: String,
    /// Optional namespace
    pub namespace: Option<String>,
}

impl ConfigId {
    pub fn new(data_id: impl Into<String>, group: impl Into<String>) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_instance_new() {
        let instance = ServiceInstance::new("order-service", "192.168.1.10", 8080);
        assert_eq!(instance.service_name, "order-service");
        assert_eq!(instance.instance_id, "order-service-192.168.1.10-8080");
        assert_eq!(instance.ip, "192.168.1.10");
        assert_eq!(instance.port, 8080);
        assert_eq!(instance.weight, 1.0);
        assert!(instance.healthy);
        assert!(instance.metadata.is_empty());
    }

    #[test]
    fn test_service_instance_builder() {
        let instance = ServiceInstance::new("user-service", "10.0.0.1", 9090)
            .with_weight(2.0)
            .with_metadata("version", "1.0.0")
            .with_metadata("region", "us-east-1")
            .with_instance_id("custom-id");

        assert_eq!(instance.weight, 2.0);
        assert_eq!(instance.metadata.get("version").unwrap(), "1.0.0");
        assert_eq!(instance.metadata.get("region").unwrap(), "us-east-1");
        assert_eq!(instance.instance_id, "custom-id");
    }

    #[test]
    fn test_service_instance_address() {
        let instance = ServiceInstance::new("svc", "192.168.1.1", 3000);
        assert_eq!(instance.address(), "192.168.1.1:3000");
    }

    #[test]
    fn test_service_instance_default() {
        let instance = ServiceInstance::default();
        assert_eq!(instance.ip, "127.0.0.1");
        assert_eq!(instance.port, 8080);
        assert_eq!(instance.weight, 1.0);
        assert!(instance.healthy);
    }

    #[test]
    fn test_service_instance_serde_roundtrip() {
        let instance =
            ServiceInstance::new("test-svc", "10.0.0.1", 8080).with_metadata("env", "prod");
        let json = serde_json::to_string(&instance).unwrap();
        let deserialized: ServiceInstance = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.service_name, "test-svc");
        assert_eq!(deserialized.metadata.get("env").unwrap(), "prod");
    }

    #[test]
    fn test_service_group_builder() {
        let group = ServiceGroup::new()
            .with_namespace("production")
            .with_group("DEFAULT_GROUP");
        assert_eq!(group.namespace.unwrap(), "production");
        assert_eq!(group.group.unwrap(), "DEFAULT_GROUP");
    }

    #[test]
    fn test_service_group_default() {
        let group = ServiceGroup::default();
        assert!(group.namespace.is_none());
        assert!(group.group.is_none());
    }

    #[test]
    fn test_config_id() {
        let config =
            ConfigId::new("order-service.yaml", "DEFAULT_GROUP").with_namespace("production");
        assert_eq!(config.data_id, "order-service.yaml");
        assert_eq!(config.group, "DEFAULT_GROUP");
        assert_eq!(config.namespace.unwrap(), "production");
    }

    #[test]
    fn test_config_id_without_namespace() {
        let config = ConfigId::new("app.toml", "MY_GROUP");
        assert_eq!(config.data_id, "app.toml");
        assert_eq!(config.group, "MY_GROUP");
        assert!(config.namespace.is_none());
    }
}
