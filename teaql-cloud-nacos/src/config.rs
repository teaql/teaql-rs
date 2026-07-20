use serde::Deserialize;

/// Nacos connection configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct NacosConfig {
    /// Nacos server address (e.g. "127.0.0.1:8848")
    ///
    /// For cluster mode, use comma-separated addresses:
    /// "10.0.0.1:8848,10.0.0.2:8848"
    pub server_addr: String,

    /// Namespace ID (empty string = public namespace)
    #[serde(default)]
    pub namespace: String,

    /// Application name used in Nacos console
    #[serde(default = "default_app_name")]
    pub app_name: String,

    /// Optional authentication credentials
    #[serde(default)]
    pub auth: Option<NacosAuth>,

    /// Default group name
    #[serde(default = "default_group")]
    pub group: String,
}

/// Nacos authentication credentials.
#[derive(Debug, Clone, Deserialize)]
pub struct NacosAuth {
    pub username: String,
    pub password: String,
}

fn default_app_name() -> String {
    "teaql-service".to_string()
}

fn default_group() -> String {
    "DEFAULT_GROUP".to_string()
}

impl NacosConfig {
    /// Create a new config with server address.
    pub fn new(server_addr: impl Into<String>) -> Self {
        Self {
            server_addr: server_addr.into(),
            namespace: String::new(),
            app_name: default_app_name(),
            auth: None,
            group: default_group(),
        }
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn with_app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = app_name.into();
        self
    }

    pub fn with_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Some(NacosAuth {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = group.into();
        self
    }

    /// Get the gRPC port (server_port + 1000).
    ///
    /// Nacos v2 uses gRPC on port `server_port + 1000` by default.
    /// e.g. if server_addr is "127.0.0.1:8848", gRPC port is 9848.
    ///
    /// Note: The `nacos-sdk` crate handles this offset automatically,
    /// so you only need to provide the HTTP port.
    pub fn grpc_port_hint(&self) -> Option<u16> {
        self.server_addr
            .split(':')
            .next_back()
            .and_then(|p| p.parse::<u16>().ok())
            .map(|p| p + 1000)
    }
}

/// Convert our ServiceInstance to nacos-sdk's ServiceInstance.
pub(crate) fn to_nacos_instance(
    instance: &teaql_cloud_core::ServiceInstance,
) -> nacos_sdk::api::naming::ServiceInstance {
    nacos_sdk::api::naming::ServiceInstance {
        ip: instance.ip.clone(),
        port: instance.port as i32,
        weight: instance.weight,
        healthy: instance.healthy,
        enabled: true,
        ephemeral: true,
        cluster_name: Some("DEFAULT".to_string()),
        service_name: Some(instance.service_name.clone()),
        metadata: instance.metadata.clone(),
        instance_id: Some(instance.instance_id.clone()),
    }
}

/// Convert nacos-sdk's ServiceInstance to our ServiceInstance.
pub(crate) fn from_nacos_instance(
    instance: &nacos_sdk::api::naming::ServiceInstance,
) -> teaql_cloud_core::ServiceInstance {
    teaql_cloud_core::ServiceInstance {
        ip: instance.ip.clone(),
        port: instance.port as u16,
        weight: instance.weight,
        healthy: instance.healthy,
        service_name: instance.service_name.clone().unwrap_or_default(),
        instance_id: instance.instance_id.clone().unwrap_or_default(),
        metadata: instance.metadata.clone(),
    }
}

/// Convert a ServiceGroup into the group string used by nacos-sdk.
pub(crate) fn group_name(group: &teaql_cloud_core::ServiceGroup) -> Option<String> {
    group
        .group
        .clone()
        .or_else(|| Some("DEFAULT_GROUP".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use teaql_cloud_core::ServiceInstance;

    #[test]
    fn test_nacos_config_new() {
        let config = NacosConfig::new("127.0.0.1:8848");
        assert_eq!(config.server_addr, "127.0.0.1:8848");
        assert_eq!(config.namespace, "");
        assert_eq!(config.app_name, "teaql-service");
        assert_eq!(config.group, "DEFAULT_GROUP");
        assert!(config.auth.is_none());
    }

    #[test]
    fn test_nacos_config_builder() {
        let config = NacosConfig::new("10.0.0.1:8848")
            .with_namespace("production")
            .with_app_name("order-service")
            .with_group("MY_GROUP")
            .with_auth("admin", "secret");

        assert_eq!(config.namespace, "production");
        assert_eq!(config.app_name, "order-service");
        assert_eq!(config.group, "MY_GROUP");
        assert!(config.auth.is_some());
        let auth = config.auth.unwrap();
        assert_eq!(auth.username, "admin");
        assert_eq!(auth.password, "secret");
    }

    #[test]
    fn test_grpc_port_hint() {
        let config = NacosConfig::new("127.0.0.1:8848");
        assert_eq!(config.grpc_port_hint(), Some(9848));
    }

    #[test]
    fn test_grpc_port_hint_no_port() {
        let config = NacosConfig::new("127.0.0.1");
        assert!(config.grpc_port_hint().is_none());
    }

    #[test]
    fn test_to_nacos_instance() {
        let our = ServiceInstance::new("order-svc", "10.0.0.1", 8080)
            .with_weight(2.0)
            .with_metadata("version", "1.0.0");

        let nacos = to_nacos_instance(&our);

        assert_eq!(nacos.ip, "10.0.0.1");
        assert_eq!(nacos.port, 8080);
        assert_eq!(nacos.weight, 2.0);
        assert!(nacos.healthy);
        assert!(nacos.ephemeral);
        assert_eq!(nacos.service_name, Some("order-svc".to_string()));
        assert_eq!(nacos.metadata.get("version").unwrap(), "1.0.0");
    }

    #[test]
    fn test_from_nacos_instance() {
        let mut metadata = HashMap::new();
        metadata.insert("env".to_string(), "prod".to_string());

        let nacos = nacos_sdk::api::naming::ServiceInstance {
            ip: "192.168.1.1".to_string(),
            port: 9090,
            weight: 1.5,
            healthy: true,
            service_name: Some("user-svc".to_string()),
            instance_id: Some("user-svc-192.168.1.1-9090".to_string()),
            metadata,
            ..Default::default()
        };

        let our = from_nacos_instance(&nacos);

        assert_eq!(our.ip, "192.168.1.1");
        assert_eq!(our.port, 9090);
        assert_eq!(our.weight, 1.5);
        assert_eq!(our.service_name, "user-svc");
        assert_eq!(our.metadata.get("env").unwrap(), "prod");
    }

    #[test]
    fn test_group_name_with_group() {
        let group = teaql_cloud_core::ServiceGroup::new().with_group("MY_GROUP");
        assert_eq!(group_name(&group), Some("MY_GROUP".to_string()));
    }

    #[test]
    fn test_group_name_default() {
        let group = teaql_cloud_core::ServiceGroup::new();
        assert_eq!(group_name(&group), Some("DEFAULT_GROUP".to_string()));
    }
}
