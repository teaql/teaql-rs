use std::sync::Arc;

use async_trait::async_trait;
use nacos_sdk::api::config::{
    ConfigChangeListener, ConfigResponse, ConfigService, ConfigServiceBuilder,
};
use nacos_sdk::api::naming::{NamingService, NamingServiceBuilder};
use nacos_sdk::api::props::ClientProps;

use teaql_cloud_core::{
    CloudError, ConfigId, ConfigSource, HealthDetail, HealthIndicator, Metric, MetricsCollector,
    ServiceChangeEvent, ServiceDiscovery, ServiceGroup, ServiceInstance, ServiceRegistry,
    SubscriptionHandle, WatchHandle,
};

use crate::config::{NacosConfig, from_nacos_instance, group_name, to_nacos_instance};

/// Unified Nacos cloud client.
///
/// Holds both NamingService (for registry/discovery) and ConfigService
/// (for configuration center). Created via `NacosCloud::connect()`.
pub struct NacosCloud {
    naming: NamingService,
    config_svc: ConfigService,
    nacos_config: NacosConfig,
}

impl NacosCloud {
    /// Connect to Nacos server and create the unified client.
    ///
    /// This establishes gRPC connections for both naming and config services.
    pub async fn connect(nacos_config: NacosConfig) -> Result<Self, CloudError> {
        let client_props = build_client_props(&nacos_config);

        let naming = NamingServiceBuilder::new(client_props.clone())
            .build()
            .await
            .map_err(|e| CloudError::Network {
                source: Box::new(e),
            })?;

        let config_svc = ConfigServiceBuilder::new(client_props)
            .build()
            .await
            .map_err(|e| CloudError::Network {
                source: Box::new(e),
            })?;

        Ok(Self {
            naming,
            config_svc,
            nacos_config,
        })
    }

    /// Access the underlying NamingService.
    pub fn naming_service(&self) -> &NamingService {
        &self.naming
    }

    /// Access the underlying ConfigService.
    pub fn config_service(&self) -> &ConfigService {
        &self.config_svc
    }

    /// Get the NacosConfig used to create this client.
    pub fn config(&self) -> &NacosConfig {
        &self.nacos_config
    }
}

fn build_client_props(config: &NacosConfig) -> ClientProps {
    let mut props = ClientProps::new()
        .server_addr(&config.server_addr)
        .namespace(&config.namespace)
        .app_name(&config.app_name);

    if let Some(auth) = &config.auth {
        props = props
            .auth_username(&auth.username)
            .auth_password(&auth.password);
    }

    props
}

// --- ServiceRegistry ---

#[async_trait]
impl ServiceRegistry for NacosCloud {
    async fn register(&self, instance: &ServiceInstance) -> Result<(), CloudError> {
        let nacos_instance = to_nacos_instance(instance);
        self.naming
            .register_instance(
                instance.service_name.clone(),
                Some(self.nacos_config.group.clone()),
                nacos_instance,
            )
            .await
            .map_err(|e| CloudError::Registration(e.to_string()))
    }

    async fn deregister(&self, instance: &ServiceInstance) -> Result<(), CloudError> {
        let nacos_instance = to_nacos_instance(instance);
        self.naming
            .deregister_instance(
                instance.service_name.clone(),
                Some(self.nacos_config.group.clone()),
                nacos_instance,
            )
            .await
            .map_err(|e| CloudError::Registration(e.to_string()))
    }

    /// No-op: Nacos v2 gRPC persistent connection serves as the heartbeat.
    async fn heartbeat(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
        Ok(())
    }

    /// Returns None: no explicit heartbeat needed for Nacos v2 gRPC.
    fn heartbeat_interval(&self) -> Option<std::time::Duration> {
        None
    }
}

// --- ServiceDiscovery ---

#[async_trait]
impl ServiceDiscovery for NacosCloud {
    async fn get_instances(
        &self,
        service_name: &str,
        group: &ServiceGroup,
    ) -> Result<Vec<ServiceInstance>, CloudError> {
        let instances = self
            .naming
            .select_instances(
                service_name.to_string(),
                group_name(group),
                Vec::new(),
                true, // subscribe
                true, // healthy only
            )
            .await
            .map_err(|e| CloudError::Discovery(e.to_string()))?;

        Ok(instances.iter().map(from_nacos_instance).collect())
    }

    async fn get_all_instances(
        &self,
        service_name: &str,
        group: &ServiceGroup,
    ) -> Result<Vec<ServiceInstance>, CloudError> {
        let instances = self
            .naming
            .get_all_instances(
                service_name.to_string(),
                group_name(group),
                Vec::new(),
                false,
            )
            .await
            .map_err(|e| CloudError::Discovery(e.to_string()))?;

        Ok(instances.iter().map(from_nacos_instance).collect())
    }

    async fn subscribe(
        &self,
        service_name: &str,
        group: &ServiceGroup,
        callback: Box<dyn Fn(ServiceChangeEvent) + Send + Sync>,
    ) -> Result<SubscriptionHandle, CloudError> {
        let svc_name = service_name.to_string();
        let grp = group_name(group);

        let listener = Arc::new(NacosEventSubscriber {
            service_name: svc_name.clone(),
            callback,
        });

        self.naming
            .subscribe(svc_name.clone(), grp.clone(), Vec::new(), listener)
            .await
            .map_err(|e| CloudError::Discovery(e.to_string()))?;

        // Return a handle that can unsubscribe (no-op for now,
        // nacos-sdk 0.8 does not expose unsubscribe cleanly)
        Ok(SubscriptionHandle::new(|| {
            // nacos-sdk 0.8 does not support unsubscribe via API
        }))
    }
}

/// Bridge between nacos-sdk's event subscriber and our callback-based API.
struct NacosEventSubscriber {
    service_name: String,
    callback: Box<dyn Fn(ServiceChangeEvent) + Send + Sync>,
}

impl nacos_sdk::api::naming::NamingEventListener for NacosEventSubscriber {
    fn event(&self, event: Arc<nacos_sdk::api::naming::NamingChangeEvent>) {
        let instances = event
            .instances
            .iter()
            .flat_map(|v| v.iter())
            .map(from_nacos_instance)
            .collect();
        (self.callback)(ServiceChangeEvent {
            service_name: self.service_name.clone(),
            instances,
        });
    }
}

// --- ConfigSource ---

#[async_trait]
impl ConfigSource for NacosCloud {
    async fn get_config(&self, config_id: &ConfigId) -> Result<String, CloudError> {
        let resp = self
            .config_svc
            .get_config(config_id.data_id.clone(), config_id.group.clone())
            .await
            .map_err(|e| CloudError::Config(e.to_string()))?;
        Ok(resp.content().to_string())
    }

    async fn publish_config(&self, config_id: &ConfigId, content: &str) -> Result<(), CloudError> {
        self.config_svc
            .publish_config(
                config_id.data_id.clone(),
                config_id.group.clone(),
                content.to_string(),
                None,
            )
            .await
            .map_err(|e| CloudError::Config(e.to_string()))?;
        Ok(())
    }

    async fn watch(
        &self,
        config_id: &ConfigId,
        callback: Box<dyn Fn(String) + Send + Sync>,
    ) -> Result<WatchHandle, CloudError> {
        let listener = Arc::new(NacosConfigWatcher { callback });
        self.config_svc
            .add_listener(config_id.data_id.clone(), config_id.group.clone(), listener)
            .await
            .map_err(|e| CloudError::Config(e.to_string()))?;

        Ok(WatchHandle::new(|| {
            // nacos-sdk 0.8 does not expose remove_listener
        }))
    }
}

/// Bridge between nacos-sdk's ConfigChangeListener and our callback API.
struct NacosConfigWatcher {
    callback: Box<dyn Fn(String) + Send + Sync>,
}

impl ConfigChangeListener for NacosConfigWatcher {
    fn notify(&self, config_resp: ConfigResponse) {
        (self.callback)(config_resp.content().to_string());
    }
}

// --- HealthIndicator ---

#[async_trait]
impl HealthIndicator for NacosCloud {
    fn name(&self) -> &str {
        "nacos"
    }

    async fn check(&self) -> HealthDetail {
        // Attempt a lightweight operation to verify connectivity.
        match self
            .naming
            .select_instances(
                "__health_check__".to_string(),
                Some("DEFAULT_GROUP".to_string()),
                Vec::new(),
                false, // don't subscribe
                false, // include unhealthy
            )
            .await
        {
            Ok(_) => HealthDetail::up_with_details(serde_json::json!({
                "server": self.nacos_config.server_addr,
                "namespace": self.nacos_config.namespace,
                "grpc_port": self.nacos_config.grpc_port_hint(),
            })),
            Err(e) => HealthDetail::down(format!("Nacos connection failed: {e}")),
        }
    }
}

// --- MetricsCollector ---

#[async_trait]
impl MetricsCollector for NacosCloud {
    async fn collect(&self) -> Vec<Metric> {
        // nacos-sdk 0.8 does not expose internal metrics,
        // so we provide a connection status metric.
        let is_connected = self
            .naming
            .select_instances(
                "__metrics_probe__".to_string(),
                Some("DEFAULT_GROUP".to_string()),
                Vec::new(),
                false,
                false,
            )
            .await
            .is_ok();

        vec![
            Metric::gauge(
                "teaql_nacos_connected",
                "Whether the Nacos gRPC connection is alive (1=connected, 0=disconnected)",
                if is_connected { 1.0 } else { 0.0 },
            )
            .with_label("server", &self.nacos_config.server_addr),
        ]
    }
}
