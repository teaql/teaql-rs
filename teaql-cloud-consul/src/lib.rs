use std::time::Duration;
use async_trait::async_trait;
use serde_json::json;
use reqwest::Client;

use teaql_cloud_core::{
    CloudError, HealthIndicator, MetricsCollector, Metric, ServiceInstance, ServiceRegistry,
    HealthDetail,
};

pub struct ConsulConfig {
    pub server_addr: String,
    pub token: Option<String>,
}

impl ConsulConfig {
    pub fn new(server_addr: impl Into<String>) -> Self {
        Self {
            server_addr: server_addr.into(),
            token: None,
        }
    }
    
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }
}

pub struct ConsulCloud {
    config: ConsulConfig,
    client: Client,
}

impl ConsulCloud {
    pub async fn connect(config: ConsulConfig) -> Result<Self, CloudError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| CloudError::Network { source: Box::new(e) })?;
            
        Ok(Self { config, client })
    }
    
    fn format_service_id(instance: &ServiceInstance) -> String {
        format!("{}-{}-{}", instance.service_name, instance.ip, instance.port)
    }
}

#[async_trait]
impl ServiceRegistry for ConsulCloud {
    async fn register(&self, instance: &ServiceInstance) -> Result<(), CloudError> {
        let service_id = Self::format_service_id(instance);
        let url = format!("http://{}/v1/agent/service/register", self.config.server_addr);
        
        let payload = json!({
            "ID": service_id,
            "Name": instance.service_name,
            "Address": instance.ip,
            "Port": instance.port,
            "Check": {
                "HTTP": format!("http://{}:{}/actuator/health", instance.ip, instance.port),
                "Interval": "10s",
                "Timeout": "5s",
                "DeregisterCriticalServiceAfter": "30s"
            }
        });
        
        let mut req = self.client.put(&url).json(&payload);
        if let Some(token) = &self.config.token {
            req = req.header("X-Consul-Token", token);
        }
        
        let resp = req.send().await.map_err(|e| CloudError::Registration(e.to_string()))?;
        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(CloudError::Registration(format!("Consul register failed: {}", error)));
        }
        
        Ok(())
    }

    async fn deregister(&self, instance: &ServiceInstance) -> Result<(), CloudError> {
        let service_id = Self::format_service_id(instance);
        let url = format!("http://{}/v1/agent/service/deregister/{}", self.config.server_addr, service_id);
        
        let mut req = self.client.put(&url);
        if let Some(token) = &self.config.token {
            req = req.header("X-Consul-Token", token);
        }
        
        let resp = req.send().await.map_err(|e| CloudError::Registration(e.to_string()))?;
        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(CloudError::Registration(format!("Consul deregister failed: {}", error)));
        }
        
        Ok(())
    }

    async fn heartbeat(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
        // Consul checks our /actuator/health endpoint. No active heartbeat required.
        Ok(())
    }

    fn heartbeat_interval(&self) -> Option<Duration> {
        None
    }
}

#[async_trait]
impl HealthIndicator for ConsulCloud {
    fn name(&self) -> &str {
        "consul"
    }

    async fn check(&self) -> HealthDetail {
        let url = format!("http://{}/v1/agent/self", self.config.server_addr);
        let mut req = self.client.get(&url);
        if let Some(token) = &self.config.token {
            req = req.header("X-Consul-Token", token);
        }
        
        match req.send().await {
            Ok(resp) if resp.status().is_success() => HealthDetail::up(),
            Ok(resp) => {
                HealthDetail::down(resp.status().to_string())
            },
            Err(e) => {
                HealthDetail::down(e.to_string())
            }
        }
    }
}

#[async_trait]
impl MetricsCollector for ConsulCloud {
    async fn collect(&self) -> Vec<Metric> {
        vec![Metric::gauge("consul_alive", "Consul health status", 1.0)]
    }
}
