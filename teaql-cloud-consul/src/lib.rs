use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use std::time::Duration;

use teaql_cloud_core::{
    CloudError, HealthDetail, HealthIndicator, HealthStatus, Metric, MetricsCollector,
    ServiceInstance, ServiceRegistry,
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
            .map_err(|e| CloudError::Network {
                source: Box::new(e),
            })?;

        Ok(Self { config, client })
    }

    fn base_url(&self) -> String {
        let address = self.config.server_addr.trim_end_matches('/');
        if address.starts_with("http://") || address.starts_with("https://") {
            address.to_owned()
        } else {
            format!("http://{address}")
        }
    }
}

#[async_trait]
impl ServiceRegistry for ConsulCloud {
    async fn register(&self, instance: &ServiceInstance) -> Result<(), CloudError> {
        let url = format!("{}/v1/agent/service/register", self.base_url());

        let payload = json!({
            "ID": instance.instance_id,
            "Name": instance.service_name,
            "Address": instance.ip,
            "Port": instance.port,
            "Meta": instance.metadata,
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

        let resp = req
            .send()
            .await
            .map_err(|e| CloudError::Registration(e.to_string()))?;
        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(CloudError::Registration(format!(
                "Consul register failed: {}",
                error
            )));
        }

        Ok(())
    }

    async fn deregister(&self, instance: &ServiceInstance) -> Result<(), CloudError> {
        let service_id = urlencoding::encode(&instance.instance_id);
        let url = format!(
            "{}/v1/agent/service/deregister/{service_id}",
            self.base_url()
        );

        let mut req = self.client.put(&url);
        if let Some(token) = &self.config.token {
            req = req.header("X-Consul-Token", token);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| CloudError::Registration(e.to_string()))?;
        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(CloudError::Registration(format!(
                "Consul deregister failed: {}",
                error
            )));
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
        let url = format!("{}/v1/agent/self", self.base_url());
        let mut req = self.client.get(&url);
        if let Some(token) = &self.config.token {
            req = req.header("X-Consul-Token", token);
        }

        match req.send().await {
            Ok(resp) if resp.status().is_success() => HealthDetail::up(),
            Ok(resp) => HealthDetail::down(resp.status().to_string()),
            Err(e) => HealthDetail::down(e.to_string()),
        }
    }
}

#[async_trait]
impl MetricsCollector for ConsulCloud {
    async fn collect(&self) -> Vec<Metric> {
        let connected = f64::from(self.check().await.status == HealthStatus::Up);
        vec![
            Metric::gauge("consul_alive", "Consul health status", connected)
                .with_label("server", &self.config.server_addr),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use teaql_cloud_core::MetricValue;

    #[test]
    fn accepts_host_port_or_full_base_url() {
        let host = ConsulCloud {
            config: ConsulConfig::new("127.0.0.1:8500/"),
            client: Client::new(),
        };
        assert_eq!(host.base_url(), "http://127.0.0.1:8500");

        let url = ConsulCloud {
            config: ConsulConfig::new("https://consul.example.test/"),
            client: Client::new(),
        };
        assert_eq!(url.base_url(), "https://consul.example.test");
    }

    #[tokio::test]
    async fn unavailable_consul_reports_down_in_health_and_metrics() {
        let cloud = ConsulCloud::connect(ConsulConfig::new("127.0.0.1:1"))
            .await
            .unwrap();
        assert_eq!(cloud.check().await.status, HealthStatus::Down);
        let metrics = cloud.collect().await;
        assert_eq!(metrics.len(), 1);
        assert!(matches!(metrics[0].value, MetricValue::Gauge(value) if value == 0.0));
        assert_eq!(
            metrics[0].labels,
            vec![("server".to_owned(), "127.0.0.1:1".to_owned())]
        );
    }
}
