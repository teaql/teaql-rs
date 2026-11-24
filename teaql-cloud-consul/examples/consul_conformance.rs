use std::process;

use teaql_cloud_consul::{ConsulCloud, ConsulConfig};
use teaql_cloud_core::{
    HealthIndicator, HealthStatus, MetricValue, MetricsCollector, ServiceInstance, ServiceRegistry,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("TEAQL_TEST_CONSUL_ADDR")?;
    let base_url = if address.starts_with("http://") || address.starts_with("https://") {
        address.trim_end_matches('/').to_owned()
    } else {
        format!("http://{}", address.trim_end_matches('/'))
    };
    let instance_id = format!("teaql-consul-conformance-{}", process::id());
    let instance = ServiceInstance::new("teaql-consul-conformance", "127.0.0.1", 18080)
        .with_instance_id(&instance_id)
        .with_metadata("runtime", "rust")
        .with_metadata("evidence", "issue-65");
    let cloud = ConsulCloud::connect(ConsulConfig::new(&address)).await?;

    cloud.register(&instance).await?;
    let services: serde_json::Value = reqwest::get(format!("{base_url}/v1/agent/services"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    let registered = services
        .get(&instance_id)
        .unwrap_or_else(|| panic!("Consul did not retain explicit instance ID {instance_id}"));
    assert_eq!(registered["Service"], "teaql-consul-conformance");
    assert_eq!(registered["Address"], "127.0.0.1");
    assert_eq!(registered["Port"], 18080);
    assert_eq!(registered["Meta"]["runtime"], "rust");
    assert_eq!(registered["Meta"]["evidence"], "issue-65");

    assert_eq!(cloud.check().await.status, HealthStatus::Up);
    let metrics = cloud.collect().await;
    assert!(matches!(metrics.as_slice(), [metric]
        if matches!(metric.value, MetricValue::Gauge(value) if value == 1.0)));

    cloud.deregister(&instance).await?;
    let services: serde_json::Value = reqwest::get(format!("{base_url}/v1/agent/services"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(services.get(&instance_id).is_none());

    println!("CONSUL_CONFORMANCE_PASS instance_id={instance_id} metadata=2 health=UP metric=1");
    Ok(())
}
