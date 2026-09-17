use std::process;
use std::time::Duration;

use teaql_cloud_core::{
    ConfigId, ConfigSource, HealthIndicator, HealthStatus, MetricValue, MetricsCollector,
    ServiceDiscovery, ServiceGroup, ServiceInstance, ServiceRegistry,
};
use teaql_cloud_nacos::{NacosCloud, NacosConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("TEAQL_TEST_NACOS_ADDR")?;
    let suffix = process::id();
    let service_name = format!("teaql-nacos-conformance-{suffix}");
    let instance_id = format!("{service_name}-rust-1");
    let data_id = format!("{service_name}.yaml");
    let group_name = "TEAQL_CONFORMANCE";
    let config_id = ConfigId::new(&data_id, group_name);
    let group = ServiceGroup::new().with_group("DEFAULT_GROUP");
    let instance = ServiceInstance::new(&service_name, "127.0.0.1", 18081)
        .with_instance_id(&instance_id)
        .with_weight(2.0)
        .with_metadata("runtime", "rust")
        .with_metadata("evidence", "issue-42");
    let mut nacos_config = NacosConfig::new(&address)
        .with_app_name(&service_name)
        .with_group("DEFAULT_GROUP");
    match (
        std::env::var("TEAQL_TEST_NACOS_USERNAME").ok(),
        std::env::var("TEAQL_TEST_NACOS_PASSWORD").ok(),
    ) {
        (Some(username), Some(password)) => {
            nacos_config = nacos_config.with_auth(username, password);
        }
        (None, None) => {}
        _ => {
            return Err(
                "TEAQL_TEST_NACOS_USERNAME and TEAQL_TEST_NACOS_PASSWORD must be set together"
                    .into(),
            );
        }
    }
    let cloud = NacosCloud::connect(nacos_config).await?;

    cloud.register(&instance).await?;
    let registered = wait_for_instance(&cloud, &service_name, &instance_id, &group, true).await?;
    assert_eq!(registered.ip, "127.0.0.1");
    assert_eq!(registered.port, 18081);
    assert_eq!(registered.weight, 2.0);
    assert_eq!(
        registered.metadata.get("runtime").map(String::as_str),
        Some("rust")
    );
    assert_eq!(
        registered.metadata.get("evidence").map(String::as_str),
        Some("issue-42")
    );

    let expected_config = "feature:\n  nacos_conformance: true\n";
    cloud.publish_config(&config_id, expected_config).await?;
    assert_eq!(
        wait_for_config(&cloud, &config_id, expected_config).await?,
        expected_config
    );
    assert_eq!(cloud.check().await.status, HealthStatus::Up);
    let metrics = cloud.collect().await;
    assert!(matches!(metrics.as_slice(), [metric]
        if matches!(metric.value, MetricValue::Gauge(value) if value == 1.0)));

    cloud.deregister(&instance).await?;
    wait_for_instance(&cloud, &service_name, &instance_id, &group, false).await?;
    cloud
        .config_service()
        .remove_config(data_id, group_name.to_owned())
        .await?;

    println!(
        "NACOS_CONFORMANCE_PASS instance_id={instance_id} metadata=2 config=roundtrip health=UP metric=1 cleanup=confirmed"
    );
    Ok(())
}

async fn wait_for_config(
    cloud: &NacosCloud,
    config_id: &ConfigId,
    expected: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut last_result = None;
    for _ in 0..30 {
        match cloud.get_config(config_id).await {
            Ok(content) if content == expected => return Ok(content),
            Ok(content) => last_result = Some(format!("unexpected content {content:?}")),
            Err(error) => last_result = Some(error.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!(
        "Nacos config {}/{} did not become visible: {}",
        config_id.group,
        config_id.data_id,
        last_result.unwrap_or_else(|| "no query result".to_string())
    )
    .into())
}

async fn wait_for_instance(
    cloud: &NacosCloud,
    service_name: &str,
    instance_id: &str,
    group: &ServiceGroup,
    present: bool,
) -> Result<ServiceInstance, Box<dyn std::error::Error>> {
    for _ in 0..30 {
        let found = cloud
            .get_all_instances(service_name, group)
            .await?
            .into_iter()
            .find(|instance| instance.instance_id == instance_id);
        if present {
            if let Some(instance) = found {
                return Ok(instance);
            }
        } else if found.is_none() {
            return Ok(ServiceInstance::default());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!("Nacos instance {instance_id} did not reach present={present}").into())
}
