use std::time::Duration;

use async_trait::async_trait;

use crate::{CloudError, ServiceInstance};

/// Service registration capability.
///
/// Manages the lifecycle: register → heartbeat (loop) → deregister.
///
/// Different backends implement heartbeat differently:
/// - Nacos v2: gRPC persistent connection IS the heartbeat, `heartbeat()` is a no-op
/// - etcd: Lease KeepAlive
/// - Consul: TTL Check + PUT
/// - K8s: No heartbeat needed (kubelet manages)
#[async_trait]
pub trait ServiceRegistry: Send + Sync {
    /// Register a service instance with the registry.
    async fn register(&self, instance: &ServiceInstance) -> Result<(), CloudError>;

    /// Deregister a service instance from the registry.
    async fn deregister(&self, instance: &ServiceInstance) -> Result<(), CloudError>;

    /// Send a heartbeat / renew the lease for a service instance.
    ///
    /// For backends where the connection itself serves as the heartbeat
    /// (e.g. Nacos v2 gRPC), this can be a no-op.
    async fn heartbeat(&self, instance: &ServiceInstance) -> Result<(), CloudError>;

    /// Recommended heartbeat interval.
    ///
    /// Returns `None` if the backend does not require explicit heartbeats
    /// (e.g. Nacos v2 gRPC persistent connection).
    fn heartbeat_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(5))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockRegistry {
        should_fail: bool,
    }

    #[async_trait]
    impl ServiceRegistry for MockRegistry {
        async fn register(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            if self.should_fail {
                Err(CloudError::Registration("mock failure".to_string()))
            } else {
                Ok(())
            }
        }

        async fn deregister(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            Ok(())
        }

        async fn heartbeat(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            Ok(())
        }

        fn heartbeat_interval(&self) -> Option<Duration> {
            None // simulate gRPC-style no-heartbeat
        }
    }

    #[tokio::test]
    async fn test_mock_registry_register() {
        let registry = MockRegistry { should_fail: false };
        let instance = ServiceInstance::new("test-svc", "127.0.0.1", 8080);
        assert!(registry.register(&instance).await.is_ok());
    }

    #[tokio::test]
    async fn test_mock_registry_register_failure() {
        let registry = MockRegistry { should_fail: true };
        let instance = ServiceInstance::new("test-svc", "127.0.0.1", 8080);
        let result = registry.register(&instance).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("mock failure"));
    }

    #[tokio::test]
    async fn test_mock_registry_no_heartbeat_interval() {
        let registry = MockRegistry { should_fail: false };
        assert!(registry.heartbeat_interval().is_none());
    }
}
