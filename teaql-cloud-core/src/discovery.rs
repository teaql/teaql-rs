use async_trait::async_trait;

use crate::{CloudError, ServiceGroup, ServiceInstance};

/// Service change event — pushed when instances are added, removed, or updated.
#[derive(Debug, Clone)]
pub struct ServiceChangeEvent {
    pub service_name: String,
    pub instances: Vec<ServiceInstance>,
}

/// Handle to cancel a service subscription.
///
/// Call `unsubscribe()` to stop receiving change events.
pub struct SubscriptionHandle {
    cancel: Box<dyn FnOnce() + Send>,
}

impl SubscriptionHandle {
    pub fn new(cancel: impl FnOnce() + Send + 'static) -> Self {
        Self {
            cancel: Box::new(cancel),
        }
    }

    /// Cancel the subscription.
    pub fn unsubscribe(self) {
        (self.cancel)();
    }
}

/// Service discovery capability.
///
/// Push mechanisms vary by backend:
/// - Nacos v2: gRPC server-side push (near-instant)
/// - etcd: gRPC Watch stream
/// - Consul: Blocking Query (long polling)
///
/// All are unified under the callback-based `subscribe()` API.
#[async_trait]
pub trait ServiceDiscovery: Send + Sync {
    /// Get healthy service instances (one-shot pull).
    async fn get_instances(
        &self,
        service_name: &str,
        group: &ServiceGroup,
    ) -> Result<Vec<ServiceInstance>, CloudError>;

    /// Get all instances including unhealthy ones.
    ///
    /// Default implementation delegates to `get_instances()`.
    async fn get_all_instances(
        &self,
        service_name: &str,
        group: &ServiceGroup,
    ) -> Result<Vec<ServiceInstance>, CloudError> {
        self.get_instances(service_name, group).await
    }

    /// Subscribe to service instance changes.
    ///
    /// The callback is invoked whenever instances are added, removed, or updated.
    /// Returns a handle that can be used to cancel the subscription.
    async fn subscribe(
        &self,
        service_name: &str,
        group: &ServiceGroup,
        callback: Box<dyn Fn(ServiceChangeEvent) + Send + Sync>,
    ) -> Result<SubscriptionHandle, CloudError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct MockDiscovery {
        instances: Vec<ServiceInstance>,
    }

    #[async_trait]
    impl ServiceDiscovery for MockDiscovery {
        async fn get_instances(
            &self,
            _service_name: &str,
            _group: &ServiceGroup,
        ) -> Result<Vec<ServiceInstance>, CloudError> {
            Ok(self.instances.clone())
        }

        async fn subscribe(
            &self,
            _service_name: &str,
            _group: &ServiceGroup,
            _callback: Box<dyn Fn(ServiceChangeEvent) + Send + Sync>,
        ) -> Result<SubscriptionHandle, CloudError> {
            Ok(SubscriptionHandle::new(|| {}))
        }
    }

    #[tokio::test]
    async fn test_get_instances() {
        let discovery = MockDiscovery {
            instances: vec![
                ServiceInstance::new("order-svc", "10.0.0.1", 8080),
                ServiceInstance::new("order-svc", "10.0.0.2", 8080),
            ],
        };

        let group = ServiceGroup::new();
        let instances = discovery.get_instances("order-svc", &group).await.unwrap();
        assert_eq!(instances.len(), 2);
    }

    #[tokio::test]
    async fn test_get_all_instances_defaults_to_get_instances() {
        let discovery = MockDiscovery {
            instances: vec![ServiceInstance::new("svc", "10.0.0.1", 8080)],
        };

        let group = ServiceGroup::new();
        let all = discovery.get_all_instances("svc", &group).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn test_subscription_handle_unsubscribe() {
        let cancelled = Arc::new(Mutex::new(false));
        let cancelled_clone = cancelled.clone();
        let handle = SubscriptionHandle::new(move || {
            *cancelled_clone.lock().unwrap() = true;
        });
        handle.unsubscribe();
        assert!(*cancelled.lock().unwrap());
    }
}
