//! Integration tests: ServiceLifecycle with mock registry.
//!
//! Verifies the full lifecycle:
//! 1. Start lifecycle with a mock registry
//! 2. Verify registration was called
//! 3. Trigger shutdown
//! 4. Verify deregistration was called

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use teaql_cloud_core::{CloudError, ServiceInstance, ServiceLifecycle, ServiceRegistry};

/// Mock registry that tracks register/deregister calls.
struct MockRegistry {
    registered: AtomicBool,
    deregistered: AtomicBool,
}

impl MockRegistry {
    fn new() -> Self {
        Self {
            registered: AtomicBool::new(false),
            deregistered: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl ServiceRegistry for MockRegistry {
    async fn register(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
        self.registered.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn deregister(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
        self.deregistered.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn heartbeat(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
        Ok(())
    }

    fn heartbeat_interval(&self) -> Option<Duration> {
        None // No heartbeat loop
    }
}

#[tokio::test]
async fn test_lifecycle_register_and_shutdown() {
    let registry = Arc::new(MockRegistry::new());
    let instance = ServiceInstance::new("test-svc", "127.0.0.1", 8080);

    // Start lifecycle — should call register
    let lifecycle =
        ServiceLifecycle::start(registry.clone() as Arc<dyn ServiceRegistry>, instance).await;

    assert!(lifecycle.is_ok());
    let lifecycle = lifecycle.unwrap();

    // Verify registered
    assert!(registry.registered.load(Ordering::SeqCst));
    assert!(!registry.deregistered.load(Ordering::SeqCst));

    // Trigger shutdown — should call deregister
    let result = lifecycle.shutdown().await;
    assert!(result.is_ok());
    assert!(registry.deregistered.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_lifecycle_register_failure() {
    struct FailRegistry;

    #[async_trait]
    impl ServiceRegistry for FailRegistry {
        async fn register(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            Err(CloudError::Registration("connection refused".to_string()))
        }

        async fn deregister(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            Ok(())
        }

        async fn heartbeat(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            Ok(())
        }

        fn heartbeat_interval(&self) -> Option<Duration> {
            None
        }
    }

    let registry = Arc::new(FailRegistry);
    let instance = ServiceInstance::new("test-svc", "127.0.0.1", 8080);

    let result = ServiceLifecycle::start(registry as Arc<dyn ServiceRegistry>, instance).await;

    assert!(result.is_err());
    let err = result.err().unwrap();
    match err {
        CloudError::Registration(msg) => assert!(msg.contains("connection refused")),
        other => panic!("Expected Registration error, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_lifecycle_with_heartbeat() {
    let heartbeat_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let hb = heartbeat_count.clone();

    struct HeartbeatRegistry {
        count: Arc<std::sync::atomic::AtomicU32>,
        registered: AtomicBool,
    }

    #[async_trait]
    impl ServiceRegistry for HeartbeatRegistry {
        async fn register(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            self.registered.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn deregister(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            Ok(())
        }

        async fn heartbeat(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn heartbeat_interval(&self) -> Option<Duration> {
            Some(Duration::from_millis(50))
        }
    }

    let registry = Arc::new(HeartbeatRegistry {
        count: hb,
        registered: AtomicBool::new(false),
    });
    let instance = ServiceInstance::new("test-svc", "127.0.0.1", 8080);

    let lifecycle =
        ServiceLifecycle::start(registry.clone() as Arc<dyn ServiceRegistry>, instance).await;
    assert!(lifecycle.is_ok());
    let lifecycle = lifecycle.unwrap();

    // Wait a bit for heartbeats to fire
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Should have at least 1 heartbeat (probably 3-4 with 50ms interval over 200ms)
    let count = heartbeat_count.load(Ordering::SeqCst);
    assert!(count >= 1, "Expected at least 1 heartbeat, got {count}");

    lifecycle.shutdown().await.unwrap();
}
