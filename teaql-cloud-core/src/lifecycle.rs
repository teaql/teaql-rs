use std::sync::Arc;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::{CloudError, ServiceInstance, ServiceRegistry};

/// Service lifecycle guard.
///
/// Manages the full service registration lifecycle:
/// `register → heartbeat loop → (shutdown signal) → deregister`
///
/// # Shutdown flow
///
/// 1. Call `shutdown()` or drop the guard
/// 2. Heartbeat loop stops
/// 3. Service is deregistered from the registry
///
/// Integrate with Axum for graceful shutdown:
///
/// ```ignore
/// let lifecycle = ServiceLifecycle::start(registry, instance).await?;
/// let mut shutdown_rx = lifecycle.shutdown_receiver();
///
/// axum::serve(listener, app)
///     .with_graceful_shutdown(async move {
///         wait_for_shutdown_signal().await;
///     })
///     .await?;
///
/// lifecycle.shutdown().await?;
/// ```
pub struct ServiceLifecycle {
    shutdown_tx: watch::Sender<bool>,
    heartbeat_handle: JoinHandle<()>,
    instance: ServiceInstance,
    registry: Arc<dyn ServiceRegistry>,
}

impl ServiceLifecycle {
    /// Start the service lifecycle: register the instance and begin the heartbeat loop.
    pub async fn start(
        registry: Arc<dyn ServiceRegistry>,
        instance: ServiceInstance,
    ) -> Result<Self, CloudError> {
        // 1. Register the service instance
        registry.register(&instance).await?;

        // 2. Start heartbeat loop (if the backend requires it)
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let heartbeat_handle = {
            let registry = registry.clone();
            let instance = instance.clone();

            tokio::spawn(async move {
                let interval = registry.heartbeat_interval();
                if let Some(interval) = interval {
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(interval) => {
                                let _ = registry.heartbeat(&instance).await;
                            }
                            _ = shutdown_rx.changed() => {
                                break;
                            }
                        }
                    }
                } else {
                    // No heartbeat needed (e.g. Nacos v2 gRPC),
                    // just wait for shutdown signal
                    let _ = shutdown_rx.changed().await;
                }
            })
        };

        Ok(Self {
            shutdown_tx,
            heartbeat_handle,
            instance,
            registry,
        })
    }

    /// Get a shutdown receiver for integration with Axum's graceful shutdown.
    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// Trigger a graceful shutdown:
    /// 1. Stop the heartbeat loop
    /// 2. Deregister from the service registry
    pub async fn shutdown(self) -> Result<(), CloudError> {
        // Signal heartbeat to stop
        let _ = self.shutdown_tx.send(true);

        // Wait for heartbeat task to finish
        let _ = self.heartbeat_handle.await;

        // Deregister from service registry
        self.registry.deregister(&self.instance).await?;

        Ok(())
    }
}

/// Wait for a system shutdown signal (SIGINT or SIGTERM).
///
/// This function completes when either Ctrl+C is pressed or a SIGTERM
/// is received. Use it with Axum's `with_graceful_shutdown()`.
///
/// # Example
///
/// ```ignore
/// axum::serve(listener, app)
///     .with_graceful_shutdown(wait_for_shutdown_signal())
///     .await?;
/// ```
pub async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::time::Duration;

    struct TrackingRegistry {
        registered: AtomicBool,
        deregistered: AtomicBool,
        heartbeat_count: AtomicU32,
        heartbeat_interval: Option<Duration>,
    }

    impl TrackingRegistry {
        fn new(heartbeat_interval: Option<Duration>) -> Self {
            Self {
                registered: AtomicBool::new(false),
                deregistered: AtomicBool::new(false),
                heartbeat_count: AtomicU32::new(0),
                heartbeat_interval,
            }
        }
    }

    #[async_trait]
    impl ServiceRegistry for TrackingRegistry {
        async fn register(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            self.registered.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn deregister(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            self.deregistered.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn heartbeat(&self, _instance: &ServiceInstance) -> Result<(), CloudError> {
            self.heartbeat_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn heartbeat_interval(&self) -> Option<Duration> {
            self.heartbeat_interval
        }
    }

    #[tokio::test]
    async fn test_lifecycle_register_and_shutdown() {
        let registry = Arc::new(TrackingRegistry::new(None));
        let instance = ServiceInstance::new("test-svc", "127.0.0.1", 8080);

        let lifecycle = ServiceLifecycle::start(registry.clone(), instance)
            .await
            .unwrap();

        assert!(registry.registered.load(Ordering::SeqCst));
        assert!(!registry.deregistered.load(Ordering::SeqCst));

        lifecycle.shutdown().await.unwrap();

        assert!(registry.deregistered.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_lifecycle_with_heartbeat() {
        let registry = Arc::new(TrackingRegistry::new(Some(Duration::from_millis(50))));
        let instance = ServiceInstance::new("test-svc", "127.0.0.1", 8080);

        let lifecycle = ServiceLifecycle::start(registry.clone(), instance)
            .await
            .unwrap();

        // Wait for a few heartbeats
        tokio::time::sleep(Duration::from_millis(180)).await;

        let count_before = registry.heartbeat_count.load(Ordering::SeqCst);
        assert!(
            count_before >= 2,
            "Expected at least 2 heartbeats, got {count_before}"
        );

        lifecycle.shutdown().await.unwrap();

        assert!(registry.deregistered.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_lifecycle_no_heartbeat_when_none() {
        let registry = Arc::new(TrackingRegistry::new(None));
        let instance = ServiceInstance::new("test-svc", "127.0.0.1", 8080);

        let lifecycle = ServiceLifecycle::start(registry.clone(), instance)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // No heartbeats should have been sent
        assert_eq!(registry.heartbeat_count.load(Ordering::SeqCst), 0);

        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_shutdown_receiver() {
        let registry = Arc::new(TrackingRegistry::new(None));
        let instance = ServiceInstance::new("test-svc", "127.0.0.1", 8080);

        let lifecycle = ServiceLifecycle::start(registry.clone(), instance)
            .await
            .unwrap();
        let mut rx = lifecycle.shutdown_receiver();

        // Not yet shut down
        assert!(!*rx.borrow());

        lifecycle.shutdown().await.unwrap();

        // Receiver should now show shutdown
        rx.changed().await.unwrap();
        assert!(*rx.borrow());
    }
}
