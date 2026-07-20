use async_trait::async_trait;

use crate::CloudError;
use crate::model::ConfigId;

/// Handle to cancel a configuration watch.
///
/// Call `unwatch()` to stop receiving change notifications.
pub struct WatchHandle {
    cancel: Box<dyn FnOnce() + Send>,
}

impl WatchHandle {
    pub fn new(cancel: impl FnOnce() + Send + 'static) -> Self {
        Self {
            cancel: Box::new(cancel),
        }
    }

    /// Cancel the watch.
    pub fn unwatch(self) {
        (self.cancel)();
    }
}

/// Configuration source abstraction.
///
/// Change notification mechanisms vary by backend:
/// - Nacos v2: gRPC Push (server-initiated)
/// - etcd: gRPC Watch stream
/// - Consul: Blocking Query (HTTP long poll)
/// - Apollo: Long Polling + local cache
///
/// All are unified under the callback-based `watch()` API.
#[async_trait]
pub trait ConfigSource: Send + Sync {
    /// Get configuration content (returns raw string, caller parses as YAML/TOML/JSON).
    async fn get_config(&self, config_id: &ConfigId) -> Result<String, CloudError>;

    /// Publish configuration content.
    ///
    /// Not all backends support this operation.
    async fn publish_config(&self, config_id: &ConfigId, content: &str) -> Result<(), CloudError>;

    /// Watch for configuration changes.
    ///
    /// The callback is invoked with the new configuration content whenever
    /// a change is detected. Returns a handle to cancel the watch.
    async fn watch(
        &self,
        config_id: &ConfigId,
        callback: Box<dyn Fn(String) + Send + Sync>,
    ) -> Result<WatchHandle, CloudError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct MockConfigSource {
        content: String,
    }

    #[async_trait]
    impl ConfigSource for MockConfigSource {
        async fn get_config(&self, _config_id: &ConfigId) -> Result<String, CloudError> {
            Ok(self.content.clone())
        }

        async fn publish_config(
            &self,
            _config_id: &ConfigId,
            _content: &str,
        ) -> Result<(), CloudError> {
            Ok(())
        }

        async fn watch(
            &self,
            _config_id: &ConfigId,
            _callback: Box<dyn Fn(String) + Send + Sync>,
        ) -> Result<WatchHandle, CloudError> {
            Ok(WatchHandle::new(|| {}))
        }
    }

    #[tokio::test]
    async fn test_get_config() {
        let source = MockConfigSource {
            content: "server:\n  port: 8080".to_string(),
        };
        let config_id = ConfigId::new("app.yaml", "DEFAULT_GROUP");
        let content = source.get_config(&config_id).await.unwrap();
        assert!(content.contains("port: 8080"));
    }

    #[tokio::test]
    async fn test_publish_config() {
        let source = MockConfigSource {
            content: String::new(),
        };
        let config_id = ConfigId::new("app.yaml", "DEFAULT_GROUP");
        assert!(
            source
                .publish_config(&config_id, "new content")
                .await
                .is_ok()
        );
    }

    #[test]
    fn test_watch_handle_unwatch() {
        let cancelled = Arc::new(Mutex::new(false));
        let cancelled_clone = cancelled.clone();
        let handle = WatchHandle::new(move || {
            *cancelled_clone.lock().unwrap() = true;
        });
        handle.unwatch();
        assert!(*cancelled.lock().unwrap());
    }
}
