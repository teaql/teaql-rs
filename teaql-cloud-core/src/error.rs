use thiserror::Error;

/// Unified error type for all cloud integration operations.
#[derive(Error, Debug)]
pub enum CloudError {
    #[error("Service discovery error: {0}")]
    Discovery(String),

    #[error("Service registration error: {0}")]
    Registration(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Health check error: {0}")]
    Health(String),

    #[error("Network error: {source}")]
    Network {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Timeout after {duration:?}")]
    Timeout { duration: std::time::Duration },

    #[error("Shutdown in progress")]
    ShuttingDown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = CloudError::Discovery("service not found".to_string());
        assert_eq!(
            err.to_string(),
            "Service discovery error: service not found"
        );

        let err = CloudError::Registration("connection refused".to_string());
        assert_eq!(
            err.to_string(),
            "Service registration error: connection refused"
        );

        let err = CloudError::Config("invalid config format".to_string());
        assert_eq!(err.to_string(), "Config error: invalid config format");

        let err = CloudError::Health("db unreachable".to_string());
        assert_eq!(err.to_string(), "Health check error: db unreachable");

        let err = CloudError::Serialization("invalid json".to_string());
        assert_eq!(err.to_string(), "Serialization error: invalid json");

        let err = CloudError::Timeout {
            duration: std::time::Duration::from_secs(30),
        };
        assert!(err.to_string().contains("30"));

        let err = CloudError::ShuttingDown;
        assert_eq!(err.to_string(), "Shutdown in progress");
    }

    #[test]
    fn test_network_error_source() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err = CloudError::Network {
            source: Box::new(io_err),
        };
        assert!(err.to_string().contains("Network error"));
        assert!(std::error::Error::source(&err).is_some());
    }
}
