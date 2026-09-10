#[cfg(target_os = "linux")]
mod collector;
#[cfg(target_os = "linux")]
mod error;
#[cfg(target_os = "linux")]
mod executor;

#[cfg(target_os = "linux")]
pub use collector::Collector;
#[cfg(target_os = "linux")]
pub use error::LinuxProviderError;
#[cfg(target_os = "linux")]
pub use executor::LinuxDataServiceExecutor;

/// Placeholder error type for non-Linux platforms.
#[cfg(not(target_os = "linux"))]
#[derive(Debug)]
pub struct LinuxProviderError(String);

#[cfg(not(target_os = "linux"))]
impl std::fmt::Display for LinuxProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Linux provider is only available on Linux")
    }
}

#[cfg(not(target_os = "linux"))]
impl std::error::Error for LinuxProviderError {}
