mod process;
mod system_info;
mod thread;

pub use process::ProcessCollector;
pub use system_info::SystemInfoCollector;
pub use thread::ThreadCollector;

use teaql_core::Record;

use crate::error::LinuxProviderError;

/// A collector gathers records from a Linux subsystem (e.g. /proc).
pub trait Collector: Send + Sync {
    /// The entity name this collector produces (e.g. "SystemInfo", "Process").
    fn entity_name(&self) -> &str;

    /// Collect all records from the underlying data source.
    fn collect_all(&self) -> Result<Vec<Record>, LinuxProviderError>;
}
