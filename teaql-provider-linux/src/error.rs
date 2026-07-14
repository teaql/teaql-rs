use std::fmt;

/// Errors produced by the Linux provider.
#[derive(Debug)]
pub enum LinuxProviderError {
    /// The requested entity name is not supported by this provider.
    UnknownEntity(String),
    /// An error originating from the procfs library.
    ProcFs(String),
    /// A standard I/O error.
    Io(std::io::Error),
}

impl fmt::Display for LinuxProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownEntity(name) => write!(f, "unknown entity: {name}"),
            Self::ProcFs(msg) => write!(f, "procfs error: {msg}"),
            Self::Io(err) => write!(f, "I/O error: {err}"),
        }
    }
}

impl std::error::Error for LinuxProviderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for LinuxProviderError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<procfs::ProcError> for LinuxProviderError {
    fn from(err: procfs::ProcError) -> Self {
        Self::ProcFs(err.to_string())
    }
}
