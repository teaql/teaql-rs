//! Lightweight Blob values and trusted transport/verifier SPIs. No runtime or provider.
mod store;
mod types;
mod verification;
pub use store::*;
pub use types::*;
pub use verification::*;
pub const FORMATS: [&str; 10] = [
    "jpg", "jpeg", "png", "mp4", "mp3", "doc", "docx", "xls", "xlsx", "pdf",
];
