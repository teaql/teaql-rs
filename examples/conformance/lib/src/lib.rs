//! Generated TeaQL domain crate for `runtime-example-conformance-service-core`.
//!
//! **Before writing queries**, read the `AGENTS.md` at the workspace root.
//! It contains the entity list and the exact `cargo teaql` commands to fetch API prompts.
//!
//! AI coding agents must read this crate's `AGENTS.md` before using generated
//! APIs. If this crate was downloaded from a Cargo registry, locate the
//! unpacked crate source or vendor the dependency, then read `AGENTS.md` from
//! the crate root before writing code against it.

pub mod e;
pub mod platform;
pub mod q;
pub mod request_support;
pub mod runtime;
pub mod sample_data;
pub mod work_item;

pub use e::*;
pub use platform::*;
pub use q::*;
pub use request_support::*;
pub use runtime::*;
pub use sample_data::*;
pub use teaql_core;
pub use work_item::*;
