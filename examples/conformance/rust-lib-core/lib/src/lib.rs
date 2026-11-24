
//! Generated TeaQL domain crate for `runtime-example-conformance-service-core`.
//!
//! **Before writing queries**, read the `AGENTS.md` at the workspace root.
//! It contains the entity list and the exact `cargo teaql` commands to fetch API prompts.
//!
//! AI coding agents must read this crate's `AGENTS.md` before using generated
//! APIs. If this crate was downloaded from a Cargo registry, locate the
//! unpacked crate source or vendor the dependency, then read `AGENTS.md` from
//! the crate root before writing code against it.

// Shared metadata for one fixed model field. Keep storage aliases and the
// canonical change-set key distinct for references.
#[derive(Clone, Copy)]
pub(crate) struct GeneratedField {
    pub(crate) model_key: &'static str,
    pub(crate) query_key: &'static str,
    pub(crate) member_name: &'static str,
    pub(crate) column_name: &'static str,
    pub(crate) load_index: usize,
    pub(crate) reference: bool,
}

impl GeneratedField {
    pub(crate) const fn mutation_key(self) -> &'static str {
        if self.reference { self.model_key } else { self.query_key }
    }
}

pub mod e;
pub mod q;
pub mod request_support;
pub mod runtime;
pub mod sample_data;
pub mod platform;
pub mod work_item;

pub use teaql_core;
pub use e::*;
pub use q::*;
pub use request_support::*;
pub use runtime::*;
pub use sample_data::*;
pub use platform::*;
pub use work_item::*;