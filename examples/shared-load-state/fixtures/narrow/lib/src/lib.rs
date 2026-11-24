
//! Generated TeaQL domain crate for `school-management-service-core`.
//!
//! **Before writing queries**, read the `AGENTS.md` at the workspace root.
//! It contains the entity list and the exact `cargo teaql` commands to fetch API prompts.
//!
//! The generated library is not the API-discovery surface. Read the generated
//! application's `AGENTS.md`, then request model-aware object/field Assist.
//! A registry dependency does not require vendoring or browsing generated
//! domain-library source to learn method names. If Assist lacks an operation,
//! report `MISSING_ASSIST` for that path.

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
pub mod school_type;
pub mod school;
pub mod school_capacity_summary;

pub use teaql_core;
pub use teaql_runtime::LedgerEntity;
pub use e::*;
pub use q::*;
pub use request_support::*;
pub use runtime::*;
pub use sample_data::*;
pub use platform::*;
pub use school_type::*;
pub use school::*;
pub use school_capacity_summary::*;