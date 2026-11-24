
//! #  Service Workspace
//!
//! **Before writing queries**, read the generated `AGENTS.md` at the workspace root.
//! It contains the entity list and the exact `cargo teaql` commands to fetch API prompts.

pub use checker_natural_message_service_core::{teaql_core, E, Q};

pub fn generated_domain_crate() -> &'static str {
    "checker-natural-message-service-core"
}