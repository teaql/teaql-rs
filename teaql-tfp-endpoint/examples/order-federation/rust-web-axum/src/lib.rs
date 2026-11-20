
//! #  Axum Web Workspace
//!
//! **Before writing queries or handlers**, read the generated `AGENTS.md` at the workspace root.
//! It contains the entity list and exact `cargo teaql` commands to fetch API prompts.

pub use order_federation_service_core::{teaql_core, E, Q};
pub use teaql_web_integration_axum::{AxumTeaError, ContextProvider, TeaContext, WebResponse};

pub fn generated_domain_crate() -> &'static str {
    "order-federation-service-core"
}