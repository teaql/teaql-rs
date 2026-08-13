
//! Generated TeaQL domain crate for `order-management-service-core`.
//!
//! **Before writing queries**, read the `AGENTS.md` at the workspace root.
//! It contains the entity list and the exact `cargo teaql` commands to fetch API prompts.
//!
//! AI coding agents must read this crate's `AGENTS.md` before using generated
//! APIs. If this crate was downloaded from a Cargo registry, locate the
//! unpacked crate source or vendor the dependency, then read `AGENTS.md` from
//! the crate root before writing code against it.

pub mod e;
pub mod q;
pub mod request_support;
pub mod runtime;
pub mod sample_data;
pub mod commerce_platform;
pub mod customer;
pub mod order_status;
pub mod customer_order;
pub mod product;
pub mod order_line;
pub mod order_search_preset;

pub use teaql_core;
pub use e::*;
pub use q::*;
pub use request_support::*;
pub use runtime::*;
pub use sample_data::*;
pub use commerce_platform::*;
pub use customer::*;
pub use order_status::*;
pub use customer_order::*;
pub use product::*;
pub use order_line::*;
pub use order_search_preset::*;