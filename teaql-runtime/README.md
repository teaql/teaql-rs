# teaql-runtime

Runtime repository and graph persistence layer for TeaQL Rust.

`teaql-runtime` connects TeaQL metadata, dialects, executors, repositories,
behaviors, checkers, events, and graph writes through `UserContext`.

## What It Provides

- `UserContext` for metadata, resources, request locals, and runtime options
- repository registry and behavior registry
- memory repository for tests and lightweight execution
- checker registry and translated validation results
- entity event sink support
- graph save planning and execution
- typed entity graph extraction
- optional `sqlx` support for PostgreSQL and SQLite
- schema bootstrap helpers for SQLite and PostgreSQL
- SQL debug logging options on `UserContext`

## Example

```rust
use teaql_runtime::{InMemoryMetadataStore, UserContext};

let mut ctx = UserContext::new()
    .with_metadata_store(InMemoryMetadataStore::default())
    .enable_all_sql_log();

let logs = ctx.sql_logs();
assert!(logs.is_empty());
```

With generated code, applications typically register a generated runtime module
and then use generated entity/request helpers:

```rust
let ctx = UserContext::new()
    .with_module(my_domain::module())
    .with_repository_registry(my_domain::repository_registry());
```

Enable the `sqlx` feature when using the built-in PostgreSQL or SQLite
executors:

```toml
teaql-runtime = { version = "0.7", features = ["sqlx"] }
```
