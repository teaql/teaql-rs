# teaql-runtime

Minimal runtime repository and graph persistence layer for TeaQL Rust.

`teaql-runtime` owns TeaQL's runtime concepts: metadata lookup, repository
assembly, behaviors, checkers, events, graph planning, and in-memory execution.
Database execution is supplied by provider crates.

## What It Provides

- `UserContext` for metadata, typed resources, request locals, and runtime options
- request policy hooks for platform-level security, observability, and resource limits
- whole-graph mutation policy review, exact policy approval, warnings, and audit evidence
- repository registry and behavior registry
- in-memory query engine plus an internal data-service test harness
- checker registry and translated validation results
- entity event sink support
- graph save planning and execution
- typed entity graph extraction
- SQL debug logging options on `UserContext`

## Source Layout

The repository layer is split by concern under `src/repository/`:

- `types.rs`: public repository structs and relation load plan types
- `executor.rs`: `QueryExecutor` and graph transaction boundary types
- `cache.rs`: aggregation cache traits and in-memory cache
- `base.rs`: metadata-backed `Repository` compile and CRUD helpers
- `context.rs`: `UserContext` repository assembly and context repository logging/cache invalidation
- `resolved.rs`: entity-scoped repository query and mutation entry points
- `graph.rs`: graph planning, graph save execution, and graph node validation
- `relation.rs`: relation plans, relation enhancement, child queries, and relation aggregates
- `helpers.rs`: shared bucket keys, cache keys, projection helpers, and graph relation validation

## Request Policy and Repository Behavior

`RequestPolicy` is the platform-level boundary. It runs on `ResolvedRepository`
select and mutation entry points before SQL is compiled or executed, and it is
intended for tenant isolation, permission enforcement, raw SQL control,
observability, and infrastructure protection.

`RepositoryBehavior` remains entity-scoped. Use it for domain-specific behavior
such as default relation loading or entity-specific command enrichment.

The runtime applies entity behavior first and request policy last, so platform
policy remains the final enforcement point before repository execution.

## Customer-Owned Mutation Policy

`MutationPolicy` is the application-owned boundary for an audited graph save.
After Checker/Fix and graph planning produce the complete immutable
`MutationPlan`, the runtime reviews it before the first provider mutation. A
denial on the context-owned save path happens before transaction begin. An
allowed save attaches one `MutationGovernanceSnapshot` to every audit event.

Applications install a typed `MutationPolicyRegistry`, an optional exact
`MutationPolicyApprovalProvider`, and an optional warning sink through trusted
`UserContext` assembly. Remote payloads cannot choose these implementations.
No customer policy remains backward compatible but emits
`MUTATION-POLICY-001`; an unapproved or identity-mismatched customer policy
emits `MUTATION-POLICY-002`. Approval matches policy ID, version, and
fingerprint.

See the maintained `examples/order-management` mutation-policy probe for an
approved save, a denied save with zero persisted rows, and a repeated run
against the same SQLite database.

## Example

```rust
use teaql_runtime::{InMemoryMetadataStore, UserContext};

let mut ctx = UserContext::new()
    .with_metadata(InMemoryMetadataStore::default());

let logs = ctx.sql_logs();
assert!(logs.is_empty());
```

SQL execution logging is enabled for select and mutation statements by default.
Use `ctx.disable_sql_log()` to turn it off, or `ctx.set_sql_log_options(...)`
to restrict which operations are recorded.

With generated code, applications typically register a generated runtime module
and then use generated entity/request helpers:

```rust
let ctx = UserContext::new()
    .with_module(my_domain::module())
    .with_repository_registry(my_domain::repository_registry());
```

Database, cache, message, search, AI, and filesystem integrations live outside
this crate as runtime providers/adapters. For SQLx-backed databases, choose one
database-specific provider crate:

```toml
teaql-runtime = "0.7"
teaql-provider-sqlx-postgres = "0.7"
teaql-provider-sqlx-sqlite = "0.7"
teaql-provider-sqlx-mysql = "0.7"
```

Provider crates expose a small registration helper. Runtime code registers
whichever provider the application selected, then calls the database-neutral
schema entry point:

```rust
use teaql_provider_sqlx_postgres::{PgMutationExecutor, PostgresProviderExt};
use teaql_runtime::{RuntimeError, UserContext};

let mut ctx = UserContext::new()
    .with_module(my_domain::module())
    .with_repository_registry(my_domain::repository_registry());

ctx.use_postgres_provider(PgMutationExecutor::new(pg_pool));
ctx.ensure_schema().await?;
```

The provider-specific helpers remain available for low-level use, but
application code should prefer `ctx.ensure_schema().await?` once a provider is
registered.

`teaql-runtime` itself no longer has a `sqlx` feature and no longer exports
`sqlx_support`.
