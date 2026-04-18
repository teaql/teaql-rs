# TeaQL Rust Rewrite Progress

This file tracks the current feature parity between the original Java TeaQL implementation and the Rust rewrite in `teaql-rs`.

Status labels used here:

- `Done`: implemented and verified in the current Rust codebase
- `MVP`: core path is implemented, but not yet feature-complete
- `Partial`: some support exists, but there are clear gaps
- `Not Started`: no meaningful implementation yet
- `Dropped`: intentionally not carried over into the Rust rewrite

Current progress estimates:

- Core runtime/kernel: `80%-85%`
- SQLite usable path: `85%+`
- PostgreSQL usable path: `70%-75%`
- Full Java feature parity: `50%-60%`

## Feature Matrix

| Capability | Java TeaQL | Rust `teaql-rs` current state | Status | Next Priority |
|---|---|---|---|---|
| Core entity metadata | Complete | `EntityDescriptor/PropertyDescriptor/RelationDescriptor` exists in `teaql-core` | Done | Low |
| Query DSL | Complete and richer | `Expr/OrderBy/Aggregate/SelectQuery` now has richer builders for predicates, projections, expression projections, expression/function sorting, pagination, relations, extended aggregates, `HAVING`, Java-style string match builders, PG array-bound `IN_LARGE`/`NOT_IN_LARGE`, subquery filters, and `soundlike`/`SOUNDEX` | MVP+ | Medium |
| Repository abstraction | Complete | `Repository/ContextRepository/ResolvedRepository` implemented | Done | Low |
| Insert/update/delete | Complete | Supported | Done | Low |
| Optimistic lock | Complete | Supported, `version` remains `i64` | Done | Low |
| Soft delete / recover | Complete | Supported | Done | Low |
| Batch mutation | Complete | `insert_many/update_many/delete_many/recover_many` exists | MVP | Low |
| Relation metadata | Complete | Supported in descriptors and derive macro | Done | Low |
| Relation preload planning | Complete | Relation load plans support nested paths like `lines.product` | Done | Medium |
| Single-level relation enhancement | Complete | Supported and verified | Done | Low |
| Multi-level relation enhancement | Complete | Recursive enhancement implemented and tested on SQLite | MVP | Medium |
| UserContext | Core concept | Implemented as runtime resource index and request-scope store | Done | Medium |
| RepositoryRegistry | Present | In-memory registry implemented | MVP | Low |
| Entity-level behavior hooks | Present | `before_select/insert/update/delete/recover` supported | MVP | Medium |
| SQL compiler | Complete and mature | `teaql-sql` supports select/insert/update/delete/recover, grouped aggregates, `COUNT(*)`, and extended predicates | MVP+ | Medium |
| SQLite dialect | Present | Implemented and verified with `sqlx` integration tests | Done | Low |
| PostgreSQL dialect | Present | Implemented and validated against a real PostgreSQL instance through Docker-backed tests | MVP | Medium |
| SQL execution layer | Complete | `sqlx` execution path exists for SQLite and PostgreSQL | MVP | Medium |
| Result decoding | Complete | Primitive types plus `u64`, JSON, date, and timestamp are supported | MVP | Medium |
| `u64` id model | Java usually uses signed `Long` | Rust now uses `u64` ids while keeping `version` as `i64` | Done | Low |
| Derive macro for entities | Java relies on reflection/metadata classes | `#[derive(TeaqlEntity)]` implemented | MVP | Medium |
| Batch entity registration | Present via framework patterns | `register_entities!` implemented | Done | Low |
| Declarative runtime assembly | Spring/config driven | `RuntimeModule` and `module!` implemented | MVP | Medium |
| In-memory repository | Java has `teaql-memory` | `MemoryRepository` supports query filtering/sorting/paging/projection, basic aggregates, typed `SmartList<T>`, and mutations | MVP | Medium |
| GraphQL integration | Java has `teaql-graphql` | Not implemented | Not Started | Low |
| Spring Boot autoconfigure | Java has `teaql-autoconfigure` | Not applicable in pure Rust rewrite | Dropped | None |
| Web/BaseService action entry | Java has `BaseService` | Not implemented | Not Started | Low |
| Multi-database dialect matrix | Java supports many databases | Rust intentionally focuses on PostgreSQL and SQLite | Dropped | None |
| Reflection-style parser registration | Java-style runtime mechanism | Not carried over into Rust | Dropped | None |
| JSON/time/date value support | More complete in Java | Binding/decoding works for JSON, `NaiveDate`, and `DateTime<Utc>` | MVP | Medium |
| PostgreSQL integration tests | Mature path | Real `sqlx` integration tests run when `TEAQL_TEST_PG_URL` is provided | MVP | Medium |
| Schema ensure / bootstrap | Partial capability in Java ecosystem | SQLite and PostgreSQL `ensure_schema` support create-table and add-missing-column flows | MVP | Medium |
| Base entity model | Java has `BaseEntity` | `BaseEntityData` and `BaseEntity` cover `id/version/dynamic` | MVP | Low |
| SmartList and typed entities | Java has `SmartList<Entity>` | `SmartList<T>`, typed fetch, and typed nested relation enhancement are implemented | MVP | Medium |
| Id generator | Java has internal id generator | `InternalIdGenerator` plus `SnowflakeIdGenerator` integrated into repository inserts | Done | Low |
| Multi-level create graph write | Java has `saveGraph` | `GraphNode` and `save_graph_create()` support nested create writes and relation-key maintenance | MVP | Medium |
| Multi-level update graph diff | Java reloads and merges graph updates | `save_graph()` supports parent update, child merge, child insert, and missing-child soft delete | MVP | High |
| Graph entity state semantics | Java has new/reference/remove/deleted status concepts | `GraphOperation::{Upsert, Reference, Remove}` covers first-pass upsert/reference/delete semantics | MVP | High |
| Attach relation metadata | Java uses attach/reverse relation metadata | `RelationDescriptor` supports `attach/detached` and `delete_missing/keep_missing`; derive supports `attach = false` and `delete_missing = false` | MVP | Medium |
| Graph write transaction boundary | Java runs through repository/service transaction facilities | SQLite rollback helpers and PostgreSQL connection-scoped transaction executor are verified with graph-write rollback | MVP | Medium |
| Module/file organization | Java code is already split by concern | Rust core/runtime/macros/sql crates are now split into focused source modules | Done | Low |

## Current Strengths

- Core metadata model is in place
- Query AST and SQL compilation are usable
- Query builders now cover common predicates, sorting, pagination, relation loads, and aggregate construction
- CRUD, optimistic lock, soft delete, and recover are implemented
- `UserContext` is present as a first-class runtime concept
- Repository registry and behavior hooks are implemented
- Single-level and nested relation enhancement are working
- Entity derive macro exists and supports descriptor generation and typed record mapping
- `SmartList<T>` and typed nested entity loading are working
- Dynamic-property JSON flattening is available for aggregate-style output
- Runtime module assembly exists
- SQLite `sqlx` integration is real and tested
- PostgreSQL `sqlx` integration and `ensure_schema` are validated
- `MemoryRepository` exists for no-database tests and simplified execution
- Create-graph writes can persist nested rows and maintain foreign keys in SQLite
- Graph upsert can update nested rows and soft-delete missing children in SQLite
- Graph writes now support reference-only nodes, explicit remove nodes, and keep-missing relation metadata
- SQLite graph writes can be wrapped in an explicit transaction and rolled back
- PostgreSQL graph writes can be wrapped in a connection-scoped transaction and rolled back
- SQL and memory paths both support grouped aggregates and extended predicates

## Most Important Gaps

1. Richer Java-style reload/merge semantics, including explicit reference validation and more status transitions
2. Typed entity graph extraction so callers do not need to manually build `GraphNode`
3. Broader value support beyond the current primitive/JSON/date/timestamp set
4. More complete `MemoryRepository` parity, especially relation enhancement
5. Runnable examples for schema bootstrap, CRUD, typed entities, graph writes, and relations
6. Higher-level service layer if Rust-side application APIs are needed
7. More complete schema migration tooling beyond additive `ensure_schema`

## Suggested Next Steps

1. Add typed entity graph extraction so `save_graph()` can be driven from typed entities
2. Add a real `examples/` directory so the current architecture is easier to run and review
3. Extend value binding and decoding for `Uuid`, decimal, and bytes
4. Tighten type handling so `u64` and signed integer behavior is explicit end-to-end
5. Decide whether a Rust-native service layer is needed, or whether repository-level APIs are enough
6. Expand `MemoryRepository` toward relation enhancement and richer query parity
