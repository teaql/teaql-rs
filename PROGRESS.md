# TeaQL Rust Rewrite Progress

This file tracks the current feature parity between the original Java TeaQL implementation and the Rust rewrite in `teaql-rs`.

Status labels used here:

- `Done`: implemented and verified in the current Rust codebase
- `MVP`: core path is implemented, but not yet feature-complete
- `Partial`: some support exists, but there are clear gaps
- `Not Started`: no meaningful implementation yet
- `Dropped`: intentionally not carried over into the Rust rewrite

Current progress estimates:

- Core runtime/kernel: `70%-80%`
- SQLite usable path: `80%+`
- PostgreSQL usable path: `55%-65%`
- Full Java feature parity: `40%-50%`

## Feature Matrix

| Capability | Java TeaQL | Rust `teaql-rs` current state | Status | Next Priority |
|---|---|---|---|---|
| Core entity metadata | Complete | `EntityDescriptor/PropertyDescriptor/RelationDescriptor` exists in `teaql-core` | Done | Low |
| Query DSL | Complete and richer | `Expr/OrderBy/Aggregate/SelectQuery` covers basic filter/sort/page/aggregate | MVP | Medium |
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
| SQL compiler | Complete and mature | `teaql-sql` supports select/insert/update/delete/recover | MVP | Medium |
| SQLite dialect | Present | Implemented and verified with `sqlx` integration tests | Done | Low |
| PostgreSQL dialect | Present | Implemented, but not yet validated against a real PG instance | Partial | High |
| SQL execution layer | Complete | `sqlx` execution path exists | MVP | Medium |
| Result decoding | Complete | Basic types supported: `NULL/bool/i64/f64/text`; `u64` ids bind correctly | Partial | High |
| `u64` id model | Java usually uses signed `Long` | Rust now uses `u64` ids while keeping `version` as `i64` | Done | Low |
| Derive macro for entities | Java relies on reflection/metadata classes | `#[derive(TeaqlEntity)]` implemented | MVP | Medium |
| Batch entity registration | Present via framework patterns | `register_entities!` implemented | Done | Low |
| Declarative runtime assembly | Spring/config driven | `RuntimeModule` and `module!` implemented | MVP | Medium |
| In-memory repository | Java has `teaql-memory` | Rust only has in-memory metadata/registry, not a real memory repository | Not Started | Medium |
| GraphQL integration | Java has `teaql-graphql` | Not implemented | Not Started | Low |
| Spring Boot autoconfigure | Java has `teaql-autoconfigure` | Not applicable in pure Rust rewrite | Dropped | None |
| Web/BaseService action entry | Java has `BaseService` | Not implemented | Not Started | Low |
| Multi-database dialect matrix | Java supports many databases | Rust intentionally focuses on PostgreSQL and SQLite | Dropped | None |
| Reflection-style parser registration | Java-style runtime mechanism | Not carried over into Rust | Dropped | None |
| JSON/time/date value support | More complete in Java | Metadata types exist, but execution/binding/decoding is incomplete | Not Started | High |
| PostgreSQL integration tests | Mature path | Not implemented yet | Not Started | High |
| Schema migration / schema tooling | Partial capability in Java ecosystem | Not implemented | Not Started | Medium |

## Current Strengths

- Core metadata model is in place
- Query AST and SQL compilation are usable
- CRUD, optimistic lock, soft delete, and recover are implemented
- `UserContext` is present as a first-class runtime concept
- Repository registry and behavior hooks are implemented
- Single-level and nested relation enhancement are working
- Entity derive macro exists and supports descriptor generation
- Runtime module assembly exists
- SQLite `sqlx` integration is real and tested

## Most Important Gaps

1. PostgreSQL real integration validation
2. Richer value support, especially JSON and time/date types
3. Result decoding beyond the current primitive set
4. Optional in-memory repository for tests and simplified execution
5. Higher-level service layer if Rust-side application APIs are needed

## Suggested Next Steps

1. Add PostgreSQL integration tests using an environment-provided connection string
2. Extend value binding and decoding for JSON, timestamp, and date
3. Tighten type handling so `u64` and signed integer behavior is explicit end-to-end
4. Decide whether a Rust-native service layer is needed, or whether repository-level APIs are enough
5. Add a real `examples/` directory so the current architecture is easier to run and review
