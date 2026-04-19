# TeaQL Rust Rewrite Progress

This file tracks the current feature parity between the original Java TeaQL implementation and the Rust rewrite in `teaql-rs`.

Status labels used here:

- `Done`: implemented and verified in the current Rust codebase
- `MVP`: core path is implemented, but not yet feature-complete
- `Partial`: some support exists, but there are clear gaps
- `Not Started`: no meaningful implementation yet
- `Dropped`: intentionally not carried over into the Rust rewrite

Current progress estimates:

- Core runtime/kernel: `90%`
- SQLite usable path: `85%-90%`
- PostgreSQL usable path: `85%-90%`
- Full Java feature parity: `60%-65%`

## Feature Matrix

| Capability | Java TeaQL | Rust `teaql-rs` current state | Status | Next Priority |
|---|---|---|---|---|
| Core entity metadata | Complete | `EntityDescriptor/PropertyDescriptor/RelationDescriptor` exists in `teaql-core` | Done | Low |
| Query DSL | Complete and richer | `Expr/OrderBy/Aggregate/SelectQuery` now covers core predicates, Java-style string match builders, `soundlike`/`SOUNDEX`, PG array-bound `IN_LARGE`/`NOT_IN_LARGE`, subquery filters, projections, expression projections, expression/function sorting, pagination, extended aggregates, and `HAVING`; decimal aggregate results use `Value::Decimal` instead of lossy `f64` | MVP+ | Medium |
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
| Checker infrastructure | Java has `Checker`, `CheckResult`, `ObjectLocation`, and `checkAndFix` through `UserContext` | `Checker`, `CheckResult`, `ObjectLocation`, object status, and `CheckerRegistry` are implemented; insert/update paths invoke the same `UserContext::check_and_fix_record()` entry and checkers distinguish create/update by object status | MVP | Medium |
| Entity mutation events | Java has `io.teaql.data.event` and `UserContext.sendEvent` | `EntityEventKind::{Created, Updated, Deleted, Recovered}`, `EntityEvent`, and `EntityEventSink` are implemented; ordinary repository writes and mixed graph writes dispatch events through `UserContext` | MVP | Medium |
| SQL compiler | Complete and mature | `teaql-sql` supports select/insert/update/delete/recover, grouped and extended aggregates, `COUNT(*)`, expression projection/sort, `HAVING`, subquery filters, and extended predicates | MVP+ | Medium |
| SQLite dialect | Present | Implemented and verified with `sqlx` integration tests | Done | Low |
| PostgreSQL dialect | Present | Implemented and validated against a real PostgreSQL instance through Docker-backed tests, including custom `soundex`, array-bound large IN, subqueries, extended aggregates, Decimal/NUMERIC decoding, and `teaql_id_space` id generation | MVP+ | Medium |
| SQL execution layer | Complete | `sqlx` execution path exists for SQLite and PostgreSQL, with explicit SQLite transaction helpers and PostgreSQL transaction executor | MVP+ | Medium |
| Result decoding | Complete | Primitive types plus checked `u64`/signed integer boundaries, Decimal/NUMERIC, JSON, date, and timestamp are supported | MVP+ | Medium |
| `u64` id model | Java usually uses signed `Long` | Rust now uses `u64` ids while keeping `version` as `i64` | Done | Low |
| Derive macro for entities | Java relies on reflection/metadata classes | `#[derive(TeaqlEntity)]` implemented | MVP | Medium |
| Batch entity registration | Present via framework patterns | `register_entities!` implemented | Done | Low |
| Declarative runtime assembly | Spring/config driven | `RuntimeModule` and `module!` implemented | MVP | Medium |
| In-memory repository | Java has `teaql-memory` | `MemoryRepository` supports query filtering/sorting/paging/projection, extended aggregates with Decimal results, typed `SmartList<T>`, and mutations | MVP | Medium |
| GraphQL integration | Java has `teaql-graphql` | Not implemented | Not Started | Low |
| Spring Boot autoconfigure | Java has `teaql-autoconfigure` | Not applicable in pure Rust rewrite | Dropped | None |
| Web/BaseService action entry | Java has `BaseService` | Not implemented | Not Started | Low |
| Multi-database dialect matrix | Java supports many databases | Rust intentionally focuses on PostgreSQL and SQLite | Dropped | None |
| Reflection-style parser registration | Java-style runtime mechanism | Not carried over into Rust | Dropped | None |
| JSON/time/date value support | More complete in Java | Binding/decoding works for JSON, `NaiveDate`, and `DateTime<Utc>` | MVP | Medium |
| PostgreSQL integration tests | Mature path | Real `sqlx` integration tests run when `TEAQL_TEST_PG_URL` is provided; query, graph transaction, id-space generation, and extended aggregate paths are Docker-validated | MVP+ | Medium |
| Schema ensure / bootstrap | Partial capability in Java ecosystem | SQLite and PostgreSQL `ensure_schema` support create-table and add-missing-column flows; PostgreSQL also installs the custom `soundex(text)` helper and both SQLx schema paths create `teaql_id_space` | MVP+ | Medium |
| Base entity model | Java has `BaseEntity` | `BaseEntityData` and `BaseEntity` cover `id/version/dynamic` | MVP | Low |
| SmartList and typed entities | Java has `SmartList<Entity>` | `SmartList<T>`, typed fetch, typed nested relation enhancement, and typed entity graph extraction are implemented | MVP+ | Medium |
| Id generator | Java has internal id generator and SQL `teaql_id_space` fallback | `InternalIdGenerator`, `SnowflakeIdGenerator`, and SQLx-backed `PgIdSpaceGenerator`/`SqliteIdSpaceGenerator` are integrated with repository insert preparation | Done | Low |
| Multi-level create graph write | Java has `saveGraph` | `save_graph()` and `save_entity_graph()` support nested create writes and relation-key maintenance | MVP+ | Medium |
| Multi-level update graph diff | Java reloads and merges graph updates | The same `save_graph()` and `save_entity_graph()` APIs support parent update, child merge, child insert, missing-child soft delete, reference-only nodes, explicit remove nodes, keep-missing relation metadata, duplicate child-id rejection, and reference reload validation | MVP+ | High |
| Graph mutation planning | Java classifies mixed entity states before persistence | `GraphMutationPlan` classifies mixed graph requests by entity type and operation (`Create/Update/Delete/Reference`) before execution; `UserContext::plan_for_save_graph()` exposes the plan for debugging; missing create ids are assigned before batching, creates/deletes merge by entity type, and updates merge only when updated fields match exactly | MVP | Medium |
| Graph entity state semantics | Java has new/reference/remove/deleted status concepts | `GraphOperation::{Upsert, Reference, Remove}` covers first-pass upsert/reference/remove semantics; reference/remove now validate existence, deleted state, child-relation conflicts, and reference version conflicts | MVP+ | High |
| Attach relation metadata | Java uses attach/reverse relation metadata | `RelationDescriptor` supports `attach/detached` and `delete_missing/keep_missing`; derive supports `attach = false` and `delete_missing = false` | MVP | Medium |
| Graph write transaction boundary | Java runs through repository/service transaction facilities | `save_graph()` now requires a transactional executor; SQLite auto transaction rollback and PostgreSQL connection-scoped transaction rollback are verified | MVP+ | Medium |
| Module/file organization | Java code is already split by concern | Rust core/runtime/macros/sql crates are now split into focused source modules | Done | Low |

## Current Strengths

- Core metadata model is in place
- Query AST and SQL compilation are now broad enough for most core Java-style querying
- Query builders cover common predicates, Java-style string matching, `soundlike`, large-IN, subqueries, sorting, pagination, relation loads, aggregate construction, expression projection/sort, and `HAVING`
- Decimal/NUMERIC aggregate output avoids lossy `f64` conversion for SQL aggregate results
- Entity mapping and SQLx bind/decode paths now make `u64`, signed integer, and Decimal conversion boundaries explicit
- CRUD, optimistic lock, soft delete, and recover are implemented
- `UserContext` is present as a first-class runtime concept
- Repository registry and behavior hooks are implemented
- Java-style checker registration is implemented with a single object-status-driven check/fix entry
- Java-style mutation event dispatch is available through `UserContext`
- Single-level and nested relation enhancement are working
- Entity derive macro exists and supports descriptor generation and typed record mapping
- `SmartList<T>` and typed nested entity loading are working
- Typed entity graphs can now be extracted from derived entities and passed directly into graph saves
- Dynamic-property JSON flattening is available for aggregate-style output
- Runtime module assembly exists
- SQLite `sqlx` integration is real and tested
- Runnable examples exist for SQLite schema bootstrap, CRUD, typed fetch, relation enhancement, graph writes, and PostgreSQL query features
- PostgreSQL `sqlx` integration and `ensure_schema` are validated, including custom `soundex(text)` bootstrap
- Java-style `teaql_id_space` id generation is available for PostgreSQL and SQLite SQLx paths
- `MemoryRepository` exists for no-database tests and simplified execution
- Create-graph writes can persist nested rows and maintain foreign keys in SQLite from either `GraphNode` or typed entities
- Graph upsert can update nested rows and soft-delete missing children in SQLite
- Graph writes now support validated reference-only nodes, explicit remove nodes, keep-missing relation metadata, duplicate child-id rejection, and stricter state-transition conflicts
- Graph writes are classified into mutation plans by entity and operation before execution
- `UserContext::plan_for_save_graph()` exposes graph plans for debugging, including generated create ids and merge batches
- SQLite graph writes are automatically wrapped in a transaction and rolled back on failure
- PostgreSQL graph writes can be wrapped in a connection-scoped transaction and rolled back
- SQL and memory paths both support grouped/extended aggregates, Decimal aggregate output, and extended predicates
- PostgreSQL query paths are validated for array-bound large IN, subqueries, expression projection/function ordering, extended aggregates, grouped aggregates, bit aggregates, and `HAVING`

## Most Important Gaps

1. More complete `MemoryRepository` parity for relation enhancement and subquery execution
2. Typed checker generation and richer Java-style validation semantics such as translated messages, Java status mapping, and nested typed object locations
3. Richer event payloads for old/new values and typed entity snapshots, matching Java `BaseEntity` property-change details
4. Broader value support beyond the current primitive/Decimal/JSON/date/timestamp set, especially `Uuid` and bytes
5. Higher-level service layer if Rust-side application APIs are needed
6. More complete schema migration tooling beyond additive `ensure_schema`

## Suggested Next Steps

1. Expand `MemoryRepository` toward relation enhancement and richer query parity, especially subqueries
2. Add derive/helper support for typed entity checkers so common validation can be registered without hand-written `Record` access
3. Expand event payloads if consumers need old/new property values instead of changed-field names plus final values
4. Extend value binding and decoding for `Uuid` and bytes
5. Decide whether a Rust-native service layer is needed, or whether repository-level APIs are enough
