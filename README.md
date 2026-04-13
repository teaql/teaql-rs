# teaql-rs

Pure Rust rewrite of TeaQL with a narrowed scope:

- PostgreSQL and SQLite only
- Rust-native metadata and query AST
- SQL compiler and runtime separated from web/framework concerns
- Compatibility with the Java implementation is not a goal

Progress tracking lives in [PROGRESS.md](./PROGRESS.md).

## Workspace layout

- `teaql-core`: metadata, values, filters, ordering, aggregates, query model
- `teaql-sql`: SQL dialect trait and AST-to-SQL compiler
- `teaql-runtime`: `UserContext`, metadata lookup, repository boundary, repository registry, behavior registry, `RuntimeModule`, optional `sqlx` PG/SQLite executors
- `teaql-macros`: `TeaqlEntity` derive macro for declarative metadata definitions
- `teaql-dialect-pg`: PostgreSQL quoting and placeholder strategy
- `teaql-dialect-sqlite`: SQLite quoting and placeholder strategy

## Current scope

The current implementation focuses on the Rust-native core runtime:

- entity and relation descriptors
- query projection, filter, sort, group-by, limit, offset
- aggregate projection
- PostgreSQL and SQLite placeholder differences
- insert, update, delete, and recover command models
- optimistic locking through `id + version` predicates on update/delete/recover
- recover and batch mutation helpers
- in-memory metadata store for bootstrapping descriptors
- Rust-native `UserContext` for metadata resolution, typed resources, named resources, and local request-scope storage
- `UserContext` can assemble a repository from registered dialect and executor resources, making it the main runtime entry point
- `UserContext` can also resolve repositories by entity type through `RepositoryRegistry`
- per-entity `RepositoryBehavior` hooks can mutate queries/commands and expose relation-load plans
- relation preload plans can now be resolved from behavior hooks and converted into child batch queries from parent rows
- relation enhancement supports batch child-query generation and backfilling related records into parent records
- nested relation enhancement supports paths like `lines.product`
- `TeaqlEntity` derive support for declarative entity descriptors
- declarative runtime assembly through `RuntimeModule` and `module!`
- optional `sqlx` support module for PostgreSQL and SQLite execution
- SQLite in-memory integration tests for CRUD and relation enhancement under `--features sqlx`

## Next steps

1. Validate the PostgreSQL executor against a real PostgreSQL instance.
2. Extend value binding and decoding for JSON, timestamp, and date types.
3. Add examples that show entity derive, module assembly, CRUD, and relation enhancement.
4. Decide whether a Rust-native service layer is needed above repository/runtime APIs.
