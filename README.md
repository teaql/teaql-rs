# teaql-rs

Pure Rust rewrite of TeaQL with a narrowed scope:

- PostgreSQL and SQLite only
- Rust-native metadata and query AST
- SQL compiler and runtime separated from web/framework concerns
- Compatibility with the Java implementation is not a goal

## Workspace layout

- `teaql-core`: metadata, values, filters, ordering, aggregates, query model
- `teaql-sql`: SQL dialect trait and AST-to-SQL compiler
- `teaql-runtime`: metadata lookup, repository boundary, executor abstraction
- `teaql-runtime`: metadata lookup, repository boundary, executor abstraction, optional `sqlx` mutation executors
- `teaql-runtime`: metadata lookup, repository boundary, executor abstraction, optional `sqlx` PG/SQLite read-write executors
- `teaql-runtime`: metadata lookup, `UserContext` resource index, repository boundary, optional `sqlx` PG/SQLite read-write executors
- `teaql-runtime`: metadata lookup, `UserContext` resource index, repository registry, optional `sqlx` PG/SQLite read-write executors
- `teaql-runtime`: metadata lookup, `UserContext` resource index, repository registry, behavior registry, optional `sqlx` PG/SQLite read-write executors
- `teaql-runtime`: metadata lookup, `UserContext` resource index, repository registry, behavior registry, `RuntimeModule`, optional `sqlx` PG/SQLite read-write executors
- `teaql-macros`: `TeaqlEntity` derive macro for declarative metadata definitions
- `teaql-dialect-pg`: PostgreSQL quoting and placeholder strategy
- `teaql-dialect-sqlite`: SQLite quoting and placeholder strategy

## Current scope

The first scaffold focuses on read-path compilation:

- entity and relation descriptors
- query projection, filter, sort, group-by, limit, offset
- aggregate projection
- PostgreSQL and SQLite placeholder differences
- insert, update, delete command models
- optimistic locking through `id + version` predicates on update/delete
- recover and batch mutation helpers
- in-memory metadata store for bootstrapping descriptors
- Rust-native `UserContext` for metadata resolution, typed resources, named resources, and local request-scope storage
- `UserContext` can assemble a repository from registered dialect and executor resources, making it the main runtime entry point
- `UserContext` can also resolve repositories by entity type through `RepositoryRegistry`
- per-entity `RepositoryBehavior` hooks can mutate queries/commands and expose relation-load plans
- relation preload plans can now be resolved from behavior hooks and converted into child batch queries from parent rows
- relation enhancement now supports batch child-query generation and backfilling related records into parent records
- optional `sqlx` support module for PostgreSQL and SQLite execution
- SQLite in-memory integration test for insert/select/update/delete/recover under `--features sqlx`
- SQLite in-memory integration tests for both CRUD and relation enhancement under `--features sqlx`
- first `TeaqlEntity` derive support for declarative entity descriptors
- declarative runtime assembly through `RuntimeModule` and `module!`

## Next steps

1. Add joins and relation preloading plans.
2. Validate the PostgreSQL executor against a real Postgres instance.
3. Add derive macros for entities and descriptors.
4. Add joins and relation preloading plans to runtime execution.
