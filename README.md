# teaql-rs

Pure Rust rewrite of TeaQL with a narrowed scope:

- PostgreSQL and SQLite only
- Rust-native metadata and query AST
- SQL compiler and runtime separated from web/framework concerns
- Compatibility with the Java implementation is not a goal

Progress tracking lives in [PROGRESS.md](./PROGRESS.md).

## Workspace layout

- `teaql-core`: metadata, entity traits, base entity data, values, filters, ordering, aggregates, query model, and `SmartList<T>`
- `teaql-sql`: SQL dialect trait, compiled query types, DDL helpers, and AST-to-SQL compiler
- `teaql-runtime`: `UserContext`, metadata lookup, repository boundary, repository registry, behavior registry, id generation, `RuntimeModule`, and optional `sqlx` PG/SQLite executors
- `teaql-macros`: `TeaqlEntity` derive macro plus attribute parsing and record/entity mapping generation
- `teaql-dialect-pg`: PostgreSQL quoting and placeholder strategy
- `teaql-dialect-sqlite`: SQLite quoting and placeholder strategy

The large crates are now split by function instead of keeping all implementation in a single
`lib.rs`:

- `teaql-core/src`: `entity.rs`, `expr.rs`, `list.rs`, `meta.rs`, `mutation.rs`, `naming.rs`, `query.rs`, `value.rs`
- `teaql-sql/src`: `dialect.rs`, `types.rs`
- `teaql-runtime/src`: `context.rs`, `error.rs`, `id.rs`, `memory.rs`, `registry.rs`, `repository.rs`, `sqlx_support.rs`
- `teaql-macros/src`: `attr.rs`, `derive_impl.rs`, `mapping.rs`, `types.rs`

## Current scope

The current implementation focuses on the Rust-native core runtime:

- entity and relation descriptors
- query projection, filter, sort, group-by, limit, offset
- aggregate projection
- richer query builders for expressions, projections, sort, pagination, relation loads, and aggregates
- extended predicates including `between`, `is null`, `is not null`, Java-style `contain`/`begin_with`/`end_with`, `not like`, `not in`, and `soundlike` through `SOUNDEX`
- grouped aggregate SQL and memory execution, including `COUNT(*)`
- aggregate decimal results use `Value::Decimal`/PostgreSQL `NUMERIC` instead of lossy `f64`
- PostgreSQL `IN_LARGE`/`NOT_IN_LARGE` compile to array binds with `ANY`/`ALL`
- subquery filters can compile `field IN (SELECT ...)` / `field NOT IN (SELECT ...)`
- expression projections, expression/function ordering, extended aggregates, and `HAVING`
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
- typed entity mapping through `Entity` and `SmartList<T>`
- typed nested relation enhancement through `fetch_enhanced_entities::<T>()`
- first-pass create-graph write path through `GraphNode` and `save_graph_create()`
- graph upsert path through `save_graph()`, including parent update, child merge, child insert, and missing-child soft delete
- graph write state hints: `Upsert`, `Reference`, and `Remove`
- relation metadata for graph writes: `attach/detached` and `delete_missing/keep_missing`
- SQLite transaction boundary helpers for graph-write wrapping and rollback testing
- PostgreSQL connection-scoped transaction executor for graph-write wrapping and rollback testing
- declarative runtime assembly through `RuntimeModule` and `module!`
- built-in `SnowflakeIdGenerator` and `UserContext`-driven id generation
- `BaseEntityData` / `BaseEntity` for shared `id + version + dynamic` entity state
- dynamic-property capture through `#[teaql(dynamic)]`, with JSON flattening for aggregate-style outputs
- `MemoryRepository` for no-database tests and lightweight in-memory execution
- optional `sqlx` support module for PostgreSQL and SQLite execution
- SQLite `ensure_schema` support for create-table and add-missing-column flows
- PostgreSQL `ensure_schema` support with real multi-table integration validation
- `UserContext::ensure_sqlite_schema()` as the high-level SQLite schema entry point
- `UserContext::ensure_postgres_schema()` as the high-level PostgreSQL schema entry point
- JSON, date, and timestamp bind/decode support in the `sqlx` execution path
- SQLite in-memory integration tests for CRUD and relation enhancement under `--features sqlx`
- SQLite integration coverage for nested create-graph writes
- SQLite integration coverage for nested graph update diff
- SQLite integration coverage for reference-only nodes, explicit remove, keep-missing relation metadata, and transaction rollback
- PostgreSQL integration coverage for graph-write transaction rollback when `TEAQL_TEST_PG_URL` is provided
- PostgreSQL integration tests under `--features sqlx` when `TEAQL_TEST_PG_URL` is provided

## Typed entities and `SmartList<T>`

`TeaqlEntity` derive now generates both metadata and typed `Entity` mapping. Repository APIs can
return either raw `Record` rows or typed `SmartList<T>` collections.

```rust
use teaql_core::{Expr, SelectQuery, SmartList, TeaqlEntity};

#[derive(Clone, Debug, Default, TeaqlEntity)]
#[teaql(entity = "CatalogProduct", table = "catalog_product_data")]
struct CatalogProductRow {
    #[teaql(id, column = "id")]
    id: u64,
    #[teaql(version, column = "version")]
    version: i64,
    #[teaql(column = "name")]
    name: String,
}

fn query() -> SelectQuery {
    SelectQuery::new("CatalogProduct").filter(Expr::eq("name", "desk"))
}

async fn fetch_products(
    repo: &teaql_runtime::ResolvedRepository<'_>,
) -> Result<SmartList<CatalogProductRow>, teaql_runtime::RepositoryError> {
    repo.fetch_entities::<CatalogProductRow>(&query())
}
```

`SmartList<T>` keeps TeaQL-style list metadata alongside the typed rows:

- `data`
- `total_count`
- `aggregations`
- `summary`

When the entity defines `#[teaql(id)]` or `#[teaql(version)]`, `SmartList<T>` also exposes:

- `ids()`
- `versions()`
- `into_records()`

## Typed relation enhancement

`fetch_enhanced_entities::<T>()` runs record-based relation enhancement first, then converts the
result into typed nested entities.

```rust
use teaql_core::{SelectQuery, SmartList, TeaqlEntity};

#[derive(Clone, Debug, Default, TeaqlEntity)]
#[teaql(entity = "Product", table = "product_data")]
struct ProductRow {
    #[teaql(id, column = "id")]
    id: u64,
    #[teaql(version, column = "version")]
    version: i64,
    #[teaql(column = "name")]
    name: String,
}

#[derive(Clone, Debug, Default, TeaqlEntity)]
#[teaql(entity = "OrderLine", table = "order_line_data")]
struct OrderLineRow {
    #[teaql(id, column = "id")]
    id: u64,
    #[teaql(version, column = "version")]
    version: i64,
    #[teaql(column = "order_id")]
    order_id: u64,
    #[teaql(
        relation(
            target = "Product",
            local_key = "product_id",
            foreign_key = "id"
        )
    )]
    product: Option<ProductRow>,
}

#[derive(Clone, Debug, Default, TeaqlEntity)]
#[teaql(entity = "Order", table = "order_data")]
struct OrderRow {
    #[teaql(id, column = "id")]
    id: u64,
    #[teaql(version, column = "version")]
    version: i64,
    #[teaql(
        relation(
            target = "OrderLine",
            local_key = "id",
            foreign_key = "order_id",
            many
        )
    )]
    lines: SmartList<OrderLineRow>,
}

async fn fetch_orders(
    repo: &teaql_runtime::ResolvedRepository<'_>,
) -> Result<SmartList<OrderRow>, teaql_runtime::RepositoryError> {
    repo.fetch_enhanced_entities::<OrderRow>(&SelectQuery::new("Order"))
}
```

For nested enhancement, register relation paths from repository behavior, for example:

- `lines`
- `lines.product`

## SQLite schema bootstrap

With the `sqlx` feature enabled, SQLite schema setup can be driven directly from `UserContext`.

```rust
use sqlx::sqlite::SqlitePoolOptions;
use teaql_core::{DataType, EntityDescriptor, PropertyDescriptor};
use teaql_dialect_sqlite::SqliteDialect;
use teaql_runtime::sqlx_support::SqliteMutationExecutor;
use teaql_runtime::{RuntimeModule, UserContext};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;

    let order = EntityDescriptor::new("Order")
        .table_name("orders")
        .property(PropertyDescriptor::new("id", DataType::U64).column_name("id").id().not_null())
        .property(
            PropertyDescriptor::new("version", DataType::I64)
                .column_name("version")
                .version()
                .not_null(),
        )
        .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"));

    let module = RuntimeModule::new().descriptor(order);
    let mut ctx = UserContext::new().with_module(module);
    ctx.insert_resource(SqliteDialect);
    ctx.insert_resource(SqliteMutationExecutor::new(pool));

    ctx.ensure_sqlite_schema().await?;
    Ok(())
}
```

Current SQLite `ensure_schema` scope:

- create missing tables
- add missing columns to existing tables
- do not attempt destructive migrations such as drop column, type rewrite, or primary-key rebuild

## Next steps

1. Add runnable examples that show module assembly, schema bootstrap, CRUD, typed entities, and relation enhancement.
2. Expand graph writes toward richer Java-style reload/merge semantics and typed graph extraction.
3. Keep expanding value coverage beyond the current JSON/date/timestamp set, especially `Uuid`, decimal, and bytes.
4. Decide whether a Rust-native service layer is needed above repository/runtime APIs.
5. Expand `MemoryRepository` toward relation enhancement and richer parity with the SQL-backed path.
