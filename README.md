# teaql-rs

TeaQL is a DDD-oriented data runtime for applications that want generated,
typed domain APIs instead of hand-written repository boilerplate.

The Rust workspace provides the runtime pieces: entity metadata, a query AST,
SQL compilation, relation enhancement, graph writes, checkers, mutation events,
and PostgreSQL/SQLite executors. The sibling `teaql-code-gen` project can turn a
compact domain model into a typed Rust service crate with entity structs,
`Q::merchants()`-style query builders, behavior/checker hooks, and graph-save
entrypoints.

TeaQL is not trying to be a general replacement for Diesel, SeaORM, or direct
`sqlx` use:

- use TeaQL when the domain model is central, relation graphs matter, and you
  want generated high-level APIs that resemble the Java TeaQL style;
- use Diesel or SeaORM when you want a conventional Rust ORM with a broad
  ecosystem;
- use `sqlx` directly when explicit SQL is the right abstraction and generated
  domain APIs would get in the way.

The Rust rewrite keeps the scope deliberately narrow:

- PostgreSQL and SQLite only
- Rust-native metadata and query AST
- SQL compiler and runtime separated from web/framework concerns
- compatibility with every Java implementation detail is not a goal, but the
  high-level TeaQL programming model is being carried over where it is useful

Progress tracking lives in [PROGRESS.md](./PROGRESS.md).

Current published release: `4.0.0`.

## Try It

The quickest demo uses SQLite in memory and needs no database server:

```bash
cargo run -p teaql-examples --bin sqlite_relations_graph
```

It bootstraps the schema, saves an `Order` graph with nested `OrderLine` and
`Product` objects, reloads the relation path `lines.product`, and prints the
typed result.

For a smaller schema/bootstrap and CRUD path:

```bash
cargo run -p teaql-examples --bin sqlite_schema_crud
```

For generated-service style APIs, use `teaql-code-gen` to generate a service
crate and write application code against `Q`:

```rust
use crm_erp_service::Q;

let platforms = Q::platforms()
    .select_merchant_list_with(
        Q::merchants()
            .select_name()
            .which_names_contain("TeaQL"),
    )
    .execute_for_list(&ctx)
    .await?;
```

## Workspace layout

- `teaql-core`: metadata, entity traits, base entity data, values, filters, ordering, aggregates, query model, and `SmartList<T>`
- `teaql-sql`: SQL dialect trait, compiled query types, DDL helpers, and AST-to-SQL compiler
- `teaql-runtime`: minimal runtime mechanism, `UserContext`, metadata lookup, repository boundary, repository registry, behavior registry, checker registry, entity event sink, id generation, `RuntimeModule`, and in-memory execution
- `teaql-provider-postgres`: PostgreSQL native adapter (deadpool-postgres), schema bootstrap, transaction wrapper, row decoding, and ID-space generator
- `teaql-provider-sqlite`: SQLite native adapter (rusqlite), schema bootstrap, transaction wrapper, row decoding, and ID-space generator
- `teaql-provider-mysql`: MySQL native adapter (mysql_async), schema bootstrap, transaction wrapper, row decoding, and ID-space generator
- `teaql-provider-rusqlite`: synchronous SQLite adapter for embedded and multi-architecture deployments such as routers, robots, and appliance controllers
- `teaql-macros`: `TeaqlEntity` derive macro plus attribute parsing and record/entity mapping generation

The large crates are now split by function instead of keeping all implementation in a single
`lib.rs`:

- `teaql-core/src`: `entity.rs`, `expr.rs`, `list.rs`, `meta.rs`, `mutation.rs`, `naming.rs`, `query.rs`, `value.rs`
- `teaql-sql/src`: `dialect.rs`, `types.rs`
- `teaql-runtime/src`: `checker.rs`, `context.rs`, `error.rs`, `event.rs`, `graph.rs`, `id.rs`, `memory.rs`, `registry.rs`, `repository/`
- `teaql-provider-postgres/src`: PostgreSQL provider adapter
- `teaql-provider-sqlite/src`: SQLite provider adapter
- `teaql-provider-mysql/src`: MySQL provider adapter
- `teaql-provider-rusqlite/src`: synchronous rusqlite provider adapter
- `teaql-runtime/src/repository`: `base.rs`, `cache.rs`, `context.rs`, `executor.rs`, `graph.rs`, `helpers.rs`, `relation.rs`, `resolved.rs`, `types.rs`
- `teaql-macros/src`: `attr.rs`, `derive_impl.rs`, `mapping.rs`, `types.rs`

## Current scope

The current implementation focuses on the Rust-native core runtime:

- entity and relation descriptors
- query projection, filter, sort, group-by, limit, offset
- aggregate projection
- richer query builders for expressions, projections, sort, pagination, relation loads, and aggregates
- Java-style null-safe value expressions through `SafeExpression`, including `apply`, `or_else`, empty checks, callbacks, `BaseEntity` id/version helpers, and `SmartList` size/first/get helpers
- Java-style web API payload helpers through `WebStyle`, `WebAction`, and `WebResponse` for server-controlled display metadata and frontend response data
- Java-style simplified Excel block model through `XlsBlock`, `XlsBlockBuildContext`, `XlsPage`, and `XlsWorkbook`
- extended predicates including `between`, `is null`, `is not null`, Java-style `contain`/`begin_with`/`end_with`, `not like`, `not in`, and `soundlike` through `SOUNDEX`
- grouped aggregate SQL and memory execution, including `COUNT(*)`
- aggregate decimal results use `Value::Decimal`/PostgreSQL `NUMERIC` instead of lossy `f64`
- `u64`, signed integers, and Decimal now have explicit checked conversion behavior in entity mapping and database bind/decode paths
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
- Java-style checker infrastructure through `Checker`, `CheckResult`, `ObjectLocation`, object status, and `CheckerRegistry`
- repository insert/update preparation invokes the single `UserContext::check_and_fix_record()` path and lets checkers distinguish create/update by object status
- Java-style natural-language checker message translation with 15 built-in languages and `UserContext` language switching
- Java-style entity mutation events through `EntityEvent`, `EntityEventKind`, and `EntityEventSink`
- `UserContext` can dispatch created/updated/deleted/recovered events from ordinary repository mutations and graph writes
- relation preload plans can now be resolved from behavior hooks and converted into child batch queries from parent rows
- relation enhancement supports batch child-query generation and backfilling related records into parent records
- nested relation enhancement supports paths like `lines.product`
- relation aggregate enhancement supports count/statistic properties on parent rows, including cases where the database column name differs from the entity property name
- `TeaqlEntity` derive support for declarative entity descriptors
- typed entity mapping through `Entity` and `SmartList<T>`
- typed nested relation enhancement through `fetch_enhanced_entities::<T>()`
- unified graph write path through `GraphNode` and `save_graph()`, covering create and upsert requests
- graph upsert path through `save_graph()`, including parent update, child merge, child insert, and missing-child soft delete
- graph writes build a `GraphMutationPlan` classified by entity type and operation before execution
- graph planning is available through `UserContext::plan_for_save_graph()` for debugging
- graph planning assigns missing create ids before batching; creates/deletes merge by entity type, updates merge only when the updated field set is identical
- `save_graph()` requires a transactional executor and rolls back the graph write when any planned operation fails
- typed entity graph extraction through `graph_node_from_entity()` and `save_entity_graph()`
- graph write state hints: `Upsert`, `Reference`, and `Remove`
- stricter graph state semantics: reference nodes validate existence/version/deleted state, remove nodes validate existence, and many-relation merge rejects duplicate child ids
- relation metadata for graph writes: `attach/detached` and `delete_missing/keep_missing`
- native transaction boundary helpers for graph-write wrapping and rollback testing live in the per-database provider crates
- declarative runtime assembly through `RuntimeModule` and `module!`
- built-in `SnowflakeIdGenerator` and `UserContext`-driven id generation; native `teaql_id_space` generators live in the per-database provider crates
- `BaseEntityData` / `BaseEntity` for shared `id + version + dynamic` entity state
- dynamic-property capture through `#[teaql(dynamic)]`, with JSON flattening for aggregate-style outputs
- `MemoryRepository` for no-database tests and lightweight in-memory execution
- PostgreSQL, SQLite, and MySQL execution moved to per-database provider crates using native drivers
- SQLite `ensure_schema` support for create-table and add-missing-column flows in `teaql-provider-sqlite`
- PostgreSQL `ensure_schema` support with real multi-table integration validation, including `soundex(text)` and `teaql_id_space` bootstrap, in `teaql-provider-postgres`
- `UserContext::ensure_schema()` as the database-neutral schema entry point after a provider is registered
- JSON, Decimal, date, and timestamp bind/decode support in the per-database providers
- SQLite in-memory integration tests for CRUD and relation enhancement in the provider
- SQLite integration coverage for nested create-graph writes
- SQLite integration coverage for nested graph update diff
- SQLite integration coverage for reference-only nodes, explicit remove, keep-missing relation metadata, and transaction rollback
- SQLite relation aggregate coverage through generated high-level `Q` APIs in an external generated service crate
- PostgreSQL integration coverage for graph-write transaction rollback when `TEAQL_TEST_PG_URL` is provided
- PostgreSQL integration tests in the provider when `TEAQL_TEST_PG_URL` is provided

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

## Database provider schema bootstrap

Schema setup is implemented by the selected database provider, but exposed through
the database-neutral runtime entry point:

```rust
ctx.ensure_schema().await?;
```

Each database provider exposes a registration helper that installs its dialect,
executor, and schema provider into `UserContext`.

PostgreSQL:

```rust
use teaql_provider_postgres::{
    PgMutationExecutor, PostgresProviderExt,
};

ctx.use_postgres_provider(PgMutationExecutor::new(pg_pool));
ctx.ensure_schema().await?;
```

SQLite:

```rust
use teaql_provider_sqlite::{SqliteMutationExecutor, SqliteProviderExt};

ctx.use_sqlite_provider(SqliteMutationExecutor::new(sqlite_pool));
ctx.ensure_schema().await?;
```

MySQL:

```rust
use teaql_provider_mysql::{
    MysqlMutationExecutor, MysqlProviderExt,
};

ctx.use_mysql_provider(MysqlMutationExecutor::new(mysql_pool));
ctx.ensure_schema().await?;
```

rusqlite:

```rust
use rusqlite::Connection;
use teaql_provider_rusqlite::{RusqliteMutationExecutor, RusqliteProviderExt};

let executor = RusqliteMutationExecutor::new(Connection::open("app.db")?);
ctx.use_rusqlite_provider(executor);
ctx.ensure_schema().await?;
```

Current `ensure_schema` scope:

- create missing tables
- add missing columns to existing tables
- do not attempt destructive migrations such as drop column, type rewrite, or primary-key rebuild

## Examples

Runnable examples live in the `teaql-examples` workspace package:

```bash
cargo run -p teaql-examples --bin sqlite_schema_crud
cargo run -p teaql-examples --bin sqlite_relations_graph
```

The PostgreSQL example uses real schema bootstrap and PG query features. Set
`TEAQL_TEST_PG_URL` before running it:

```bash
TEAQL_TEST_PG_URL=postgres://postgres:postgres@127.0.0.1:55440/teaql_examples \
  cargo run -p teaql-examples --bin pg_query_features
```

Current examples cover:

- SQLite schema bootstrap, CRUD, optimistic lock delete/recover, and typed entity fetch
- SQLite typed entity graph writes through `save_entity_graph()`
- SQLite relation enhancement through `fetch_enhanced_entities::<T>()`
- PostgreSQL `soundlike`/`SOUNDEX`, array-bound large IN, expression projection/sort, extended aggregates, and `HAVING`
- Generated-service validation with high-level `Q` APIs covers complex object commit, subtrait-style DDD methods, JSON serialization, JSON-expression search, and simple-to-relation statistics against SQLite

## Environment Variables

TeaQL supports the following environment variables for configuration and debugging:

- `TEAQL_LOG_ENDPOINT`: If set, specifies an absolute file path where TeaQL will append all internal execution logs (e.g., SQL queries, audit events). This allows external AI agents or log-collectors to analyze the runtime execution of the framework directly.
- `TEAQL_LOG_FORMAT`: Controls the format of the output log specified by `TEAQL_LOG_ENDPOINT`. Can be set to `json` (or `debug`) for structured JSON logging, or `human` (default) for human-readable output.
- `TEAQL_TEST_PG_URL`: The PostgreSQL connection URL used during tests and examples.

## Next steps

1. Expand `MemoryRepository` toward relation enhancement and richer parity with the SQL-backed path.
2. Add typed checker generation and richer Java-style validation semantics.
3. Keep expanding value coverage beyond the current JSON/date/timestamp/Decimal set, especially `Uuid` and bytes.
4. Decide whether a Rust-native service layer is needed above repository/runtime APIs.
