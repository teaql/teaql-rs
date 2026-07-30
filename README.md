# teaql-rs

[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/13608/badge)](https://www.bestpractices.dev/projects/13608)

**TeaQL is an AI-native runtime designed for Coding Agents and modern application development.** 

While traditional frameworks assume a human is writing every line of code, TeaQL provides a strict, typed, and auditable Capability Sandbox tailored specifically for autonomous AI Agents (and humans) to execute code securely.

### The Five Safeguards of AI Coding

To ensure absolute safety and governance when AI Agents interact with production systems, the TeaQL runtime enforces the following five safeguards:

1. **Mandatory Identity (UserContext):** Every operation must pass through a runtime `UserContext`. The system explicitly records whether the action was performed by a human or an AI Agent.
2. **Intent Auditing (Typestate/Builder):** Agents cannot simply call `.execute()`. They are forced by the compiler to declare their intent using `.purpose()` (for reads) or `.audit_as()` (for writes) before the execution terminal is unlocked.
3. **Capability Sandbox (SPI/Features):** Dangerous operations (HTTP, File IO, Message Queues) are physically isolated. Unless explicitly granted in the project dependencies (via Cargo features), the Agent is structurally blocked from accessing them.
4. **Graph Mutability Control:** Agents do not manually assemble SQL `UPDATE`s or relationship loops. They operate on typed Entity Graphs (`save_graph`), reducing hallucination-induced data corruption.
5. **Universal Error Translation:** The runtime intercepts infrastructure errors and translates them into semantic business codes, preventing the Agent from getting stuck in stack trace loops.

---

TeaQL is a DDD-oriented data runtime for applications that want generated,
typed domain APIs instead of hand-written repository boilerplate.

The Rust workspace provides the runtime pieces: entity metadata, a query AST,
SQL compilation, relation enhancement, graph writes, checkers, mutation events,
and PostgreSQL, MySQL, and SQLite executors. The sibling `teaql-code-gen`
project can turn a compact domain model into a typed Rust service crate with
entity structs, `Q::merchants()`-style query builders, behavior/checker hooks,
and audited graph-save entrypoints.

TeaQL is not trying to be a general replacement for Diesel, SeaORM, or direct
`sqlx` use:

- use TeaQL when the domain model is central, relation graphs matter, and you
  want generated high-level APIs that resemble the Java TeaQL style;
- use Diesel or SeaORM when you want a conventional Rust ORM with a broad
  ecosystem;
- use `sqlx` directly when explicit SQL is the right abstraction and generated
  domain APIs would get in the way.

The Rust rewrite keeps the scope deliberately narrow:

- PostgreSQL, MySQL, and SQLite database providers
- Rust-native metadata and query AST
- SQL compiler and runtime separated from optional web and cache integrations
- compatibility with every Java implementation detail is not a goal, but the
  high-level TeaQL programming model is being carried over where it is useful

Progress tracking lives in [PROGRESS.md](./PROGRESS.md).

Current published release: `4.2.1`.

## Cloud Integration

TeaQL provides cloud-native crates for embedding Rust services into Java (Spring Cloud) microservice architectures:

```rust
use teaql_cloud_starter::CloudApp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    CloudApp::new()
        .nacos("127.0.0.1:8848")
        .namespace("production")
        .service_name("order-service-rust")
        .port(8080)
        .routes(my_business_routes())
        .start()
        .await?;
    Ok(())
}
```

| Crate | Purpose |
|-------|---------|
| `teaql-cloud-core` | Backend-agnostic traits (ServiceRegistry, ServiceDiscovery, ConfigSource, HealthIndicator, MetricsCollector) |
| `teaql-cloud-actuator` | Spring Boot Actuator-compatible endpoints (health, info, metrics) |
| `teaql-cloud-nacos` | Nacos v2 gRPC implementation |
| `teaql-cloud-starter` | One-line bootstrap — connects Nacos, registers service, starts HTTP, graceful shutdown |

See [design document](docs/2026-07-20-cloud-integration-design.md) for details.

## Build & Install

To build TeaQL from source:

```bash
git clone https://github.com/teaql/teaql-rs.git
cd teaql-rs
cargo build --workspace --all-targets --exclude teaql-provider-linux
```

On Linux, include the `/proc` data provider with `cargo build --all-targets`.

To use TeaQL in your project, add the relevant crates to your `Cargo.toml`:

```toml
[dependencies]
teaql-core = "4.2.1"
# Add other providers as needed
```

## Try It

The quickest demo uses SQLite in memory and needs no database server:

```bash
cargo run -p teaql-examples --bin sqlite_relations_graph
```

It bootstraps the schema, submits an `Order` graph containing `OrderLine` and
`Product` objects, fetches the order again, and prints the typed result.

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
            .with_name_containing("TeaQL"),
    )
    .comment("Load platforms with filtered merchant names")
    .purpose("Displaying platform-merchant overview for dashboard")
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
- `teaql-data-service`: database-neutral query and mutation service contracts
- `teaql-web-integration-axum`: Axum integration for TeaQL web responses and request contexts
- `teaql-cache-integration-redis`: Redis-backed runtime cache integration
- `teaql-provider-meilisearch`: Meilisearch query provider
- `teaql-provider-linux`: Linux `/proc` data provider
- `teaql-macros`: `TeaqlEntity` derive macro plus attribute parsing and record/entity mapping generation

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
- unified internal graph write path through `GraphNode`, covering create and upsert requests
- audited graph upserts, including parent update, child merge, child insert, and missing-child soft delete
- graph writes build a `GraphMutationPlan` classified by entity type and operation before execution
- graph planning is available through `UserContext::plan_for_save_graph()` for debugging
- graph planning assigns missing create ids before batching; creates/deletes merge by entity type, updates merge only when the updated field set is identical
- audited graph saves require a transactional executor and roll back when any planned operation fails
- typed entity graph extraction through `graph_node_from_entity()` and audited persistence through `.audit_as(...).save(...)`
- graph write state hints: `Upsert`, `Reference`, and `Remove`
- stricter graph state semantics: reference nodes validate existence/version/deleted state, remove nodes validate existence, and many-relation merge rejects duplicate child ids
- relation metadata for graph writes: `attach/detached` and `delete_missing/keep_missing`
- native transaction boundary helpers for graph-write wrapping and rollback testing live in the per-database provider crates
- declarative runtime assembly through `RuntimeModule` and `module!`
- built-in `SnowflakeIdGenerator` and `UserContext`-driven id generation; native `teaql_id_space` generators live in the per-database provider crates
- `BaseEntityData` / `BaseEntity` for shared `id + version + dynamic` entity state
- dynamic-property capture through `#[teaql(dynamic)]`, with JSON flattening for aggregate-style outputs
- `InMemoryQueryEngine` for no-database query tests and lightweight in-memory evaluation
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

## Typed entities and `SmartList<T>`

`TeaqlEntity` derive now generates both metadata and typed `Entity` mapping.
Entity data-service APIs can return either raw `Record` rows or typed
`SmartList<T>` collections.

```rust
use teaql_core::{Expr, SelectQuery, SmartList, TeaqlEntity};
use teaql_macros::TeaqlEntity;
use teaql_runtime::PurposedSelectQuery;

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

let query = PurposedSelectQuery::new(
    SelectQuery::new("CatalogProduct")
        .filter(Expr::eq("name", "desk"))
        .comment("Load catalog products named desk"),
    "Display matching catalog products",
);
let products: SmartList<CatalogProductRow> =
    data_service.fetch_entities::<CatalogProductRow>(&query).await?;
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
use teaql_macros::TeaqlEntity;
use teaql_runtime::PurposedSelectQuery;

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
    #[teaql(column = "product_id")]
    product_id: u64,
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

let query = PurposedSelectQuery::new(
    SelectQuery::new("Order").comment("Load orders and configured relations"),
    "Display orders and configured relations",
);
let orders: SmartList<OrderRow> =
    data_service.fetch_enhanced_entities::<OrderRow>(&query).await?;
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
use rusqlite::Connection;
use teaql_provider_sqlite::{SqliteMutationExecutor, SqliteProviderExt};

let executor = SqliteMutationExecutor::from_connection(Connection::open("app.db")?);
ctx.use_sqlite_provider(executor);
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

Current `ensure_schema` scope:

- create missing tables
- add missing columns to existing tables
- do not attempt destructive migrations such as drop column, type rewrite, or primary-key rebuild

## Examples

Runnable examples live in the `teaql-examples` workspace package:

```bash
cargo run -p teaql-examples --bin sqlite_schema_crud
cargo run -p teaql-examples --bin sqlite_relations_graph
cargo run -p teaql-examples --bin school_example
cargo run -p teaql-examples --bin test_default_log
```

Current examples cover:

- SQLite schema bootstrap, audited create/update/delete, and typed entity fetch
- SQLite entity graph writes and typed relation enhancement
- audited graph saves through `entity.audit_as("why").save(&ctx)`
- default SQL trace-log output

## Environment Variables

TeaQL supports the following environment variables for configuration and debugging:

- `TEAQL_LOG_ENDPOINT`: Sends internal execution logs to `stdout` or appends them to the specified file path.
- `TEAQL_LOG_FORMAT`: Controls the format of the output log specified by `TEAQL_LOG_ENDPOINT`. Can be set to `json` (or `debug`) for structured JSON logging, or `human` (default) for human-readable output.
- `TEAQL_DOMAIN`: Sets the default log filename to `<domain>.log` when `TEAQL_LOG_ENDPOINT` is not set.

## Reporting Bugs

If you find a bug, please create an issue on [GitHub Issues](https://github.com/teaql/teaql-rs/issues).
Please include as much detail as possible, such as TeaQL version, Rust version, database type, and steps to reproduce.
For security vulnerabilities, please see our [Security Policy](SECURITY.md).

## Next steps

1. Expand `MemoryRepository` toward relation enhancement and richer parity with the SQL-backed path.
2. Add typed checker generation and richer Java-style validation semantics.
3. Keep expanding value coverage beyond the current JSON/date/timestamp/Decimal set, especially `Uuid` and bytes.
4. Decide whether a Rust-native service layer is needed above repository/runtime APIs.
