# 2026-06-02: Data Service Executor Design
**Design Goal: Replace database executor abstraction with data service executor abstraction, making SQL databases just one execution backend for TeaQL Runtime**

## 1. Abstract

The current TeaQL Rust runtime repository uses `QueryExecutor` to simultaneously handle queries, writes, and graph save transaction boundaries. This design works, but the naming and responsibilities are imprecise: it appears to be a query executor, but actually also executes mutations and exposes transaction control.

The goal is to introduce an execution model centered on `DataServiceExecutor`. TeaQL Runtime programs against data service protocols, not database or SQL executor protocols. Databases, remote services, in-memory storage, HTTP data services, and GraphQL data sources can all become data service execution backends.

---

## 2. Design Principles

1. **Data service first, not database first**: The runtime's core abstraction is named `DataServiceExecutor`, not `DbExecutor`.
2. **Separate read and write capabilities**: Queries and mutations are different capabilities, expressed by `QueryExecutor` and `MutationExecutor` respectively.
3. **Transactions are capabilities, not default methods**: Transactions are explicitly expressed by `TransactionExecutor`; executors that cannot transactionalize should not expose graph save capabilities.
4. **SQL is an adapter, not a runtime protocol**: `SqlDialect` and `CompiledQuery` belong to the SQL adapter layer and should not be part of the core data service protocol.
5. **Mutations should preserve semantic-level structure**: The runtime should pass `InsertCommand`, `UpdateCommand`, `DeleteCommand`, `RecoverCommand` and other semantic commands, rather than degrading to SQL prematurely.
6. **Transaction scopes should be typed**: Avoid raw `begin/commit/rollback` scattered across callers; prefer transaction scope or transaction objects to express lifecycles.

---

## 3. Target Architecture

The target layering is as follows:

```text
teaql-core
  Entity / Record / SelectQuery / MutationCommand / GraphNode

teaql-runtime
  Repository / ResolvedRepository / checker / policy / event / graph orchestration

teaql-data-service
  DataServiceExecutor traits
  QueryRequest / MutationRequest / QueryResult / MutationResult / Transaction API

teaql-sql
  SqlDialect
  SqlDataServiceExecutor adapter
  SelectQuery -> CompiledQuery
  MutationRequest -> CompiledQuery

teaql-provider-*
  rusqlite / sqlx / mysql / postgres concrete transports
```

`teaql-runtime` depends on the data service protocol, not directly on the SQL executor. `teaql-sql` is responsible for compiling TeaQL's semantic-level query/mutation into SQL and handing SQL to the provider transport for execution.

---

## 4. Core Traits

### 4.1 DataServiceExecutor

`DataServiceExecutor` is the base identity for all data service executors, carrying only unified error type and capability description.

```rust
pub trait DataServiceExecutor {
    type Error: std::error::Error + Send + Sync + 'static;

    fn capabilities(&self) -> DataServiceCapabilities;
}
```

Capability description is used by the runtime to determine whether the executor supports specific functionality before startup or invocation.

```rust
#[derive(Debug, Clone, Default)]
pub struct DataServiceCapabilities {
    pub query: bool,
    pub mutation: bool,
    pub transaction: bool,
    pub schema: bool,
    pub id_generation: bool,
    pub batch_mutation: bool,
    pub returning: bool,
}
```

### 4.2 QueryExecutor

The query executor receives semantic-level query requests and returns structured query results.

```rust
pub trait QueryExecutor: DataServiceExecutor {
    fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error>;
}
```

```rust
pub struct QueryRequest {
    pub query: SelectQuery,
    pub trace_chain: Vec<TraceNode>,
    pub comment: Option<String>,
}

pub struct QueryResult {
    pub rows: Vec<Record>,
    pub metadata: ExecutionMetadata,
}
```

### 4.3 MutationExecutor

The mutation executor receives semantic-level mutation requests and returns structured mutation results.

```rust
pub trait MutationExecutor: DataServiceExecutor {
    fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error>;
}
```

```rust
pub enum MutationRequest {
    Insert(InsertCommand),
    Update(UpdateCommand),
    Delete(DeleteCommand),
    Recover(RecoverCommand),
    Batch(Vec<MutationRequest>),
}

pub struct MutationResult {
    pub affected_rows: u64,
    pub generated_values: Record,
    pub metadata: ExecutionMetadata,
}
```

`generated_values` is used to carry server-generated id, version, audit id, backend request id, and other extension information. Even if the current SQL provider does not use it, it should be preserved at the protocol layer.

### 4.4 TransactionExecutor

Transactions should not be a default empty implementation of a regular mutation executor, but an independent capability.

```rust
pub trait TransactionExecutor: DataServiceExecutor {
    type Tx<'a>: QueryExecutor<Error = Self::Error>
        + MutationExecutor<Error = Self::Error>
        + Transaction<Error = Self::Error>
    where
        Self: 'a;

    fn begin(&self) -> Result<Self::Tx<'_>, Self::Error>;
}
```

```rust
pub trait Transaction {
    type Error: std::error::Error + Send + Sync + 'static;

    fn commit(self) -> Result<(), Self::Error>;
    fn rollback(self) -> Result<(), Self::Error>;
}
```

The transaction object itself implements `QueryExecutor` and `MutationExecutor`, so graph save can read current rows, insert, update, delete, and commit within the same transaction scope.

---

## 5. SQL Adapter

SQL is no longer the runtime's direct executor protocol, but a data service adapter.

```rust
pub struct SqlDataServiceExecutor<D, T> {
    pub dialect: D,
    pub transport: T,
}
```

Where:

- `D: SqlDialect` is responsible for compiling `SelectQuery` and `MutationRequest` into `CompiledQuery`.
- `T` is the SQL transport, responsible for executing `CompiledQuery`.

The SQL transport can continue to maintain a lower-level interface:

```rust
pub trait SqlQueryTransport {
    type Error: std::error::Error + Send + Sync + 'static;

    fn fetch_all_sql(&self, query: &CompiledQuery) -> Result<Vec<Record>, Self::Error>;
}

pub trait SqlMutationTransport {
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute_sql(&self, query: &CompiledQuery) -> Result<u64, Self::Error>;
}
```

This makes provider crate responsibilities clearer:

- `teaql-provider-rusqlite` provides rusqlite transport.
- `teaql-provider-sqlx-postgres` provides postgres sqlx transport.
- `teaql-provider-sqlx-sqlite` provides sqlite sqlx transport.
- `teaql-provider-sqlx-mysql` provides mysql sqlx transport.
- `teaql-sql` provides `SqlDataServiceExecutor`, bridging TeaQL semantic requests to SQL transport.

---

## 6. Runtime Repository Responsibilities

`ResolvedRepository` no longer directly compiles SQL. It handles domain-level runtime logic:

1. Apply `RepositoryBehavior`.
2. Apply `RequestPolicy`.
3. Execute checkers and fixes.
4. Generate id and initial version.
5. Organize graph save.
6. Send entity events.
7. Record trace chain and comments.
8. Invalidate aggregation cache.
9. Call `QueryExecutor` or `MutationExecutor`.

Example:

```rust
impl<'a, S> ResolvedRepository<'a, S>
where
    S: QueryExecutor + MutationExecutor,
{
    pub fn update(&self, command: &UpdateCommand) -> Result<MutationResult, RepositoryError<S::Error>> {
        let command = self.prepare_update_command(command)?;
        let result = self.data_service.mutate(MutationRequest::Update(command))?;
        self.emit_update_event(...)?;
        self.invalidate_aggregation_cache(...);
        Ok(result)
    }
}
```

Graph save explicitly requires transaction capability:

```rust
impl<'a, S> ResolvedRepository<'a, S>
where
    S: QueryExecutor + MutationExecutor + TransactionExecutor,
{
    pub fn save_graph(&self, node: GraphNode) -> Result<GraphNode, RepositoryError<S::Error>> {
        let tx = self.data_service.begin()?;
        let saved = self.save_graph_in_transaction(&tx, node)?;
        tx.commit()?;
        Ok(saved)
    }
}
```

This is stricter than returning `Unsupported` at runtime. Data services that cannot transactionalize cannot call graph save at compile time.

---

## 7. Graph Save Design

Graph save is the part of the mutation engine that most needs transactions. The target design requires graph save to satisfy:

1. All nodes upsert/delete complete within a single transaction scope.
2. Use intra-transaction queries when determining create/update to avoid read-write inconsistency.
3. `Reference` nodes only verify existence, not execute writes.
4. `Remove` nodes execute delete/recover semantics, not directly concatenate SQL.
5. Relation attach only modifies semantic-level records, then hands them to the mutation executor.
6. Dirty fields continue to be used for building minimal update commands.
7. Trace chain and comments are passed through `ExecutionMetadata` and request context.

`GraphMutationPlan` should become a true semantic execution plan, not just a statistical preview:

```rust
pub struct GraphMutationPlan {
    pub root: GraphNode,
    pub items: Vec<GraphMutationPlanItem>,
    pub batches: Vec<GraphMutationBatch>,
}
```

If batches are retained, they should be executed by `MutationRequest::Batch`; if batch optimization is not yet supported, the API should not imply that batch optimization has occurred.

---

## 8. Schema and ID Generation

Schema and ID generation also belong to data service capabilities and should not be hardcoded as附属 methods of database providers.

```rust
pub trait SchemaExecutor: DataServiceExecutor {
    fn ensure_schema(&self, request: SchemaRequest) -> Result<SchemaResult, Self::Error>;
}

pub trait IdGeneratorExecutor: DataServiceExecutor {
    fn next_id(&self, entity: &str) -> Result<u64, Self::Error>;
}
```

SQL providers can implement `IdGeneratorExecutor` through id space tables. Remote data services can generate ids through remote interfaces. Test or in-memory implementations can use local counters.

---

## 9. Execution Metadata

All queries and mutations should return unified execution metadata.

```rust
pub struct ExecutionMetadata {
    pub backend: String,
    pub operation: DataServiceOperation,
    pub started_at: std::time::SystemTime,
    pub ended_at: std::time::SystemTime,
    pub affected_rows: Option<u64>,
    pub result_count: Option<usize>,
    pub trace_chain: Vec<TraceNode>,
    pub comment: Option<String>,
    pub backend_request_id: Option<String>,
}
```

SQL logs are no longer a special path, but a rendering result of `ExecutionMetadata`. Audit logs, TUI logs, and debug SQL can all be derived from the same structured metadata.

---

## 10. Provider Naming

Provider structs should no longer be called `*MutationExecutor`, because they support both query and mutation.

Recommended naming:

- `RusqliteDataService`
- `SqliteDataService`
- `PostgresDataService`
- `MysqlDataService`

Or if they are just SQL transports:

- `RusqliteTransport`
- `SqliteSqlxTransport`
- `PostgresSqlxTransport`
- `MysqlSqlxTransport`

Final recommended composition:

```rust
let transport = PostgresSqlxTransport::new(pool);
let data_service = SqlDataServiceExecutor::new(PostgresDialect, transport);
```

This way the name accurately expresses two layers: SQL adapter and specific transport.

---

## 11. Synchronous and Asynchronous

The target design should clearly define synchronous and asynchronous as two sets of boundaries, rather than implicitly bridging through `block_on`.

Synchronous trait:

```rust
pub trait QueryExecutor: DataServiceExecutor {
    fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error>;
}
```

Asynchronous trait:

```rust
pub trait AsyncQueryExecutor: DataServiceExecutor {
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error>;
}
```

Synchronous runtime uses synchronous executor. Asynchronous runtime uses asynchronous executor. Bridging wrappers can exist, but should be explicit adapters, not providers secretly using `block_on` internally.

---

## 12. Expected Benefits

1. **Accurate abstraction level**: TeaQL Runtime programs against data services, not bound to database implementations.
2. **Clear read/write responsibilities**: query, mutation, transaction, schema, id generation are all independent capabilities.
3. **Safer graph save**: Transaction capabilities are expressed by the type system, not runtime `Unsupported`.
4. **Easier provider extension**: SQL, HTTP, memory, remote services can share the runtime protocol.
5. **More direct testing**: Test executors can assert semantic-level `MutationRequest`, without parsing SQL.
6. **More unified logging and auditing**: Execution results carry structured metadata; SQL logs are just one manifestation.
7. **Room for batch writes and remote data services**: `MutationRequest::Batch` and `MutationResult::generated_values` can carry richer backend capabilities.

---

## 13. Main Risks

1. **Large scope of changes**: `Repository`, `ContextRepository`, `ResolvedRepository`, providers, and examples will all be affected.
2. **Increased generic complexity**: Especially designs like `TransactionExecutor::Tx<'a>` (GAT), which require careful control of API readability.
3. **SQL compilation location needs rearrangement**: Currently the repository directly compiles SQL; the target design requires this to move down to `SqlDataServiceExecutor`.
4. **Synchronous/asynchronous boundaries must be clear**: If `block_on` continues to be mixed, the new abstraction will be weakened.
5. **High short-term compatibility cost**: Existing `*MutationExecutor` names and APIs may require aliases or breaking changes.

---

## 14. Conclusion

The final design should be centered on `DataServiceExecutor`, not on database executor or SQL executor. `QueryExecutor` and `MutationExecutor` express read/write capabilities, `TransactionExecutor` expresses transaction capability, and `SqlDataServiceExecutor` is just one implementation that adapts TeaQL's semantic protocol to SQL.

This direction can upgrade TeaQL Rust's mutation engine from a "database CRUD execution layer" to a "data service execution layer". It preserves the advantages of existing commands, dialects, graph save, checkers, and events, while leaving clear extension points for non-SQL backends, remote data services, batch mutations, and structured execution logs.
