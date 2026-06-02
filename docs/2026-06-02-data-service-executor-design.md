# 2026-06-02: Data Service Executor Design
**设计目标：以数据服务执行器抽象替代数据库执行器抽象，让 SQL 数据库只是 TeaQL Runtime 的一种执行后端**

## 1. 摘要

当前 TeaQL Rust 的 runtime repository 使用 `QueryExecutor` 同时承担查询、写入和 graph save 事务边界。这个设计已经能工作，但命名和职责并不精确：它表面上是 query executor，实际却同时执行 mutation，并且暴露事务控制。

目标设计是引入以 `DataServiceExecutor` 为中心的执行模型。TeaQL Runtime 面向数据服务协议编程，而不是面向数据库或 SQL 执行器编程。数据库、远程服务、内存存储、HTTP 数据服务、GraphQL 数据源都可以成为数据服务执行后端。

---

## 2. 设计原则

1. **数据服务优先，而不是数据库优先**：runtime 的核心抽象命名为 `DataServiceExecutor`，不使用 `DbExecutor`。
2. **读写能力分离**：查询和变更是不同能力，分别由 `QueryExecutor` 和 `MutationExecutor` 表达。
3. **事务是能力，不是默认方法**：事务由 `TransactionExecutor` 显式表达，不能事务化的 executor 不应该暴露 graph save 能力。
4. **SQL 是 adapter，不是 runtime 协议**：`SqlDialect` 和 `CompiledQuery` 属于 SQL 适配层，不应该成为最终的数据服务协议核心。
5. **mutation 应保留语义级结构**：runtime 应尽量传递 `InsertCommand`、`UpdateCommand`、`DeleteCommand`、`RecoverCommand` 等语义命令，而不是过早降级为 SQL。
6. **事务作用域应类型化**：避免裸 `begin/commit/rollback` 分散在调用方，优先使用 transaction scope 或 transaction object 表达生命周期。

---

## 3. 目标架构

目标分层如下：

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

`teaql-runtime` 依赖数据服务协议，不直接依赖 SQL executor。`teaql-sql` 负责把 TeaQL 的语义级 query/mutation 编译为 SQL，并把 SQL 交给 provider transport 执行。

---

## 4. 核心 Trait

### 4.1 DataServiceExecutor

`DataServiceExecutor` 是所有数据服务执行器的基础身份，只承载统一错误类型和能力描述。

```rust
pub trait DataServiceExecutor {
    type Error: std::error::Error + Send + Sync + 'static;

    fn capabilities(&self) -> DataServiceCapabilities;
}
```

能力描述用于 runtime 在启动或调用前判断 executor 是否支持特定功能。

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

查询 executor 接收语义级查询请求，返回结构化查询结果。

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

变更 executor 接收语义级 mutation 请求，返回结构化变更结果。

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

`generated_values` 用于承载服务端生成的 id、version、审计 id、后端 request id 等扩展信息。即使当前 SQL provider 暂时不使用，也应在协议层保留。

### 4.4 TransactionExecutor

事务不应作为普通 mutation executor 的默认空实现，而应是独立能力。

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

事务对象本身实现 `QueryExecutor` 和 `MutationExecutor`，所以 graph save 可以在同一个事务作用域内读取当前行、插入、更新、删除和提交。

---

## 5. SQL Adapter

SQL 不再是 runtime 的直接 executor 协议，而是一个 data service adapter。

```rust
pub struct SqlDataServiceExecutor<D, T> {
    pub dialect: D,
    pub transport: T,
}
```

其中：

- `D: SqlDialect` 负责把 `SelectQuery` 和 `MutationRequest` 编译成 `CompiledQuery`。
- `T` 是 SQL transport，负责执行 `CompiledQuery`。

SQL transport 可以继续保持较低层的接口：

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

这样 provider crate 的职责更清楚：

- `teaql-provider-rusqlite` 提供 rusqlite transport。
- `teaql-provider-sqlx-postgres` 提供 postgres sqlx transport。
- `teaql-provider-sqlx-sqlite` 提供 sqlite sqlx transport。
- `teaql-provider-sqlx-mysql` 提供 mysql sqlx transport。
- `teaql-sql` 提供 `SqlDataServiceExecutor`，把 TeaQL 语义请求桥接到 SQL transport。

---

## 6. Runtime Repository 职责

`ResolvedRepository` 不再直接 compile SQL。它负责领域级运行时逻辑：

1. 应用 `RepositoryBehavior`。
2. 应用 `RequestPolicy`。
3. 执行 checker 和 fix。
4. 生成 id 和初始 version。
5. 组织 graph save。
6. 发送 entity events。
7. 记录 trace chain 和 comment。
8. 失效 aggregation cache。
9. 调用 `QueryExecutor` 或 `MutationExecutor`。

示意：

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

graph save 明确要求事务能力：

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

这比运行时返回 `Unsupported` 更严格。不能事务化的数据服务在编译期就无法调用 graph save。

---

## 7. Graph Save 设计

graph save 是 mutation engine 中最需要事务的部分。目标设计中 graph save 应满足：

1. 所有节点 upsert/delete 在一个 transaction scope 内完成。
2. 判断 create/update 时使用 transaction 内查询，避免读写不一致。
3. `Reference` 节点只校验存在性，不执行写入。
4. `Remove` 节点执行 delete/recover 语义，而不是直接拼 SQL。
5. relation attach 只修改语义级 record，再交给 mutation executor。
6. dirty fields 继续用于构建最小 update command。
7. trace chain 和 comment 通过 `ExecutionMetadata` 和 request context 传递。

`GraphMutationPlan` 应成为真正的语义执行计划，而不只是统计预览：

```rust
pub struct GraphMutationPlan {
    pub root: GraphNode,
    pub items: Vec<GraphMutationPlanItem>,
    pub batches: Vec<GraphMutationBatch>,
}
```

如果保留 batch，则 batch 应由 `MutationRequest::Batch` 执行；如果暂不支持批量执行，就不应让 API 暗示已经批量优化。

---

## 8. Schema 和 ID Generation

schema 和 id generation 也属于数据服务能力，不应硬编码为数据库 provider 的附属方法。

```rust
pub trait SchemaExecutor: DataServiceExecutor {
    fn ensure_schema(&self, request: SchemaRequest) -> Result<SchemaResult, Self::Error>;
}

pub trait IdGeneratorExecutor: DataServiceExecutor {
    fn next_id(&self, entity: &str) -> Result<u64, Self::Error>;
}
```

SQL provider 可以通过 id space table 实现 `IdGeneratorExecutor`。远程 data service 可以通过远程接口生成 id。测试或内存实现可以用本地计数器。

---

## 9. 执行元数据

所有 query 和 mutation 都应返回统一的执行元数据。

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

SQL 日志不再是特殊路径，而是 `ExecutionMetadata` 的一种渲染结果。审计日志、TUI 日志、调试 SQL 都可以从同一份结构化元数据派生。

---

## 10. Provider 命名

provider 结构体不应继续叫 `*MutationExecutor`，因为它们同时支持 query 和 mutation。

推荐命名：

- `RusqliteDataService`
- `SqliteDataService`
- `PostgresDataService`
- `MysqlDataService`

或者如果它们只是 SQL transport：

- `RusqliteTransport`
- `SqliteSqlxTransport`
- `PostgresSqlxTransport`
- `MysqlSqlxTransport`

最终推荐组合：

```rust
let transport = PostgresSqlxTransport::new(pool);
let data_service = SqlDataServiceExecutor::new(PostgresDialect, transport);
```

这样名称准确表达了两个层次：SQL adapter 和具体 transport。

---

## 11. 同步与异步

目标设计应明确同步和异步是两套边界，而不是通过 `block_on` 隐式桥接。

同步 trait：

```rust
pub trait QueryExecutor: DataServiceExecutor {
    fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error>;
}
```

异步 trait：

```rust
pub trait AsyncQueryExecutor: DataServiceExecutor {
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error>;
}
```

同步 runtime 使用同步 executor。异步 runtime 使用异步 executor。桥接 wrapper 可以存在，但应该是显式 adapter，而不是 provider 内部偷偷 `block_on`。

---

## 12. 预期收益

1. **抽象层级准确**：TeaQL Runtime 面向数据服务，不被数据库实现绑死。
2. **读写职责清晰**：query、mutation、transaction、schema、id generation 都是独立能力。
3. **graph save 更安全**：事务能力由类型系统表达，而不是运行时 `Unsupported`。
4. **provider 更容易扩展**：SQL、HTTP、memory、remote service 可以共用 runtime 协议。
5. **测试更直接**：测试 executor 可以断言语义级 `MutationRequest`，不必解析 SQL。
6. **日志和审计更统一**：执行结果携带结构化 metadata，SQL 日志只是其中一种表现形式。
7. **为批量写和远程数据服务预留空间**：`MutationRequest::Batch` 和 `MutationResult::generated_values` 可承载更丰富的后端能力。

---

## 13. 主要风险

1. **改动面大**：`Repository`、`ContextRepository`、`ResolvedRepository`、provider、examples 都会受影响。
2. **泛型复杂度会上升**：尤其是 `TransactionExecutor::Tx<'a>` 这种 GAT 设计，需要谨慎控制 API 可读性。
3. **SQL 编译位置需要重排**：当前 repository 直接 compile SQL，目标设计要下沉到 `SqlDataServiceExecutor`。
4. **同步/异步边界必须明确**：如果继续混用 `block_on`，新的抽象会被削弱。
5. **短期兼容成本高**：现有 `*MutationExecutor` 名称和 API 可能需要 alias 或 breaking change。

---

## 14. 结论

最终设计应以 `DataServiceExecutor` 为中心，而不是以数据库 executor 或 SQL executor 为中心。`QueryExecutor` 和 `MutationExecutor` 表达读写能力，`TransactionExecutor` 表达事务能力，`SqlDataServiceExecutor` 只是把 TeaQL 语义协议适配到 SQL 的一个实现。

这个方向能让 TeaQL Rust 的 mutation engine 从“数据库 CRUD 执行层”升级为“数据服务执行层”。它保留现有 command、dialect、graph save、checker、event 的优点，同时为非 SQL 后端、远程数据服务、批量 mutation、结构化执行日志留下清晰扩展点。
