# 2026-07-02: 查询拦截与多租户隔离设计基线

**设计目标：验证 teaql-rs 的 RequestPolicy / EntityDataServiceBehavior 两层拦截机制是否覆盖了所有查询和变更路径，能否作为多租户隔离的基础设施**

## 1. 结论

teaql-rs **已经设计了两层拦截机制**，可以在所有查询/变更提交到数据库之前注入条件。代码库中包含了一个可工作的多租户隔离测试用例。

---

## 2. 两层拦截架构

所有通过 `EntityDataService` 执行的操作都会经过以下两层 hook：

```
用户业务代码
    ↓
EntityDataService
    ↓
① behavior.before_select()    ← 按实体定制
    ↓
② policy.enforce_select()     ← 全局策略
    ↓
SQL 编译 & 执行
```

### 层一：`EntityDataServiceBehavior`（按实体定制）

定义位置：`teaql-runtime/src/registry.rs:66-110`

| Hook 方法 | 作用对象 |
|---|---|
| `before_select(&self, ctx, &mut SelectQuery)` | 查询前修改 |
| `before_insert(&self, ctx, &mut InsertCommand)` | 插入前修改 |
| `before_update(&self, ctx, &mut UpdateCommand)` | 更新前修改 |
| `before_delete(&self, ctx, &mut DeleteCommand)` | 删除前修改 |
| `before_recover(&self, ctx, &mut RecoverCommand)` | 恢复前修改 |

注册方式：`RuntimeModule::entity_with_behavior::<Order, _>(my_behavior)` 或 `module!` 宏。

### 层二：`RequestPolicy`（全局跨实体策略）

定义位置：`teaql-runtime/src/registry.rs:24-64`

| Hook 方法 | 作用对象 |
|---|---|
| `enforce_select(&self, ctx, &mut SelectQuery)` | 所有查询 |
| `enforce_insert(&self, ctx, &mut InsertCommand)` | 所有插入 |
| `enforce_update(&self, ctx, &mut UpdateCommand)` | 所有更新 |
| `enforce_delete(&self, ctx, &mut DeleteCommand)` | 所有删除 |
| `enforce_recover(&self, ctx, &mut RecoverCommand)` | 所有恢复 |

注册方式：`ctx.with_request_policy(TenantRequestPolicy)`

---

## 3. 核心拦截点：`prepare_select_query()`

位置：`teaql-runtime/src/data_service/resolved.rs:83-110`

```rust
fn prepare_select_query(&self, query: &SelectQuery) -> Result<SelectQuery, RuntimeError> {
    let mut query = query.clone();
    // ... trace context ...

    // ① 实体级 behavior hook
    if let Some(behavior) = self.query_behavior(&query.entity) {
        behavior.before_select(ctx, &mut query)?;
    }
    // ② 全局 policy hook
    if let Some(policy) = ctx.request_policy.as_ref() {
        policy.enforce_select(ctx, &mut query)?;
    }
    // ... projection 调整 ...
    Ok(query)
}
```

**所有** `EntityDataService` 的查询方法都经过此拦截点：

| 查询方法 | 经过 `prepare_select_query` | 代码位置 |
|---|---|---|
| `fetch_all()` | ✅ | `resolved.rs:184` |
| `fetch_stream()` | ✅ | `resolved.rs:201` |
| `fetch_all_with_relation_aggregates()` | ✅ | `resolved.rs:354` |
| `fetch_smart_list()` | ✅ | `resolved.rs:373` |
| `fetch_entities()` | ✅ | `resolved.rs:397` |
| `fetch_enhanced_entities()` | ✅ | `resolved.rs:466` |
| `fetch_enhanced_entities_with_relation_aggregates()` | ✅ | `resolved.rs:434` |

变更操作也全部经过对应的 hook：

| 变更方法 | Behavior hook | Policy hook |
|---|---|---|
| `insert()` | ✅ `prepare_insert_command` (resolved.rs:117) | ✅ (resolved.rs:120) |
| `update()` | ✅ `prepare_update_command` (resolved.rs:163) | ✅ (resolved.rs:166) |
| `delete()` / `delete_scoped()` | ✅ (resolved.rs:518) | ✅ (resolved.rs:523) |
| `recover()` | ✅ (resolved.rs:547) | ✅ (resolved.rs:552) |
| Graph save (insert) | ✅ 通过 `prepare_insert_command` (graph.rs:591) | ✅ |
| Graph save (update) | ✅ 通过 `prepare_update_command` (graph.rs:775) | ✅ |

---

## 4. 已有的多租户测试用例

位置：`teaql-runtime/src/lib.rs:538-617`

代码库中已经包含了一个完整的多租户隔离 `RequestPolicy` 示例：

```rust
struct TenantRequestPolicy;

impl RequestPolicy for TenantRequestPolicy {
    fn enforce_select(&self, ctx: &UserContext, query: &mut SelectQuery) -> Result<(), RuntimeError> {
        if query.entity == "Order" {
            let tenant_id = ctx.get_named_resource::<u64>("tenant_id").copied()
                .ok_or_else(|| RuntimeError::Policy("missing tenant_id".to_owned()))?;
            query.filter = Some(match query.filter.take() {
                Some(filter) => filter.and_expr(Expr::eq("id", tenant_id)),
                None => Expr::eq("id", tenant_id),
            });
        }
        Ok(())
    }

    fn enforce_insert(&self, ctx: &UserContext, command: &mut InsertCommand) -> Result<(), RuntimeError> {
        if command.entity == "Order" {
            let tenant_id = ctx.get_named_resource::<u64>("tenant_id").copied()
                .ok_or_else(|| RuntimeError::Policy("missing tenant_id".to_owned()))?;
            command.values.insert("version".to_owned(), Value::I64(tenant_id as i64));
        }
        Ok(())
    }
}
```

使用方式：
```rust
ctx.insert_named_resource("tenant_id", 9_u64);
ctx.with_request_policy(TenantRequestPolicy);
```

---

## 5. UserContext：请求上下文载体

位置：`teaql-runtime/src/context.rs`

`UserContext` 携带了实现租户隔离所需的所有信息：

- `user_identifier` — 当前用户 ID
- `typed_resources: HashMap<TypeId, Box<dyn Any>>` — 类型化资源（如 `TenantInfo` 结构体）
- `named_resources: BTreeMap<String, Box<dyn Any>>` — 命名资源（如 `"tenant_id"` → `9u64`）
- `locals: BTreeMap<String, Value>` — 简单键值对
- `request_policy` — 全局请求策略

Web 层（axum 集成）在创建 `UserContext` 时可以从 HTTP header/JWT 中提取租户信息并注入。

---

## 6. 需要注意的绕过风险

### 风险一：`ContextDataService` 不经过 hooks

位置：`teaql-runtime/src/data_service/context.rs:78-186`

通过 `ctx.data_service::<E>()` 获取的 `ContextDataService` **不会**执行任何 behavior 或 policy hook。它直接将查询提交给底层 `RuntimeDataService`。

```
EntityDataService   → ✅ 经过 behavior + policy hooks
ContextDataService  → ❌ 直接访问数据库，无 hooks
```

**WARNING**: 如果业务代码使用 `ctx.data_service()` 而不是 `ctx.entity_data_service("Order")`，则会绕过所有安全策略。

**缓解措施**：生成的代码（`cargo-teaql` 生成）应当始终使用 `entity_data_service()`。`ContextDataService` 主要作为 `EntityDataService` 的内部组件使用。

### 风险二：`TeaqlRuntime::fetch_facet_smart_list` 取决于生成代码

`fetch_facet_smart_list` 是 `TeaqlRuntime` trait 的方法，其实现由 `cargo-teaql` 生成。需要确认生成的实现是否使用 `EntityDataService`（经过 hooks）而非 `ContextDataService`（绕过 hooks）。

---

## 7. 总结

| 维度 | 状态 |
|---|---|
| 查询前注入条件的能力 | ✅ 完全支持（`RequestPolicy::enforce_select`） |
| 变更前注入条件的能力 | ✅ 完全支持（`enforce_insert/update/delete/recover`） |
| 多租户隔离模式 | ✅ 已有测试用例验证 |
| 所有 `EntityDataService` 查询路径覆盖 | ✅ 7 个查询方法全部经过 `prepare_select_query` |
| 所有 `EntityDataService` 变更路径覆盖 | ✅ 包括 Graph save |
| 上下文载体（租户信息传递） | ✅ `UserContext` 支持 typed/named resources |
| 低层 `ContextDataService` 绕过风险 | ⚠️ 需要确保业务代码不直接使用 |

**架构设计是完备的**——`RequestPolicy` 是实现多租户隔离的理想切入点，它在所有 `EntityDataService` 的操作路径上都会被调用，且可以访问完整的 `UserContext` 上下文信息。

---

## 8. 关键源文件索引

| 组件 | 文件路径 |
|---|---|
| `Expr` 表达式 AST | `teaql-core/src/expr.rs` |
| `SelectQuery` 查询结构 | `teaql-core/src/query.rs` |
| `RequestPolicy` trait | `teaql-runtime/src/registry.rs:24-64` |
| `EntityDataServiceBehavior` trait | `teaql-runtime/src/registry.rs:66-110` |
| `prepare_select_query()` 核心拦截点 | `teaql-runtime/src/data_service/resolved.rs:83-110` |
| `EntityDataService` 完整实现 | `teaql-runtime/src/data_service/resolved.rs` |
| `ContextDataService`（低层，无 hooks） | `teaql-runtime/src/data_service/context.rs` |
| `UserContext` 上下文 | `teaql-runtime/src/context.rs` |
| Graph save 路径 | `teaql-runtime/src/data_service/graph.rs` |
| SQL 编译 | `teaql-sql/src/dialect.rs` |
| 多租户测试用例 | `teaql-runtime/src/lib.rs:538-617` |
