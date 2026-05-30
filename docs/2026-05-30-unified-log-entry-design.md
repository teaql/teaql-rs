# 2026-05-30: Unified Log Entry Design
**结构化统一日志与追踪模型设计**

## 1. 摘要 (Abstract)
随着 TeaQL 框架在业务应用中的复杂化，纯文本格式的日志缓冲（`TuiLogBuffer` 保存 String）暴露出诸多局限，包括 TUI 渲染时的脆弱字符串解析（Magic Strings 匹配）以及丢失实体层级调用关系（Lineage / Trace Chain）的结构化信息。

本设计提出构建一个 **统一日志实体 (Unified Log Entry)** 模型。核心理念是：**所有的日志追踪与变更数据，在被最终渲染或序列化输出前，必须始终保持原生的强类型和结构化特征。**

---

## 2. 核心架构与模型设计 (Core Domain Model)

日志将通过顶层统一的 `UnifiedLogEntry` 进行包装，内部挂载具有特定业务语境的 Payload，并携带完整的结构化调用链上下文。

### 2.1 顶层包装：统一元数据与追踪上下文 (The Wrapper)
```rust
pub struct UnifiedLogEntry {
    pub timestamp: std::time::SystemTime,
    pub user_identifier: Option<String>,
    
    // 核心：无损保留的因果树分支，不再提前拍平为单一字符串
    pub trace_chain: Vec<TraceNode>, 
    
    // 具体的日志载荷
    pub payload: LogPayload,
}

pub enum LogPayload {
    Sql(SqlLogEntry),
    Audit(AuditLogEntry),
    Info(InfoLogEntry),
}
```

### 2.2 结构化的调用链节点 (Structured Trace Chain)
彻底抛弃文本拼接格式（如 `"Task(23) -> ..."`），将栈上 `ScopedCommentNode` 构建的层级关系无损保存进数组：
```rust
pub struct TraceNode {
    pub entity_type: String,     
    
    // 使用 Option<u64> 完美处理实体尚未持久化分配 ID 前的 "Pending" 状态
    pub entity_id: Option<u64>,  
    pub comment: String,         
}
```

### 2.3 载荷一：SQL 物理层追踪 (SQL Payload)
```rust
pub struct SqlLogEntry {
    pub operation: SqlLogOperation,       
    pub sql: String,                      
    pub params: Vec<teaql_core::Value>,               
    pub debug_sql: String,                
    pub pretty_sql: String,               
    
    // 性能指标
    pub started_at: std::time::SystemTime,
    pub ended_at: std::time::SystemTime,
    pub elapsed: std::time::Duration,                
    
    // 结果元数据
    pub result_count: Option<usize>,      
    pub result_type: Option<String>,      
    pub affected_rows: Option<u64>,       
}
```

### 2.4 载荷二：领域审计追踪 (Audit Payload)
记录实体状态精确的变动历史：
```rust
pub struct AuditLogEntry {
    pub entity_type: String,              
    pub entity_id: u64,                   
    pub action: EntityAction,             
    pub changes: Vec<AuditChange>,        
}
```

**字段级变更 (Audit Change)** 不再依赖格式化，而是通过强类型 `teaql_core::Value` 携带完整类型元数据，精准区分空值（`Null`）和空字符串（`""`），同时省去无谓的早期字符串序列化开销。
```rust
pub struct AuditChange {
    pub field: String,   
    pub old_value: teaql_core::Value, 
    pub new_value: teaql_core::Value, 
}
```

### 2.5 载荷三：业务信息与命令 (Info Payload)
用于纯业务流程节点的观测，以及自定义上下文参数：
```rust
pub struct InfoLogEntry {
    pub message: String,
}
```

---

## 3. 设计优势与价值 (Benefits)

1. **解决表现层耦合与脆弱性**：`ui.rs` 将直接依据强类型的 Enum (`LogPayload`) 和 `Value` 进行解析，无需执行任何复杂的正则匹配或 `find('[')` 等容易导致越界的低效操作，代码更精简且高度可靠。
2. **顶级的可观测性**：基于纯数字的 `entity_id` 配合 `Value` 体系，当输出 JSON 给外部日志收集系统（如 ELK / Datadog）时，支持直接建立高效率的数字索引，为业务故障的精确过滤和定位提供强大支撑。
3. **零信息丢失**：通过 `trace_chain` 将微服务或复杂系统内并发执行过程中的多级路由、衍生动作无损记录，从而实现了完美的事务级根因溯源。
