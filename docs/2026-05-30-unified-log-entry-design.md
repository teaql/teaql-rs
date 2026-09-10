# 2026-05-30: Unified Log Entry Design
**Structured Unified Logging and Trace Model Design**

## 1. Abstract

As the TeaQL framework grows in complexity within business applications, plain-text log buffers (e.g., `TuiLogBuffer` storing `String`) expose several limitations, including fragile string parsing during TUI rendering (magic string matching) and loss of structured information about entity hierarchy relationships (Lineage / Trace Chain).

This design proposes building a **Unified Log Entry** model. The core principle is: **all log trace and change data must maintain native strong typing and structured characteristics until final rendering or serialization.**

---

## 2. Core Domain Model

Logs are wrapped by a top-level `UnifiedLogEntry`, which carries business-context-specific payloads and complete structured call chain context.

### 2.1 Top-Level Wrapper: Unified Metadata and Trace Context

```rust
pub struct UnifiedLogEntry {
    pub timestamp: std::time::SystemTime,
    pub user_identifier: Option<String>,
    
    // Core: losslessly preserved causal tree branches, no longer flattened to a single string
    pub trace_chain: Vec<TraceNode>, 
    
    // Specific log payload
    pub payload: LogPayload,
}

pub enum LogPayload {
    Sql(SqlLogEntry),
    Audit(AuditLogEntry),
    Info(InfoLogEntry),
}
```

### 2.2 Structured Trace Chain Nodes

Completely abandon text concatenation format (e.g., `"Task(23) -> ..."`), and losslessly preserve the hierarchical relationships built by `ScopedCommentNode` on the stack into an array:

```rust
pub struct TraceNode {
    pub entity_type: String,     
    
    // Use Option<u64> to perfectly handle the "Pending" state before entity ID is assigned
    pub entity_id: Option<u64>,  
    pub comment: String,         
}
```

### 2.3 Payload 1: SQL Physical Layer Trace

```rust
pub struct SqlLogEntry {
    pub operation: SqlLogOperation,       
    pub sql: String,                      
    pub params: Vec<teaql_core::Value>,               
    pub debug_sql: String,                
    pub pretty_sql: String,               
    
    // Performance metrics
    pub started_at: std::time::SystemTime,
    pub ended_at: std::time::SystemTime,
    pub elapsed: std::time::Duration,                
    
    // Result metadata
    pub result_count: Option<usize>,      
    pub result_type: Option<String>,      
    pub affected_rows: Option<u64>,       
}
```

### 2.4 Payload 2: Domain Audit Trace

Records precise change history of entity state:

```rust
pub struct AuditLogEntry {
    pub entity_type: String,              
    pub entity_id: u64,                   
    pub action: EntityAction,             
    pub changes: Vec<AuditChange>,        
}
```

**Field-level changes (AuditChange)** no longer depend on formatting, but instead carry complete type metadata through strongly-typed `teaql_core::Value`, precisely distinguishing null values (`Null`) from empty strings (`""`), while eliminating unnecessary early string serialization overhead.

```rust
pub struct AuditChange {
    pub field: String,   
    pub old_value: teaql_core::Value, 
    pub new_value: teaql_core::Value, 
}
```

### 2.5 Payload 3: Business Information and Commands

Used for observing pure business process nodes and custom context parameters:

```rust
pub struct InfoLogEntry {
    pub message: String,
}
```

---

## 3. Benefits

1. **Decouples presentation layer and eliminates fragility**: `ui.rs` directly parses based on strongly-typed enums (`LogPayload`) and `Value`, without any complex regex matching or `find('[')` operations that can cause out-of-bounds errors. The code is more concise and highly reliable.

2. **First-class observability**: Based on numeric `entity_id` combined with the `Value` system, when outputting JSON to external log collection systems (e.g., ELK / Datadog), it supports direct establishment of efficient numeric indexes, providing powerful support for precise filtering and locating of business failures.

3. **Zero information loss**: Through `trace_chain`, multi-level routing and derived actions during concurrent execution in microservices or complex systems are losslessly recorded, achieving perfect transaction-level root cause traceability.
