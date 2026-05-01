# teaql-core

Core model and query primitives for TeaQL Rust.

`teaql-core` contains the stack-neutral pieces used by the rest of the TeaQL
Rust crates:

- entity and relation metadata
- typed values and records
- select, insert, update, delete, and recover command models
- expression builders and filters
- ordering, grouping, projection, aggregation, and relation aggregate models
- `SmartList<T>` collection metadata
- `TeaqlEntity` and `Entity` traits for typed row mapping
- `SafeExpression` helpers for null-safe value access

## Example

```rust
use teaql_core::{Expr, SelectQuery};

let query = SelectQuery::new("Merchant")
    .select("id")
    .select("name")
    .filter(Expr::eq("name", "TeaQL"))
    .page(1, 20);
```

Use this crate when you need TeaQL metadata, query AST, entity traits, or
in-memory value conversion without choosing a SQL dialect or runtime executor.

## Workspace

This crate is part of the `teaql-rs` workspace:

- `teaql-sql` compiles `teaql-core` query models into SQL.
- `teaql-runtime` executes repository operations and graph persistence.
- `teaql-macros` derives `TeaqlEntity` implementations.
