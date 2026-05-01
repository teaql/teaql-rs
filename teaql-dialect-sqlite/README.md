# teaql-dialect-sqlite

SQLite SQL dialect support for TeaQL Rust.

This crate implements the `teaql-sql` dialect contract for SQLite.

## What It Provides

- SQLite identifier quoting
- SQLite placeholder rendering
- SQLite DDL rendering for TeaQL descriptors
- insert, update, delete, recover, and select SQL compatibility helpers
- schema setup behavior used by `teaql-runtime`

## Example

```rust
use teaql_core::{DataType, EntityDescriptor, Expr, PropertyDescriptor, SelectQuery};
use teaql_dialect_sqlite::SqliteDialect;
use teaql_sql::SqlDialect;

let entity = EntityDescriptor::new("Merchant")
    .table_name("merchant")
    .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"));
let query = SelectQuery::new("Merchant")
    .filter(Expr::eq("name", "TeaQL"));
let compiled = SqliteDialect.compile_select(&entity, &query)?;

assert!(compiled.sql.contains("?"));
# Ok::<(), teaql_sql::SqlCompileError>(())
```

Most applications use this crate through `teaql-runtime` with the `sqlx`
feature enabled.
