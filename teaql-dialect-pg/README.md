# teaql-dialect-pg

PostgreSQL SQL dialect support for TeaQL Rust.

This crate implements the `teaql-sql` dialect contract for PostgreSQL.

## What It Provides

- PostgreSQL identifier quoting
- PostgreSQL placeholder rendering
- PostgreSQL DDL rendering for TeaQL descriptors
- PostgreSQL-specific SQL fragments such as large `IN` array binds
- schema setup hooks used by `teaql-runtime`

## Example

```rust
use teaql_core::{DataType, EntityDescriptor, Expr, PropertyDescriptor, SelectQuery};
use teaql_dialect_pg::PostgresDialect;
use teaql_sql::SqlDialect;

let entity = EntityDescriptor::new("Merchant")
    .table_name("merchant")
    .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"));
let query = SelectQuery::new("Merchant")
    .filter(Expr::eq("name", "TeaQL"));
let compiled = PostgresDialect.compile_select(&entity, &query)?;

assert!(compiled.sql.contains("$1"));
# Ok::<(), teaql_sql::SqlCompileError>(())
```

Most applications use this crate through `teaql-runtime` with the `sqlx`
feature enabled.
