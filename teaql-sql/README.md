# teaql-sql

SQL compiler support for TeaQL Rust.

`teaql-sql` turns TeaQL query and mutation models from `teaql-core` into
parameterized SQL plus bind values. Dialect-specific syntax is supplied by a
`SqlDialect` implementation, such as PostgreSQL or SQLite.

## What It Provides

- `SqlDialect` trait
- compiled query and mutation structures
- select SQL compilation
- insert, update, delete, and recover SQL compilation
- DDL helpers for schema creation and missing-column migration
- debug SQL rendering with inlined values for diagnostics

## Example

```rust
use teaql_core::{Expr, SelectQuery, EntityDescriptor, PropertyDescriptor, DataType};
use teaql_sql::{CompiledQuery, SqlCompileError, SqlDialect};

fn compile_for<D: SqlDialect>(dialect: &D) -> Result<CompiledQuery, SqlCompileError> {
    let entity = EntityDescriptor::new("Merchant")
        .table_name("merchant")
        .property(PropertyDescriptor::new("name", DataType::Text).column_name("name"));

    let query = SelectQuery::new("Merchant")
        .filter(Expr::eq("name", "TeaQL"))
        .page(1, 20);

    dialect.compile_select(&entity, &query)
}
```

Most applications should use `teaql-runtime`; use `teaql-sql` directly when
building a custom executor, dialect, or SQL inspection tool.
