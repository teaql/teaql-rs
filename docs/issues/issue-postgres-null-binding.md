# Bug: Generic NULL binding in Postgres provider causes type mismatch serialization errors

## Description
When serializing `Value::Null` (representing optional/nullable fields that are not populated), `teaql-provider-postgres` maps it to `Option::<i32>::None` (which targets `INT4` OID in PostgreSQL):

```rust
Value::Null => {
    args.add(Option::<i32>::None);
}
```

If the target column has a non-integer type (for example, `timestamptz` for columns like `paid_at`), PostgreSQL throws a serialization type-mismatch error:
`graph write error: Transport error: error serializing parameter X` (e.g. parameter 8).

This happens because `tokio-postgres` attempts to bind an `INT4` OID type to a query parameter corresponding to a `TIMESTAMPTZ` column.

## Expected Behavior
Nullable values of any type should be successfully bound and persisted as `NULL` without throwing OID serialization mismatch errors.

## Suggested Solution
Define a generic, type-agnostic `PgNull` struct that implements `tokio_postgres::types::ToSql`. By returning `true` for all types in `accepts` and `IsNull::Yes` in `to_sql`, it acts as a generic SQL `NULL` compatible with all columns:

```rust
#[derive(Debug, Clone, Copy)]
struct PgNull;

impl tokio_postgres::types::ToSql for PgNull {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(tokio_postgres::types::IsNull::Yes)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(tokio_postgres::types::IsNull::Yes)
    }
}
```

In `bind_pg`, handle `Value::Null` by adding `PgNull` to arguments:
```rust
Value::Null => {
    args.add(PgNull);
}
```
