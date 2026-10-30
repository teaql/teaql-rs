# teaql-provider-postgres

PostgreSQL provider adapter for TeaQL runtime, using tokio-postgres and deadpool.

This crate owns PostgreSQL-specific execution, schema bootstrap, transaction
wrapping, row decoding, and ID-space generation. It is intentionally separate
from the SQLite and MySQL providers.

Native SQL wrappers expose framework infrastructure on the same resource and
physical transaction. Each transaction has a fresh opaque identity and read-only
completion witness. Cancelled or uncertain completion removes its connection from
the pool; successful commit/rollback may recycle it. SQL and explicit logical
failures mark rollback-only so a caught statement error cannot report an aborted
COMMIT as success. UNKNOWN remains uncertain even when the server already committed.
See the optional [Blob catalog component](../teaql-blob-reference/README.md) for
the trusted infrastructure boundary and its separate remaining implementation gates.

## Usage

```toml
teaql-runtime = "0.7"
teaql-provider-sqlx-postgres = "0.7"
```

```rust
use teaql_provider_sqlx_postgres::{
    PgMutationExecutor, PostgresProviderExt,
};
use teaql_runtime::UserContext;

let pool = sqlx::PgPool::connect(&database_url).await?;
let mut ctx = UserContext::new()
    .with_module(my_domain::module())
    .with_repository_registry(my_domain::repository_registry());

ctx.use_postgres_provider(PgMutationExecutor::new(pool));
ctx.ensure_schema().await?;
```
