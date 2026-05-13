# teaql-provider-sqlx-sqlite

SQLite SQLx provider adapter for TeaQL runtime.

This crate owns SQLite-specific execution, schema bootstrap, transaction
wrapping, row decoding, and ID-space generation. It is intentionally separate
from the PostgreSQL and MySQL providers.

## Usage

```toml
teaql-runtime = "0.7"
teaql-provider-sqlx-sqlite = "0.7"
```

```rust
use teaql_provider_sqlx_sqlite::{
    SqliteMutationExecutor, SqliteProviderExt,
};
use teaql_runtime::UserContext;

let pool = sqlx::SqlitePool::connect("sqlite::memory:").await?;
let mut ctx = UserContext::new()
    .with_module(my_domain::module())
    .with_repository_registry(my_domain::repository_registry());

ctx.use_sqlite_provider(SqliteMutationExecutor::new(pool));
ctx.ensure_schema().await?;
```
