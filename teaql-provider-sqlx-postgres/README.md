# teaql-provider-sqlx-postgres

PostgreSQL SQLx provider adapter for TeaQL runtime.

This crate owns PostgreSQL-specific execution, schema bootstrap, transaction
wrapping, row decoding, and ID-space generation. It is intentionally separate
from the SQLite and MySQL providers.

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
