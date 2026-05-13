# teaql-provider-sqlx-mysql

MySQL SQLx provider adapter for TeaQL runtime.

This crate owns MySQL-specific execution, schema bootstrap, transaction
wrapping, row decoding, and ID-space generation. It is intentionally separate
from the PostgreSQL and SQLite providers.

## Usage

```toml
teaql-runtime = "0.7"
teaql-provider-sqlx-mysql = "0.7"
```

```rust
use teaql_provider_sqlx_mysql::{
    MysqlMutationExecutor, MysqlProviderExt,
};
use teaql_runtime::UserContext;

let pool = sqlx::MySqlPool::connect(&database_url).await?;
let mut ctx = UserContext::new()
    .with_module(my_domain::module())
    .with_repository_registry(my_domain::repository_registry());

ctx.use_mysql_provider(MysqlMutationExecutor::new(pool));
ctx.ensure_schema().await?;
```
