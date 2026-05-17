# teaql-provider-rusqlite

Synchronous rusqlite provider adapter for TeaQL runtime.

This provider targets embedded and multi-architecture deployments where a small
SQLite stack is preferable, such as routers, robots, and appliance controllers.

```toml
teaql-provider-rusqlite = "0.7"
```

```rust
use rusqlite::Connection;
use teaql_provider_rusqlite::{RusqliteMutationExecutor, RusqliteProviderExt};
use teaql_runtime::UserContext;

let connection = Connection::open("app.db")?;
let executor = RusqliteMutationExecutor::new(connection);

let mut ctx = UserContext::new();
ctx.use_rusqlite_provider(executor);
ctx.ensure_schema().await?;
```
