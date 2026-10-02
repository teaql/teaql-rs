# teaql-provider-sqlite

Synchronous rusqlite provider adapter for TeaQL runtime.

This provider targets embedded and multi-architecture deployments where a small
SQLite stack is preferable, such as routers, robots, and appliance controllers.

For development verification, resolve this crate and `teaql-runtime` from the
same local checkout. Public artifact availability is a separate release gate.

```rust
use rusqlite::Connection;
use teaql_provider_sqlite::{SqliteMutationExecutor, SqliteProviderExt};
use teaql_runtime::UserContext;

let connection = Connection::open("app.db")?;
let executor = SqliteMutationExecutor::from_connection(connection);

let mut ctx = UserContext::new();
// Register the generated module's metadata before schema initialization.
ctx.use_sqlite_provider(executor);
ctx.ensure_schema().await?;
```

## Transaction ownership

On this development branch, `SqlTransactionTransport::begin_sql()` returns a
non-cloneable `SqliteTransaction`. It holds an asynchronous lease on the
provider's single connection until commit, rollback or drop. Clones of the
provider share this lease; root SQL transport calls wait instead of accidentally
joining an unrelated transaction. Cancellation or dropping an active transaction
rolls back before the lease is released. Transaction I/O uses the returned
transaction transport and does not reacquire its own lease.

Prepared repeated queries and streaming preserve their existing behavior. A
root stream holds the lease for its cursor lifetime. Drop the stream before
starting a transaction on the same provider; use the transaction transport for
queries within a transaction. Generated Context Q/E/save APIs are unchanged.

This guarantee covers `SqlTransport`, `StreamingSqlTransport` and
`SqlTransactionTransport` on one provider and its clones. Independently wrapping
the same raw connection does not share the lease. Synchronous raw connection,
schema and ID allocation primitives are not covered by this transport test
gate. Do not mix these low-level calls with an active transaction.

Regression tests are retained in `tests/transaction_lease.rs` and
`tests/request_trace_log.rs`, tracked by
[#240](https://github.com/teaql/teaql-rs/issues/240) and
[#239](https://github.com/teaql/teaql-rs/issues/239). They cover concurrent native
Context batches, waiting I/O, cancellation, rollback, repeated queries and
streaming. They do not prove the complete generated Trace Chain graph.
