# Order Management — Rust + SQLite

Run without a database server, fixture DB, model input, or generator installation:

```bash
cd examples/order-management/rust-app-console
cargo run
```

The first run creates `../.local/order.db`, ensures schema, seeds through generated entities, performs a compile-time governed query, and saves an audited preset. The second run demonstrates idempotency.

Read `rust-app-console/src/main.rs` first (handwritten), then `rust-lib-core/lib/src/q.rs`, `customer_order/request.rs`, and `customer_order/entity.rs` (generated). Notice that `purpose(...)` returns the only type exposing execute methods; `comment(...)` may occur earlier in the request-building chain.

## Verify the first result

Expect `WEB-2026-001`, `2026-08-12`, and the exact decimal `129.95`. Run twice: only the first run mutates. This also proves SQLite numeric decoding does not expose a binary floating-point tail.

## Customize it

Edit the typed filter or ordering in `src/main.rs`. Compiler errors lead directly to the generated request API—do not guess method names. Keep application policy in the console and regenerate `rust-lib-core`; the shared generation model is provenance, not a runtime input.
### Materialized-list hard limit

`execute_for_list` protects the service by applying a default hard limit of 10,000 rows. A requested page size above that ceiling fails explicitly. Trusted application code can call `hard_limit(...)` to override the outer-query ceiling. **Caution:** most applications should not override it; do so only for a reviewed, exceptional requirement. This setting does not describe streaming execution.

### Streaming large root queries

`execute_for_stream` returns a lazy stream of generated entities. The cursor is released when the stream finishes or is dropped:

```rust
let mut orders = request.stream(500).comment("export orders").purpose("reviewed export").execute_for_stream(&ctx).await?;
while let Some(order) = orders.next().await { write_order(order?); }
```

The chunk size controls provider fetch batches, not a client-visible collection. **Caution:** normally keep the default 1,000. Streaming relation or aggregate enhancement is rejected; use a root query or `execute_for_list`. Ordinary TFP federation does not transport this stream and requires a separate protocol.
