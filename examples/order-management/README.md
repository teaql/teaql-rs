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
