<!-- ephemeral -->

# Rust Assist — List page `Customer Order`

Use the exact generated `Q::customer_orders_minimal()` entry point.
Apply only model-derived filters and projections, a deterministic ID tie-breaker,
and the runtime's trusted materialization ceiling. Do not accept raw dynamic
query JSON or let a client override the hard limit.

```rust
use teaql_core::SmartList;
use trace_chain_service_core::{
    Q, CustomerOrder, TeaqlRuntime,
};

pub async fn list_customer_order_page(
    context: &impl TeaqlRuntime,
    offset: u64,
    limit: u64,
) -> Result<SmartList<CustomerOrder>, Box<dyn std::error::Error>> {
    let page = Q::customer_orders_minimal()
        .select_order_number()
        .select_description()
        .select_platform_with(Q::platforms_minimal())
        .select_order_item_list()
        .select_payment_list()
        .select_shipment_list()
        .order_by_id_asc()
        .comment("what: load a stable page of Customer Order rows")
        .purpose("why: serve the authorized bounded Customer Order list")
        .execute_for_page(context, offset, limit)
        .await?;

    Ok(page)
}
```

Request field-level Assist before adding or changing a field predicate. Boolean
field Assist documents the distinct `true`, `false`, `unknown`, and `known`
states without expanding every field vocabulary into this entity overview.

Compile the source unchanged. Prove exact filtering, projection and relation
selection, stable non-overlapping pages, filtered total count, enforcement of
the default 10,000-row hard limit, intent checks, and compilation failure for
unknown fields or client-controlled hard-limit APIs.

## Bounded scalar streaming

Use `futures_util::StreamExt` (an application dependency) to consume the stream.
This is a scalar-only request: relation and aggregate streaming is not supported.
Chunk size controls delivery, not the total query bound; keep `.limit(...)`.

```rust
use futures_util::StreamExt;

let mut rows = Q::customer_orders_minimal()
    .select_order_number()
    .select_description()
    .order_by_id_asc()
    .limit(20)
    .stream(1)
    .comment("what: stream bounded Customer Order scalar rows")
    .purpose("why: process authorized rows incrementally")
    .execute_for_stream(context)
    .await?;
while let Some(row) = rows.next().await {
    let row = row?;
    // Use current expression Assist to read selected fields through E.
}
drop(rows);
```

The request owns its intent; consuming it later must not borrow another query's
comment or purpose. Exhaustion reports success; dropping a partially consumed
stream releases its cursor and reports cancellation when diagnostics are enabled.
The diagnostic row count is rows delivered by the provider in chunks, not the
number already processed by application code. Use chunk size 1 for exact
single-row early-close probes. A never-polled generated stream issues no SQL
and must not invent an executed SQL record. For mutation, fully load all fields
using the non-minimal Q entry point and use the current update/save Assist.


---

## TeaQL seven-language assist contract

Apply the verified Rust semantic ceiling while using only the exact RUST generated and
runtime APIs. Discover APIs through the generated application AGENTS.md and progressive
model-aware Assist. Do not inspect generated domain-library source.

- Do not create plurals by appending `s` or `es`; use the centralized generated plural.
- Human and non-human entities use different generated predicate vocabularies. Preserve
  forms such as “who are active” and “whose email is”; never infer them from English.
- Configure filters, projection, paging, and other query options before `purpose(...)`.
  Comment may appear anywhere in the chain. Purpose enters the executable stage; execution
  requires both values, but comment does not have to immediately precede purpose.
- Every execute/list/stream and every save accepts exactly one context argument:
  `UserContext`. Name that argument `context`, never `runtime`; data services and global
  policy are injected when the context is built. Reserve `runtime` for process-level
  runtime ownership, provider/pool setup, and module assembly.
- Tenant, merchant, identity, permissions, request policy, purpose policy, hard limit,
  and continuous-page cursor policy come only from trusted context, never dynamic JSON or TFP.
- If the required operation is absent after current entity/action and required field
  Assist, stop that path and report MISSING_ASSIST. Do not guess an API or search the
  generated library as a fallback.
- Create each application-owned source file once. After its first compile attempt,
  repair only the smallest block identified by the exact compiler or test diagnostic.
  Preserve unrelated code; do not rewrite the complete file as an error-recovery loop.
- Before a repair that would replace more than 25% of an existing application file,
  stop and report LARGE_REWRITE_REQUEST with the file, exact diagnostic, reason, and
  estimated scope. Initial creation and model-driven regeneration are not repairs.

Capability: `list-page`.

- Validate offset, page size, filters, deep paths, IN-list size, and sort against
  explicit allow-lists. Reject invalid input instead of widening the query.
- Use a stable unique ordering and retain the runtime hard limit. Continuous-page
  optimization is opt-in, browsing-only, local runtime policy and cannot cross TFP.
- Run count only when explicitly requested; otherwise use the returned list length.
