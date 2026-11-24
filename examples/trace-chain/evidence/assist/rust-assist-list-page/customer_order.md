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
    if limit == 0 || limit > 10_000 {
        return Err("page size must be between 1 and 10,000".into());
    }
    let page = Q::customer_orders_minimal()
        .select_order_number()
        .select_description()
        .select_platform_with(Q::platforms_minimal())
        .select_order_item_list()
        .select_payment_list()
        .select_shipment_list()
        .order_by_id_asc()
        .offset(offset, limit).count()
        .comment("what: load a stable page of Customer Order rows")
        .purpose("why: serve the authorized bounded Customer Order list")
        .execute_for_list(context)
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

## Bounded stream

Streaming emits typed entities. Set the chunk size before `purpose`; consume
with `futures_util::StreamExt` and retain no more rows than the declared limit.
This ID-only projection does not load ordinary fields or relation details.

```rust
use futures_util::StreamExt;
use trace_chain_service_core::{Q, TeaqlRuntime};

pub async fn stream_customer_order_ids(context: &impl TeaqlRuntime)
    -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let mut stream = Q::customer_orders_minimal()
        .order_by_id_asc().limit(100).stream(32)
        .comment("what: stream bounded Customer Order identities")
        .purpose("why: process a small authorized batch")
        .execute_for_stream(context).await?;
    let mut ids = Vec::new();
    while let Some(row) = stream.next().await { ids.push(row?.id()); }
    Ok(ids)
}
```


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
