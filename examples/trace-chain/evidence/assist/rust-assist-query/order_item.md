<!-- ephemeral -->

# Rust Assist — Query `Order Item`

Use the exact generated `Q::order_items_minimal()` entry point.
The request is allow-listed from the model, uses stable ordering and bounded
results, and receives trusted policy/provider state only through the context.

```rust
use teaql_core::SmartList;
use trace_chain_service_core::{
    Q, OrderItem, TeaqlRuntime,
};

pub async fn query_order_item_by_id(
    context: &impl TeaqlRuntime,
    entity_id: u64,
) -> Result<SmartList<OrderItem>, Box<dyn std::error::Error>> {
    let rows = Q::order_items_minimal()
        .with_id_is(entity_id)

        .order_by_id_asc()
        .limit(20)
        .comment("what: load the requested Order Item row")
        .purpose("why: serve the authorized Order Item view")
        .execute_for_list(context)
        .await?;

    Ok(rows)
}
```

The example is complete source, not pseudocode. Use only these generated field,
predicate, ordering, projection, and relation-selection methods. Keep the same
active filters for rows, counts, facets, and aggregates. Missing/blank intent,
unknown fields, unbounded reads, and trusted-context input must fail. Use generated
relation loading rather than handwritten child-query loops; never guess a plural name.

For field-specific predicates and projections, request
`rust-assist-query/Order Item.[field_name]`; boolean fields have distinct
true, false, known, and unknown predicates in that field-level help.


## Field-specific Query Assist

Use the canonical KSML field name from this list. Do not substitute a language member name, JSON name, or database column.

| KSML field | Type | Field help |
| --- | --- | --- |
| `id` | `id` | `rust-assist-query/order_item.id` |
| `customer_order` | `relation<customer_order>` | `rust-assist-query/order_item.customer_order` |
| `name` | `string` | `rust-assist-query/order_item.name` |
| `version` | `version` | `rust-assist-query/order_item.version` |


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

Capability: `query`.

- Use generated projection, relation, predicate, sort, and aggregate APIs; never
  concatenate application SQL or silently ignore an unsupported filter.
- Rows, record count, facets, and aggregates must share the same active filter.
- Add negative tests for missing purpose/comment and forbidden dynamic fields.

### Optional per-parent Top-N optimization

- Configure `top_n_probe_parent_threshold(threshold)` on the nested child request before its intent/execution
  stage. Use the field-specific relation Assist for the exact generated selector.
- Give the child a per-parent limit and deterministic ordering with an ID tie-breaker;
  bound the parent query separately. A single-parent detail query is not batch Top-N.
- Server providers default to a window query (`threshold = 0`). A positive threshold
  permits bounded probes only when the already-loaded parent count is at or below it;
  above it, use the window plan. SQLite uses its internal AlwaysProbe policy by default.
- Measure before opting in. Do not infer a threshold from table size or execute COUNT
  or statistics queries to choose a plan. Keep an index on foreign key + sort + ID.
- Inspect selected-plan, parent-count, per-parent-limit and probe-count telemetry.
  Runtime-managed probes are not application-owned N+1 loops; neither strategy changes
  authorization, loaded-state semantics or the required comment/purpose.
