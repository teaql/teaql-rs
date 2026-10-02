<!-- ephemeral -->

Please help me implement safe generated expression access for `Order Item`.

TeaQL expressions preserve three states until evaluation:

- `Value(value)` becomes `Some(value)`.
- `Null` means the field or relation was loaded and is legitimately absent, so
  it becomes `None`.
- `NotLoaded` means the query did not preload required data. This is a coding
  error and `.eval()` intentionally panics with the missing access path.

When a default is required only for loaded Null, the underlying generated API
provides `.or_if_null(value)`, `.or_else_if_null(|| value)`, and
`.or_default_if_null()`. These deliberately preserve the same fail-fast behavior for `NotLoaded`.

The following is complete model-derived Rust source, not pseudocode. Scalar,
forward-relation, and generated reverse-list methods are emitted only when they
exist in the generated `E` facade.

```rust
use trace_chain_service_core::{E, OrderItem};
use trace_chain_service_core::CustomerOrder;


pub fn extract_order_item_id(entity: &OrderItem) -> Option<u64 > {
    E::order_item(entity).get_id().eval()
}

pub fn extract_order_item_id_or_if_null(entity: &OrderItem, fallback: u64) -> u64 {
    E::order_item(entity).get_id().or_if_null(fallback)
}

pub fn extract_order_item_name(entity: &OrderItem) -> Option<String > {
    E::order_item(entity).get_name().eval()
}

pub fn extract_order_item_name_or_if_null(entity: &OrderItem, fallback: String) -> String {
    E::order_item(entity).get_name().or_if_null(fallback)
}

pub fn extract_order_item_version(entity: &OrderItem) -> Option<i64 > {
    E::order_item(entity).get_version().eval()
}

pub fn extract_order_item_version_or_if_null(entity: &OrderItem, fallback: i64) -> i64 {
    E::order_item(entity).get_version().or_if_null(fallback)
}

pub fn extract_order_item_customer_order_id(entity: &OrderItem) -> Option<u64 > {
    E::order_item(entity).get_customer_order_id().eval()
}

pub fn traverse_order_item_customer_order<'a>(entity: &'a OrderItem) -> Option<&'a CustomerOrder> {
    E::order_item(entity).get_customer_order().eval()
}


```

Use only the generated functions above. Select every traversed field or
relation in the corresponding query. A loaded null/list miss may be handled as
`None`; do not catch or default a NotLoaded panic.
>>


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

Capability: `expression`.

- Distinguish a loaded null from a field or relation that was not projected. A
  NotLoaded/coding error must remain visible; do not turn it into an ordinary null.
- Select every traversed relation first and use the generated E/expression API for
  scalar, object, and list traversal. Do not translate Java accessor names by guess.
