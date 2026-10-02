<!-- ephemeral -->

# Rust Assist — Soft delete `Order Item`

Load the current generated entity so its original optimistic version is used.
Deletion is soft: never issue raw SQL or a physical delete command.

```rust
use trace_chain_service_core::teaql_core::Entity;
use trace_chain_service_core::{Q, TeaqlRuntime, AuditedSave};

pub async fn delete_example(
    context: &impl TeaqlRuntime,
    entity_id: u64,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(mut entity) = Q::order_items()
        .with_id_is(entity_id)
        .comment("what: load current Order Item for soft delete")
        .purpose("why: preserve original version and authorize deletion")
        .execute_for_one(context)
        .await?
    else {
        return Ok(false);
    };

    entity.mark_for_deletion();
    entity
        .audit_as("business reason: soft delete authorized Order Item")
        .save(context)
        .await?;
    Ok(true)
}
```

Compile and execute this source unchanged. Prove the row disappears from normal
queries but remains visible through generated deleted-row queries, an
independently loaded stale copy conflicts, missing rows return false, and
missing audit or invented physical-delete APIs fail compilation.


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

Capability: `delete`.

- Load the tenant-scoped current entity and use the generated hard-delete or
  domain-specific soft-delete API; do not invent a deletion method.
- Require an audit reason and optimistic version. Test missing audit and stale
  version as explicit failures.
