<!-- ephemeral -->

Please help me complete the update (Update/Modify) service business code for the `Customer Order` object.

An entity update is not a partial DTO patch. Load every scalar field before
modification so the checker can validate the complete business state. Do not
load the mutation target with a minimal request or a reduced `select_xxx()`
projection: if you have not seen the complete entity, you must not change it.

To ensure absolute correctness of the API, please refer to and strictly imitate the following **real update code example for `Customer Order`**.

### Standard Update Example (Reference)
Please carefully observe the querying mechanism, the `update_xxx()` property setting methods, and the strictly required `.audit_as()` and `.save()` cascade in the example code:

```rust
use trace_chain_service_core::teaql_core::Entity;
use trace_chain_service_core::{Q, CustomerOrder, TeaqlRuntime, AuditedSave};

pub async fn update_example(
    context: &impl TeaqlRuntime,
    entity_id: u64,
    new_order_number: String,
    new_description: String,

) -> Result<Option<CustomerOrder>, Box<dyn std::error::Error>>
{
    let Some(mut existing_entity) = Q::customer_orders()
        .with_id_is(entity_id)

        .comment("what: Find entity for modification")
        .purpose("why: Need to update properties")
        .execute_for_one(context)
        .await?
    else { return Ok(None); };

    existing_entity.update_order_number(new_order_number);
    existing_entity.update_description(new_description);


    let persisted = existing_entity
        .audit_as("business reason: update authorized fields")
        .save(context).await?;

    Ok(Some(persisted))
}
```

### Adding a newly created child to this entity's save graph

Only when the business operation needs one graph save: load this entity with
the full non-minimal request above, create the child with its own exact
`rust-assist-create/[child]` API, and set the child's modeled parent-reference
ID before composing. The generated library re-exports the public trait:

```rust
use trace_chain_service_core::LedgerEntity as _;

existing_entity.include_pending_mutations_from(&new_child)?;
let persisted = existing_entity.audit_as("business reason: save the graph")
    .save(context).await?;
```

This only includes pending mutations. It does not load relations, issue SQL,
or save automatically. Never join independent operations through
`UserContext`, clone before save, or invent a relationship setter; request
the child Assist and report `MISSING_ASSIST` if the exact operation is absent.

To give a child an additional local reason without a separate save:

```rust
let new_child = new_child.audit_as("business reason: authorize payment").into_entity();
existing_entity.include_pending_mutations_from(&new_child)?;
```

`into_entity()` applies the validated reason but performs no SQL and does not
inject trace nodes. Leave children unannotated when they inherit the root
reason. The runtime discovers nested ancestry from modeled parent-reference
IDs, not from Context or the order of include calls. Save only the audited root.

### Your Task
Please completely imitate the framework, imports, and syntax features of the above code to implement the real update logic for `Customer Order` based on my specific business needs. Please output the Rust source code directly.

**CRITICAL**: We have already generated the correct plural/singular form for the `Q::` method in the example above. Copy its EXACT spelling. Do not replace it with the `_minimal()` form or add a reduced projection for an entity that will be saved.


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

Capability: `update`.

- Load the tenant-scoped current entity first so its original version participates
  in optimistic locking; do not reconstruct versioned state from untrusted JSON.
- Allow-list writable fields, attach the generated audit-reason API, and save with
  the same UserContext. Add a stale-version rejection test.
