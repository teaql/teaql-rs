<!-- ephemeral -->

Please help me complete the creation (Create) service business code for the `Payment` object.

To ensure absolute correctness of the API, please refer to and strictly imitate the following **real creation code example for `Payment`**.

### Standard Creation Example (Reference)
Please carefully observe the initialization of the object, the `update_xxx()` property setting methods, and the strictly required `.audit_as()` and `.save()` cascade in the example code.

```rust
use trace_chain_service_core::teaql_core::Entity;
use trace_chain_service_core::{Q, Payment, TeaqlRuntime, AuditedSave};

pub async fn create_example(
    context: &impl TeaqlRuntime,
) -> Result<Payment, Box<dyn std::error::Error>> {
    // 1. Initialize the new entity
    let mut new_entity = Q::payments()
        .comment("what: Initialize a new entity instance")
        .purpose("why: Create a new entity instance")
        .new_entity(context);

    // 2. Set property values (replace dummy data with actual inputs)
    // new_entity.update_customer_order_id(/* input data */);
    // new_entity.update_reference_code(/* input data */);


    // 3. CRITICAL: Security audit constraints must be attached before calling save()!
    let persisted = new_entity
        .audit_as("Why this create operation was executed (audit record)")
        .save(context).await?;

    Ok(persisted)
}
```

### When this new entity belongs to an existing mutation graph

This is optional. If the operation must save a new child and a loaded parent as
one graph, first set the child's **modeled parent-reference ID** using the
`update_xxx_id(...)` method listed above. Load the parent with its full
non-minimal `Q` request. Then explicitly join the child's pending mutation
intent to the parent before one audited parent save:

```rust
use trace_chain_service_core::LedgerEntity as _;

parent.include_pending_mutations_from(&new_entity)?;
let persisted_parent = parent.audit_as("business reason: save the graph")
    .save(context).await?;
```

`include_pending_mutations_from` is the exact public runtime contract. It
does not query, save, or create a context-owned ledger. Do not save the child
separately, clone entities to compose them, or guess the parent's query and
the child's relationship setter. If either exact generated API is absent
from Assist, report `MISSING_ASSIST` for that path.

If this child contributes an additional local business reason, apply the
validated audit wrapper without saving it separately:

```rust
let new_entity = new_entity.audit_as("business reason: authorize payment").into_entity();
parent.include_pending_mutations_from(&new_entity)?;
```

`into_entity()` applies the reason to the child, without SQL or a fabricated
trace chain. Omit that wrapper when the child only inherits the parent reason.
Set the modeled parent-reference IDs on descendants before composition; the
runtime discovers ancestry from those relationships. Save only the audited root.

### Your Task
Please completely imitate the framework, imports, and syntax features of the above code to implement the real creation logic for `Payment` based on my specific business needs. Please output the Rust source code directly.

**CRITICAL**: We have already generated the correct plural/singular form for the `Q::` methods in the example above (e.g. `Q::payments_minimal()`). Do NOT try to guess or invent a plural form yourself. You MUST copy the EXACT spelling of the `Q::` method from the example.


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

Capability: `create`.

- Validate and allow-list writable business fields; never mass-assign dynamic JSON.
- Create through the generated request/entity API, attach a non-empty audit reason,
  save with the same UserContext, and return the runtime's native save result.
- Add a negative test proving a missing audit reason cannot write.
