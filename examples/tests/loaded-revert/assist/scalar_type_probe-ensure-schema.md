<!-- ephemeral -->
# TeaQL Rust Ensure Schema

Schema reconciliation is an explicit application action. Creating or installing `module()` or
`module_with_behaviors_and_checkers()` is passive.

```rust
let context = service_runtime(config).await?;
context.ensure_schema().await?;
```

`ensure_schema()` reads the generated module metadata already installed in the context. Never
duplicate its entity list or emit handwritten DDL. Propagate provider errors and run it only from
startup, an administrative command, or an explicit test fixture. Do not run it per request, from a
liveness endpoint, module installation, or telemetry attachment.

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

Capability: `ensure-schema`.

- Runtime Module installation is a passive manifest operation. Schema changes
  occur only after an explicit ensure-schema call by application startup, an
  administrative command, or an isolated test fixture.
- Use the generated module metadata as the only entity list; never duplicate
  descriptors or maintain parallel handwritten DDL. Propagate provider failures.
- Do not run schema reconciliation per request, from liveness, telemetry setup,
  or as an implicit side effect of module installation.
