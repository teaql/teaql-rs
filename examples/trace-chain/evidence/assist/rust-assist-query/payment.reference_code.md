<!-- ephemeral -->

# Rust Query Field Assist — `payment.reference_code`

KSML entity: `Payment`  
KSML field: `reference_code`  
Type: `string`

| Capability | Generated API |
| --- | --- |
| Select | `select_reference_code()` |
| Entity accessor | `reference_code()` |
| Equality/set | `with_reference_code_is(value)`, `with_reference_code_is_not(value)`, `with_reference_code_in(values)`, `with_reference_code_not_in(values)` |
| Comparison | `with_reference_code_greater_than(value)`, `with_reference_code_greater_than_or_equal_to(value)`, `with_reference_code_less_than(value)`, `with_reference_code_less_than_or_equal_to(value)`, `with_reference_code_between(lower, upper)` |
| Null state | `with_reference_code_is_known()`, `with_reference_code_is_unknown()` |
| String | `with_reference_code_containing(value)`, `with_reference_code_not_containing(value)`, `with_reference_code_starting_with(value)`, `with_reference_code_ending_with(value)`, `with_reference_code_sounding_like(value)` |
| Order | `order_by_reference_code_asc()`, `order_by_reference_code_desc()` |
| Group | `group_by_reference_code()`, `group_by_reference_code_as(alias)` |
Apply methods before `.purpose(...)`; execution still requires non-empty
`.comment(...)`, `.purpose(...)`, and exactly one context argument.


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
