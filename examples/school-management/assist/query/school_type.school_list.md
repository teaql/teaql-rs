<!-- ephemeral -->

# Rust Query Reverse-Relation Assist — `school_type.school_list`

KSML entity: `school_type`  
Generated reverse relation: `school_list`  
Child entity: `school`

| Capability | Generated API |
| --- | --- |
| Select | `.select_school_list()`, `.select_school_list_with(Q::schools_minimal())` |
| Filter nested request | `with_school_list_matching(request)`, `without_school_list_matching(request)` |
| Presence | `have_schools()`, `have_no_schools()` |
| Count | `count_schools()`, `count_schools_as(alias)`, `count_schools_with(alias, child_request)` |
| Grouped statistics | `stats_from_schools_as(alias, request)` |
| Aggregate `established_date` | `min_established_date_of_schools()`, `min_established_date_of_schools_as(alias, request)` |
| Aggregate `established_date` | `max_established_date_of_schools()`, `max_established_date_of_schools_as(alias, request)` |
| Aggregate `student_capacity` | `sum_student_capacity_of_schools()`, `sum_student_capacity_of_schools_as(alias, request)` |
| Aggregate `student_capacity` | `min_student_capacity_of_schools()`, `min_student_capacity_of_schools_as(alias, request)` |
| Aggregate `student_capacity` | `max_student_capacity_of_schools()`, `max_student_capacity_of_schools_as(alias, request)` |
| Aggregate `student_capacity` | `avg_student_capacity_of_schools()`, `avg_student_capacity_of_schools_as(alias, request)` |
| Aggregate `student_capacity` | `standard_deviation_student_capacity_of_schools()`, `standard_deviation_student_capacity_of_schools_as(alias, request)` |
| Aggregate `student_capacity` | `square_root_of_population_standard_deviation_student_capacity_of_schools()`, `square_root_of_population_standard_deviation_student_capacity_of_schools_as(alias, request)` |
| Aggregate `student_capacity` | `sample_variance_student_capacity_of_schools()`, `sample_variance_student_capacity_of_schools_as(alias, request)` |
| Aggregate `student_capacity` | `sample_population_variance_student_capacity_of_schools()`, `sample_population_variance_student_capacity_of_schools_as(alias, request)` |
| Aggregate `create_time` | `min_create_time_of_schools()`, `min_create_time_of_schools_as(alias, request)` |
| Aggregate `create_time` | `max_create_time_of_schools()`, `max_create_time_of_schools_as(alias, request)` |
| Aggregate `update_time` | `min_update_time_of_schools()`, `min_update_time_of_schools_as(alias, request)` |
| Aggregate `update_time` | `max_update_time_of_schools()`, `max_update_time_of_schools_as(alias, request)` |
| Entity access | `entity.school_list()` returns `RelationHandle<'_, SmartList<School>>` |

`RelationHandle` never performs an implicit database query. Inspect `state()` when the distinction matters, and call `value()` only after the relation was explicitly selected. `value()` is present for Loaded and loaded-empty lists; it is absent for NotLoaded (and for an empty to-one relation).

```rust
let handle = entity.school_list();
let value = handle.value().ok_or("school_list was not loaded")?;
```

Facet membership counts: configure this reverse-relation count on the Facet target request. The runtime intersects it with the full active outer filter, ignoring the outer page limit/offset. Read the configured alias from the Facet row; an absent alias is not a zero count. Do not substitute the target's ordinary `count_as(alias)` aggregate.

For a generated typed entity, use the runtime `Entity` projection to read the alias without changing the ledger:

```rust
use teaql_core::{Entity as _, Value};
let values = entity.clone().into_values();
let count = values.get("configuredCountAlias").and_then(Value::try_i64)
    .ok_or("requested aggregate alias was not returned")?;
```

Generated entities do not implement `BaseEntity`; do not call `dynamic_i64` on them. Facet rows are `CompactRow` values: use `row.get(alias)` directly there. Never default a missing alias to zero.

Apply query methods before `.purpose(...)`; execution still requires non-empty `.comment(...)`, `.purpose(...)`, and exactly one context argument.


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
