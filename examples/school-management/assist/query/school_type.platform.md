<!-- ephemeral -->

# Rust Query Field Assist — `school_type.platform`

KSML entity: `school_type`

KSML field: `platform`

Type: `reference to platform`

| Capability | Generated API |
| --- | --- |
| Select | `select_platform()`, `select_platform_with(Q::platforms_minimal())` |
| Filter nested request | `with_platform_matching(request)`, `without_platform_matching(request)` |
| Presence | `have_platform()`, `have_no_platform()` |
| Order by reference ID | `order_by_platform_asc()`, `order_by_platform_desc()` |
| Group | `group_by_platform()`, `group_by_platform_as(alias)` |
| Group expression | `group_by_platform_with_function(alias, function)` |
| Filter statistical results | `having(expr)`; repeated calls combine with AND |
| Order statistical results | `add_order_by_ascending(result_key)`, `add_order_by_descending(result_key)` |
| Count non-null references | `count_platform()`, `count_platform_as(alias)` |
| Group with details | `group_by_platform_with_details()`, `group_by_platform_with_details_from(request)` |
| Configure parent for roll-up | `roll_up_to_platform()`, `roll_up_to_platform_with(request)` |
| Count distinct non-null references | `count_distinct_platform()`, `count_distinct_platform_as(alias)` |
| Facet | `facet_by_platform_as(name, request)`, `facet_by_platform_as_with_options(name, request, include_all_facets)` |
Apply methods before `.purpose(...)`; execution still requires non-empty
`.comment(...)`, `.purpose(...)`, and exactly one context argument.

Grouped projections use `.execute_for_rows(&context)`, not entity-shaped
`.execute_for_list(&context)`. Each result is a `CompactRow`: read the dimension
with `row.get("platform_id")` and a measure with `row.get(alias)`.
Physical database column names are resolved through metadata; do not use them as
logical query fields.
`group_by_platform_as(alias)` replaces the default result key with
the supplied alias, matching Java. The raw field is not returned as an additional
key unless it is also explicitly grouped. Read the value with `row.get(alias)`.
The generated field ordering methods still order this selected grouping key.
Pass `teaql_core::ExprFunction::Identity` to
`group_by_platform_with_function(alias, function)` to group by this
field's unchanged value under the supplied alias (Java `AggrFunction.SELF`).
This preserves the field's metadata type and null grouping key; it does not
convert numeric, date, Boolean or reference values to text.
SOUNDEX requires text; use Identity or ordinary grouping for this field.
Aggregate functions are not scalar grouping transforms and are rejected;
GBK grouping is not yet supported by this runtime's result-value representation.

Use `having(teaql_core::Expr::gt("selected_measure_alias", value))` for predicates
on completed statistical cells, not a field filter on the source rows. A parent
request's HAVING is applied to its own grouped cells before its ordering/page;
configure it on the typed parent returned by roll-up.
For a measure alias or nested result path, use `add_order_by_ascending(result_key)`
or `add_order_by_descending(result_key)` before `limit(...)`. The key must be an
already selected statistical result, not a physical column or SQL fragment.
These methods preserve the live parent configuration and match Java's
`addOrderByAscending` / `addOrderByDescending`, with Rust builder ownership.
Grouped details add an object at `row.get("platform")` and retain
the scalar FK at `row.get("platform_id")`.

`roll_up_to_platform()` borrows the source request and returns a
typed `PlatformRequest`, following Java navigation.
Configure that parent with its own field Assist, then execute the original source
statistics request. Parent filters and projections remain linked even while its
handle is alive; no closure or finalization call is required. An explicit
`.clone()` takes an independent snapshot.
The selected Rust 7.1 runtime also supports grouped/aggregate parent requests and
repeated navigation: navigating the same relation again replaces only its previous
navigation binding, including on a cloned request. Independent business filters
remain in effect, and the original request is not rebound by changes to its clone.
It also supports
multi-hop roll-up. Parent-local measures retain their own entity identity grain;
they are not counted once per source child. A grouped parent's result keys replace
the intermediate FK dimension; inspect the parent object at
`row.get("platform")`, not the intermediate ID as a final group key.
For example:

```rust
let mut source = Q::school_types().count_as("n").limit(20);
let parent = source.roll_up_to_platform();
// Configure parent using its own field Assist, then execute source with
// comment, purpose, and execute_for_rows(&context).
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
