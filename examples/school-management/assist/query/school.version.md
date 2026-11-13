<!-- ephemeral -->

# Rust Query Field Assist — `school.version`

KSML entity: `school`

KSML field: `version`

Type: `version`

| Capability | Generated API |
| --- | --- |
| Select | Automatically included in entity-shaped queries; no select or unselect helper |
| Entity accessor | `version()` |
| Row visibility | `with_deleted_rows()`, `deleted_rows_only()` |
| Order | `order_by_version_asc()`, `order_by_version_desc()` |
| Group | `group_by_version()`, `group_by_version_as(alias)` |
| Group expression | `group_by_version_with_function(alias, function)` |
| Aggregate expression | `add_aggregate(named_expression)` |
| Filter statistical results | `having(expr)`; repeated calls combine with AND |
| Order statistical results | `add_order_by_ascending(result_key)`, `add_order_by_descending(result_key)` |
| Count non-null values | `count_version()`, `count_version_as(alias)` |
| Count distinct non-null values | `count_distinct_version()`, `count_distinct_version_as(alias)` |
Apply methods before `.purpose(...)`; execution still requires non-empty
`.comment(...)`, `.purpose(...)`, and exactly one context argument.

Grouped projections use `.execute_for_rows(&context)`, not entity-shaped
`.execute_for_list(&context)`. Each result is a `CompactRow`: read the dimension
with `row.get("version")` and a measure with `row.get(alias)`.
Physical database column names are resolved through metadata; do not use them as
logical query fields.
`group_by_version_as(alias)` replaces the default result key with
the supplied alias, matching Java. The raw field is not returned as an additional
key unless it is also explicitly grouped. Read the value with `row.get(alias)`.
Use `add_aggregate(teaql_core::NamedExpr::new(alias, expression))` for an aggregate
AST, matching Java `addAggregate(SimpleNamedExpression)`. An existing aggregate
alias retains its first expression, just as in Java; use a different alias for
another measure. For this field, an
unchanged distinct input is
`teaql_core::Expr::count_distinct(teaql_core::Expr::identity(teaql_core::Expr::column("version")))`.
Scalar inputs are bound by metadata before SQL and evaluated at the request's
own object grain; parent values are not repeated for every source child.
Numeric SUM/AVG/statistical inputs must be numeric; bitwise inputs must be
integers. Nested aggregates and subqueries are not scalar inputs. Parameters,
nulls and exact Decimal values retain their domains. Execute the original source
request for roll-up, using `execute_for_rows`, and keep its typed parent live.
The generated field ordering methods still order this selected grouping key.
Pass `teaql_core::ExprFunction::Identity` to
`group_by_version_with_function(alias, function)` to group by this
field's unchanged value under the supplied alias (Java `AggrFunction.SELF`).
This preserves the field's metadata type and null grouping key; it does not
convert numeric, date, Boolean or reference values to text.
SOUNDEX requires text; use Identity or ordinary grouping for this field.
GBK also requires text input.
Aggregate functions are not scalar grouping transforms and are rejected.

Use `having(teaql_core::Expr::gt("selected_measure_alias", value))` for predicates
on completed statistical cells, not a field filter on the source rows. A parent
request's HAVING is applied to its own grouped cells before its ordering/page;
configure it on the typed parent returned by roll-up.
For a measure alias or nested result path, use `add_order_by_ascending(result_key)`
or `add_order_by_descending(result_key)` before `limit(...)`. The key must be an
already selected statistical result, not a physical column or SQL fragment.
These methods preserve the live parent configuration and match Java's
`addOrderByAscending` / `addOrderByDescending`, with Rust builder ownership.


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
