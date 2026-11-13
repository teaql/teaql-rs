# School Management bootstrap example

This generated example retains `models/school-model.xml`. `cargo run` verifies
explicit SQLite schema creation and idempotent repeated bootstrap of Platform
`id=1` and SchoolType constants `1001`/`1002` against the local runtime.
It also checks that a sparse audited School create missing required `address`
returns a field-specific Checker error before SQLite writes any row.

The application-owned `src/bin/soft_delete_return_probe.rs` follows current
`rust-assist-delete/school`: fully load School, call `mark_for_deletion()`, then
audited `save(context)`. It asserts the returned strong entity has the
persisted `version=-2` tombstone. It also deletes a second School inside an
explicit transaction: a test-only SQLite trigger changes that tombstone to
`version=-9`, and the strong Save result must read back `-9` rather than guess
`-2`. A newly allocated School marked for deletion before Save must report
`cancelled new root` with no database row, on both ordinary and explicit
transaction Save paths. The controlled example script runs
this probe twice on one SQLite file without cleanup between runs; its SQLite
rows were checked directly on September 15.

`bash scripts/verify-school-bootstrap-example.sh` (from the repository root)
also runs the application-owned `bootstrap_trace_probe` twice per database,
with SQL logging on and off independently. It verifies first/repeated seeds,
audited constant drift and reconciliation, no-op reseeding, fixed identities,
caller restoration, SQL trace routes and committed safe audit lineage. At audit
delivery, an independent read-only SQLite connection must already see the
persisted bootstrap version. Safe audits retain `bootstrap_audit` attribution;
ordinary application saves have no bootstrap attribution. Derived readbacks
retain intent, with target IDs redacted only in free-text diagnostics, not in
SQL bindings or typed identities. Generated-library hashes must stay unchanged.
Outputs and databases are retained at the path printed by the script. This is
local-source acceptance, not a published-package or Registry replay.

From the repository root, `examples/verify-runtime-examples.sh` runs this and
the minimum conformance example against current path-patched runtime sources.
Its acceptance groups must all pass before a local runtime change is
called example-verified. The current repository-wide gate is `scripts/verify-examples.sh`.

SQL privacy is field-aware: this model marks `School.name` with
`_audit_mask_fields="name"`, while ordinary `address` bindings remain visible in
expanded SQL. The generated library must propagate that declaration; do not
hand-patch an entity to make the test pass. The retained
`examples/tests/sql_log_masking.rs` fixture separately marks a business field
for the shared mask algorithm, protects a password, and verifies actual SQLite
CRUD, cache reuse and independent batch statements without changing stored values.
An explicit debug sink always labels its opt-in; unknown bindings and credentials
remain hidden. A stale-write sensitive name must not appear in ordinary logs or
unapproved debug sinks; the explicit plaintext debug sink remains opt-in.

## Grouped queries over mapped columns

Logical fields and physical database columns are separate metadata. For example,
School's logical FK key is `school_type_id`, while its SQLite column is
`school_type`. A grouped row must expose `school_type_id`; selecting grouped
details adds a `school_type` object without replacing the scalar FK. An explicit
`group_by_school_type_as("kindKey")` replaces the default grouping result key;
it does not rename the database column or return an extra `school_type_id` key.

From the repository root, run:

```bash
node examples/school-management/verify-grouped-mapping.cjs --native --output /absolute/new/evidence-directory
```

`tests/grouped_field_mapping.rs` starts with existing legacy tables whose columns
include `Legacy Kind Key` and `Legacy Display Text`. Audited Context mutations
populate them. The tests check logical keys, explicit aliases, grouped SUM and
COUNT, grouped relation details through both row and SmartList execution, and
grouping without aggregate functions. The original table definitions must remain
unchanged. Physical column names are not accepted as logical query fields.

The model explicitly selects runtime 7.1. The application-owned
`grouped_mapping_probe` exercises the regenerated, unmodified generated
Q API: `group_by_school_type`, explicit aliases, `group_by_school_type_with_details`,
and `roll_up_to_school_type`. It runs twice on one SQLite file, and an independent
read-only query checks the stored FK values. Grouping without an aggregate returns
only dimensions, not arbitrary fields from one member of the group. The verifier
checks generated-library hashes and removes its disposable compilation output.
Cargo metadata must resolve all six runtime components to this checkout at 7.1;
old versions, registry artifacts and unrelated source directories fail the gate.
The accompanying `native-verification.test.cjs` covers those rejection cases.

The same gate runs the 31-case `rollup_statistics_probe` twice, including
parent-local measures, two-hop navigation, HAVING, statistical paging, aliases,
distinct counts, Identity and SOUNDEX grouping. Independent relation predicates
retain their nested statistics and cell paging. Repeating navigation on a cloned
request replaces its old navigation target without changing the original or
discarding an independent business filter. Six generated helper tests cover
snapshot/cycle guards and preservation of independent relation-filter metadata.
The gate also runs the main application,
deletion/environment probes twice and bootstrap reconciliation with SQL logging
on/off twice. Default distinct-count aliases use KSML names, not display labels
such as `School Type`.
The optional `--native` flag also runs all targets of the core, SQL, runtime and
SQLite crates. This is local-source verification, not release acceptance.

The gate also runs `numeric_rollup_probe` twice. It changes only the mutable
SchoolType presentation ranks through audited Mutation, checks persistence with
Q/E, and restores them after each run. Totals of 2 and 10 exercise numeric rather
than lexical ordering through `add_order_by_ascending` and
`add_order_by_descending`, ordinary/parent/nested paging and exact HAVING.
The eight scenarios include rejecting an unknown layered sort result and
unselected or ambiguous ordinary sort operands. The legacy mapping fixture
keeps both FK result keys, orders by the unique explicit alias, and rejects
the ambiguous source expression. A read-only oracle confirms the original
ranks 1 and 2 remain in the database.

The same numeric probe runs four GBK guard scenarios twice: ordinary ascending
and descending ordering, a parent-cell page and a nested final-result page.
Names change through audited Mutation and Q/E readback, then return to Primary
and Secondary. This SQLite example requires an explicit unsupported-operation
error; it does not demonstrate working GBK sorting. The PostgreSQL generated
consumer gate verifies those four scenarios with real GBK ordering. GBK grouping
and binary projections are separate capabilities and are not enabled by sorting.

Nine bitwise scenarios also run twice through the generated Q API and its
existing generic integer aggregate methods. They cover ordinary and grouped
results, parent-local and two-hop measures, HAVING/paging, and empty scalar,
layered and grouped populations. Parent XOR must be 3 from IDs 1001 and 1002,
not 1002 from repeating those parents for five schools. The grandparent
contributes once. Bitwise result values remain integers; empty scalar measures
are null, and empty grouped queries return no cells. Current integer field
Assist supplies the exact logical keys and methods.
The ninth scenario rejects all three bitwise operations over a string field,
both on the source and on its roll-up parent. The metadata-bound aggregate
compiler reports `bitwise aggregate requires an integer field` before SQL;
Boolean, Decimal, floating-point, JSON and temporal field domains are not
implicitly converted to integers. Native compiler/provider tests separately
exercise integer expression operands, nested Identity wrappers, signed boundaries
and real nulls. PostgreSQL binds scalar integer parameters with an explicit BIGINT
cast, preserving values and masking policies. These native cases do not add
generated literal-parameter helpers or establish general layered expression
projection support; the nine generated-Q scenarios above remain unchanged.

Regenerate library, current Assist and workspace guidance together from the
model using `scripts/regenerate-rust-native-school.cjs` in the code-generator
repository. Its `--runtime` option points to this checkout, and `--output` must
be a fresh absolute evidence directory. Generated destinations must be clean;
unknown emitted paths are rejected before installation. Application-owned probes
are not overwritten. Do not repair generated library source manually.
