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

The same gate runs the 26-case `rollup_statistics_probe` twice, including
parent-local measures, two-hop navigation, HAVING, statistical paging, aliases,
distinct counts, Identity and SOUNDEX grouping. It also runs the main application,
deletion/environment probes twice and bootstrap reconciliation with SQL logging
on/off twice. Default distinct-count aliases use KSML names, not display labels
such as `School Type`.
The optional `--native` flag also runs all targets of the core, SQL, runtime and
SQLite crates. This is local-source verification, not release acceptance.

Regenerate library, current Assist and workspace guidance together from the
model using `scripts/regenerate-rust-native-school.cjs` in the code-generator
repository. Its `--runtime` option points to this checkout, and `--output` must
be a fresh absolute evidence directory. Generated destinations must be clean;
unknown emitted paths are rejected before installation. Application-owned probes
are not overwritten. Do not repair generated library source manually.
