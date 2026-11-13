# Inherited save example

Save an Academy once, then load and edit the same object through Academy and
School Q APIs. Generated E expressions verify the inherited values. No
application code inserts a parent row or writes SQL.

The six-object model includes `School → Academy → TrainingAcademy`, a College
sibling, Platform and SchoolType constants. The lifecycle executable covers
Academy creation, unchanged save, incomplete-target rejection, inherited and
own-field updates, stale-version rejection, parent edits and mark-to-delete
plus save. The root owns the version: `1 → 2 → 3 → -4`. Normal parent and child
queries hide the tombstone; an explicit deleted-row query retains it.

## Run locally

From the runtime repository root:

```bash
INHERITANCE_MATCHING_SERVICE_CORE_DATABASE_URL=/tmp/my-new-inheritance-demo.sqlite \
  cargo run --manifest-path examples/inheritance-save/Cargo.toml

cargo test --manifest-path examples/inheritance-save/Cargo.toml

node examples/inheritance-save/verify.cjs --output /absolute/new/evidence-directory
```

Choose a new database path for a separate demonstration. Each invocation creates
one new Academy. Repeating it against the same database checks idempotent
schema/bootstrap and independent IDs; it does not clear previous rows.

The integration test runs the executable twice on one temporary database and
uses read-only SQL to verify the physical mapping and root-owned versions. The
verification script additionally checks local dependency origins, runs the test
suite twice, runs two executable rounds on one retained database and verifies
the generated library hashes. It removes its owned compilation output after
the child processes exit and retains compact logs, hashes and the database.
Python 3 supplies the independent read-only database oracle.

## Generated library and customization boundary

`lib/` and `teaql/` are unchanged generation outputs, bound by
`generated-files.json` and `generation-origin.json`. All TeaQL dependencies
resolve to this repository through the example's local Cargo patches; package
version labels do not substitute for local-source verification.

Edit `src/main.rs` and `tests/lifecycle.rs`, not the generated library. Current
model-aware API help is retained in `assist/`. For a model change, evaluate and
regenerate the library and Assist together, then replace the complete generated
fixture and its provenance. Do not discover APIs by reading generated source.

This example exercises SQLite. The separate PostgreSQL lifecycle tests and
fresh generated consumers qualify that provider; this executable does not
claim to switch database providers at runtime.
