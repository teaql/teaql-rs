# Inherited save example

Save an Academy once, then load and edit the same object through Academy and
School Q APIs. Generated E expressions verify the inherited values. No
application code inserts a parent row or writes SQL. Tests alone may create
reject-only fault-injection triggers under the maintainer-approved DDL exception.

The six-object model includes `School → Academy → TrainingAcademy`, a College
sibling, Platform and SchoolType constants. The lifecycle executable covers
Academy creation, unchanged save, incomplete-target rejection, inherited and
own-field updates, stale-version rejection, parent edits and mark-to-delete
plus save, mark-to-recover plus save, recovery with field corrections, and a
failed mixed-object save followed by retry. The root owns the version:
`1 → 2 → 3 → -4 → 5 → -6 → 7 → 8`. Normal parent and child queries hide the
tombstone; an explicit deleted-row query retains it. Stale, live and partially
loaded recovery targets reject without publishing committed audit events.

Recovery is a pending ledger operation, not an immediate database call.
`mark_to_recover()` followed by the ordinary audited save can also correct
declaring-table fields; the root version advances once. Recovery audit events
retain the reason, trace lineage and corrected fields.

Association lists are readonly views. After cloning and changing a child,
explicitly compose its intent into the intended save boundary with
`LedgerEntity::include_pending_mutations_from(&child)`. This does not merge
independent Context operations. The source ledger stays intact; re-include it
after further edits before retrying. The example proves that an earlier root
write rolls back when the child write fails, publishes no committed audit,
and that the same graph can be corrected and retried successfully.

Queries keep their native typed view. `Entity::runtime_type()` reports the
next subtype without constructing or loading that subtype: a School view of
TrainingAcademy reports Academy, and an Academy view reports TrainingAcademy.
The readonly marker is not a mutation field or checker-dispatch key. The
executable also checks that parent-view updates and unchanged saves preserve
the marker, and that a generic audited-save helper preserves its entity type.

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

The integration test runs the executable twice on one temporary database,
creates a reject-only trigger, and uses read-only SQL to verify the physical
mapping and root-owned versions. The trigger checks that this round's root
write is visible before rejecting the child, ruling out a before-write failure.
No test SQL inserts, repairs or compensates business rows. The
verification script additionally checks local dependency origins, runs the test
suite twice, runs two executable rounds on one retained database and verifies
the generated library hashes. It removes its owned compilation output after
the child processes exit and retains compact logs, hashes and the database.
Python 3 supplies the independent database oracle and test-fixture DDL setup.
An additional test verifies marker-only recovery survives SmartList conversion;
the gate compiles the existing TFP adapter for source compatibility only.

## Generated library and customization boundary

`lib/` and `teaql/` are unchanged generation outputs, bound by
`generated-files.json` and `generation-origin.json`. All TeaQL dependencies
resolve to this repository through the example's local Cargo patches; package
version labels do not substitute for local-source verification.

Edit application-owned `src/` and `tests/`, not the generated library. Targeted
model-aware API help is retained in `assist/` at its recorded generation
revision; newer composition guidance does not rewrite that snapshot. For a model change, evaluate and
regenerate the library and Assist together, then replace the complete generated
fixture and its provenance. Do not discover APIs by reading generated source.

This checkout's example exercises SQLite. The generator's controlled
`verify-rust-inheritance-natural-parent.cjs --application runtime-example`
gate also copies these same three application source files unchanged into a
fresh PostgreSQL consumer and executes the full lifecycle twice. This is not
runtime database-provider switching or proof of a published artifact.
