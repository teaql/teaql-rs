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

The SQLite provider's retained runtime test additionally changes one constant,
verifies that its version advances exactly once, and verifies an unchanged
constant remains at version 1.

From the repository root, `examples/verify-runtime-examples.sh` runs this and
the minimum conformance example against current path-patched runtime sources.
Its four acceptance groups must all pass before a local runtime change is
called example-verified.
