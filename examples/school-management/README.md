# School Management bootstrap example

This generated example retains `models/school-model.xml`. `cargo run` verifies
explicit SQLite schema creation and idempotent repeated bootstrap of Platform
`id=1` and SchoolType constants `1001`/`1002` against the local runtime.

The SQLite provider's retained runtime test additionally changes one constant,
verifies that its version advances exactly once, and verifies an unchanged
constant remains at version 1.
