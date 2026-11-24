# Audited school enrollment

Riverside Primary registers its first pupils in one audited enrollment graph.
The school system explicitly provisions the district platform and school types.
It reopens the same database without duplicating seed or enrollment records.

`model.xml` declares Platform, SchoolType, School and Student. The
`rust-lib-core` contains the output of Generator 4.3.0 for this model.
`showcase.json` records the current library checksum and the initial installation receipts.
`rust-app-console` configures a trusted audit sink on the generated Context,
saves the school and both pupils as one graph, and reads the roster through
generated Q. A missing required name fails without adding a pupil or emitting a
committed audit. The handwritten relation behavior remains as a calibration.

Run `bash examples/src/bin/school_enrollment/verify.sh` from the Rust repository.
The verifier checks the local runtime origin and reads the SQLite database
independently. It retains compact logs and SQLite results at
`TEAQL_RUST_ENROLLMENT_EVIDENCE_DIR` when set.
