# Number two procurement orders

`model.xml` defines the purchase order and its `ORD` Business ID.
`rust-lib-core/` contains the generated domain library. The console at
`rust-app-console/src/main.rs` installs the trusted business clock and SQLite allocator,
records two orders, retries the first, and reads both back by number.
It rejects a blank audit reason and verifies the denied order is absent.

The original four allocator, Root, and Ledger calibration tests live under
`rust-app-console/calibration/`. Their `teaql-examples` test target is retained.
The focused gate runs both the calibration and two workspace processes on one
SQLite file.
