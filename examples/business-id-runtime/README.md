# Business ID focused runtime example

The executable test at `../tests/business_id_runtime.rs` verifies the default
six-character volume-obscuring Base36 profile, a Context-controlled business
date, Domain Root scoping, the byte-identical Java/Rust golden-vector
algorithm, strong `OrderNumber` assignment through a Mutation Ledger adapter,
retry reuse,
explicit SQLite schema setup, concurrent allocation, and restart continuity.

The example installs a deterministic static 32-byte test key. Production
applications provide a versioned `BusinessIdKeyProvider` through the Context;
the key never belongs in generated code or model metadata. Business IDs remain
ordinary identifiers and still require authorization and anti-enumeration.

```bash
cargo test -p teaql-examples --test business_id_runtime
```
