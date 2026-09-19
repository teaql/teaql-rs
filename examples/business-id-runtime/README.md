# Business ID focused runtime example

The executable test at `../tests/business_id_runtime.rs` verifies the default
eight-digit daily sequence, a Context-controlled business date, tenant scoping,
strong `OrderNumber` assignment through a Mutation Ledger adapter, retry reuse,
explicit SQLite schema setup, concurrent allocation, and restart continuity.

```bash
cargo test -p teaql-examples --test business_id_runtime
```
