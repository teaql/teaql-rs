
# Rust minimum runtime conformance example

This retained SQLite workspace is generated from `model.xml`. It verifies
explicit `ensure_schema`, Checker rejection before persistence, Create, typed Q
and `SmartList`, E loaded/null/not-loaded semantics, Update/version, and Delete.

```bash
cargo run
```

The generated Runtime Module is installed when the context is constructed but
remains a passive manifest. Schema reconciliation is invoked separately and
explicitly.
