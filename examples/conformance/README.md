
# Rust minimum runtime conformance example

This retained SQLite workspace is generated from `model.xml`. It verifies
explicit `ensure_schema`, Checker rejection before persistence, Create, typed Q
and `SmartList`, E loaded/null/not-loaded semantics, Update/version, and Delete.
It also proves that optimistic versions remain isolated when different entity
types use the same numeric ID and their mutation ledgers are composed.

```bash
examples/verify-runtime-examples.sh
```

Run the command from the repository root. Both retained examples use current
path-patched runtime sources and isolated temporary SQLite databases.

The generated Runtime Module is installed when the context is constructed but
remains a passive manifest. Schema reconciliation is invoked separately and
explicitly.
