# Rust committed-event source and local target example

Run `bash examples/committed-events/verify.sh`. The binary uses local runtime
source and a small SQLite model. `context.ensure_schema()` installs both domain
and event tables. A database trigger changes an inserted value; the normal
audited ledger save stores that authoritative value in one Outbox row before
commit. A bounded publisher delivers that row to a separate SQLite target
through model-checked application and advances its durable source cursor only
after the target acknowledges it. With the source Outbox table unavailable,
the next save rolls back its domain row. The script runs the complete flow twice.

This is an **in-process** delivery example, not authenticated network transport,
complete cross-language codec, snapshot-plus-tail, or a production replication
guarantee. Dynamic fields fail closed while capture is enabled. Updates carry
changed modeled fields; the companion provider test covers ambiguous
acknowledgement and restart recovery.
