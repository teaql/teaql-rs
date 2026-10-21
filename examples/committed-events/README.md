# Rust committed-event source and local target example

Run `bash examples/committed-events/verify.sh`. The binary uses local runtime
source and a small SQLite model. `context.ensure_schema()` installs both domain
and event tables. A database trigger changes an inserted value; the normal
audited ledger save stores that authoritative value in one Outbox row before
commit. A bounded publisher delivers that row to a separate SQLite target
through model-checked application and advances its durable source cursor only
after the target acknowledges it. With the source Outbox table unavailable,
the next save rolls back its domain row. The script runs the complete flow twice.

The same binary also uses a separate file-backed source with two ordered
events. It loses the first target acknowledgement after target commit, confirms
that the source cursor stays at zero, reopens the source, then accepts the
target's duplicate acknowledgement before applying sequence 2. This recovery
flow runs in both verifier rounds and in the installed-artifact consumer.

This is an **in-process** delivery example, not authenticated network transport,
complete cross-language codec, snapshot-plus-tail, or a production replication
guarantee. Dynamic fields fail closed while capture is enabled. Updates carry
changed modeled fields; provider tests add adversarial cases around ambiguous
acknowledgement and restart recovery.

The verifier also runs the application-owned School example twice on separate
SQLite source/target files. It uses generated audited Save and Q APIs for
replicated readback, and checks that generated-library source hashes do not
change. Both examples use local runtime source; public-package qualification
is separate.
