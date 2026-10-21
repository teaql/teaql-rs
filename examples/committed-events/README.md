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

## Real source-process crash (CMR-03)

Run `bash examples/committed-events/verify-process-crash.sh`, or use the complete
verifier above. The dedicated small binary spawns its own seed/recover/verify
workers. A seed performs the audited ledger save, signals a durable post-commit,
pre-publish barrier and waits without dropping the Context. Its owning parent
kills and reaps it (SIGKILL on Unix); a fresh recovery process delivers its
Outbox row, exercises lost acknowledgement/duplicate handling and persists the
source cursor. Another new process checks the target and duplicate receipt.

Both rounds share the same source/target DBs: counts/cursors advance one then
two. The business-invocation file remains one entry per writer. A normally
exited worker without a barrier is a negative control, not successful recovery.
The parent owns/reaps every child, bounds waits and logs, and never targets an
unrelated process. This is a test-only worker CLI, not a remote runtime API.

`TEAQL_CMR03_EVIDENCE_DIR` chooses a fresh retained evidence directory; without
it the binary prints an owned temporary path. Preserve its compact DBs/logs,
barriers and `result.json`. Never erase old data to rerun a gate. Set a compatible
`CARGO_TARGET_DIR` for native builds; one-off callers clean only their own build
outputs after recording results. No power-failure, snapshot-crash, network,
production, full CMR-12 or installed-public-package qualification is claimed.
