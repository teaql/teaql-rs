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

## Consistent snapshot plus tail (CMR-11)

Run `bash examples/committed-events/verify-snapshot.sh`. It runs the current
local `teaql-provider-sqlite` example twice offline, overrides any shared target
with an owned temporary target and removes those build outputs on exit. The
complete verifier also invokes this gate.

The small in-memory fixture calls Context `ensure_schema`, then simulates two
old orders with no captured history: a live row and a retained tombstone, with
an ID above JavaScript's exact-integer range, negative epoch time, Boolean/NULL,
and an empty entity catalog entry. Fixture SQL simulates one committed tail;
normal application audited-save capture is shown by `committed_events` instead.

The snapshot at N=0 plus tail 1 matches the source; retries do not rewind the
current target. Set `TEAQL_SNAPSHOT_EXPORT_DIR` on a single example invocation
to create `snapshot.json`, `tail.json` and SHA-256 sidecars. A separate invocation
with `TEAQL_SNAPSHOT_IMPORT_DIR` consumes those files, including Java exports.
The two roles are mutually exclusive and export files are create-new only.
Use fresh task-owned export directories and retain compact evidence explicitly.
The files are not an authenticated transport, and this does not qualify
process-crash snapshot recovery, production scale or public artifacts.

## Managed bootstrap process recovery (CMR-12 candidate)

`bash examples/committed-events/verify-bootstrap-crash.sh` builds the local provider
example offline in a fresh owned target, then executes that same binary twice.
Each invocation keeps one source database through two rounds (snapshot/tail
1/2, then 3/4). The second target is a new consumer slot, not a reset database;
the first then catches up to 4 without replacing its original snapshot. Four
source changes use the audited ledger API; one legacy tombstone predates capture.

In each round the owner SIGKILLs and reaps seven workers: snapshot read after N,
uncommitted publication retention, partial target installation, committed target
before snapshot receipt, source cursor insert before ACK commit, partial tail
application, and committed tail before event receipt. Fresh read-only connections
record actual state after every exit. A negative control rejects ordinary exit
without a PID/phase barrier. All workers/logs have 30-second/256KiB bounds.
Metadata and SQLite trigger barriers are test-only, not runtime fault hooks.

`TEAQL_CMR12_EVIDENCE_ROOT` optionally selects a **nonexistent absolute path**
for compact DBs, child logs, barriers, result JSON, binary hash and Cargo origins.
Otherwise successful disposable fixtures are removed after owned children exit;
failed fixtures print a retained diagnostic path. The verifier stops/reaps only
its own process groups and removes its own build target on exit. It never cleans
a shared Cargo cache/target. This is controlled Linux/SQLite source proof, not
power loss, installed-package, authenticated transport or full CMR-12 acceptance.
