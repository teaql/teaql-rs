# Rust committed-event source example

Run `bash examples/committed-events/verify.sh`. The binary uses local runtime
source and a small SQLite model. `context.ensure_schema()` installs both domain
and event tables. A database trigger changes an inserted value; the normal
audited ledger save stores that authoritative value in one Outbox row before
commit. With the Outbox table unavailable, the next save rolls back its domain
row. The script runs the complete flow twice.

This is a source-capture example, not a completed publisher, target projection,
cross-language codec, or production replication guarantee. Dynamic fields fail
closed while capture is enabled. Update payloads still contain a full modeled
field image rather than the final changed-field-only canonical representation.
