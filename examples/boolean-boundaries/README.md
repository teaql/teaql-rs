# Typed Boolean boundaries

Run `node examples/boolean-boundaries/verify-local.cjs` from this repository.
The verifier uses the local runtime, the existing evaluated scalar fixture and
its unchanged generated library. No public package or generator deployment is
required. The application uses generated Q/E and audited save; direct SQLite
access is limited to physical assertions and a restored corrupt-storage negative.
Both `scripts/verify-examples.sh` and `examples/verify-runtime-examples.sh` run
this example. Full-inventory source copies must set
`TEAQL_RUST_EXAMPLE_SOURCE_ROOT` to their real Git origin; copy and origin hashes
must match before and after execution.

Two runs share one retained SQLite database. Each creates and updates true/false,
clears a nullable Boolean, checks generated true/false/unknown predicates and
distinguishes loaded null from NotLoaded. Eight invalid post-Fix create/update
attempts must stop before provider entry without changing pending ledger intent.
A stored integer `2` must be rejected rather than fabricated as `true`.

Generated query method names come from the model-aware field Assist for
`scalar_type_probe.optional_boolean`; E/create entry points and the complete
typed fixture preparation come from `../tests/loaded-revert/assist/` and its
application-owned `rust-integral-generated.rs`. The verifier checks the original
fixture provenance and copies those bytes; it never edits generated source.

Build outputs are task-owned and removed after children exit. The compact report,
commands, input hashes and database are retained in the configured
`TEAQL_BOOLEAN_EVIDENCE_DIR`, or a freshly printed temporary evidence directory.
Passing this focused example does not qualify the complete example inventory or
an installed private SDK.
