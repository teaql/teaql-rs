# Request intent admission

This small generated order model verifies that query and mutation intent is
required independently of SQL logging. It uses local runtime code, generated Q/E
APIs and real SQLite; it does not install or publish a runtime package.

```sh
node examples/request-admission/verify-local.cjs
```

Run on Linux with Node 24, a C compiler and the Rust toolchain. Dependencies must
already be downloaded for the offline Cargo gate. An optional absolute
`TEAQL_REQUEST_ADMISSION_EVIDENCE_DIR` selects a new evidence directory; otherwise
the script creates and reports one under the system temporary directory.

Each round executes 120 rejected-input cases across logging enabled/disabled,
list, one, exact count, counted list and streaming queries, plus blank audited
saves. Inputs include missing query comment and five empty/Unicode-whitespace
forms. Valid child comments cannot supply a missing root reason. Two rounds use
the same unreset SQLite database.

Valid audited graph saves and queries calibrate the query policy, graph Mutation
Policy, governance registry, SQLite statements, safe audits and metadata callback.
The existing RuntimeTelemetry SPI independently counts actual provider-scope
starts before provider work is invoked; no OTLP service or SDK is required.
Each invalid request must leave all observations, database changes and entity
identities unchanged. Loaded E traversal must issue no SQL. Provider metadata
callbacks measure completed observations, **not provider entry**. Provider-scope
starts measure the runtime boundary, not every internal provider method.

Two separate Cargo checks must fail with the expected `E0599`: an unpurposed
request has no list-execution API, and a bare root cannot save even when it
includes an independently annotated child's pending changes. Positive streaming
controls use supported scalar roots; invalid stream requests retain a child
selection to prove that intent rejection precedes the capability check.

The generated fixture is hash-bound to its evaluated model and producer receipt.
Do not edit it by hand. The verifier retains source inputs, lockfile, commands,
compiler diagnostics, logs and the test database. Its Linux subreaper waits for
owned descendants before deleting only the disposable build directory. Shared
Cargo caches and the evidence database are never cleaned by this script.
