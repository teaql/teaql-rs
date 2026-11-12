# Natural Checker messages through generated Q and E

Checker returns displayable text and an unchanged machine location. For an
invalid second child, English says `The name of the 2nd entry is required.`;
Chinese says `请填写第2项明细的名称。`. Both retain `/entryList/1/name`.
This generated model uses the generic Chinese classifier `项`.

Run from this runtime repository:

```bash
python3 scripts/verify-natural-messages.py --all-targets --example --output /new/evidence/path
```

The verifier requires the output path to be absent. It checks the native
core/runtime/macro suites and shared message cases, builds the automatic library
against six repository-local crates, then runs this example twice against the same SQLite
database without cleanup between rounds. It retains source and fixture hashes,
commands, raw logs, structured responses and the database, and removes its own
temporary executable/build output on success or failure.

`automatic-fixture/` contains the validated two-entity model, 21 generated files,
passive presentation metadata, source provenance and five retained object/field
Assist responses. The generated factory installs its original Checker and model
descriptors; application code supplies neither replacements nor hand-built
presentation metadata. The earlier Context fixture remains unchanged for that
separate example.

The consumer asserts four structured responses: required English, required
Chinese, a private 101-scalar/102-UTF-16-unit name exceeding the generated bound
of 100, and a safe customer-renderer literal. Rust reports 101 Unicode scalar
values, not Java's UTF-16 count or a recounted mask. The renderer must run once
for its violation. A SQLite statement hook counts executions without retaining
or logging SQL text; rejection must execute no statements, including BEGIN,
and commit no audit.

The required cases supply explicit `Value::Null` through the generated setter.
An empty string is not missing under this model's native Required rule; this
presentation example does not install a custom nonblank rule or change that
validation policy.

The parent is unchanged while a valid first child and invalid second child
participate. Errors retain the parent's graph origin and the second child's
zero-based path. Reverting both edits must produce no Context-only write.
A later corrected save must update one child, advance its version, use the
injected business clock and leave the parent/sibling unchanged. Generated E
checks the fully loaded name and update time; a separate read-only SQLite
oracle checks the final rows after each round.

Only manifests copied into the owned consumer are adapted to local source
versions. Implementation and passive metadata hashes remain unchanged. The
fresh driver ABI must already match; the verifier rejects driver substitution.
Run through the verifier rather than building the frozen candidate manifest
directly against an older published package. The complete repository inventory,
other runtimes and published artifacts remain separate acceptance gates.

Tracking: [teaql-rs #324](https://github.com/teaql/teaql-rs/issues/324),
[conformance #244](https://github.com/teaql/teaql-conformance/issues/244).
