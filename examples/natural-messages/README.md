# Natural Checker messages through generated Q and E

Checker returns displayable text and an unchanged machine location. For an
invalid second child, English says `The name of the 2nd entry is required.`;
Chinese says `请填写第2条明细的名称。`. Both retain `/entryList/1/name`.

Run from this runtime repository:

```bash
python3 scripts/verify-natural-messages.py --all-targets --example --output /new/evidence/path
```

The verifier requires the output path to be absent. It checks the native
core/runtime/macro suites and shared message cases, builds against six
repository-local crates, then runs this example twice against the same SQLite
database without cleanup between rounds. It retains source and fixture hashes,
commands, raw logs, structured responses and the database, and removes its own
temporary executable/build output on success or failure.

This example reuses the complete, unchanged two-entity generated library from
`../context-derived/fixture/`, with its original manifest. Application-owned
Checker rules require a nonempty name, limit it to eight Unicode scalar values,
and exercise the model-masked token on a failing probe. All writes use generated
audited Save. Bounded Q loads the two-child graph; E verifies the stored name.
Failed saves must leave SQLite's change counter and committed audit count
unchanged. A corrected save updates only the child and increments its version.
The rejected graph contains a valid first pending edit and an invalid second
edit. Before the corrected save, the first edit is restored. Positions describe
the participating validation graph, not a promised index in an earlier query.

The runtime receives explicit immutable presentation metadata for Catalog and
Entry. Automatic generator emission, the entire repository example inventory,
other runtimes and published-package acceptance are separate gates. String
length messages preserve Rust's existing Unicode-scalar measurement; they do
not report the mask's length or change validation to Java's UTF-16 measurement.

Tracking: [teaql-rs #324](https://github.com/teaql/teaql-rs/issues/324),
[conformance #244](https://github.com/teaql/teaql-conformance/issues/244).
