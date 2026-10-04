# Generated Facet Trace example

This small School fixture exercises generated Q APIs with real SQLite, without
handwritten application SQL or generated-source repair. The 24 `lib/src` files
are the exact bytes retained by the producer's School Facet acceptance (issue
teaql-code-gen #251, `RustGeneratedCrateCompileTest`). Only manifest wiring is
portable here; the example always patches runtime dependencies to this repository.

```bash
bash examples/facet-trace/verify.sh
```

Cargo dependencies must already be fetched; the verifier runs offline. Reuse
`CARGO_TARGET_DIR` to avoid recompiling unchanged dependencies. It creates a fresh
retained database, executes the four tests twice without cleanup, checks exact
test identities, and verifies every library input hash before/after.

The tests cover 16 combinations: four root/nested include-all/matched-only
queries; eight typed loaded-relation empty/nonempty include-all combinations;
and four root/loaded future-binding privacy combinations. Every group includes
diagnostic logging on/off. They check inherited intent, canonical physical
paths, real constant identities, Loaded/Empty collection state, retained empty
Facet metadata, no persistent query-metadata fields, and privacy isolation from
the next independent request. `FACET_CASE` records retain actual result sizes.

This is the generated materialization/privacy subset of `TC-SQL-09`, not its
entire count contract or the complete Trace Chain suite. These queries do not
request COUNT aggregates. Current entity/ID Assist did not provide a Facet-count
configuration method (`MISSING_ASSIST`); do not guess one or inspect generated
source to discover it. The separate native SQLite Facet suite is not substituted
for missing generated-count acceptance. No Facet-specific E traversal or new
mutation/audit claim is made here; those belong to the existing Trace Chain
example.

Historical generated source includes trailing spaces and blank EOF lines.
Those bytes are deliberately preserved; formatting applies only to the
application-owned test. Do not report a blanket generated-source formatting pass.
