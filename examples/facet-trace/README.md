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
retained database, executes eight tests twice without cleanup, checks exact
test identities, and verifies every library input hash before/after.

The original four tests cover 16 combinations: four root/nested include-all/matched-only
queries; eight typed loaded-relation empty/nonempty include-all combinations;
and four root/loaded future-binding privacy combinations. Every group includes
diagnostic logging on/off. They check inherited intent, canonical physical
paths, real constant identities, Loaded/Empty collection state, retained empty
Facet metadata, no persistent query-metadata fields, and privacy isolation from
the next independent request. `FACET_CASE` records retain actual result sizes.

The four counted-Facet tests add 22 combinations: root membership counts with
and without the active filter, nested counts through an empty root, loaded typed
relations with empty/nonempty targets, and a sensitive binding used only in a
future count query. Counts use the full matching set even when the visible page
has one row. Every emitted statement must retain the original root and exact
relation ancestry; an empty target skips aggregate SQL instead of fabricating a
trace. Logging off still verifies real results, with no diagnostic SQL entries.
`COUNTED_FACET` records contain actual counts, statement paths and intent, and
privacy isolation from the next independent request.

Count methods are discovered through reverse-relation field Assist, for example
`rust-assist-query/platform.school_type_list` and
`rust-assist-query/school_type.school_list`. Configure
`count_school_types_with("typeCount", child_request)` on the Platform Facet
target. Ordinary `count_as` aggregates the current query; it is not a replacement
for Facet membership counting. This documentation gap is repaired in the local
producer branch; deployed Assist is a separate gate.

These are generated materialization/count/privacy cases for `TC-SQL-09`, not
the complete numbered Trace Chain suite. No Facet-specific E traversal or new
mutation/audit claim is made here; those belong to the existing Trace Chain
example.

Historical generated source includes trailing spaces and blank EOF lines.
Those bytes are deliberately preserved; formatting applies only to the
application-owned test. Do not report a blanket generated-source formatting pass.
