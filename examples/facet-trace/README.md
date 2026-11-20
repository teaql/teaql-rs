# School directory: Facet browsing and membership counts

This example starts with the School/SchoolType/Platform domain in `model.xml`.
The generated `rust-lib-core/` library and `rust-app-console/` workspace come
from that same model. Application-owned code publishes three demonstration
schools through generated creation and audited save APIs, then browses a
one-school page with school-type Facets and full membership counts.

```bash
cargo run --locked --manifest-path examples/facet-trace/rust-app-console/Cargo.toml -- /tmp/school-directory.sqlite
```

The business result has two primary schools and one secondary school. The
one-row page retains counts `PRIMARY=2, SECONDARY=1`; filtering to primary
schools produces `PRIMARY=2`, and include-all choices retain `SECONDARY=0`.
Repeating the command reuses the three records. A rejected blank audit reason
cannot publish a fourth school. No application SQL or hand-written descriptor
is used for these business operations.

The root Cargo manifest only aggregates the two named scope projects. Native
runtime dependencies resolve to this checkout through explicit local wiring.
Generated implementations remain byte-identical to the recorded producer.

The complete verifier runs the business workspace twice on one fresh retained
business database and preserves the original nine integration tests on their
separate retained fixture database:

```bash
bash examples/facet-trace/verify.sh
```

Cargo dependencies must already be fetched; the verifier runs offline. Reuse
`CARGO_TARGET_DIR` to avoid recompiling unchanged dependencies. It creates a fresh
retained database, executes nine tests twice without cleanup, checks exact
test identities, and verifies every library input hash before/after. Set
`TEAQL_FACET_EVIDENCE_DIR` to an unused output directory for compact receipts.
An automatically owned build directory is removed on exit; a caller-supplied
`CARGO_TARGET_DIR` stays under that caller's lifecycle.

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

The response test wraps one bounded, minimally selected entity through the
single-entity, entity-collection and SmartList Core WebResponse constructors.
Responses reuse `Entity::into_json()`: unselected fields stay absent, and
mutation comments, dirty fields and original editing values do not become
application data. The runtime macro also respects older shared-column load
states; no generated-library repair is required. First-level Facet values and
an explicitly supplied total count survive wrapping. The supplied count is an
adapter calibration, not a claim that the query executed COUNT. Nested Facet
response metadata remains a separate open contract.

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
