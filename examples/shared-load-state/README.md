# Generated shared load state example

This small SQLite example verifies a freshly generated School library against
the runtime source in this repository. It never substitutes published TeaQL
crates and never edits generated library source.

## Run

Requires the local generator checkout, Java/Maven, Cargo, Bash, `rg`, `jq`, and
`sha256sum`. Third-party dependencies may be downloaded on the first run.

```bash
export TEAQL_CODEGEN_DIR=/path/to/teaql-code-gen
export CARGO_TARGET_DIR=/path/to/teaql-rs/target-load-state
bash examples/shared-load-state/verify.sh --generate
```

The producer uses `RustGeneratedCrateCompileTest#generatedRustSharedLoadStateRuntimeExample`.
It generates the library and current Query/Create/Expression/Delete Assist under
`target/generated`, with explicit local dependency paths. To repeat execution
without changing that generated artifact, omit `--generate`.

For the overflow gate, use `bash examples/shared-load-state/verify.sh --generate --wide`.
The local producer appends 130 nullable probe fields to the School, emits the exact
model and fixture mode, and produces 141 fixed slots. The same two-round Q/E/save
flow checks generated slots 0/31/32/63/64/65/129, loaded NULL versus sparse
NotLoaded and independent overflow copy-on-write. `--wide` without generation
rejects a narrow artifact. An existing wide artifact is checked in wide mode
automatically, so replay cannot silently omit its boundary assertions.

Add `--inheritance` during generation to append one Academy subtype with a
campus code. The base model has Platform, SchoolType, School and a small
SchoolCapacitySummary reporting target. The producer
retains a separate inheritance marker; replay activates the inherited test
target automatically and a requested inherited run rejects a flat artifact.
With `--wide --inheritance`, inherited fixed positions, private type identity,
shared snapshots, loaded NULL/zero/false, sparse Checker rejection and audited
create/update/delete pass twice on one database. This gate covers the concrete
typed subtype path, not cross-language physical inheritance-storage migration
or every polymorphic parent-query strategy.

Every verification invocation retains a unique temporary directory. Both test
rounds use the same SQLite file without deleting it. The script records dependency
provenance, per-round logs and generated-source hashes, and rejects a non-local
TeaQL dependency or a generated-source change during application verification.

## Acceptance

`tests/support/nested_graph.rs` also verifies bounded Platform-to-Schools and
School-to-Platform-to-Schools graphs through generated Q/E and borrowed JSON.
Loaded empty lists serialize as arrays; unselected lists stay absent. A filtered
forward target retains the FK but remains NotLoaded; its E negative test catches
the documented fail-fast diagnostic, not ordinary Null. Both rounds require
this graph gate to pass.

Both rounds also decode full and minimal native JSON through
`context.decode_json_entity::<School>(&json)` and check generated E APIs,
loaded NULL versus omitted NotLoaded, dates, zero/false and clean mutation state.
`decode_json_entities` shares actual projection dictionaries and indexed
snapshots across compatible rows without sharing their values or ledgers.
The reader uses installed context metadata and CompactRow, not a Record API or
serde deserialization of internal state. Native property aliases normalize to
the same field; unknown keys, duplicate aliases and incompatible metadata reject.

Typed nested relation graphs are checked separately by the graph helpers.
Persistent `#` input cannot establish trusted storage provenance. Readonly `_`
data requires an actual dynamic-property carrier and receives no fixed indexes.
Decoded JSON is presentation data, not database authority or permission to save.
Load governed data before business mutations and retain full Checker/audit requirements.

`tests/support/dynamic_stream.rs` uses generated Q/E and audited Mutation APIs
with durable extensions in an isolated SQLite file reused across both rounds.
Cursor-local loading preserves Value, Null and NotLoaded at chunk sizes 1, 2,
3 and 73. An early close releases the connection and transaction lease before
an audited save of the held complete entity. Trusted definitions, selection and
storage identity are checked before a cursor opens; the request cannot choose
a storage namespace. Only providers implementing this protocol support dynamic
field streams. Relation/aggregate stream enhancement remains unsupported.

`tests/support/materialization.rs` exercises a bounded two-row Q/E capacity
calculation. `_total_capacity` is a readonly result on the source School: it has
no fixed slot and a native audited save must not persist it. To retain the result,
the example creates the modeled SchoolCapacitySummary through its generated
Mutation API, saves with an audit reason, and queries it back through generated
Q/E. A nonzero total of 37 and contributor count of 2 prevent a missing value
from passing as zero. The reporting test has its own SQLite file (the main
path with `.materialization` appended), reused across both rounds. Keeping it
separate also avoids enlarging the already-large wide-entity async test frame.

- Schema bootstrap is idempotent; constants keep IDs 1001 and 1002.
- Generated field indexes match the installed shared type layout.
- Compatible list rows share the exact immutable snapshot reference; full and
  sparse projections remain distinct.
- Zero and false remain loaded values. Filling an omitted field detaches its
  state without widening a sibling row. A value-only update keeps its snapshot.
- Generated Q and E traverse both single-word and multiword forward relations.
- JSON includes the selected forward graph detail even though it is not embedded
  in the entity struct. A loaded FK without selected detail stays absent from
  the nested output, and presentation leaves the immutable snapshot unchanged.
- The Checker rejects a sparse whole-object update; audited create, update and
  soft delete preserve the independent sibling's value and optimistic version.
- Generated dynamic-field selection and audited save persist a Text value,
  explicit NULL and Delete through the same transaction-backed provider.
- A held dynamic view rejects a changed storage profile before DML, retains
  pending intent, and retries after restoring the original profile with a
  reconstructed provider over the same executor.

This example covers a School graph and optional wide fixed layout, not every
nested graph, broader provider, serialization or performance case. Its SQLite
functional verifier is separate from the allocation probes below.

The runtime's `dynamic_field_query` tests separately cover borrowed non-Clone
graph nodes, cyclic ID/version references, reverse empty versus unselected views,
sibling/filter isolation and nested namespace/private-key boundaries. Broader
generated nested/reverse Q graphs and typed JSON roundtrips remain separate gates.

For a new object with no queried extension view, use the configured database
provider's `definitions_for_context(&context, owner_type)` before preparing its
empty `DynamicFieldValues`. This resolves storage-bound definitions without
reading owner data. Unbound definitions cannot be silently assigned to the
current storage during save.

## State allocation probe

```bash
CARGO_TARGET_DIR="$PWD/target-load-state" bash examples/shared-load-state/verify-allocations.sh
```

This separate release-mode probe counts thread-local allocator calls and gross
requested bytes for warmed state bookkeeping at 4/64/130 fixed fields and
1/100/10,000 operations. It excludes fixture creation, payloads, database I/O and
clock sampling from the counted region. Retained CSV/logs are not a full-query
latency, retained-heap or ORM benchmark. Unchanged dynamic selections are compared
with borrowed codes before any temporary set or prefixed string is constructed.
Use `cargo clippy --manifest-path examples/shared-load-state/Cargo.toml
--all-targets --no-deps -- -D warnings` for application-owned code. Full dependency
linting currently exposes existing generated-template warnings and is not a
passing zero-warning gate.

After generating the wide fixture, add `--wide` to the allocation script. It
also measures the actual generated School decoder at 3/64/full selected fields
and 1/100/10,000 rows, checks exact shared overflow state and COW isolation, and
retains a separate wide hydration CSV plus unchanged generated-source hashes.
Prepared input rows and database/logging costs remain outside the measured region.

## Matched driver logging modes

## Generated query allocation and latency probe

With the existing wide generated fixture, run:

```bash
bash examples/shared-load-state/benchmark-generated.sh --smoke
bash examples/shared-load-state/benchmark-generated.sh
bash examples/shared-load-state/benchmark-generated.sh --default-log
```

Run these sequentially. Five generated Q/E shapes cover sparse fields, all 141
fixed fields, durable dynamic fields, forward references and a bounded reverse
list at 1/100/10,000 School rows. Full sampling uses 31 measurements after five
warmups per shape; `--smoke` uses three. The native test process records p50/p95,
QPS and thread-local allocation calls/requested bytes. Compilation, fixture
setup, validation and result cleanup are outside each timed query. The CPU
receipt covers the test process including setup and validation, not compilation.
These are warmed queries, not process-cold startup or an ORM ranking.

Every run uses a unique database. One complete prototype is created through the
generated audited Mutation API, then cloned with fixture-only SQL before
measurement. This expansion is not a production seeding recipe. Two generated
audited saves establish dynamic Value and NULL; the remaining rows have absent
extensions. Q/E assertions check IDs, versions, dates, zero/false/NotLoaded
semantics, bounded relations, actual shared snapshot identity and clean ledgers.
The measured reverse result retains its parent graph without cloning its child
entities to manufacture the output.

Default-log mode retains runtime policy and formatting, writing to a private
masked log file. Off mode disables steady-state query logging. The runner
rejects ambient log overrides, checks generated-source hashes before/after,
records source/toolchain identity and verifies a single SQLite binding tree.

## Matched driver logging modes

```bash
bash examples/shared-load-state/benchmark-driver.sh
bash examples/shared-load-state/benchmark-driver.sh --default-log
```

Run these sequentially. Each run retains 31 alternating samples for sparse/full
1/100/10,000-row queries, native driver SQL/binds, typed equality and snapshot
sharing. Default mode retains the runtime policy and formatter, writing masked
SQL to an isolated file; off mode disables steady-state query logging. The
initial diagnostic query is not a process-cold measurement. Buffer cleanup and
validation are outside timing. Scripts reject external logging overrides and
plaintext opt-in. Compare only within this workload: Java uses a different
connection/output strategy, and native-driver drift between processes is not
logging overhead. No publication occurs.
