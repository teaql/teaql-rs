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
campus code. The normal three-object model remains unchanged. The producer
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
