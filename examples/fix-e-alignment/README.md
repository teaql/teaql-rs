# Generated Fix/E and graph-mutation example

`model.xml` owns `rust-lib-core` and `rust-app-console`. The twenty original application files now live in the native workspace source folder with unchanged bytes. Its manifest declares the original test and expression targets, consumes the sibling generated domain and uses this SDK source. The verifier copies those exact scopes into owned scratch and rebases only SDK paths for that disposable location; generated domain manifests and implementations remain unchanged. `showcase.json` records current qualification.


```sh
node examples/fix-e-alignment/verify-local.cjs
```

This compiles the unchanged three-object generated fixture against local runtime
crates. Every application probe runs twice on its retained SQLite database.
The verifier retains compact logs, locks and databases, and removes its owned
build output after all child processes exit. It does not publish packages.

`partial_ancestor_probe.rs` checks both incomplete-leaf rejection and saving a
complete modified leaf through two untouched sparse ancestors. Its managed-root
probe uses a second real Tenant, rejects the same graph twice with the wrong
active root, preserves pending intent and both sparse LoadState snapshots, then
retries with the correct context. Calibrated observers cover SQL statements,
ID allocation, policy lookup/review and safe audit events. Only the child may
change in storage. Business time is injected; fixture creation uses generated
Q/mutation APIs, while independent raw SQL only observes stored rows.

Successful Rust save keeps its established authoritative-root readback behavior:
the returned root has loaded own fields and invalidated affected relation views.
It is not required to preserve the sparse returned-root projection used by other
languages. The same-graph retry and immutable database oracles verify correctness
without changing this approved semantic difference.

## Checker admission boundaries

The normal two-round example also runs the retained pipeline and non-root
completeness probes. They exercise schema-free module installation, explicit
required-null updates, genuine typed customer Fix/min/max checks, isolated
NotLoaded business fields with the root FK loaded, and untouched partial
relations. Business writes use generated Q/E/audited Save. Only the two named
external-writer controls deliberately issue invalid SQL to verify SQLite's
NOT NULL defense; these are not application mutation paths.

`checker_wire_probe.rs` composes the existing public object-local JSON adapter
with generated saves. Three wire naming profiles exercise real accepted aliases,
unknown object-valued fields, equal/unequal canonical-alias collisions in both
submitted orders, and valid controls. It is not a new HTTP/TFP endpoint or a
claim of a generated recursive transport adapter. Invalid input does not reach
mapping/save; invalid saves are measured after the explicit factory ID reservation.

## Context-root human diagnostics

The managed-root case checks both the complete structured location and the
readable default message. Builtin `invalid_type`, missing-root and mismatched-root
messages resolve in English and Chinese, with the existing builtin fallback for
other supported locales. They describe the field and rule without echoing input
or system root values. Native regressions also cover partial customer catalogs,
customer overrides, explicit messages and all three JSON naming profiles.

## Creation identity

`new_entity(&context)` returns `Result<Entity, RuntimeError>`. Use `?` in
application services so allocator failures reach the caller. A successful
return already has a nonzero ID; saving does not allocate it again. Abandoned
entities may leave ID gaps. There is no infallible compatibility alias.

The normal `node examples/fix-e-alignment/verify-local.cjs` gate also runs
`creation_identity.rs` twice: positive allocation, error propagation, zero-ID
rejection, recovery after allocation failure, and independent mutation ledgers
for two creations in one context. It uses the fixture regenerated from the
approved fallible factory template; domain source is not patched by the tests.

## Model and workspace

This showcase uses `model.xml`: Tenant owns PurchaseOrder and OrderItem.
The generated domain library is read-only in `rust-lib-core/`; application code is
in `rust-app-console/src`. The workspace hosts the original scenario checks and
trusted Context/runtime setup.

From the SDK repository root, run:

```bash
node examples/fix-e-alignment/verify-local.cjs
```

The verifier checks the model and exact generated-library hashes, builds the
native workspace with the local SDK, then runs the original business cases
twice on retained stores. Assertions cover real generated Q/E and audited
saves, precise loaded state, rejected writes and transaction outcomes. It
keeps compact receipts and result data and removes owned build output.
`showcase.json` binds this model, both generation scopes and the focused result;
the final full SDK example inventory remains a separate gate.
