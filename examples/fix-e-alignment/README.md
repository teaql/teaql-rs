# Generated Fix/E and graph-mutation example

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
