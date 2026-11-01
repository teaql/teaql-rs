# Generated E diagnostics under model JSON naming profiles

Run from the runtime repository with the local Rust toolchain and Node:

```sh
node examples/expression-profiles/verify-local.cjs
```

This example compiles three evaluated generated domain libraries. The models
only differ by their `json_field_naming` option: default `camelCase`,
`snake_case`, or `PascalCase`. Generated files are unmodified fixtures, with
their producer identity and hashes in `provenance.json` and the manifest.

For each model the same application initializes SQLite through the generated
context, runs a bounded generated Q query, then traverses an unloaded reverse
list through the generated E API. The native error must preserve its canonical
KSML location, the complete attempted expression and the first `<broken>`
node. Its default machine DTO must use the model's JSON profile without the
application overriding the diagnostic profile. A calibrated SQLite observer
checks that E issues no SQL; the already-loaded name remains unchanged.

Each model runs twice against the same database without resetting it. The
runner checks all six local runtime crate origins, hashes fixtures and source,
and removes its owned build scratch after all children exit. It retains small
logs, the dependency lock, results and three test databases. To choose a new
evidence directory, set `TEAQL_GENERATED_E_PROFILE_EVIDENCE_DIR` to an unused
absolute path. Existing evidence is never overwritten.

This is a local-source E diagnostic example, not a published-package test,
an audited-save example, or full acceptance of every E operation. The related
native runtime test owns literal punctuation and JSON-pointer escaping; KSML
field names in this example remain legal multiword identifiers.
