# Known persisted value restoration

Run from the repository root (Linux, Rust, Node.js and a C compiler):

```sh
node examples/tests/loaded-revert/verify-local.cjs
```

This offline example always links all six TeaQL crates to this repository,
never to a released package. The evaluated two-object scalar model, immutable
generated library, current model-specific Assist, producer hashes and authored
Q/E/save probe are retained here. Discover APIs through `assist/`; do not inspect
or hand-edit generated library source. `provenance.json` detects fixture edits.

Two consecutive rounds share one retained SQLite database without cleanup:

- Loaded value and loaded null changed and restored cause no SQL statements,
  policy review, mutation audit or optimistic version bump.
- Loaded relation detail survives the no-op. An unchanged partial view is valid;
  a genuinely edited partial view is still rejected.
- A malformed native-member ledger alias cannot disappear as an equal no-op.
- A different real edit saves once, with model-owned Fix behavior. A separate
  pending root in the same context remains untouched.

The probe also checks SQLite directly, independently of generated E access.
Known null is loaded and `try_eval()` returns `Ok(None)`; it is not NotLoaded.
Raw ledger `set` keeps input until model admission, where only known persisted
native fields may be normalized. NEW objects have no persisted baseline.

Set `TEAQL_RESTORATION_EVIDENCE_DIR` to a new absolute directory to choose where
compact commands, hashes, raw logs and the test database are retained. Owned
compiler outputs are removed on success or failure after descendant processes
exit; shared download/build caches and services are untouched. A passing example
is not complete example-inventory or published-artifact qualification.
