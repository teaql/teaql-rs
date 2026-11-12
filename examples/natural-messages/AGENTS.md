# Natural Checker message example

Use `python3 scripts/verify-natural-messages.py --all-targets --example --output
/new/evidence/path` from the runtime repository. The output path must not exist.

The current model, unedited generated library, passive metadata and retained
model-aware Assist belong to `automatic-fixture/`. Read the application source
and only the required `automatic-fixture/assist/` object/action or field help;
do not read generated implementation to discover APIs. Use `model.xml` there
for a fresh Assist request if an operation is missing. Never install hand-built
presentation metadata or a replacement Checker in this application.

Only the verifier's owned consumer manifests may bind candidate dependency
versions to the repository-local crates. Generated implementation/metadata must
retain their original hashes, and fresh generated SQLite driver coordinates
must already match the runtime. Normal verification uses the committed lock;
after a deliberate dependency update, use the explicit preparation command:

```bash
python3 scripts/natural_message_consumer.py --prepare-lock --output /new/lock-evidence
```

Lock preparation is not runtime acceptance. Do not remove a lock or downgrade
the driver to make a failed compilation pass. The old Context fixture remains
unchanged at `../context-derived/fixture/` for its own separate example.

Keep queries bounded with nonempty comment and purpose. Every write uses the
generated audited-save API. Preserve complete-load and Context Fix behavior;
messages must not alter validation or create a new mutation. Never print a
private name. The verifier retains compact logs, response JSON and one SQLite
database used twice; it removes only its own build tree after children exit.
