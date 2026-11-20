# Edit an order through a scoped line reference

The [model](model.xml) defines a merchant, a sales order and its editable line.
The Rust workspace installs the Context reference runtime, resolves an actual
SQLite row and saves an audited quantity change. It rejects changed tokens,
wrong scopes, changed permission and a replay against an outdated line version.

```bash
bash examples/src/bin/order-edit-reference/verify.sh
```

The original executable remains a calibration for governed and local diagnostic
presentations. The workspace also runs the original runtime reference tests.
