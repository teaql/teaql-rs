# Facet Trace application rules

Edit only application-owned tests, verification scripts and manifest wiring.
`lib/src` is a read-only generated fixture. Do not read/search it for API discovery
or reformat it. Use `cargo teaql --input model.xml rust-assist-query/<entity>` and
required `rust-assist-query/<entity>.<field>` help for new operations; if absent,
report `MISSING_ASSIST` and stop that path rather than invent a method.

All queries are bounded and carry nonblank root comment/purpose. Derived queries
inherit intent; do not repeat it to repair propagation. Schema/bootstrap runs
through context. Verification uses a fresh retained SQLite file and runs twice
without cleanup. Library hashes must remain identical.
