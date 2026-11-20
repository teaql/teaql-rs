# Facet Trace application rules

Edit application-owned workspace code/tests, verification scripts and manifest wiring.
Read `rust-app-console/AGENTS.md` before changing the business application.
`rust-lib-core/lib/src` and the generated workspace's embedded `lib/src` are read-only.
Do not read/search generated implementations for API discovery
or reformat it. Use `cargo teaql --input model.xml rust-assist-query/<entity>` and
required `rust-assist-query/<entity>.<field>` help for new operations; if absent,
report `MISSING_ASSIST` and stop that path rather than invent a method.

All queries are bounded and carry nonblank root comment/purpose. Derived queries
inherit intent; do not repeat it to repair propagation. Schema/bootstrap runs
through context. Verification uses a fresh retained SQLite file and runs twice
without cleanup. Library hashes must remain identical. The root Cargo manifest is a
virtual workspace; library and consumer retain their named scope directories.
