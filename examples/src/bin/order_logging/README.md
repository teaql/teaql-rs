# Shipping order changes in human and JSON logs

The dispatch team creates a shipping order with one returnable case line,
updates its handling instructions, and checks that a refused carrier override
leaves the committed amendment intact. `model.xml` defines the order and line
and masks the customer reference. The generated `rust-app-console` uses the
immutable `rust-lib-core` and an application-owned safe audit sink on the
generated Context.

Run `bash examples/src/bin/order_logging/verify.sh` from the runtime repository.
The two modes run against fresh SQLite stores. The ordinary files show the query
and mutation intent, result counts, masked SQL and relation trace. The committed
audit sink and SQLite results verify that only the accepted changes persisted.
The original `test_default_log` formatter probe runs through its retained Cargo
target after the shipping order runs.
