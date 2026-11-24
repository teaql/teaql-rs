# Rust Mutation Ledger randomized-invariant evidence

Date: 2026-09-27

Tracking: [teaql-rs#214](https://github.com/teaql/teaql-rs/issues/214)

Baseline: `origin/main` at `2047f49e2b0a2d64f5e2e250cdb4533c7dc4ff5c`.

Scope: deterministic, seed-replayable test deepening only. No production API,
runtime behavior, crate version, release tag, or external artifact is changed.

## Retained invariant suites

1. Eight fixed seeds execute 2,048 mixed operations each across three
   independent ledgers. After every operation, a separate model checks exact
   field mutations, typed `(entity_type, id)` identity, new/delete intent,
   optimistic versions, commit cleanup, and isolation from the other roots.
2. One fixed seed executes 4,096 nested change-set operations. A separate stack
   model checks latest-value lookup, stack rollback, and current-set clearing.
3. 512 deterministic graph-composition trials each generate 32 mutations.
   Equal versions compose without sharing state; conflicting versions reject
   atomically without partially copying fields, identities, or versions.
4. Existing real save/planning tests remain the authoritative checks for
   create-then-delete cancellation, existing-entity deletion, failed-save
   retention, and successful-commit cleanup.

The random input is not obtained from the operating system. Every failure
message includes the fixed seed, step or trial, and root where applicable, so
the exact sequence can be replayed locally and in CI.

## Focused commands and results

```text
cargo test -p teaql-runtime \
  entity_runtime::composition_api_tests::deterministic_randomized_composition_is_atomic_and_seed_replayable \
  --lib
  1 passed; 0 failed

cargo test -p teaql-runtime --test test_incremental_ledger
  4 passed; 0 failed
```

Full formatting, Clippy, all-target/all-feature tests, and runtime-example
verification completed successfully:

```text
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
./examples/verify-runtime-examples.sh
  PASS Rust runtime examples: 10/10
```

The Redis integration test remains intentionally ignored unless
`TEAQL_REDIS_URL` is supplied. No other test is skipped because of this change.
