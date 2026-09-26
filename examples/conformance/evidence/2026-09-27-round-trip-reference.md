# Rust context-bound Round-Trip Reference core evidence

Date: 2026-09-27

Tracking: [teaql-rs#209](https://github.com/teaql/teaql-rs/issues/209)

Baseline: `origin/main` at `f5e01ec1990164741dafa5f540381463b9571370`.

Scope: the bounded `tqr1` independent-reference core. A complete encrypted
document manifest and generated `openDocument` / `acceptDocument` adapters are
not claimed by this evidence.

## Implemented boundary

- high-level `UserContext::reference_for` / `resolve_reference` boundary;
- trusted Actor realm/subject and Domain Root identity supplied at runtime
  assembly, never accepted from the wire;
- service, deployment environment, document ID/purpose, Aggregate
  type/identity/revision and entity type/identity/version binding;
- AES-256-GCM, 96-bit CSPRNG nonce, HKDF-SHA-256 and unpadded Base64URL;
- current encryption key plus decode-only previous keys;
- expiry, environment isolation, restart continuity with the same key ring,
  fresh-nonce unlinkability and a retained deterministic golden vector;
- current authorization check on both issue and consume;
- exact development/test-only raw-ID acknowledgement, with production
  fail-closed behavior and safe downgrade metadata;
- legacy per-call raw fallback removed. Reference mode is fixed when the
  process-level runtime is assembled.

## RAW-01 through RAW-09

| Case | Deterministic evidence |
| --- | --- |
| RAW-01 | `raw_01_unset_uses_governed_references` |
| RAW-02 | `raw_02_exact_acknowledgement_enables_development_raw_mode` |
| RAW-03 | `raw_03_exact_acknowledgement_enables_test_raw_mode` |
| RAW-04 | `raw_04_near_match_does_not_enable_raw_mode` |
| RAW-05 | Unit test plus production subprocess in `verify-runtime-examples.sh` |
| RAW-06 | `raw_06_reference_from_another_actor_is_still_rejected_by_current_authorization` |
| RAW-07 | `raw_07_mode_change_rejects_old_wire_shape` |
| RAW-08 | `raw_08_retains_version_type_and_authorization_guards`; raw mode is confined to wire representation, while the unchanged full runtime suite verifies projection, version, Checker/Fix, audit and Mutation Ledger enforcement |
| RAW-09 | `raw_09_exposes_safe_downgrade_metadata_without_secret_material` plus development subprocess checking the ERROR startup warning |

## Commands and results

```text
cargo test -p teaql-runtime round_trip_reference -- --nocapture
  17 passed; 0 failed

cargo clippy --all-targets --all-features -- -D warnings
  exit 0

cargo fmt --all -- --check
./scripts/test-release-provenance-contract.sh
./scripts/test-publish-contract.sh
./scripts/test-pin-rust-order-replay-version.sh
./scripts/test-pin-rust-tfp-replay-version.sh
cargo build --all-targets --all-features
cargo test --all-targets --all-features
  all exit 0; one Redis integration test ignored because TEAQL_REDIS_URL was not supplied

./examples/verify-runtime-examples.sh
  PASS Rust runtime examples: 10/10
```

The example verifier exercises governed mode, development raw mode, production
failure, and all retained School/Order graph-save examples. It is run twice in
the acceptance procedure without repository cleanup between runs.

No crate version, release tag or external artifact is produced by this work.

## Follow-on test work

The optional deterministic randomized Mutation Ledger work started under
[teaql-rs#214](https://github.com/teaql/teaql-rs/issues/214). Its first retained
test executes 2,048 seed-replayable mixed ledger operations and checks typed
identity, field replacement, new/delete sets, optimistic versions and committed
state cleanup after every operation. The issue remains separate from the
Round-Trip Reference feature and does not change production APIs.
