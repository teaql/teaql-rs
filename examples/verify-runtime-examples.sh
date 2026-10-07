#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_dir="$(mktemp -d)"
trap 'rm -rf "$run_dir"' EXIT

run_example() {
  local name="$1"
  local manifest="$2"
  local marker="$3"
  local bin="${4:-}"
  local log="$run_dir/$name.log"

  local command=(cargo run --quiet --manifest-path "$manifest")
  if [[ -n "$bin" ]]; then
    command+=(--bin "$bin")
  fi
  local exit_code=0
  "${command[@]}" >"$log" 2>&1 || exit_code=$?
  if [[ "$exit_code" -ne 0 ]]; then
    printf 'FAIL %s exited %s\n' "$name" "$exit_code" >&2
    sed -n '1,240p' "$log" >&2
    return "$exit_code"
  fi
  if ! grep -Fq "$marker" "$log"; then
    printf 'FAIL %s completed without its acceptance marker\n' "$name" >&2
    sed -n '1,240p' "$log" >&2
    return 1
  fi
  printf 'PASS %s\n' "$name"
}

run_example "conformance" "$repo_dir/examples/conformance/Cargo.toml" \
  "PASS Rust minimum runtime conformance: 8/8"
run_example "security-foundations" "$repo_dir/examples/Cargo.toml" \
  "PASS Rust security foundations: opaque reference is portable and purpose-bound" \
  "security_foundations"
run_example "round-trip-reference-governed" "$repo_dir/examples/Cargo.toml" \
  "PASS Rust round-trip reference governed mode" \
  "round_trip_references"

cargo test --quiet --manifest-path "$repo_dir/examples/Cargo.toml" \
  --test context_bound_order_document
printf 'PASS context-bound Order document round trip\n'

context_document_database="sqlite:file:$run_dir/context_bound_document.sqlite"
context_document_log="$run_dir/context_bound_document.log"
TEAQL_CONTEXT_DOCUMENT_DATABASE="$context_document_database" \
  cargo run --quiet \
    --manifest-path "$repo_dir/examples/order-management/rust-app-console/Cargo.toml" \
    --example context_bound_document_sqlite >"$context_document_log" 2>&1
if ! grep -Eq '^CONTEXT_BOUND_SQLITE_QE_PASS order_id=[0-9]+ update_version=2 delete_version=3 remaining_line_id=[0-9]+$' "$context_document_log"; then
  printf 'FAIL generated Q/E SQLite context-bound document missing acceptance evidence\n' >&2
  sed -n '1,240p' "$context_document_log" >&2
  exit 1
fi
printf 'PASS generated Q/E SQLite context-bound Order document\n'

raw_reference_log="$run_dir/round-trip-reference-raw.log"
TEAQL_REFERENCE_PROFILE=development \
  TEAQL_UNSAFE_EXPOSE_RAW_ENTITY_IDS=I_UNDERSTAND_THIS_EXPOSES_INTERNAL_ENTITY_IDS_FOR_LOCAL_DEBUGGING_ONLY \
  cargo run --quiet --manifest-path "$repo_dir/examples/Cargo.toml" \
  --bin round_trip_references >"$raw_reference_log" 2>&1
if ! grep -Fq 'ERROR: TeaQL raw internal entity reference mode is active for local diagnostics' "$raw_reference_log" \
    || ! grep -Fq 'PASS Rust round-trip reference raw diagnostic mode' "$raw_reference_log"; then
  printf 'FAIL round-trip reference raw diagnostic mode missing downgrade evidence\n' >&2
  sed -n '1,240p' "$raw_reference_log" >&2
  exit 1
fi
printf 'PASS round-trip reference raw diagnostic downgrade\n'

production_reference_log="$run_dir/round-trip-reference-production.log"
if TEAQL_REFERENCE_PROFILE=production \
  TEAQL_UNSAFE_EXPOSE_RAW_ENTITY_IDS=I_UNDERSTAND_THIS_EXPOSES_INTERNAL_ENTITY_IDS_FOR_LOCAL_DEBUGGING_ONLY \
  cargo run --quiet --manifest-path "$repo_dir/examples/Cargo.toml" \
  --bin round_trip_references >"$production_reference_log" 2>&1; then
  printf 'FAIL round-trip reference production accepted raw diagnostic mode\n' >&2
  sed -n '1,240p' "$production_reference_log" >&2
  exit 1
fi
if ! grep -Fq 'ROUND_TRIP_REFERENCE_CONFIGURATION_INVALID' "$production_reference_log"; then
  printf 'FAIL round-trip reference production rejection lacked stable error code\n' >&2
  sed -n '1,240p' "$production_reference_log" >&2
  exit 1
fi
printf 'PASS round-trip reference production fail-closed\n'
run_example "school-management" "$repo_dir/examples/school-management/Cargo.toml" \
  "PASS Rust School bootstrap, ID-set pagination, portable Query, native SQLite Facet, independent ledger isolation, and sparse ledger Checker parity"

school_delete_database="$run_dir/school_soft_delete_return.sqlite"
for attempt in 1 2; do
  school_delete_log="$run_dir/school_soft_delete_return_$attempt.log"
  TEAQL_SCHOOL_DELETE_PROBE_DATABASE="$school_delete_database" \
    cargo run --quiet --manifest-path "$repo_dir/examples/school-management/Cargo.toml" \
    --bin soft_delete_return_probe >"$school_delete_log" 2>&1
  if ! grep -Eq '^SCHOOL_SOFT_DELETE_RETURN_PASS id=[0-9]+ version=-2 transaction_id=[0-9]+ transaction_version=-9 cancelled_id=[0-9]+ transaction_cancelled_id=[0-9]+$' "$school_delete_log"; then
    printf 'FAIL school-management Save return run %s missing persisted tombstone/cancellation result\n' "$attempt" >&2
    sed -n '1,240p' "$school_delete_log" >&2
    exit 1
  fi
done
printf 'PASS school-management Save return (ordinary and transaction tombstones, cancelled root; two runs, same database)\n'

nested_database="sqlite:file:$run_dir/nested_graph_probe.sqlite"
for attempt in 1 2; do
  nested_log="$run_dir/nested_graph_probe_$attempt.log"
  TEAQL_NESTED_PROBE_DATABASE="$nested_database" \
    cargo run --quiet --manifest-path "$repo_dir/examples/order-management/rust-app-console/Cargo.toml" \
    --bin nested_graph_probe >"$nested_log" 2>&1
  if ! grep -Fq 'NEGATIVE:' "$nested_log" || ! grep -Fq 'order_line_list' "$nested_log" \
      || ! grep -Fq 'POSITIVE:' "$nested_log" || ! grep -Fq 'MIXED:' "$nested_log" \
      || ! grep -Fq 'STALE:' "$nested_log" || ! grep -Fq 'HIDDEN_CONFLICT:' "$nested_log" \
      || ! grep -Fq 'CANCELLED:' "$nested_log" \
      || ! grep -Fq 'TRACE_LINEAGE:' "$nested_log"; then
    printf 'FAIL order-management nested graph probe run %s missing acceptance markers\n' "$attempt" >&2
    sed -n '1,240p' "$nested_log" >&2
    exit 1
  fi
done
printf 'PASS order-management nested graph probe (two runs, same database, no cleanup between)\n'

mutation_policy_database="$run_dir/mutation_policy_probe.sqlite"
for attempt in 1 2; do
  mutation_policy_log="$run_dir/mutation_policy_probe_$attempt.log"
  TEAQL_EXAMPLE_DATABASE="$mutation_policy_database" \
    cargo run --quiet --manifest-path "$repo_dir/examples/order-management/rust-app-console/Cargo.toml" \
    --bin mutation_policy_probe >"$mutation_policy_log" 2>&1
  if ! grep -Fq 'MUTATION_POLICY_PASS' "$mutation_policy_log" \
      || ! grep -Fq 'persisted_denied=0' "$mutation_policy_log"; then
    printf 'FAIL order-management mutation policy probe run %s missing acceptance markers\n' "$attempt" >&2
    sed -n '1,240p' "$mutation_policy_log" >&2
    exit 1
  fi
done
printf 'PASS order-management mutation policy (approved identity, denial leaves zero rows; two runs, same database)\n'

for attempt in 1 2; do
  relation_log="$run_dir/save_loaded_relation_$attempt.log"
  if ! TEAQL_SAVE_LOAD_STATE_DATABASE="$nested_database" \
    cargo run --quiet --manifest-path "$repo_dir/examples/order-management/rust-app-console/Cargo.toml" \
    --bin save_loaded_relation_probe >"$relation_log" 2>&1; then
    printf 'FAIL order-management Save relation state run %s exited nonzero\n' "$attempt" >&2
    sed -n '1,240p' "$relation_log" >&2
    exit 1
  fi
  if ! grep -Fq 'SAVE_LOADED_RELATION_PASS' "$relation_log" \
      || ! grep -Fq 'TRANSACTION_SAVE_LOADED_RELATION_PASS' "$relation_log" \
      || ! grep -Fq 'SAVE_CHANGED_RELATION_INVALIDATED' "$relation_log" \
      || ! grep -Fq 'SAVE_EMPTY_RELATION_PASS' "$relation_log" \
      || ! grep -Fq 'SAVE_SCALAR_STATE_PASS' "$relation_log"; then
    printf 'FAIL order-management Save relation state run %s missing acceptance markers\n' "$attempt" >&2
    sed -n '1,240p' "$relation_log" >&2
    exit 1
  fi
done
printf 'PASS order-management Save relation/scalar state (ordinary/transaction Loaded/Null, NotLoaded rejected, changed invalidated; two runs, same database)\n'

for attempt in 1 2; do
  forward_log="$run_dir/save_forward_fk_$attempt.log"
  if ! TEAQL_SAVE_LOAD_STATE_DATABASE="$nested_database" \
    cargo run --quiet --manifest-path "$repo_dir/examples/order-management/rust-app-console/Cargo.toml" \
    --bin save_forward_fk_probe >"$forward_log" 2>&1; then
    printf 'FAIL order-management forward-FK Save run %s exited nonzero\n' "$attempt" >&2
    sed -n '1,240p' "$forward_log" >&2
    exit 1
  fi
  if ! grep -Eq '^SAVE_FORWARD_FK_PASS order_id=[0-9]+ old_customer_id=[0-9]+ new_customer_id=[0-9]+$' "$forward_log"; then
    printf 'FAIL order-management forward-FK Save run %s missing acceptance marker\n' "$attempt" >&2
    sed -n '1,240p' "$forward_log" >&2
    exit 1
  fi
done
printf 'PASS order-management forward-FK Save (old loaded relation invalidated, new FK hydrated; two runs, same database)\n'

bash "$repo_dir/examples/trace-chain/verify.sh"
if [[ -n "${TEAQL_CODEGEN_DIR:-}" ]]; then
  bash "$repo_dir/examples/shared-load-state/verify.sh" --generate
else
  bash "$repo_dir/examples/shared-load-state/verify.sh"
fi
printf 'PASS Rust runtime examples: 15/15\n'
