#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_dir="$(mktemp -d)"
trap 'rm -rf "$run_dir"' EXIT

run_example() {
  local name="$1"
  local manifest="$2"
  local marker="$3"
  local log="$run_dir/$name.log"

  cargo run --quiet --manifest-path "$manifest" >"$log" 2>&1
  if ! grep -Fq "$marker" "$log"; then
    printf 'FAIL %s completed without its acceptance marker\n' "$name" >&2
    sed -n '1,240p' "$log" >&2
    return 1
  fi
  printf 'PASS %s\n' "$name"
}

run_example "conformance" "$repo_dir/examples/conformance/Cargo.toml" \
  "PASS Rust minimum runtime conformance: 8/8"
run_example "school-management" "$repo_dir/examples/school-management/Cargo.toml" \
  "PASS Rust School bootstrap, ID-set pagination, portable Query, native SQLite Facet, and sparse ledger Checker parity"

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
      || ! grep -Fq 'CANCELLED:' "$nested_log"; then
    printf 'FAIL order-management nested graph probe run %s missing acceptance markers\n' "$attempt" >&2
    sed -n '1,240p' "$nested_log" >&2
    exit 1
  fi
done
printf 'PASS order-management nested graph probe (two runs, same database, no cleanup between)\n'

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
      || ! grep -Fq 'SAVE_EMPTY_RELATION_PASS' "$relation_log"; then
    printf 'FAIL order-management Save relation state run %s missing acceptance markers\n' "$attempt" >&2
    sed -n '1,240p' "$relation_log" >&2
    exit 1
  fi
done
printf 'PASS order-management Save relation state (ordinary/transaction Loaded, Empty retained, changed invalidated; two runs, same database)\n'

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

printf 'PASS Rust runtime examples: 6/6\n'
