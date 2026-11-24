#!/usr/bin/env bash
set -euo pipefail
runtime_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
case "${1:-}" in
  "") export TEAQL_DRIVER_BENCHMARK_LOG_MODE=off ;;
  --default-log) export TEAQL_DRIVER_BENCHMARK_LOG_MODE=default ;;
  *) printf 'Usage: bash benchmark-driver.sh [--default-log]\n' >&2; exit 2 ;;
esac
(( $# <= 1 )) || { printf 'Unexpected arguments\n' >&2; exit 2; }
for setting in TEAQL_SQL_LOG TEAQL_TRACE_MODE TEAQL_TRACE_OFF_ACK TEAQL_LOG_ENDPOINT TEAQL_DOMAIN TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS; do
  if [[ -n "${!setting:-}" ]]; then printf 'Unset %s for an isolated logging benchmark\n' "$setting" >&2; exit 2; fi
done
run_dir="$(mktemp -d -t teaql-rust-driver.XXXXXXXX)"
cd "$runtime_dir"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$runtime_dir/target-load-state}"
# Default policy and formatter are retained; only the destination is isolated.
export TEAQL_LOG_ENDPOINT="$run_dir/runtime-masked.log"
rustc -Vv >"$run_dir/toolchain.txt"
git rev-parse HEAD >"$run_dir/base-commit.txt"
sha256sum teaql-provider-sqlite/tests/driver_query_benchmark.rs Cargo.lock >"$run_dir/source.sha256"
cargo test --release -p teaql-provider-sqlite --test driver_query_benchmark \
  --no-run --message-format=json >"$run_dir/build.jsonl" 2>"$run_dir/build.log"
test_binary="$(jq -r 'select(.reason == "compiler-artifact" and .target.name == "driver_query_benchmark" and .executable != null) | .executable' "$run_dir/build.jsonl")"
[[ -f "$test_binary" && -x "$test_binary" ]]
# Compile excluded; CPU is whole test process, including warmup and assertions.
/usr/bin/time -v -o "$run_dir/cpu.txt" "$test_binary" \
  --ignored --nocapture --test-threads=1 >"$run_dir/results.log" 2>&1
rg -F 'PASS matched typed driver results and shared immutable snapshots' "$run_dir/results.log"
sha256sum --check --quiet "$run_dir/source.sha256"
printf 'Driver benchmark evidence: %s\n' "$run_dir"
