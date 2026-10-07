#!/usr/bin/env bash
set -euo pipefail
runtime_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_dir="$(mktemp -d -t teaql-rust-driver.XXXXXXXX)"
cd "$runtime_dir"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$runtime_dir/target-load-state}"
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
