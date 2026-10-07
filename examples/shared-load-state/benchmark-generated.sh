#!/usr/bin/env bash
set -euo pipefail
samples=31
mode=off
for option in "$@"; do
  case "$option" in
    --smoke) samples=3 ;;
    --default-log) mode=default ;;
    *) printf 'Usage: bash benchmark-generated.sh [--smoke] [--default-log]\n' >&2; exit 2 ;;
  esac
done
for setting in TEAQL_SQL_LOG TEAQL_TRACE_MODE TEAQL_TRACE_OFF_ACK TEAQL_LOG_ENDPOINT TEAQL_DOMAIN TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS; do
  if [[ -n "${!setting:-}" ]]; then printf 'Unset %s for an isolated logging benchmark\n' "$setting" >&2; exit 2; fi
done
runtime_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_dir="$(mktemp -d -t teaql-rust-generated-query.XXXXXXXX)"
printf 'Generated query evidence: %s\n' "$run_dir"
cd "$runtime_dir"
git diff HEAD --exit-code -- examples/shared-load-state teaql-core teaql-macros teaql-runtime teaql-sql teaql-provider-sqlite
git rev-parse HEAD >"$run_dir/base-commit.txt"
rustc -Vv >"$run_dir/toolchain.txt"
sha256sum examples/shared-load-state/tests/generated_query_benchmark.rs examples/shared-load-state/Cargo.toml examples/shared-load-state/Cargo.lock teaql-runtime/tests/support/allocation_counter.rs >"$run_dir/source.sha256"
rg --files --hidden --no-ignore examples/shared-load-state/target/generated/lib/src -0 |
  sort -z | xargs -0 sha256sum >"$run_dir/generated-source.sha256"
cargo tree --manifest-path examples/shared-load-state/Cargo.toml --locked -i libsqlite3-sys >"$run_dir/sqlite-binding.txt"
cargo test --manifest-path examples/shared-load-state/Cargo.toml --release --locked \
  --test generated_query_benchmark --no-run --message-format=json \
  >"$run_dir/build.jsonl" 2>"$run_dir/build.log"
test_binary="$(jq -r 'select(.reason == "compiler-artifact" and .target.name == "generated_query_benchmark" and .executable != null) | .executable' "$run_dir/build.jsonl")"
[[ -f "$test_binary" && -x "$test_binary" ]]
TEAQL_GENERATED_SAMPLES="$samples" TEAQL_GENERATED_DATABASE="$run_dir/school.sqlite" \
  TEAQL_GENERATED_LOG_MODE="$mode" TEAQL_LOG_ENDPOINT="$run_dir/runtime-masked.log" \
  /usr/bin/time -v -o "$run_dir/cpu.txt" "$test_binary" --ignored --nocapture --test-threads=1 >"$run_dir/results.log" 2>&1
rg -F 'PASS generated Rust wide dynamic forward reverse Q/E sharing benchmark' "$run_dir/results.log"
sha256sum --check --quiet "$run_dir/source.sha256"
sha256sum --check --quiet "$run_dir/generated-source.sha256"
