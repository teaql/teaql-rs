#!/usr/bin/env bash
set -euo pipefail
mode=off
case "${1:-}" in
  '') ;;
  --default-log) mode=default ;;
  *) printf 'Usage: bash benchmark-cold.sh [--default-log]\n' >&2; exit 2 ;;
esac
(( $# <= 1 )) || exit 2
for setting in TEAQL_SQL_LOG TEAQL_TRACE_MODE TEAQL_TRACE_OFF_ACK TEAQL_LOG_ENDPOINT TEAQL_DOMAIN TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS; do
  if [[ -n "${!setting:-}" ]]; then printf 'Unset %s for an isolated logging benchmark\n' "$setting" >&2; exit 2; fi
done
runtime_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_dir="$(mktemp -d -t teaql-rust-cold-query.XXXXXXXX)"
printf 'Cold query evidence: %s\n' "$run_dir"
cd "$runtime_dir"
git diff HEAD --exit-code -- examples/shared-load-state teaql-core teaql-runtime teaql-sql teaql-provider-sqlite
git rev-parse HEAD >"$run_dir/base-commit.txt"
rustc -Vv >"$run_dir/toolchain.txt"
sha256sum examples/shared-load-state/examples/cold_query_probe.rs examples/shared-load-state/Cargo.toml examples/shared-load-state/Cargo.lock teaql-runtime/tests/support/allocation_counter.rs >"$run_dir/source.sha256"
rg --files --hidden --no-ignore examples/shared-load-state/target/generated/lib/src -0 |
  sort -z | xargs -0 sha256sum >"$run_dir/generated-source.sha256"
cargo build --manifest-path examples/shared-load-state/Cargo.toml --release --locked --example cold_query_probe --message-format=json >"$run_dir/build.jsonl" 2>"$run_dir/build.log"
probe="$(jq -r 'select(.reason == "compiler-artifact" and .target.name == "cold_query_probe" and .executable != null) | .executable' "$run_dir/build.jsonl")"
[[ -x "$probe" ]]
TEAQL_COLD_LOG_MODE=off TEAQL_LOG_ENDPOINT="$run_dir/prepare-masked.log" "$probe" prepare "$run_dir/school.sqlite" >"$run_dir/prepare.log" 2>&1
sha256sum "$run_dir/school.sqlite" >"$run_dir/database.sha256"
for round in 1 2 3 4 5 6 7; do
  TEAQL_COLD_LOG_MODE="$mode" TEAQL_LOG_ENDPOINT="$run_dir/runtime-masked.log" \
    /usr/bin/time -v -o "$run_dir/cpu-$round.txt" "$probe" query "$run_dir/school.sqlite" >"$run_dir/round-$round.log" 2>&1
  rg -F 'PASS generated Rust process-cold Q/E and shared snapshot' "$run_dir/round-$round.log"
done
sha256sum --check --quiet "$run_dir/database.sha256"
sha256sum --check --quiet "$run_dir/source.sha256"
sha256sum --check --quiet "$run_dir/generated-source.sha256"
