#!/usr/bin/env bash
set -euo pipefail
(( $# == 0 )) || { printf 'Usage: bash benchmark-mainstream.sh\n' >&2; exit 2; }
runtime_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_dir="$(mktemp -d -t teaql-rust-mainstream.XXXXXXXX)"
printf 'Mainstream benchmark evidence: %s\n' "$run_dir"
cd "$runtime_dir"
git diff HEAD --exit-code -- examples/shared-load-state teaql-core teaql-macros teaql-runtime teaql-sql teaql-provider-sqlite
git rev-parse HEAD >"$run_dir/base-commit.txt"
rustc -Vv >"$run_dir/toolchain.txt"
sha256sum examples/shared-load-state/tests/mainstream_orm_benchmark.rs examples/shared-load-state/Cargo.toml examples/shared-load-state/Cargo.lock teaql-runtime/tests/support/allocation_counter.rs >"$run_dir/source.sha256"
cargo tree --manifest-path examples/shared-load-state/Cargo.toml --locked -i libsqlite3-sys >"$run_dir/sqlite-binding.txt"
cargo test --manifest-path examples/shared-load-state/Cargo.toml --release --locked \
  --test mainstream_orm_benchmark --no-run --message-format=json \
  >"$run_dir/build.jsonl" 2>"$run_dir/build.log"
test_binary="$(jq -r 'select(.reason == "compiler-artifact" and .target.name == "mainstream_orm_benchmark" and .executable != null) | .executable' "$run_dir/build.jsonl")"
[[ -f "$test_binary" && -x "$test_binary" ]]
TEAQL_ORM_SAMPLES=31 /usr/bin/time -v -o "$run_dir/cpu.txt" "$test_binary" \
  --ignored --nocapture --test-threads=1 >"$run_dir/results.log" 2>&1
rg -F 'PASS mainstream Diesel and TeaQL bounded typed results with independent version and prefix filters' "$run_dir/results.log"
sha256sum --check --quiet "$run_dir/source.sha256"
