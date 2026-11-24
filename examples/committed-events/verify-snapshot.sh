#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
df -h "$repo" "${TMPDIR:-/tmp}"
scratch="$(mktemp -d -t teaql-rust-snapshot.XXXXXXXX)"
cleanup() { status=$?; trap - EXIT; rm -rf -- "$scratch"; exit "$status"; }
trap cleanup EXIT
export CARGO_TARGET_DIR="$scratch/target" CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
cd "$repo"
for round in 1 2; do
  cargo run --offline --locked -p teaql-provider-sqlite --example committed_snapshot \
    2>&1 | tee "$scratch/round-$round.log"
  rg -Fq 'CMR11_EXAMPLE_PASS language=rust mode=local snapshot=0 tail=1 rows=2 catalog=2 duplicate_after_tail=1' "$scratch/round-$round.log"
  echo "PASS Rust committed snapshot example round $round"
done
