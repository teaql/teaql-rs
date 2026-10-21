#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo"
# The caller selects a compatible CARGO_TARGET_DIR; this script never evicts a
# shared cache. The small DB/log evidence directory is retained and printed.
output="$(cargo run --quiet -p teaql-examples --bin committed_process_crash 2>&1)" || {
  printf '%s\n' "$output" >&2
  exit 1
}
printf '%s\n' "$output"
for round in 1 2; do
  if ! rg -q "^CMR03_PROCESS_CRASH_PASS language=rust round=$round " <<< "$output"; then
    echo "missing Rust real-process crash round $round" >&2
    exit 1
  fi
done
echo 'PASS Rust CMR-03 real source-process crash rounds=2 same_databases=1'
