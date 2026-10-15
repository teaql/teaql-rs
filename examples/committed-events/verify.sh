#!/usr/bin/env bash
set -euo pipefail
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_dir"
for round in 1 2; do
  if ! output="$(cargo run --quiet -p teaql-examples --bin committed_events 2>&1)"; then
    printf 'Rust committed-events example round %s failed\n%s\n' "$round" "$output" >&2
    exit 1
  fi
  if [[ "$output" != *"COMMITTED_EVENTS_PASS authoritative=database accepted outbox=1 target=1 rollback=1"* ]]; then
    printf 'Rust committed-events example round %s failed or lacked evidence\n%s\n' "$round" "$output" >&2
    exit 1
  fi
  printf 'PASS Rust committed-events example round %s\n' "$round"
done
