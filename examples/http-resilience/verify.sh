#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo"
cargo test --quiet -p teaql-http-reqwest
for round in 1 2; do
  output="$(cargo run --quiet -p teaql-examples --bin http_resilience 2>&1)"
  if [[ "$output" != *"HTTP_RESILIENCE_PASS status=200 attempts=1 requests=2 trace=2 lineage=3"* ]]; then
    printf 'Rust HTTP example round %s failed or lacked acceptance marker\n%s\n' "$round" "$output" >&2
    exit 1
  fi
  if [[ "$output" == *"PRIVATE_QUERY"* || "$output" == *"PRIVATE_HEADER"* || "$output" == *"PRIVATE_BODY"* ]]; then
    printf 'Rust HTTP example round %s leaked a private request canary\n' "$round" >&2
    exit 1
  fi
  printf 'PASS Rust HTTP example round %s\n' "$round"
done
