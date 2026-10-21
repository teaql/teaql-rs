#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo"
cargo test --quiet -p teaql-http-reqwest
for round in 1 2; do
  output="$(cargo run --quiet -p teaql-examples --bin http_resilience 2>&1)"
  if [[ "$output" != *"HTTP_RESILIENCE_PASS status=200 attempts=1 requests=4 trace=4 lineage=3"* ]]; then
    printf 'Rust HTTP example round %s failed or lacked acceptance marker\n%s\n' "$round" "$output" >&2
    exit 1
  fi
  for inner in 1 2; do
    if [[ "$output" != *"HTTP_SAME_FIXTURE_PASS language=rust round=$inner requests=$((2 * inner)) trace=$((2 * inner)) lineage=3"* ]]; then
      printf 'Rust HTTP example lacked same-fixture round %s\n' "$inner" >&2
      exit 1
    fi
  done
  if [[ "$output" == *"PRIVATE_QUERY"* || "$output" == *"PRIVATE_HEADER"* || "$output" == *"PRIVATE_BODY"* ]]; then
    printf 'Rust HTTP example round %s leaked a private request canary\n' "$round" >&2
    exit 1
  fi
  printf '%s\n' "$output"
  printf 'PASS Rust HTTP example round %s\n' "$round"
done
